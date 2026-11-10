#!/usr/bin/env bun
/**
 * GTFS Import Pipeline
 *
 * Downloads GTFS feeds from Transitland, parses stop/route data,
 * imports into PostGIS, and computes walking transfers.
 *
 * Usage:
 *   bun run import/import-gtfs.ts --region nc --api-key tlk_xxx --output-dir ./data/gtfs
 */

import { parseArgs } from 'util'
import { existsSync, mkdirSync, writeFileSync, readdirSync, renameSync, rmSync } from 'fs'
import { join, basename } from 'path'
import { injectFaresV2 } from './inject-fares-v2'
import { injectBikesAllowed } from './inject-bikes-allowed'
import { injectTripShapesAll } from './inject-trip-shapes'
import { dedupeFeedTrips } from './dedupe-feed-trips'
import { importFeedFile, injectTransfersTxt, NoFixedRouteService } from './feed-import'
import { ensureGtfsSchema } from '../src/db'
import { stationTransferTimes, type StationTransfer } from '../src/services/station-paths.service'
import type { GtfsRtUrl } from '../src/services/gtfs.service'
import {
  fetchFeedList,
  computeAllTransfers,
  generateTransfersTxt,
  transferKey,
  gatedStops,
  generateMotisConfig,
  sanitizeGtfsZip,
  loadFeedIdentities,
  withholdFeed,
  type GtfsFeedInfo,
} from '../src/services/gtfs.service'
import { isExcludedByPolicy, licensePolicy } from '../src/lib/feed-license'
import {
  alignRealtimeTripIds,
  describeAlignment,
  resolvesClearlyMore,
  sampleRealtimeTrips,
  type AlignmentResult,
} from './align-trip-ids'
import { rtEgressProxy, selectRtUrls } from '../src/services/rt-url-selection'
import { measureRtNormalize } from './measure-rt-normalize'
import { parseCsvRows, headerIndex } from '../src/lib/csv'
import JSZip from 'jszip'

// ── CLI args ────────────────────────────────────────────────────────

const { values: args } = parseArgs({
  options: {
    region: { type: 'string', default: 'nc' },
    'api-key': { type: 'string' },
    'output-dir': { type: 'string', default: './data/gtfs' },
    'motis-config': { type: 'string', default: './motis/config.yml' },
    'skip-download': { type: 'boolean', default: false },
    'skip-transfers': { type: 'boolean', default: false },
    'max-feeds': { type: 'string' },
    'feed-ids': { type: 'string' },
    'transfer-distance': { type: 'string', default: '500' },
    'transfer-concurrency': { type: 'string', default: '8' },
  },
})

const region = args.region!
const apiKey = args['api-key']!
const outputDir = args['output-dir']!
const skipDownload = args['skip-download']!
const skipTransfers = args['skip-transfers']!
const maxFeeds = args['max-feeds'] ? parseInt(args['max-feeds']) : undefined
const onlyFeedIds = args['feed-ids'] ? new Set(args['feed-ids'].split(',').map(s => s.trim())) : undefined
const motisConfigPath = args['motis-config']!
const transferDistance = parseInt(args['transfer-distance']!)
const transferConcurrency = parseInt(args['transfer-concurrency']!)

if (!apiKey && !skipDownload) {
  console.error('Error: --api-key is required (unless --skip-download is set)')
  process.exit(1)
}

// ── Main pipeline ───────────────────────────────────────────────────

/**
 * Align a feed's static trip_ids with the ids its realtime feed publishes, and
 * say what happened. A skipped alignment still imports the feed, which then
 * serves schedules and no realtime. That is what an affected feed did before
 * this step existed, so it is reported rather than treated as a failure.
 */
async function alignTripIds(
  buffer: ArrayBuffer,
  feed: { feedId: string; onestopId?: string; name?: string; rtUrls?: GtfsRtUrl[] },
  aligned?: { buffer: ArrayBuffer; result: AlignmentResult },
): Promise<ArrayBuffer> {
  const label = feed.name || feed.onestopId || feed.feedId
  const { buffer: out, result } = aligned ?? await alignRealtimeTripIds(buffer, feed, {
    onWarn: message => console.error(`  ⚠ ${label}: ${message}`),
  })

  if (result.applied) {
    const counts = Object.entries(result.rewrittenRows)
      .map(([file, n]) => `${file} ${n}`)
      .join(', ')
    console.log(`  ✓ Realtime trip_ids did not match static. Rewrote to ${describeAlignment(result)} (${counts})`)
    console.log(
      `    ${result.matched}/${result.sampleSize} sampled realtime trips now resolve; ` +
        `${result.heldTrips} trip(s) kept their published id so no two trips share one`,
    )
  } else if (result.skipReason === 'date-overlap') {
    console.error(
      `  ✗ trip_id alignment SKIPPED for ${label}: ${result.overlaps.length} id(s) would be ` +
        'shared by trips running on the same date, which MOTIS cannot tell apart. Realtime ' +
        'will not resolve for this feed.',
    )
    for (const o of result.overlaps.slice(0, 5)) {
      console.error(`      ${o.tripId}: ${o.serviceA} vs ${o.serviceB} share ${o.sharedDates.join(', ')}`)
    }
  } else if (result.skipReason === 'no-improvement') {
    console.error(
      `  ⚠ ${label}: ${result.matched}/${result.sampleSize} sampled realtime trips resolve and ` +
        'no rewrite improved on that. The two id spaces may differ in a way this cannot repair.',
    )
  }

  return out
}

/**
 * A feed's static zip from the agency, or Transitland's archived copy of its
 * latest version when the agency's server fails. A feed left on its last zip
 * keeps serving that timetable after it runs out.
 */
/** The zip a URL names, where `archive.zip#inner.zip` is a feed packed inside another zip (SEPTA). */
async function staticFromResponse(response: Response, url: string): Promise<ArrayBuffer | null> {
  const buffer = await response.arrayBuffer()
  const inner = new URL(url).hash.slice(1)
  if (!inner) return buffer
  const entry = (await JSZip.loadAsync(buffer)).file(decodeURIComponent(inner))
  if (!entry) console.error(`  ✗ ${inner} is not in the downloaded archive`)
  return entry ? entry.async('arraybuffer') : null
}

/** An agency that refuses this network leaves the connection hanging rather than refusing it. */
const DOWNLOAD_TIMEOUT_MS = 120_000

async function fetchStatic(url: string): Promise<Response> {
  const attempt = (proxy?: string) => fetch(url, { signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS), ...(proxy && { proxy }) } as RequestInit)
  const proxy = rtEgressProxy()
  try {
    return await attempt()
  } catch (err) {
    if (!proxy) throw err
    console.log('  ↻ Retrying through the egress proxy')
    return attempt(proxy)
  }
}

async function downloadStatic(feed: { url: string; onestopId?: string }): Promise<ArrayBuffer | null> {
  try {
    const response = await fetchStatic(feed.url)
    if (response.ok) return await staticFromResponse(response, feed.url)
    console.error(`  ✗ Download failed: ${response.status}`)
  } catch (err) {
    console.error(`  ✗ Download failed: ${err instanceof Error ? err.message : err}`)
  }
  if (!feed.onestopId) return null
  const archived = await fetch(
    `https://transit.land/api/v2/rest/feeds/${encodeURIComponent(feed.onestopId)}/download_latest_feed_version?apikey=${apiKey}`,
  ).catch(() => null)
  if (!archived?.ok) return null
  console.log("  ↻ Using Transitland's copy of the latest version")
  return archived.arrayBuffer()
}

/** Downloads of alternate static feeds, shared by every feed of one operator. */
const alternateDownloads = new Map<string, Promise<ArrayBuffer | null>>()

function downloadAlternate(url: string): Promise<ArrayBuffer | null> {
  let pending = alternateDownloads.get(url)
  if (!pending) {
    pending = fetch(url)
      .then(async response => (response.ok ? (await sanitizeGtfsZip(await response.arrayBuffer())).buffer : null))
      .catch(() => null)
    alternateDownloads.set(url, pending)
  }
  return pending
}

/**
 * Align the feed, or one of its operator's other static feeds in its place.
 *
 * An alternate is taken only when it serves exactly the same routes — so it is
 * the same service, not an aggregate that would duplicate another feed — and,
 * aligned against the same realtime sample, reaches clearly more of it. That
 * is how a schedule that also carries the coming week's planned changes wins
 * over the base timetable, without the import knowing which agencies publish
 * one.
 */
async function alignBestStatic(
  buffer: ArrayBuffer,
  feed: { feedId: string; onestopId?: string; name?: string; url: string; rtUrls?: GtfsRtUrl[]; alternateUrls?: string[] },
): Promise<{ buffer: ArrayBuffer; url: string }> {
  if (!feed.alternateUrls?.length || !feed.rtUrls?.length) {
    return { buffer: await alignTripIds(buffer, feed), url: feed.url }
  }

  const label = feed.name || feed.onestopId || feed.feedId
  const rtTrips = await sampleRealtimeTrips(feed.rtUrls, fetch, message => console.error(`  ⚠ ${label}: ${message}`))
  let best = { url: feed.url, source: buffer, ...await alignRealtimeTripIds(buffer, feed, { rtTrips }) }
  const routes = await staticRouteIds(buffer)

  for (const url of feed.alternateUrls) {
    const alternate = await downloadAlternate(url)
    if (!alternate) continue
    const alternateRoutes = await staticRouteIds(alternate)
    if (alternateRoutes.size !== routes.size || [...routes].some(r => !alternateRoutes.has(r))) continue

    const aligned = await alignRealtimeTripIds(alternate, feed, { rtTrips })
    if (!resolvesClearlyMore(aligned.result, best.result)) continue
    console.log(
      `  ✓ ${url} serves the same routes and resolves ${aligned.result.resolving}/${aligned.result.sampleSize} ` +
        `sampled realtime trips against ${best.result.resolving}; importing it instead`,
    )
    best = { url, source: alternate, ...aligned }
  }

  return { buffer: await alignTripIds(best.source, feed, best), url: best.url }
}

async function columnValues(zip: JSZip, name: string, column: string): Promise<Set<string>> {
  const file = zip.file(name)
  if (!file) return new Set()
  const rows = parseCsvRows(await file.async('string'))
  const col = rows.length ? headerIndex(rows[0]!).get(column) : undefined
  if (col === undefined) return new Set()
  return new Set(rows.slice(1).map(cells => cells[col]?.trim() ?? '').filter(Boolean))
}

/**
 * Transitland's realtime URLs plus the ones already on record, which may have
 * been found by backfill-rt-urls.ts or corrected by hand. Selection then drops
 * whichever no longer serve the feed.
 */
function withStoredRealtime(listed: GtfsRtUrl[] | undefined, stored: GtfsRtUrl[] | null | undefined): GtfsRtUrl[] | undefined {
  const all = [...(stored ?? []), ...(listed ?? [])]
  const seen = new Set<string>()
  const out = all.filter(rt => {
    const key = `${rt.type ?? ''} ${rt.url}`
    return !seen.has(key) && !!seen.add(key)
  })
  return out.length ? out : undefined
}

const staticRouteIds = async (buffer: ArrayBuffer) => columnValues(await JSZip.loadAsync(buffer), 'routes.txt', 'route_id')

/**
 * Drop the discovered realtime URLs that don't serve this feed, so neither the
 * alignment sample nor MOTIS sees another agency's trips.
 */
async function selectRealtime(
  buffer: ArrayBuffer,
  feed: { feedId: string; onestopId?: string; name?: string; rtUrls?: GtfsRtUrl[] },
): Promise<GtfsRtUrl[] | undefined> {
  if (!feed.rtUrls?.length) return feed.rtUrls
  const zip = await JSZip.loadAsync(buffer)
  const { kept, dropped } = await selectRtUrls(feed.rtUrls, await columnValues(zip, 'routes.txt', 'route_id'), fetch, {
    trips: await columnValues(zip, 'trips.txt', 'trip_id'),
    stops: await columnValues(zip, 'stops.txt', 'stop_id'),
  })
  const label = feed.name || feed.onestopId || feed.feedId
  for (const { url, reason } of dropped) {
    console.log(`  ⚠ ${label}: dropped realtime ${url.split('?')[0]} (${reason})`)
  }
  return kept
}

/**
 * Withhold a feed GTFS_LICENCE_POLICY excludes: drop its rows and its zip, so
 * neither the database nor the next MOTIS build serves it.
 */
async function withhold(feed: GtfsFeedInfo, zipPath: string) {
  const terms = feed.license
  console.log(
    `  – Withheld ${feed.name || feed.feedId}: licence forbids ` +
      (terms?.commercialUseAllowed === 'no' ? 'commercial use' : 'redistribution') +
      ` (${terms?.spdxIdentifier ?? terms?.url ?? 'custom licence'})`,
  )
  await withholdFeed(feed)
  rmSync(zipPath, { force: true })
}

async function main() {
  console.log(`\n=== GTFS Import Pipeline ===`)
  console.log(`Region: ${region}`)
  console.log(`Output: ${outputDir}`)
  console.log(`Transfer distance: ${transferDistance}m`)
  console.log(`Transfer concurrency: ${transferConcurrency}`)
  const policy = licensePolicy()
  console.log(`Licence policy: ${policy}`)
  console.log('')

  // Ensure schema exists
  console.log('Ensuring GTFS schema...')
  await ensureGtfsSchema()

  // Create output directory
  mkdirSync(outputDir, { recursive: true })

  let feedFiles: string[] = []
  // The feed id each file holds. A downloaded file's name is a sanitized copy
  // of the id, so the name alone does not always match gtfs_stops.feed_id.
  const feedIdOf = new Map<string, string>()
  // How many feeds we tried, so a run where every one failed can be told apart
  // from a region that legitimately has none.
  let attemptedFeeds = 0
  // Feed ids that failed, and how many had nothing to import. A failed feed
  // keeps its previous database contents (importFeedFile is all-or-nothing).
  const failedFeeds: string[] = []
  let skippedFeeds = 0
  const recordFailure = (feedId: string, err: unknown) => {
    if (err instanceof NoFixedRouteService) {
      skippedFeeds++
      console.log(`  – Skipped: ${err.message}`)
      return
    }
    failedFeeds.push(feedId)
    console.error(`  ✗ Error: ${err instanceof Error ? err.message : err}`)
  }

  // Read before anything is imported: importFeedFile clears each feed's row,
  // so whatever a caller doesn't carry forward is gone. rt_urls in particular
  // come from backfill-rt-urls.ts, never from this script.
  const stored = await loadFeedIdentities()

  if (!skipDownload) {
    // Step 1: Fetch feed list from Transitland
    console.log(`\nFetching feed list from Transitland (region: ${region})...`)
    let feeds = await fetchFeedList(region, apiKey)
    console.log(`Found ${feeds.length} GTFS feeds`)

    const rtCount = feeds.filter(f => f.rtUrls?.length).length
    console.log(`  ${rtCount} feeds have GTFS-RT URLs`)

    if (onlyFeedIds) {
      feeds = feeds.filter(f => onlyFeedIds.has(f.feedId) || onlyFeedIds.has(f.onestopId ?? ''))
      console.log(`Limited to ${feeds.length} feed(s) named by --feed-ids`)
    }

    if (maxFeeds) {
      feeds = feeds.slice(0, maxFeeds)
      console.log(`Limited to ${maxFeeds} feeds`)
    }

    // Feeds whose licence forbids commercial use or redistribution are not
    // downloaded under the `commercial` policy. Unknown terms are allowed.
    const excluded = feeds.filter(f => isExcludedByPolicy(f.license, policy))
    if (excluded.length) {
      console.log(`\nWithholding ${excluded.length} feed(s) under GTFS_LICENCE_POLICY=${policy}:`)
      for (const feed of excluded) {
        const zip = join(outputDir, `${feed.feedId.replace(/[^a-zA-Z0-9_-]/g, '_')}.zip`)
        await withhold({ ...feed, rtUrls: feed.rtUrls ?? stored.get(feed.feedId)?.rtUrls ?? undefined }, zip)
      }
      feeds = feeds.filter(f => !excluded.includes(f))
    }

    // Step 2: Download each feed
    attemptedFeeds = feeds.length
    for (let i = 0; i < feeds.length; i++) {
      const feed = feeds[i]
      const filename = `${feed.feedId.replace(/[^a-zA-Z0-9_-]/g, '_')}.zip`
      const filepath = join(outputDir, filename)

      console.log(`\n[${i + 1}/${feeds.length}] Downloading ${feed.name || feed.feedId}...`)
      console.log(`  URL: ${feed.url}`)
      if (feed.rtUrls?.length) {
        console.log(`  RT: ${feed.rtUrls.length} realtime feed(s)`)
      }

      try {
        const rawBuffer = await downloadStatic(feed)
        if (!rawBuffer) continue

        console.log(`  ✓ Downloaded ${(rawBuffer.byteLength / 1024 / 1024).toFixed(1)} MB`)

        // Strip GTFS-Flex extension files that crash MOTIS
        const { buffer, removedFiles, repairedFiles } = await sanitizeGtfsZip(rawBuffer)
        if (removedFiles.length > 0) {
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files: ${removedFiles.join(', ')}`)
        }
        if (repairedFiles.length > 0) console.log(`  ⚠ Repaired padding or timezones in ${repairedFiles.join(', ')}`)

        // Transitland's feed listing only finds RT URLs published on the
        // feed's own record or its `~rt` companion; backfill-rt-urls.ts also
        // walks the operator listing and finds more. Keep what's stored when
        // this run turned up nothing — the alignment step needs them too, to
        // sample what the feed's realtime actually publishes.
        const merged = {
          ...feed,
          rtUrls: withStoredRealtime(feed.rtUrls, stored.get(feed.feedId)?.rtUrls),
        }
        merged.rtUrls = await selectRealtime(buffer, merged)

        const chosen = await alignBestStatic(buffer, merged)
        merged.url = chosen.url
        merged.rtUrls = await measureRtNormalize(chosen.buffer, merged.rtUrls)

        // Step 3: Parse and import. Staged beside the feed's zip and moved
        // into place only once the import commits, so a feed that fails or is
        // skipped keeps the zip that matches its previous database contents.
        // MOTIS builds from every zip gtfs_feeds names, and a broken download
        // left in place (one night: a near-empty zip that later steps padded
        // with only transfers.txt) aborts its whole import. The `.part`
        // suffix keeps a leftover out of --skip-download's *.zip listing.
        const staged = `${filepath}.part`
        writeFileSync(staged, Buffer.from(chosen.buffer))
        try {
          await importFeedFile(staged, merged)
        } catch (err) {
          rmSync(staged, { force: true })
          throw err
        }
        renameSync(staged, filepath)
        feedFiles.push(filepath)
        feedIdOf.set(filepath, merged.feedId)
      } catch (err) {
        recordFailure(feed.feedId, err)
      }
    }
  } else {
    // Skip download — import existing files
    console.log('\nSkipping download, importing existing GTFS files...')
    feedFiles = readdirSync(outputDir)
      .filter(f => f.endsWith('.zip'))
      .map(f => join(outputDir, f))
    attemptedFeeds = feedFiles.length
    const withheld = new Set<string>()

    for (const filepath of feedFiles) {
      const feedId = basename(filepath, '.zip')

      // The filename is the only identity this path has. Everything else has
      // to come from the row already in gtfs_feeds, or the re-import
      // overwrites the real onestop id with the numeric feed id and drops the
      // feed's name, URL and realtime URLs.
      const existing = stored.get(feedId)
      const feed = {
        feedId,
        onestopId: existing?.onestopId || feedId,
        name: existing?.name || feedId,
        url: existing?.url || '',
        region: existing?.region || region,
        rtUrls: existing?.rtUrls ?? undefined,
      }

      // Same policy as the download path, read from the licence on record.
      if (isExcludedByPolicy(existing?.license, policy)) {
        await withhold({ ...feed, license: existing!.license }, filepath)
        withheld.add(filepath)
        continue
      }

      // Sanitize existing files too (they may pre-date the flex strip), then
      // align trip_ids. Re-running over existing zips has to reach the same
      // state as a fresh download, or --skip-download quietly produces a feed
      // whose realtime never resolves. Running it twice is safe: an already
      // aligned feed measures as resolving and is left alone.
      try {
        const existingBuffer = await Bun.file(filepath).arrayBuffer()
        const { buffer: cleanBuffer, removedFiles } = await sanitizeGtfsZip(existingBuffer)
        if (removedFiles.length > 0) {
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files from ${basename(filepath)}`)
        }
        feed.rtUrls = await selectRealtime(cleanBuffer, feed)
        const aligned = await alignTripIds(cleanBuffer, feed)
        feed.rtUrls = await measureRtNormalize(aligned, feed.rtUrls)
        if (aligned !== existingBuffer) {
          writeFileSync(filepath, Buffer.from(aligned))
        }
      } catch (err) {
        console.error(`  ⚠ Zip preparation failed for ${basename(filepath)}: ${err}`)
      }

      try {
        await importFeedFile(filepath, feed)
      } catch (err) {
        recordFailure(feedId, err)
      }
    }
    feedFiles = feedFiles.filter(f => !withheld.has(f))
    attemptedFeeds -= withheld.size
  }

  // Step 4: Compute walking transfers
  // A run that downloaded nothing is a failure, not an empty success. It used
  // to print "Processed 0 feeds" and exit 0, which let scripts/gtfs-watch.sh
  // record the upstream shas as imported and stop retrying the drift.
  if (feedFiles.length === 0 && attemptedFeeds > 0) {
    console.error(
      `\n✗ Import failed: 0 of ${attemptedFeeds} feeds were written to ${outputDir}. ` +
        'See the per-feed errors above.',
    )
    process.exit(1)
  }

  if (!skipTransfers && feedFiles.length > 0) {
    console.log('\n=== Computing Walking Transfers ===')
    console.log(`Max distance: ${transferDistance}m`)

    const transfers = await computeAllTransfers(
      transferDistance,
      transferConcurrency,
      globalThis.fetch,
      (completed, total) => {
        if (completed % 100 === 0 || completed === total) {
          console.log(`  ${completed}/${total} pairs computed`)
        }
      },
      // Only the feeds this run wrote get a transfers.txt, so only their stop
      // pairs are worth a GraphHopper call.
      feedFiles.map((f) => feedIdOf.get(f) ?? basename(f, '.zip')),
    )

    console.log(`Computed ${transfers.length} transfer pairs`)

    // In-station times from the station paths build override the street walk
    // and the agency's flat minimum. Before the first build there are none.
    const station = await stationTransferTimes(feedFiles.map((f) => feedIdOf.get(f) ?? basename(f, '.zip')))
      .catch((err) => {
        console.warn(`  ! station transfer times: ${(err as Error).message}`)
        return [] as StationTransfer[]
      })
    const gated = await gatedStops(feedFiles.map((f) => feedIdOf.get(f) ?? basename(f, '.zip')))
      .catch((err) => {
        console.warn(`  ! gated stops: ${(err as Error).message}`)
        return new Map<string, Set<string>>()
      })
    if (station.length) {
      const bySource = new Map<string, number>()
      for (const s of station) bySource.set(s.source, (bySource.get(s.source) ?? 0) + 1)
      console.log(`  ${station.length} in-station transfer times (${[...bySource].map(([k, v]) => `${k} ${v}`).join(', ')})`)
    }

    // Write feed-specific transfers.txt into each feed ZIP
    // Only include transfers where BOTH stops belong to the target feed,
    // preventing stop ID collisions across different transit agencies.
    if (transfers.length > 0 || station.length > 0) {
      for (const filepath of feedFiles) {
        const feedId = feedIdOf.get(filepath) ?? basename(filepath, '.zip')
        const own = station.filter((s) => s.feedId === feedId)
        const transfersTxt = generateTransfersTxt(transfers, feedId, own)
        const transferCount = transfersTxt.trim().split('\n').length - 1 // minus header
        try {
          const rail = new Set([...own.flatMap((s) => [s.fromStopId, s.toStopId]), ...(gated.get(feedId) ?? [])])
          await injectTransfersTxt(filepath, transfersTxt, new Map(own.map((s) => [transferKey(s.fromStopId, s.toStopId), { seconds: s.seconds, source: s.source }])), rail)
          console.log(`  ✓ Injected ${transferCount} transfers into ${basename(filepath)}`)
        } catch (err) {
          console.error(`  ✗ Failed to inject into ${basename(filepath)}: ${err}`)
        }
      }
    }
  }

  // Step 4c: Default bikes_allowed to "allowed" where a feed leaves it
  // unstated, so bike-carriage routing isn't dead everywhere. An explicit
  // "not allowed" is preserved.
  console.log('\n=== Defaulting bikes_allowed ===')
  for (const filepath of feedFiles) {
    try {
      const status = await injectBikesAllowed(filepath)
      if (status.startsWith('filled')) {
        console.log(`  ✓ ${basename(filepath)}: ${status}`)
      }
    } catch (err) {
      console.error(`  ✗ ${basename(filepath)}: ${err}`)
    }
  }

  // Every zip, not just this run's: the feed a trip is repeated from may not have changed.
  console.log('\n=== Dropping trips another feed already runs ===')
  await dedupeFeedTrips(readdirSync(outputDir).filter(f => f.endsWith('.zip')).map(f => join(outputDir, f)))

  // Step 4d: Lend shapeless trips a sibling's shape, or MOTIS draws them as
  // straight lines between stops.
  console.log('\n=== Filling trip shapes ===')
  await injectTripShapesAll(feedFiles)

  // Step 4b: Synthesize GTFS Fares v2 from v1 fare data so MOTIS (which
  // reads v2 only) can price itineraries from the agency's own feed.
  console.log('\n=== Converting Fares v1 → v2 ===')
  for (const filepath of feedFiles) {
    try {
      const status = await injectFaresV2(filepath)
      if (status.startsWith('converted')) {
        console.log(`  ✓ ${basename(filepath)}: ${status}`)
      }
    } catch (err) {
      console.error(`  ✗ ${basename(filepath)}: ${err}`)
    }
  }

  // Step 5: Generate MOTIS config with GTFS-RT feeds
  console.log('\n=== Generating MOTIS Config ===')
  try {
    const configYaml = await generateMotisConfig()
    mkdirSync(join(motisConfigPath, '..'), { recursive: true })
    writeFileSync(motisConfigPath, configYaml)
    console.log(`✓ Wrote MOTIS config to ${motisConfigPath}`)

    // Count RT-enabled feeds
    const rtLines = configYaml.split('\n').filter(l => l.trim().startsWith('- url:'))
    if (rtLines.length > 0) {
      console.log(`  ${rtLines.length} GTFS-RT feed URLs configured`)
    } else {
      console.log('  No GTFS-RT feeds found for this region')
    }
  } catch (err) {
    console.error(`✗ Failed to generate MOTIS config: ${err instanceof Error ? err.message : err}`)
  }

  console.log('\n=== Import Complete ===')
  console.log(`Processed ${feedFiles.length} feeds`)
  if (skippedFeeds > 0) console.log(`  ${skippedFeeds} feed(s) had no fixed-route service to import`)
  console.log(`Output directory: ${outputDir}`)
  console.log(`MOTIS config: ${motisConfigPath}`)
  console.log('')
  console.log('Next steps:')
  // NOT `docker compose restart motis`: `motis server` only serves the
  // pre-built dataset at /data/data and never re-imports, so a restart keeps
  // serving the old schedules and the feeds just downloaded never reach riders.
  console.log('  1. Rebuild the MOTIS dataset so these feeds take effect')
  console.log('     bash scripts/rebuild-motis.sh   (a restart alone keeps the old timetable)')
  console.log('')

  // Out of a region's worth of upstream feeds, some are always broken: a dead
  // URL, a malformed CSV. Each keeps its previous contents, and failing the
  // run for one of them stops every other feed reaching riders, since
  // gtfs-watch only rebuilds MOTIS after a clean import. So only a failure
  // rate that looks like our bug rather than theirs fails the run. A parser
  // that misread every BOM-prefixed file failed a quarter of the US.
  const attempted = skipDownload ? feedFiles.length : attemptedFeeds
  if (failedFeeds.length > 0) {
    const share = failedFeeds.length / Math.max(attempted, 1)
    console.error(
      `${failedFeeds.length} of ${attempted} feed(s) failed and kept their previous contents: ` +
        failedFeeds.join(', '),
    )
    if (share > MAX_FAILED_FEED_SHARE) {
      console.error(
        `✗ More than ${MAX_FAILED_FEED_SHARE * 100}% of feeds failed — that looks systemic, not upstream.`,
      )
      process.exit(1)
    }
  }

  process.exit(0)
}

/** Above this share of failed feeds, the import fails the run (see the end of main). */
const MAX_FAILED_FEED_SHARE = 0.15

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
