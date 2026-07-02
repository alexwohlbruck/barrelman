#!/usr/bin/env bun
/**
 * GTFS Import Pipeline
 *
 * Downloads GTFS feeds from Transitland into data/gtfs (raw + flex-sanitized,
 * kept pristine), transforms each into a fully preprocessed copy under
 * data/gtfs-processed (shape rewrite hook, display overrides baked in), then
 * parses the PROCESSED zips into PostGIS, computes walking transfers, injects
 * transfers.txt + Fares v2 into the processed zips, and generates the MOTIS
 * config. MOTIS must only ever ingest the processed zips — load them with
 * scripts/rebuild-motis.sh after this pipeline finishes.
 *
 * Usage:
 *   bun run import/import-gtfs.ts --region nc --api-key tlk_xxx --output-dir ./data/gtfs
 */

import { parseArgs } from 'util'
import { existsSync, mkdirSync, writeFileSync, readdirSync, readFileSync, copyFileSync, rmSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { sql } from 'drizzle-orm'
import { injectFaresV2 } from './inject-fares-v2'
import { ensureGtfsSchema, db } from '../src/db'
import {
  fetchFeedList,
  parseStops,
  parseRoutes,
  parseAgencies,
  parseShapes,
  deriveStopRoutes,
  resolveServiceCalendar,
  parseGtfsRecords,
  parseStopParents,
  deriveTripPatterns,
  importTripPatterns,
  parseTransfers,
  importTransfers,
  deriveRouteShapes,
  deriveBikesAllowed,
  importStops,
  importRoutes,
  importStopRoutes,
  importShapes,
  updateRouteShapes,
  updateBikesAllowed,
  recordFeed,
  applyDisplayOverrides,
  bakeDisplayOverridesIntoZip,
  selectFeeds,
  clearFeed,
  computeAllTransfers,
  generateTransfersTxt,
  generateMotisConfig,
  sanitizeGtfsZip,
  type GtfsFeedInfo,
} from '../src/services/gtfs.service'

// ── CLI args ────────────────────────────────────────────────────────

const { values: args } = parseArgs({
  options: {
    region: { type: 'string', default: 'nc' },
    'api-key': { type: 'string' },
    'output-dir': { type: 'string', default: './data/gtfs' },
    'processed-dir': { type: 'string', default: './data/gtfs-processed' },
    'motis-config': { type: 'string', default: './motis/config.yml' },
    'skip-download': { type: 'boolean', default: false },
    'skip-transfers': { type: 'boolean', default: false },
    'max-feeds': { type: 'string' },
    'transfer-distance': { type: 'string', default: '500' },
    'transfer-concurrency': { type: 'string', default: '8' },
    'feed-allow': { type: 'string' },
    'feed-deny': { type: 'string' },
  },
})

const region = args.region!
const apiKey = args['api-key']!
const outputDir = args['output-dir']!
const processedDir = args['processed-dir']!
const skipDownload = args['skip-download']!
const skipTransfers = args['skip-transfers']!
const maxFeeds = args['max-feeds'] ? parseInt(args['max-feeds']) : undefined
const motisConfigPath = args['motis-config']!
const transferDistance = parseInt(args['transfer-distance']!)
const transferConcurrency = parseInt(args['transfer-concurrency']!)

if (!apiKey && !skipDownload) {
  console.error('Error: --api-key is required (unless --skip-download is set)')
  process.exit(1)
}

// ── Main pipeline ───────────────────────────────────────────────────

async function main() {
  console.log(`\n=== GTFS Import Pipeline ===`)
  console.log(`Region: ${region}`)
  console.log(`Output: ${outputDir}`)
  console.log(`Processed: ${processedDir}`)
  console.log(`Transfer distance: ${transferDistance}m`)
  console.log(`Transfer concurrency: ${transferConcurrency}`)
  console.log('')

  // Ensure schema exists
  console.log('Ensuring GTFS schema...')
  await ensureGtfsSchema()

  // Create output directories
  mkdirSync(outputDir, { recursive: true })
  mkdirSync(processedDir, { recursive: true })

  // Raw (sanitized) zips paired with their feed metadata, in pipeline order.
  const rawFeeds: Array<{ filepath: string; feed: GtfsFeedInfo }> = []

  if (!skipDownload) {
    // Stage 1: Download raw feeds from Transitland → data/gtfs
    console.log(`\nFetching feed list from Transitland (region: ${region})...`)
    let feeds = await fetchFeedList(region, apiKey)
    console.log(`Found ${feeds.length} GTFS feeds`)

    // DMFR-style curated, license-aware selection: drop feeds we can't
    // redistribute, plus any explicit allow/deny list (comma-sep onestop ids).
    const before = feeds.length
    feeds = selectFeeds(feeds, {
      excludeUnredistributable: true,
      allow: args['feed-allow']?.split(',').map(s => s.trim()).filter(Boolean),
      deny: args['feed-deny']?.split(',').map(s => s.trim()).filter(Boolean),
    })
    if (feeds.length !== before) {
      console.log(`Selected ${feeds.length}/${before} feeds after license + allow/deny filter`)
    }

    const rtCount = feeds.filter(f => f.rtUrls?.length).length
    console.log(`  ${rtCount} feeds have GTFS-RT URLs`)

    if (maxFeeds) {
      feeds = feeds.slice(0, maxFeeds)
      console.log(`Limited to ${maxFeeds} feeds`)
    }

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
        const response = await fetch(feed.url)
        if (!response.ok) {
          console.error(`  ✗ Download failed: ${response.status}`)
          continue
        }

        const rawBuffer = await response.arrayBuffer()
        console.log(`  ✓ Downloaded ${(rawBuffer.byteLength / 1024 / 1024).toFixed(1)} MB`)

        // Strip GTFS-Flex extension files that crash MOTIS
        const { buffer, removedFiles } = await sanitizeGtfsZip(rawBuffer)
        if (removedFiles.length > 0) {
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files: ${removedFiles.join(', ')}`)
        }

        writeFileSync(filepath, Buffer.from(buffer))
        rawFeeds.push({ filepath, feed })
      } catch (err) {
        console.error(`  ✗ Error: ${err instanceof Error ? err.message : err}`)
      }
    }
  } else {
    // Stage 1 (skip-download): use existing raw files in data/gtfs
    console.log('\nSkipping download, using existing GTFS files...')
    for (const name of readdirSync(outputDir).filter(f => f.endsWith('.zip'))) {
      const filepath = join(outputDir, name)
      const feedId = basename(filepath, '.zip')

      // Sanitize existing files too (they may pre-date the flex strip)
      try {
        const existingBuffer = await Bun.file(filepath).arrayBuffer()
        const { buffer: cleanBuffer, removedFiles } = await sanitizeGtfsZip(existingBuffer)
        if (removedFiles.length > 0) {
          writeFileSync(filepath, Buffer.from(cleanBuffer))
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files from ${basename(filepath)}`)
        }
      } catch (err) {
        console.error(`  ⚠ Flex sanitization failed for ${basename(filepath)}: ${err}`)
      }

      rawFeeds.push({
        filepath,
        feed: { feedId, onestopId: feedId, name: feedId, url: '', region },
      })
    }
  }

  // Stage 2: Transform — produce the fully preprocessed zips MOTIS ingests.
  // Every rewrite (shape rewrite, display overrides) lands here, BEFORE the DB
  // parse, so PostGIS, the display pipeline, and MOTIS all read one artifact.
  console.log('\n=== Transforming Feeds (raw → processed) ===')
  const processedFeeds: Array<{ filepath: string; feed: GtfsFeedInfo }> = []
  for (const { filepath, feed } of rawFeeds) {
    try {
      const processedPath = await transformFeed(filepath, feed)
      processedFeeds.push({ filepath: processedPath, feed })
    } catch (err) {
      console.error(`  ✗ Transform failed for ${basename(filepath)}: ${err instanceof Error ? err.message : err}`)
    }
  }
  console.log(`Transformed ${processedFeeds.length}/${rawFeeds.length} feeds → ${processedDir}`)

  // Stage 3: Parse the PROCESSED zips into PostGIS
  console.log('\n=== Importing Feeds into PostGIS ===')
  const feedFiles: string[] = []
  for (const { filepath, feed } of processedFeeds) {
    console.log(`\nImporting ${feed.name || feed.feedId} (${basename(filepath)})...`)
    await importFeedFile(filepath, feed)
    feedFiles.push(filepath)
  }

  // Stage 4: Compute walking transfers (needs the stops in PostGIS, so it
  // runs after the DB import) and inject transfers.txt into the PROCESSED zips
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
    )

    console.log(`Computed ${transfers.length} transfer pairs`)

    // Write feed-specific transfers.txt into each PROCESSED zip.
    // Only include transfers where BOTH stops belong to the target feed,
    // preventing stop ID collisions across different transit agencies.
    if (transfers.length > 0) {
      for (const filepath of feedFiles) {
        const feedId = basename(filepath, '.zip')
        const transfersTxt = generateTransfersTxt(transfers, feedId)
        const transferCount = transfersTxt.trim().split('\n').length - 1 // minus header
        try {
          await injectTransfersTxt(filepath, transfersTxt)
          console.log(`  ✓ Injected ${transferCount} transfers into ${basename(filepath)}`)
        } catch (err) {
          console.error(`  ✗ Failed to inject into ${basename(filepath)}: ${err}`)
        }
      }
    }
  }

  // Stage 5: Synthesize GTFS Fares v2 from v1 fare data so MOTIS (which
  // reads v2 only) can price itineraries from the agency's own feed.
  // Injected into the PROCESSED zips.
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

  // Stage 6: Generate MOTIS config with GTFS-RT feeds. Its dataset paths
  // ("gtfs/<feed_id>.zip") name the volume dir scripts/rebuild-motis.sh
  // fills from the PROCESSED zips — see generateMotisConfig.
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

  // Stage 7: (Re)create GTFS display views so Martin can serve route/stop tiles
  // from the freshly imported data (import/create-gtfs-display-views.sql).
  console.log('\n=== Creating GTFS Display Views ===')
  try {
    const viewsSql = readFileSync(
      join(import.meta.dir, 'create-gtfs-display-views.sql'),
      'utf8',
    )
    await db.execute(sql.raw(viewsSql))
    console.log('✓ transit_routes / transit_stops views ready')
    console.log('  Restart Martin to pick up view changes: docker compose restart martin')
  } catch (err) {
    console.error(`✗ Failed to create display views: ${err instanceof Error ? err.message : err}`)
  }

  // Stage 8: Hand off to MOTIS. Restarting the container does NOT re-ingest —
  // only `/motis import` does, and it must see the processed zips.
  console.log('\n=== Import Complete ===')
  console.log(`Processed ${feedFiles.length} feeds`)
  console.log(`Raw zips: ${outputDir}`)
  console.log(`Processed zips: ${processedDir}`)
  console.log(`MOTIS config: ${motisConfigPath}`)
  console.log('')
  console.log('Next step — load the processed feeds into MOTIS:')
  console.log('  scripts/rebuild-motis.sh')
  console.log('  (syncs processed zips + config into the MOTIS volume, runs')
  console.log('   /motis import, then restarts the container)')
  console.log('')

  process.exit(0)
}

// ── Feed transform (raw → processed) ────────────────────────────────

/**
 * Build the fully preprocessed copy of a feed under data/gtfs-processed.
 * This is the ONLY artifact downstream consumers (PostGIS parse, LOOM,
 * MOTIS) may read — the raw zip stays pristine (download + flex-sanitize
 * only), so re-running the transform is always reproducible.
 *
 * Order matters: the shape rewrite runs before anything reads the zip so
 * importShapes, the display pipeline, and MOTIS get identical geometry.
 */
async function transformFeed(rawPath: string, feed: GtfsFeedInfo): Promise<string> {
  const processedPath = join(processedDir, basename(rawPath))

  // a. Start from the sanitized raw zip
  copyFileSync(rawPath, processedPath)

  // b. OSM shape rewrite (shapesnap) — per-feed opt-in via config/shapesnap.json
  const rewritten = await applyShapeRewrite(feed.feedId, processedPath)
  if (rewritten) {
    console.log(`  ✓ Rewrote shapes in ${basename(processedPath)}`)
  }

  // c. Bake display overrides into routes.txt/stops.txt so MOTIS returns the
  //    same colours/names our display views show (config/transit-overrides.json)
  const baked = await bakeDisplayOverridesIntoZip(feed, processedPath)
  if (baked > 0) {
    console.log(`  ✓ Baked ${baked} display override(s) into ${basename(processedPath)}`)
  }

  return processedPath
}

/**
 * shapesnap OSM shape rewrite (docs/shapesnap.md).
 *
 * Map-matches every pattern of the feed's configured modes onto OSM ways and
 * replaces shapes.txt inside the processed zip IN PLACE (python -m
 * shapesnap.run via uv). It must run here — before the DB parse and before
 * MOTIS ingestion — so importShapes and display read the exact geometry MOTIS
 * routes on.
 *
 * Per-feed opt-in: config/shapesnap.json {enabled, modes}; default disabled.
 * The import NEVER hard-fails on shapesnap: a non-zero exit, a missing
 * summary, or a gate-summary anomaly (zero matched patterns) logs loudly and
 * restores the unrewritten zip.
 *
 * @returns true if shapes were rewritten, false when skipped or failed.
 */
async function applyShapeRewrite(feedId: string, zipPath: string): Promise<boolean> {
  const repoRoot = join(import.meta.dir, '..')
  const configPath = join(repoRoot, 'config', 'shapesnap.json')

  let feedCfg: { enabled?: boolean; modes?: string[] } | undefined
  try {
    const cfg = JSON.parse(readFileSync(configPath, 'utf8'))
    feedCfg = cfg.feeds?.[feedId]
  } catch (err) {
    console.error(`  ⚠ shapesnap: cannot read ${configPath} (${err}) — skipping rewrite`)
    return false
  }
  if (!feedCfg?.enabled) return false

  // Keep a pristine copy: any failure below continues with the unrewritten zip
  const backupPath = `${zipPath}.preshapesnap`
  copyFileSync(zipPath, backupPath)
  const bail = (why: string): false => {
    console.error(`  ✗✗✗ shapesnap(${feedId}) FAILED: ${why}`)
    console.error(`  ✗✗✗ continuing with the UNREWRITTEN zip — MOTIS/display will use the feed's own shapes`)
    copyFileSync(backupPath, zipPath)
    return false
  }

  try {
    const cmd = [
      'uv', 'run', '--with-requirements', 'shapesnap/requirements.txt',
      'python', '-m', 'shapesnap.run', '--feed', feedId, '--zip', zipPath,
    ]
    if (feedCfg.modes?.length) cmd.push('--modes', feedCfg.modes.join(','))
    console.log(`  shapesnap: ${cmd.join(' ')}`)

    const proc = Bun.spawn(cmd, { cwd: repoRoot, stdout: 'pipe', stderr: 'pipe' })
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited,
    ])
    if (stdout.trim()) console.log(stdout.trimEnd().split('\n').map(l => `    ${l}`).join('\n'))
    if (stderr.trim()) console.error(stderr.trimEnd().split('\n').map(l => `    ${l}`).join('\n'))
    if (exitCode !== 0) return bail(`exit code ${exitCode}`)

    // Gate-summary anomaly check — the CLI's last line is machine-readable:
    //   [shapesnap] SUMMARY {json}
    const marker = '[shapesnap] SUMMARY '
    const line = stdout.split('\n').reverse().find(l => l.includes(marker))
    if (!line) return bail('no SUMMARY line in output')
    let summary: { patterns?: number; matched?: number }
    try {
      summary = JSON.parse(line.slice(line.indexOf(marker) + marker.length))
    } catch {
      return bail('unparseable SUMMARY line')
    }
    if ((summary.patterns ?? 0) > 0 && (summary.matched ?? 0) === 0) {
      return bail(`gate anomaly: 0/${summary.patterns} patterns matched (all fallback/passthrough)`)
    }
    // matched === 0 with 0 patterns (e.g. no routes of the configured modes)
    // is a clean no-op: the CLI left the zip untouched
    return (summary.matched ?? 0) > 0
  } catch (err) {
    return bail(err instanceof Error ? err.message : String(err))
  } finally {
    rmSync(backupPath, { force: true })
  }
}

// ── Feed import ─────────────────────────────────────────────────────

async function importFeedFile(filepath: string, feedInfo: GtfsFeedInfo) {
  try {
    const buffer = await Bun.file(filepath).arrayBuffer()
    const zip = await JSZip.loadAsync(buffer)

    // Read required files from ZIP
    const stopsContent = await readZipEntry(zip, 'stops.txt')
    const routesContent = await readZipEntry(zip, 'routes.txt')
    const agencyContent = await readZipEntry(zip, 'agency.txt')

    if (!stopsContent) {
      console.log(`  ⚠ No stops.txt found, skipping`)
      return
    }

    // Clear existing data for this feed
    await clearFeed(feedInfo.feedId)

    // Parse and import stops
    const stops = parseStops(stopsContent, feedInfo.feedId)
    const stopsImported = await importStops(stops)
    console.log(`  ✓ Imported ${stopsImported} stops`)

    // Parse and import routes
    let routesImported = 0
    if (routesContent) {
      const agencyMap = agencyContent ? parseAgencies(agencyContent) : new Map()
      const routes = parseRoutes(routesContent, feedInfo.feedId, agencyMap)
      routesImported = await importRoutes(routes)
      console.log(`  ✓ Imported ${routesImported} routes`)
    }

    // Derive and import stop→route associations
    let stopRoutesImported = 0
    const tripsContent = await readZipEntry(zip, 'trips.txt')
    const stopTimesContent = await readZipEntry(zip, 'stop_times.txt')
    if (tripsContent && stopTimesContent) {
      // Parse the (large) files once and share the records across both
      // derivers, rather than re-parsing stop_times.txt per call.
      const tripRecords = parseGtfsRecords(tripsContent)
      const stopTimeRecords = parseGtfsRecords(stopTimesContent)

      // calendar + frequencies drive the representative-day service counts
      // that gate which routes a station actually shows (see deriveStopRoutes
      // / resolveServiceCalendar).
      const calendarContent = await readZipEntry(zip, 'calendar.txt')
      const calendarDatesContent = await readZipEntry(zip, 'calendar_dates.txt')
      const frequenciesContent = await readZipEntry(zip, 'frequencies.txt')
      const resolution = resolveServiceCalendar(
        calendarContent ?? undefined,
        calendarDatesContent ?? undefined,
        tripRecords,
      )
      console.log(
        `  service regime: ${resolution.regime}` +
          (resolution.regime === 'fail-open'
            ? ''
            : `; rep dates weekday=${resolution.repWeekday} sat=${resolution.repSaturday} sun=${resolution.repSunday}`),
      )
      const associations = deriveStopRoutes(
        tripRecords,
        stopTimeRecords,
        feedInfo.feedId,
        calendarContent ?? undefined,
        calendarDatesContent ?? undefined,
        frequenciesContent ?? undefined,
        resolution,
      )
      stopRoutesImported = await importStopRoutes(associations)
      console.log(`  ✓ Imported ${stopRoutesImported} stop-route associations`)

      // Trip patterns — the ordered station sequence each route runs, powering
      // "every line that serves this board→alight directly" alternate lookups.
      const stopParents = parseStopParents(stopsContent)
      const patterns = deriveTripPatterns(tripRecords, stopTimeRecords, stopParents, feedInfo.feedId)
      const patternsImported = await importTripPatterns(feedInfo.feedId, patterns)
      console.log(`  ✓ Imported ${patternsImported} trip patterns`)
    }

    // Agency transfers — station-complex membership + min connection times
    const transfersContent = await readZipEntry(zip, 'transfers.txt')
    if (transfersContent) {
      const transfers = parseTransfers(transfersContent, feedInfo.feedId)
      const transfersImported = await importTransfers(transfers)
      if (transfersImported > 0) {
        console.log(`  ✓ Imported ${transfersImported} agency transfers`)
      }
    }

    // Parse and import shapes (for route-snapped vehicle interpolation)
    const shapesContent = await readZipEntry(zip, 'shapes.txt')
    if (shapesContent) {
      const shapes = parseShapes(shapesContent)
      const shapesImported = await importShapes(shapes, feedInfo.feedId)
      console.log(`  ✓ Imported ${shapesImported} shapes`)

      // Link routes to their canonical shape_id and bikes_allowed
      if (tripsContent) {
        const routeShapes = deriveRouteShapes(tripsContent)
        await updateRouteShapes(routeShapes, feedInfo.feedId)
        console.log(`  ✓ Linked ${routeShapes.size} routes to shapes`)

        const bikesAllowed = deriveBikesAllowed(tripsContent)
        const bikeRoutes = [...bikesAllowed.values()].filter(v => v > 0).length
        if (bikeRoutes > 0) {
          await updateBikesAllowed(bikesAllowed, feedInfo.feedId)
          console.log(`  ✓ ${bikeRoutes} routes with bikes allowed`)
        }
      }
    }

    // Record feed in tracking table
    await recordFeed(feedInfo, stopsImported, routesImported)

    // Apply manual display overrides to the DB rows. The processed zip already
    // has them baked in (transformFeed), so this is usually a no-op re-patch —
    // kept as the idempotent pass that also covers out-of-band imports.
    const overridden = await applyDisplayOverrides(feedInfo)
    if (overridden > 0) {
      console.log(`  ✓ Applied ${overridden} display override(s)`)
    }
  } catch (err) {
    console.error(`  ✗ Import error: ${err instanceof Error ? err.message : err}`)
  }
}

async function readZipEntry(zip: JSZip, filename: string): Promise<string | null> {
  const entry = zip.file(filename)
  if (!entry) return null
  return await entry.async('string')
}

/**
 * Inject transfers.txt into an existing GTFS ZIP.
 * Replaces any existing transfers.txt.
 */
async function injectTransfersTxt(zipPath: string, transfersTxt: string): Promise<void> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  zip.file('transfers.txt', transfersTxt)

  const updatedBuffer = await zip.generateAsync({ type: 'nodebuffer' })
  writeFileSync(zipPath, updatedBuffer)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
