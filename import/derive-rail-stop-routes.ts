/**
 * STREAMING stop->route derivation for feeds whose stop_times is too large to
 * parse in memory (e.g. CTA: 362MB, buses + trains — OOMs the in-memory
 * backfill). Reads the feed ZIP directly (processed dirs preferred, raw
 * data/gtfs fallback — same resolution as backfill-stop-route-service.ts),
 * streams stop_times.txt through a proper quote-aware CSV parser, filters to
 * the given route_types (default 1 = rail/metro), and upserts gtfs_stop_routes
 * with representative-day service counts (same semantics as deriveStopRoutes:
 * resolveServiceCalendar + frequencies.txt expansion). Only the small files
 * (routes/trips/calendar/frequencies) are loaded whole; stop_times never is.
 *
 * Usage (feed already imported):
 *   docker exec barrelman bun run import/derive-rail-stop-routes.ts 29 [routeTypes=1]
 *   DRY_RUN=1 bun run import/derive-rail-stop-routes.ts 29     # derive + log, no upsert
 */
import { existsSync } from 'fs'
import { join } from 'path'
import JSZip from 'jszip'
import { parse as parseCsvStream } from 'csv-parse'
import {
  parseGtfsRecords,
  parseFrequencies,
  resolveServiceCalendar,
  repServiceSets,
  createStopRouteAccumulator,
  gtfsTimeToSeconds,
  importStopRoutes,
} from '../src/services/gtfs.service'

const feedId = process.argv[2]
const routeTypes = new Set((process.argv[3] || '1').split(',').map(s => s.trim()))

if (!feedId) {
  console.error('Usage: derive-rail-stop-routes.ts <feedId> [routeTypes=1]')
  process.exit(1)
}

// Prefer the fully preprocessed zips (what MOTIS ingests) so derived counts
// match routing; fall back to the raw downloads. GTFS_DIR pins one directory.
const CANDIDATE_DIRS: string[] = process.env.GTFS_DIR
  ? [process.env.GTFS_DIR]
  : [
      process.env.GTFS_PROCESSED_DIR, // /gtfs-data/gtfs in the barrelman container
      './data/gtfs-processed',
      '/gtfs-data/gtfs',
      process.env.GTFS_DATA_DIR, // /gtfs-zips in the barrelman container
      './data/gtfs',
      '/data/gtfs',
    ].filter((d): d is string => !!d)

function zipPathForFeed(id: string): string | undefined {
  for (const dir of CANDIDATE_DIRS) {
    const p = join(dir, `${id}.zip`)
    if (existsSync(p)) return p
  }
  return undefined
}

async function readEntry(zip: JSZip, name: string): Promise<string> {
  const e = zip.file(name)
  return e ? await e.async('string') : ''
}

async function main() {
  const zipPath = zipPathForFeed(feedId)
  if (!zipPath) {
    console.warn(`⚠ ${feedId}: no ZIP found in ${CANDIDATE_DIRS.join(', ')}`)
    process.exit(1)
  }
  console.log(`Feed ${feedId}: ${zipPath}`)
  const zip = await JSZip.loadAsync(await Bun.file(zipPath).arrayBuffer())
  const stopTimesEntry = zip.file('stop_times.txt')
  if (!stopTimesEntry) {
    console.warn(`⚠ ${feedId}: no stop_times.txt in ${zipPath}`)
    process.exit(1)
  }

  // Small files: rail route ids, trip -> route/service, calendar, frequencies.
  const railRoutes = new Set(
    parseGtfsRecords(await readEntry(zip, 'routes.txt'))
      .filter(r => routeTypes.has(r.route_type))
      .map(r => r.route_id),
  )
  const trips = parseGtfsRecords(await readEntry(zip, 'trips.txt'))
  const tripToRoute = new Map<string, string>()
  const tripToService = new Map<string, string>()
  for (const t of trips) {
    if (!railRoutes.has(t.route_id)) continue
    tripToRoute.set(t.trip_id, t.route_id)
    if (t.service_id) tripToService.set(t.trip_id, t.service_id)
  }

  const res = resolveServiceCalendar(
    await readEntry(zip, 'calendar.txt'),
    await readEntry(zip, 'calendar_dates.txt'),
    trips,
  )
  console.log(
    `  service regime: ${res.regime}` +
      (res.regime === 'fail-open'
        ? ' (no calendar info — every trip eligible)'
        : `; horizon ${res.horizonStart}..${res.horizonEnd}; rep dates ` +
          `weekday=${res.repWeekday} sat=${res.repSaturday} sun=${res.repSunday}`),
  )

  const acc = createStopRouteAccumulator({
    tripToRoute,
    tripToService,
    frequencies: parseFrequencies(await readEntry(zip, 'frequencies.txt')),
    services: repServiceSets(res),
  })
  console.log(`${railRoutes.size} rail routes, ${tripToRoute.size} rail trips; streaming stop_times...`)

  // Stream stop_times.txt through a real CSV parser (quoted fields with
  // embedded commas — e.g. stop_headsign — would break a naive line.split(',')).
  const parser = stopTimesEntry.nodeStream().pipe(
    parseCsvStream({ columns: true, bom: true, trim: true, relax_column_count: true, skip_empty_lines: true }),
  )
  let n = 0
  for await (const st of parser as AsyncIterable<Record<string, string>>) {
    acc.add(st.trip_id, st.stop_id, gtfsTimeToSeconds(st.departure_time || st.arrival_time))
    if (++n % 2_000_000 === 0) console.log(`  …${n} stop_times rows`)
  }

  const associations = acc.finalize(feedId)
  const withSvc = associations.filter(a => a.tripsWeekdayDay >= 2).length
  if (process.env.DRY_RUN) {
    console.log(`DRY_RUN: ${associations.length} rail (stop,route) pairs (${withSvc} with >=2 rep-weekday-day trips); not upserting.`)
    process.exit(0)
  }
  const imported = await importStopRoutes(associations)
  console.log(`Done: ${imported} rail (stop,route) pairs upserted (${withSvc} with >=2 rep-weekday-day trips).`)
  process.exit(0)
}
main()
