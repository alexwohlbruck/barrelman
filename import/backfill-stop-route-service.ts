/**
 * Backfill gtfs_stop_routes service counts for already-imported feeds.
 *
 * Re-derives the representative-day service counts per (stop, route)
 * (trips_weekday_day/_any, trips_weekend_day, trips_any + the weekday_trips
 * back-compat alias — see resolveServiceCalendar) from each feed's retained
 * GTFS ZIP and upserts them, so the station-label display filter
 * (import/create-transit-stations.sql) works without a full re-import.
 * Idempotent.
 *
 * Usage (inside the barrelman container, where processed ZIPs are at
 * /gtfs-data/gtfs and raw ZIPs at /gtfs-zips — both resolved via env):
 *   docker exec barrelman bun run import/backfill-stop-route-service.ts 5
 *   docker exec barrelman bun run import/backfill-stop-route-service.ts     # all feeds with rows
 */
import { existsSync } from 'fs'
import { join } from 'path'
import JSZip from 'jszip'
import { sql } from 'drizzle-orm'
import { db } from '../src/db'
import {
  parseGtfsRecords,
  deriveStopRoutes,
  importStopRoutes,
  resolveServiceCalendar,
} from '../src/services/gtfs.service'

// Prefer the fully preprocessed zips (what MOTIS ingests) so derived counts
// match routing; fall back per feed to the raw downloads for feeds that
// haven't been through the transform stage. GTFS_DIR pins a single directory.
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

/** Resolve a feed's zip, preferring processed dirs over raw ones. */
function zipPathForFeed(feedId: string): string | undefined {
  for (const dir of CANDIDATE_DIRS) {
    const p = join(dir, `${feedId}.zip`)
    if (existsSync(p)) return p
  }
  return undefined
}

async function readEntry(zip: JSZip, name: string): Promise<string | undefined> {
  const e = zip.file(name)
  return e ? await e.async('string') : undefined
}

async function feedsToBackfill(): Promise<string[]> {
  const argv = process.argv.slice(2).filter(a => !a.startsWith('-'))
  if (argv.length) return argv
  const rows = await db.execute(
    sql.raw(`SELECT DISTINCT feed_id FROM gtfs_stop_routes ORDER BY feed_id`),
  )
  return (rows as unknown as Array<{ feed_id: string }>).map(r => r.feed_id)
}

async function backfillFeed(feedId: string): Promise<void> {
  const zipPath = zipPathForFeed(feedId)
  if (!zipPath) {
    console.warn(`  ⚠ ${feedId}: no ZIP in ${CANDIDATE_DIRS.join(', ')}, skipping`)
    return
  }
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const trips = await readEntry(zip, 'trips.txt')
  const stopTimes = await readEntry(zip, 'stop_times.txt')
  if (!trips || !stopTimes) {
    console.warn(`  ⚠ ${feedId}: missing trips/stop_times, skipping`)
    return
  }
  const calendar = await readEntry(zip, 'calendar.txt')
  const calendarDates = await readEntry(zip, 'calendar_dates.txt')
  const frequencies = await readEntry(zip, 'frequencies.txt')

  const tripRecords = parseGtfsRecords(trips)
  const stopTimeRecords = parseGtfsRecords(stopTimes)

  const resolution = resolveServiceCalendar(calendar, calendarDates, tripRecords)
  console.log(
    `  ${feedId}: service regime ${resolution.regime}` +
      (resolution.regime === 'fail-open'
        ? ' (no calendar info — every trip eligible)'
        : `; horizon ${resolution.horizonStart}..${resolution.horizonEnd}; rep dates ` +
          `weekday=${resolution.repWeekday} sat=${resolution.repSaturday} sun=${resolution.repSunday}`),
  )

  const associations = deriveStopRoutes(
    tripRecords,
    stopTimeRecords,
    feedId,
    calendar,
    calendarDates,
    frequencies,
    resolution,
  )
  const n = await importStopRoutes(associations)
  const withService = associations.filter(a => a.tripsWeekdayDay >= 2).length
  console.log(
    `  ✓ ${feedId}: ${n} pairs upserted (${withService} with ≥2 rep-weekday-day trips)`,
  )
}

async function main() {
  const feeds = await feedsToBackfill()
  console.log(`Backfilling service counts for ${feeds.length} feed(s): ${feeds.join(', ')}`)
  for (const feedId of feeds) {
    try {
      await backfillFeed(feedId)
    } catch (err) {
      console.error(`  ✗ ${feedId}: ${(err as Error).message}`)
    }
  }
  console.log('Done.')
  process.exit(0)
}

main()
