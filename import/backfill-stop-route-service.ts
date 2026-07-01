/**
 * Backfill gtfs_stop_routes.weekday_trips for already-imported feeds.
 *
 * Re-derives the regular weekday-daytime trip count per (stop, route) from each
 * feed's retained GTFS ZIP and upserts it, so the station-label display filter
 * (routes that actually serve a station in regular service — see
 * import/create-transit-stations.sql) works without a full re-import.
 * Idempotent.
 *
 * Usage (inside the barrelman container, where the ZIPs are at /data/gtfs):
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
} from '../src/services/gtfs.service'

// ZIPs live at /data/gtfs in the container (./data mounted at /data); fall back
// to the repo-relative path when run on the host.
const GTFS_DIR =
  process.env.GTFS_DIR ||
  (existsSync('/data/gtfs') ? '/data/gtfs' : './data/gtfs')

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
  const zipPath = join(GTFS_DIR, `${feedId}.zip`)
  if (!existsSync(zipPath)) {
    console.log(`  ⚠ ${feedId}: no ZIP at ${zipPath}, skipping`)
    return
  }
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const trips = await readEntry(zip, 'trips.txt')
  const stopTimes = await readEntry(zip, 'stop_times.txt')
  if (!trips || !stopTimes) {
    console.log(`  ⚠ ${feedId}: missing trips/stop_times, skipping`)
    return
  }
  const calendar = await readEntry(zip, 'calendar.txt')
  const calendarDates = await readEntry(zip, 'calendar_dates.txt')

  const tripRecords = parseGtfsRecords(trips)
  const stopTimeRecords = parseGtfsRecords(stopTimes)
  const associations = deriveStopRoutes(
    tripRecords,
    stopTimeRecords,
    feedId,
    calendar,
    calendarDates,
  )
  const n = await importStopRoutes(associations)
  const withService = associations.filter(a => a.weekdayTrips >= 2).length
  console.log(
    `  ✓ ${feedId}: ${n} pairs upserted (${withService} with ≥2 weekday-daytime trips)`,
  )
}

async function main() {
  const feeds = await feedsToBackfill()
  console.log(`Backfilling weekday_trips for ${feeds.length} feed(s): ${feeds.join(', ')}`)
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
