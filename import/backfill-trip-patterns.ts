/**
 * Backfill gtfs_trip_patterns for already-imported feeds, without a full
 * re-import. Reads each feed's zip from the GTFS dir, derives its distinct trip
 * patterns, and replaces that feed's pattern rows. Safe to re-run.
 *
 * Usage:
 *   bun run import/backfill-trip-patterns.ts                # all imported feeds
 *   bun run import/backfill-trip-patterns.ts 5 10 34        # specific feed ids
 *   bun run import/backfill-trip-patterns.ts --dir ./data/gtfs-processed 5
 */
import { join } from 'path'
import { existsSync } from 'fs'
import JSZip from 'jszip'
import { sql } from 'drizzle-orm'
import { db, ensureGtfsSchema } from '../src/db'
import {
  parseStopParents,
  deriveTripPatterns,
  importTripPatterns,
} from '../src/services/gtfs.service'

async function readZipEntry(zip: JSZip, filename: string): Promise<string | null> {
  const file = zip.file(filename)
  return file ? await file.async('text') : null
}

/** The importer's zip filename convention (import-gtfs.ts). */
function zipNameForFeed(feedId: string): string {
  return `${feedId.replace(/[^a-zA-Z0-9_-]/g, '_')}.zip`
}

async function main() {
  const argv = process.argv.slice(2)
  // Prefer the fully preprocessed zips (what MOTIS ingests); fall back to raw.
  let dir = existsSync('./data/gtfs-processed') ? './data/gtfs-processed' : './data/gtfs'
  const feedArgs: string[] = []
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--dir') dir = argv[++i]
    else feedArgs.push(argv[i])
  }

  await ensureGtfsSchema()

  let feedIds = feedArgs
  if (feedIds.length === 0) {
    const rows = (await db.execute(
      sql.raw('SELECT feed_id FROM gtfs_feeds ORDER BY feed_id'),
    )) as Array<{ feed_id: string }>
    feedIds = rows.map((r) => r.feed_id)
  }

  console.log(`Backfilling trip patterns for ${feedIds.length} feed(s) from ${dir}`)
  let totalPatterns = 0
  for (const feedId of feedIds) {
    const path = join(dir, zipNameForFeed(feedId))
    if (!existsSync(path)) {
      console.log(`  ⚠ ${feedId}: ${path} not found, skipping`)
      continue
    }
    try {
      const buffer = await Bun.file(path).arrayBuffer()
      const zip = await JSZip.loadAsync(buffer)
      const stops = await readZipEntry(zip, 'stops.txt')
      const trips = await readZipEntry(zip, 'trips.txt')
      const stopTimes = await readZipEntry(zip, 'stop_times.txt')
      if (!stops || !trips || !stopTimes) {
        console.log(`  ⚠ ${feedId}: missing stops/trips/stop_times, skipping`)
        continue
      }
      const parents = parseStopParents(stops)
      const patterns = deriveTripPatterns(trips, stopTimes, parents, feedId)
      const n = await importTripPatterns(feedId, patterns)
      totalPatterns += n
      console.log(`  ✓ ${feedId}: ${n} trip patterns`)
    } catch (err) {
      console.error(`  ✗ ${feedId}: ${err instanceof Error ? err.message : err}`)
    }
  }
  console.log(`Done — ${totalPatterns} patterns across ${feedIds.length} feed(s).`)
  process.exit(0)
}

main()
