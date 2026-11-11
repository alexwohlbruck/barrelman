#!/usr/bin/env bun
/**
 * Decide which realtime URLs MOTIS should poll through barrelman's matcher.
 *
 * A URL is marked `normalize` when matching its updates to the timetable by
 * stop times reaches trains that MOTIS's exact trip_id lookup would drop.
 * A feed that already resolves is left alone. Imports run this for each feed;
 * run it by hand to re-measure the zips on disk without re-importing, then
 * regenerate the MOTIS config.
 *
 * Usage:
 *   bun run import/measure-rt-normalize.ts                 # every feed with realtime
 *   bun run import/measure-rt-normalize.ts --feed-id 5
 *   bun run import/measure-rt-normalize.ts --dry-run
 */
import { parseArgs } from 'util'
import { existsSync, readFileSync } from 'fs'
import { join } from 'path'
import { eq, sql } from 'drizzle-orm'
import { db } from '../src/db'
import { gtfsFeeds, type RtUrlEntry } from '../src/schema/gtfs'
import { fetchRealtime, indexFromZip } from '../src/services/rt-normalize.service'
import { normalizeFeed, type NormalizeStats } from '../src/lib/rt-normalize/normalize'

/** Whether the matcher reaches enough trains MOTIS would otherwise drop. */
export const worthNormalizing = (s: NormalizeStats) =>
  s.reordered > 0 || s.dated + s.matched + s.added >= Math.max(3, 0.1 * s.updates)

const isTripUpdates = (rt: RtUrlEntry) => rt.type === 'tripUpdates' || rt.type === undefined

/** The feed's realtime URLs with `normalize` decided afresh for each trip-update URL. */
export async function measureRtNormalize(
  buffer: ArrayBuffer | Uint8Array,
  rtUrls: RtUrlEntry[] | undefined,
  log: (message: string) => void = console.log,
): Promise<RtUrlEntry[] | undefined> {
  if (!rtUrls?.some(isTripUpdates)) return rtUrls
  let index
  try {
    index = await indexFromZip(buffer)
  } catch (err) {
    log(`  – realtime matching skipped: ${err instanceof Error ? err.message : err}`)
    return rtUrls.map(({ normalize: _, ...rt }) => rt)
  }

  const out: RtUrlEntry[] = []
  for (const rt of rtUrls) {
    const { normalize: _, ...base } = rt
    if (!isTripUpdates(rt)) { out.push(base); continue }
    try {
      const { stats } = normalizeFeed(index, await fetchRealtime(rt))
      // Once on, stay on while it repairs anything: one quiet sample shouldn't flap it off.
      const on = worthNormalizing(stats) || (!!rt.normalize && stats.dated + stats.matched + stats.added + stats.cancelled + stats.reordered > 0)
      log(
        `  ${on ? '✓' : '–'} realtime ${new URL(rt.url).pathname}: ${stats.updates} updates, ` +
          `${stats.exact} resolve as published, ${stats.dated} lack a start date, ${stats.reordered} out of block order, matching adds ${stats.matched} matched + ${stats.added} new, ` +
          `${stats.cancelled} cancelled${on ? ' → normalized' : ''}`,
      )
      out.push(on ? { ...base, normalize: true } : base)
    } catch (err) {
      // An unreachable feed keeps its previous decision rather than flapping.
      log(`  ⚠ realtime matching not measured: ${err instanceof Error ? err.message : err}`)
      out.push(rt)
    }
  }
  return out
}

async function main() {
  const { values: args } = parseArgs({
    options: {
      'feed-id': { type: 'string' },
      'dry-run': { type: 'boolean', default: false },
      'gtfs-dir': { type: 'string', default: process.env.GTFS_DATA_DIR || './data/gtfs' },
    },
  })

  const feeds = await db
    .select({ feedId: gtfsFeeds.feedId, rtUrls: gtfsFeeds.rtUrls })
    .from(gtfsFeeds)
    .where(args['feed-id']
      ? eq(gtfsFeeds.feedId, args['feed-id'])
      : sql`${gtfsFeeds.rtUrls} IS NOT NULL AND ${gtfsFeeds.rtUrls}::text NOT IN ('null', '[]')`)

  let marked = 0
  for (const feed of feeds) {
    const zip = join(args['gtfs-dir']!, `${feed.feedId}.zip`)
    if (!existsSync(zip) || !feed.rtUrls?.length) continue
    console.log(`Feed ${feed.feedId}`)
    try {
      const rtUrls = await measureRtNormalize(readFileSync(zip), feed.rtUrls)
      if (rtUrls?.some(rt => rt.normalize)) marked++
      if (!args['dry-run']) await db.update(gtfsFeeds).set({ rtUrls }).where(eq(gtfsFeeds.feedId, feed.feedId))
    } catch (err) {
      console.error(`  ✗ ${err instanceof Error ? err.message : err}`)
    }
  }
  console.log(`\n${marked} feed(s) normalized${args['dry-run'] ? ' (dry run, nothing written)' : ''}`)
  console.log('Regenerate the MOTIS config and restart MOTIS for it to take effect.')
  process.exit(0)
}

if (import.meta.main) main()
