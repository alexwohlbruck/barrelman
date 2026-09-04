/**
 * Loads portolan's GTFS-stop → OSM-object indexes (the stops.json portolan
 * writes next to each tile pyramid, served at /tiles/portolan/:feed/stops.json)
 * into the portolan_stop_links table, so SQL can ask "does OSM cover this
 * stop?". The matching itself happens in portolan and only here — a name is
 * ambiguous and proximity worse (see the stops.json route in routes/tiles.ts).
 *
 * Runs at API startup and at the end of every portolan sync (which rewrites
 * the files). A missing tiles dir — portolan not set up — is a no-op, and the
 * table stays empty; the transit stop search layer then returns unmatched
 * GTFS stops only by the in-result name+distance dedupe.
 */

import { readdirSync, statSync } from 'fs'
import { join } from 'path'
import { eq } from 'drizzle-orm'
import { db } from '../db'
import { portolanStopLinks } from '../schema/gtfs'
import { resolvePortolanTilesDir } from '../config/portolan'

const OSM_REF_RE = /^(node|way|relation)\/\d+$/

const INSERT_CHUNK = 1000

/**
 * Re-read every feed's stops.json under the portolan tiles dir and replace
 * that feed's rows. Returns the number of links loaded.
 */
export async function syncPortolanStopLinks(dir = resolvePortolanTilesDir()): Promise<number> {
  let feeds: string[]
  try {
    feeds = readdirSync(dir).filter((entry) => {
      try { return statSync(join(dir, entry)).isDirectory() } catch { return false }
    })
  } catch {
    return 0 // portolan not set up
  }

  let total = 0
  for (const feed of feeds) {
    const file = Bun.file(join(dir, feed, 'stops.json'))
    if (!(await file.exists())) continue

    let index: Record<string, string>
    try {
      index = await file.json()
    } catch (err) {
      console.warn(`[portolan-links] unreadable stops.json for ${feed}: ${err}`)
      continue
    }

    // Keys are `<feed-onestop-id>:<stop_id>`; stop ids may themselves contain
    // colons, so split on the first one only.
    const byFeed = new Map<string, { feedOnestopId: string; stopId: string; osmRef: string }[]>()
    for (const [key, osmRef] of Object.entries(index)) {
      const sep = key.indexOf(':')
      if (sep <= 0 || typeof osmRef !== 'string' || !OSM_REF_RE.test(osmRef)) continue
      const feedOnestopId = key.slice(0, sep)
      const rows = byFeed.get(feedOnestopId) ?? []
      rows.push({ feedOnestopId, stopId: key.slice(sep + 1), osmRef })
      byFeed.set(feedOnestopId, rows)
    }

    for (const [feedOnestopId, rows] of byFeed) {
      await db.transaction(async (tx) => {
        await tx.delete(portolanStopLinks)
          .where(eq(portolanStopLinks.feedOnestopId, feedOnestopId))
        for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
          await tx.insert(portolanStopLinks)
            .values(rows.slice(i, i + INSERT_CHUNK))
            .onConflictDoNothing()
        }
      })
      total += rows.length
    }
  }

  if (total > 0) console.log(`[portolan-links] loaded ${total} stop→OSM links`)
  return total
}
