/**
 * Update Bridge Decks: rebuild the decks recent OSM changes touched.
 *
 *   bun run import/update-bridge-decks.ts
 *
 * Works off the bridge_decks rows of detail_dirty, which replicate-extract.sh
 * fills when BRIDGE_DECKS_INCREMENTAL=1 (import/queue-bridge-decks.sql): the
 * box of each bridge a diff touched and of each deck something under or
 * beside it changed. Each entry is rebuilt in 0.02° grid cells (see
 * src/lib/bridge-decks/queue.ts for which), nearby cells together, with the
 * same rebuild as Build Bridge Decks, so a deck is replaced in the one cell
 * that owns it and a deleted or untagged bridge loses its deck.
 *
 * Entries outside the cells Build Bridge Decks covered are dropped, so an
 * update never builds where nobody asked for decks. A run takes whole entries
 * while their cells fit in BRIDGE_DECKS_MAX_CELLS; the rest stay queued. An
 * entry over a cell that fails is retried in later runs, and dropped after
 * three. Steps aside, leaving the queue, while Build Bridge Decks runs.
 *
 * Called by update-osm.sh after each replication run, and runnable on its own.
 * Exits 1 when a cell failed, which update-osm.sh reports as a warning.
 */
import postgres from 'postgres'
import { envNumber } from '../src/config/env'
import { dbUrl, onnotice } from '../src/db'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { mercator } from '../src/lib/bridge-decks/profile'
import { blockOf, cellBox, pick, skipReason, strays, type Entry } from '../src/lib/bridge-decks/queue'
import { LOCK, rebuildBoxes } from './bridge-deck-cells'

const MAX_CELLS = envNumber('BRIDGE_DECKS_MAX_CELLS', 1000)
const MAX_ATTEMPTS = 3
/** How near a stored deck an entry may lie and still count as built, for decks built before cells were recorded. */
const NEAR_BUILT = 0.05

const log = (message: string) => console.log(`[${new Date().toISOString().slice(0, 19).replace('T', ' ')}] ${message}`)

type Sql = postgres.Sql

async function main(sql: Sql): Promise<number> {
  const [state] = await sql`
    SELECT to_regclass('bridge_decks') IS NOT NULL AS decks, to_regclass('bridge_deck_cells') IS NOT NULL AS cells,
           to_regclass('detail_dirty') IS NOT NULL AS queue`
  const queued = state.queue
    ? (await sql`SELECT count(*)::int AS n FROM detail_dirty WHERE layer = 'bridge_decks'`)[0].n
    : 0
  const terrain = demConfigured(DEM_TILES)
  const skip = skipReason({ table: state.decks && state.cells, queue: queued > 0, terrain })
  if (skip) {
    if (!terrain && queued) await sql`DELETE FROM detail_dirty WHERE layer = 'bridge_decks'`
    log(`Bridge decks: ${skip}${!terrain && queued ? ` Emptied the queue (${queued} entries).` : ''}`)
    return 0
  }

  const lock = await sql.reserve()
  try {
    const [{ free }] = await lock`SELECT pg_try_advisory_lock(${LOCK[0]}, ${LOCK[1]}) AS free`
    if (!free) {
      log('Bridge decks: a build is running; leaving the queue for the next run.')
      return 0
    }
    try {
      return await updateDecks(sql, new Dem(), MAX_CELLS)
    } finally {
      await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
    }
  } finally {
    lock.release()
  }
}

/** One run over the queue; 1 when something failed and is left for the next. */
export async function updateDecks(sql: Sql, dem: Dem, maxCells: number): Promise<number> {
  const [{ gaveUp, unbuilt }] = await sql`
    WITH gone AS (
      DELETE FROM detail_dirty d
      WHERE d.layer = 'bridge_decks'
        AND (d.attempts >= ${MAX_ATTEMPTS}
             OR (NOT EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && d.box)
                 AND NOT EXISTS (SELECT 1 FROM bridge_decks b WHERE b.anchor && ST_Expand(d.box, ${NEAR_BUILT}))))
      RETURNING d.attempts >= ${MAX_ATTEMPTS} AS gave_up
    )
    SELECT count(*) FILTER (WHERE gave_up)::int AS "gaveUp", count(*) FILTER (WHERE NOT gave_up)::int AS unbuilt FROM gone`
  if (unbuilt) log(`Bridge decks: dropped ${unbuilt} entries where no decks are built.`)
  if (gaveUp) console.warn(`WARNING: bridge decks: gave up on ${gaveUp} entries after ${MAX_ATTEMPTS} failed runs.`)

  // Plenty to fill a run; the rest are read by the next one.
  const rows = await sql`
    SELECT d.id::int AS id, ARRAY[ST_XMin(d.box), ST_YMin(d.box), ST_XMax(d.box), ST_YMax(d.box)] AS box,
           ARRAY(SELECT ARRAY[ST_X(b.anchor), ST_Y(b.anchor)] FROM bridge_decks b
                 WHERE b.geom && d.box AND ST_Intersects(b.geom, d.box)) AS anchors
    FROM detail_dirty d
    WHERE d.layer = 'bridge_decks'
    ORDER BY d.attempts, d.queued_at, d.id
    LIMIT ${maxCells * 20}`
  const entries: Entry[] = rows.map(r => ({ id: r.id, box: r.box, anchors: r.anchors }))
  if (!entries.length) {
    log('Bridge decks: nothing queued.')
    return 0
  }
  const { picked, cells } = pick(entries, maxCells)

  // A terrain source that is down would fail every cell after minutes of
  // retries each; find out once, and leave the queue as it is.
  const [w, s, e, n] = picked[0].box
  try {
    await dem.load([mercator((w + e) / 2, (s + n) / 2)])
  } catch (err) {
    console.error(`ERROR: bridge decks: terrain from ${DEM_TILES} is unavailable (${(err as Error).message}); the queue is left for the next run.`)
    return 1
  }

  log(`Bridge decks: ${picked.length} queued entries, ${cells.size} cell(s) to rebuild.`)
  const started = Date.now()
  const failed = new Set<string>()
  let stored = 0
  let done = 0
  let pending = [...cells.keys()]
  while (pending.length) {
    const block = blockOf(pending[0])
    const batch = pending.filter(c => blockOf(c) === block)
    pending = pending.filter(c => blockOf(c) !== block)
    try {
      const result = await rebuildBoxes(sql, dem, batch.map(c => cellBox(c)))
      stored += result.stored
      done += batch.length
      for (const [extra, by] of strays(result.built, picked, cells)) {
        cells.set(extra, by)
        pending.push(extra)
      }
    } catch (err) {
      for (const c of batch) failed.add(c)
      console.error(`  cells ${batch.join(' ')} FAILED, skipped: ${(err as Error).message}`)
    }
  }

  const retry = picked.filter(entry => [...cells].some(([cell, by]) => failed.has(cell) && by.includes(entry.id))).map(e => e.id)
  const clear = picked.map(e => e.id).filter(id => !retry.includes(id))
  if (clear.length) await sql`DELETE FROM detail_dirty WHERE id = ANY(${clear})`
  if (retry.length) await sql`UPDATE detail_dirty SET attempts = attempts + 1 WHERE id = ANY(${retry})`
  const [{ remaining }] = await sql`SELECT count(*)::int AS remaining FROM detail_dirty WHERE layer = 'bridge_decks'`
  log(`Bridge decks: rebuilt ${done} cell(s), ${stored} decks, in ${((Date.now() - started) / 1000).toFixed(1)} s; cleared ${clear.length} entries, ${remaining} left.`)
  if (failed.size) {
    console.warn(`WARNING: bridge decks: ${failed.size} cell(s) failed; ${retry.length} entries over them go to the back of the queue.`)
    return 1
  }
  return 0
}

if (import.meta.main) {
  const sql = postgres(dbUrl, { onnotice, max: 3 })
  const code = await main(sql)
  await sql.end()
  process.exit(code)
}
