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
import { envRaw } from '../src/config/env'
import { dbUrl, onnotice } from '../src/db'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { mercator } from '../src/lib/bridge-decks/profile'
import { blockOf, cellAt, cellBox, entriesOver, maxCellsFrom, pick, skipReason, strays, type Entry } from '../src/lib/bridge-decks/queue'
import { DeckConflict, LOCK, missingTables, rebuildBoxes } from './bridge-deck-cells'

const MAX_ATTEMPTS = 3
/**
 * How near a stored deck an entry may lie and still count as built, used only
 * while no cells are recorded (decks built before bridge_deck_cells existed).
 * Once there are cells it is never used, or coverage would creep outward.
 */
const NEAR_BUILT = 0.05

const log = (message: string) => console.log(`[${new Date().toISOString().slice(0, 19).replace('T', ' ')}] ${message}`)

type Sql = postgres.Sql

async function main(sql: Sql, maxCells: number): Promise<number> {
  const missing = await missingTables(sql)
  const [{ queue }] = await sql`SELECT to_regclass('detail_dirty') IS NOT NULL AS queue`
  const queued = queue
    ? (await sql`SELECT count(*)::int AS n FROM detail_dirty WHERE layer = 'bridge_decks'`)[0].n
    : 0
  const terrain = demConfigured(DEM_TILES)
  const skip = skipReason({ missing, queue: queued > 0, terrain })
  if (skip) {
    const empty = !missing.length && !terrain && queued > 0
    if (empty) await sql`DELETE FROM detail_dirty WHERE layer = 'bridge_decks'`
    log(`Bridge decks: ${skip}${empty ? ` Emptied the queue (${queued} entries).` : ''}`)
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
      return await updateDecks(sql, new Dem(), maxCells)
    } finally {
      await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
    }
  } finally {
    lock.release()
  }
}

/** One run over the queue; 1 when something failed and is left for the next. */
export async function updateDecks(sql: Sql, dem: Dem, maxCells: number): Promise<number> {
  if (!Number.isInteger(maxCells) || maxCells < 1) throw new Error(`cells per run must be a whole number of at least 1, got ${maxCells}`)
  const [{ gaveUp, unbuilt }] = await sql`
    WITH recorded AS (SELECT EXISTS (SELECT 1 FROM bridge_deck_cells) AS cells),
    gone AS (
      DELETE FROM detail_dirty d
      WHERE d.layer = 'bridge_decks'
        AND (d.attempts >= ${MAX_ATTEMPTS}
             OR NOT CASE WHEN (SELECT cells FROM recorded)
                         THEN EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && d.box)
                         ELSE EXISTS (SELECT 1 FROM bridge_decks b WHERE b.anchor && ST_Expand(d.box, ${NEAR_BUILT}))
                    END)
      RETURNING d.attempts >= ${MAX_ATTEMPTS} AS gave_up
    )
    SELECT count(*) FILTER (WHERE gave_up)::int AS "gaveUp", count(*) FILTER (WHERE NOT gave_up)::int AS unbuilt FROM gone`
  if (unbuilt) log(`Bridge decks: dropped ${unbuilt} entries where no decks are built.`)
  if (gaveUp) console.warn(`WARNING: bridge decks: gave up on ${gaveUp} entries after ${MAX_ATTEMPTS} failed runs.`)

  // Plenty to fill a run; the rest are read by the next one.
  const rows = await sql`
    SELECT d.id::text AS id, ARRAY[ST_XMin(d.box), ST_YMin(d.box), ST_XMax(d.box), ST_YMax(d.box)] AS box,
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
  const retried = new Set<string>()
  let stored = 0
  let done = 0
  let pending = [...cells.keys()]
  while (pending.length) {
    const block = blockOf(pending[0])
    const batch = pending.filter(c => blockOf(c) === block)
    pending = pending.filter(c => blockOf(c) !== block)
    const plan = (cell: string, by: string[]) => {
      if (cells.has(cell)) return
      cells.set(cell, by)
      pending.push(cell)
    }
    try {
      const result = await rebuildBoxes(sql, dem, batch.map(c => cellBox(c)))
      stored += result.stored
      done += batch.length
      for (const [extra, by] of strays(result.built, picked, cells)) plan(extra, by)
    } catch (err) {
      // A deck that moved here from a cell not yet rebuilt still holds its id
      // there: rebuild that cell first, then this block again.
      if (err instanceof DeckConflict && !batch.some(c => retried.has(c))) {
        const by = [...entriesOver(batch, cells)]
        for (const anchor of err.anchors) plan(cellAt(anchor), by)
        for (const c of batch) retried.add(c)
        pending.push(...batch)
        log(`  cells ${batch.join(' ')}: ${err.message}; rebuilding where they are held first.`)
        continue
      }
      for (const c of batch) failed.add(c)
      console.error(`  cells ${batch.join(' ')} FAILED, skipped: ${(err as Error).message}`)
    }
  }

  const retry = entriesOver(failed, cells)
  const clear = picked.map(e => e.id).filter(id => !retry.has(id))
  if (clear.length) await sql`DELETE FROM detail_dirty WHERE id = ANY(${clear}::bigint[])`
  if (retry.size) await sql`UPDATE detail_dirty SET attempts = attempts + 1 WHERE id = ANY(${[...retry]}::bigint[])`
  const [{ remaining }] = await sql`SELECT count(*)::int AS remaining FROM detail_dirty WHERE layer = 'bridge_decks'`
  log(`Bridge decks: rebuilt ${done} cell(s), ${stored} decks, in ${((Date.now() - started) / 1000).toFixed(1)} s; cleared ${clear.length} entries, ${remaining} left.`)
  if (failed.size) {
    console.warn(`WARNING: bridge decks: ${failed.size} cell(s) failed; ${retry.size} entries over them go to the back of the queue.`)
    return 1
  }
  return 0
}

if (import.meta.main) {
  let maxCells: number
  try {
    maxCells = maxCellsFrom(envRaw('BRIDGE_DECKS_MAX_CELLS'), 1000)
  } catch (err) {
    console.error(`ERROR: ${(err as Error).message}`)
    process.exit(1)
  }
  const sql = postgres(dbUrl, { onnotice, max: 3 })
  const code = await main(sql, maxCells)
  await sql.end()
  process.exit(code)
}
