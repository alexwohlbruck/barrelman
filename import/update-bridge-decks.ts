/**
 * Update Bridge Decks: rebuild the decks recent OSM changes touched.
 *
 *   bun run import/update-bridge-decks.ts
 *
 * Works off the bridge_decks rows of detail_dirty, which replicate-extract.sh
 * fills when BRIDGE_DECKS_INCREMENTAL=1 (import/queue-bridge-decks.sql): the
 * box of each bridge a diff touched and of each deck something under or
 * beside it changed. Each entry is rebuilt in 0.025° grid cells (see
 * src/lib/bridge-decks/queue.ts for which), nearby cells together, with the
 * same rebuild as Build Bridge Decks, so a deck is replaced in the one cell
 * that owns it and a deleted or untagged bridge loses its deck.
 *
 * Entries outside the cells Build Bridge Decks recorded are dropped, and a
 * cell is only written where it overlaps one, so an update never stores a deck
 * where nobody asked for decks. A deck whose id a cell outside the run still
 * holds is rebuilt together with that cell. A run takes whole entries
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
import { blockOf, cells, entriesOver, entryIndex, maxCellsFrom, pick, skipReason, strays, type Entry } from '../src/lib/bridge-decks/queue'
import { coveredParts, LOCK, missingTables, rebuildCells } from './bridge-deck-cells'

const MAX_ATTEMPTS = 3

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
    WITH gone AS (
      DELETE FROM detail_dirty d
      WHERE d.layer = 'bridge_decks'
        AND (d.attempts >= ${MAX_ATTEMPTS} OR NOT EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && d.box))
      RETURNING d.attempts >= ${MAX_ATTEMPTS} AS gave_up
    )
    SELECT count(*) FILTER (WHERE gave_up)::int AS "gaveUp", count(*) FILTER (WHERE NOT gave_up)::int AS unbuilt FROM gone`
  if (unbuilt) log(`Bridge decks: dropped ${unbuilt} entries where no decks are built.`)
  if (gaveUp) console.warn(`WARNING: bridge decks: gave up on ${gaveUp} entries after ${MAX_ATTEMPTS} failed runs.`)

  // Plenty to fill a run; the rest are read by the next one.
  const rows = await sql`
    SELECT d.id::text AS id, ARRAY[ST_XMin(d.box), ST_YMin(d.box), ST_XMax(d.box), ST_YMax(d.box)] AS box,
           ARRAY(SELECT ARRAY[b.anchor_x, b.anchor_y] FROM bridge_decks b
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
  const { picked, cells: planned } = pick(entries, maxCells)

  // A terrain source that is down would fail every cell after minutes of
  // retries each; find out once, and leave the queue as it is.
  const [w, s, e, n] = picked[0].box
  try {
    await dem.load([mercator((w + e) / 2, (s + n) / 2)])
  } catch (err) {
    console.error(`ERROR: bridge decks: terrain from ${DEM_TILES} is unavailable (${(err as Error).message}); the queue is left for the next run.`)
    return 1
  }

  log(`Bridge decks: ${picked.length} queued entries, ${planned.size} cell(s) to rebuild.`)
  const started = Date.now()
  const failed = new Set<string>()
  const index = entryIndex(picked)
  const cell = { partsOf: (key: string) => coveredParts(sql, cells.box(key)), keyOf: cells.at }
  let stored = 0
  let done = 0
  let pending = [...planned.keys()]
  while (pending.length) {
    const block = blockOf(pending[0])
    const batch = pending.filter(c => blockOf(c) === block)
    pending = pending.filter(c => blockOf(c) !== block)
    try {
      const result = await rebuildCells(sql, dem, batch, cell)
      stored += result.stored
      done += result.keys.length
      // Cells taken in for holding a clashing id were rebuilt here too, on
      // behalf of the same entries.
      const extra = new Set(result.keys.filter(k => !batch.includes(k)))
      if (extra.size) {
        const by = [...entriesOver(batch, planned)]
        for (const k of extra) if (!planned.has(k)) planned.set(k, by)
        pending = pending.filter(k => !extra.has(k))
        log(`  cells ${batch.join(' ')} rebuilt with ${[...extra].join(' ')}, which held some of their deck ids.`)
      }
      for (const [k, by] of strays(result.built, index, planned)) {
        planned.set(k, by)
        pending.push(k)
      }
      // A deck these cells held whose midpoint now lies elsewhere goes to
      // that cell, or it would be gone from both.
      for (const k of new Set(result.moved.map(cells.at)))
        if (!planned.has(k)) {
          planned.set(k, [...entriesOver(batch, planned)])
          pending.push(k)
        }
    } catch (err) {
      for (const c of batch) failed.add(c)
      console.error(`  cells ${batch.join(' ')} FAILED, skipped: ${(err as Error).message}`)
    }
  }

  const retry = entriesOver(failed, planned)
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
