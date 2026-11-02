/**
 * Update Tunnel Cuts: rebuild the cuts recent OSM changes touched.
 *
 *   bun run import/update-tunnel-cuts.ts
 *
 * Works off the tunnel_cuts rows of detail_dirty, which replicate-extract.sh
 * fills when TUNNEL_CUTS_INCREMENTAL=1 (import/queue-tunnel-cuts.sql): the
 * box each touched tunnel's or cut's portals can lie in. Each box is rebuilt
 * as Build Tunnel Cuts would, so a cut is replaced in the box that owns it and
 * a deleted or untagged tunnel loses its cuts. A run takes up to
 * TUNNEL_CUTS_MAX_BOXES entries; the rest stay queued. An entry that fails is
 * retried in later runs, and dropped after three. Steps aside, leaving the
 * queue, while Build Tunnel Cuts runs.
 *
 * Called by update-osm.sh after each replication run, and runnable on its own.
 * Exits 1 when a box failed, which update-osm.sh reports as a warning.
 */
import postgres from 'postgres'
import { envRaw } from '../src/config/env'
import { dbUrl, onnotice } from '../src/db'
import { Dem, DEM_TILES, demConfigured } from '../src/lib/bridge-decks/dem'
import { unitBox, type UnitBox } from '../src/lib/bridge-decks/grid'
import { LOCK, rebuildBoxes, tableMissing } from './tunnel-cut-cells'

const MAX_ATTEMPTS = 3

const log = (message: string) => console.log(`[${new Date().toISOString().slice(0, 19).replace('T', ' ')}] ${message}`)

type Sql = postgres.Sql

/** One run over the queue; 1 when something failed and is left for the next. */
export async function updateCuts(sql: Sql, dem: Dem, maxBoxes: number): Promise<number> {
  const [{ gaveUp }] = await sql`
    WITH gone AS (DELETE FROM detail_dirty WHERE layer = 'tunnel_cuts' AND attempts >= ${MAX_ATTEMPTS} RETURNING 1)
    SELECT count(*)::int AS "gaveUp" FROM gone`
  if (gaveUp) console.warn(`WARNING: tunnel cuts: gave up on ${gaveUp} entries after ${MAX_ATTEMPTS} failed runs.`)
  const rows: Array<{ id: string; box: [number, number, number, number] }> = await sql`
    SELECT id::text AS id, ARRAY[ST_XMin(box), ST_YMin(box), ST_XMax(box), ST_YMax(box)] AS box
    FROM detail_dirty WHERE layer = 'tunnel_cuts'
    ORDER BY attempts, queued_at, id
    LIMIT ${maxBoxes}`
  if (!rows.length) {
    log('Tunnel cuts: nothing queued.')
    return 0
  }
  log(`Tunnel cuts: ${rows.length} queued box(es).`)
  const started = Date.now()
  const failed: string[] = []
  const cleared: string[] = []
  let stored = 0
  for (const { id, box } of rows) {
    try {
      stored += await rebuildBoxes(sql, dem, [unitBox(box) as UnitBox])
      cleared.push(id)
    } catch (err) {
      failed.push(id)
      console.error(`  box ${box.map(v => v.toFixed(4)).join(',')} FAILED, skipped: ${(err as Error).message}`)
    }
  }
  if (cleared.length) await sql`DELETE FROM detail_dirty WHERE id = ANY(${cleared}::bigint[])`
  if (failed.length) await sql`UPDATE detail_dirty SET attempts = attempts + 1 WHERE id = ANY(${failed}::bigint[])`
  const [{ remaining }] = await sql`SELECT count(*)::int AS remaining FROM detail_dirty WHERE layer = 'tunnel_cuts'`
  log(`Tunnel cuts: rebuilt ${cleared.length} box(es), ${stored} cuts, in ${((Date.now() - started) / 1000).toFixed(1)} s; ${remaining} left.`)
  if (failed.length) {
    console.warn(`WARNING: tunnel cuts: ${failed.length} box(es) failed and go to the back of the queue.`)
    return 1
  }
  return 0
}

async function main(sql: Sql, maxBoxes: number): Promise<number> {
  const [{ queue }] = await sql`SELECT to_regclass('detail_dirty') IS NOT NULL AS queue`
  if (await tableMissing(sql) || !queue) {
    log('Tunnel cuts: tunnel_cuts or its queue does not exist yet; nothing to do.')
    return 0
  }
  if (!demConfigured(DEM_TILES)) {
    await sql`DELETE FROM detail_dirty WHERE layer = 'tunnel_cuts'`
    log('Tunnel cuts: BRIDGE_DECKS_DEM_TILES is off, so there is no terrain to cut into. Emptied the queue.')
    return 0
  }
  const lock = await sql.reserve()
  try {
    const [{ free }] = await lock`SELECT pg_try_advisory_lock(${LOCK[0]}, ${LOCK[1]}) AS free`
    if (!free) {
      log('Tunnel cuts: a build is running; leaving the queue for the next run.')
      return 0
    }
    try {
      return await updateCuts(sql, new Dem(), maxBoxes)
    } finally {
      await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
    }
  } finally {
    lock.release()
  }
}

if (import.meta.main) {
  const raw = envRaw('TUNNEL_CUTS_MAX_BOXES')?.trim() || '500'
  const maxBoxes = Number(raw)
  if (!Number.isInteger(maxBoxes) || maxBoxes < 1) {
    console.error(`ERROR: TUNNEL_CUTS_MAX_BOXES must be a whole number of at least 1, got "${raw}"`)
    process.exit(1)
  }
  const sql = postgres(dbUrl, { onnotice, max: 3 })
  const code = await main(sql, maxBoxes)
  await sql.end()
  process.exit(code)
}
