/**
 * Update Sidewalk Street Names: rename the sidewalks recent OSM changes touched.
 *
 *   bun run import/update-sidewalk-street-names.ts
 *
 * Works off the sidewalk_street_names rows of detail_dirty, which
 * replicate-extract.sh fills when SIDEWALK_STREET_NAMES_INCREMENTAL=1
 * (import/queue-sidewalk-street-names.sql): the box around each highway a diff
 * touched. Each box is rebuilt as Build Sidewalk Street Names would. A run
 * takes up to SIDEWALK_STREET_NAMES_MAX_BOXES entries; the rest stay queued.
 * An entry that fails is retried in later runs, and dropped after three. Steps
 * aside, leaving the queue, while Build Sidewalk Street Names runs.
 *
 * Called by update-osm.sh after each replication run, and runnable on its own.
 * Exits 1 when a box failed, which update-osm.sh reports as a warning.
 */
import postgres from 'postgres'
import type { Bbox } from '../src/config/regions'
import { envRaw } from '../src/config/env'
import { dbUrl, onnotice } from '../src/db'
import { LOCK, rebuildBox, tableMissing } from './sidewalk-street-name-cells'

const MAX_ATTEMPTS = 3
const LAYER = 'sidewalk_street_names'

const log = (message: string) => console.log(`[${new Date().toISOString().slice(0, 19).replace('T', ' ')}] ${message}`)

type Sql = postgres.Sql

/** One run over the queue; 1 when something failed and is left for the next. */
export async function updateNames(sql: Sql, maxBoxes: number): Promise<number> {
  const [{ gaveUp }] = await sql`
    WITH gone AS (DELETE FROM detail_dirty WHERE layer = ${LAYER} AND attempts >= ${MAX_ATTEMPTS} RETURNING 1)
    SELECT count(*)::int AS "gaveUp" FROM gone`
  if (gaveUp) console.warn(`WARNING: sidewalk street names: gave up on ${gaveUp} entries after ${MAX_ATTEMPTS} failed runs.`)
  const rows: Array<{ id: string; box: Bbox }> = await sql`
    SELECT id::text AS id, ARRAY[ST_XMin(box), ST_YMin(box), ST_XMax(box), ST_YMax(box)] AS box
    FROM detail_dirty WHERE layer = ${LAYER}
    ORDER BY attempts, queued_at, id
    LIMIT ${maxBoxes}`
  if (!rows.length) {
    log('Sidewalk street names: nothing queued.')
    return 0
  }
  log(`Sidewalk street names: ${rows.length} queued box(es).`)
  const started = Date.now()
  const failed: string[] = []
  const cleared: string[] = []
  let named = 0
  for (const { id, box } of rows) {
    try {
      named += (await rebuildBox(sql, box)).named
      cleared.push(id)
    } catch (err) {
      failed.push(id)
      console.error(`  box ${box.map(v => v.toFixed(4)).join(',')} FAILED, skipped: ${(err as Error).message}`)
    }
  }
  if (cleared.length) await sql`DELETE FROM detail_dirty WHERE id = ANY(${cleared}::bigint[])`
  if (failed.length) await sql`UPDATE detail_dirty SET attempts = attempts + 1 WHERE id = ANY(${failed}::bigint[])`
  const [{ remaining }] = await sql`SELECT count(*)::int AS remaining FROM detail_dirty WHERE layer = ${LAYER}`
  log(`Sidewalk street names: rebuilt ${cleared.length} box(es), ${named} ways named, in ${((Date.now() - started) / 1000).toFixed(1)} s; ${remaining} left.`)
  if (failed.length) {
    console.warn(`WARNING: sidewalk street names: ${failed.length} box(es) failed and go to the back of the queue.`)
    return 1
  }
  return 0
}

async function main(sql: Sql, maxBoxes: number): Promise<number> {
  const [{ queue }] = await sql`SELECT to_regclass('detail_dirty') IS NOT NULL AS queue`
  if (await tableMissing(sql) || !queue) {
    log('Sidewalk street names: sidewalk_street_names or its queue does not exist yet; nothing to do.')
    return 0
  }
  const lock = await sql.reserve()
  try {
    const [{ free }] = await lock`SELECT pg_try_advisory_lock(${LOCK[0]}, ${LOCK[1]}) AS free`
    if (!free) {
      log('Sidewalk street names: a build is running; leaving the queue for the next run.')
      return 0
    }
    try {
      return await updateNames(sql, maxBoxes)
    } finally {
      await lock`SELECT pg_advisory_unlock(${LOCK[0]}, ${LOCK[1]})`
    }
  } finally {
    lock.release()
  }
}

if (import.meta.main) {
  const raw = envRaw('SIDEWALK_STREET_NAMES_MAX_BOXES')?.trim() || '2000'
  const maxBoxes = Number(raw)
  if (!Number.isInteger(maxBoxes) || maxBoxes < 1) {
    console.error(`ERROR: SIDEWALK_STREET_NAMES_MAX_BOXES must be a whole number of at least 1, got "${raw}"`)
    process.exit(1)
  }
  const sql = postgres(dbUrl, { onnotice, max: 3 })
  const code = await main(sql, maxBoxes)
  await sql.end()
  process.exit(code)
}
