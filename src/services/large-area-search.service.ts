/**
 * Large named areas (parks, campuses, lakes, airports) for typeahead.
 *
 * Typeahead's density probe walks the rows nearest the viewport, and a dense
 * query is answered from that walk alone (see LOCAL_PROBE_ROWS in
 * search.service.ts). A large area's centroid sits far inside it, so it is
 * never among the nearest rows: "central park" typed in Midtown matched 47
 * buildings, bus stops and streets named for the park within the 2000 nearest
 * rows, and never the park, whose centroid is 2 km north.
 *
 * So the areas get a stage of their own, over a partial index of the ~0.15% of
 * rows that are named areas of 10 ha or more (~315K on the national import).
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { buildPartialIndexes } from '../lib/partial-indexes'

/** Rows the stage searches. Must match the WHERE of LARGE_AREA_INDEX exactly —
 *  the planner only uses a partial index when the query restates its
 *  predicate. 10 ha takes in a city park and leaves out a big-box store. */
export const LARGE_AREA_PREDICATE =
  `ts IS NOT NULL AND name IS NOT NULL AND geom_type = 'area' AND area_m2 >= 100000`

const LARGE_AREA_INDEX = 'geo_places_large_area_ts_idx'

let indexReady = false
let indexProbedAt = 0

/** Without its index the stage would fall onto the full ts index, so it stays
 *  off until the index exists. Looked for at most once a minute, since the
 *  background build can finish after startup. */
export async function largeAreaIndexReady(): Promise<boolean> {
  if (indexReady || Date.now() - indexProbedAt < 60_000) return indexReady
  indexProbedAt = Date.now()
  const rows = await db.execute(sql`
    SELECT bool_or(i.indisvalid) AS ready
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    WHERE c.relname = ${LARGE_AREA_INDEX}
  `).catch(() => null)
  const row = (rows as any[] | null)?.[0]
  if (row) indexReady = Boolean(row.ready)
  return indexReady
}

/** Build the stage's index in the background. */
export async function ensureLargeAreaIndex(): Promise<void> {
  try {
    await buildPartialIndexes([[LARGE_AREA_INDEX, `USING GIN (ts) WHERE ${LARGE_AREA_PREDICATE}`]])
    indexReady = true
  } catch (err) {
    // Costs typeahead its large areas, nothing else.
    console.error('[schema] could not build the large-area search index:', err)
  }
}

/** Test hook: the stage is a no-op until its index is confirmed. */
export function setLargeAreaIndexReady(ready: boolean): void {
  indexReady = ready
  indexProbedAt = ready ? 0 : Date.now()
}
