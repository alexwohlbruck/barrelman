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
 *
 * The index holds each area's words and the 1° grid cell it lies in, and a
 * search asks for both, so GIN intersects them before reading a table row.
 * By words alone the cost is what they match nationally: "park:*" is in 41K
 * large areas, all of them fetched to keep the hundred near the viewport. By
 * place alone it is every large area near the viewport (2,666 around Midtown),
 * for each keystroke of a word that names none. Together it is the smaller.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { buildPartialIndexes } from '../lib/partial-indexes'

/** Rows the stage searches. Must match the WHERE of LARGE_AREA_INDEX exactly —
 *  the planner only uses a partial index when the query restates its
 *  predicate. 10 ha takes in a city park and leaves out a big-box store. */
export const LARGE_AREA_PREDICATE =
  `ts IS NOT NULL AND name IS NOT NULL AND geom_type = 'area' AND area_m2 >= 100000`

/** The 1° grid cell a centroid lies in, numbered column by column, so the
 *  cells of one column of a box are a single range. Must match the index
 *  expression exactly. */
export const LARGE_AREA_CELL =
  `((floor(ST_X(centroid))::int + 180) * 180 + (floor(ST_Y(centroid))::int + 90))`

const LARGE_AREA_INDEX = 'geo_places_large_area_cell_idx'

/** Most grid columns a search may span. The typeahead box spans two; a wider
 *  caller radius past this would OR together a GIN scan per column. */
const MAX_CELL_COLUMNS = 4

/**
 * The LARGE_AREA_CELL ranges covering a box of `halfWidthDeg` around a point,
 * one per grid column, or null when the box spans too many columns.
 */
export function cellRanges(lng: number, lat: number, halfWidthDeg: number): Array<[number, number]> | null {
  const x0 = Math.floor(Math.max(-180, lng - halfWidthDeg))
  const x1 = Math.floor(Math.min(179.999999, lng + halfWidthDeg))
  const y0 = Math.floor(Math.max(-90, lat - halfWidthDeg))
  const y1 = Math.floor(Math.min(89.999999, lat + halfWidthDeg))
  if (x1 - x0 + 1 > MAX_CELL_COLUMNS) return null
  const ranges: Array<[number, number]> = []
  for (let x = x0; x <= x1; x++) ranges.push([(x + 180) * 180 + y0 + 90, (x + 180) * 180 + y1 + 90])
  return ranges
}

let indexReady = false
let indexProbedAt = 0

/** Without its index the stage would read every area its words match
 *  nationally, so it stays off until the index exists. Looked for at most once a minute, since the
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
    // btree_gin lets one GIN index hold the cell beside the words.
    await db.execute(sql`CREATE EXTENSION IF NOT EXISTS btree_gin`)
    await buildPartialIndexes([[LARGE_AREA_INDEX, `USING GIN (ts, ${LARGE_AREA_CELL}) WHERE ${LARGE_AREA_PREDICATE}`]])
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
