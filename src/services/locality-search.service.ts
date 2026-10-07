/**
 * The locality layer of /search: countries, states, counties, cities, towns,
 * neighbourhoods and postal codes, found by name.
 *
 * These are in geo_places like everything else, but the general text layers
 * cannot surface them:
 *
 *   - A place name is made of common words. "New Jersey" matches ~350K rows
 *     through the main ts index, the ranked FTS query over them runs past the
 *     statement timeout, and the layer returns nothing at all.
 *   - When they do match, the 50 km proximity decay buries a city 800 km away
 *     under every cafe nearby that shares its name.
 *
 * So this layer searches only the ~0.1% of rows that are places (admin
 * boundaries, `place=*`, postal-code boundaries) through a partial index of
 * their own, ranks them by how important a place they are, and decays distance
 * on a scale that grows with that importance. search.service pins its hits to
 * the top of the list, the same way it pins an exact IATA code.
 */

import { db, maintenanceConnection } from '../db'
import { sql } from 'drizzle-orm'

/**
 * Rows the locality layer searches. Must match the WHERE of
 * geo_places_locality_ts_idx exactly — the planner only uses a partial index
 * when the query restates its predicate. `admin_level` is only ever set for
 * boundary=administrative (see osm2pgsql-flex.lua).
 */
const LOCALITY_PREDICATE = `ts IS NOT NULL AND name IS NOT NULL AND (admin_level IS NOT NULL OR tags->>'place' IS NOT NULL OR tags->>'boundary' = 'postal_code')`

/** Same, for postal-code boundaries looked up by code. Most carry no name
 *  (260 of 265 in Berlin), so they have no tsvector to match. */
const POSTAL_PREDICATE = `tags->>'boundary' = 'postal_code'`

/**
 * How important a place is (0-1) and how far, in km, its pull reaches before
 * proximity halves it. A country is worth surfacing from another continent; a
 * hamlet only from nearby. `place=*` is preferred over admin_level, whose
 * meaning varies by country (8 is a US city, a German Gemeinde, a French
 * commune).
 */
const IMPORTANCE = sql.raw(`CASE
  WHEN tags->>'place' IN ('continent', 'country') OR admin_level = 2 THEN 1.0
  WHEN tags->>'place' IN ('state', 'province', 'region') OR admin_level IN (3, 4) THEN 0.95
  WHEN tags->>'place' = 'city' THEN 0.9
  WHEN tags->>'place' IN ('county', 'municipality', 'borough', 'town', 'district')
    OR admin_level BETWEEN 5 AND 8 THEN 0.8
  WHEN tags->>'place' IN ('suburb', 'quarter', 'neighbourhood', 'village')
    OR admin_level BETWEEN 9 AND 11 OR tags->>'boundary' = 'postal_code' THEN 0.7
  WHEN tags->>'place' IN ('hamlet', 'island', 'archipelago') THEN 0.5
  ELSE 0.3
END`)

const REACH_KM = sql.raw(`CASE
  WHEN tags->>'place' IN ('continent', 'country') OR admin_level = 2 THEN 5000
  -- Not 5000 like a country: "new york" from Brooklyn is the city, not the
  -- state whose label node sits 313 km upstate.
  WHEN tags->>'place' IN ('state', 'province', 'region') OR admin_level IN (3, 4) THEN 2000
  WHEN tags->>'place' = 'city' THEN 1000
  WHEN tags->>'place' IN ('county', 'municipality', 'borough', 'town', 'district')
    OR admin_level BETWEEN 5 AND 8 THEN 300
  WHEN tags->>'place' IN ('suburb', 'quarter', 'neighbourhood', 'village')
    OR admin_level BETWEEN 9 AND 11 OR tags->>'boundary' = 'postal_code' THEN 50
  ELSE 20
END`)

/**
 * Lowest score a locality needs to be returned (and so pinned above every
 * other result). Score = name similarity × importance × distance decay.
 * Measured against: "brooklyn" → the borough (0.75); "charlotte" from NYC →
 * the city (0.49); "star" → Star Lake hamlet (0.25, left to the FTS layer so it
 * cannot sit above a Starbucks).
 */
const MIN_SCORE = 0.35

/** Shortest query the layer runs for: "ny" or "be" would pin whatever place
 *  happens to share two trigrams with it. */
export const LOCALITY_MIN_QUERY = 3

/** A query that could be a postal code: a digit, no more than ten characters,
 *  at most one space ("SW1A 1AA"). */
export function isPostalShaped(query: string): boolean {
  return /\d/.test(query) && query.length <= 10 && !/\s.*\s/.test(query)
}

export interface LocalityLayerParams {
  /** Sanitized query text (same form the geo_places layers receive). */
  query: string
  /** The tsquery text from buildTsQueryText, prefix-expanded for typeahead. */
  tsQueryText: string
  lat?: number
  lng?: number
  autocomplete?: boolean
  limit: number
}

let indexReady = false
let indexProbedAt = 0

/** The layer's index can land after startup: a first import finishes, or the
 *  background build completes. Look again at most once a minute. */
async function localityIndexReady(): Promise<boolean> {
  if (indexReady || Date.now() - indexProbedAt < 60_000) return indexReady
  indexProbedAt = Date.now()
  const rows = await db.execute(sql`
    SELECT count(*) FILTER (WHERE i.indisvalid) = 2 AS ready FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    WHERE c.relname IN ('geo_places_locality_ts_idx', 'geo_places_postal_code_idx')
  `).catch(() => [] as any[])
  indexReady = Boolean((rows as any[])[0]?.ready)
  return indexReady
}

/**
 * Locality hits, best first, in the geo_places result shape.
 *
 * Duplicates are collapsed, because OSM maps most places twice or more: a
 * `place=city` label node inside the city's boundary relation, or a borough
 * and the identically named locality nested inside it (Neukölln). The node is
 * dropped in favour of the area, which carries the outline, and the nested
 * area in favour of the larger one. The surviving area keeps the importance of
 * whatever it absorbed: NYC's relation is admin_level 5 (0.8), but its label
 * node is a `place=city` (0.9). Each hit lists what it absorbed in
 * `absorbed_ids`, so the merge can keep the other layers from bringing those
 * duplicates back.
 */
export async function searchLocalities({
  query, tsQueryText, lat, lng, autocomplete = false, limit,
}: LocalityLayerParams): Promise<any[]> {
  // Until the partial index exists, the query would fall back to the full ts
  // index and run into the very timeout this layer exists to avoid.
  if (query.length < LOCALITY_MIN_QUERY || !(await localityIndexReady())) return []

  const hasPoint = lat != null && lng != null
  const point = hasPoint ? sql`ST_SetSRID(ST_MakePoint(${lng!}, ${lat!}), 4326)` : null
  const distanceKm = point
    ? sql`ST_Distance(centroid::geography, ${point}::geography) / 1000`
    : sql`0`
  const distanceSelect = point
    ? sql`ST_Distance(centroid::geography, ${point}::geography)`
    : sql`NULL::float`

  const postalCode = query.toUpperCase()
  const postalMatch = autocomplete
    ? sql`upper(tags->>'postal_code') LIKE ${postalCode.replace(/[\\%_]/g, '\\$&') + '%'}`
    : sql`upper(tags->>'postal_code') = ${postalCode}`
  const postalBranch = isPostalShaped(query)
    ? sql`
      UNION ALL
      SELECT id, osm_type, osm_id, COALESCE(name, tags->>'postal_code') AS name, name_abbrev,
             categories, tags, address, hours, phones, websites, geom_type, centroid, geom, area_m2,
             length(${postalCode})::float / greatest(length(tags->>'postal_code'), 1) AS sim,
             ${IMPORTANCE} AS importance, ${REACH_KM} AS reach_km
      FROM geo_places
      WHERE ${sql.raw(POSTAL_PREDICATE)} AND ${postalMatch}`
    : sql``

  const rows = await db.execute(sql`
    WITH hits AS (
      SELECT DISTINCT ON (id) * FROM (
        SELECT id, osm_type, osm_id, name, name_abbrev, categories, tags,
               address, hours, phones, websites, geom_type, centroid, geom, area_m2,
               similarity(name, ${query}) AS sim,
               ${IMPORTANCE} AS importance, ${REACH_KM} AS reach_km
        FROM geo_places
        WHERE ${sql.raw(LOCALITY_PREDICATE)}
          AND ts @@ to_tsquery('simple', unaccent(${tsQueryText}))
        ${postalBranch}
      ) matched
      -- ts also matches parent_context, so "new york" reaches every
      -- neighbourhood *in* New York. The name has to be what matched.
      WHERE sim >= 0.3
      ORDER BY id, sim DESC
    ),
    merged AS (
      SELECT h.*, absorbed.ids AS absorbed_ids,
        GREATEST(h.importance, absorbed.importance) AS rank_importance
      FROM hits h
      LEFT JOIN LATERAL (
        SELECT array_agg(p.id) AS ids, max(p.importance) AS importance FROM hits p
        WHERE h.geom_type = 'area' AND p.id <> h.id AND lower(p.name) = lower(h.name)
          AND (p.geom_type <> 'area' OR h.area_m2 > p.area_m2)
          AND ST_Intersects(h.geom, p.centroid)
      ) absorbed ON true
      WHERE NOT EXISTS (
        SELECT 1 FROM hits o
        WHERE o.id <> h.id AND lower(o.name) = lower(h.name)
          AND o.geom_type = 'area'
          AND (h.geom_type <> 'area' OR o.area_m2 > h.area_m2)
          AND ST_Intersects(o.geom, h.centroid)
      )
    )
    SELECT * FROM (
      SELECT id, osm_type, osm_id, name, name_abbrev, categories, tags,
             address, hours, phones, websites, geom_type,
             ST_AsGeoJSON(centroid)::jsonb AS geometry,
             sim * rank_importance / (1 + ${distanceKm} / reach_km) AS text_rank,
             ${distanceSelect} AS distance_m,
             absorbed_ids
      FROM merged
    ) scored
    WHERE text_rank >= ${MIN_SCORE}
    ORDER BY text_rank DESC
    LIMIT ${limit}
  `).catch(() => [] as any[])
  return Array.from(rows as any[])
}

/**
 * Build the two small partial indexes the layer reads, in the background.
 *
 * CONCURRENTLY, because each build still scans the whole table (minutes on a
 * national import) and a plain build would hold off replication writes for
 * that long. That rules out the startup DDL batch — CONCURRENTLY cannot run in
 * a transaction — and leaves a failed build behind as an INVALID index that
 * IF NOT EXISTS would then skip forever, so an invalid one is dropped and
 * rebuilt.
 */
export async function ensureLocalityIndexes(): Promise<void> {
  const client = maintenanceConnection()
  try {
    const indexes = [
      ['geo_places_locality_ts_idx', `USING GIN (ts) WHERE ${LOCALITY_PREDICATE}`],
      ['geo_places_postal_code_idx', `(upper(tags->>'postal_code') text_pattern_ops) WHERE ${POSTAL_PREDICATE}`],
    ]
    for (const [name, definition] of indexes) {
      const [existing] = await client<{ valid: boolean }[]>`
        SELECT i.indisvalid AS valid FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = ${name}
      `
      if (existing?.valid) continue
      if (existing) await client.unsafe(`DROP INDEX CONCURRENTLY IF EXISTS ${name}`)
      console.log(`[schema] building ${name}`)
      await client.unsafe(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${name} ON geo_places ${definition}`)
    }
    indexReady = true
  } catch (err) {
    // Costs the locality layer, nothing else — search still runs without it.
    // Thrown from an un-awaited task it would take the process down instead.
    console.error('[schema] could not build the locality search indexes:', err)
  } finally {
    await client.end({ timeout: 5 })
  }
}

/** Test hook: the layer is a no-op until its index is confirmed. */
export function setLocalityIndexReady(ready: boolean): void {
  indexReady = ready
  indexProbedAt = ready ? 0 : Date.now()
}
