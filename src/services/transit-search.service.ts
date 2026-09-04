/**
 * The transit layers of /search: GTFS routes (lines) and GTFS stops, queried
 * by name alongside the geo_places layers.
 *
 * Routes come only from GTFS — OSM `type=route` relations carry no ids a
 * client can open a schedule with, while a GTFS hit hands back the same
 * (feedId, routeId) pair every /transit endpoint is keyed by. Stops are the
 * opposite: OSM already covers most of them in geo_places with far richer
 * detail, so the stop layer only surfaces GTFS stops that OSM does *not*
 * cover — those with no portolan_stop_links row (see
 * portolan-links.service.ts) — and the merge in search.service drops the
 * stragglers that duplicate an OSM hit by name+distance
 * (lib/transit-search.ts).
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { routeTypeMode, transitCoreQuery } from '../lib/transit-search'

export interface TransitLayerParams {
  /** Sanitized query text (same form the geo_places layers receive). */
  query: string
  lat?: number
  lng?: number
  /** Typeahead: swap the trigram filters (measured 22-90ms — fine beside the
   *  submitted-search layers, an order of magnitude over typeahead's budget)
   *  for indexed prefix/substring matches (4-6ms). Typo tolerance returns on
   *  submit, exactly like the geo_places trigram layer. */
  autocomplete?: boolean
  /** Routes only: match nothing but an exact short name. This is the
   *  micro-query mode — a 1-character "7" or "q" names a line precisely but
   *  would match half the long names as a substring. */
  exactOnly?: boolean
  /** Autocomplete with a viewport: bound the stop scan to a box, like the
   *  FTS fast path. Routes are never bounded — the table is small and a
   *  line's centroid can sit far from the rider searching for it. */
  localBoxRadiusM?: number
  limit: number
}

const point = (lng: number, lat: number) =>
  sql`ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)`

/**
 * Shape shared with geo_places hits so consumers can treat the list
 * uniformly, plus `kind` to discriminate and `transit` with the ids that
 * open the line or stop on the /transit endpoints.
 */
function baseHit(row: any) {
  return {
    id: row.id,
    kind: row.kind,
    osm_type: null,
    osm_id: null,
    name: row.name,
    name_abbrev: null,
    tags: {},
    address: null,
    hours: null,
    phones: null,
    websites: null,
    geometry: row.geometry,
    text_rank: row.text_rank,
    distance_m: row.distance_m,
  }
}

/**
 * Search GTFS routes by name. An exact route_short_name match (after
 * stripping mode words — "7 train" → "7") is near-definitive, like the codes
 * layer; long names rank by trigram similarity. Proximity decays the rank
 * with the same half-life the geo_places layers use, so "route 40" finds the
 * local 40, not every 40 on the planet.
 */
export async function searchTransitRoutes(
  { query, lat, lng, autocomplete, exactOnly, limit }: TransitLayerParams,
): Promise<any[]> {
  const core = transitCoreQuery(query)
  const lower = query.toLowerCase()
  // Exact, or the query followed by a separator — agencies decorate short
  // names ("M60" is filed as "M60-SBS", "Q44" as "Q44-SBS"), and the
  // boundary keeps "7" from claiming route 70. The core is alphanumeric+
  // spaces by construction, so it is regex-safe as-is.
  const boundary = (q: string) => `^${q}([^a-z0-9]|$)`
  const shortNameMatch = core && core !== lower
    ? sql`(LOWER(r.route_short_name) IN (${lower}, ${core})
        OR LOWER(r.route_short_name) ~ ${boundary(core)}
        OR LOWER(r.route_short_name) ~ ${boundary(lower)})`
    : sql`(LOWER(r.route_short_name) = ${lower}
        OR LOWER(r.route_short_name) ~ ${boundary(lower)})`
  // The mode-word-stripped phrase reaches names the raw query overshoots:
  // "harlem line" finds Metro-North's "Harlem", "east river ferry" the NYC
  // Ferry "East River" — and the agency haystack finds carriers whose long
  // names are all destinations ("flixbus", "amtrak").
  const corePhrase = core && core !== lower ? core : null
  const rankExpr = sql`LEAST(1.0, GREATEST(
    CASE WHEN ${shortNameMatch} THEN 0.95 ELSE 0 END,
    similarity(COALESCE(r.route_long_name, ''), ${query}) * 1.1,
    ${corePhrase
      ? sql`CASE WHEN r.route_long_name ILIKE '%' || ${corePhrase} || '%' THEN 0.8 ELSE 0 END,`
      : sql``}
    CASE WHEN r.agency_name ILIKE '%' || ${lower} || '%' THEN 0.6 ELSE 0 END,
    similarity(COALESCE(r.route_short_name, '') || ' ' || COALESCE(r.route_long_name, ''), ${query})
  ))`

  const hasPoint = lat != null && lng != null
  const distanceSelect = hasPoint
    ? sql`ST_Distance(r.centroid::geography, ${point(lng!, lat!)}::geography) AS distance_m`
    : sql`NULL::float AS distance_m`
  // Same 50 km half-life decay as the geo_places layers (search.service.ts).
  const order = hasPoint
    ? sql`ORDER BY ((${rankExpr}) / (1.0 + (r.centroid <-> ${point(lng!, lat!)}) / 0.45)) DESC NULLS LAST`
    : sql`ORDER BY (${rankExpr}) DESC`

  const rows = await db.execute(sql`
    SELECT
      'transit-route/' || r.feed_id || ':' || r.route_id AS id,
      'transit_route' AS kind,
      COALESCE(NULLIF(r.route_long_name, ''), r.route_short_name) AS name,
      r.feed_id, f.onestop_id AS feed_onestop_id, r.route_id,
      r.route_short_name, r.route_long_name, r.route_type,
      r.route_color, r.route_text_color, r.agency_name,
      ST_AsGeoJSON(r.centroid)::jsonb AS geometry,
      ${rankExpr} AS text_rank,
      ${distanceSelect}
    FROM gtfs_routes r
    JOIN gtfs_feeds f ON f.feed_id = r.feed_id
    WHERE ${shortNameMatch}
       ${exactOnly ? sql`` : sql`OR r.route_long_name ILIKE '%' || ${query} || '%'`}
       ${!exactOnly && corePhrase
         ? sql`OR r.route_long_name ILIKE '%' || ${corePhrase} || '%'`
         : sql``}
       ${exactOnly || query.length < 4
         ? sql``
         : sql`OR r.agency_name ILIKE '%' || ${query} || '%'`}
       ${autocomplete || exactOnly
         ? sql``
         : sql`OR (COALESCE(r.route_short_name, '') || ' ' || COALESCE(r.route_long_name, '')) % ${query}`}
    ${order}
    LIMIT ${limit}
  `).catch(() => [] as any[])

  return (rows as any[]).map((row) => ({
    ...baseHit(row),
    categories: [`transit/route/${routeTypeMode(row.route_type)}`],
    geom_type: 'line',
    transit: {
      feedId: row.feed_id,
      feedOnestopId: row.feed_onestop_id,
      routeId: row.route_id,
      shortName: row.route_short_name,
      longName: row.route_long_name,
      routeType: row.route_type,
      mode: routeTypeMode(row.route_type),
      color: row.route_color,
      textColor: row.route_text_color,
      agency: row.agency_name,
    },
  }))
}

/**
 * Search GTFS stops that OSM does not cover. Only stations and independent
 * stops (platforms resolve to their parent), and only those with no
 * portolan_stop_links row — a linked stop *is* an OSM place, and geo_places
 * already returns it.
 */
export async function searchTransitStops(
  { query, lat, lng, autocomplete, localBoxRadiusM, limit }: TransitLayerParams,
): Promise<any[]> {
  const hasPoint = lat != null && lng != null
  const boxFilter = hasPoint && localBoxRadiusM
    ? sql`AND s.geom && ST_Expand(${point(lng!, lat!)}::geometry, ${localBoxRadiusM / 111320})`
    : sql``
  // Prefix via gtfs_stops_name_lower_idx for typeahead; trigram on submit.
  const nameFilter = autocomplete
    ? sql`LOWER(s.stop_name) LIKE LOWER(${query}) || '%'`
    : sql`(s.stop_name % ${query} OR s.stop_name ILIKE ${query} || '%')`
  const rankExpr = sql`GREATEST(
    similarity(s.stop_name, ${query}),
    CASE WHEN s.stop_name ILIKE ${query} || '%' THEN 0.7 ELSE 0 END
  )`
  const distanceSelect = hasPoint
    ? sql`ST_Distance(s.geom::geography, ${point(lng!, lat!)}::geography) AS distance_m`
    : sql`NULL::float AS distance_m`
  const order = hasPoint
    ? sql`ORDER BY ((${rankExpr}) / (1.0 + (s.geom <-> ${point(lng!, lat!)}) / 0.45)) DESC NULLS LAST`
    : sql`ORDER BY (${rankExpr}) DESC`

  const rows = await db.execute(sql`
    SELECT x.*, (
      -- The lowest route_type serving the stop (directly or via its child
      -- platforms) picks the icon: rail beats bus at an interchange. Runs on
      -- the LIMITed rows only.
      SELECT MIN(r.route_type)
      FROM gtfs_stop_routes sr
      JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
      WHERE sr.feed_id = x.feed_id
        AND (sr.stop_id = x.stop_id OR sr.stop_id IN (
          SELECT c.stop_id FROM gtfs_stops c
          WHERE c.feed_id = x.feed_id AND c.parent_station = x.stop_id
        ))
    ) AS min_route_type
    FROM (
      SELECT
        'transit-stop/' || s.feed_id || ':' || s.stop_id AS id,
        'transit_stop' AS kind,
        s.stop_name AS name,
        s.feed_id, f.onestop_id AS feed_onestop_id, s.stop_id, s.location_type,
        ST_AsGeoJSON(s.geom)::jsonb AS geometry,
        ${rankExpr} AS text_rank,
        ${distanceSelect}
      FROM gtfs_stops s
      JOIN gtfs_feeds f ON f.feed_id = s.feed_id
      WHERE s.stop_name IS NOT NULL
        AND (s.location_type = 1 OR ((s.location_type = 0 OR s.location_type IS NULL)
             AND NULLIF(s.parent_station, '') IS NULL))
        AND ${nameFilter}
        AND NOT EXISTS (
          SELECT 1 FROM portolan_stop_links l
          WHERE l.feed_onestop_id = f.onestop_id AND l.stop_id = s.stop_id
        )
        ${boxFilter}
      ${order}
      LIMIT ${limit}
    ) x
  `).catch(() => [] as any[])

  return (rows as any[]).map((row) => ({
    ...baseHit(row),
    categories: [
      row.min_route_type != null
        ? `transit/stop/${routeTypeMode(row.min_route_type)}`
        : 'transit/stop',
    ],
    geom_type: 'point',
    transit: {
      feedId: row.feed_id,
      feedOnestopId: row.feed_onestop_id,
      stopId: row.stop_id,
      locationType: row.location_type,
      mode: row.min_route_type != null ? routeTypeMode(row.min_route_type) : null,
    },
  }))
}
