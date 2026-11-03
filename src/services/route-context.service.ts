/**
 * What a route passes that voice guidance can name and the routing graph
 * doesn't carry: the road each crosswalk crosses, the roads and railways that
 * pass over it, the states and parks it runs through, and on foot or by bike
 * what each edge's ways and nodes are tagged (edge-tags.ts).
 *
 * Read straight from geo_places through partial indexes
 * (import/create-route-context-indexes.sql), so replication keeps it current;
 * states from state_parts (import/build-state-parts.sql), rebuilt with it.
 * Every query runs under a budget and answers nothing rather than late.
 */
import { sql, type SQL } from 'drizzle-orm'
import { executeWithin } from '../lib/bounded-query'
import { crossingSpans, diagonalCrossings, nameCrossings, type CrossedRoad, type CrossingName, type CrossingSpan } from '../lib/route-context/crossing-names'
import {
  crossingStretches,
  nodeMarks,
  routeWays,
  wayStretches,
  type EdgeTagMark,
  type EdgeTagStretch,
  type Tags,
  type WayRecord,
} from '../lib/route-context/edge-tags'
import { overpassesOver, structureCrossings, wayLevel, type LevelAt, type Overpass, type Structure } from '../lib/route-context/overpasses'
import { areaStretches, type AreaHit, type AreaStretch } from '../lib/route-context/area-stretches'
import { bearing, routePath, routePieces, toleranceDegrees, type LonLat, type RoutePath, type RoutePiece } from '../lib/route-hazards/route-path'
import { piecesCte } from './route-hazards.service'

const BUDGET_MS = 600
/** Longer routes get no overpasses, and states from coarser pieces. */
const MAX_OVERPASS_METERS = 300_000
const OVERPASS_PIECE_METERS = 150
const STATE_PIECE_METERS = 2_000
const MAX_STATE_PIECES = 250
const PARK_PIECE_METERS = 300
const MAX_PARK_METERS = 50_000
/** Shorter visits are a path clipping a park's corner; shorter gaps a path stepping out and back. */
const PARK_MIN_METERS = 20
const PARK_JOIN_METERS = 15
const EDGE_TAG_PROFILES = new Set(['foot', 'bike'])
const MAX_EDGE_TAG_METERS = 100_000
const MAX_EDGE_WAYS = 5_000
const NODE_PIECE_METERS = 150
/** GraphHopper's simplified line stays within a metre of the nodes it runs through. */
const NODE_METERS = 1.5

/** Must match the predicate of geo_places_route_overpass_idx. */
export const OVERPASS_PREDICATE = `geom_type = 'line'
    AND (tags ? 'highway' OR tags ? 'railway')
    AND (tags->>'bridge' <> 'no' OR tags->>'layer' IN ('1', '2', '3', '4', '5'))`

/** Must match the predicate of geo_places_route_park_idx. */
export const PARK_PREDICATE = `geom_type = 'area' AND tags->>'leisure' = 'park'`

/** Named ways a crosswalk can cross: roads, not the paths beside them. Must match geo_places_route_named_road_idx. */
export const NAMED_ROAD_PREDICATE = `geom_type = 'line' AND name IS NOT NULL AND tags ? 'highway'
    AND tags->>'highway' NOT IN ('footway', 'path', 'cycleway', 'steps', 'pedestrian', 'corridor', 'bridleway', 'platform', 'proposed', 'construction', 'elevator')`

/** Nodes that start or mark an edge: station entrances, elevators, crossing islands and push buttons. Must match geo_places_route_edge_node_idx. */
export const EDGE_NODE_PREDICATE = `geom_type = 'point'
    AND (tags->>'railway' IN ('subway_entrance', 'train_station_entrance')
         OR tags->>'public_transport' = 'entrance'
         OR tags->>'highway' = 'elevator'
         OR tags->>'crossing' = 'traffic_island'
         OR tags->>'crossing:island' = 'yes'
         OR tags->>'button_operated' = 'yes')`

/** Way tags edge-tags.ts reads, besides every cycleway:* key. */
const EDGE_WAY_KEYS = ['highway', 'footway', 'cycleway', 'path', 'crossing', 'crossing:island', 'crossing:scramble', 'button_operated',
  'tunnel', 'covered', 'indoor', 'level', 'conveying', 'oneway', 'junction']
const EDGE_NODE_KEYS = ['railway', 'public_transport', 'highway', 'crossing', 'crossing:island', 'button_operated', 'ref', 'local_ref']

export interface EdgeNodeRow {
  tags: Tags
  coordinate: LonLat
}

export interface RouteContextSource {
  crossedRoads(spans: CrossingSpan[]): Promise<CrossedRoad[]>
  structures(pieces: RoutePiece[]): Promise<Structure[]>
  /** OSM layer of each way, by id. */
  wayLevels(ids: number[]): Promise<Map<number, number>>
  states(pieces: RoutePiece[]): Promise<AreaHit[]>
  parks(pieces: RoutePiece[]): Promise<AreaHit[]>
  /** Way tags for edge-tags.ts, by id, with the line of each way that has a bike lane. */
  edgeWays(ids: number[]): Promise<Map<number, WayRecord>>
  edgeNodes(pieces: RoutePiece[], tolerance: number): Promise<EdgeNodeRow[]>
}

const multiLine = (lines: LonLat[][]) => JSON.stringify({ type: 'MultiLineString', coordinates: lines })
const bigints = (ids: number[]) => `{${ids.join(',')}}`
const texts = (values: string[]) => `{${values.map(v => `"${v}"`).join(',')}}`

/** The lines of a piece inside an area, unless the area covers it whole. */
const PARTS = sql`CASE WHEN k.covers THEN NULL
  ELSE ST_AsGeoJSON(ST_Multi(ST_CollectionExtract(ST_Intersection(a.geom, p.line), 2)))::jsonb -> 'coordinates' END`

type AreaRow = { id: string; name: string | null; code: string | null; piece: number; covers: boolean; parts: LonLat[][] | null }

const areaHits = (rows: AreaRow[]): AreaHit[] => rows.map(r => ({
  id: r.id,
  ...(r.name && { name: r.name }),
  ...(r.code && { code: r.code }),
  piece: r.piece - 1,
  covers: r.covers,
  ...(r.parts && { parts: r.parts }),
}))

/** Runs a read query under a budget in ms, answering no rows rather than late. */
export type Execute = (query: SQL, ms: number) => Promise<any[]>

export const routeContextSource = (execute: Execute): RouteContextSource => ({
  async crossedRoads(spans) {
    const rows: { span: number; name: string; lon: number; lat: number; run: LonLat[] | null }[] = await execute(sql`
      WITH x AS (
        SELECT d.path[1] AS span, d.geom AS line
        FROM ST_Dump(ST_SetSRID(ST_GeomFromGeoJSON(${multiLine(spans.map(s => s.line))}), 4326)) AS d
        LIMIT ${spans.length}
      )
      SELECT x.span, g.name, ST_X(i.pt) AS lon, ST_Y(i.pt) AS lat,
             -- About 5 m of the road either side of the crossing, for its bearing there.
             CASE WHEN ST_GeometryType(g.geom) = 'ST_LineString' THEN
               ST_AsGeoJSON(ST_MakeLine(
                 ST_LineInterpolatePoint(g.geom, greatest(0, f.at - f.reach)),
                 ST_LineInterpolatePoint(g.geom, least(1, f.at + f.reach))))::jsonb -> 'coordinates'
             END AS run
      FROM x
      JOIN geo_places g ON ${sql.raw(NAMED_ROAD_PREDICATE)} AND ST_Intersects(g.geom, x.line)
      CROSS JOIN LATERAL (SELECT ST_ClosestPoint(g.geom, ST_LineInterpolatePoint(x.line, 0.5)) AS pt) i
      CROSS JOIN LATERAL (
        SELECT CASE WHEN ST_GeometryType(g.geom) = 'ST_LineString' THEN ST_LineLocatePoint(g.geom, i.pt) END AS at,
               0.00005 / greatest(ST_Length(g.geom), 0.00005) AS reach
      ) f
    `, BUDGET_MS)
    return rows.map(r => {
      const run = r.run && r.run.length === 2 && (r.run[0][0] !== r.run[1][0] || r.run[0][1] !== r.run[1][1]) ? r.run : null
      return { span: r.span - 1, name: r.name, at: [r.lon, r.lat], ...(run && { bearing: bearing(run[0], run[1]) }) }
    })
  },

  async structures(pieces) {
    const rows: { osm_id: string | number; first: number; last: number; tags: Record<string, string>; coordinates: LonLat[] }[] = await execute(sql`
      ${piecesCte(pieces)},
      -- Not GROUP BY: Postgres sizes its hash table from the index's estimate of millions of matches, ~100 MB a request.
      matched AS (
        SELECT DISTINCT ON (g.osm_type, g.osm_id) g.osm_type, g.osm_id, min(p.piece) OVER w AS first, max(p.piece) OVER w AS last
        FROM pieces p
        JOIN geo_places g ON ${sql.raw(OVERPASS_PREDICATE)} AND ST_Intersects(g.geom, p.line)
        WINDOW w AS (PARTITION BY g.osm_type, g.osm_id)
      )
      SELECT m.osm_id, m.first, m.last,
             jsonb_strip_nulls(jsonb_build_object(
               'highway', g.tags->>'highway', 'railway', g.tags->>'railway', 'layer', g.tags->>'layer',
               'bridge', g.tags->>'bridge', 'tunnel', g.tags->>'tunnel', 'name', g.name)) AS tags,
             ST_AsGeoJSON(g.geom)::jsonb -> 'coordinates' AS coordinates
      FROM matched m
      JOIN geo_places g ON g.osm_type = m.osm_type AND g.osm_id = m.osm_id AND g.geom_type = 'line'
    `, BUDGET_MS)
    return rows.map(r => ({
      osmId: Number(r.osm_id),
      firstSegment: pieces[r.first - 1].firstSegment,
      lastSegment: pieces[r.last - 1].lastSegment,
      tags: r.tags,
      coordinates: r.coordinates,
    }))
  },

  async wayLevels(ids) {
    if (!ids.length) return new Map()
    const rows: { osm_id: string | number; layer?: string; bridge?: string; tunnel?: string }[] = await execute(sql`
      SELECT osm_id, tags->>'layer' AS layer, tags->>'bridge' AS bridge, tags->>'tunnel' AS tunnel
      FROM geo_places
      WHERE osm_type = 'W' AND osm_id = ANY(${bigints(ids)}::bigint[]) AND geom_type = 'line'
    `, BUDGET_MS)
    return new Map(rows.map(r => [Number(r.osm_id), wayLevel(r)]))
  },

  async states(pieces) {
    return areaHits(await execute(sql`
      ${piecesCte(pieces)}
      SELECT a.id, a.name, a.code, p.piece, k.covers, ${PARTS} AS parts
      FROM pieces p
      JOIN state_parts a ON a.geom && p.line AND ST_Intersects(a.geom, p.line)
      CROSS JOIN LATERAL (SELECT ST_Covers(a.geom, p.line) AS covers) k
    `, BUDGET_MS))
  },

  async parks(pieces) {
    return areaHits(await execute(sql`
      ${piecesCte(pieces)},
      -- Each park read once, through the partial index, then tested against the pieces it boxes.
      areas AS MATERIALIZED (
        SELECT DISTINCT ON (g.id) g.id, g.name, NULL::text AS code, g.geom
        FROM pieces p JOIN geo_places g ON ${sql.raw(PARK_PREDICATE)} AND g.geom && p.line
      )
      SELECT a.id, a.name, a.code, p.piece, k.covers, ${PARTS} AS parts
      FROM areas a
      JOIN pieces p ON a.geom && p.line AND ST_Intersects(a.geom, p.line)
      CROSS JOIN LATERAL (SELECT ST_Covers(a.geom, p.line) AS covers) k
    `, BUDGET_MS))
  },

  async edgeWays(ids) {
    const rows: { osm_id: string | number; tags: Tags | null; coordinates: LonLat[] | null }[] = await execute(sql`
      SELECT g.osm_id, t.tags,
             CASE WHEN g.geom_type = 'line' AND t.lanes THEN ST_AsGeoJSON(g.geom)::jsonb -> 'coordinates' END AS coordinates
      FROM geo_places g
      CROSS JOIN LATERAL (
        SELECT jsonb_object_agg(key, value) AS tags, bool_or(key LIKE 'cycleway%') AS lanes
        FROM jsonb_each_text(g.tags)
        WHERE key = ANY(${texts(EDGE_WAY_KEYS)}::text[]) OR key LIKE 'cycleway:%'
      ) t
      WHERE g.osm_type = 'W' AND g.osm_id = ANY(${bigints(ids)}::bigint[]) AND t.tags IS NOT NULL
    `, BUDGET_MS)
    return new Map(rows.map(r => [Number(r.osm_id), { tags: r.tags ?? {}, ...(r.coordinates && { coordinates: r.coordinates }) }]))
  },

  async edgeNodes(pieces, tolerance) {
    const rows: { tags: Tags; lon: number; lat: number }[] = await execute(sql`
      ${piecesCte(pieces)}
      SELECT DISTINCT ON (g.osm_id) t.tags || jsonb_strip_nulls(jsonb_build_object('name', g.name)) AS tags, ST_X(g.geom) AS lon, ST_Y(g.geom) AS lat
      FROM pieces p
      JOIN geo_places g ON ${sql.raw(EDGE_NODE_PREDICATE)} AND ST_DWithin(g.geom, p.line, ${tolerance})
      CROSS JOIN LATERAL (
        SELECT jsonb_object_agg(key, value) AS tags FROM jsonb_each_text(g.tags) WHERE key = ANY(${texts(EDGE_NODE_KEYS)}::text[])
      ) t
    `, BUDGET_MS)
    return rows.map(r => ({ tags: r.tags, coordinate: [r.lon, r.lat] }))
  },
})

export const dbRouteContextSource = routeContextSource(executeWithin)

export interface RouteContext {
  crossingNames: CrossingName[]
  overpasses: Overpass[]
  states: AreaStretch[]
  parks: AreaStretch[]
  edgeTags: EdgeTagStretch[]
  edgeMarks: EdgeTagMark[]
}

export interface RouteContextInput {
  coordinates: number[][]
  distances: number[]
  details?: Record<string, unknown[]>
  profile?: string
}

const NONE: RouteContext = { crossingNames: [], overpasses: [], states: [], parks: [], edgeTags: [], edgeMarks: [] }

/** The value of a GraphHopper detail over the coordinate interval holding `index`. */
function detailAt<T>(entries: unknown[] | undefined, index: number): T | undefined {
  for (const e of entries ?? []) {
    if (!Array.isArray(e)) continue
    const [from, to, value] = e as [number, number, T]
    if (from <= index && index < to) return value
  }
  return undefined
}

/** The route's level where it passes under something: its way's layer, else a bridge a level up; nothing in a tunnel. */
async function levels(details: Record<string, unknown[]> | undefined, distances: number[], source: RouteContextSource, at: number[]): Promise<LevelAt> {
  const indexAt = (d: number) => {
    let lo = 0
    let hi = distances.length - 1
    while (hi - lo > 1) {
      const mid = (lo + hi) >> 1
      if (distances[mid] <= d) lo = mid
      else hi = mid
    }
    return lo
  }
  const ids = [...new Set(at.map(d => detailAt<number>(details?.osm_way_id, indexAt(d))).filter((id): id is number => typeof id === 'number'))]
  const known = await source.wayLevels(ids)
  return distance => {
    const index = indexAt(distance)
    const environment = String(detailAt<string>(details?.road_environment, index) ?? '').toLowerCase()
    if (environment === 'tunnel') return { level: null }
    const wayId = detailAt<number>(details?.osm_way_id, index)
    const own = wayId != null ? known.get(wayId) : undefined
    return { level: own ?? (environment === 'bridge' ? 1 : 0), ...(wayId != null && { wayId }) }
  }
}

async function overpasses(path: RoutePath, input: RouteContextInput, source: RouteContextSource): Promise<Overpass[]> {
  if (path.length > MAX_OVERPASS_METERS) return []
  const structures = await source.structures(routePieces(path, OVERPASS_PIECE_METERS))
  const crossed = structures.length ? structureCrossings(path, structures) : []
  if (!crossed.length) return []
  return overpassesOver(crossed, await levels(input.details, input.distances, source, crossed.map(c => c.distance)))
}

async function states(path: RoutePath, source: RouteContextSource): Promise<AreaStretch[]> {
  const pieces = routePieces(path, Math.max(STATE_PIECE_METERS, path.length / MAX_STATE_PIECES))
  return areaStretches(pieces, await source.states(pieces))
}

async function parks(path: RoutePath, input: RouteContextInput, source: RouteContextSource): Promise<AreaStretch[]> {
  if (input.profile !== 'foot' || path.length > MAX_PARK_METERS) return []
  const pieces = routePieces(path, PARK_PIECE_METERS)
  return areaStretches(pieces, await source.parks(pieces), { joinMeters: PARK_JOIN_METERS, minMeters: PARK_MIN_METERS })
}

interface Crossings {
  names: CrossingName[]
  diagonals: EdgeTagStretch[]
}

async function crossings(input: RouteContextInput, source: RouteContextSource): Promise<Crossings> {
  const spans = crossingSpans(input.coordinates, input.distances, input.details)
  if (!spans.length) return { names: [], diagonals: [] }
  const roads = await source.crossedRoads(spans)
  return {
    names: nameCrossings(spans, roads),
    diagonals: diagonalCrossings(spans, roads).map(i => ({ startDistance: spans[i].startDistance, endDistance: spans[i].endDistance, crossingDiagonal: true })),
  }
}

interface EdgeTagging {
  stretches: EdgeTagStretch[]
  marks: EdgeTagMark[]
}

async function edgeTags(path: RoutePath, input: RouteContextInput, source: RouteContextSource): Promise<EdgeTagging> {
  if (!EDGE_TAG_PROFILES.has(input.profile ?? '') || path.length > MAX_EDGE_TAG_METERS) return { stretches: [], marks: [] }
  const ways = routeWays(input.distances, input.details)
  const ids = [...new Set(ways.map(w => w.osmWayId))]
  const pieces = routePieces(path, NODE_PIECE_METERS)
  const [records, rows] = await Promise.all([
    ids.length && ids.length <= MAX_EDGE_WAYS ? source.edgeWays(ids).catch(() => new Map<number, WayRecord>()) : new Map<number, WayRecord>(),
    source.edgeNodes(pieces, toleranceDegrees(NODE_METERS, path.coordinates[0][1])).catch(() => [] as EdgeNodeRow[]),
  ])
  const nodes = rows.flatMap(r => path.project(r.coordinate, NODE_METERS).map(p => ({ distance: p.distance, tags: r.tags })))
  const spans = crossingSpans(input.coordinates, input.distances, input.details)
  return {
    stretches: [...wayStretches(ways, records, path), ...crossingStretches(spans, nodes, ways, records)],
    marks: nodeMarks(nodes),
  }
}

export async function getRouteContext(input: RouteContextInput, source: RouteContextSource = dbRouteContextSource): Promise<RouteContext> {
  if (input.coordinates.length < 2) return NONE
  const path = routePath(input.coordinates.map(c => [c[0], c[1]] as LonLat))
  const settle = <T>(p: Promise<T[]>) => p.catch(() => [] as T[])
  const [crossed, over, inStates, inParks, tagged] = await Promise.all([
    crossings(input, source).catch((): Crossings => ({ names: [], diagonals: [] })),
    settle(overpasses(path, input, source)),
    settle(states(path, source)),
    settle(parks(path, input, source)),
    edgeTags(path, input, source).catch((): EdgeTagging => ({ stretches: [], marks: [] })),
  ])
  return {
    crossingNames: crossed.names,
    overpasses: over,
    states: inStates,
    parks: inParks,
    edgeTags: [...tagged.stretches, ...(EDGE_TAG_PROFILES.has(input.profile ?? '') ? crossed.diagonals : [])],
    edgeMarks: tagged.marks,
  }
}
