/**
 * Point and way hazards along a route, from OSM tags the routing graph does not
 * carry: signals, stop and give-way signs, tram rails, unlit and flood-prone
 * ways, clearances, tolls, cameras and posted hazards.
 *
 * Read straight from geo_places through partial indexes over exactly the
 * predicates below (import/create-route-hazard-indexes.sql), so replication
 * keeps it current and nothing is derived or stored.
 */

import { sql } from 'drizzle-orm'
import { db } from '../db'
import { ALONG_METERS, crossings, runsAlong } from '../lib/route-hazards/along'
import {
  appliesTo,
  controlsRoute,
  facesRoute,
  isCrossableTram,
  parseDirection,
  pointHazards,
  routeWay,
  wayHazards,
  ROADSIDE_METERS,
  type HazardKind,
  type HazardProfile,
  type PointHazard,
  type Tags,
  type WayAtNode,
} from '../lib/route-hazards/classify'
import { routePath, routePieces, toleranceDegrees, type LonLat, type Projection, type RoutePiece } from '../lib/route-hazards/route-path'

export interface RouteHazardsRequest {
  /** The route, [lon, lat]. */
  coordinates: LonLat[]
  profile: HazardProfile
  /** Only these kinds; all that apply to the profile when left out. */
  kinds?: HazardKind[]
}

export interface RouteHazard {
  kind: HazardKind
  /** Metres along the route. */
  distance: number
  /** Where a stretch ends; absent for a point. */
  endDistance?: number
  coordinate: LonLat
  /** The OSM object, `node/123` or `way/456`. */
  osm: string
  detail?: string
}

export interface RouteHazardsResponse {
  hazards: RouteHazard[]
}

export class RouteHazardsRequestError extends Error {}

export const MAX_COORDINATES = 20_000
export const MAX_LENGTH_METERS = 300_000
const PIECE_METERS = 150
/** Below this a rider's wheel can drop into the flangeway. */
const SHALLOW_TRAM_DEG = 30
/** A shallow crossing runs beside the rails for a few tens of metres; riding them lasts longer. */
const TRAM_RIDE_METERS = 50
/** A stretch picks up where the last of its kind ended, across a way split. */
const JOIN_METERS = 10

/** Must match the predicate of geo_places_route_hazard_points_idx, or the planner cannot use it. */
export const POINT_PREDICATE = `geom_type = 'point'
    AND (tags->>'highway' IN ('traffic_signals', 'stop', 'give_way', 'speed_camera')
         OR tags->>'barrier' = 'toll_booth'
         OR tags ?| array['hazard', 'traffic_calming', 'maxheight'])`

/** Must match the predicate of geo_places_route_hazard_ways_idx. */
export const WAY_PREDICATE = `geom_type = 'line'
    AND (tags->>'railway' = 'tram'
         OR (tags ? 'highway'
             AND (tags->>'lit' = 'no' OR tags->>'flood_prone' = 'yes' OR tags->>'toll' = 'yes'
                  OR tags->>'bridge' = 'movable' OR tags->>'sidewalk' IN ('no', 'none')
                  OR tags->>'sidewalk:both' = 'no'
                  OR (tags->>'sidewalk:left' = 'no' AND tags->>'sidewalk:right' = 'no')
                  OR tags ?| array['hazard', 'maxheight', 'bridge:movable'])))`

export interface PointRow {
  osmId: number
  piece: number
  tags: Tags
  coordinate: LonLat
}

export interface WayRow {
  osmId: number
  pieces: number[]
  tags: Tags
  coordinates: LonLat[]
}

export interface HazardSource {
  points(pieces: RoutePiece[], tolerance: number): Promise<PointRow[]>
  ways(pieces: RoutePiece[], tolerance: number): Promise<WayRow[]>
  /** The highways through each node, keyed by node id. */
  waysAtNodes(nodes: { osmId: number; coordinate: LonLat }[]): Promise<Map<number, WayAtNode[]>>
}

const piecesJson = (pieces: RoutePiece[]) =>
  JSON.stringify({ type: 'MultiLineString', coordinates: pieces.map(p => p.coordinates) })

/**
 * The pieces as a `pieces (piece, line)` CTE, `piece` 1-based. The LIMIT only
 * tells the planner how many there are: guessing ST_Dump's default 1000, it
 * sizes hash tables for hundreds of thousands of matches.
 */
export const piecesCte = (pieces: RoutePiece[]) => sql`
  WITH pieces AS (
    SELECT d.path[1] AS piece, d.geom AS line
    FROM ST_Dump(ST_SetSRID(ST_GeomFromGeoJSON(${piecesJson(pieces)}), 4326)) AS d
    LIMIT ${pieces.length}
  )`

export const dbHazardSource: HazardSource = {
  async points(pieces, tolerance) {
    const rows = (await db.execute(sql`
      ${piecesCte(pieces)}
      SELECT p.piece, g.osm_id, g.tags, ST_X(g.geom) AS lon, ST_Y(g.geom) AS lat
      FROM pieces p
      JOIN geo_places g ON ${sql.raw(POINT_PREDICATE)} AND ST_DWithin(g.geom, p.line, ${tolerance})
    `)) as unknown as { piece: number; osm_id: string | number; tags: Tags; lon: number; lat: number }[]
    return Array.from(rows).map(r => ({ osmId: Number(r.osm_id), piece: r.piece - 1, tags: r.tags, coordinate: [r.lon, r.lat] }))
  },

  async ways(pieces, tolerance) {
    const rows = (await db.execute(sql`
      ${piecesCte(pieces)},
      matched AS (
        SELECT g.osm_type, g.osm_id, array_agg(p.piece) AS pieces
        FROM pieces p
        JOIN geo_places g ON ${sql.raw(WAY_PREDICATE)} AND ST_DWithin(g.geom, p.line, ${tolerance})
        GROUP BY g.osm_type, g.osm_id
      )
      SELECT m.osm_id, m.pieces, g.tags, ST_AsGeoJSON(g.geom)::jsonb -> 'coordinates' AS coordinates
      FROM matched m
      JOIN geo_places g ON g.osm_type = m.osm_type AND g.osm_id = m.osm_id AND g.geom_type = 'line'
    `)) as unknown as { osm_id: string | number; pieces: number[]; tags: Tags; coordinates: LonLat[] }[]
    return Array.from(rows).map(r => ({ osmId: Number(r.osm_id), pieces: r.pieces.map(p => p - 1), tags: r.tags, coordinates: r.coordinates }))
  },

  async waysAtNodes(nodes) {
    if (nodes.length === 0) return new Map()
    const rows = (await db.execute(sql`
      SELECT n.id AS osm_id, w.tags->>'highway' AS highway,
             degrees(ST_Azimuth(ST_LineInterpolatePoint(w.geom, greatest(0, w.f - w.e)),
                                ST_LineInterpolatePoint(w.geom, least(1, w.f + w.e)))) AS azimuth
      FROM unnest(${`{${nodes.map(n => n.osmId).join(',')}}`}::bigint[],
                  ${`{${nodes.map(n => n.coordinate[0]).join(',')}}`}::float8[],
                  ${`{${nodes.map(n => n.coordinate[1]).join(',')}}`}::float8[]) AS n(id, lon, lat)
      CROSS JOIN LATERAL (SELECT ST_SetSRID(ST_MakePoint(n.lon, n.lat), 4326) AS pt) q
      CROSS JOIN LATERAL (
        SELECT g.geom, g.tags, ST_LineLocatePoint(g.geom, q.pt) AS f,
               least(0.5, 3 / greatest(ST_Length(g.geom::geography), 1)) AS e
        FROM geo_places g
        WHERE g.geom_type = 'line' AND g.tags ? 'highway' AND ST_DWithin(g.geom, q.pt, 1e-6)
      ) w
    `)) as unknown as { osm_id: string | number; highway: string; azimuth: number | null }[]
    const out = new Map<number, WayAtNode[]>()
    for (const r of Array.from(rows)) {
      if (r.azimuth == null) continue
      const id = Number(r.osm_id)
      out.set(id, [...(out.get(id) ?? []), { azimuth: Number(r.azimuth), highway: r.highway }])
    }
    return out
  },
}

interface Sighting {
  osmId: number
  coordinate: LonLat
  hazard: PointHazard
  tags: Tags
  pass: Projection
}

/** Stop and give-way signs need their junction; signals and cameras only when tagged with a way direction. */
function needsWays(s: Sighting): boolean {
  if (s.hazard.kind === 'stop' || s.hazard.kind === 'give_way') return true
  return s.hazard.directionKey != null && parseDirection(s.tags[s.hazard.directionKey]).type === 'way'
}

function seen(s: Sighting, ways: WayAtNode[] | undefined): boolean {
  const facing = s.hazard.directionKey ? parseDirection(s.tags[s.hazard.directionKey]) : { type: 'any' as const }
  const atNode = ways ?? []
  if (!facesRoute(facing, s.pass.bearing, routeWay(atNode, s.pass.bearing))) return false
  const sign = s.hazard.kind === 'stop' || s.hazard.kind === 'give_way'
  if (sign && facing.type === 'any' && s.hazard.detail !== 'all') return controlsRoute(atNode, s.pass.bearing)
  return true
}

/** Stretches of one kind that meet across a way split read as one. */
function joinStretches(hazards: RouteHazard[]): RouteHazard[] {
  const out: RouteHazard[] = []
  const open = new Map<string, RouteHazard>()
  for (const h of hazards) {
    if (h.endDistance == null) {
      out.push(h)
      continue
    }
    const key = `${h.kind}:${h.detail ?? ''}`
    const last = open.get(key)
    if (last && h.distance - last.endDistance! <= JOIN_METERS) {
      last.endDistance = Math.max(last.endDistance!, h.endDistance)
      continue
    }
    open.set(key, h)
    out.push(h)
  }
  return out
}

const round1 = (n: number) => Math.round(n * 10) / 10

export async function getRouteHazards(request: RouteHazardsRequest, source: HazardSource = dbHazardSource): Promise<RouteHazardsResponse> {
  const coordinates = request.coordinates
  if (!Array.isArray(coordinates) || coordinates.length < 2) throw new RouteHazardsRequestError('coordinates needs at least 2 points')
  if (coordinates.length > MAX_COORDINATES) throw new RouteHazardsRequestError(`coordinates is limited to ${MAX_COORDINATES} points`)
  const path = routePath(coordinates)
  if (path.length > MAX_LENGTH_METERS) throw new RouteHazardsRequestError(`routes are limited to ${MAX_LENGTH_METERS / 1000} km`)

  const wanted = (kind: HazardKind) => appliesTo(kind, request.profile) && (!request.kinds || request.kinds.includes(kind))
  const pieces = routePieces(path, PIECE_METERS)
  const latitude = Math.max(...coordinates.map(c => Math.abs(c[1])))
  const [pointRows, wayRows] = await Promise.all([
    source.points(pieces, toleranceDegrees(ROADSIDE_METERS, latitude)),
    source.ways(pieces, toleranceDegrees(ALONG_METERS, latitude)),
  ])

  const segmentsOf = (indices: number[]) => {
    const segments = new Set<number>()
    for (const i of indices) {
      const piece = pieces[i]
      if (!piece) continue
      for (let s = piece.firstSegment; s <= piece.lastSegment; s++) segments.add(s)
    }
    return segments
  }

  const piecesByNode = new Map<number, { row: PointRow; pieces: number[] }>()
  for (const row of pointRows) {
    const entry = piecesByNode.get(row.osmId)
    if (entry) entry.pieces.push(row.piece)
    else piecesByNode.set(row.osmId, { row, pieces: [row.piece] })
  }

  const sightings: Sighting[] = []
  for (const { row, pieces: found } of piecesByNode.values()) {
    const candidates = pointHazards(row.tags).filter(h => wanted(h.kind))
    if (candidates.length === 0) continue
    const reach = Math.max(...candidates.map(h => h.reach))
    for (const pass of path.project(row.coordinate, reach, segmentsOf(found))) {
      for (const hazard of candidates) {
        if (pass.offset <= hazard.reach) sightings.push({ osmId: row.osmId, coordinate: row.coordinate, hazard, tags: row.tags, pass })
      }
    }
  }

  const contextNodes = [...new Map(sightings.filter(needsWays).map(s => [s.osmId, { osmId: s.osmId, coordinate: s.coordinate }])).values()]
  const waysAt = await source.waysAtNodes(contextNodes)

  const hazards: RouteHazard[] = []
  for (const s of sightings) {
    if (!seen(s, waysAt.get(s.osmId))) continue
    hazards.push({
      kind: s.hazard.kind,
      distance: round1(s.pass.distance),
      coordinate: s.coordinate,
      osm: `node/${s.osmId}`,
      ...(s.hazard.detail && { detail: s.hazard.detail }),
    })
  }

  for (const way of wayRows) {
    const segments = [...segmentsOf(way.pieces)]
    if (segments.length === 0) continue
    const first = Math.min(...segments)
    const last = Math.max(...segments)
    const from = path.cumulative[first]
    const to = path.cumulative[last + 1] ?? path.length
    const kinds = wayHazards(way.tags).filter(h => wanted(h.kind))
    const tram = wanted('tram_crossing') && isCrossableTram(way.tags)
    if (kinds.length === 0 && !tram) continue
    const along = runsAlong(path, way.coordinates, from, to)
    const runs = way.tags.railway === 'tram' ? along.filter(r => r.to - r.from >= TRAM_RIDE_METERS) : along
    for (const run of runs) {
      for (const h of kinds) {
        hazards.push({
          kind: h.kind,
          distance: round1(run.from),
          endDistance: round1(run.to),
          coordinate: path.at(run.from),
          osm: `way/${way.osmId}`,
          ...(h.detail && { detail: h.detail }),
        })
      }
    }
    if (tram) {
      for (const c of crossings(path, way.coordinates, first, last)) {
        if (c.angle >= SHALLOW_TRAM_DEG) continue
        if (runs.some(r => c.distance >= r.from - JOIN_METERS && c.distance <= r.to + JOIN_METERS)) continue
        hazards.push({ kind: 'tram_crossing', distance: round1(c.distance), coordinate: c.coordinate, osm: `way/${way.osmId}`, detail: String(Math.round(c.angle)) })
      }
    }
  }

  hazards.sort((a, b) => a.distance - b.distance || a.kind.localeCompare(b.kind))
  return { hazards: joinStretches(hazards) }
}
