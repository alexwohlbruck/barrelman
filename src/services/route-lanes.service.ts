/**
 * Lane-level route geometry and per-maneuver lane plans, computed for one
 * request from the tags of the ways a route drives (GraphHopper's osm_way_id
 * path detail). Nothing is stored: way ids change as OSM is edited, so the
 * tags are read fresh each time.
 */

import { sql } from 'drizzle-orm'
import { db } from '../db'
import { crossSection, type CrossSectionLane, type DrivingSide, type Tags } from '../lib/lanes/cross-section'
import { drivingSideFor } from '../lib/lanes/driving-side'
import { bikeOffset, laneFor, lanePlan, recommendedLane, turnFromAngle, type PlanLane, type Turn } from '../lib/lanes/lane-plan'
import {
  cumulativeDistances,
  distanceAlong,
  headingChange,
  laneLine,
  type Corner,
  type LaneRun,
} from '../lib/lanes/lane-geometry'

export interface RouteLanesRequest {
  /** The route, [lon, lat]. */
  coordinates: [number, number][]
  /** Which way each stretch drives, by metres along the route. */
  ways: { from: number; to: number; osmWayId: number }[]
  /** Where the route turns, by metres along it. A missing `turn` is read from the geometry. */
  maneuvers?: { distance: number; turn?: Turn }[]
  /** ISO 3166-1 alpha-3, for the side of the road traffic keeps to. */
  country?: string
  drivingSide?: DrivingSide
  /** `bike` draws the line in the bike lane, or beside the traffic; it has no lane plan. */
  profile?: LanesProfile
}

export type LanesProfile = 'car' | 'bike'

export interface RouteLane extends Omit<CrossSectionLane, 'offset'> {
  /** Metres right of the route to the lane's middle. */
  offset: number
}

export interface RouteLanesWay {
  osmWayId: number
  from: number
  to: number
  /** With the way's nodes or against them. */
  direction: 'forward' | 'backward'
  lanes: RouteLane[]
  /** The lane the route takes onto the way, then the one it leaves in. */
  entryLane: number | null
  exitLane: number | null
}

export interface ManeuverLanePlan {
  /** Index into the request's maneuvers. */
  maneuver: number
  distance: number
  turn: Turn
  lanes: PlanLane[]
}

export interface RouteLanesResponse {
  drivingSide: DrivingSide
  lanePlan: ManeuverLanePlan[]
  /** The route in its lanes; `measures` is the route distance each coordinate stands for. */
  laneGeometry: { type: 'LineString'; coordinates: [number, number][]; measures: number[] } | null
  ways: RouteLanesWay[]
}

export interface WayRecord {
  tags: Tags
  coordinates: [number, number][]
}

export type LoadWays = (ids: number[]) => Promise<Map<number, WayRecord>>

export class RouteLanesRequestError extends Error {}

export const MAX_COORDINATES = 20_000
export const MAX_WAYS = 5_000
/** Kerb radius at a corner, as the road surfaces' fillets are drawn. */
const KERB_RADIUS_METERS = 4
/** A maneuver this close to where one way meets the next is at that join. */
const AT_JOIN_METERS = 3
/** A lane change is done this far before the stop line. */
const SETTLE_METERS = 8
const STOP_LINE_BACK_METERS = 1.5
/** Streets, as against paths mapped on their own, which keep to their own line. */
const STREETS = new Set([
  'motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link',
  'tertiary', 'tertiary_link', 'unclassified', 'residential', 'living_street', 'service', 'busway',
])

export const loadWaysFromDb: LoadWays = async (ids) => {
  if (ids.length === 0) return new Map()
  const rows = (await db.execute(sql`
    SELECT osm_id, tags, ST_AsGeoJSON(geom)::jsonb -> 'coordinates' AS coordinates
    FROM geo_places
    WHERE osm_type = 'W' AND osm_id = ANY(${`{${ids.join(',')}}`}::bigint[]) AND geom_type = 'line'
  `)) as unknown as { osm_id: number | string; tags: Tags; coordinates: [number, number][] }[]
  return new Map(Array.from(rows).map(r => [Number(r.osm_id), { tags: r.tags, coordinates: r.coordinates }]))
}

interface Stretch {
  from: number
  to: number
  osmWayId: number | null
}

/** The ways end to end over the route, joined where one runs on, gaps filled. */
export function stretches(ways: RouteLanesRequest['ways'], length: number): Stretch[] {
  const sorted = ways.filter(w => w.to > w.from).sort((a, b) => a.from - b.from)
  const out: Stretch[] = []
  let at = 0
  for (const w of sorted) {
    const from = Math.max(at, Math.min(w.from, length))
    const to = Math.min(length, w.to)
    if (to <= from) continue
    if (from > at + 0.5) out.push({ from: at, to: from, osmWayId: null })
    const last = out.at(-1)
    if (last && last.osmWayId === w.osmWayId && from - last.to < 0.5) last.to = to
    else out.push({ from: last ? last.to : from, to, osmWayId: w.osmWayId })
    at = to
  }
  if (length - at > 0.5 || out.length === 0) out.push({ from: at, to: length, osmWayId: null })
  else out[out.length - 1].to = length
  return out
}

interface Driven {
  stretch: Stretch
  lanes: RouteLane[]
  direction: 'forward' | 'backward'
  kerbLeft: number
  kerbRight: number
  width: number
  laneWidth: number
}

function pointAlong(route: [number, number][], cumulative: number[], s: number): [number, number] {
  let lo = 0
  let hi = cumulative.length - 1
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1
    if (cumulative[mid] <= s) lo = mid
    else hi = mid
  }
  const span = cumulative[hi] - cumulative[lo]
  const t = span > 0 ? Math.min(1, Math.max(0, (s - cumulative[lo]) / span)) : 0
  const a = route[lo]
  const b = route[hi]
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]
}

/** A way's lanes as the route meets them, offsets right of the route. */
function driven(stretch: Stretch, way: WayRecord | undefined, route: [number, number][], cumulative: number[], side: DrivingSide, profile: LanesProfile): Driven | null {
  if (!way || way.coordinates.length < 2 || !STREETS.has(way.tags.highway ?? '')) return null
  const pointAt = (s: number) => pointAlong(route, cumulative, s)
  const inset = Math.min(1, (stretch.to - stretch.from) / 4)
  const start = pointAt(stretch.from + inset)
  const along = distanceAlong(way.coordinates, pointAt(stretch.to - inset)) >= distanceAlong(way.coordinates, start)
  const section = crossSection(way.tags, { americas: start[0] < -30, side })
  // A car can only follow a one-way's traffic; a bike may ride a contraflow lane.
  const withTraffic = (section.oneway && profile === 'car') || along !== section.flip
  const lanes = (withTraffic ? section.forward : section.backward).map(l => ({ ...l, offset: withTraffic ? -l.offset : l.offset }))
  const kerbs = withTraffic ? [-section.kerbLeft, -section.kerbRight] : [section.kerbRight, section.kerbLeft]
  return {
    stretch,
    lanes,
    direction: along ? 'forward' : 'backward',
    kerbLeft: kerbs[0],
    kerbRight: kerbs[1],
    width: section.width,
    laneWidth: section.laneWidth,
  }
}

export async function getRouteLanes(request: RouteLanesRequest, loadWays: LoadWays = loadWaysFromDb): Promise<RouteLanesResponse> {
  const coords = request.coordinates
  if (!Array.isArray(coords) || coords.length < 2) throw new RouteLanesRequestError('coordinates needs at least 2 points')
  if (coords.length > MAX_COORDINATES) throw new RouteLanesRequestError(`At most ${MAX_COORDINATES} coordinates`)
  if (request.ways.length > MAX_WAYS) throw new RouteLanesRequestError(`At most ${MAX_WAYS} ways`)

  const side = request.drivingSide ?? drivingSideFor(request.country)
  const profile = request.profile ?? 'car'
  const cumulative = cumulativeDistances(coords)
  const length = cumulative.at(-1)!
  const parts = stretches(request.ways, length)
  const ids = [...new Set(parts.flatMap(p => (p.osmWayId == null ? [] : [p.osmWayId])))]
  const records = await loadWays(ids)
  const roads = parts.map(p => (p.osmWayId == null ? null : driven(p, records.get(p.osmWayId), coords, cumulative, side, profile)))

  // Each maneuver at the join it turns at, with the stretch it leaves.
  const maneuvers = (request.maneuvers ?? [])
    .map((m, index) => ({ ...m, index }))
    .filter(m => m.distance > AT_JOIN_METERS && m.distance < length - AT_JOIN_METERS)
  const atJoin = new Map<number, { index: number; distance: number; turn: Turn }>()
  for (const m of maneuvers) {
    const join = parts.findIndex(p => Math.abs(p.to - m.distance) <= AT_JOIN_METERS)
    const i = join >= 0 ? join : parts.findIndex(p => m.distance < p.to)
    if (i < 0 || i === parts.length - 1) continue
    const at = join >= 0 ? parts[i].to : m.distance
    atJoin.set(i, { index: m.index, distance: at, turn: m.turn ?? turnFromAngle(headingChange(coords, at)) })
  }

  if (profile === 'bike') return bikeLanes(parts, roads, atJoin, coords, side)

  const lanePlans: ManeuverLanePlan[] = []
  const runs: LaneRun[] = []
  const corners: Corner[] = []
  const ways: RouteLanesWay[] = []
  let current: number | null = null
  let arrivedBy: Turn | null = null

  parts.forEach((part, i) => {
    const road = roads[i]
    const maneuver = atJoin.get(i)
    if (!road) {
      runs.push({ from: part.from, to: part.to, entry: 0, exit: 0, settleBefore: 0, laneWidth: 3 })
      current = null
      arrivedBy = maneuver && maneuver.turn !== 'through' ? maneuver.turn : null
      return
    }
    const entry = laneFor(road.lanes, arrivedBy, side, current)
    let exit = entry
    let settleBefore = 0
    if (maneuver) {
      const plan = lanePlan(road.lanes, maneuver.turn, side)
      if (plan) lanePlans.push({ maneuver: maneuver.index, distance: maneuver.distance, turn: maneuver.turn, lanes: plan })
      exit = recommendedLane(road.lanes, plan) ?? (maneuver.turn === 'through' ? entry : laneFor(road.lanes, maneuver.turn, side, entry?.offset ?? null))
      const crossing = roads[i + 1]
      settleBefore = (crossing ? crossing.width / 2 : 0) + STOP_LINE_BACK_METERS + SETTLE_METERS
      if (maneuver.turn !== 'through' && exit) {
        const inner = maneuver.turn.includes('right') ? road.kerbRight - exit.offset : exit.offset - road.kerbLeft
        corners.push({ at: maneuver.distance, radius: KERB_RADIUS_METERS + Math.max(1, inner) })
      }
    }
    runs.push({
      from: part.from,
      to: part.to,
      entry: entry?.offset ?? 0,
      exit: exit?.offset ?? 0,
      settleBefore,
      laneWidth: road.laneWidth,
    })
    ways.push({
      osmWayId: part.osmWayId!,
      from: part.from,
      to: part.to,
      direction: road.direction,
      lanes: road.lanes,
      entryLane: entry?.index ?? null,
      exitLane: exit?.index ?? null,
    })
    current = exit?.offset ?? null
    arrivedBy = maneuver && maneuver.turn !== 'through' ? maneuver.turn : null
  })

  const laneGeometry = ways.length > 0 ? { type: 'LineString' as const, ...laneLine(coords, runs, corners) } : null
  return { drivingSide: side, lanePlan: lanePlans, laneGeometry, ways }
}

/** A cycling route: in each street's bike lane or beside its traffic, on paths along their own line. */
function bikeLanes(
  parts: Stretch[],
  roads: (Driven | null)[],
  atJoin: Map<number, { distance: number; turn: Turn }>,
  coords: [number, number][],
  side: DrivingSide,
): RouteLanesResponse {
  const runs: LaneRun[] = []
  const corners: Corner[] = []
  const ways: RouteLanesWay[] = []
  parts.forEach((part, i) => {
    const road = roads[i]
    const offset = road ? bikeOffset(road.lanes, side) : null
    runs.push({ from: part.from, to: part.to, entry: offset ?? 0, exit: offset ?? 0, settleBefore: 0, laneWidth: road?.laneWidth ?? 3 })
    const maneuver = atJoin.get(i)
    if (maneuver && maneuver.turn !== 'through' && road && offset != null) {
      const inner = maneuver.turn.includes('right') ? road.kerbRight - offset : offset - road.kerbLeft
      corners.push({ at: maneuver.distance, radius: KERB_RADIUS_METERS + Math.max(1, inner) })
    }
    if (!road || offset == null) return
    const lane = road.lanes.reduce((best, l) => (Math.abs(l.offset - offset) < Math.abs(best.offset - offset) ? l : best))
    ways.push({
      osmWayId: part.osmWayId!,
      from: part.from,
      to: part.to,
      direction: road.direction,
      lanes: road.lanes,
      entryLane: lane.index,
      exitLane: lane.index,
    })
  })
  const laneGeometry = ways.length > 0 ? { type: 'LineString' as const, ...laneLine(coords, runs, corners) } : null
  return { drivingSide: side, lanePlan: [], laneGeometry, ways }
}
