/** What guidance can say about each edge of a walking or cycling route, from the tags of the ways it runs and the nodes it passes. */
import { STREETS, type DrivingSide } from '../lanes/cross-section'
import { drivingSideFor } from '../lanes/driving-side'
import { distanceAlong } from '../lanes/lane-geometry'
import type { LonLat, RoutePath } from '../route-hazards/route-path'
import type { CrossingSpan } from './crossing-names'

export type Tags = Record<string, string | undefined>
export type Passage = 'building' | 'arcade' | 'indoor'
export type CyclewaySide = 'left' | 'right' | 'both'

export interface StationEntrance {
  name?: string
  ref?: string
}

export interface EdgeTags {
  crossingIsland?: boolean
  buttonOperated?: boolean
  crossingDiagonal?: boolean
  passage?: Passage
  cyclewaySide?: CyclewaySide
  entrance?: StationEntrance
  level?: string
  conveying?: boolean
  elevator?: boolean
}

export interface EdgeTagStretch extends EdgeTags {
  startDistance: number
  endDistance: number
}

/** Tags of the edge that starts where the route passes a node. */
export interface EdgeTagMark extends EdgeTags {
  distance: number
}

export interface RouteWay {
  osmWayId: number
  startDistance: number
  endDistance: number
  /** ISO 3166-1 alpha-3, for the side traffic keeps to. */
  country?: string
}

export interface WayRecord {
  tags: Tags
  /** Only for ways whose direction matters. */
  coordinates?: LonLat[]
}

/** A tagged node, by metres along the route where the route passes it. */
export interface RouteNode {
  distance: number
  tags: Tags
}

/** A node this far past either end of a crossing is the next way's, not the crossing's. */
const CROSSING_NODE_SLACK_METERS = 0.5
/** An island way meeting a crossing within this is the refuge it crosses to. */
const ISLAND_JOIN_METERS = 1
/** A node at most this far from an edge's start starts the edge. */
const MARK_METERS = 2

const BIKE_LANES = new Set(['lane', 'exclusive_lane', 'opposite_lane', 'track', 'opposite_track', 'share_busway', 'opposite_share_busway'])
const CONVEYING = new Set(['yes', 'forward', 'backward', 'reversible'])
const INDOOR = new Set(['yes', 'corridor'])
const ENTRANCE_RAILWAYS = new Set(['subway_entrance', 'train_station_entrance'])

const yes = (value: string | undefined) => value === 'yes'
const first = (...values: (string | undefined)[]) => values.find(v => v != null)
const opposite = (side: DrivingSide): DrivingSide => (side === 'left' ? 'right' : 'left')

function detailAt(entries: unknown[] | undefined, index: number): unknown {
  for (const e of entries ?? []) {
    if (Array.isArray(e) && e[0] <= index && index < e[1]) return e[2]
  }
  return undefined
}

/** The ways the route runs, from GraphHopper's `osm_way_id` detail. */
export function routeWays(distances: number[], details: Record<string, unknown[]> | undefined): RouteWay[] {
  const ways: RouteWay[] = []
  for (const entry of details?.osm_way_id ?? []) {
    if (!Array.isArray(entry)) continue
    const [from, to, id] = entry as [number, number, unknown]
    if (typeof id !== 'number' || to <= from) continue
    const country = detailAt(details?.country, from)
    ways.push({
      osmWayId: id,
      startDistance: distances[from] ?? 0,
      endDistance: distances[to] ?? distances[from] ?? 0,
      ...(typeof country === 'string' && country !== 'MISSING' && { country }),
    })
  }
  return ways
}

export function passageOf(tags: Tags): Passage | undefined {
  if (tags.tunnel === 'building_passage') return 'building'
  if (tags.covered === 'arcade') return 'arcade'
  if (INDOOR.has(tags.indoor ?? '') || tags.highway === 'corridor') return 'indoor'
  return undefined
}

const isCrossingWay = (tags: Tags) =>
  tags.footway === 'crossing' || tags.cycleway === 'crossing' || tags.path === 'crossing' || (tags.crossing != null && tags.crossing !== 'no')

const isDiagonal = (tags: Tags) => tags.crossing === 'diagonal' || /diagonal/.test(tags['crossing:scramble'] ?? '')

const isIsland = (tags: Tags | undefined) => tags?.footway === 'traffic_island' || tags?.crossing === 'traffic_island'

/** What a way's own tags say about an edge along it. */
export function wayEdgeTags(tags: Tags): EdgeTags {
  const passage = passageOf(tags)
  const conveying = CONVEYING.has(tags.conveying ?? '')
  const elevator = tags.highway === 'elevator'
  const crossing = isCrossingWay(tags)
  const level = tags.level?.trim()
  // Outdoors nearly every level tag is the ground's 0, which says nothing.
  const inside = passage != null || conveying || elevator || tags.indoor != null || tags.highway === 'steps'
  return {
    ...(crossing && yes(tags['crossing:island']) && { crossingIsland: true }),
    ...(crossing && yes(tags.button_operated) && { buttonOperated: true }),
    ...(crossing && isDiagonal(tags) && { crossingDiagonal: true }),
    ...(passage && { passage }),
    ...(level && (inside || level !== '0') && { level }),
    ...(conveying && { conveying: true }),
    ...(elevator && { elevator: true }),
  }
}

/** The sides of a road, as its nodes run, that have a bike lane. */
export function bikeLaneSides(tags: Tags, drivingSide: DrivingSide): { left: boolean; right: boolean } {
  const oneway = ['yes', 'true', '1', '-1'].includes(tags.oneway ?? '') || tags.junction === 'roundabout'
  const kerb = tags.oneway === '-1' ? opposite(drivingSide) : drivingSide
  // A one-way's plain cycleway is on the side its traffic keeps to; a contraflow lane on the other.
  const plain = (side: DrivingSide) => {
    const c = tags.cycleway
    if (c == null || !oneway) return c
    return (side === kerb) !== c.startsWith('opposite') ? c : undefined
  }
  const lane = (side: DrivingSide) => BIKE_LANES.has(first(tags[`cycleway:${side}`], tags['cycleway:both'], plain(side)) ?? '')
  return { left: lane('left'), right: lane('right') }
}

/** Which side of the road its bike lane is on, to someone travelling with (`along`) or against the way's nodes. */
export function cyclewaySide(tags: Tags, along: boolean, drivingSide: DrivingSide): CyclewaySide | undefined {
  const { left, right } = bikeLaneSides(tags, drivingSide)
  if (left && right) return 'both'
  if (!left && !right) return undefined
  return left === along ? 'left' : 'right'
}

/** Whether the route runs a way's stretch in the direction of its nodes. */
export function runsAlong(way: LonLat[], path: RoutePath, from: number, to: number): boolean {
  const inset = Math.min(1, (to - from) / 4)
  return distanceAlong(way, path.at(to - inset)) >= distanceAlong(way, path.at(from + inset))
}

export function wayStretches(ways: RouteWay[], records: Map<number, WayRecord>, path: RoutePath): EdgeTagStretch[] {
  const out: EdgeTagStretch[] = []
  for (const way of ways) {
    const record = records.get(way.osmWayId)
    if (!record) continue
    const tags = wayEdgeTags(record.tags)
    if (STREETS.has(record.tags.highway ?? '') && record.coordinates && record.coordinates.length >= 2) {
      const along = runsAlong(record.coordinates, path, way.startDistance, way.endDistance)
      const side = cyclewaySide(record.tags, along, drivingSideFor(way.country))
      if (side) tags.cyclewaySide = side
    }
    if (Object.keys(tags).length) out.push({ startDistance: way.startDistance, endDistance: way.endDistance, ...tags })
  }
  return out
}

/** Islands and push buttons on the crossings, from the nodes along them and the island ways between them. */
export function crossingStretches(spans: CrossingSpan[], nodes: RouteNode[], ways: RouteWay[], records: Map<number, WayRecord>): EdgeTagStretch[] {
  const islands = ways.filter(w => isIsland(records.get(w.osmWayId)?.tags))
  const out: EdgeTagStretch[] = []
  for (const { startDistance, endDistance } of spans) {
    const on = nodes.filter(n => n.distance >= startDistance - CROSSING_NODE_SLACK_METERS && n.distance <= endDistance + CROSSING_NODE_SLACK_METERS)
    const island = on.some(n => yes(n.tags['crossing:island']) || n.tags.crossing === 'traffic_island')
      || islands.some(w => Math.abs(w.startDistance - endDistance) <= ISLAND_JOIN_METERS || Math.abs(w.endDistance - startDistance) <= ISLAND_JOIN_METERS)
    const button = on.some(n => yes(n.tags.button_operated))
    if (island || button) out.push({ startDistance, endDistance, ...(island && { crossingIsland: true }), ...(button && { buttonOperated: true }) })
  }
  return out
}

export const isStationEntrance = (tags: Tags) => ENTRANCE_RAILWAYS.has(tags.railway ?? '') || tags.public_transport === 'entrance'

/** Station entrances and elevators the route passes through. */
export function nodeMarks(nodes: RouteNode[]): EdgeTagMark[] {
  return nodes.flatMap((node): EdgeTagMark[] => {
    const { tags, distance } = node
    if (isStationEntrance(tags)) {
      const ref = tags.ref ?? tags.local_ref
      return [{ distance, entrance: { ...(tags.name && { name: tags.name }), ...(ref && { ref }) } }]
    }
    return tags.highway === 'elevator' ? [{ distance, elevator: true }] : []
  })
}

/**
 * Applies stretches to the segments their middles fall in, and each mark to
 * the segment starting nearest it, if one starts close enough: a node the route
 * walks past mid-way is a sidewalk's, not one it goes through.
 */
export function tagEdgeSegments<T extends { startDistance: number; endDistance: number }>(
  segments: T[],
  stretches: EdgeTagStretch[],
  marks: EdgeTagMark[],
): (T & EdgeTags)[] {
  if (!stretches.length && !marks.length) return segments
  const marked = new Map<number, EdgeTags>()
  for (const { distance, ...tags } of marks) {
    let best = -1
    for (let i = 0; i < segments.length; i++) {
      const off = Math.abs(segments[i].startDistance - distance)
      if (off <= MARK_METERS && (best < 0 || off < Math.abs(segments[best].startDistance - distance))) best = i
    }
    if (best >= 0) marked.set(best, { ...marked.get(best), ...tags })
  }
  return segments.map((seg, i) => {
    const mid = (seg.startDistance + seg.endDistance) / 2
    const over = stretches.filter(s => s.startDistance - 0.1 <= mid && mid <= s.endDistance + 0.1)
    const mark = marked.get(i)
    if (!over.length && !mark) return seg
    return Object.assign({ ...seg }, ...over.map(({ startDistance, endDistance, ...tags }) => tags), mark)
  })
}
