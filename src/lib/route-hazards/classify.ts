/**
 * Which OSM tags make a hazard, for whom, and which way a sign faces. Pure, so
 * the rules are tested without a database; the SQL only narrows the candidates.
 */

import { angleBetween } from './route-path'

export type Tags = Record<string, string>

export const PROFILES = ['car', 'bike', 'foot'] as const
export type HazardProfile = (typeof PROFILES)[number]

export const HAZARD_KINDS = [
  'traffic_signals',
  'stop',
  'give_way',
  'tram_tracks',
  'tram_crossing',
  'unlit',
  'flood_prone',
  'school_zone',
  'traffic_calming',
  'no_sidewalk',
  'low_clearance',
  'toll',
  'speed_camera',
  'movable_bridge',
  'hazard',
] as const
export type HazardKind = (typeof HAZARD_KINDS)[number]

const PROFILE_KINDS: Record<HazardProfile, ReadonlySet<HazardKind>> = {
  car: new Set(['traffic_signals', 'stop', 'give_way', 'flood_prone', 'school_zone', 'traffic_calming', 'low_clearance', 'toll', 'speed_camera', 'movable_bridge', 'hazard']),
  bike: new Set(['traffic_signals', 'stop', 'give_way', 'tram_tracks', 'tram_crossing', 'unlit', 'flood_prone', 'school_zone', 'traffic_calming', 'movable_bridge', 'hazard']),
  foot: new Set(['traffic_signals', 'unlit', 'flood_prone', 'no_sidewalk', 'movable_bridge', 'hazard']),
}

export function appliesTo(kind: HazardKind, profile: HazardProfile): boolean {
  return PROFILE_KINDS[profile].has(kind)
}

export interface PointHazard {
  kind: HazardKind
  detail?: string
  /** Metres from the route it may stand: on the way, or a sign at the roadside. */
  reach: number
  /** The tag saying which way it faces, if it has one. */
  directionKey?: string
}

/** A node on the way the route drives. GraphHopper simplifies its geometry by about a metre. */
export const ON_WAY_METERS = 3
/** A sign or camera beside the road. */
export const ROADSIDE_METERS = 15

const CALMING = new Set(['bump', 'hump', 'table', 'cushion', 'dip', 'rumble_strip', 'yes'])

/** Hazard values that name a kind of their own. */
const HAZARD_ALIASES: Record<string, HazardKind> = {
  school_zone: 'school_zone',
  children: 'school_zone',
  flooding: 'flood_prone',
}

export function pointHazards(tags: Tags): PointHazard[] {
  const out: PointHazard[] = []
  switch (tags.highway) {
    case 'traffic_signals':
      out.push({ kind: 'traffic_signals', reach: ON_WAY_METERS, directionKey: tags['traffic_signals:direction'] ? 'traffic_signals:direction' : 'direction' })
      break
    case 'stop':
      out.push({ kind: 'stop', reach: ON_WAY_METERS, directionKey: 'direction', ...(tags.stop === 'all' && { detail: 'all' }) })
      break
    case 'give_way':
      out.push({ kind: 'give_way', reach: ON_WAY_METERS, directionKey: 'direction' })
      break
    case 'speed_camera':
      out.push({ kind: 'speed_camera', reach: ROADSIDE_METERS, directionKey: 'direction', ...(tags.maxspeed && { detail: tags.maxspeed }) })
      break
  }
  const calming = tags.traffic_calming
  if (calming && CALMING.has(calming)) {
    out.push({ kind: 'traffic_calming', reach: ON_WAY_METERS, detail: tags.highway === 'crossing' ? 'raised_crossing' : calming })
  }
  if (tags.barrier === 'toll_booth') out.push({ kind: 'toll', reach: ON_WAY_METERS, detail: 'booth' })
  const clearance = maxheightMeters(tags.maxheight)
  if (clearance != null) out.push({ kind: 'low_clearance', reach: ON_WAY_METERS, detail: String(clearance) })
  if (tags.hazard) out.push(hazardSign(tags.hazard))
  return out
}

function hazardSign(value: string): PointHazard {
  const kind = HAZARD_ALIASES[value]
  return kind ? { kind, reach: ROADSIDE_METERS, directionKey: 'direction' } : { kind: 'hazard', detail: value, reach: ROADSIDE_METERS, directionKey: 'direction' }
}

export interface WayHazard {
  kind: HazardKind
  detail?: string
}

/** Roads someone on foot walks in the carriageway of when there is no sidewalk. */
const WALKED_ROADS = new Set(['trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified', 'residential'])

function noSidewalk(tags: Tags): boolean {
  if (!WALKED_ROADS.has(tags.highway)) return false
  const sidewalk = tags.sidewalk
  if (sidewalk === 'no' || sidewalk === 'none') return true
  if (tags['sidewalk:both'] === 'no') return true
  return tags['sidewalk:left'] === 'no' && tags['sidewalk:right'] === 'no'
}

/** Hazards of a way the route runs along. */
export function wayHazards(tags: Tags): WayHazard[] {
  if (tags.railway === 'tram') return tags.embedded && tags.embedded !== 'no' ? [{ kind: 'tram_tracks' }] : []
  if (!tags.highway) return []
  const out: WayHazard[] = []
  if (tags.lit === 'no') out.push({ kind: 'unlit' })
  if (tags.flood_prone === 'yes') out.push({ kind: 'flood_prone' })
  if (tags.toll === 'yes') out.push({ kind: 'toll' })
  if (tags.bridge === 'movable' || tags['bridge:movable']) out.push({ kind: 'movable_bridge', ...(tags['bridge:movable'] && { detail: tags['bridge:movable'] }) })
  if (noSidewalk(tags)) out.push({ kind: 'no_sidewalk', detail: tags.highway })
  const clearance = maxheightMeters(tags.maxheight)
  if (clearance != null) out.push({ kind: 'low_clearance', detail: String(clearance) })
  if (tags.hazard) {
    const kind = HAZARD_ALIASES[tags.hazard]
    out.push(kind ? { kind } : { kind: 'hazard', detail: tags.hazard })
  }
  return out
}

/** A tram line the route crosses, other than over or under it. */
export function isCrossableTram(tags: Tags): boolean {
  return tags.railway === 'tram' && (!tags.tunnel || tags.tunnel === 'no') && (!tags.bridge || tags.bridge === 'no')
}

/**
 * `maxheight` in metres: `3.5`, `3.5 m`, `12'6"`. `below_default` (a bridge
 * lower than the legal vehicle height) reads as 0; `none` and `default` as null.
 */
export function maxheightMeters(value: string | undefined): number | null {
  if (!value) return null
  const v = value.trim()
  if (v === 'below_default') return 0
  const feet = v.match(/^(\d+(?:\.\d+)?)\s*'\s*(?:(\d+(?:\.\d+)?)\s*")?$/)
  if (feet) return round2(Number(feet[1]) * 0.3048 + Number(feet[2] ?? 0) * 0.0254)
  const meters = v.match(/^(\d+(?:\.\d+)?)\s*(?:m)?$/)
  return meters ? round2(Number(meters[1])) : null
}

const round2 = (n: number) => Math.round(n * 100) / 100

const CARDINALS: Record<string, number> = {
  N: 0, NNE: 22.5, NE: 45, ENE: 67.5, E: 90, ESE: 112.5, SE: 135, SSE: 157.5,
  S: 180, SSW: 202.5, SW: 225, WSW: 247.5, W: 270, WNW: 292.5, NW: 315, NNW: 337.5,
}

export type Facing =
  | { type: 'any' }
  | { type: 'way'; forward: boolean }
  /** The compass bearing the sign faces; traffic meeting it travels the other way. */
  | { type: 'bearing'; bearing: number }

export function parseDirection(value: string | undefined): Facing {
  if (!value) return { type: 'any' }
  const v = value.trim()
  if (v === 'forward') return { type: 'way', forward: true }
  if (v === 'backward') return { type: 'way', forward: false }
  const cardinal = CARDINALS[v.toUpperCase()]
  if (cardinal != null) return { type: 'bearing', bearing: cardinal }
  const degrees = Number(v)
  return Number.isFinite(degrees) && v !== '' ? { type: 'bearing', bearing: ((degrees % 360) + 360) % 360 } : { type: 'any' }
}

/** A way at a node: its bearing there, along its nodes, and its class. */
export interface WayAtNode {
  azimuth: number
  highway: string
}

/** Within this, a way at a node runs the route's way rather than across it. */
const PARALLEL_DEG = 35

const parallelAngle = (way: WayAtNode, routeBearing: number) => {
  const d = angleBetween(way.azimuth, routeBearing)
  return Math.min(d, 180 - d)
}

export function routeWay(ways: WayAtNode[], routeBearing: number): WayAtNode | null {
  let best: WayAtNode | null = null
  let bestAngle = PARALLEL_DEG
  for (const way of ways) {
    const angle = parallelAngle(way, routeBearing)
    if (angle <= bestAngle) {
      best = way
      bestAngle = angle
    }
  }
  return best
}

/** Whether a sign or signal facing as tagged is seen by traffic on the route's bearing. */
export function facesRoute(facing: Facing, routeBearing: number, onWay: WayAtNode | null): boolean {
  switch (facing.type) {
    case 'any':
      return true
    case 'way': {
      if (!onWay) return true
      const withWay = angleBetween(onWay.azimuth, routeBearing) < 90
      return withWay === facing.forward
    }
    case 'bearing':
      return angleBetween((facing.bearing + 180) % 360, routeBearing) < 90
  }
}

const ROAD_RANK: Record<string, number> = {
  motorway: 9, motorway_link: 9, trunk: 8, trunk_link: 8, primary: 7, primary_link: 7,
  secondary: 6, secondary_link: 6, tertiary: 5, tertiary_link: 5, unclassified: 4,
  residential: 3, living_street: 2, service: 1,
}

export function isRoad(highway: string): boolean {
  return highway in ROAD_RANK
}

/**
 * Whether an untagged stop or give-way sign at a junction is for the route. A
 * sign on the junction node itself does not say which road it controls, and
 * the minor road is the one that yields; a sign on the way before the junction
 * has no crossing road at its node and always counts.
 */
export function controlsRoute(ways: WayAtNode[], routeBearing: number): boolean {
  const own = routeWay(ways, routeBearing)
  const crossing = ways.filter(w => isRoad(w.highway) && parallelAngle(w, routeBearing) > PARALLEL_DEG)
  if (!own || crossing.length === 0) return true
  const ownRank = ROAD_RANK[own.highway] ?? 0
  return crossing.every(w => (ROAD_RANK[w.highway] ?? 0) >= ownRank)
}
