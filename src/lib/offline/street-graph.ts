import {
  OFFLINE_PACKAGE_VERSION,
  STREET_FLAGS,
  type PackageBounds,
  type StreetsPackage,
} from '../../types/offline-package.types'
import { haversineM } from './bbox'

export interface StreetWay {
  id: string
  tags: Record<string, string>
  /** `[lng, lat]` in way order. */
  coords: [number, number][]
}

const FOOT_HIGHWAYS = new Set([
  'footway', 'path', 'pedestrian', 'steps', 'living_street', 'residential', 'service', 'unclassified',
  'tertiary', 'tertiary_link', 'secondary', 'secondary_link', 'primary', 'primary_link', 'track',
  'corridor', 'cycleway', 'road', 'bridleway',
])
const BIKE_HIGHWAYS = new Set([
  'cycleway', 'path', 'residential', 'service', 'unclassified', 'tertiary', 'tertiary_link',
  'secondary', 'secondary_link', 'primary', 'primary_link', 'living_street', 'track', 'road',
])
const CAR_FREE_HIGHWAYS = new Set(['footway', 'path', 'pedestrian', 'cycleway', 'steps', 'track', 'corridor', 'bridleway'])
const BUSY_HIGHWAYS = new Set(['primary', 'primary_link', 'secondary', 'secondary_link', 'trunk', 'trunk_link'])
const ALLOW = new Set(['yes', 'designated', 'permissive', 'official'])
const DENY = new Set(['no', 'private', 'use_sidepath', 'discouraged'])
const BIKE_LANE_VALUES = new Set(['lane', 'track', 'separate', 'shared_lane', 'opposite_lane', 'opposite_track'])

/** Who may use a way and how it rides, as `STREET_FLAGS`. */
export function streetFlags(tags: Record<string, string>): number {
  const highway = tags.highway
  if (!highway || tags.area === 'yes') return 0
  const access = tags.access
  const foot = tags.foot
  const bicycle = tags.bicycle
  const blocked = access !== undefined && DENY.has(access)
  let flags = 0

  const footAllowed = ALLOW.has(foot ?? '')
    || (foot === undefined || !DENY.has(foot))
      && !blocked
      && (FOOT_HIGHWAYS.has(highway)
        || (highway === 'trunk' || highway === 'trunk_link')
          && (tags.sidewalk ?? 'no') !== 'no' && tags.sidewalk !== 'none')
  if (footAllowed && highway !== 'motorway' && highway !== 'motorway_link') flags |= STREET_FLAGS.FOOT

  const bikeAllowed = highway !== 'steps' && highway !== 'motorway' && highway !== 'motorway_link' && (
    ALLOW.has(bicycle ?? '')
    || (bicycle === undefined || !DENY.has(bicycle)) && !blocked && BIKE_HIGHWAYS.has(highway)
  )
  if (bikeAllowed) {
    flags |= STREET_FLAGS.BIKE
    const oneway = tags['oneway:bicycle'] ?? tags.oneway
    const contraflow = tags['oneway:bicycle'] === 'no'
      || Object.entries(tags).some(([k, v]) => k.startsWith('cycleway') && v.startsWith('opposite'))
    if (!contraflow) {
      if (oneway === 'yes' || oneway === 'true' || oneway === '1') flags |= STREET_FLAGS.BIKE_FORWARD_ONLY
      else if (oneway === '-1' || oneway === 'reverse') flags |= STREET_FLAGS.BIKE_BACKWARD_ONLY
      else if (oneway === undefined && (tags.junction === 'roundabout' || tags.junction === 'circular')) {
        flags |= STREET_FLAGS.BIKE_FORWARD_ONLY
      }
    }
  }

  if (highway === 'steps') flags |= STREET_FLAGS.STEPS
  if (highway === 'cycleway'
    || Object.entries(tags).some(([k, v]) => k.startsWith('cycleway') && BIKE_LANE_VALUES.has(v))) {
    flags |= STREET_FLAGS.BIKE_LANE
  }
  if (CAR_FREE_HIGHWAYS.has(highway)) flags |= STREET_FLAGS.CAR_FREE
  if (BUSY_HIGHWAYS.has(highway)) flags |= STREET_FLAGS.BUSY
  return flags & (STREET_FLAGS.FOOT | STREET_FLAGS.BIKE) ? flags : 0
}

const E7 = 1e7
const keyOf = ([lng, lat]: [number, number]) => `${Math.round(lng * E7)},${Math.round(lat * E7)}`
const toE6 = (v: number) => Math.round(v * 1e6)

/**
 * A walk and bike graph from OSM ways. Ways meet where they share a vertex, as
 * OSM ways do at junctions; a bridge over a street shares none, so it stays
 * unconnected.
 */
export function buildStreetGraph(
  ways: StreetWay[],
  options: { bounds: PackageBounds; generatedAt?: string },
): StreetsPackage {
  const usable = ways
    .map(way => ({ way, flags: streetFlags(way.tags) }))
    .filter(({ way, flags }) => flags && way.coords.length >= 2)

  const uses = new Map<string, number>()
  for (const { way } of usable) {
    way.coords.forEach((c, i) => {
      const key = keyOf(c)
      const endpoint = i === 0 || i === way.coords.length - 1
      uses.set(key, (uses.get(key) ?? 0) + (endpoint ? 2 : 1))
    })
  }

  const nodeIndex = new Map<string, number>()
  const nodeCoords: [number, number][] = []
  const nodeFor = (c: [number, number]) => {
    const key = keyOf(c)
    let index = nodeIndex.get(key)
    if (index === undefined) {
      index = nodeCoords.length
      nodeCoords.push(c)
      nodeIndex.set(key, index)
    }
    return index
  }

  const names: string[] = []
  const nameIndex = new Map<string, number>()
  const edges: number[] = []
  const shapes: number[] = []

  for (const { way, flags } of usable) {
    const name = way.tags.name
    let nameRef = -1
    if (name) {
      nameRef = nameIndex.get(name) ?? names.length
      if (nameRef === names.length) { names.push(name); nameIndex.set(name, nameRef) }
    }
    let start = 0
    for (let i = 1; i < way.coords.length; i++) {
      if (i < way.coords.length - 1 && (uses.get(keyOf(way.coords[i])) ?? 0) < 2) continue
      const segment = way.coords.slice(start, i + 1)
      let length = 0
      for (let j = 1; j < segment.length; j++) {
        length += haversineM(segment[j - 1][0], segment[j - 1][1], segment[j][0], segment[j][1])
      }
      const from = nodeFor(segment[0])
      const to = nodeFor(segment[segment.length - 1])
      if (from !== to || segment.length > 2) {
        const inner = segment.slice(1, -1)
        edges.push(from, to, Math.max(1, Math.round(length * 10)), flags, nameRef, inner.length)
        let prev = [toE6(segment[0][0]), toE6(segment[0][1])]
        for (const c of inner) {
          const cur = [toE6(c[0]), toE6(c[1])]
          shapes.push(cur[0] - prev[0], cur[1] - prev[1])
          prev = cur
        }
      }
      start = i
    }
  }

  const nodes: number[] = []
  let prevLng = 0
  let prevLat = 0
  for (const [lng, lat] of nodeCoords) {
    const x = toE6(lng)
    const y = toE6(lat)
    nodes.push(x - prevLng, y - prevLat)
    prevLng = x
    prevLat = y
  }

  return {
    version: OFFLINE_PACKAGE_VERSION,
    bounds: options.bounds,
    generatedAt: options.generatedAt ?? new Date().toISOString(),
    nodes,
    edges,
    shapes,
    names,
  }
}
