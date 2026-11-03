/**
 * Where a road or railway passes over the route: a bridge or raised way that
 * crosses its line at a higher level than the way the route is on.
 */
import { crossings } from '../route-hazards/along'
import { haversine, type LonLat, type RoutePath } from '../route-hazards/route-path'

export type OverpassKind = 'road' | 'rail'

export interface Structure {
  osmId: number
  /** Route segments the structure's line came near, to search crossings in. */
  firstSegment: number
  lastSegment: number
  tags: Record<string, string>
  coordinates: LonLat[]
}

export interface Overpass {
  /** Metres along the route where it passes under the first of the structures. */
  distance: number
  /** Where it leaves the last, for a pair of carriageways or a run of tracks. */
  endDistance: number
  kind: OverpassKind
  name?: string
}

/** Where the route is, as an OSM `layer`; null where it can't see what passes over (a tunnel). */
export type LevelAt = (distance: number) => { level: number | null; wayId?: number }

const RAIL = new Set(['rail', 'light_rail', 'subway', 'tram', 'monorail', 'narrow_gauge', 'funicular'])
/** Ways that aren't roads a driver would call an overpass. */
const NOT_ROADS = new Set(['footway', 'path', 'cycleway', 'steps', 'pedestrian', 'corridor', 'bridleway', 'platform', 'proposed', 'construction', 'elevator'])
/** Shallower than this the structure runs alongside the route, as an elevated line above a street does. */
const MIN_ANGLE_DEG = 25
/** A crossing this near both lines' vertices is a junction the two share, not one over the other. */
const JUNCTION_METERS = 1
/** Structures crossed within this of each other are one overpass: twin carriageways, parallel tracks. */
const CLUSTER_METERS = 40

export function structureKind(tags: Record<string, string>): OverpassKind | null {
  if (tags.railway && RAIL.has(tags.railway)) return 'rail'
  if (tags.highway && !NOT_ROADS.has(tags.highway)) return 'road'
  return null
}

/** A way's OSM layer, with a bridge counted a level up and a tunnel a level down when the layer is left out. */
export function wayLevel(tags: { layer?: string; bridge?: string; tunnel?: string }): number {
  const layer = Number.parseInt(tags.layer ?? '', 10)
  if (Number.isFinite(layer)) return layer
  if (tags.bridge && tags.bridge !== 'no') return 1
  if (tags.tunnel && tags.tunnel !== 'no') return -1
  return 0
}

function atVertex(line: LonLat[], point: LonLat): boolean {
  return line.some(v => haversine(v, point) <= JUNCTION_METERS)
}

export interface StructureCrossing {
  distance: number
  kind: OverpassKind
  /** The structure's own level. */
  level: number
  osmId: number
  name?: string
}

/** Where the route's line crosses each structure's, steeply enough and away from any shared junction. */
export function structureCrossings(path: RoutePath, structures: Structure[]): StructureCrossing[] {
  const out: StructureCrossing[] = []
  for (const s of structures) {
    const kind = structureKind(s.tags)
    if (!kind) continue
    for (const c of crossings(path, s.coordinates, s.firstSegment, s.lastSegment)) {
      if (c.angle < MIN_ANGLE_DEG) continue
      if (atVertex(s.coordinates, c.coordinate) && atVertex(path.coordinates, c.coordinate)) continue
      out.push({ distance: c.distance, kind, level: wayLevel(s.tags), osmId: s.osmId, ...(s.tags.name && { name: s.tags.name }) })
    }
  }
  return out.sort((a, b) => a.distance - b.distance)
}

/** The crossings that pass over the route, with those close together read as one overpass. */
export function overpassesOver(crossed: StructureCrossing[], levelAt: LevelAt): Overpass[] {
  const out: Overpass[] = []
  for (const p of crossed) {
    const route = levelAt(p.distance)
    if (route.level == null || route.wayId === p.osmId || p.level <= route.level) continue
    const last = out[out.length - 1]
    if (last && p.distance - last.endDistance <= CLUSTER_METERS) {
      last.endDistance = p.distance
      if (p.kind === 'rail') last.kind = 'rail'
      if (last.name !== p.name) delete last.name
      continue
    }
    out.push({ distance: p.distance, endDistance: p.distance, kind: p.kind, ...(p.name && { name: p.name }) })
  }
  const round = (n: number) => Math.round(n * 10) / 10
  return out.map(o => ({ ...o, distance: round(o.distance), endDistance: round(o.endDistance) }))
}
