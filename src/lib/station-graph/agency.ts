/**
 * Agency in-station times from GTFS `pathways.txt` (VBB, MBTA, PID, IDFM,
 * SEPTA publish one). Enrichment only: the station graph never depends on it,
 * but where a feed has it, it outranks the geometry estimate, and it is the
 * regression set the graph is checked against.
 */
import { components, shortestPaths, type Adjacency } from './dijkstra'
import { LocalProjection, pointWayDistance, type Pt } from './geometry'

type Row = Record<string, string>

/** MBTA times walkways by length only; these fill the gaps the way the PAR-350 spike did. */
const WALK_MPS = 1.3
const STEP_S = 0.6
const GATE_S = 5
const LIMIT_S = 1800

export interface AgencyStop {
  stopId: string
  kind: 'platform' | 'entrance'
  lon: number
  lat: number
  level: number | null
}

export interface AgencyTime {
  from: string
  to: string
  seconds: number
}

function pathwayCost(p: Row): number | null {
  const time = Number(p.traversal_time)
  if (p.traversal_time?.trim() && Number.isFinite(time)) return time
  if (p.pathway_mode === '6' || p.pathway_mode === '7') return GATE_S
  if (p.pathway_mode === '2' && p.stair_count?.trim()) return Math.abs(Number(p.stair_count)) * STEP_S
  const length = Number(p.length)
  if (p.length?.trim() && Number.isFinite(length)) return length / WALK_MPS
  return null
}

/**
 * Every platform-to-platform (in-system: entrances are never passed through),
 * platform-to-entrance and entrance-to-platform time a feed's pathways give.
 */
export function agencyStationTimes(feed: { stops: Row[]; pathways: Row[]; levels?: Row[] }): { stops: AgencyStop[]; times: AgencyTime[] } {
  const levelIndex = new Map((feed.levels ?? []).map((l) => [l.level_id, Number(l.level_index)]))
  const adj: Adjacency = new Map()
  const link = (a: string, b: string, cost: number) => {
    const list = adj.get(a) ?? []
    list.push({ to: b, cost, edge: -1, vertical: false })
    adj.set(a, list)
  }
  for (const p of feed.pathways) {
    const c = pathwayCost(p)
    if (c === null) continue
    link(p.from_stop_id, p.to_stop_id, c)
    if (p.is_bidirectional === '1') link(p.to_stop_id, p.from_stop_id, c)
  }
  const touched = new Set([...adj.keys(), ...[...adj.values()].flatMap((l) => l.map((a) => a.to))])

  const stops: AgencyStop[] = []
  for (const s of feed.stops) {
    const type = s.location_type ?? ''
    const kind = type === '0' || type === '' ? 'platform' : type === '2' ? 'entrance' : null
    if (!kind || !touched.has(s.stop_id)) continue
    const level = levelIndex.get(s.level_id)
    stops.push({ stopId: s.stop_id, kind, lon: Number(s.stop_lon), lat: Number(s.stop_lat), level: Number.isFinite(level) ? level! : null })
  }
  const kindOf = new Map(stops.map((s) => [s.stopId, s.kind]))
  const entrances = new Set(stops.filter((s) => s.kind === 'entrance').map((s) => s.stopId))

  // One search graph per station (connected component), so each search is small.
  const find = components(adj)
  const parts = new Map<string, { free: Adjacency; inSystem: Adjacency }>()
  for (const [from, arcs] of adj) {
    const root = find(from)
    const part = parts.get(root) ?? { free: new Map(), inSystem: new Map() }
    part.free.set(from, arcs)
    // In-system searches never pass through a street entrance.
    if (!entrances.has(from)) part.inSystem.set(from, arcs)
    parts.set(root, part)
  }
  const times: AgencyTime[] = []
  for (const s of stops) {
    const part = parts.get(find(s.stopId))
    if (!part) continue
    const free = shortestPaths(part.free, [s.stopId], { limit: LIMIT_S }).reached()
    const inSystem = s.kind === 'platform' ? shortestPaths(part.inSystem, [s.stopId], { limit: LIMIT_S }).reached() : free
    for (const [to, seconds] of s.kind === 'platform' ? inSystem : free) {
      if (to === s.stopId || !kindOf.has(to)) continue
      if (s.kind === 'entrance' && kindOf.get(to) !== 'platform') continue
      if (s.kind === 'platform' && kindOf.get(to) === 'entrance') continue
      times.push({ from: s.stopId, to, seconds: Math.round(seconds) })
    }
    if (s.kind === 'platform') {
      for (const [to, seconds] of free) {
        if (kindOf.get(to) === 'entrance') times.push({ from: s.stopId, to, seconds: Math.round(seconds) })
      }
    }
  }
  return { stops, times }
}

export interface OsmPlatformShape {
  id: string
  level: number | null
  /** Rings in lon/lat. */
  rings: number[][][]
  area: boolean
}

/** How far a GTFS quay may sit from the OSM platform it stands for. */
export const PLATFORM_MATCH_M = 10
export const ENTRANCE_MATCH_M = 20

/**
 * Agency times keyed by OSM ids (`${from}>${to}`, the shortest where several
 * quays map to one platform). Each quay maps to the nearest OSM platform
 * within PLATFORM_MATCH_M whose level has the same sign, each entrance to
 * the nearest OSM entrance within ENTRANCE_MATCH_M.
 */
export function agencyTimesByOsm(
  agency: { stops: AgencyStop[]; times: AgencyTime[] },
  platforms: OsmPlatformShape[],
  entrances: Array<{ id: string; lon: number; lat: number }>,
  opts: { platformM?: number } = {},
): Map<string, number> {
  const out = new Map<string, number>()
  if (!agency.stops.length || (!platforms.length && !entrances.length)) return out
  const ref = agency.stops[0]
  const proj = new LocalProjection(ref.lon, ref.lat)
  const shapes = platforms.map((p) => ({ ...p, pm: p.rings.map((r) => r.map(([x, y]) => proj.project(x, y))) }))
  const ents = entrances.map((e) => ({ id: e.id, at: proj.project(e.lon, e.lat) }))
  const maxPlatform = opts.platformM ?? PLATFORM_MATCH_M

  const osmOf = new Map<string, string>()
  for (const s of agency.stops) {
    const at: Pt = proj.project(s.lon, s.lat)
    let best: { d: number; id: string } | null = null
    if (s.kind === 'platform') {
      for (const p of shapes) {
        if (s.level !== null && p.level !== null && Math.sign(s.level) !== Math.sign(p.level) && s.level !== 0 && p.level !== 0) continue
        const d = pointWayDistance(at, p.pm, p.area).d
        if (d <= maxPlatform && (!best || d < best.d)) best = { d, id: p.id }
      }
    } else {
      for (const e of ents) {
        const d = Math.hypot(at[0] - e.at[0], at[1] - e.at[1])
        if (d <= ENTRANCE_MATCH_M && (!best || d < best.d)) best = { d, id: e.id }
      }
    }
    if (best) osmOf.set(s.stopId, best.id)
  }

  for (const t of agency.times) {
    const a = osmOf.get(t.from), b = osmOf.get(t.to)
    if (!a || !b || a === b) continue
    const key = `${a}>${b}`
    out.set(key, Math.min(out.get(key) ?? Infinity, t.seconds))
  }
  return out
}
