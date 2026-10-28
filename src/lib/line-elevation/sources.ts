/**
 * Where a line drawn over the map stands off the terrain: on bridge decks, at
 * the decks' own heights, and sunk through tunnels below the ground over them.
 * Positions in Web Mercator units, heights in metres.
 */
import { along, densify, holds, metresPerUnit, type Bounds, type Point } from '../bridge-mesh/decks'
import { kindOf, underground, WIDTH, type Way } from '../tunnel-cuts/build'
import { PROFILES, type BoreKind } from '../tunnel-cuts/tunnels'

export type SourceKind = BoreKind

export type HeightSource = {
  points: Point[]
  /** Metres at each point: above sea level in the terrain's datum, or for a relative source above the terrain. */
  heights: number[]
  relative: boolean
  /** Metres above the terrain at each point, to weigh a line's climb from one source to another. */
  lifts: number[]
  /** Metres from the line to the edge of what it carries. */
  half: number
  kind: SourceKind
  /** OSM ways the line is made of. */
  ways: number[]
}

/** A bridge_decks row, with `coords` in degrees. */
export type DeckRow = { ways: Array<string | number>; kind: string; edges: number[]; heights: number[]; ground: number[]; step: number; coords: number[][] }

/** A deck's line with the height its surface is drawn at, at each vertex: the samples lie every `step` metres, the last at its end. */
export function deckSource(row: DeckRow, project: (lngLat: number[]) => Point): HeightSource | null {
  if (!['rail', 'road', 'path'].includes(row.kind) || row.coords.length < 2 || !row.heights.length || row.heights.length !== row.ground.length) return null
  const points = row.coords.map(project)
  const d = along(points)
  const last = row.heights.length - 1
  const sampled = (z: number[]) => d.map(at => {
    const s = row.step > 0 ? Math.min(at / row.step, last) : 0
    const i = Math.min(Math.floor(s), last - 1)
    return i < 0 ? z[0] : z[i] + (z[i + 1] - z[i]) * (s - i)
  })
  const heights = sampled(row.heights)
  const ground = sampled(row.ground)
  return { points, heights, relative: false, lifts: heights.map((h, i) => h - ground[i]), half: Math.max(...row.edges, 0), kind: row.kind as SourceKind, ways: row.ways.map(Number) }
}

/** Metres a tunnel lies below the ground, by what runs through it. One depth, whatever its layer, keeps a line from stepping where its tunnel's layer changes. */
export const DEPTH: Record<SourceKind, number> = { rail: 9, road: 8, path: 5 }
/** Metres between the heights given along a tunnel. */
export const TUNNEL_STEP = 6
/** Metres within which two tunnel ends join. */
const JOIN = 1
/** Metres from a tunnel's end within which a solved portal makes that end a portal. */
const PORTAL_NEAR = 20

/** Metres past a tile the tunnels are read, so the deepest one finds a portal it climbs to. */
export const TUNNEL_MARGIN = Math.ceil(Math.max(...(Object.keys(DEPTH) as SourceKind[]).map(k => DEPTH[k] / PROFILES[k].grade)))

/**
 * Each underground tunnel way sunk to its depth, climbing to the ground at the
 * nearest portal no steeper than its grade. Ways of a kind joined end to end
 * are one bore, measured along however it branches. A portal is an end near
 * one of `portals`, those solved for the tunnel cuts: an end that joins no
 * other tunnel is as often an underground station mapped without the tag.
 */
export function tunnelSources(ways: Way[], read: Bounds, portals: Point[]): HeightSource[] {
  const tunnels = ways.filter(w => underground(w) && w.points.length > 1).map(w => ({ way: w, kind: kindOf(w.cls), points: densify(w.points, TUNNEL_STEP) }))
  if (!tunnels.length) return []
  const tolerance = JOIN / metresPerUnit((read.minY + read.maxY) / 2)
  const cell = tolerance * 4
  const key = (p: Point) => `${Math.floor(p[0] / cell)},${Math.floor(p[1] / cell)}`
  const vertices = new Map<string, Array<{ p: Point; tunnel: number; end: boolean }>>()
  tunnels.forEach((t, tunnel) => t.way.points.forEach((p, i) => {
    const k = key(p), list = vertices.get(k) ?? []
    list.push({ p, tunnel, end: i === 0 || i === t.way.points.length - 1 })
    vertices.set(k, list)
  }))
  const around = (p: Point) => {
    const [cx, cy] = [Math.floor(p[0] / cell), Math.floor(p[1] / cell)]
    const out: Array<{ p: Point; tunnel: number; end: boolean }> = []
    for (let dx = -1; dx <= 1; dx++)
      for (let dy = -1; dy <= 1; dy++)
        for (const v of vertices.get(`${cx + dx},${cy + dy}`) ?? []) if (Math.abs(v.p[0] - p[0]) < tolerance && Math.abs(v.p[1] - p[1]) < tolerance) out.push(v)
    return out
  }

  // Ends that meet are one node; each way an edge between its two.
  const node = new Map<string, number>()
  const nodes: Array<{ portal: boolean }> = []
  const ends = tunnels.map((t, tunnel) => [t.way.points[0], t.way.points.at(-1)!].map(p => {
    const id = node.get(`${tunnel}:${p}`)
    if (id !== undefined) return id
    const meeting = around(p).filter(v => tunnels[v.tunnel].kind === t.kind)
    const joined = meeting.some(v => v.tunnel !== tunnel)
    const index = nodes.push({ portal: !joined && holds(read, p) && portals.some(q => Math.hypot(q[0] - p[0], q[1] - p[1]) * metresPerUnit(p[1]) < PORTAL_NEAR) }) - 1
    for (const v of meeting) if (v.end) node.set(`${v.tunnel}:${v.p}`, index)
    return index
  }))

  const lengths = tunnels.map(t => along(t.points).at(-1)!)
  const reach = nearestPortal(nodes.map(n => n.portal), ends.map(([a, b], t) => [a, b, lengths[t]]))

  return tunnels.map(({ way, kind, points }, t) => {
    const d = along(points), depth = DEPTH[kind], { grade } = PROFILES[kind]
    const [a, b] = ends[t]
    const heights = d.map(at => -Math.min(depth, grade * Math.min(reach[a] + at, reach[b] + lengths[t] - at)))
    return {
      points,
      heights,
      relative: true,
      lifts: heights,
      half: WIDTH[way.cls] / 2,
      kind,
      ways: way.id === undefined ? [] : [way.id],
    }
  })
}

/** Metres from each node to the nearest portal over the edges [a, b, metres] between them; Infinity where none is reached. */
function nearestPortal(portals: boolean[], edges: Array<[number, number, number]>): number[] {
  const links: Array<Array<[number, number]>> = portals.map(() => [])
  for (const [a, b, metres] of edges) {
    links[a].push([b, metres])
    links[b].push([a, metres])
  }
  const reach = portals.map(p => (p ? 0 : Infinity))
  const queue: Array<[number, number]> = reach.flatMap((r, i) => (r === 0 ? [[0, i] as [number, number]] : []))
  while (queue.length) {
    let min = 0
    for (let i = 1; i < queue.length; i++) if (queue[i][0] < queue[min][0]) min = i
    const [r, node] = queue[min]
    queue[min] = queue[queue.length - 1]
    queue.pop()
    if (r > reach[node]) continue
    for (const [next, metres] of links[node])
      if (r + metres < reach[next]) {
        reach[next] = r + metres
        queue.push([reach[next], next])
      }
  }
  return reach
}
