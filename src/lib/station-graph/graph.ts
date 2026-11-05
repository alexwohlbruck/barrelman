/**
 * A level-aware walking graph of one station complex, built from OSM ways.
 *
 * Ways join where they share a vertex, with four derived corrections, each a
 * rule so it regenerates on every OSM update:
 *
 * 1. A vertex shared by non-vertical ways on disjoint levels is not a junction
 *    (stacked platforms glued at a node). Stairs, escalators and lifts join
 *    only the copies on a level they serve.
 * 2. A stair end within `snapM` of a way joins it only if that way is on the
 *    end's own level.
 * 3. A lone entrance node joins the nearest station-way vertex within
 *    `entranceSnapM`.
 * 4. (In the solver) an underground platform reaches the street only after a
 *    climb, and bridges never join platform to platform.
 */
import {
  bearingOf, centroidOf, dist, isArea, LocalProjection, pointWayDistance, principalAxis, rings, vertexKey, type Pt,
} from './geometry'
import { components, type Adjacency, type Arc } from './dijkstra'
import type { StationPathParams } from './params'
import { inStation, isElevated, isEntrance, isFareGate, isUnderground, levelOf, levelsOf, wayClass, type WayClass } from './tags'
import type { OsmNode, OsmWay, Profile, Tags } from './types'

export type EdgeKind = WayClass | 'area' | 'snap' | 'split' | 'entrance' | 'bridge'
export type VerticalKind = 'steps' | 'escalator' | 'elevator'

export interface GraphEdge {
  a: string
  b: string
  /** Scaled seconds each way; Infinity where the edge can't be used that way (against an escalator). */
  costAB: number
  costBA: number
  len: number
  kind: EdgeKind
  vertical: VerticalKind | null
  station: boolean
  wayId: string | null
}

interface Way {
  id: string
  tags: Tags
  cls: WayClass
  station: boolean
  pm: Pt[][]
  keys: string[][]
  keySet: Set<string>
  levels: Set<number>
  area: boolean
}

export interface PlatformGeometry {
  id: string
  tags: Tags
  level: number | null
  underground: boolean
  elevated: boolean
  pm: Pt[][]
  area: boolean
  axisBearing: number
  lengthM: number
}

const CELL = 20
const cellOf = (p: Pt) => `${Math.floor(p[0] / CELL)}:${Math.floor(p[1] / CELL)}`
const disjoint = (a: Set<number>, b: Set<number>) => [...a].every((x) => !b.has(x))
const sortedLevels = (s: Set<number>) => [...s].sort((x, y) => x - y).join(',')

/** Whether a staircase climbs in its drawn direction: `incline`, else the levels at its ends. */
function climbsForward(t: Tags, ends: [Set<number>, Set<number>]): boolean | null {
  if (t.incline === 'up') return true
  if (t.incline === 'down') return false
  const mean = (s: Set<number>) => (s.size ? [...s].reduce((a, b) => a + b, 0) / s.size : null)
  const a = mean(ends[0]), b = mean(ends[1])
  if (a === null || b === null || a === b) return null
  return b > a
}

function verticalKind(t: Tags): VerticalKind | null {
  if (t.conveying !== undefined) return 'escalator'
  if (t.highway === 'steps') return 'steps'
  if (t.highway === 'elevator') return 'elevator'
  return null
}

export class StationGraph {
  readonly proj: LocalProjection
  readonly xy = new Map<string, Pt>()
  readonly edges: GraphEdge[] = []
  readonly entranceKey = new Map<string, string>()
  readonly elevatorKeys = new Set<string>()
  readonly gateKeys = new Set<string>()
  readonly centre = new Map<string, string>()
  readonly positions = new Map<string, { nodes: string[]; fractions: number[] }>()
  readonly platforms = new Map<string, PlatformGeometry>()
  readonly splitKeys = new Set<string>()
  /** Levels of the non-vertical ways at each vertex. */
  private readonly hlev = new Map<string, Set<number>>()
  /** Levels of the ends of stairs entered both ways round. */
  private readonly copyLevel = new Map<string, number>()
  private readonly pendingAttach: Array<{ wayId: string; node: string; at: Pt }> = []
  private readonly ways: Way[] = []
  private readonly grid = new Map<string, Way[]>()

  constructor(input: { ways: OsmWay[]; nodes: OsmNode[] }, readonly params: StationPathParams) {
    const pts = input.ways.flatMap((w) => rings(w.geometry).flat())
    const c = pts.length ? pts.reduce((s, p) => [s[0] + p[0], s[1] + p[1]], [0, 0]).map((v) => v / pts.length) : [0, 0]
    this.proj = new LocalProjection(c[0], c[1])

    for (const w of input.ways) {
      const cls = wayClass(w.tags)
      if (!cls) continue
      const rs = rings(w.geometry).filter((r) => r.length > 0)
      if (!rs.length) continue
      const keys = rs.map((r) => r.map(([lon, lat]) => vertexKey(lon, lat)))
      const pm = rs.map((r) => r.map(([lon, lat]) => this.proj.project(lon, lat)))
      keys.forEach((r, ri) => r.forEach((k, i) => this.xy.set(k, pm[ri][i])))
      this.ways.push({
        id: w.id, tags: w.tags, cls, station: inStation(w.tags, cls), pm, keys,
        keySet: new Set(keys.flat()), levels: levelsOf(w.tags), area: isArea(w.geometry),
      })
    }

    for (const n of input.nodes) {
      const k = vertexKey(n.lon, n.lat)
      if (!this.xy.has(k)) this.xy.set(k, this.proj.project(n.lon, n.lat))
      if (isEntrance(n.tags)) this.entranceKey.set(n.id, k)
      if (n.tags.highway === 'elevator') this.elevatorKeys.add(k)
      if (isFareGate(n.tags)) this.gateKeys.add(k)
    }

    this.build()
  }

  private build() {
    const p = this.params
    const vways = new Map<string, Way[]>()
    const use = new Map<string, number>()
    const stKeys = new Set<string>()
    for (const w of this.ways) {
      for (const k of w.keySet) {
        const at = vways.get(k)
        if (at) at.push(w)
        else vways.set(k, [w])
        use.set(k, (use.get(k) ?? 0) + 1)
        if (w.station) stKeys.add(k)
        if (w.cls !== 'vertical') {
          const s = this.hlev.get(k) ?? new Set<number>()
          w.levels.forEach((l) => s.add(l))
          this.hlev.set(k, s)
        }
      }
      if (!w.station) continue
      for (const r of w.pm) {
        for (let i = 0; i < r.length; i++) {
          const a = r[i], b = r[i + 1] ?? r[i]
          const [x0, x1] = [Math.min(a[0], b[0]), Math.max(a[0], b[0])].map((v) => Math.floor(v / CELL))
          const [y0, y1] = [Math.min(a[1], b[1]), Math.max(a[1], b[1])].map((v) => Math.floor(v / CELL))
          for (let x = x0; x <= x1; x++) {
            for (let y = y0; y <= y1; y++) {
              const list = this.grid.get(`${x}:${y}`) ?? []
              if (list[list.length - 1] !== w) list.push(w)
              this.grid.set(`${x}:${y}`, list)
            }
          }
        }
      }
    }

    // Rule 1: split vertices shared by non-vertical ways on disjoint levels.
    for (const [k, ws] of vways) {
      if (this.elevatorKeys.has(k)) continue
      const leveled = ws.filter((w) => w.cls !== 'vertical' && w.levels.size)
      if (leveled.some((a, i) => leveled.slice(i + 1).some((b) => disjoint(a.levels, b.levels)))) this.splitKeys.add(k)
    }
    const nid = (k: string, w: Way) => {
      if (!this.splitKeys.has(k)) return k
      return w.cls !== 'vertical' && w.levels.size ? `s|${k}|${sortedLevels(w.levels)}` : `sv|${k}`
    }
    // Unleveled flat ways at a split vertex reach every copy; a stair or lift
    // end joins only the copy on its own level (see node() below).
    const copiesAt = new Map<string, Map<string, Set<number>>>()
    for (const k of this.splitKeys) {
      const ws = vways.get(k)!
      const copies = new Map<string, Set<number>>()
      for (const w of ws) if (w.cls !== 'vertical' && w.levels.size) copies.set(sortedLevels(w.levels), w.levels)
      copiesAt.set(k, copies)
      const unleveled = ws.some((w) => w.cls !== 'vertical' && !w.levels.size)
      this.xy.set(`sv|${k}`, this.xy.get(k)!)
      for (const tag of copies.keys()) {
        const copy = `s|${k}|${tag}`
        this.xy.set(copy, this.xy.get(k)!)
        if (unleveled) this.addEdge(`sv|${k}`, copy, 0, 0, 0, 'split', null, true, null)
      }
    }

    // A station way's ends: the levels each one is on, from the ways it
    // touches, or for a stair the level its other end doesn't have. A stair
    // with both ends loose (common where it is drawn over a stacked platform)
    // is entered both ways round, so whichever end is the bottom, every route
    // between its levels still has to climb it.
    const entranceKeys = new Set(this.entranceKey.values())
    const otherLevels = (k: string, self: Way) => {
      const out = new Set<number>()
      for (const o of vways.get(k) ?? []) if (o !== self) o.levels.forEach((l) => out.add(l))
      return out
    }
    const orientations = (w: Way, rk: string[]): Array<{ tag: string; ends: [Set<number>, Set<number>] }> => {
      const touching: [Set<number>, Set<number>] = [otherLevels(rk[0], w), otherLevels(rk[rk.length - 1], w)]
      if (w.cls !== 'vertical' || w.levels.size < 2) return [{ tag: '', ends: touching }]
      // Each end of a stair is on one of the stair's own levels, never both.
      const streetMost = [...w.levels].reduce((a, b) => (Math.abs(b) < Math.abs(a) ? b : a))
      const own = touching.map((t, e) => {
        const on = [...t].filter((l) => w.levels.has(l))
        return on.length || !entranceKeys.has(rk[e === 0 ? 0 : rk.length - 1]) ? on : [streetMost]
      })
      const farthest = (from: number) => [...w.levels].reduce((x, y) => (Math.abs(y - from) > Math.abs(x - from) ? y : x))
      if (own[0].length && own[1].length) return [{ tag: '', ends: [new Set([Math.min(...own[0])]), new Set([Math.min(...own[1])])] }]
      for (const e of [0, 1] as const) {
        if (!own[e].length) continue
        const here = Math.min(...own[e])
        const ends: [Set<number>, Set<number>] = e === 0 ? [new Set([here]), new Set([farthest(here)])] : [new Set([farthest(here)]), new Set([here])]
        return [{ tag: 'x', ends }]
      }
      const lo = Math.min(...w.levels), hi = Math.max(...w.levels)
      return [{ tag: 'a', ends: [new Set([lo]), new Set([hi])] }, { tag: 'b', ends: [new Set([hi]), new Set([lo])] }]
    }

    const attach = new Map<string, Array<{ node: string; at: Pt }>>()
    for (const w of this.ways) {
      const vk = w.cls === 'vertical' ? verticalKind(w.tags) : null
      const nseg = Math.max(1, w.pm.reduce((s, r) => s + r.length - 1, 0))
      w.keys.forEach((rk, ri) => {
        if (vk === 'elevator' && (w.area || (rk.length > 2 && rk[0] === rk[rk.length - 1]))) {
          if (ri === 0) this.shaft(w, vways)
          return
        }
        for (const { tag, ends } of orientations(w, rk)) {
          // A loose end of a stair entered both ways round, or an end shared only
          // with ways off its level, stands apart from the vertex it is drawn on.
          const last = rk.length - 1
          const node = (k: string, e?: 0 | 1) => {
            if (e !== undefined && w.cls === 'vertical' && this.splitKeys.has(k)) {
              const want = [...ends[e]][0]
              for (const [t, lv] of copiesAt.get(k)!) if (want !== undefined && lv.has(want)) return `s|${k}|${t}`
              return `sv|${k}`
            }
            const shared = (use.get(k) ?? 0) > 1
            if (shared) {
              const lv = otherLevels(k, w)
              const offLevel = e !== undefined && tag && lv.size && ![...lv].some((l) => ends[e].has(l))
              if (!offLevel) return nid(k, w)
            } else if ((tag !== 'a' && tag !== 'b') || entranceKeys.has(k)) {
              return nid(k, w)
            }
            const copy = `o|${w.id}|${tag}|${k}`
            this.xy.set(copy, this.xy.get(k)!)
            if (e !== undefined) this.copyLevel.set(copy, [...ends[e]][0])
            return copy
          }
          const endOf = (i: number) => (i === 0 ? 0 : i === last ? 1 : undefined)
          const upForward = vk === 'steps' ? climbsForward(w.tags, ends) : null
          const r = w.pm[ri]
          for (let i = 0; i + 1 < r.length; i++) {
            const len = dist(r[i], r[i + 1])
            let base = len / p.costs.walkMps
            if (vk === 'escalator') base = Math.max(len, p.costs.escalatorMinM / nseg) / p.costs.escalatorMps
            else if (vk === 'steps') base = Math.max(len, p.costs.stairMinM / nseg) / p.costs.stairMps
            else if (vk === 'elevator') base = p.costs.elevatorS / nseg
            let ab = base, ba = base
            if (upForward !== null) {
              ab = base * (upForward ? p.costs.stairUpFactor : p.costs.stairDownFactor)
              ba = base * (upForward ? p.costs.stairDownFactor : p.costs.stairUpFactor)
            }
            if (vk === 'escalator' && w.tags.conveying === 'forward') ba = Infinity
            if (vk === 'escalator' && w.tags.conveying === 'backward') ab = Infinity
            this.addEdge(node(rk[i], endOf(i)), node(rk[i + 1], endOf(i + 1)), ab, ba, len, w.cls, vk, w.station, w.id)
          }

          // Rule 2: a loose end joins a station way within snapM only on its own level.
          if (!w.station || w.cls === 'platform') continue
          ;[0, rk.length - 1].forEach((idx, e) => {
            const k = rk[idx]
            const at = r[idx]
            const hits: Array<{ o: Way; hit: ReturnType<typeof pointWayDistance> }> = []
            for (const o of this.near(at)) {
              if (o === w || o.keySet.has(k) || !o.station) continue
              if (w.levels.size && o.levels.size && disjoint(w.levels, o.levels)) continue
              if (ends[e].size && o.levels.size && disjoint(ends[e], o.levels)) continue
              const hit = pointWayDistance(at, o.pm, o.area)
              if (hit.d <= p.graph.snapM) hits.push({ o, hit })
            }
            // An end of unknown level over stacked ways joins only the shallowest of them.
            const leveled = hits.filter((h) => h.o.levels.size)
            const shallowest = leveled.length
              ? leveled.map((h) => Math.max(...h.o.levels)).reduce((a, b) => (Math.abs(b) < Math.abs(a) ? b : a))
              : null
            for (const { o, hit } of hits) {
              if (shallowest !== null && o.levels.size && !o.levels.has(shallowest)) continue
              if (o.area) {
                const list = attach.get(o.id) ?? []
                list.push({ node: node(k, e as 0 | 1), at })
                attach.set(o.id, list)
              } else if (hit.seg >= 0) {
                for (const q of [o.keys[hit.ring][hit.seg], o.keys[hit.ring][hit.seg + 1]]) {
                  const L = dist(at, this.xy.get(q)!)
                  this.addEdge(node(k, e as 0 | 1), nid(q, o), L / p.costs.walkMps, L / p.costs.walkMps, L, 'snap', null, true, w.id)
                }
              }
            }
          })
        }
      })
    }

    for (const a of this.pendingAttach) {
      const list = attach.get(a.wayId) ?? []
      list.push({ node: a.node, at: a.at })
      attach.set(a.wayId, list)
    }

    // Rule 3: lone entrances join the nearest station-way vertex.
    for (const [, k] of this.entranceKey) {
      if (stKeys.has(k)) continue
      const at = this.xy.get(k)!
      let best: { d: number; node: string } | null = null
      for (const o of this.near(at)) {
        if (o.cls === 'platform' || !o.station) continue
        o.keys.forEach((r, ri) => r.forEach((q, i) => {
          const d = dist(at, o.pm[ri][i])
          if (d <= p.graph.entranceSnapM && (!best || d < best.d)) best = { d, node: nid(q, o) }
        }))
      }
      if (best) {
        const { d, node } = best as { d: number; node: string }
        this.addEdge(k, node, d / p.costs.walkMps, d / p.costs.walkMps, d, 'entrance', null, true, null)
      }
    }

    // Platforms and indoor areas are crossed in straight lines between their attached points.
    for (const w of this.ways) {
      if (w.cls === 'vertical' || !w.station || !(w.area || w.cls === 'platform')) continue
      const att = new Map<string, Pt>()
      w.keys.forEach((r, ri) => r.forEach((k, i) => {
        if ((use.get(k) ?? 0) > 1 || entranceKeys.has(k) || this.elevatorKeys.has(k)) att.set(nid(k, w), w.pm[ri][i])
      }))
      for (const a of attach.get(w.id) ?? []) att.set(a.node, a.at)

      if (w.cls === 'platform') {
        const ringPts = w.pm.flatMap((r) => (w.area ? r.slice(0, -1) : r))
        const reach = new Map(att)
        w.keys.forEach((r, ri) => r.forEach((k, i) => reach.set(nid(k, w), w.pm[ri][i])))
        const centre = `c|${w.id}`
        this.xy.set(centre, centroidOf(ringPts))
        this.centre.set(w.id, centre)
        const axis = principalAxis(ringPts)
        const span = axis.max - axis.min
        const n = Math.max(2, Math.round(span / p.graph.positionStepM) + 1)
        const nodes: string[] = []
        const fractions: number[] = []
        for (let i = 0; i < n; i++) {
          const f = i / (n - 1)
          const t = axis.min + f * span
          const node = `p|${w.id}|${i}`
          this.xy.set(node, [axis.centre[0] + t * axis.dir[0], axis.centre[1] + t * axis.dir[1]])
          nodes.push(node)
          fractions.push(Math.round(f * 1000) / 1000)
        }
        this.positions.set(w.id, { nodes, fractions })
        for (const from of [centre, ...nodes]) {
          for (const [to, at] of reach) this.straight(from, to, this.xy.get(from)!, at, 'area', w.id)
        }
        const level = levelOf(w.tags)
        this.platforms.set(w.id, {
          id: w.id, tags: w.tags, level, underground: isUnderground(w.tags), elevated: isElevated(w.tags),
          pm: w.pm, area: w.area, axisBearing: Math.round(bearingOf(axis.dir) * 10) / 10, lengthM: Math.round(span),
        })
      }
      if (w.area && att.size <= p.graph.maxAreaLinks) {
        const list = [...att]
        for (let i = 0; i < list.length; i++) {
          for (let j = i + 1; j < list.length; j++) this.straight(list[i][0], list[j][0], list[i][1], list[j][1], 'area', w.id)
        }
      }
    }
  }

  /**
   * A lift drawn as its shaft outline: one stop per level it serves, joined
   * through the car (a full ride costs elevatorS), and each stop reached only
   * by ways on its own level.
   */
  private shaft(w: Way, vways: Map<string, Way[]>) {
    const p = this.params
    const centre = centroidOf(w.pm[0].slice(0, -1))
    const hub = `e|${w.id}`
    this.xy.set(hub, centre)
    const levels = w.levels.size ? [...w.levels] : [0]
    const stop = (l: number) => `e|${w.id}|${l}`
    for (const l of levels) {
      this.xy.set(stop(l), centre)
      this.copyLevel.set(stop(l), l)
      this.addEdge(stop(l), hub, p.costs.elevatorS / 2, p.costs.elevatorS / 2, 0, 'vertical', 'elevator', w.station, w.id)
    }
    const stopsFor = (o: Way) => {
      if (!o.levels.size) return [levels.reduce((a, b) => (Math.abs(b) < Math.abs(a) ? b : a))]
      return levels.filter((l) => o.levels.has(l))
    }
    const nidOf = (k: string, o: Way) =>
      !this.splitKeys.has(k) ? k : o.cls !== 'vertical' && o.levels.size ? `s|${k}|${sortedLevels(o.levels)}` : `sv|${k}`
    const join = (a: string, b: string, at: Pt) => {
      const L = dist(at, centre)
      this.addEdge(a, b, L / p.costs.walkMps, L / p.costs.walkMps, L, 'snap', null, true, w.id)
    }
    const shaftRing = w.pm[0]
    const touching = new Set<Way>()
    for (const k of w.keySet) for (const o of vways.get(k) ?? []) if (o !== w) touching.add(o)
    for (const o of this.near(centre)) if (o !== w && o.station) touching.add(o)
    for (const o of touching) {
      const stops = stopsFor(o)
      if (!stops.length) continue
      if (o.area) {
        if (pointWayDistance(centre, o.pm, true).d > p.graph.snapM + 5) continue
        for (const l of stops) this.pendingAttach.push({ wayId: o.id, node: stop(l), at: centre })
        continue
      }
      o.keys.forEach((r, ri) => r.forEach((k, i) => {
        const at = o.pm[ri][i]
        const onShaft = w.keySet.has(k) || pointWayDistance(at, [shaftRing], true).d <= p.graph.snapM
        if (onShaft) for (const l of stops) join(stop(l), nidOf(k, o), at)
      }))
    }
  }

  private near(at: Pt): Way[] {
    const [cx, cy] = cellOf(at).split(':').map(Number)
    const out = new Set<Way>()
    for (let dx = -1; dx <= 1; dx++) for (let dy = -1; dy <= 1; dy++) for (const w of this.grid.get(`${cx + dx}:${cy + dy}`) ?? []) out.add(w)
    return [...out]
  }

  private straight(a: string, b: string, pa: Pt, pb: Pt, kind: EdgeKind, wayId: string) {
    if (a === b) return
    const L = dist(pa, pb)
    this.addEdge(a, b, L / this.params.costs.walkMps, L / this.params.costs.walkMps, L, kind, null, true, wayId)
  }

  private addEdge(a: string, b: string, ab: number, ba: number, len: number, kind: EdgeKind, vertical: VerticalKind | null, station: boolean, wayId: string | null) {
    const s = this.params.costs.scale
    this.edges.push({ a, b, costAB: ab * s, costBA: ba * s, len, kind, vertical, station, wayId })
  }

  /**
   * Arcs for a search. Station-only drops the street; `ban` keeps a search
   * from passing through nodes (street entrances, for in-system transfers);
   * `reverse` flips every arc, for searching backwards from a destination.
   */
  adjacency(opts: { profile: Profile; reverse?: boolean; ban?: Set<string>; extra?: GraphEdge[] } = { profile: 'foot' }): Adjacency {
    const adj: Adjacency = new Map()
    const half = (this.params.costs.elevatorS * this.params.costs.scale) / 2
    const all = opts.extra ? [...this.edges, ...opts.extra] : this.edges
    const push = (from: string, arc: Arc) => {
      const list = adj.get(from)
      if (list) list.push(arc)
      else adj.set(from, [arc])
    }
    all.forEach((e, i) => {
      if (!e.station) return
      if (opts.profile === 'wheelchair' && (e.vertical === 'steps' || e.vertical === 'escalator')) return
      if (opts.ban?.has(e.a) || opts.ban?.has(e.b)) return
      const lift = (n: string) => (this.elevatorKeys.has(n) ? half : 0)
      const vertical = e.vertical !== null || this.elevatorKeys.has(e.a) || this.elevatorKeys.has(e.b)
      const ab = e.costAB + lift(e.a) + lift(e.b)
      const ba = e.costBA + lift(e.a) + lift(e.b)
      const [fwd, back] = opts.reverse ? [ba, ab] : [ab, ba]
      if (Number.isFinite(fwd)) push(e.a, { to: e.b, cost: fwd, edge: i, vertical })
      if (Number.isFinite(back)) push(e.b, { to: e.a, cost: back, edge: i, vertical })
    })
    return adj
  }

  /** The level at a node, where the map pins one down. */
  vertexLevel(node: string): number | null {
    if (node.startsWith('c|') || node.startsWith('p|')) {
      const id = node.startsWith('c|') ? node.slice(2) : node.slice(2, node.lastIndexOf('|'))
      return this.platforms.get(id)?.level ?? null
    }
    if (node.startsWith('s|')) return Number(node.slice(node.lastIndexOf('|') + 1).split(',')[0])
    if (this.copyLevel.has(node)) return this.copyLevel.get(node)!
    const k = this.baseKey(node)
    const lv = this.hlev.get(k)
    return lv && lv.size === 1 ? [...lv][0] : null
  }

  /** The map vertex a graph node stands for. */
  baseKey(node: string): string {
    if (node.startsWith('s|')) return node.slice(2, node.lastIndexOf('|'))
    if (node.startsWith('sv|')) return node.slice(3)
    if (node.startsWith('o|')) return node.slice(node.lastIndexOf('|') + 1)
    return node
  }

  lonLat(node: string): [number, number] {
    return this.proj.unproject(this.xy.get(node)!)
  }

  /**
   * Bridge edges between the components of `adj`, joining non-platform
   * vertices at most `bridge.maxM` apart: a gap in the map (a missing
   * mezzanine) rather than a wall. Never platform to platform, which would
   * cross the tracks.
   */
  bridges(adj: Adjacency): GraphEdge[] {
    const p = this.params
    const find = components(adj)

    const candidates = new Set<string>()
    for (const e of this.edges) {
      if (!e.station || !(e.kind === 'vertical' || e.kind === 'corridor' || e.kind === 'footway' || e.kind === 'entrance')) continue
      if (adj.has(e.a)) candidates.add(e.a)
      if (adj.has(e.b)) candidates.add(e.b)
    }
    for (const k of this.entranceKey.values()) if (adj.has(k)) candidates.add(k)

    const G = p.bridge.maxM
    const grid = new Map<string, string[]>()
    const cell = (q: Pt) => [Math.floor(q[0] / G), Math.floor(q[1] / G)]
    for (const k of candidates) {
      const [x, y] = cell(this.xy.get(k)!)
      grid.set(`${x}:${y}`, [...(grid.get(`${x}:${y}`) ?? []), k])
    }
    const best = new Map<string, GraphEdge>()
    for (const k of candidates) {
      const pk = this.xy.get(k)!
      const rk = find(k)
      const [x, y] = cell(pk)
      for (let dx = -1; dx <= 1; dx++) {
        for (let dy = -1; dy <= 1; dy++) {
          for (const m of grid.get(`${x + dx}:${y + dy}`) ?? []) {
            const rm = find(m)
            if (rm === rk) continue
            const d = dist(pk, this.xy.get(m)!)
            if (d > G) continue
            const la = this.levelSet(k), lb = this.levelSet(m)
            let levels = 0
            if (la.size && lb.size && disjoint(la, lb)) levels = Math.min(...[...la].flatMap((a) => [...lb].map((b) => Math.abs(a - b))))
            const cost = ((d * p.bridge.detour) / p.costs.walkMps + p.bridge.levelS * levels) * p.costs.scale
            const id = `${k}>${rm}`
            if (!best.has(id) || cost < best.get(id)!.costAB) {
              best.set(id, { a: k, b: m, costAB: cost, costBA: cost, len: d, kind: 'bridge', vertical: null, station: true, wayId: null })
            }
          }
        }
      }
    }
    return [...best.values()]
  }

  private levelSet(node: string): Set<number> {
    if (node.startsWith('s|')) return new Set(node.slice(node.lastIndexOf('|') + 1).split(',').map(Number))
    if (this.copyLevel.has(node)) return new Set([this.copyLevel.get(node)!])
    return this.hlev.get(this.baseKey(node)) ?? new Set()
  }

  /** Vertices that sit on a street way or are an entrance: where a surfacing path may end. */
  streetKeys(): Set<string> {
    const station = new Set<string>(), street = new Set<string>()
    for (const w of this.ways) for (const k of w.keySet) (w.station ? station : street).add(k)
    const out = new Set<string>(this.entranceKey.values())
    for (const k of station) if (street.has(k)) out.add(k)
    return out
  }
}
