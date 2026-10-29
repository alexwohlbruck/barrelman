/**
 * Heights along a drawn line, read off the sources it rides: points every few
 * metres along it, each given the height of the source it lies on, or none.
 */
import { along, densify, metresPerUnit, type Point } from '../bridge-mesh/decks'
import type { HeightSource, SourceKind } from './sources'

/** Metres between the points a line is read at. */
export const SAMPLE_STEP = 6
/** Metres past a source's edge a line may lie and still ride it. */
export const SLACK = 4
/** Sharpest angle between a line and a source it rides, as its cosine. */
const ALIGNED = Math.cos((30 * Math.PI) / 180)

/**
 * Which sources a line may ride: those holding its own OSM way; those holding any of the ways it was drawn along,
 * reached across the distance its drawing strays from them; or any of some kinds that run alongside it.
 */
export type Rides = { way: number } | { ways: number[] } | { kinds: SourceKind[] }

/** Metres past a source's edge a line drawn along its ways, smoothed and offset from them, may lie and still ride it. */
export const WAYS_SLACK = 25

/** A point of a line, with the height of what it rides there, if anything, and whether that height is over the terrain. */
export type Sample = { p: Point; d: number; height: number | null; relative?: boolean }

type Ride = { height: number; relative: boolean }

type Segment = { source: HeightSource; i: number }

/** The segments of some sources, bucketed on a grid by where a point riding them could lie. */
export class SourceIndex {
  private cells = new Map<string, Segment[]>()
  private byWay = new Map<number, HeightSource[]>()
  private size: number

  constructor(sources: HeightSource[]) {
    const y = sources[0]?.points[0]?.[1] ?? 0.5
    this.size = 32 / metresPerUnit(y)
    for (const source of sources) {
      for (const way of source.ways) this.byWay.set(way, [...(this.byWay.get(way) ?? []), source])
      const pad = (source.half + SLACK) / metresPerUnit(source.points[0][1])
      for (let i = 1; i < source.points.length; i++) {
        const [a, b] = [source.points[i - 1], source.points[i]]
        const [x0, x1] = [Math.floor((Math.min(a[0], b[0]) - pad) / this.size), Math.floor((Math.max(a[0], b[0]) + pad) / this.size)]
        const [y0, y1] = [Math.floor((Math.min(a[1], b[1]) - pad) / this.size), Math.floor((Math.max(a[1], b[1]) + pad) / this.size)]
        for (let x = x0; x <= x1; x++)
          for (let yy = y0; yy <= y1; yy++) {
            const k = `${source.kind}:${x},${yy}`
            const list = this.cells.get(k)
            if (list) list.push({ source, i })
            else this.cells.set(k, [{ source, i }])
          }
      }
    }
  }

  segmentsNear(p: Point, kinds: SourceKind[]): Segment[] {
    const cell = `${Math.floor(p[0] / this.size)},${Math.floor(p[1] / this.size)}`
    return kinds.length === 1 ? this.cells.get(`${kinds[0]}:${cell}`) ?? [] : kinds.flatMap(kind => this.cells.get(`${kind}:${cell}`) ?? [])
  }

  segmentsOf(way: number): Segment[] {
    return (this.byWay.get(way) ?? []).flatMap(source => source.points.slice(1).map((_, k) => ({ source, i: k + 1 })))
  }
}

/** Where a point lies on a segment: metres off it, and how far along, or null off either end. */
function onSegment(a: Point, b: Point, q: Point): { distance: number; t: number } | null {
  const dx = b[0] - a[0], dy = b[1] - a[1]
  const length = dx * dx + dy * dy
  if (!length) return null
  const t = ((q[0] - a[0]) * dx + (q[1] - a[1]) * dy) / length
  if (t < 0 || t > 1) return null
  return { distance: Math.hypot(a[0] + dx * t - q[0], a[1] + dy * t - q[1]) * metresPerUnit(q[1]), t }
}

/** A source a point may ride: how far off its edge the point lies, and the source's height and lift there. */
type Candidate = { source: HeightSource; off: number; height: number; lift: number }

/** The nearest place on each source a point rides, one per source; `direction` (unit) is the line's there, checked when given. */
export function candidatesAt(segments: Segment[], p: Point, direction?: Point, slack = SLACK): Candidate[] {
  const best = new Map<HeightSource, Candidate>()
  for (const { source, i } of segments) {
    const [a, b] = [source.points[i - 1], source.points[i]]
    const hit = onSegment(a, b, p)
    if (!hit || hit.distance > source.half + slack) continue
    if (direction) {
      const l = Math.hypot(b[0] - a[0], b[1] - a[1])
      if (Math.abs((direction[0] * (b[0] - a[0]) + direction[1] * (b[1] - a[1])) / l) < ALIGNED) continue
    }
    const off = Math.max(0, hit.distance - source.half)
    if ((best.get(source)?.off ?? Infinity) <= off) continue
    const at = (z: number[]) => z[i - 1] + (z[i] - z[i - 1]) * hit.t
    best.set(source, { source, off, height: at(source.heights), lift: at(source.lifts) })
  }
  return [...best.values()]
}

/** What riding the ground costs a point, against riding a source it lies on by the metres it stands off it: more than any it rides. */
const groundCost = (slack: number) => slack + 1
/** What moving from one source to another, or to or from the ground, costs, and what each metre it climbs or drops does. */
const SWITCH_COST = 10
const CLIMB_COST = 2

/**
 * A line read every SAMPLE_STEP metres, its own vertices included, with the height each point rides at. What it rides
 * is chosen along its whole length at once: staying on one deck or tunnel is cheap, moving between them dear, and
 * dearer the further it climbs, so a line passing under or over another structure keeps to its own.
 */
export function sampleLine(line: Point[], index: SourceIndex, rides: Rides): Sample[] {
  const points = densify(line, SAMPLE_STEP)
  const d = along(points)
  const own = 'way' in rides ? index.segmentsOf(rides.way) : 'ways' in rides ? rides.ways.flatMap(way => index.segmentsOf(way)) : null
  if (own && !own.length) return points.map((p, i) => ({ p, d: d[i], height: null }))
  const kinds = 'kinds' in rides ? rides.kinds : []
  const slack = 'ways' in rides ? WAYS_SLACK : SLACK
  const options = points.map((p, i): Array<Candidate | null> => {
    if (own && 'way' in rides) return [null, ...candidatesAt(own, p)]
    const [a, b] = i < points.length - 1 ? [p, points[i + 1]] : [points[i - 1], p]
    const l = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    return [null, ...candidatesAt(own ?? index.segmentsNear(p, kinds), p, [(b[0] - a[0]) / l, (b[1] - a[1]) / l], slack)]
  })
  const chosen = cheapestRide(options, groundCost(slack))
  return points.map((p, i) => {
    const ride = chosen[i]
    return { p, d: d[i], height: ride?.height ?? null, relative: ride?.source.relative }
  })
}

const move = (from: Candidate | null, to: Candidate | null) =>
  from?.source === to?.source ? 0 : SWITCH_COST + CLIMB_COST * Math.abs((from?.lift ?? 0) - (to?.lift ?? 0))

/** The cheapest choice of one option per point, by the costs above. */
function cheapestRide(options: Array<Array<Candidate | null>>, ground: number): Array<Candidate | null> {
  if (!options.length) return []
  const emit = (c: Candidate | null) => (c ? c.off : ground)
  let cost = options[0].map(emit)
  const back: number[][] = []
  for (let i = 1; i < options.length; i++) {
    const prev = options[i - 1]
    const step = options[i].map(to => {
      let best = 0, total = Infinity
      prev.forEach((from, k) => {
        const c = cost[k] + move(from, to)
        if (c < total) [best, total] = [k, c]
      })
      return [best, total + emit(to)] as const
    })
    back.push(step.map(([k]) => k))
    cost = step.map(([, c]) => c)
  }
  let k = cost.indexOf(Math.min(...cost))
  const out: Array<Candidate | null> = new Array(options.length)
  for (let i = options.length - 1; i >= 0; i--) {
    out[i] = options[i][k]
    if (i) k = back[i - 1][k]
  }
  return out
}
