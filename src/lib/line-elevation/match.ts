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

/** Which sources a line may ride: those holding its own OSM way, or any of some kinds that run alongside it. */
export type Rides = { way: number } | { kinds: SourceKind[] }

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

/** The height at `p` of the nearest segment it rides, or null; `direction` (unit) is the line's there, checked when given. */
export function heightAt(segments: Segment[], p: Point, direction?: Point): Ride | null {
  let best: { distance: number; height: number; relative: boolean } | null = null
  for (const { source, i } of segments) {
    const [a, b] = [source.points[i - 1], source.points[i]]
    const hit = onSegment(a, b, p)
    if (!hit || hit.distance > source.half + SLACK || (best && hit.distance >= best.distance)) continue
    if (direction) {
      const l = Math.hypot(b[0] - a[0], b[1] - a[1])
      if (Math.abs((direction[0] * (b[0] - a[0]) + direction[1] * (b[1] - a[1])) / l) < ALIGNED) continue
    }
    best = { distance: hit.distance, height: source.heights[i - 1] + (source.heights[i] - source.heights[i - 1]) * hit.t, relative: source.relative }
  }
  return best && { height: best.height, relative: best.relative }
}

/** A line read every SAMPLE_STEP metres, its own vertices included, with the height each point rides at. */
export function sampleLine(line: Point[], index: SourceIndex, rides: Rides): Sample[] {
  const points = densify(line, SAMPLE_STEP)
  const d = along(points)
  const own = 'way' in rides ? index.segmentsOf(rides.way) : null
  if (own && !own.length) return points.map((p, i) => ({ p, d: d[i], height: null }))
  const kinds = 'kinds' in rides ? rides.kinds : []
  return points.map((p, i) => {
    const sample = (ride: Ride | null): Sample => ({ p, d: d[i], height: ride?.height ?? null, relative: ride?.relative })
    if (own) return sample(heightAt(own, p))
    const [a, b] = i < points.length - 1 ? [p, points[i + 1]] : [points[i - 1], p]
    const l = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    const near = index.segmentsNear(p, kinds)
    return sample(near.length ? heightAt(near, p, [(b[0] - a[0]) / l, (b[1] - a[1]) / l]) : null)
  })
}
