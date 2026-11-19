/**
 * Road beds: the stretches of a road on the ground where the terrain does not
 * hold it as it runs. A DEM blurs the walls of an embankment or a cutting into
 * slopes a few metres wide, and a wide road's outer lanes, draped on them,
 * lean and wobble. Under a deck or another road above it the terrain is not the
 * road's at all: lidar takes the structure overhead for ground. A bed holds the
 * ground under the carriageway at the road's own height, which a map levels the
 * ground to.
 */
import { metresPerUnit, type Point } from '../bridge-mesh/decks'

export const BED = {
  /** Metres between the samples along a road. */
  step: 4,
  /** Metres the ground at an edge may lie off the centreline's before the road is not level across. */
  uneven: 0.6,
  /** Metres the road's own height may lie off the terrain under it before the terrain is levelled to it. */
  off: 0.3,
  /** Metres a bed runs on past its uneven samples, and the shortest gap between two that is not bridged. */
  pad: 8,
  gap: 16,
  /** Shortest bed kept, in metres. */
  min: 12,
}

/**
 * A road on the ground, way by way, end to end. `under` marks the ways whose
 * terrain is not theirs, where it is covered or runs beneath a higher road;
 * `pins` are heights OSM gives its nodes, by vertex.
 */
export type BedRoad = { ways: Array<{ id: number; points: Point[]; half: number; pins: Array<number | null>; under: boolean | ((p: Point) => boolean) }> }

/** A bed on one way: its centreline, the floor along it, and whether it lowers the ground as well as filling it. */
export type Bed = { way: number; points: Point[]; floor: number[]; level: boolean }

/** Samples each side a floor is averaged over. */
const SMOOTH = 2

type Sample = { p: Point; way: number; half: number; pin: number | null; under: boolean; across: Point }

/** The road sampled every BED.step metres, each sample keeping its way and the pin at a vertex on it. */
function sampled(road: BedRoad): Sample[] {
  const out: Sample[] = []
  for (const w of road.ways) {
    const under = (p: Point) => (typeof w.under === 'function' ? w.under(p) : w.under)
    for (let i = 1; i < w.points.length; i++) {
      const [a, b] = [w.points[i - 1], w.points[i]]
      const unit = metresPerUnit((a[1] + b[1]) / 2)
      const length = Math.hypot(b[0] - a[0], b[1] - a[1])
      if (!length) continue
      const n = Math.max(1, Math.ceil((length * unit) / BED.step))
      const across: Point = [-(b[1] - a[1]) / length / unit, (b[0] - a[0]) / length / unit]
      // Each vertex is sampled once: a segment's first is the last of the one before it.
      for (let k = out.length ? 1 : 0; k <= n; k++) {
        const p: Point = [a[0] + ((b[0] - a[0]) * k) / n, a[1] + ((b[1] - a[1]) * k) / n]
        out.push({ p, way: w.id, half: w.half, pin: k === 0 ? w.pins[i - 1] : k === n ? w.pins[i] : null, under: under(p), across })
      }
    }
  }
  return out
}

/** The middle of a value and its neighbours, of those known: a lone spike in the terrain taken out. */
const median3 = (v: Array<number | null>, k: number) => {
  const near = [v[k - 1], v[k], v[k + 1]].filter((x): x is number => x != null).sort((x, y) => x - y)
  return near[near.length >> 1]
}

/** Each run of known values evened out, and the unknown between them filled in straight; held flat beyond the last. */
function profile(known: Array<number | null>, fixed: boolean[]): Array<number | null> {
  const out = [...known]
  for (let i = 0; i < out.length; ) {
    if (out[i] !== null) { i++; continue }
    let j = i
    while (j < out.length && out[j] === null) j++
    const [a, b] = [out[i - 1] ?? null, out[j] ?? null]
    for (let k = i; k < j; k++) out[k] = a !== null && b !== null ? a + ((b - a) * (k - i + 1)) / (j - i + 1) : (a ?? b)
    i = j
  }
  // Pins stay where OSM puts them; between them the road is evened out, so a pin meets the terrain beyond without a kink.
  return out.map((v, k) => {
    if (v === null || fixed[k]) return v
    const near = out.slice(Math.max(0, k - SMOOTH), k + SMOOTH + 1).filter((x): x is number => x !== null)
    return near.reduce((s, x) => s + x, 0) / near.length
  })
}

/**
 * The beds of a road from the ground `at` each point (null where unknown):
 * where the terrain is uneven across the carriageway, filled up to its
 * centreline's height; where it is not the road's, levelled to the height
 * the road runs at, from its pins and the trusted terrain either side.
 */
export function beds(road: BedRoad, at: (p: Point) => number | null): Bed[] {
  const line = sampled(road)
  if (line.length < 2) return []
  const centre = line.map(s => at(s.p))
  const trusted = centre.map((c, k) => (line[k].under ? null : c))
  const median = trusted.map((c, k) => (c === null ? null : median3(trusted, k)))
  const height = profile(line.map((s, k) => s.pin ?? median[k]), line.map(s => s.pin !== null))
  const off = line.map((s, k) => height[k] !== null && centre[k] !== null && Math.abs(height[k]! - centre[k]!) > BED.off && (s.under || s.pin !== null))
  const uneven = line.map((s, k) => {
    const c = centre[k]
    if (c === null || s.under) return false
    return [1, -1].some(side => {
      const g = at([s.p[0] + s.across[0] * s.half * side, s.p[1] + s.across[1] * s.half * side])
      return g !== null && Math.abs(g - c) > BED.uneven
    })
  })
  const steps = (m: number) => Math.round(m / BED.step)
  const wanted = line.map((_, k) => off[k] || uneven[k])
  const marked = wanted.map((_, i) => wanted.slice(Math.max(0, i - steps(BED.pad)), i + steps(BED.pad) + 1).some(Boolean))
  // Close the short gaps between marked runs, so a bed is not cut in two by a sample that happened to read level.
  for (let i = 0, last = -1; i < marked.length; i++) {
    if (!marked[i]) continue
    if (last >= 0 && i - last - 1 <= steps(BED.gap)) for (let k = last + 1; k < i; k++) marked[k] = true
    last = i
  }
  const out: Bed[] = []
  for (let i = 0; i < line.length; ) {
    if (!marked[i]) { i++; continue }
    let j = i
    while (j + 1 < line.length && marked[j + 1]) j++
    if ((j - i) * BED.step >= BED.min && height.slice(i, j + 1).every(h => h !== null)) {
      const level = off.slice(i, j + 1).some(Boolean)
      // One bed a way, joined where the ways meet.
      for (let a = i; a < j; ) {
        let b = a
        while (b < j && line[b + 1].way === line[a + 1].way) b++
        const way = line[Math.min(a + 1, j)].way
        if (b > a) out.push({ way, points: line.slice(a, b + 1).map(s => s.p), floor: height.slice(a, b + 1) as number[], level })
        a = b
      }
    }
    i = j + 1
  }
  return out
}
