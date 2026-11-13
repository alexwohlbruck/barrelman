/**
 * Road beds: the stretches of a road on the ground where the terrain is not
 * level across its carriageway. A DEM blurs the walls of an embankment or a
 * cutting into slopes a few metres wide, and a wide road's outer lanes, draped
 * on them, lean and wobble. A bed holds the terrain under the carriageway at
 * its centreline's height, which a map levels the ground to.
 */
import { densify, metresPerUnit, type Point } from '../bridge-mesh/decks'

export const BED = {
  /** Metres between the samples along a road. */
  step: 4,
  /** Metres the ground at an edge may lie off the centreline's before the road is not level across. */
  uneven: 0.6,
  /** Metres a bed runs on past its uneven samples, and the shortest gap between two that is not bridged. */
  pad: 8,
  gap: 16,
  /** Shortest bed kept, in metres. */
  min: 12,
}

export type Bed = { points: Point[]; floor: number[] }

/** Samples each side a floor is averaged over. */
const SMOOTH = 2

/**
 * The beds of a road `half` metres wide each side of its centreline, from the
 * ground `at` each point (null where unknown): its centreline's height,
 * smoothed, where an edge lies BED.uneven off it.
 */
export function beds(points: Point[], half: number, at: (p: Point) => number | null): Bed[] {
  const line = densify(points, BED.step)
  if (line.length < 2) return []
  const centre = line.map(at)
  const uneven = line.map((p, i) => {
    const c = centre[i]
    if (c === null) return false
    const [a, b] = [line[Math.max(0, i - 1)], line[Math.min(line.length - 1, i + 1)]]
    const length = Math.hypot(b[0] - a[0], b[1] - a[1])
    if (!length) return false
    const reach = half / metresPerUnit(p[1]) / length
    const across: Point = [-(b[1] - a[1]) * reach, (b[0] - a[0]) * reach]
    return [1, -1].some(s => {
      const g = at([p[0] + across[0] * s, p[1] + across[1] * s])
      return g !== null && Math.abs(g - c) > BED.uneven
    })
  })
  const steps = (m: number) => Math.round(m / BED.step)
  const marked = uneven.map((_, i) => uneven.slice(Math.max(0, i - steps(BED.pad)), i + steps(BED.pad) + 1).some(Boolean))
  // Close the short gaps between marked runs, so a bed is not cut in two by a sample that happened to read level.
  for (let i = 0, last = -1; i < marked.length; i++) {
    if (!marked[i]) continue
    if (last >= 0 && i - last - 1 <= steps(BED.gap)) for (let k = last + 1; k < i; k++) marked[k] = true
    last = i
  }
  const out: Bed[] = []
  for (let i = 0; i < line.length;) {
    if (!marked[i]) { i++; continue }
    let j = i
    while (j + 1 < line.length && marked[j + 1]) j++
    const run = line.slice(i, j + 1)
    const heights = centre.slice(i, j + 1)
    if ((j - i) * BED.step >= BED.min && heights.every(h => h !== null)) {
      const h = heights as number[]
      const median = h.map((v, k) => [h[Math.max(0, k - 1)], v, h[Math.min(h.length - 1, k + 1)]].sort((x, y) => x - y)[1])
      // Evened over SMOOTH samples too: the terrain's noise along a road would otherwise ripple its paint.
      const floor = median.map((_, k) => {
        const near = median.slice(Math.max(0, k - SMOOTH), k + SMOOTH + 1)
        return near.reduce((a, b) => a + b, 0) / near.length
      })
      out.push({ points: run, floor })
    }
    i = j + 1
  }
  return out
}
