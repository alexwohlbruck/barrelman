/**
 * The `elevation` feature property: runs separated by `;`, each a list of
 * integer triples. The first is lng×1e7, lat×1e7 and height in decimetres; each
 * later one the change in lng×1e7 and lat×1e7 from the point before, and that
 * point's own height. Heights between a run's points are interpolated; off any
 * run a line lies on the ground.
 */
import { unprojected, type Point } from '../bridge-mesh/decks'
import type { Sample } from './match'

/** Metres a dropped point may stand off the line between the points kept around it. */
export const THIN_TOLERANCE = 0.2

export type RunPoint = { p: Point; d: number; height: number }

/** The stretches of a sampled line that ride something, each with the ground point either side of it to climb from. */
export function runsOf(samples: Sample[]): RunPoint[][] {
  const runs: RunPoint[][] = []
  for (let i = 0; i < samples.length; i++) {
    if (samples[i].height === null) continue
    const start = i
    while (i + 1 < samples.length && samples[i + 1].height !== null) i++
    const from = Math.max(0, start - 1), to = Math.min(samples.length - 1, i + 1)
    runs.push(samples.slice(from, to + 1).map(s => ({ p: s.p, d: s.d, height: s.height ?? 0 })))
  }
  return runs
}

/** A run with every point dropped that interpolating its kept neighbours puts within `tolerance` of its height. */
export function thin(run: RunPoint[], tolerance = THIN_TOLERANCE): RunPoint[] {
  if (run.length < 3) return run
  const keep = new Array<boolean>(run.length).fill(false)
  keep[0] = keep[run.length - 1] = true
  const stack: Array<[number, number]> = [[0, run.length - 1]]
  while (stack.length) {
    const [a, b] = stack.pop()!
    let worst = -1, off = tolerance
    for (let i = a + 1; i < b; i++) {
      const t = (run[i].d - run[a].d) / (run[b].d - run[a].d || 1)
      const error = Math.abs(run[i].height - (run[a].height + (run[b].height - run[a].height) * t))
      if (error > off) [worst, off] = [i, error]
    }
    if (worst < 0) continue
    keep[worst] = true
    stack.push([a, worst], [worst, b])
  }
  return run.filter((_, i) => keep[i])
}

/** A run as its triples, or null when it is too short or never leaves the ground. */
export function encodeRun(run: RunPoint[]): string | null {
  const triples: number[][] = []
  for (const { p, height } of run) {
    const [lng, lat] = unprojected(p)
    const point = [Math.round(lng * 1e7), Math.round(lat * 1e7), Math.round(height * 10)]
    const last = triples.at(-1)
    if (last && last[0] === point[0] && last[1] === point[1]) continue
    triples.push(point)
  }
  if (triples.length < 2 || triples.every(t => t[2] === 0)) return null
  return triples.map((t, i) => (i ? [t[0] - triples[i - 1][0], t[1] - triples[i - 1][1], t[2]] : t).join(',')).join(',')
}

/** The property for a line's sampled parts, or null when none of it leaves the ground. */
export function encodeElevation(parts: Sample[][]): string | null {
  const runs = parts.flatMap(runsOf).map(run => encodeRun(thin(run))).filter((r): r is string => r !== null)
  return runs.length ? runs.join(';') : null
}

/** The runs of a property, as [lng, lat, metres] points. */
export function decodeElevation(text: string): Array<Array<[number, number, number]>> {
  return text.split(';').map(run => {
    const n = run.split(',').map(Number)
    const out: Array<[number, number, number]> = []
    let lng = 0, lat = 0
    for (let i = 0; i + 2 < n.length; i += 3) {
      lng += n[i]
      lat += n[i + 1]
      out.push([lng / 1e7, lat / 1e7, n[i + 2] / 10])
    }
    return out
  })
}
