/**
 * The `elevation` of each cycle way on a deck or in a tunnel, computed over
 * its whole length once at import rather than per tile.
 */
import type { Bounds, Point } from '../bridge-mesh/decks'
import { encodeElevation } from './encode'
import { sampleLine, SourceIndex } from './match'
import type { HeightSource } from './sources'
import { SOURCE_ZOOM } from './pyramid'

export type CycleWay = { id: number; points: Point[] }

/** Ways batched by the tile their first point lies in, so neighbours share one read of their sources. */
export function batchByTile(ways: CycleWay[], z = SOURCE_ZOOM): CycleWay[][] {
  const n = 2 ** z
  const batches = new Map<string, CycleWay[]>()
  for (const way of ways) {
    const [x, y] = way.points[0]
    const key = `${Math.floor(x * n)}/${Math.floor(y * n)}`
    batches.set(key, [...(batches.get(key) ?? []), way])
  }
  return [...batches.values()]
}

export function boundsOf(ways: CycleWay[]): Bounds {
  const points = ways.flatMap(w => w.points)
  const xs = points.map(p => p[0]), ys = points.map(p => p[1])
  return { minX: Math.min(...xs), minY: Math.min(...ys), maxX: Math.max(...xs), maxY: Math.max(...ys) }
}

/** Each way's `elevation`, or null for one on the ground along its whole length. A cycle way rides only its own way's decks and tunnels. */
export function wayElevations(ways: CycleWay[], sources: HeightSource[]): Array<string | null> {
  const index = new SourceIndex(sources)
  return ways.map(way => encodeElevation([sampleLine(way.points, index, { way: way.id })]))
}
