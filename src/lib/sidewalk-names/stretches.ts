/**
 * A route's stretches along named sidewalks, from the zones stored per way and
 * the way each stretch of the route follows (GraphHopper's osm_way_id detail).
 */
import { cumulativeLengths, localProjection, type XY } from './zones'
import type { StoredZone } from './ways'

export interface WayNames {
  /** The way's line as the zones measure it, [lon, lat]. */
  coords: XY[]
  zones: StoredZone[]
}

export interface StreetStretch {
  startDistance: number
  endDistance: number
  name: string
  source: StoredZone['source']
}

/** Gaps this short between two stretches of one street are joined. */
const JOIN_METERS = 1

/** Where along the way, as a fraction of its length, each point lies; and the way's length in metres. */
function fractionsAlong(way: XY[], points: XY[]): { fractions: number[]; length: number } {
  const project = localProjection(way[0])
  const line = way.map(project)
  const lengths = cumulativeLengths(line)
  const total = lengths.at(-1) || 1
  const fractions = points.map(point => {
    const [px, py] = project(point)
    let best = { d2: Infinity, s: 0 }
    for (let i = 1; i < line.length; i++) {
      const [[ax, ay], [bx, by]] = [line[i - 1], line[i]]
      const [dx, dy] = [bx - ax, by - ay]
      const len2 = dx * dx + dy * dy
      const t = len2 > 0 ? Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / len2)) : 0
      const d2 = (ax + dx * t - px) ** 2 + (ay + dy * t - py) ** 2
      if (d2 < best.d2) best = { d2, s: lengths[i - 1] + (lengths[i] - lengths[i - 1]) * t }
    }
    return best.s / total
  })
  return { fractions, length: total }
}

const isClosed = (way: XY[]) => way.length > 3 && way[0][0] === way.at(-1)![0] && way[0][1] === way.at(-1)![1]

/**
 * The named stretches of a route. `coordinates` are the route's [lon, lat],
 * `distances` its cumulative metres, `wayIds` GraphHopper's [from, to, way]
 * intervals over coordinate indexes.
 */
export function streetStretches(
  coordinates: number[][],
  distances: number[],
  wayIds: Array<[number, number, number]>,
  ways: Map<number, WayNames>,
): StreetStretch[] {
  const pieces: StreetStretch[] = []
  for (const [from, to, id] of wayIds) {
    const way = ways.get(id)
    if (!way || way.coords.length < 2 || to <= from) continue
    const { fractions: f, length } = fractionsAlong(way.coords, coordinates.slice(from, to + 1).map(c => [c[0], c[1]] as XY))
    const closed = isClosed(way.coords)
    const along = (fa: number, fb: number, da: number, db: number) => {
      for (const zone of way.zones) {
        const lo = Math.max(Math.min(fa, fb), zone.from)
        const hi = Math.min(Math.max(fa, fb), zone.to)
        if (hi <= lo) continue
        const at = (x: number) => da + ((x - fa) / (fb - fa)) * (db - da)
        const [x, y] = [at(lo), at(hi)].sort((p, q) => p - q)
        pieces.push({ startDistance: x, endDistance: y, name: zone.name, source: zone.source })
      }
    }
    for (let i = 0; i < f.length - 1; i++) {
      const [fa, fb] = [f[i], f[i + 1]]
      const [da, db] = [distances[from + i], distances[from + i + 1]]
      if (fa === fb) continue
      const span = Math.abs(fb - fa)
      // A step across the seam of a closed way goes the short way round: on to its end, then on from its start.
      if (closed && Math.abs((1 - span) * length - (db - da)) < Math.abs(span * length - (db - da))) {
        const [end, start] = fa > fb ? [1, 0] : [0, 1]
        const seam = da + (Math.abs(end - fa) / (1 - span)) * (db - da)
        along(fa, end, da, seam)
        along(start, fb, seam, db)
      } else along(fa, fb, da, db)
    }
  }
  pieces.sort((a, b) => a.startDistance - b.startDistance)
  const joined: StreetStretch[] = []
  for (const piece of pieces) {
    const last = joined.at(-1)
    if (last && last.name === piece.name && last.source === piece.source && piece.startDistance - last.endDistance <= JOIN_METERS)
      last.endDistance = Math.max(last.endDistance, piece.endDistance)
    else joined.push({ ...piece })
  }
  const round = (d: number) => Math.round(d * 10) / 10
  return joined.map(s => ({ ...s, startDistance: round(s.startDistance), endDistance: round(s.endDistance) }))
}

export interface NameableSegment {
  startDistance: number
  endDistance: number
  streetName?: string
  streetNameSource?: StoredZone['source']
}

/** Splits segments where a named stretch starts or ends inside one, and names the parts it covers. */
export function nameSegments<T extends NameableSegment>(segments: T[], stretches: StreetStretch[]): T[] {
  if (!stretches.length) return segments
  const out: T[] = []
  for (const segment of segments) {
    const cuts = [segment.startDistance, segment.endDistance]
    for (const s of stretches)
      for (const d of [s.startDistance, s.endDistance]) if (d > segment.startDistance && d < segment.endDistance) cuts.push(d)
    const sorted = [...new Set(cuts)].sort((a, b) => a - b)
    for (let i = 0; i < sorted.length - 1; i++) {
      const [startDistance, endDistance] = [sorted[i], sorted[i + 1]]
      const mid = (startDistance + endDistance) / 2
      const stretch = stretches.find(s => s.startDistance <= mid && mid <= s.endDistance)
      out.push({
        ...segment,
        startDistance,
        endDistance,
        ...(stretch && { streetName: stretch.name, streetNameSource: stretch.source }),
      })
    }
  }
  return out
}
