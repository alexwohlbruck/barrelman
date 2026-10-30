/**
 * The stretches of a route inside areas — states, parks — from how each area
 * meets the route's pieces: a piece it covers whole, or the parts of a piece
 * inside it, as the database cut them.
 */
import { haversine, type LonLat, type RoutePiece } from '../route-hazards/route-path'

export interface AreaHit {
  /** The area, `relation/123`. */
  id: string
  name?: string
  code?: string
  piece: number
  covers: boolean
  /** The piece's lines inside the area, where it doesn't cover the piece. */
  parts?: LonLat[][]
}

export interface AreaStretch {
  startDistance: number
  endDistance: number
  id: string
  name?: string
  code?: string
}

/** Metres along the piece of the point on it nearest `point`. */
function along(piece: RoutePiece, point: LonLat): number {
  const line = piece.coordinates
  let best = { offset: Infinity, distance: piece.startDistance }
  let run = piece.startDistance
  for (let i = 0; i < line.length - 1; i++) {
    const span = haversine(line[i], line[i + 1])
    const [ax, ay] = line[i]
    const [dx, dy] = [line[i + 1][0] - ax, line[i + 1][1] - ay]
    const len2 = dx * dx + dy * dy
    const t = len2 > 0 ? Math.max(0, Math.min(1, ((point[0] - ax) * dx + (point[1] - ay) * dy) / len2)) : 0
    const offset = haversine([ax + dx * t, ay + dy * t], point)
    if (offset < best.offset) best = { offset, distance: run + span * t }
    run += span
  }
  return best.distance
}

/**
 * Each area's stretches along the route, in route order. Stretches of one
 * area closer than `joinMeters` merge; shorter than `minMeters` are dropped.
 */
export function areaStretches(pieces: RoutePiece[], hits: AreaHit[], { joinMeters = 1, minMeters = 0 } = {}): AreaStretch[] {
  const raw: AreaStretch[] = []
  for (const hit of hits) {
    const piece = pieces[hit.piece]
    if (!piece) continue
    const label = { id: hit.id, ...(hit.name && { name: hit.name }), ...(hit.code && { code: hit.code }) }
    if (hit.covers) {
      raw.push({ startDistance: piece.startDistance, endDistance: piece.endDistance, ...label })
      continue
    }
    for (const part of hit.parts ?? []) {
      if (part.length < 2) continue
      const [a, b] = [along(piece, part[0]), along(piece, part[part.length - 1])].sort((x, y) => x - y)
      if (b > a) raw.push({ startDistance: a, endDistance: b, ...label })
    }
  }
  raw.sort((a, b) => a.startDistance - b.startDistance)

  const open = new Map<string, AreaStretch>()
  const joined: AreaStretch[] = []
  for (const s of raw) {
    const last = open.get(s.id)
    if (last && s.startDistance - last.endDistance <= joinMeters) {
      last.endDistance = Math.max(last.endDistance, s.endDistance)
      continue
    }
    const copy = { ...s }
    open.set(s.id, copy)
    joined.push(copy)
  }
  const round = (n: number) => Math.round(n * 10) / 10
  return joined
    .filter(s => s.endDistance - s.startDistance >= minMeters)
    .map(s => ({ ...s, startDistance: round(s.startDistance), endDistance: round(s.endDistance) }))
}
