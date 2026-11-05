import type { Geometry } from './types'

export type Pt = [number, number]

/** Coordinate rings of any line or polygon geometry, holes included. */
export function rings(g: Geometry): number[][][] {
  switch (g.type) {
    case 'LineString': return [g.coordinates]
    case 'MultiLineString':
    case 'Polygon': return g.coordinates
    case 'MultiPolygon': return g.coordinates.flat()
  }
}

export const isArea = (g: Geometry) => g.type === 'Polygon' || g.type === 'MultiPolygon'

/** A shared OSM node, approximated by an identical coordinate: barrelman keeps geometry, not node lists. */
export const vertexKey = (lon: number, lat: number) => `${Math.round(lon * 1e7)}:${Math.round(lat * 1e7)}`

/** Equirectangular metres around a complex: plenty for a few hundred metres. */
export class LocalProjection {
  readonly kx: number
  constructor(readonly lon0: number, readonly lat0: number) {
    this.kx = Math.cos((lat0 * Math.PI) / 180) * 111320
  }
  project(lon: number, lat: number): Pt {
    return [(lon - this.lon0) * this.kx, (lat - this.lat0) * 110540]
  }
  unproject([x, y]: Pt): [number, number] {
    return [Math.round((x / this.kx + this.lon0) * 1e7) / 1e7, Math.round((y / 110540 + this.lat0) * 1e7) / 1e7]
  }
}

export const dist = (a: Pt, b: Pt) => Math.hypot(a[0] - b[0], a[1] - b[1])

/** Distance from p to segment ab, and where along it (0–1) the nearest point is. */
export function segmentDistance(p: Pt, a: Pt, b: Pt): { d: number; t: number } {
  const dx = b[0] - a[0]
  const dy = b[1] - a[1]
  const len2 = dx * dx + dy * dy
  const t = len2 === 0 ? 0 : Math.max(0, Math.min(1, ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / len2))
  return { d: Math.hypot(p[0] - a[0] - t * dx, p[1] - a[1] - t * dy), t }
}

export function pointInRing(p: Pt, ring: Pt[]): boolean {
  let inside = false
  for (let i = 0; i + 1 < ring.length; i++) {
    const [x1, y1] = ring[i]
    const [x2, y2] = ring[i + 1]
    if ((y1 > p[1]) !== (y2 > p[1]) && p[0] < ((x2 - x1) * (p[1] - y1)) / (y2 - y1) + x1) inside = !inside
  }
  return inside
}

/** Nearest segment of a projected way to p; distance 0 inside an area. */
export function pointWayDistance(p: Pt, ringsM: Pt[][], area: boolean): { d: number; ring: number; seg: number } {
  let best = { d: Infinity, ring: -1, seg: -1 }
  ringsM.forEach((r, ri) => {
    for (let i = 0; i + 1 < r.length; i++) {
      const { d } = segmentDistance(p, r[i], r[i + 1])
      if (d < best.d) best = { d, ring: ri, seg: i }
    }
    if (r.length === 1 && dist(p, r[0]) < best.d) best = { d: dist(p, r[0]), ring: ri, seg: -1 }
  })
  if (area && ringsM.some((r) => pointInRing(p, r))) best = { ...best, d: 0 }
  return best
}

/** Smallest distance between two projected ways (0 where they touch or overlap). */
export function wayDistance(a: { pm: Pt[][]; area: boolean }, b: { pm: Pt[][]; area: boolean }): number {
  let best = Infinity
  for (const p of a.pm.flat()) best = Math.min(best, pointWayDistance(p, b.pm, b.area).d)
  for (const p of b.pm.flat()) best = Math.min(best, pointWayDistance(p, a.pm, a.area).d)
  return best
}

export function centroidOf(points: Pt[]): Pt {
  const n = points.length || 1
  return [points.reduce((s, p) => s + p[0], 0) / n, points.reduce((s, p) => s + p[1], 0) / n]
}

/** Long axis by PCA: unit direction and the extent of the points along it from the centroid. */
export function principalAxis(points: Pt[]): { centre: Pt; dir: Pt; min: number; max: number } {
  const centre = centroidOf(points)
  let sxx = 0, syy = 0, sxy = 0
  for (const [x, y] of points) {
    const dx = x - centre[0]
    const dy = y - centre[1]
    sxx += dx * dx; syy += dy * dy; sxy += dx * dy
  }
  const angle = 0.5 * Math.atan2(2 * sxy, sxx - syy)
  let dir: Pt = [Math.cos(angle), Math.sin(angle)]
  // A stable orientation: the axis always points into the northern or, if exactly east-west, eastern half.
  if (dir[1] < 0 || (dir[1] === 0 && dir[0] < 0)) dir = [-dir[0], -dir[1]]
  let min = 0, max = 0
  for (const p of points) {
    const t = (p[0] - centre[0]) * dir[0] + (p[1] - centre[1]) * dir[1]
    min = Math.min(min, t); max = Math.max(max, t)
  }
  return { centre, dir, min, max }
}

/** Compass bearing of a projected direction. */
export const bearingOf = (dir: Pt) => ((Math.atan2(dir[0], dir[1]) * 180) / Math.PI + 360) % 360
