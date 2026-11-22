import type { PackageBounds } from '../../types/offline-package.types'

/** Largest region side, in degrees. Past this a package takes too long to build. */
export const MAX_REGION_DEGREES = 0.5

/** `w,s,e,n` as bounds, or an error message for the caller. */
export function parseBbox(raw: unknown): PackageBounds | string {
  if (typeof raw !== 'string') return 'bbox is required as w,s,e,n'
  const parts = raw.split(',').map(Number)
  if (parts.length !== 4 || parts.some(n => !Number.isFinite(n))) return 'bbox must be four numbers: w,s,e,n'
  const [w, s, e, n] = parts
  if (w < -180 || e > 180 || s < -90 || n > 90 || w >= e || s >= n) return 'bbox is out of range or inverted'
  if (e - w > MAX_REGION_DEGREES || n - s > MAX_REGION_DEGREES) {
    return `bbox sides must be at most ${MAX_REGION_DEGREES} degrees`
  }
  return [w, s, e, n]
}

/** Split bounds into cells of about `size` degrees, so each query stays well inside the statement timeout. */
export function bboxCells(bounds: PackageBounds, size: number): PackageBounds[] {
  const [w, s, e, n] = bounds
  const cols = Math.max(1, Math.ceil((e - w) / size))
  const rows = Math.max(1, Math.ceil((n - s) / size))
  const cells: PackageBounds[] = []
  for (let i = 0; i < cols; i++) {
    for (let j = 0; j < rows; j++) {
      cells.push([
        w + ((e - w) * i) / cols,
        s + ((n - s) * j) / rows,
        w + ((e - w) * (i + 1)) / cols,
        s + ((n - s) * (j + 1)) / rows,
      ])
    }
  }
  return cells
}

export const inBounds = ([w, s, e, n]: PackageBounds, lng: number, lat: number) =>
  lng >= w && lng <= e && lat >= s && lat <= n

export const round6 = (v: number) => Math.round(v * 1e6) / 1e6

/** Great-circle distance in metres. */
export function haversineM(lng1: number, lat1: number, lng2: number, lat2: number): number {
  const rad = Math.PI / 180
  const dLat = (lat2 - lat1) * rad
  const dLng = (lng2 - lng1) * rad
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * rad) * Math.cos(lat2 * rad) * Math.sin(dLng / 2) ** 2
  return 2 * 6_371_008.8 * Math.asin(Math.min(1, Math.sqrt(a)))
}
