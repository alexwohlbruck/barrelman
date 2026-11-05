import { LocalProjection, pointWayDistance, rings, isArea, type Pt } from './geometry'
import type { Geometry } from './types'

export interface StationElement {
  id: string
  name: string | null
  lon: number
  lat: number
}

export interface Complex {
  /** The smallest member id: a station id clients already hold. */
  id: string
  name: string | null
  members: StationElement[]
  lon: number
  lat: number
}

const M_PER_DEG = 111320

/** Station elements within `radiusM` of one another, chained, form one complex. */
export function clusterStations(elements: StationElement[], radiusM: number): Complex[] {
  const cell = (e: StationElement) => {
    const kx = Math.cos((e.lat * Math.PI) / 180) * M_PER_DEG
    return [Math.floor((e.lon * kx) / radiusM), Math.floor((e.lat * M_PER_DEG) / radiusM)]
  }
  const parent = elements.map((_, i) => i)
  const find = (i: number): number => (parent[i] === i ? i : (parent[i] = find(parent[i])))
  const grid = new Map<string, number[]>()
  elements.forEach((e, i) => {
    const [x, y] = cell(e)
    const key = `${x}:${y}`
    grid.set(key, [...(grid.get(key) ?? []), i])
  })
  elements.forEach((e, i) => {
    const [x, y] = cell(e)
    const kx = Math.cos((e.lat * Math.PI) / 180) * M_PER_DEG
    for (let dx = -1; dx <= 1; dx++) {
      for (let dy = -1; dy <= 1; dy++) {
        for (const j of grid.get(`${x + dx}:${y + dy}`) ?? []) {
          if (j <= i) continue
          const o = elements[j]
          if (Math.hypot((o.lon - e.lon) * kx, (o.lat - e.lat) * M_PER_DEG) <= radiusM) parent[find(i)] = find(j)
        }
      }
    }
  })
  const groups = new Map<number, StationElement[]>()
  elements.forEach((e, i) => groups.set(find(i), [...(groups.get(find(i)) ?? []), e]))
  return [...groups.values()].map((members) => {
    const sorted = [...members].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
    const names = new Map<string, number>()
    for (const m of members) if (m.name) names.set(m.name, (names.get(m.name) ?? 0) + 1)
    return {
      id: sorted[0].id,
      name: [...names].sort((a, b) => b[1] - a[1])[0]?.[0] ?? null,
      members: sorted,
      lon: members.reduce((s, m) => s + m.lon, 0) / members.length,
      lat: members.reduce((s, m) => s + m.lat, 0) / members.length,
    }
  })
}

/**
 * The complex each item belongs to: the one with a member station nearest
 * to it, within `maxM`. Items are platforms (their geometry is measured, so a
 * long platform belongs to the station drawn at its end) or entrance points.
 */
export function assignToComplexes<T extends { geometry: Geometry } | { lon: number; lat: number }>(
  items: T[],
  complexes: Complex[],
  maxM: number,
): Map<T, Complex> {
  const out = new Map<T, Complex>()
  if (!items.length || !complexes.length) return out
  const proj = new LocalProjection(complexes[0].lon, complexes[0].lat)
  const members = complexes.flatMap((c) => c.members.map((m) => ({ c, at: proj.project(m.lon, m.lat) })))
  for (const item of items) {
    let best: { d: number; c: Complex } | null = null
    if ('geometry' in item) {
      const pm = rings(item.geometry).map((r) => r.map(([x, y]) => proj.project(x, y)))
      const box = pm.flat().reduce((b, p) => [Math.min(b[0], p[0]), Math.min(b[1], p[1]), Math.max(b[2], p[0]), Math.max(b[3], p[1])], [Infinity, Infinity, -Infinity, -Infinity])
      for (const m of members) {
        if (m.at[0] < box[0] - maxM || m.at[0] > box[2] + maxM || m.at[1] < box[1] - maxM || m.at[1] > box[3] + maxM) continue
        const d = pointWayDistance(m.at, pm, isArea(item.geometry)).d
        if (d <= maxM && (!best || d < best.d)) best = { d, c: m.c }
      }
    } else {
      const at: Pt = proj.project(item.lon, item.lat)
      for (const m of members) {
        const d = Math.hypot(at[0] - m.at[0], at[1] - m.at[1])
        if (d <= maxM && (!best || d < best.d)) best = { d, c: m.c }
      }
    }
    if (best) out.set(item, best.c)
  }
  return out
}

/** A lon/lat box around points, grown by `marginM`. */
export function boxAround(points: Array<{ lon: number; lat: number }>, marginM: number): [number, number, number, number] {
  const lat = points.reduce((s, p) => s + p.lat, 0) / points.length
  const dx = marginM / (Math.cos((lat * Math.PI) / 180) * M_PER_DEG)
  const dy = marginM / M_PER_DEG
  return [
    Math.min(...points.map((p) => p.lon)) - dx, Math.min(...points.map((p) => p.lat)) - dy,
    Math.max(...points.map((p) => p.lon)) + dx, Math.max(...points.map((p) => p.lat)) + dy,
  ]
}

export function geometryBox(g: Geometry): [number, number, number, number] {
  const pts = rings(g).flat()
  return [Math.min(...pts.map((p) => p[0])), Math.min(...pts.map((p) => p[1])), Math.max(...pts.map((p) => p[0])), Math.max(...pts.map((p) => p[1]))]
}

export const boxesTouch = (a: number[], b: number[]) => a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]
