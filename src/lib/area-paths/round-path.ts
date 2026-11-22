/**
 * A GraphHopper path with its bends inside pedestrian areas rounded: its
 * points, the point indices its details and instructions refer to, and its
 * distances and times, so the ETA is the walk along the curve.
 */
import { haversine, type LonLat } from '../route-hazards/route-path'
import { roundBends, type Space } from './bends'

/** Ways from here up are paths the graph adds across pedestrian areas; see import/synthesize-area-paths.py. */
export const AREA_PATH_WAY_ID_BASE = 2_100_000_000

type Detail = [number, number, ...unknown[]]

function valueAt(entries: unknown[] | undefined, line: number): unknown {
  for (const e of entries ?? []) {
    if (Array.isArray(e) && e[0] <= line && line < e[1]) return e[2]
  }
  return undefined
}

/** The area-path ways a GraphHopper path takes. */
export function areaPathWays(path: any): number[] {
  const ids = (path?.details?.osm_way_id ?? [])
    .map((e: unknown[]) => e?.[2])
    .filter((id: unknown): id is number => typeof id === 'number' && id >= AREA_PATH_WAY_ID_BASE)
  return [...new Set<number>(ids)]
}

const isAreaPath = (id: unknown) => typeof id === 'number' && id >= AREA_PATH_WAY_ID_BASE
// GraphHopper signs for reaching a waypoint and the destination, which always stay.
const FINISH = 4
const VIA = 5

/**
 * The path with no maneuvers at the corners inside a pedestrian area: crossing
 * one is a single step, from the way in to the way out, not a turn at every
 * corner the graph happens to bend at.
 */
export function foldAreaManeuvers(path: any): any {
  const ways = path?.details?.osm_way_id
  if (!Array.isArray(path?.instructions) || !ways) return path
  const folded: any[] = []
  for (const ins of path.instructions) {
    const from = ins.interval?.[0]
    const prev = folded[folded.length - 1]
    const inside = typeof from === 'number' && isAreaPath(valueAt(ways, from - 1)) && isAreaPath(valueAt(ways, from))
    if (prev && inside && ins.sign !== FINISH && ins.sign !== VIA) {
      folded[folded.length - 1] = {
        ...prev,
        ...(!prev.street_name && ins.street_name && { street_name: ins.street_name }),
        interval: [prev.interval[0], ins.interval[1]],
        distance: prev.distance + ins.distance,
        time: prev.time + ins.time,
      }
      continue
    }
    folded.push(ins)
  }
  return folded.length === path.instructions.length ? path : { ...path, instructions: folded }
}

function lengthOf(coordinates: number[][], from: number, to: number): number {
  let total = 0
  for (let i = from; i < to; i++) total += haversine(coordinates[i] as LonLat, coordinates[i + 1] as LonLat)
  return total
}

export function roundPath(path: any, spaces: Map<number, Space>): any {
  const coordinates: number[][] = path?.points?.coordinates
  if (!Array.isArray(coordinates) || coordinates.length < 3 || !spaces.size) return path
  const ways = path.details?.osm_way_id
  const fixed = new Set<number>()
  const waypoints: number[][] = path.snapped_waypoints?.coordinates ?? []
  coordinates.forEach((c, i) => {
    if (waypoints.some(w => Math.abs(w[0] - c[0]) < 1e-7 && Math.abs(w[1] - c[1]) < 1e-7)) fixed.add(i)
  })

  const { coordinates: rounded, indexOf } = roundBends(coordinates, line => {
    const id = valueAt(ways, line)
    return typeof id === 'number' ? spaces.get(id) : undefined
  }, fixed)
  if (rounded === coordinates) return path

  const fallbackSpeed = path.time > 0 ? path.distance / (path.time / 1000) : 1.4
  const lines = coordinates.length - 1
  const extraMetres = new Float64Array(lines)
  const extraMs = new Float64Array(lines)
  for (let s = 0; s < lines; s++) {
    if (indexOf[s + 1] - indexOf[s] === 1 && rounded[indexOf[s]] === coordinates[s] && rounded[indexOf[s + 1]] === coordinates[s + 1]) continue
    const kmh = Number(valueAt(path.details?.average_speed, s))
    const speed = kmh > 0 ? kmh / 3.6 : fallbackSpeed
    extraMetres[s] = lengthOf(rounded, indexOf[s], indexOf[s + 1]) - lengthOf(coordinates, s, s + 1)
    extraMs[s] = (extraMetres[s] / speed) * 1000
  }
  const sum = (values: Float64Array, from: number, to: number) => {
    let total = 0
    for (let s = from; s < to; s++) total += values[s]
    return total
  }

  const details = Object.fromEntries(Object.entries(path.details ?? {}).map(([key, entries]) => [
    key,
    (entries as Detail[]).map(([from, to, ...rest]) => [indexOf[from], indexOf[to], ...rest]),
  ]))
  const instructions = Array.isArray(path.instructions)
    ? path.instructions.map((ins: any) => {
      if (!Array.isArray(ins.interval)) return ins
      const [from, to] = ins.interval
      return {
        ...ins,
        interval: [indexOf[from], indexOf[to]],
        distance: Math.max(0, ins.distance + sum(extraMetres, from, to)),
        time: Math.max(0, Math.round(ins.time + sum(extraMs, from, to))),
      }
    })
    : path.instructions

  return {
    ...path,
    points: { ...path.points, coordinates: rounded },
    details,
    instructions,
    distance: path.distance + sum(extraMetres, 0, lines),
    time: Math.round(path.time + sum(extraMs, 0, lines)),
  }
}
