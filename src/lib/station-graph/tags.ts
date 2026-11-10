import type { Tags } from './types'

/** Every number in a tag value: "-1;-2" → [-1, -2], "0-2" → [0, 2]. */
export function nums(value: string | undefined): number[] {
  const out: number[] = []
  for (const raw of (value ?? '').split(/[;,]/)) {
    const tok = raw.trim()
    const range = tok.match(/^(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)$/)
    if (range) {
      out.push(Number(range[1]), Number(range[2]))
      continue
    }
    if (tok !== '' && Number.isFinite(Number(tok))) out.push(Number(tok))
  }
  return out
}

/** The levels a way is on. A range ("-2--1") counts every whole level in it. */
export function levelsOf(tags: Tags): Set<number> {
  const raw = tags.level ?? ''
  const v = nums(raw)
  if (/\d-[-]?\d/.test(raw) && v.length === 2) {
    const out = new Set<number>()
    for (let l = Math.ceil(Math.min(...v)); l <= Math.max(...v); l++) out.add(l)
    return out
  }
  return new Set(v)
}

const yes = (v: string | undefined) => v !== undefined && v !== 'no'

export function isPlatform(t: Tags): boolean {
  if (t.bus === 'yes' || t.tram === 'yes' || t.trolleybus === 'yes' || t.highway === 'bus_stop') return false
  if (t.railway === 'platform') return true
  return t.public_transport === 'platform' && t.highway !== 'platform'
    && (t.subway === 'yes' || t.train === 'yes' || t.light_rail === 'yes')
}

export function isVertical(t: Tags): boolean {
  return t.highway === 'steps' || t.highway === 'elevator' || t.conveying !== undefined
}

export type WayClass = 'platform' | 'vertical' | 'corridor' | 'footway'

export function wayClass(t: Tags): WayClass | null {
  if (isPlatform(t)) return 'platform'
  if (isVertical(t)) return 'vertical'
  if (t.highway === 'corridor' || ['corridor', 'area', 'room'].includes(t.indoor ?? '')) return 'corridor'
  if (['footway', 'path', 'pedestrian'].includes(t.highway ?? '')) return 'footway'
  return null
}

/** Whether a way belongs to the station rather than the street around it. */
export function inStation(t: Tags, cls: WayClass): boolean {
  if (cls !== 'footway') return true
  if (t.footway === 'sidewalk' || t.footway === 'crossing') return false
  if (nums(t.level).some((x) => x !== 0)) return true
  if (yes(t.indoor) || yes(t.tunnel)) return true
  if (nums(t.layer).some((x) => x !== 0)) return true
  if (t.location === 'underground' || t.location === 'indoor') return true
  return t.subway === 'yes' || t.railway !== undefined
}

export function isUnderground(t: Tags): boolean {
  return [...nums(t.level), ...nums(t.layer)].some((x) => x < 0)
    || t.location === 'underground' || yes(t.tunnel)
}

export function isElevated(t: Tags): boolean {
  return [...nums(t.layer), ...nums(t.level)].some((x) => x > 0)
    || yes(t.bridge) || ['elevated', 'overground', 'roof'].includes(t.location ?? '')
}

/** A platform's level: `level`, then a negative `layer`, then what the platform type implies. */
export function levelOf(t: Tags): number | null {
  const level = nums(t.level)
  if (level.length) return Math.min(...level)
  const layer = nums(t.layer)
  if (layer.length && Math.min(...layer) < 0) return Math.min(...layer)
  if (isUnderground(t)) return -1
  if (isElevated(t)) return 1
  return null
}

export function isEntrance(t: Tags): boolean {
  return t.railway === 'subway_entrance' || t.railway === 'train_station_entrance'
}

export function isFareGate(t: Tags): boolean {
  if (t.barrier === 'turnstile' || t.barrier === 'full-height_turnstile') return true
  return t.barrier === 'gate' && yes(t.fee)
}

/** Lines a platform serves: `route_ref`, else the "(1,2,3)" NYC-style suffix of its name. */
export function platformRefs(t: Tags): string[] {
  const raw = t.route_ref ?? t.name?.match(/\(([^()]+)\)\s*$/)?.[1] ?? ''
  return [...new Set(raw.split(/[;,/]/).map((s) => s.trim()).filter(Boolean))]
}

export function gtfsStopIdsOf(t: Tags): string[] {
  return (t['gtfs:stop_id'] ?? t['ref:gtfs'] ?? '').split(';').map((s) => s.trim()).filter(Boolean)
}
