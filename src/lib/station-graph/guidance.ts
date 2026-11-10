/**
 * What a transit plan tells a rider about the stations it passes through:
 * how each transfer is made, which entrance and exit to use, where to stand on
 * the platform, and the walk inside the station step by step. Pure: the
 * service feeds it station_paths rows and agency facts.
 */
import { DEFAULT_PARAMS } from './params'
import { SOURCES, type PathSource, type PathStep, type StationPath, type StepType } from './types'

/** Seconds to pass a fare gate once, tapping in or out. */
export const FARE_GATE_DELAY_S = 10

/**
 * How much longer a trip may take before a saved fare stops being worth it.
 *
 * A rider asked for directions, not for the cheapest possible routing, so a
 * one-fare trip that takes half an hour longer is not the answer to their
 * question. Ten minutes is about where the trade turns for a city fare:
 * below it, most riders take the cheaper trip; above it, they do not.
 */
export const EXTRA_FARE_S = 10 * 60

/** Straight line to street walk, for ranking entrances before the street router has a say. */
export const STREET_DETOUR = 1.3

/** A boarding hint is only worth giving when the platform's far end costs this much more. */
export const BOARDING_HINT_MIN_S = 45

/** A paid walk at least this long between differently named stations is a passage, not a change. */
export const PASSAGE_MIN_M = 150

const WALKED: ReadonlySet<PathSource> = new Set(['graph', 'bridged'])
export const isWalked = (source: PathSource) => WALKED.has(source)

export type TransferClass = 'same_platform' | 'in_station' | 'in_system_passage' | 'out_of_system'

export interface TransferFacts {
  /** The alighting and boarding stops share an OSM platform. */
  samePlatform: boolean
  /** The path between the two platforms, when both are in one complex. */
  row: StationPath | null
  /** Whether the agency joins the boarded line to the alighting station without leaving the paid area; null unknown. */
  inSystem: boolean | null
  /** The platforms belong to differently named stations. */
  namedApart: boolean
  /** Whether one fare covers both rides (MOTIS fare grouping); null when the feeds publish no fares. */
  oneFare: boolean | null
}

export interface TransferClassification {
  class: TransferClass
  /** The walk goes up to street level. Null when only an estimate is known. */
  surfaces: boolean | null
  /** For out-of-system transfers: whether re-entering is free. Null when unknown or not applicable. */
  reentryFree: boolean | null
  /** Time added for tapping out and back in. */
  fareGateDelayS: number
}

/**
 * The kind of change a transfer is. A walked path that crosses a fare gate,
 * or a line the agency does not join to the station, means leaving fare
 * control. With no path in one complex, only the agency's word keeps a
 * transfer inside.
 */
export function classifyTransfer(f: TransferFacts): TransferClassification {
  const inside = (cls: TransferClass, surfaces: boolean | null): TransferClassification =>
    ({ class: cls, surfaces, reentryFree: null, fareGateDelayS: 0 })
  const outside = (surfaces: boolean | null): TransferClassification =>
    ({ class: 'out_of_system', surfaces, reentryFree: f.oneFare, fareGateDelayS: 2 * FARE_GATE_DELAY_S })

  if (f.samePlatform) return inside('same_platform', false)
  const walked = f.row && isWalked(f.row.source)
  if (f.inSystem === false || (walked && f.row!.crossesFareGate)) return outside(f.row ? f.row.surfaces : true)
  if (!f.row) return f.inSystem ? inside('in_system_passage', null) : outside(true)
  const passage = f.namedApart && f.row.metres >= PASSAGE_MIN_M
  return inside(passage ? 'in_system_passage' : 'in_station', walked ? f.row.surfaces : null)
}

/** The best source present, and among its rows the one nearest their median time. */
export function representativeRow<T extends Pick<StationPath, 'source' | 'seconds'>>(rows: T[]): T | null {
  if (!rows.length) return null
  const best = Math.min(...rows.map((r) => SOURCES.indexOf(r.source)))
  const tier = rows.filter((r) => SOURCES.indexOf(r.source) === best).sort((a, b) => a.seconds - b.seconds)
  return tier[(tier.length - 1) >> 1]
}

export interface EntranceCandidate {
  id: string
  lat: number
  lng: number
  row: StationPath
}

export interface EntranceChoice<C extends EntranceCandidate> {
  candidate: C
  streetSeconds: number
  totalSeconds: number
}

const toRad = (d: number) => (d * Math.PI) / 180

export function haversineM(a: { lat: number; lng: number }, b: { lat: number; lng: number }): number {
  const dLat = toRad(b.lat - a.lat), dLng = toRad(b.lng - a.lng)
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2
  return 2 * 6371008.8 * Math.asin(Math.sqrt(h))
}

/**
 * The entrance (or exit) that makes the whole walk shortest: the street walk
 * to it, estimated from the straight line, plus the station path through it.
 */
export function chooseEntrance<C extends EntranceCandidate>(
  street: { lat: number; lng: number },
  candidates: C[],
  walkMps: number,
): EntranceChoice<C> | null {
  let best: EntranceChoice<C> | null = null
  for (const candidate of candidates) {
    const streetSeconds = Math.round((haversineM(street, candidate) * STREET_DETOUR) / walkMps)
    const totalSeconds = streetSeconds + candidate.row.seconds
    if (!best || totalSeconds < best.totalSeconds) best = { candidate, streetSeconds, totalSeconds }
  }
  return best
}

/** Modes that stop at platforms inside a station; a bus, tram or ferry stops at the kerb or a pier. */
const KERBSIDE_MODES = new Set(['BUS', 'COACH', 'TRAM', 'FERRY', 'AIRPLANE', 'AERIAL_LIFT', 'ODM'])

export interface RideLeg {
  transitLeg: boolean
  mode: string
  from: { lat: number; lng: number; stopId?: string }
  to: { lat: number; lng: number; stopId?: string }
  routeShortName?: string
  routeLongName?: string
  directionLabel?: string
}

export const atStation = (leg: RideLeg) => leg.transitLeg && !KERBSIDE_MODES.has(leg.mode)

/** What the station signs call the line: rail feeds put the train number in the short name. */
export function routeSign(leg: RideLeg): string | null {
  const line = leg.routeLongName && /^\d{3,}$/.test(leg.routeShortName ?? '') ? leg.routeLongName : leg.routeShortName
  if (!line) return null
  return `the ${line}${leg.directionLabel ? ` · ${leg.directionLabel}` : ''}`
}

/**
 * The station rides a trip walks into from the street and out to it, with
 * the street end of each walk. A ride straight after a bus is entered from
 * the street like the first one; a change between two station rides is a
 * transfer instead.
 */
export function stationVisits<L extends RideLeg>(legs: L[]) {
  const enter: Array<{ ride: L; street: { lat: number; lng: number } }> = []
  const leave: Array<{ ride: L; street: { lat: number; lng: number } }> = []
  legs.forEach((leg, i) => {
    if (!atStation(leg)) return
    const before = legs[i - 1], after = legs[i + 1]
    const prevRide = legs.slice(0, i).reverse().find((l) => l.transitLeg)
    const nextRide = legs.slice(i + 1).find((l) => l.transitLeg)
    if (before && !before.transitLeg && !(prevRide && atStation(prevRide))) enter.push({ ride: leg, street: before.from })
    if (after && !after.transitLeg && !(nextRide && atStation(nextRide))) leave.push({ ride: leg, street: after.to })
  })
  return { enter, leave }
}

/** Consecutive rides that change inside a station: both stop at platforms. */
export function stationChanges<L extends RideLeg>(legs: L[]): Array<readonly [L, L]> {
  const rides = legs.filter((l) => l.transitLeg)
  return rides.slice(1).flatMap((b, i) => (atStation(rides[i]) && atStation(b) ? [[rides[i], b] as const] : []))
}

export function bearing(a: [number, number], b: [number, number]): number {
  const [lon1, lat1, lon2, lat2] = [toRad(a[0]), toRad(a[1]), toRad(b[0]), toRad(b[1])]
  const y = Math.sin(lon2 - lon1) * Math.cos(lat2)
  const x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(lon2 - lon1)
  return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360
}

/**
 * The train's heading as it pulls in: the bearing over the last stretch of
 * the ride's line, far enough back to ignore a kink at the platform.
 */
export function arrivalBearing(line: Array<[number, number]>, backM = 150): number | null {
  if (line.length < 2) return null
  const end = line[line.length - 1]
  let travelled = 0
  for (let i = line.length - 2; i >= 0; i--) {
    travelled += haversineM({ lng: line[i][0], lat: line[i][1] }, { lng: line[i + 1][0], lat: line[i + 1][1] })
    if (travelled >= backM || i === 0) {
      return travelled > 1 ? bearing(line[i], end) : null
    }
  }
  return null
}

export type TrainSection = 'front' | 'middle' | 'back'

export interface BoardingHint {
  section: TrainSection
  /** 0 at the back of the train, 1 at the front. */
  fraction: number
  /** Seconds saved on the alighting platform against standing at the worst end. */
  savesSeconds: number
}

/**
 * Where to stand to alight closest to what comes next. `position` is the
 * best place on the alighting platform (0 at its tail, 1 at its head, along
 * `axisBearing`). A platform-length train berths with its front at the end
 * it is heading for, so the train's heading decides which end is the front.
 * Null when the platform is short enough that it barely matters.
 */
export function boardingHint(
  platform: { axisBearing: number; lengthM: number },
  position: number,
  heading: number,
  walkMps: number,
  costScale = 1,
): BoardingHint | null {
  const diff = Math.abs(((heading - platform.axisBearing + 540) % 360) - 180)
  const fraction = diff < 90 ? position : 1 - position
  const savesSeconds = Math.round((Math.max(position, 1 - position) * platform.lengthM * costScale) / walkMps)
  if (savesSeconds < BOARDING_HINT_MIN_S) return null
  const section: TrainSection = fraction >= 2 / 3 ? 'front' : fraction <= 1 / 3 ? 'back' : 'middle'
  return { section, fraction: Math.round(fraction * 100) / 100, savesSeconds }
}

/**
 * A boarding hint needs the stairs measured along the platform: a walked
 * path whose first stretch, to the first stair or lift, is not a bridged gap.
 */
export function positionIsMeasured(row: StationPath): boolean {
  if (!isWalked(row.source) || row.bestPosition === null || !row.steps) return false
  for (const step of row.steps) {
    if (step.type === 'bridge') return false
    if (step.type === 'stairs' || step.type === 'escalator' || step.type === 'elevator') return true
  }
  return true
}

// ── Step-by-step ────────────────────────────────────────────────────

export type GuidanceStepType = StepType | 'enter' | 'exit' | 'board'

export interface GuidanceStep {
  type: GuidanceStepType
  instruction: string
  /** Metres walked in the step. */
  distance: number
  /** Expected seconds. */
  duration: number
  fromLevel: number | null
  toLevel: number | null
  /** Levels climbed (positive) or descended (negative); null when the map doesn't say. */
  levelChange: number | null
  /** Signs or places to look for: a line, a direction, an exit's name. */
  landmark: string | null
  /** True across a short gap the map does not connect; no turn-by-turn there. */
  bridged: boolean
  /** Vertex range in the walk's geometry. */
  fromVertex: number
  toVertex: number
}

export interface WalkContext {
  kind: 'access' | 'egress' | 'transfer'
  /** What to follow toward the boarding platform: "the 7 · Queens". */
  sign?: string | null
  /** The entrance or exit used: "Broadway & West 28th Street · NW corner". */
  entrance?: string | null
  /** For transfers that tap out and in again. */
  reentryFree?: boolean | null
  /** Levels of the walk's two ends, for stairs the map leaves unlevelled. */
  levels?: { from: number | null; to: number | null }
}

const VERTICAL = new Set<StepType>(['stairs', 'escalator', 'elevator'])
/** Shorter flat steps fold into their neighbour, so a walk reads in a handful of steps. */
const FOLD_M = 8

const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? '' : 's'}`
const levelCount = (n: number) => (n === 1 ? 'one level' : `${n} levels`)
const metresText = (m: number) => `${Math.max(5, Math.round(m / 5) * 5)} m`

function levelNamer(steps: GuidanceStep[], ctx: WalkContext) {
  const levels = steps.flatMap((s) => [s.fromLevel, s.toLevel]).filter((l): l is number => l !== null)
  const start = steps.find((s) => s.fromLevel !== null)?.fromLevel ?? null
  const end = [...steps].reverse().find((s) => s.toLevel !== null)?.toLevel ?? null
  const middle = [...new Set(levels)].filter((l) => l !== start && l !== end)
  return (level: number | null): string | null => {
    if (level === null) return null
    const platformEnd = ctx.kind === 'egress' ? start : end
    if (level === platformEnd) return 'the platform'
    const streetEnd = ctx.kind === 'access' ? start : ctx.kind === 'egress' ? end : 0
    if (level === streetEnd) return 'street level'
    return middle.length === 1 && middle[0] === level ? 'the mezzanine' : null
  }
}

const isFlat = (x: GuidanceStep) => x.type === 'walk' || x.type === 'platform'
/** One lift ride past a level it doesn't stop at, or one unmapped gap, is still one step. */
const RUNS = new Set<GuidanceStepType>(['elevator', 'bridge'])

function absorb(into: GuidanceStep, s: GuidanceStep, after: boolean): GuidanceStep {
  return {
    ...into, distance: into.distance + s.distance, duration: into.duration + s.duration,
    ...(after ? { toVertex: s.toVertex } : { fromVertex: s.fromVertex }),
  }
}

/** Runs of flat steps merge, and a short flat step folds into the step beside it. */
function fold(steps: GuidanceStep[]): GuidanceStep[] {
  const merged: GuidanceStep[] = []
  for (const s of steps) {
    const prev = merged[merged.length - 1]
    if (prev && isFlat(prev) && isFlat(s)) {
      const keep = prev.distance >= s.distance ? prev.type : s.type
      merged[merged.length - 1] = { ...absorb(prev, s, true), type: keep, toLevel: s.toLevel }
      continue
    }
    merged.push({ ...s })
  }
  const folded: GuidanceStep[] = []
  merged.forEach((s, i) => {
    const short = isFlat(s) && s.distance < FOLD_M && merged.length > 1
    if (short && folded.length) folded[folded.length - 1] = absorb(folded[folded.length - 1], s, true)
    else if (short && i + 1 < merged.length) merged[i + 1] = absorb(merged[i + 1], s, false)
    else folded.push(s)
  })
  const out: GuidanceStep[] = []
  for (const s of folded) {
    const prev = out[out.length - 1]
    if (prev && prev.type === s.type && RUNS.has(s.type)) out[out.length - 1] = { ...absorb(prev, s, true), toLevel: s.toLevel }
    else out.push(s)
  }
  return out
}

/** Below this, the platform walk a path's steps leave out is not worth a step of its own. */
export const PLATFORM_REMAINDER_MIN_S = 5

/**
 * The walk along the platform that a path's time includes but its steps do
 * not: the time runs from the platform's centre, the drawn steps from the
 * nearest point on it.
 */
export function platformRemainder(row: Pick<StationPath, 'seconds' | 'steps'>): { seconds: number; metres: number } | null {
  const walked = (row.steps ?? []).reduce((t, s) => t + s.seconds, 0)
  const seconds = row.seconds - walked
  if (seconds <= 0) return null
  const { walkMps, scale } = DEFAULT_PARAMS.costs
  return { seconds, metres: Math.round((seconds * walkMps) / scale) }
}

/** A stair the map leaves unlevelled takes its levels from the ways either side of it. */
function fillLevels(steps: PathStep[], ends?: WalkContext['levels']): PathStep[] {
  const out = steps.map((s) => ({ ...s }))
  out.forEach((s, i) => {
    s.fromLevel ??= i ? out[i - 1].toLevel : ends?.from ?? null
  })
  for (let i = out.length - 1; i >= 0; i--) {
    out[i].toLevel ??= i + 1 < out.length ? out[i + 1].fromLevel : ends?.to ?? null
  }
  return out
}

/** Up (1) or down (-1) for a step whose own levels the map leaves out, from the nearest known levels either side. */
function trend(steps: GuidanceStep[], i: number, ends?: WalkContext['levels']): number | null {
  const known = (xs: Array<number | null | undefined>) => xs.filter((l): l is number => l !== null && l !== undefined)
  const before = known([ends?.from, ...steps.slice(0, i).flatMap((s) => [s.fromLevel, s.toLevel]), steps[i].fromLevel]).at(-1)
  const after = known([steps[i].toLevel, ...steps.slice(i + 1).flatMap((s) => [s.fromLevel, s.toLevel]), ends?.to])[0]
  return before === undefined || after === undefined || before === after ? null : Math.sign(after - before)
}

/**
 * Written steps for a walked station path, built only from what the map
 * says: level changes, fare gates, the lines on the far platform and the
 * entrance's name. Distances and times stay per step for guidance without GPS.
 */
export function guidanceSteps(row: StationPath, ctx: WalkContext): GuidanceStep[] {
  if (!row.steps?.length) return []
  const filled = fillLevels(row.steps, ctx.levels)
  const extra = platformRemainder(row)
  if (extra) {
    const atEnd = ctx.kind === 'access'
    const edge = atEnd ? filled[filled.length - 1] : filled[0]
    const level = atEnd ? edge.toLevel : edge.fromLevel
    const vertex = atEnd ? edge.toVertex : edge.fromVertex
    const platform: PathStep = { type: 'platform', metres: extra.metres, seconds: extra.seconds, fromLevel: level, toLevel: level, fromVertex: vertex, toVertex: vertex }
    if (extra.seconds < PLATFORM_REMAINDER_MIN_S) edge.seconds += extra.seconds
    else if (atEnd) filled.push(platform)
    else filled.unshift(platform)
  }
  const base: GuidanceStep[] = filled.map((s) => ({
    type: s.type, instruction: '', distance: s.metres, duration: s.seconds, fromLevel: s.fromLevel, toLevel: s.toLevel,
    levelChange: s.fromLevel !== null && s.toLevel !== null ? s.toLevel - s.fromLevel : null,
    landmark: null, bridged: s.type === 'bridge', fromVertex: s.fromVertex, toVertex: s.toVertex,
  }))
  const steps = fold(base)
  const levelName = levelNamer(steps, ctx)
  const lastVertex = steps[steps.length - 1].toVertex
  let gates = 0

  steps.forEach((s, i) => {
    s.distance = Math.round(s.distance * 10) / 10
    s.duration = Math.round(s.duration)
    s.levelChange = s.fromLevel !== null && s.toLevel !== null ? s.toLevel - s.fromLevel : null
    const next = steps.slice(i + 1).find((x) => x.type !== 'walk' && x.type !== 'platform')
    if (VERTICAL.has(s.type as StepType)) {
      const way = s.levelChange ?? trend(steps, i, ctx.levels)
      const dir = way === null ? '' : way > 0 ? ' up' : way < 0 ? ' down' : ''
      const n = s.levelChange === null ? null : Math.abs(s.levelChange)
      const what = s.type === 'stairs' ? 'the stairs' : `the ${s.type}`
      const name = levelName(s.toLevel)
      const count = !n ? '' : !name ? ` ${levelCount(n)}` : s.type !== 'elevator' && n > 1 ? ` ${plural(n, 'level')}` : ''
      s.instruction = `Take ${what}${dir}${count}${name ? ` to ${name}` : ''}`
    } else if (s.type === 'fare_gate') {
      gates++
      const leaving = ctx.kind === 'egress' || (ctx.kind === 'transfer' && gates % 2 === 1)
      s.instruction = leaving ? 'Exit through the fare gates'
        : ctx.kind === 'transfer' ? `Re-enter through the fare gates${ctx.reentryFree === true ? ' · free transfer' : ctx.reentryFree === false ? ' · pay again' : ''}`
        : 'Go through the fare gates'
    } else if (s.type === 'bridge') {
      const toward = next && VERTICAL.has(next.type as StepType) ? (next.type === 'stairs' ? 'the stairs' : `the ${next.type}`)
        : ctx.kind === 'egress' ? 'the exit' : 'the platform'
      s.instruction = `Continue to ${toward}`
    } else {
      const along = s.type === 'platform' && (i === 0 || i === steps.length - 1) ? ' along the platform' : ''
      s.instruction = `Walk ${metresText(s.distance)}${along}`
    }
  })

  // The first step points the way: the line and direction the signs show.
  if (ctx.kind !== 'egress' && ctx.sign) {
    const s = steps[0]
    s.landmark = ctx.sign
    s.instruction = isFlat(s)
      ? `Follow signs for ${ctx.sign} · ${metresText(s.distance)}`
      : `Follow signs for ${ctx.sign} and ${s.instruction.charAt(0).toLowerCase()}${s.instruction.slice(1)}`
  }

  if (ctx.kind === 'access' && ctx.entrance) {
    steps.unshift(marker('enter', `Enter at ${ctx.entrance}`, ctx.entrance, steps[0].fromLevel, 0))
  }
  if (ctx.kind === 'egress' && ctx.entrance) {
    steps.push(marker('exit', `Exit at ${ctx.entrance}`, ctx.entrance, steps[steps.length - 1].toLevel, lastVertex))
  }
  if (ctx.kind !== 'egress' && ctx.sign) {
    steps.push(marker('board', `Board ${ctx.sign}`, ctx.sign, steps[steps.length - 1].toLevel, lastVertex))
  }
  return steps
}

function marker(type: GuidanceStepType, instruction: string, landmark: string, level: number | null, vertex: number): GuidanceStep {
  return {
    type, instruction, distance: 0, duration: 0, fromLevel: level, toLevel: level, levelChange: 0, landmark,
    bridged: false, fromVertex: vertex, toVertex: vertex,
  }
}
