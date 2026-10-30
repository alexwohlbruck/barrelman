/**
 * Bring a feed's static trip_ids into the id space its GTFS-RT feed uses.
 *
 * MOTIS resolves a realtime trip by exact trip_id match. When an agency
 * publishes realtime ids that differ from its schedule ids, nothing resolves
 * and the feed serves schedules with no realtime — every departure comes back
 * `realTime: false`, and MOTIS logs `trip_resolve_error` on every trip update.
 * The GTFS-realtime validator calls this E003, and it is a per-feed defect
 * rather than a property of any one agency: MTA drops a schedule-version
 * prefix from its subway ids,
 *
 *   static  ASP26GEN-1038-Sunday-00_000600_1..S03R
 *   RT                              000600_1..S03R
 *
 * while others truncate a variant suffix, add an agency prefix, or use a
 * different separator. There is no published list of affected feeds, so this
 * module does not carry one. It samples the feed's own realtime data, tries a
 * fixed set of candidate transforms against it, and keeps the one that
 * measurably resolves more trips — or none, and leaves the feed alone.
 *
 * Deciding by measurement rather than by an allowlist means a feed that
 * already resolves is never touched, and a transform that stops working is
 * caught on the next import instead of silently producing ids matching
 * nothing.
 *
 * One feed can hold several id conventions. The same subway zip carries lines
 * whose realtime ids match the schedule after the prefix strip and lines whose
 * realtime ids also drop the path suffix (`091250_L..S` for
 * `091250_L..S01R`). So after the feed-wide choice, each route the realtime
 * sample names gets the same measurement over its own trips, and keeps a
 * further transform only if it clears the same bar.
 */
import JSZip from 'jszip'
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import { parseCsvRows, serializeCsvRows, headerIndex, rewriteCsvColumn } from './csv'

// Decoded through the live binding at call time so a test mock of
// `gtfs-realtime-bindings` applies regardless of import order — same reason as
// vehicles.service.ts.
const decodeFeedMessage = (buf: Uint8Array) =>
  GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buf)

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

export interface RtUrl {
  url: string
  headers?: Record<string, string>
  type?: string
}

/**
 * Files carrying a `trip_id` column that has to move with trips.txt.
 *
 * `transfers.txt` is absent because GTFS spells its trip references
 * `from_trip_id` and `to_trip_id`, and barrelman overwrites that file with
 * computed walking transfers later in the import anyway.
 */
const TRIP_ID_BEARING_FILES = ['stop_times.txt', 'frequencies.txt', 'attributions.txt'] as const

// ── Candidate transforms ────────────────────────────────────────────

export interface TripIdTransform {
  /** Stable key, used in logs and to name the winner. */
  id: string
  /** What it does, in words, for the import log. */
  describe: string
  apply: (tripId: string) => string
}

/**
 * Separators agencies build composite trip ids out of.
 *
 * Kept short. Every extra separator multiplies the candidate set, and a
 * candidate that wins by luck on a small sample is worse than no rewrite.
 */
const SEPARATORS = ['_', '-', ':', '.'] as const

const IDENTITY: TripIdTransform = {
  id: 'identity',
  describe: 'leave trip_ids as published',
  apply: (t: string) => t,
}

/**
 * How many sampled realtime ids to learn a literal affix from.
 *
 * Scanning every static id for each one is O(static × sample), so this is
 * capped. A shared affix shows up in the first handful or not at all.
 */
const AFFIX_SAMPLE = 40

/** An affix has to explain this many realtime ids before it is worth trying. */
const MIN_AFFIX_SUPPORT = 3

/**
 * Literal affixes learned from the feed's own realtime ids.
 *
 * Where a realtime id is the tail of some static id, the head that precedes it
 * is a candidate prefix to drop; where it is the head, the tail is a candidate
 * suffix. The most frequently seen of each becomes a transform.
 *
 * This catches agency-wide affixes that contain no separator, which the
 * separator candidates cannot express. Deriving from the realtime sample
 * rather than from what the static ids happen to share matters: the longest
 * shared prefix of `AGENCYXX1000`…`AGENCYXX1039` is `AGENCYXX10`, which eats
 * into the part that identifies the trip.
 */
export function derivedAffixTransforms(staticIds: string[], rtIds: Set<string>): TripIdTransform[] {
  const prefixes = new Map<string, number>()
  const suffixes = new Map<string, number>()

  let seen = 0
  for (const rt of rtIds) {
    if (seen++ >= AFFIX_SAMPLE) break
    if (!rt) continue
    for (const id of staticIds) {
      if (id.length <= rt.length) continue
      if (id.endsWith(rt)) {
        const prefix = id.slice(0, id.length - rt.length)
        prefixes.set(prefix, (prefixes.get(prefix) ?? 0) + 1)
      } else if (id.startsWith(rt)) {
        const suffix = id.slice(rt.length)
        suffixes.set(suffix, (suffixes.get(suffix) ?? 0) + 1)
      }
    }
  }

  const best = (counts: Map<string, number>): string | null => {
    let winner: string | null = null
    let top = 0
    for (const [affix, count] of counts) {
      if (count > top || (count === top && winner !== null && affix.length > winner.length)) {
        winner = affix
        top = count
      }
    }
    return top >= MIN_AFFIX_SUPPORT ? winner : null
  }

  const transforms: TripIdTransform[] = []

  const prefix = best(prefixes)
  if (prefix) {
    transforms.push({
      id: 'drop-learned-prefix',
      describe: `drop the leading "${prefix}"`,
      apply: t => (t.startsWith(prefix) ? t.slice(prefix.length) : t),
    })
  }

  const suffix = best(suffixes)
  if (suffix) {
    transforms.push({
      id: 'drop-learned-suffix',
      describe: `drop the trailing "${suffix}"`,
      apply: t => (t.endsWith(suffix) ? t.slice(0, t.length - suffix.length) : t),
    })
  }

  return transforms
}

/**
 * Static ids cut to the length the realtime feed publishes.
 *
 * Covers realtime ids that are the head of a static id whose tail varies per
 * trip, which a single learned suffix cannot express: `075250_L..S` for both
 * `075250_L..S01R` and `075250_L..S02R`. The cut length is the one most
 * sampled realtime ids agree on.
 */
export function truncationTransforms(staticIds: string[], rtIds: Set<string>): TripIdTransform[] {
  const lengths = new Map<number, number>()

  let seen = 0
  for (const rt of rtIds) {
    if (seen++ >= AFFIX_SAMPLE) break
    if (!rt) continue
    if (staticIds.some(id => id.length > rt.length && id.startsWith(rt))) {
      lengths.set(rt.length, (lengths.get(rt.length) ?? 0) + 1)
    }
  }

  let length = 0
  let top = 0
  for (const [n, count] of lengths) {
    if (count > top) { length = n; top = count }
  }
  if (top < MIN_AFFIX_SUPPORT) return []

  return [{
    id: `truncate-to:${length}`,
    describe: `keep the first ${length} characters`,
    apply: t => (t.length > length ? t.slice(0, length) : t),
  }]
}

/** Transforms whose affix or length was read off today's sample, not off the id's structure. */
const isFitted = (t: TripIdTransform) => t.id.startsWith('truncate-to:') || t.id.startsWith('drop-learned-')

/**
 * Every transform worth testing against this feed's realtime ids.
 *
 * Four shapes per separator: drop everything through the first or last
 * separator (the agency prefixes its schedule ids), and drop everything from
 * the first or last separator onward (the agency truncates a variant in
 * realtime). Separator candidates handle affixes that vary per trip, which
 * MTA's schedule-version prefixes do. The learned affixes handle fixed ones
 * with no separator to key on, and truncation handles realtime ids cut to a
 * fixed length.
 */
export function candidateTransforms(staticIds: string[], rtIds: Set<string>): TripIdTransform[] {
  const candidates: TripIdTransform[] = []

  for (const sep of SEPARATORS) {
    candidates.push(
      {
        id: `drop-through-first:${sep}`,
        describe: `drop everything through the first "${sep}"`,
        apply: t => { const i = t.indexOf(sep); return i === -1 ? t : t.slice(i + 1) },
      },
      {
        id: `drop-through-last:${sep}`,
        describe: `drop everything through the last "${sep}"`,
        apply: t => { const i = t.lastIndexOf(sep); return i === -1 ? t : t.slice(i + 1) },
      },
      {
        id: `drop-from-first:${sep}`,
        describe: `drop everything from the first "${sep}" onward`,
        apply: t => { const i = t.indexOf(sep); return i === -1 ? t : t.slice(0, i) },
      },
      {
        id: `drop-from-last:${sep}`,
        describe: `drop everything from the last "${sep}" onward`,
        apply: t => { const i = t.lastIndexOf(sep); return i === -1 ? t : t.slice(0, i) },
      },
    )
  }

  return [
    ...candidates,
    ...derivedAffixTransforms(staticIds, rtIds),
    ...truncationTransforms(staticIds, rtIds),
  ]
}

// ── Choosing one ────────────────────────────────────────────────────

/**
 * Realtime ids needed before a sample is worth deciding on.
 *
 * A late-night or holiday snapshot of a working feed can be nearly empty, and
 * reads exactly like a broken one. Below this the answer is "ask again next
 * import", never "rewrite".
 */
export const MIN_SAMPLE = 20

/** Above this share already resolving, the feed works and is left alone. */
const ALREADY_RESOLVING = 0.1

/** A transform has to reach at least this share of the sample to be worth it. */
const MIN_IMPROVEMENT = 0.25

/** …and beat what the feed already manages by this multiple. */
const CLEAR_WIN = 4

/** Whether reaching `matched` of the sample is worth a rewrite over `baseline`. */
function clearsBar(matched: number, baseline: number, sampleSize: number): boolean {
  return matched >= sampleSize * MIN_IMPROVEMENT && matched >= Math.max(baseline, 1) * CLEAR_WIN
}

export interface TransformScore {
  transform: TripIdTransform
  /** Sampled realtime ids this transform makes reachable. */
  matched: number
  /** Static ids it actually alters, so a partial transform is visible. */
  changed: number
  /** Distinct static ids it produces, so a collapsing transform is visible. */
  distinct: number
}

export interface TransformChoice {
  transform: TripIdTransform | null
  reason: 'applied' | 'sample-too-small' | 'already-resolving' | 'no-improvement'
  sampleSize: number
  baselineMatched: number
  best?: TransformScore
  /** Every candidate that matched anything, best first. */
  scores: TransformScore[]
}

/**
 * Pick the transform that best reconciles the two id spaces, or none.
 *
 * Scored on how many sampled realtime ids each transform makes reachable.
 * Some realtime ids legitimately have no schedule entry (ADDED and unscheduled
 * trips), so the bar is a clear improvement rather than a complete match.
 *
 * Ties break on how many static ids the transform actually alters, then on how
 * few it collapses together. Coverage comes first because a transform that
 * reaches every sampled realtime id while rewriting only half the schedule
 * leaves the other half unresolvable the moment its trips appear in realtime.
 *
 * A learned affix or length cut loses a tie to a separator. It is fitted to
 * the ids in today's sample, while a separator that explains the same trips
 * also explains the ids of days the sample did not see.
 */
export function chooseTransform(
  staticIds: string[],
  rtIds: Set<string>,
  options: { minSample?: number } = {},
): TransformChoice {
  const score = (transform: TripIdTransform): TransformScore => {
    const transformed = new Set<string>()
    let changed = 0
    for (const id of staticIds) {
      const next = transform.apply(id)
      if (next !== id) changed++
      transformed.add(next)
    }
    let matched = 0
    for (const rt of rtIds) if (transformed.has(rt)) matched++
    return { transform, matched, changed, distinct: transformed.size }
  }

  const baseline = score(IDENTITY)
  const sampleSize = rtIds.size
  const base = {
    sampleSize,
    baselineMatched: baseline.matched,
    scores: [] as TransformScore[],
  }

  if (sampleSize < (options.minSample ?? MIN_SAMPLE)) {
    return { ...base, transform: null, reason: 'sample-too-small' }
  }
  if (baseline.matched >= sampleSize * ALREADY_RESOLVING) {
    return { ...base, transform: null, reason: 'already-resolving' }
  }

  const scores = candidateTransforms(staticIds, rtIds)
    .map(score)
    .filter(s => s.matched > 0)
    .sort((a, b) =>
      b.matched - a.matched ||
      Number(isFitted(a.transform)) - Number(isFitted(b.transform)) ||
      b.changed - a.changed ||
      b.distinct - a.distinct,
    )

  const best = scores[0]
  if (!best || !clearsBar(best.matched, baseline.matched, sampleSize)) {
    return { ...base, scores, transform: null, reason: 'no-improvement', best }
  }

  return { ...base, scores, transform: best.transform, reason: 'applied', best }
}

// ── Sampling the realtime feed ──────────────────────────────────────

/** What a sampled trip update named besides its trip_id ('' where absent). */
export interface RtTripRef {
  routeId: string
  /** YYYYMMDD. */
  startDate: string
}

/** Sampled realtime trips, keyed by trip_id. */
export type RtSample = Map<string, RtTripRef>

/**
 * Trips from a feed's realtime trip updates, unioned across its URLs.
 *
 * Never throws. A feed whose realtime is unreachable, rate-limited or
 * malformed yields an empty set, which reads as "not enough evidence" and
 * leaves the feed unmodified — the same outcome as before this step existed.
 */
export async function sampleRealtimeTrips(
  rtUrls: RtUrl[] | undefined,
  fetchFn: FetchFn = globalThis.fetch,
  onWarn?: (message: string) => void,
): Promise<RtSample> {
  const trips: RtSample = new Map()
  // Feeds recorded before discovery stored a type are untyped; try those too
  // rather than skipping a feed whose only defect is an old database row.
  const tripUpdateUrls = (rtUrls ?? []).filter(u => u.type === 'tripUpdates' || u.type === undefined)

  for (const rt of tripUpdateUrls) {
    try {
      const response = await fetchFn(rt.url, {
        headers: { ...(rt.headers ?? {}) },
        signal: AbortSignal.timeout(15_000),
      })
      if (!response.ok) {
        onWarn?.(`realtime probe returned ${response.status}`)
        continue
      }

      const message = decodeFeedMessage(new Uint8Array(await response.arrayBuffer()))
      for (const entity of message.entity ?? []) {
        const trip = entity.tripUpdate?.trip
        if (trip?.tripId) trips.set(trip.tripId, { routeId: trip.routeId ?? '', startDate: trip.startDate ?? '' })
      }
    } catch (err) {
      onWarn?.(`realtime probe failed: ${err instanceof Error ? err.message : err}`)
    }
  }

  return trips
}

// ── Applying it ─────────────────────────────────────────────────────

export interface TripIdOverlap {
  tripId: string
  serviceA: string
  serviceB: string
  /** Up to five shared dates, as YYYYMMDD. Enough to diagnose, not a dump. */
  sharedDates: string[]
}

export interface AlignmentResult {
  applied: boolean
  /** The winning transform's id, when one was applied. */
  transform?: string
  describe?: string
  /** Why nothing was rewritten. */
  skipReason?:
    | 'no-trips'
    | 'no-realtime'
    | 'sample-too-small'
    | 'already-resolving'
    | 'no-improvement'
    | 'date-overlap'
  /** Rows whose trip_id changed, per file. */
  rewrittenRows: Record<string, number>
  /** Distinct trip_ids claimed by more than one trip after the transform. */
  collidingTripIds: number
  overlaps: TripIdOverlap[]
  /** Further transforms kept for single routes, as route_id → description. */
  routeTransforms: Record<string, string>
  /** Trips left on their previous id, because the transform would merge them with another. */
  heldTrips: number
  /** Sampled realtime ids, and how many the returned feed's trip_ids reach. */
  sampleSize: number
  matched: number
  /** Of those, how many reach a trip that runs on the update's start date. */
  resolving: number
}

const DAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

/** The dates a service_id is active on, as YYYYMMDD, from calendar + exceptions. */
export function expandServiceDates(
  calendar: string | null,
  calendarDates: string | null,
): Map<string, Set<string>> {
  const dates = new Map<string, Set<string>>()
  const add = (serviceId: string, day: string) => {
    const set = dates.get(serviceId) ?? new Set<string>()
    set.add(day)
    dates.set(serviceId, set)
  }

  if (calendar) {
    const rows = parseCsvRows(calendar)
    if (rows.length) {
      const col = headerIndex(rows[0]!)
      const dayCols = DAYS.map(d => col.get(d))
      for (const cells of rows.slice(1)) {
        const serviceId = cells[col.get('service_id') ?? -1]?.trim()
        const start = cells[col.get('start_date') ?? -1]?.trim()
        const end = cells[col.get('end_date') ?? -1]?.trim()
        if (!serviceId || !start || !end) continue

        const active = dayCols.map(c => (c === undefined ? false : cells[c]?.trim() === '1'))
        const from = Date.UTC(+start.slice(0, 4), +start.slice(4, 6) - 1, +start.slice(6, 8))
        const to = Date.UTC(+end.slice(0, 4), +end.slice(4, 6) - 1, +end.slice(6, 8))
        for (let t = from; t <= to; t += 86_400_000) {
          const d = new Date(t)
          if (active[d.getUTCDay()]) {
            add(serviceId, d.toISOString().slice(0, 10).replace(/-/g, ''))
          }
        }
      }
    }
  }

  if (calendarDates) {
    const rows = parseCsvRows(calendarDates)
    if (rows.length) {
      const col = headerIndex(rows[0]!)
      for (const cells of rows.slice(1)) {
        const serviceId = cells[col.get('service_id') ?? -1]?.trim()
        const day = cells[col.get('date') ?? -1]?.trim()
        const type = cells[col.get('exception_type') ?? -1]?.trim()
        if (!serviceId || !day) continue
        if (type === '1') add(serviceId, day)
        else dates.get(serviceId)?.delete(day)
      }
    }
  }

  return dates
}

/**
 * Trip_id groups a transform would merge, and whether any of them run together.
 *
 * A transform that drops part of an id makes some ids collide, and MOTIS cannot
 * load a feed with duplicate trip_ids. nigiri's GTFS loader keys trips by id,
 * so every stop_times row of every duplicate lands on the one trip that kept
 * the id, its times run backwards, and the loader drops it ("invalid event
 * times, skipping"). Every trip sharing the id is lost from the timetable,
 * not just the extras. MTA's 2026-08-26 subway feed reuses one run id across
 * up to nine service variants. Stripping its prefix put 71,786 of 81,850
 * trips on shared ids, and MOTIS routed around a subway that had mostly
 * vanished. So `holdApart` never writes a shared id. This reports the groups
 * a rewrite would create, and which of them run on the same date, where not
 * even realtime could tell the trips apart.
 */
export function findDateOverlaps(
  tripsToServices: Map<string, string[]>,
  serviceDates: Map<string, Set<string>>,
): { overlaps: TripIdOverlap[]; collidingTripIds: number } {
  const overlaps: TripIdOverlap[] = []
  let collidingTripIds = 0

  for (const [tripId, services] of tripsToServices) {
    if (services.length < 2) continue
    collidingTripIds++
    for (let i = 0; i < services.length; i++) {
      for (let j = i + 1; j < services.length; j++) {
        const a = serviceDates.get(services[i]!)
        const b = serviceDates.get(services[j]!)
        if (!a || !b) continue
        const shared = [...a].filter(d => b.has(d))
        if (shared.length) {
          overlaps.push({
            tripId,
            serviceA: services[i]!,
            serviceB: services[j]!,
            sharedDates: shared.slice(0, 5),
          })
        }
      }
    }
  }

  return { overlaps, collidingTripIds }
}

// ── Per-route refinement ────────────────────────────────────────────

export interface StaticTrip {
  id: string
  serviceId: string
  routeId: string
}

/**
 * Realtime trips one route needs before its own transform is worth deciding.
 *
 * Lower than the feed-wide bar because a route is a slice of a feed, but a
 * route seen this few times on a quiet night is left for the next import.
 */
export const MIN_ROUTE_SAMPLE = 10

/**
 * Routes whose realtime ids still miss after the feed-wide transform, and the
 * further transform each one measures best with.
 *
 * Every route is held to `chooseTransform`'s bar over its own trips, so a
 * route that mostly resolves already is never touched, and one route's
 * convention never decides another's.
 */
export function chooseRouteTransforms(
  trips: StaticTrip[],
  feedIds: Map<string, string>,
  rtTrips: RtSample,
): Map<string, TransformChoice> {
  const rtByRoute = new Map<string, Set<string>>()
  for (const [tripId, { routeId }] of rtTrips) {
    if (!routeId) continue
    const set = rtByRoute.get(routeId) ?? new Set<string>()
    set.add(tripId)
    rtByRoute.set(routeId, set)
  }

  const staticByRoute = new Map<string, string[]>()
  for (const trip of trips) {
    if (!rtByRoute.has(trip.routeId)) continue
    const list = staticByRoute.get(trip.routeId) ?? []
    list.push(feedIds.get(trip.id) ?? trip.id)
    staticByRoute.set(trip.routeId, list)
  }

  const choices = new Map<string, TransformChoice>()
  for (const [routeId, staticIds] of staticByRoute) {
    const choice = chooseTransform(staticIds, rtByRoute.get(routeId)!, { minSample: MIN_ROUTE_SAMPLE })
    if (choice.transform) choices.set(routeId, choice)
  }
  return choices
}

/** The first date, as YYYYMMDD, on or after `today` that a service runs. */
function nextServiceDate(
  serviceId: string,
  serviceDates: Map<string, Set<string>>,
  today: string,
): string | undefined {
  let next: string | undefined
  for (const d of serviceDates.get(serviceId) ?? []) {
    if (d >= today && (next === undefined || d < next)) next = d
  }
  return next
}

/**
 * Each trip's proposed id, where no other trip would share it.
 *
 * A shared id is never written, because MOTIS drops every trip that carries
 * one (see `findDateOverlaps`). Trips that would share an id are settled two
 * ways:
 *
 * - If they never run on the same date (the Saturday and Sunday variants of
 *   one run), the trip running soonest takes the proposed id and the rest
 *   keep their fallback id. Realtime then resolves for the variant running
 *   now. MOTIS rebuilds nightly, so the id moves with the service day.
 * - If two of them run on the same date (two trains leaving one terminal at
 *   the same minute on different paths, or ids lacking the part a separator
 *   transform expects), no trip may take the id, because realtime could not
 *   tell them apart. All of them keep their fallback id.
 *
 * Overlaps left among fallback ids are returned for the caller to refuse.
 */
function holdApart(
  trips: StaticTrip[],
  fallback: Map<string, string>,
  proposed: (trip: StaticTrip) => string,
  serviceDates: Map<string, Set<string>>,
  today: string,
) {
  const held = new Set<string>()
  for (;;) {
    const rewrite = new Map(trips.map(t => [t.id, held.has(t.id) ? fallback.get(t.id)! : proposed(t)]))
    const groups = new Map<string, StaticTrip[]>()
    for (const t of trips) {
      const key = rewrite.get(t.id)!
      const group = groups.get(key)
      if (group) group.push(t)
      else groups.set(key, [t])
    }

    const overlapping = new Set(
      findDateOverlaps(servicesByKey(trips, rewrite), serviceDates).overlaps.map(o => o.tripId),
    )
    const before = held.size
    for (const [key, group] of groups) {
      if (group.length < 2) continue
      // A trip already on its fallback id owns it; nothing else can move it.
      const claimant = overlapping.has(key)
        ? undefined
        : (group.find(t => fallback.get(t.id) === key) ??
          group.reduce((best, t) => {
            const a = nextServiceDate(t.serviceId, serviceDates, today) ?? '99999999'
            const b = nextServiceDate(best.serviceId, serviceDates, today) ?? '99999999'
            return a < b ? t : best
          }))
      for (const t of group) {
        if (t !== claimant && key !== fallback.get(t.id)) held.add(t.id)
      }
    }
    if (held.size === before) {
      return { rewrite, held, check: findDateOverlaps(servicesByKey(trips, rewrite), serviceDates) }
    }
  }
}

/**
 * Sampled realtime trips that land on a static trip running on their start
 * date, as MOTIS requires. Where realtime gives no date, today or yesterday
 * (UTC) counts, which covers any timezone's service day.
 */
function countResolving(
  trips: StaticTrip[],
  rewrite: Map<string, string>,
  rtTrips: RtSample,
  serviceDates: Map<string, Set<string>>,
  now = Date.now(),
): number {
  const services = servicesByKey(trips, rewrite)
  const day = (t: number) => new Date(t).toISOString().slice(0, 10).replace(/-/g, '')
  const undated = [day(now), day(now - 86_400_000)]
  let resolving = 0
  for (const [rt, { startDate }] of rtTrips) {
    const dates = startDate ? [startDate] : undated
    if (services.get(rt)?.some(s => dates.some(d => serviceDates.get(s)?.has(d)))) resolving++
  }
  return resolving
}

function rewriteAll(trips: StaticTrip[], transform: TripIdTransform): Map<string, string> {
  return new Map(trips.map(t => [t.id, transform.apply(t.id)]))
}

/** How many sampled realtime ids, optionally of one route, a rewrite reaches. */
function countMatched(rewrite: Map<string, string>, rtTrips: RtSample, routeId?: string): number {
  const ids = new Set(rewrite.values())
  let matched = 0
  for (const [rt, ref] of rtTrips) {
    if ((routeId === undefined || ref.routeId === routeId) && ids.has(rt)) matched++
  }
  return matched
}

/**
 * Share of the realtime sample an alternate static feed has to reach beyond
 * the feed it would replace. Small, because both are measured against one
 * sample, but enough that a coincidence on a few trips never swaps a feed.
 */
const ALTERNATE_GAIN = 0.05

/**
 * Whether alignment `a` resolves clearly more of the same realtime sample than
 * `b`. Counted against running trips, so a stale feed whose ids happen to match
 * cannot win on ids alone.
 */
export function resolvesClearlyMore(a: AlignmentResult, b: AlignmentResult): boolean {
  return a.sampleSize >= MIN_SAMPLE && a.resolving - b.resolving >= a.sampleSize * ALTERNATE_GAIN
}

/** The rewrite an applied alignment made, in words, for the import log. */
export function describeAlignment(result: AlignmentResult): string {
  const parts = result.transform && result.transform !== IDENTITY.id ? [result.describe!] : []
  for (const [route, describe] of Object.entries(result.routeTransforms)) {
    parts.push(`${describe} on route ${route}`)
  }
  return parts.join('; ')
}

function servicesByKey(trips: StaticTrip[], rewrite: Map<string, string>): Map<string, string[]> {
  const byKey = new Map<string, string[]>()
  for (const trip of trips) {
    const key = rewrite.get(trip.id)!
    const services = byKey.get(key)
    if (services) services.push(trip.serviceId)
    else byKey.set(key, [trip.serviceId])
  }
  return byKey
}

/**
 * Rewrite a feed's static trip_ids into the form its realtime feed publishes.
 *
 * On any doubt the feed imports unmodified. Schedules with no realtime is what
 * an affected feed already serves, while a failed import would take it stale or
 * absent, which is worse than the problem being avoided.
 *
 * @param options.extendCalendar must mirror MOTIS's per-dataset
 * `extend_calendar`. Barrelman never enables it, but with it on MOTIS routes
 * against synthetic calendar extensions and the overlap check would have to
 * expand against those — the subway feed's seasonal variants
 * (`Sunday-H-20260526-20260907` against `Sunday-H-20260908-20261031`) do
 * overlap once extended, even though the file says they do not. Throwing beats
 * guessing under a flag this cannot read.
 */
export async function alignRealtimeTripIds(
  buffer: ArrayBuffer,
  feed: { feedId: string; onestopId?: string; rtUrls?: RtUrl[] },
  options: {
    extendCalendar?: boolean
    fetchFn?: FetchFn
    /** Pre-sampled realtime trips, for callers that already have them. */
    rtTrips?: RtSample
    onWarn?: (message: string) => void
    /** Decides which variant of a shared run takes its realtime id. */
    now?: number
  } = {},
): Promise<{ buffer: ArrayBuffer; result: AlignmentResult }> {
  const skip = (
    skipReason: AlignmentResult['skipReason'],
    extra: Partial<AlignmentResult> = {},
  ) => ({
    buffer,
    result: {
      applied: false,
      skipReason,
      rewrittenRows: {},
      collidingTripIds: 0,
      overlaps: [],
      routeTransforms: {},
      heldTrips: 0,
      sampleSize: 0,
      matched: 0,
      resolving: 0,
      ...extra,
    },
  })

  if (options.extendCalendar) {
    throw new Error(
      'alignRealtimeTripIds: extend_calendar is enabled; the overlap check must expand ' +
        'against the extended calendar before any feed can be rewritten safely.',
    )
  }

  const zip = await JSZip.loadAsync(buffer)
  const tripsFile = zip.file('trips.txt')
  if (!tripsFile) return skip('no-trips')

  const tripRows = parseCsvRows(await tripsFile.async('string'))
  if (tripRows.length < 2) return skip('no-trips')

  const tripCols = headerIndex(tripRows[0]!)
  const tripIdCol = tripCols.get('trip_id')
  const serviceIdCol = tripCols.get('service_id')
  const routeIdCol = tripCols.get('route_id')
  if (tripIdCol === undefined) return skip('no-trips')

  const trips: StaticTrip[] = []
  for (const cells of tripRows.slice(1)) {
    const id = cells[tripIdCol]?.trim()
    if (!id) continue
    trips.push({
      id,
      serviceId: serviceIdCol === undefined ? '' : (cells[serviceIdCol]?.trim() ?? ''),
      routeId: routeIdCol === undefined ? '' : (cells[routeIdCol]?.trim() ?? ''),
    })
  }
  if (!trips.length) return skip('no-trips')

  // ── What does this feed's realtime actually publish? ────────────────
  const rtTrips =
    options.rtTrips ??
    (await sampleRealtimeTrips(feed.rtUrls, options.fetchFn ?? globalThis.fetch, options.onWarn))
  if (!rtTrips.size) return skip('no-realtime')
  const rtIds = new Set(rtTrips.keys())

  const read = async (name: string) => {
    const file = zip.file(name)
    return file ? await file.async('string') : null
  }
  const serviceDates = expandServiceDates(await read('calendar.txt'), await read('calendar_dates.txt'))
  const today = new Date(options.now ?? Date.now()).toISOString().slice(0, 10).replace(/-/g, '')

  const choice = chooseTransform(trips.map(t => t.id), rtIds)
  const feedTransform = choice.transform
  const original = new Map(trips.map(t => [t.id, t.id]))
  const asPublished = {
    sampleSize: choice.sampleSize,
    matched: choice.baselineMatched,
    resolving: countResolving(trips, original, rtTrips, serviceDates),
  }
  const unchanged = () => skip(choice.reason as AlignmentResult['skipReason'], asPublished)
  if (!feedTransform && !chooseRouteTransforms(trips, original, rtTrips).size) return unchanged()

  const refuse = (check: ReturnType<typeof findDateOverlaps>) =>
    skip('date-overlap', {
      collidingTripIds: check.collidingTripIds,
      overlaps: check.overlaps,
      ...asPublished,
    })

  // ── Feed-wide transform, kept wherever it keeps trips apart ─────────
  let base = original
  let held = 0
  if (feedTransform) {
    const settled = holdApart(trips, original, t => feedTransform.apply(t.id), serviceDates, today)
    if (settled.check.overlaps.length) return refuse(settled.check)
    if (!clearsBar(countMatched(settled.rewrite, rtTrips), choice.baselineMatched, choice.sampleSize)) {
      return refuse(findDateOverlaps(servicesByKey(trips, rewriteAll(trips, feedTransform)), serviceDates))
    }
    base = settled.rewrite
    held = settled.held.size
  }

  // ── Route transforms on top, held to the same bar ───────────────────
  let routeChoices = chooseRouteTransforms(trips, base, rtTrips)
  if (routeChoices.size && !feedTransform) {
    const check = findDateOverlaps(servicesByKey(trips, base), serviceDates)
    if (check.overlaps.length) routeChoices = new Map()
  }

  let rewrite = base
  while (routeChoices.size) {
    const current = routeChoices
    const settled = holdApart(
      trips,
      base,
      t => current.get(t.routeId)?.transform!.apply(base.get(t.id)!) ?? base.get(t.id)!,
      serviceDates,
      today,
    )
    const kept = new Map([...current].filter(([route, c]) =>
      clearsBar(countMatched(settled.rewrite, rtTrips, route), c.baselineMatched, c.sampleSize),
    ))
    if (settled.check.overlaps.length) {
      routeChoices = new Map()
    } else if (kept.size === current.size) {
      rewrite = settled.rewrite
      held += settled.held.size
      break
    } else {
      routeChoices = kept
    }
  }

  if (!feedTransform && !routeChoices.size) return unchanged()
  const { collidingTripIds } = findDateOverlaps(servicesByKey(trips, rewrite), serviceDates)

  // ── Rewrite ─────────────────────────────────────────────────────────
  const rewrittenRows: Record<string, number> = {}

  const rewriteFile = async (name: string, rows?: string[][]) => {
    const file = zip.file(name)
    if (!file && !rows) return

    // Streamed rather than parsed into rows: stop_times.txt is millions of
    // rows, and holding them as arrays ran the US import out of memory.
    if (!rows) {
      const { text, changed } = rewriteCsvColumn(await file!.async('string'), 'trip_id', id => rewrite.get(id))
      if (changed) zip.file(name, text)
      rewrittenRows[name] = changed
      return
    }

    const parsed = rows
    if (!parsed.length) return
    const col = headerIndex(parsed[0]!).get('trip_id')
    if (col === undefined) return

    let changed = 0
    for (const cells of parsed.slice(1)) {
      const original = cells[col]
      if (original === undefined) continue
      const next = rewrite.get(original.trim()) ?? original
      if (next !== original) {
        cells[col] = next
        changed++
      }
    }

    zip.file(name, serializeCsvRows(parsed))
    rewrittenRows[name] = changed
  }

  await rewriteFile('trips.txt', tripRows)
  for (const name of TRIP_ID_BEARING_FILES) await rewriteFile(name)

  const routeTransforms = Object.fromEntries(
    [...routeChoices].map(([route, c]) => [route, c.transform!.describe]),
  )

  // JSZip defaults to STORE, which would land the rewritten feed on the volume
  // MOTIS imports from several times larger than it arrived.
  const rewritten = await zip.generateAsync({ type: 'arraybuffer', compression: 'DEFLATE' })
  return {
    buffer: rewritten,
    result: {
      applied: true,
      transform: (feedTransform ?? IDENTITY).id,
      describe: (feedTransform ?? IDENTITY).describe,
      rewrittenRows,
      collidingTripIds,
      overlaps: [],
      routeTransforms,
      heldTrips: held,
      sampleSize: rtIds.size,
      matched: countMatched(rewrite, rtTrips),
      resolving: countResolving(trips, rewrite, rtTrips, serviceDates),
    },
  }
}
