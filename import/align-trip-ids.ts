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
 */
import JSZip from 'jszip'
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import { parseCsvRows, serializeCsvRows, headerIndex } from './csv'

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
 * Every transform worth testing against this feed's realtime ids.
 *
 * Four shapes per separator: drop everything through the first or last
 * separator (the agency prefixes its schedule ids), and drop everything from
 * the first or last separator onward (the agency truncates a variant in
 * realtime). Separator candidates handle affixes that vary per trip, which
 * MTA's schedule-version prefixes do. The learned affixes handle fixed ones
 * with no separator to key on.
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

  return [...candidates, ...derivedAffixTransforms(staticIds, rtIds)]
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
 */
export function chooseTransform(staticIds: string[], rtIds: Set<string>): TransformChoice {
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

  if (sampleSize < MIN_SAMPLE) {
    return { ...base, transform: null, reason: 'sample-too-small' }
  }
  if (baseline.matched >= sampleSize * ALREADY_RESOLVING) {
    return { ...base, transform: null, reason: 'already-resolving' }
  }

  const scores = candidateTransforms(staticIds, rtIds)
    .map(score)
    .filter(s => s.matched > 0)
    .sort((a, b) => b.matched - a.matched || b.changed - a.changed || b.distinct - a.distinct)

  const best = scores[0]
  if (
    !best ||
    best.matched < sampleSize * MIN_IMPROVEMENT ||
    best.matched < Math.max(baseline.matched, 1) * CLEAR_WIN
  ) {
    return { ...base, scores, transform: null, reason: 'no-improvement', best }
  }

  return { ...base, scores, transform: best.transform, reason: 'applied', best }
}

// ── Sampling the realtime feed ──────────────────────────────────────

/**
 * Trip ids from a feed's realtime trip updates, unioned across its URLs.
 *
 * Never throws. A feed whose realtime is unreachable, rate-limited or
 * malformed yields an empty set, which reads as "not enough evidence" and
 * leaves the feed unmodified — the same outcome as before this step existed.
 */
export async function sampleRealtimeTripIds(
  rtUrls: RtUrl[] | undefined,
  fetchFn: FetchFn = globalThis.fetch,
  onWarn?: (message: string) => void,
): Promise<Set<string>> {
  const ids = new Set<string>()
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
        const tripId = entity.tripUpdate?.trip?.tripId
        if (tripId) ids.add(tripId)
      }
    } catch (err) {
      onWarn?.(`realtime probe failed: ${err instanceof Error ? err.message : err}`)
    }
  }

  return ids
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
  /** Sampled realtime ids, and how many the transform reaches. */
  sampleSize: number
  matched: number
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
 * A transform that drops part of an id makes some ids collide. MOTIS tolerates
 * duplicate trip_ids: nigiri's `trip_id_to_idx_` is a sorted vector with no
 * uniqueness constraint, `resolve_static_trip_id` walks every id match, and
 * `resolve_trip` picks between them with `is_transport_active` against the
 * traffic-day bitfield using the realtime `start_date`. The subway feed
 * collapses 20,621 trips onto 17,327 ids and resolves correctly, because the
 * colliding pairs are Saturday-versus-Sunday variants of the same run.
 *
 * It stops being safe the moment two colliding trips run on the same date.
 * nigiri's trip-update path applies the update inside the `resolve_static`
 * callback and returns `kContinue` without breaking, so the update lands on
 * every match — including a transport that is not running — and counts a
 * success for each. Nothing in the match-rate metric would show it.
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
    /** Pre-sampled realtime ids, for callers that already have them. */
    rtTripIds?: Set<string>
    onWarn?: (message: string) => void
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
      sampleSize: 0,
      matched: 0,
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
  if (tripIdCol === undefined) return skip('no-trips')

  const staticIds: string[] = []
  const servicesByStaticId: Array<[string, string]> = []
  for (const cells of tripRows.slice(1)) {
    const tripId = cells[tripIdCol]?.trim()
    if (!tripId) continue
    staticIds.push(tripId)
    servicesByStaticId.push([tripId, serviceIdCol === undefined ? '' : (cells[serviceIdCol]?.trim() ?? '')])
  }
  if (!staticIds.length) return skip('no-trips')

  // ── What does this feed's realtime actually publish? ────────────────
  const rtIds =
    options.rtTripIds ??
    (await sampleRealtimeTripIds(feed.rtUrls, options.fetchFn ?? globalThis.fetch, options.onWarn))
  if (!rtIds.size) return skip('no-realtime')

  const choice = chooseTransform(staticIds, rtIds)
  if (!choice.transform) {
    return skip(choice.reason as AlignmentResult['skipReason'], {
      sampleSize: choice.sampleSize,
      matched: choice.best?.matched ?? choice.baselineMatched,
    })
  }
  const transform = choice.transform

  // ── Would it make two trips indistinguishable on a day both run? ────
  const tripsToServices = new Map<string, string[]>()
  for (const [tripId, serviceId] of servicesByStaticId) {
    const key = transform.apply(tripId)
    const services = tripsToServices.get(key)
    if (services) services.push(serviceId)
    else tripsToServices.set(key, [serviceId])
  }

  const read = async (name: string) => {
    const file = zip.file(name)
    return file ? await file.async('string') : null
  }
  const serviceDates = expandServiceDates(await read('calendar.txt'), await read('calendar_dates.txt'))
  const { overlaps, collidingTripIds } = findDateOverlaps(tripsToServices, serviceDates)

  const measured = { sampleSize: choice.sampleSize, matched: choice.best!.matched }
  if (overlaps.length) {
    return skip('date-overlap', { collidingTripIds, overlaps, ...measured })
  }

  // ── Rewrite ─────────────────────────────────────────────────────────
  const rewrittenRows: Record<string, number> = {}

  const rewriteFile = async (name: string, rows?: string[][]) => {
    const file = zip.file(name)
    if (!file && !rows) return

    const parsed = rows ?? parseCsvRows(await file!.async('string'))
    if (!parsed.length) return
    const col = headerIndex(parsed[0]!).get('trip_id')
    if (col === undefined) return

    let changed = 0
    for (const cells of parsed.slice(1)) {
      const original = cells[col]
      if (original === undefined) continue
      const next = transform.apply(original.trim())
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

  // JSZip defaults to STORE, which would land the rewritten feed on the volume
  // MOTIS imports from several times larger than it arrived.
  const rewritten = await zip.generateAsync({ type: 'arraybuffer', compression: 'DEFLATE' })
  return {
    buffer: rewritten,
    result: {
      applied: true,
      transform: transform.id,
      describe: transform.describe,
      rewrittenRows,
      collidingTripIds,
      overlaps: [],
      ...measured,
    },
  }
}
