/**
 * Align static trip_ids with the realtime feed, for feeds that publish the two
 * in different id spaces.
 *
 * MTA publishes subway realtime with the static feed's schedule-version prefix
 * removed:
 *
 *   static  ASP26GEN-1038-Sunday-00_000600_1..S03R
 *   RT                              000600_1..S03R
 *
 * nigiri resolves a realtime trip by exact id lookup, so nothing matches and
 * the whole feed serves schedule-only: every subway departure comes back with
 * `realTime: false` and MOTIS logs `trip_resolve_error` on effectively every
 * subway trip update. Measured against the live `nyct/gtfs-ace` feed on
 * 2026-09-02, 0 of 121 realtime ids were present in the static feed; after
 * stripping, 87 were.
 *
 * The fix belongs here rather than in MOTIS. The two id spaces genuinely
 * differ, and reconciling them is the importer's job.
 */
import JSZip from 'jszip'
import { parseCsvRows, serializeCsvRows, headerIndex } from './csv'

/**
 * Feeds whose GTFS-RT trip_id is a suffix of the static trip_id.
 *
 * Gated on feed identity, never on a shape heuristic over the ids. MTA Bus
 * Time publishes full static trip_ids that already match
 * (`MQ_C6-Weekday-030000_M66_502`), and stripping those would break feeds that
 * work today.
 */
export const SUFFIX_TRIP_ID_FEEDS = new Set<string>([
  'f-dr5r-nyctsubway',
  // Staten Island Railway, where it is catalogued as its own feed. Same id
  // shape (`L0S5-SI-2017-S06_147100_SI.N03R`), but matching there is
  // unverified — a third party reported in 2025 that the strip alone yielded
  // no matches, implying a second problem. Treat an SIR failure as its own
  // investigation rather than a regression of this change.
  'f-dr5r-statenislandrailway',
])

/**
 * Files carrying a `trip_id` column that has to move with trips.txt.
 *
 * `transfers.txt` is not in the list: GTFS spells its trip references
 * `from_trip_id` and `to_trip_id`, and barrelman overwrites that file with
 * stop-keyed walking transfers later in the import anyway.
 */
const TRIP_ID_BEARING_FILES = ['stop_times.txt', 'frequencies.txt', 'attributions.txt'] as const

/**
 * The remainder MTA leaves behind: six digits of origin time past midnight,
 * then the route and direction path.
 *
 * Asserted on the result, not on where the split landed. The prefix encodes
 * the schedule version and does change (`BSP26GEN-D085-Weekday-00`,
 * `ASP26GEN-1038-Sunday-00`, `L0S5-SI-2017-S06`). None contains an underscore
 * today, which is why splitting on the first one works — that is a property of
 * the current format, not a promise. Without this guard a future prefix
 * containing an underscore would silently produce ids matching nothing, which
 * looks exactly like the bug being fixed.
 *
 * It also makes the rewrite idempotent: an already-stripped id fails the guard
 * on the first row, so re-importing a zip from disk leaves it alone.
 */
const SUFFIX_SHAPE = /^\d{6}_/

const stripPrefix = (tripId: string): string => {
  const at = tripId.indexOf('_')
  return at === -1 ? tripId : tripId.slice(at + 1)
}

export interface SuffixRewriteOverlap {
  suffix: string
  serviceA: string
  serviceB: string
  /** Up to five shared dates, as YYYYMMDD. Enough to diagnose, not a dump. */
  sharedDates: string[]
}

export interface SuffixRewriteResult {
  applied: boolean
  /** Set when the rewrite was skipped. The feed then imports unmodified. */
  skipReason?: 'not-in-scope' | 'shape-guard' | 'date-overlap' | 'no-trips'
  /** Rows whose trip_id changed, per file. */
  rewrittenRows: Record<string, number>
  /** Distinct suffixes claimed by more than one trip. */
  collidingSuffixes: number
  overlaps: SuffixRewriteOverlap[]
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
 * Rewrite a feed's static trip_ids to the suffix form its realtime feed uses.
 *
 * Stripping the prefix creates duplicate trip_ids — about 2,660 in the current
 * subway feed, 17,327 distinct ids across 20,621 trips, almost all
 * Saturday-versus-Sunday variants of the same run. nigiri tolerates that:
 * `trip_id_to_idx_` is a sorted vector with no uniqueness constraint,
 * `resolve_static_trip_id` walks every id match, and `resolve_trip` picks
 * between them with `is_transport_active` against the traffic-day bitfield
 * using the realtime `start_date`.
 *
 * That only holds while no two colliding trips run on the same date. If two
 * do, nigiri's trip-update path (`src/rt/gtfsrt_update.cc`) applies the update
 * inside the `resolve_static` callback and returns `kContinue` without
 * breaking, so the update lands on both — including a transport that is not
 * running — and counts two successes. The match-rate metric would look fine.
 * Hence the disjointness check, which runs on every import rather than once at
 * qualification: the collision set changes whenever MTA reshuffles services.
 *
 * On any violation the feed imports unmodified. Schedule-only subway is what
 * this instance already serves; failing the import would take the feed stale
 * or absent, which is worse than the problem being avoided.
 *
 * @param extendCalendar must mirror MOTIS's per-dataset `extend_calendar`.
 * Barrelman never enables it, but with it on MOTIS routes against synthetic
 * calendar extensions and this check would have to expand against those — the
 * subway feed's seasonal variants (`Sunday-H-20260526-20260907` against
 * `Sunday-H-20260908-20261031`) do overlap once extended, even though the
 * file says they do not. Throwing beats guessing under a flag we cannot read.
 */
export async function normalizeSuffixTripIds(
  buffer: ArrayBuffer,
  feed: { feedId: string; onestopId?: string },
  options: { extendCalendar?: boolean } = {},
): Promise<{ buffer: ArrayBuffer; result: SuffixRewriteResult }> {
  const skip = (skipReason: SuffixRewriteResult['skipReason'], extra: Partial<SuffixRewriteResult> = {}) => ({
    buffer,
    result: { applied: false, skipReason, rewrittenRows: {}, collidingSuffixes: 0, overlaps: [], ...extra },
  })

  const identity = feed.onestopId || feed.feedId
  if (!SUFFIX_TRIP_ID_FEEDS.has(identity)) return skip('not-in-scope')

  if (options.extendCalendar) {
    throw new Error(
      'normalizeSuffixTripIds: extend_calendar is enabled; the disjointness check must ' +
        'expand against the extended calendar before this feed can be rewritten safely.',
    )
  }

  const zip = await JSZip.loadAsync(buffer)

  const tripsFile = zip.file('trips.txt')
  if (!tripsFile) return skip('no-trips')

  const tripRows = parseCsvRows(await tripsFile.async('string'))
  if (!tripRows.length) return skip('no-trips')

  const tripCols = headerIndex(tripRows[0]!)
  const tripIdCol = tripCols.get('trip_id')
  const serviceIdCol = tripCols.get('service_id')
  if (tripIdCol === undefined) return skip('no-trips')

  // ── Shape guard, and group trips by the suffix they will collapse onto ──
  const bySuffix = new Map<string, string[]>()
  for (const cells of tripRows.slice(1)) {
    const original = cells[tripIdCol]?.trim()
    if (!original) continue

    const suffix = stripPrefix(original)
    if (!SUFFIX_SHAPE.test(suffix)) return skip('shape-guard')

    const services = bySuffix.get(suffix)
    const serviceId = serviceIdCol === undefined ? '' : (cells[serviceIdCol]?.trim() ?? '')
    if (services) services.push(serviceId)
    else bySuffix.set(suffix, [serviceId])
  }

  // ── Disjointness ────────────────────────────────────────────────────
  const read = async (name: string) => {
    const file = zip.file(name)
    return file ? await file.async('string') : null
  }
  const serviceDates = expandServiceDates(await read('calendar.txt'), await read('calendar_dates.txt'))

  const overlaps: SuffixRewriteOverlap[] = []
  let collidingSuffixes = 0
  for (const [suffix, services] of bySuffix) {
    if (services.length < 2) continue
    collidingSuffixes++
    for (let i = 0; i < services.length; i++) {
      for (let j = i + 1; j < services.length; j++) {
        const a = serviceDates.get(services[i]!)
        const b = serviceDates.get(services[j]!)
        if (!a || !b) continue
        const shared = [...a].filter(d => b.has(d))
        if (shared.length) {
          overlaps.push({
            suffix,
            serviceA: services[i]!,
            serviceB: services[j]!,
            sharedDates: shared.slice(0, 5),
          })
        }
      }
    }
  }

  if (overlaps.length) return skip('date-overlap', { collidingSuffixes, overlaps })

  // ── Rewrite ─────────────────────────────────────────────────────────
  const rewrittenRows: Record<string, number> = {}

  const rewriteFile = async (name: string) => {
    const file = zip.file(name)
    if (!file) return

    const rows = parseCsvRows(await file.async('string'))
    if (!rows.length) return
    const col = headerIndex(rows[0]!).get('trip_id')
    if (col === undefined) return

    let changed = 0
    for (const cells of rows.slice(1)) {
      const original = cells[col]
      if (original === undefined) continue
      const suffix = stripPrefix(original.trim())
      if (suffix !== original) {
        cells[col] = suffix
        changed++
      }
    }

    zip.file(name, serializeCsvRows(rows))
    rewrittenRows[name] = changed
  }

  await rewriteFile('trips.txt')
  for (const name of TRIP_ID_BEARING_FILES) await rewriteFile(name)

  // JSZip defaults to STORE. Without this the rewritten subway feed lands on
  // the bind mount MOTIS imports from at 43 MB instead of 5.6 MB.
  const rewritten = await zip.generateAsync({ type: 'arraybuffer', compression: 'DEFLATE' })
  return {
    buffer: rewritten,
    result: { applied: true, rewrittenRows, collidingSuffixes, overlaps: [] },
  }
}
