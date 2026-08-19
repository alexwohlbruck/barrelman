/**
 * Service Alerts Service
 *
 * Fetches and serves GTFS-RT ServiceAlert data from transit feeds — the
 * agency's own word on what is disrupted, keyed by the entities each alert
 * informs (agency / route / route type / trip / stop).
 *
 * Like vehicle positions, alert feed URLs live in `gtfs_feeds.rt_urls`
 * (discovered from Transitland during import) and responses are cached
 * briefly so a board that refreshes every few seconds doesn't hammer the
 * upstream feed.
 *
 * Callers filter by what they are looking at — a route page, a stop board, a
 * planned trip — and get back only the alerts informing those entities,
 * newest and most severe first.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import GtfsRealtimeBindings from 'gtfs-realtime-bindings'
import { LRUCache } from 'lru-cache'
import { readAlertExtensions } from './alert-extensions'

// Decode through the live import binding at call time rather than destructuring
// at load, so a test mock of `gtfs-realtime-bindings` applies even when this
// module gets imported (transitively) before the mock is registered.
const decodeFeedMessage = (buf: Uint8Array) =>
  GtfsRealtimeBindings.transit_realtime.FeedMessage.decode(buf)

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

// ── Types ───────────────────────────────────────────────────────────

/**
 * One entity an alert informs. Every field is optional in the spec: an alert
 * naming only an agency applies to everything that agency runs, one naming a
 * route and a stop applies to that route only at that stop.
 */
export interface InformedEntity {
  agencyId?: string
  routeId?: string
  routeType?: number
  directionId?: number
  tripId?: string
  stopId?: string
}

/** A window during which the alert applies. Both ends are optional. */
export interface AlertActivePeriod {
  /** ISO timestamp; absent means "already in effect". */
  start?: string
  /** ISO timestamp; absent means "until further notice". */
  end?: string
}

export interface ServiceAlert {
  /** Stable within a feed — the RT entity id, feed-prefixed. */
  id: string
  feedId: string
  /** GTFS-RT cause, e.g. `CONSTRUCTION`. `UNKNOWN_CAUSE` when unset. */
  cause: string
  /** GTFS-RT effect, e.g. `DETOUR`. `UNKNOWN_EFFECT` when unset. */
  effect: string
  /** GTFS-RT severity, e.g. `WARNING`. `UNKNOWN_SEVERITY` when unset. */
  severity: string
  /** Short summary — the line an agency writes to be read at a glance. */
  header: string
  /** The long prose. Often several paragraphs; clients collapse it. */
  description?: string
  /** Agency page with more detail, when the feed supplies one. */
  url?: string
  /**
   * The agency's own category for this alert, verbatim — "Planned - Detour",
   * "Station Notice". Only feeds carrying a vendor extension supply it; it is
   * the agency's wording, not ours, so it is untranslated and better suited to
   * a tooltip than a chip.
   */
  category?: string
  /** The agency scheduled this rather than reacting to it. */
  planned?: boolean
  /** When the agency published the alert, when the feed says. */
  postedAt?: string
  /** When the agency last revised it. */
  updatedAt?: string
  activePeriods: AlertActivePeriod[]
  informedEntities: InformedEntity[]
}

/**
 * What the caller is looking at. Ids are feed-local (no feed prefix), so
 * `feedId` should accompany them — without it, an id from one agency could
 * collide with an unrelated id from another.
 *
 * Lists rather than single ids because a stop board is several routes at once,
 * and a planned trip is several legs at once.
 */
export interface ServiceAlertsRequest {
  feedId?: string
  routeIds?: string[]
  stopIds?: string[]
  tripIds?: string[]
  /** Include alerts whose active period hasn't started yet. Default false. */
  includeUpcoming?: boolean
}

export interface ServiceAlertsResponse {
  alerts: ServiceAlert[]
  /** Per-feed header timestamp, so clients can tell how fresh this is. */
  feedTimestamps: Record<string, string>
}

// ── Cache ───────────────────────────────────────────────────────────

interface CachedAlertFeed {
  alerts: ServiceAlert[]
  feedTimestamp: string
}

/**
 * Per-feed cache. Alerts move on the scale of minutes, not seconds — an
 * agency writes prose about a detour, it doesn't tick like a bus position —
 * so this TTL is far longer than the vehicle feed's.
 */
const alertCache = new LRUCache<string, CachedAlertFeed>({
  max: 200,
  ttl: 60_000, // 1 minute
})

/** Convert a protobuf timestamp (Long or number) to seconds. */
function toSeconds(ts: any): number {
  if (typeof ts === 'number') return ts
  if (ts && typeof ts.toNumber === 'function') return ts.toNumber()
  if (ts && typeof ts.low === 'number') return ts.low + (ts.high || 0) * 0x100000000
  return NaN
}

/** Seconds → ISO string, or undefined when the field was absent/garbage. */
function toIso(ts: any): string | undefined {
  if (ts == null) return undefined
  const sec = toSeconds(ts)
  return Number.isFinite(sec) ? new Date(sec * 1000).toISOString() : undefined
}

// ── Enum decoding ───────────────────────────────────────────────────

/**
 * The protobuf bindings hand back numeric enum values unless the decoder was
 * asked for strings. Map them to the spec's names so clients can switch on a
 * word rather than a magic number, and so a feed that already sends strings
 * passes through untouched.
 */
const CAUSE_NAMES = [
  'UNKNOWN_CAUSE', 'OTHER_CAUSE', 'TECHNICAL_PROBLEM', 'STRIKE',
  'DEMONSTRATION', 'ACCIDENT', 'HOLIDAY', 'WEATHER', 'MAINTENANCE',
  'CONSTRUCTION', 'POLICE_ACTIVITY', 'MEDICAL_EMERGENCY',
]

const EFFECT_NAMES = [
  'NO_SERVICE', 'REDUCED_SERVICE', 'SIGNIFICANT_DELAYS', 'DETOUR',
  'ADDITIONAL_SERVICE', 'MODIFIED_SERVICE', 'OTHER_EFFECT', 'UNKNOWN_EFFECT',
  'STOP_MOVED', 'NO_EFFECT', 'ACCESSIBILITY_ISSUE',
]

const SEVERITY_NAMES = [
  'UNKNOWN_SEVERITY', 'UNKNOWN_SEVERITY', 'INFO', 'WARNING', 'SEVERE',
]

/**
 * GTFS-RT numbers causes from 1 and effects from 1, so the wire value is one
 * past its index in the name table. Severity is numbered from 1 with a gap at
 * 0, which `SEVERITY_NAMES` encodes directly.
 */
function enumName(value: any, names: string[], fallback: string, offset = 1): string {
  if (typeof value === 'string') return value
  if (typeof value !== 'number') return fallback
  return names[value - offset] ?? fallback
}

/** Take the extension's answer only where the feed left the spec field unset. */
function withFallback(declared: string, unsetValue: string, fromExtension?: string): string {
  return declared === unsetValue && fromExtension ? fromExtension : declared
}

/** Pull the first translation out of a GTFS-RT TranslatedString. */
function translatedText(field: any): string | undefined {
  const text = field?.translation?.[0]?.text
  return typeof text === 'string' && text.length > 0 ? text : undefined
}

// ── Feed discovery ──────────────────────────────────────────────────

interface FeedAlertInfo {
  feedId: string
  alertUrl: string
  headers?: Record<string, string>
}

/**
 * Query gtfs_feeds for feeds publishing a service alerts URL.
 *
 * Discovery records each URL's kind, so the alert feed is picked by type. It
 * has to be: an agency's realtime feeds are found through its operator, which
 * lists all of them, so a subway feed's row also carries the bus alert URL —
 * and picking the first URL matching /alert/i could take either.
 */
async function getFeedsWithAlerts(feedIdFilter?: string): Promise<FeedAlertInfo[]> {
  const result = await db.execute(sql`
    SELECT feed_id, rt_urls
    FROM gtfs_feeds
    WHERE rt_urls IS NOT NULL
    ${feedIdFilter ? sql`AND feed_id = ${feedIdFilter}` : sql.empty()}
  `)

  const feeds: FeedAlertInfo[] = []

  for (const row of result as any[]) {
    const rtUrls: Array<{ url: string; headers?: Record<string, string>; type?: string }> =
      typeof row.rt_urls === 'string' ? JSON.parse(row.rt_urls) : row.rt_urls

    if (!Array.isArray(rtUrls)) continue

    // Discovery records what each URL is; the regex is the fallback for rows
    // written before it did. A feed with a single RT URL is worth trying
    // regardless — combined feeds carry alerts alongside everything else, and
    // the decode below only reads the alerts.
    const entry =
      rtUrls.find(u => u.type === 'alerts') ??
      rtUrls.find(u => !u.type && /alert/i.test(u.url)) ??
      (rtUrls.length === 1 ? rtUrls[0] : undefined)

    if (entry) {
      feeds.push({
        feedId: row.feed_id,
        alertUrl: entry.url,
        headers: entry.headers,
      })
    }
  }

  return feeds
}

// ── Fetch + normalise ───────────────────────────────────────────────

/**
 * Fetch and decode one feed's alerts. Failures are swallowed — an agency
 * whose alert feed is down shouldn't blank out the alerts of every other
 * agency on the board.
 */
async function fetchFeedAlerts(
  feed: FeedAlertInfo,
  fetchFn: FetchFn,
): Promise<CachedAlertFeed> {
  const cached = alertCache.get(feed.feedId)
  if (cached) return cached

  const empty: CachedAlertFeed = { alerts: [], feedTimestamp: '' }

  try {
    const headers: Record<string, string> = { Accept: '*/*' }
    if (feed.headers) Object.assign(headers, feed.headers)

    const response = await fetchFn(feed.alertUrl, {
      headers,
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return empty

    const buffer = new Uint8Array(await response.arrayBuffer())
    const feedMessage = decodeFeedMessage(buffer)

    // Whatever the stock decoder dropped: agency categories and posting times
    // carried in vendor extensions, keyed by the entity's position in the feed.
    const enrichments = readAlertExtensions(buffer)

    const alerts: ServiceAlert[] = []
    let entityIndex = -1

    for (const entity of feedMessage.entity ?? []) {
      entityIndex++
      const alert = (entity as any).alert
      if (!alert) continue

      const header = translatedText(alert.headerText)
      const description = translatedText(alert.descriptionText)
      // An alert with no words is not something we can show a rider.
      if (!header && !description) continue

      const extra = enrichments.get(entityIndex)

      alerts.push({
        id: `${feed.feedId}_${(entity as any).id ?? alerts.length}`,
        feedId: feed.feedId,
        cause: enumName(alert.cause, CAUSE_NAMES, 'UNKNOWN_CAUSE'),
        // The feed's own words win; an extension only fills a gap it left.
        effect: withFallback(
          enumName(alert.effect, EFFECT_NAMES, 'UNKNOWN_EFFECT'),
          'UNKNOWN_EFFECT',
          extra?.effect,
        ),
        severity: withFallback(
          enumName(alert.severityLevel, SEVERITY_NAMES, 'UNKNOWN_SEVERITY', 0),
          'UNKNOWN_SEVERITY',
          extra?.severity,
        ),
        header: header ?? description!,
        description: header ? description : undefined,
        url: translatedText(alert.url),
        category: extra?.category,
        planned: extra?.planned,
        postedAt: extra?.postedAt,
        updatedAt: extra?.updatedAt,
        activePeriods: (alert.activePeriod ?? []).map((p: any) => ({
          start: toIso(p.start),
          end: toIso(p.end),
        })),
        informedEntities: (alert.informedEntity ?? []).map((e: any) => ({
          agencyId: e.agencyId || undefined,
          routeId: e.routeId || undefined,
          routeType: typeof e.routeType === 'number' ? e.routeType : undefined,
          directionId: typeof e.directionId === 'number' ? e.directionId : undefined,
          tripId: e.trip?.tripId || undefined,
          stopId: e.stopId || undefined,
        })),
      })
    }

    const result: CachedAlertFeed = {
      alerts,
      feedTimestamp: toIso(feedMessage.header?.timestamp) ?? '',
    }
    alertCache.set(feed.feedId, result)
    return result
  } catch {
    return empty
  }
}

// ── Matching ────────────────────────────────────────────────────────

/**
 * Is this alert in effect (or, when `includeUpcoming`, going to be)?
 *
 * An alert with no active period at all is in effect now — the spec's way of
 * saying "until further notice".
 */
export function isAlertActive(
  alert: ServiceAlert,
  now: number,
  includeUpcoming = false,
): boolean {
  if (alert.activePeriods.length === 0) return true

  return alert.activePeriods.some(period => {
    const start = period.start ? Date.parse(period.start) : null
    const end = period.end ? Date.parse(period.end) : null
    if (end !== null && end < now) return false
    if (start !== null && start > now) return includeUpcoming
    return true
  })
}

/**
 * Does this alert inform what the caller is looking at?
 *
 * An informed entity constrains on the fields it names — but only on the
 * dimensions the caller can actually answer for. MTA scopes almost every
 * subway alert to a route *and* a stop (`{routeId: "N", stopId: "R09"}`);
 * a route page asks about the route and knows nothing about stops, and if the
 * unanswerable stop constraint were allowed to veto, that page would show
 * nothing at all. So a dimension the caller didn't supply is not checked.
 *
 * A caller that *does* supply a dimension gets the precision back: asking
 * about route B48 at stop S1 will not match an entity scoped to B48 at S2.
 *
 * At least one dimension has to be both named and asked about, so a
 * trip-scoped alert doesn't surface on a stop board that never mentioned trips.
 * An entity naming only an agency constrains nothing and matches everywhere,
 * which is what carries system-wide notices onto every page.
 *
 * With no filters at all, every alert matches.
 */
export function alertMatches(
  alert: ServiceAlert,
  filter: { routeIds?: string[]; stopIds?: string[]; tripIds?: string[] },
): boolean {
  const routes = filter.routeIds ?? []
  const stops = filter.stopIds ?? []
  const trips = filter.tripIds ?? []
  if (!routes.length && !stops.length && !trips.length) return true

  return alert.informedEntities.some(entity => {
    // Named by the entity, and something the caller asked about.
    const dimensions: Array<[string | undefined, string[]]> = [
      [entity.routeId, routes],
      [entity.stopId, stops],
      [entity.tripId, trips],
    ]

    // Nothing checkable (agency-wide, or route-type wide) — a blanket notice.
    if (dimensions.every(([value]) => !value)) return true

    let checked = 0
    for (const [value, asked] of dimensions) {
      if (!value || !asked.length) continue
      if (!asked.includes(value)) return false
      checked++
    }

    return checked > 0
  })
}

/** Severity ordering for display: the worst news goes first. */
const SEVERITY_RANK: Record<string, number> = {
  SEVERE: 3,
  WARNING: 2,
  INFO: 1,
  UNKNOWN_SEVERITY: 0,
}

/**
 * An unrated alert that says service is suspended is not "unknown severity" in
 * any useful sense. Feeds are inconsistent about setting severityLevel, so
 * fall back to what the effect implies.
 */
const EFFECT_RANK: Record<string, number> = {
  NO_SERVICE: 3,
  SIGNIFICANT_DELAYS: 2,
  REDUCED_SERVICE: 2,
  DETOUR: 2,
  STOP_MOVED: 2,
  MODIFIED_SERVICE: 1,
  ACCESSIBILITY_ISSUE: 1,
  ADDITIONAL_SERVICE: 1,
}

export function alertRank(alert: ServiceAlert): number {
  const rated = SEVERITY_RANK[alert.severity] ?? 0
  return rated > 0 ? rated : (EFFECT_RANK[alert.effect] ?? 0)
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Fetch service alerts, filtered to the entities the caller cares about.
 *
 * Without a feedId this reads every feed that publishes alerts, which is only
 * sensible for a small deployment — callers looking at one route or stop
 * should always pass the feed they got that id from.
 */
export async function getServiceAlerts(
  request: ServiceAlertsRequest = {},
  fetchFn: FetchFn = globalThis.fetch,
): Promise<ServiceAlertsResponse> {
  const feeds = await getFeedsWithAlerts(request.feedId)
  if (feeds.length === 0) return { alerts: [], feedTimestamps: {} }

  const results = await Promise.all(
    feeds.map(async feed => ({ feed, data: await fetchFeedAlerts(feed, fetchFn) })),
  )

  const now = Date.now()
  const feedTimestamps: Record<string, string> = {}
  const alerts: ServiceAlert[] = []

  for (const { feed, data } of results) {
    if (data.feedTimestamp) feedTimestamps[feed.feedId] = data.feedTimestamp

    for (const alert of data.alerts) {
      if (!isAlertActive(alert, now, request.includeUpcoming)) continue
      if (!alertMatches(alert, request)) continue
      alerts.push(alert)
    }
  }

  // Worst first, then the most recently started — a rider scanning a list
  // wants the thing that will stop them travelling at the top.
  alerts.sort((a, b) => {
    const rank = alertRank(b) - alertRank(a)
    if (rank !== 0) return rank
    return (startOf(b) ?? 0) - (startOf(a) ?? 0)
  })

  return { alerts, feedTimestamps }
}

/** Earliest start across an alert's periods, for stable ordering. */
function startOf(alert: ServiceAlert): number | null {
  const starts = alert.activePeriods
    .map(p => (p.start ? Date.parse(p.start) : null))
    .filter((v): v is number => v !== null)
  return starts.length ? Math.min(...starts) : null
}
