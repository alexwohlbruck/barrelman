/**
 * GTFS Import Service
 *
 * Handles downloading GTFS feeds from Transitland, importing stop and route
 * data into PostGIS, and pre-computing walking transfers between nearby stops
 * via GraphHopper.
 *
 * The import pipeline:
 *   1. Fetch feed list from Transitland API (filtered by region)
 *   2. Download each GTFS ZIP
 *   3. Parse stops.txt, routes.txt, trips.txt, stop_times.txt
 *   4. Import stops/routes into PostGIS with spatial geometry
 *   5. Derive stop→route associations from trips + stop_times
 *   6. Pre-compute walking transfers between nearby stop pairs via GraphHopper
 *   7. Write transfers.txt into each feed for MOTIS
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { parse } from 'csv-parse/sync'
import { readFileSync, writeFileSync } from 'fs'
import { join } from 'path'
import { type FetchFn } from './transit.service'

// ── Types ───────────────────────────────────────────────────────────

export interface GtfsRtUrl {
  url: string
  headers?: Record<string, string>
}

export interface GtfsFeedInfo {
  feedId: string
  onestopId: string
  name: string
  url: string
  region?: string
  /** GTFS-RT feed URLs discovered from Transitland (trip updates + vehicle positions) */
  rtUrls?: GtfsRtUrl[]
  /** DMFR license block from the atlas-backed Transitland catalog. */
  license?: {
    redistribution_allowed?: string
    use_without_attribution?: string
    commercial_use_allowed?: string
    [k: string]: unknown
  }
}

/**
 * Curated, license-aware feed selection (DMFR-style). Applied to the feed list
 * from the atlas-backed Transitland catalog so we only import feeds we're
 * allowed to redistribute and, optionally, an explicit allow/deny list.
 *
 *  - excludeUnredistributable: drop feeds whose DMFR license explicitly
 *    disallows redistribution (license.redistribution_allowed === 'no').
 *  - allow: if non-empty, keep ONLY these onestop_ids (or feed ids).
 *  - deny: always drop these onestop_ids (or feed ids).
 */
export interface FeedSelection {
  allow?: string[]
  deny?: string[]
  excludeUnredistributable?: boolean
}

export function selectFeeds(
  feeds: GtfsFeedInfo[],
  selection: FeedSelection = {},
): GtfsFeedInfo[] {
  const allow = new Set(selection.allow ?? [])
  const deny = new Set(selection.deny ?? [])
  return feeds.filter((f) => {
    const ids = [f.onestopId, f.feedId]
    if (deny.size && ids.some((id) => deny.has(id))) return false
    if (allow.size && !ids.some((id) => allow.has(id))) return false
    if (
      selection.excludeUnredistributable &&
      f.license?.redistribution_allowed === 'no'
    ) {
      return false
    }
    return true
  })
}

export interface ImportResult {
  feedId: string
  stopsImported: number
  routesImported: number
  stopRoutesImported: number
}

export interface TransferPair {
  fromStopId: string
  toStopId: string
  fromFeedId: string
  toFeedId: string
  fromLat: number
  fromLng: number
  toLat: number
  toLng: number
}

export interface ComputedTransfer {
  fromStopId: string
  toStopId: string
  fromFeedId: string
  toFeedId: string
  /** Walking time in seconds */
  walkTime: number
  /** Walking distance in meters */
  walkDistance: number
}

// ── Transitland feed discovery ──────────────────────────────────────

/**
 * Region bounding boxes for GTFS feed filtering.
 * Used with Transitland's bbox parameter.
 */
const REGION_BBOXES: Record<string, string> = {
  nc: '-84.5,33.8,-75.4,36.6',    // North Carolina
  nyc: '-74.3,40.45,-73.7,40.95', // NYC metro area (NJ Transit, MTA, PATH)
  chicago: '-87.95,41.64,-87.52,42.07', // Chicago metro (CTA L + Metra; downtown Loop)
  southeast: '-92,24,-75,37',       // SE United States
  us: '-125,24,-66,50',            // Continental US
}

/**
 * Fetch GTFS feed list from Transitland API.
 *
 * Returns feed download URLs filtered by region. For 'global', returns
 * all feeds without bbox filtering.
 */
export async function fetchFeedList(
  region: string,
  apiKey: string,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<GtfsFeedInfo[]> {
  const feeds: GtfsFeedInfo[] = []
  let nextUrl: string | null = buildFeedListUrl(region, apiKey)

  while (nextUrl) {
    const response = await fetchFn(nextUrl)
    if (!response.ok) {
      throw new Error(`Transitland API returned ${response.status}: ${await response.text()}`)
    }

    const data = await response.json() as any
    for (const feed of data.feeds || []) {
      // Only include GTFS feeds with a download URL
      const spec = feed.spec || ''
      if (spec !== 'gtfs' && spec !== 'GTFS') continue

      const url = feed.urls?.static_current
      if (!url) continue

      feeds.push({
        feedId: String(feed.id || feed.onestop_id || `feed_${feeds.length}`),
        onestopId: String(feed.onestop_id || ''),
        name: String(feed.name || feed.onestop_id || ''),
        url,
        region,
        license: feed.license ?? undefined,
      })
    }

    // Handle pagination
    nextUrl = data.meta?.next ? data.meta.next : null
  }

  // Discover GTFS-RT feeds and associate them with static feeds
  console.log(`Discovering GTFS-RT feeds for ${feeds.length} static feeds...`)
  const rtMap = await fetchRtFeedMap(feeds, apiKey, fetchFn)
  for (const feed of feeds) {
    const rtUrls = rtMap.get(feed.onestopId) || rtMap.get(feed.feedId)
    if (rtUrls?.length) {
      feed.rtUrls = rtUrls
    }
  }

  return feeds
}

/**
 * Fetch GTFS-RT feeds from Transitland and build a map from
 * static feed onestop_id → RT URLs.
 *
 * Transitland stores GTFS-RT as separate feed entries with
 * `spec: 'GTFS_RT'`. They follow the naming convention
 * `f-xxx-agency~rt` where the static feed is `f-xxx-agency`.
 *
 * We look up each static feed's expected RT onestop_id directly,
 * avoiding a full global scan of all RT feeds.
 */
async function fetchRtFeedMap(
  staticFeeds: GtfsFeedInfo[],
  apiKey: string,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<Map<string, GtfsRtUrl[]>> {
  const rtMap = new Map<string, GtfsRtUrl[]>()

  // Build candidate RT onestop_ids from static feeds
  const candidates = staticFeeds
    .filter(f => f.onestopId)
    .map(f => ({ staticOnestopId: f.onestopId, rtOnestopId: `${f.onestopId}~rt` }))

  if (!candidates.length) return rtMap

  // Batch lookup: query Transitland for each candidate RT feed.
  // Use small batches to avoid too many parallel requests.
  const BATCH_SIZE = 10
  for (let i = 0; i < candidates.length; i += BATCH_SIZE) {
    const batch = candidates.slice(i, i + BATCH_SIZE)
    const results = await Promise.allSettled(
      batch.map(async ({ staticOnestopId, rtOnestopId }) => {
        const url = `https://transit.land/api/v2/rest/feeds?apikey=${apiKey}&spec=GTFS_RT&onestop_id=${encodeURIComponent(rtOnestopId)}&limit=1`
        const response = await fetchFn(url)
        if (!response.ok) return null

        const data = await response.json() as any
        const feed = data.feeds?.[0]
        if (!feed) return null

        return { staticOnestopId, feed }
      }),
    )

    for (const result of results) {
      if (result.status !== 'fulfilled' || !result.value) continue
      const { staticOnestopId, feed } = result.value

      const urls = feed.urls || {}
      const rtUrls: GtfsRtUrl[] = []

      for (const key of ['realtime_trip_updates', 'realtime_vehicle_positions', 'realtime_alerts'] as const) {
        const url = urls[key]
        if (url) {
          const headers: Record<string, string> = {}
          if (feed.authorization?.type === 'header' && feed.authorization?.param_name) {
            headers[feed.authorization.param_name] = feed.authorization.param_value || ''
          }
          rtUrls.push(Object.keys(headers).length ? { url, headers } : { url })
        }
      }

      if (rtUrls.length) {
        rtMap.set(staticOnestopId, rtUrls)
      }
    }
  }

  return rtMap
}

function buildFeedListUrl(region: string, apiKey: string): string {
  const base = 'https://transit.land/api/v2/rest/feeds'
  const params = new URLSearchParams({
    apikey: apiKey,
    spec: 'gtfs',
    limit: '100',
  })

  if (region !== 'global') {
    const bbox = REGION_BBOXES[region]
    if (bbox) {
      params.set('bbox', bbox)
    }
  }

  return `${base}?${params}`
}

// ── RT URL discovery for existing feeds ─────────────────────────────

/**
 * Discover GTFS-RT URLs for feeds already in the database.
 *
 * The `gtfs_feeds` table stores Transitland's numeric feed ID as
 * `onestop_id` (e.g. "886"), but RT feed lookups require the full
 * `f-{geohash}-{agency}` onestop_id. This function:
 *
 *   1. Queries Transitland by numeric ID to resolve the real onestop_id
 *   2. Queries for the corresponding `{onestop_id}~rt` RT feed
 *   3. Extracts RT URLs and updates the database
 *
 * Returns a summary of how many feeds were checked / updated.
 */
export async function discoverRtUrls(
  feedId?: string,
  apiKey?: string,
  fetchFn: FetchFn = globalThis.fetch,
  onProgress?: (checked: number, total: number, feedId: string, found: boolean) => void,
  dryRun: boolean = false,
): Promise<{ checked: number; updated: number; errors: number }> {
  const key = apiKey || process.env.TRANSITLAND_API_KEY
  if (!key) throw new Error('TRANSITLAND_API_KEY is required')

  // Get feeds that need RT URL discovery
  const feedFilter = feedId
    ? `AND feed_id = '${feedId.replace(/'/g, "''")}'`
    : ''
  const result = await db.execute(sql.raw(`
    SELECT feed_id, onestop_id
    FROM gtfs_feeds
    WHERE onestop_id IS NOT NULL
      AND (rt_urls IS NULL OR rt_urls = '[]'::jsonb)
      ${feedFilter}
    ORDER BY feed_id
  `))

  const feeds = result as unknown as Array<{ feed_id: string; onestop_id: string }>
  let checked = 0
  let updated = 0
  let errors = 0

  for (const feed of feeds) {
    try {
      const rtUrls = await resolveRtUrlsForFeed(feed.onestop_id, key, fetchFn)
      checked++

      if (rtUrls.length > 0) {
        if (!dryRun) {
          const rtUrlsJson = JSON.stringify(rtUrls).replace(/'/g, "''")
          await db.execute(sql.raw(`
            UPDATE gtfs_feeds
            SET rt_urls = '${rtUrlsJson}'::jsonb
            WHERE feed_id = '${feed.feed_id.replace(/'/g, "''")}'
          `))
        }
        updated++
      }

      onProgress?.(checked, feeds.length, feed.feed_id, rtUrls.length > 0)

      // Rate limiting: 200ms between Transitland API calls
      if (checked < feeds.length) {
        await new Promise(r => setTimeout(r, 200))
      }
    } catch (err) {
      errors++
      checked++
      console.error(
        `[RT Discovery] Error for feed ${feed.feed_id}:`,
        err instanceof Error ? err.message : err,
      )
      onProgress?.(checked, feeds.length, feed.feed_id, false)
    }
  }

  return { checked, updated, errors }
}

/**
 * Resolve RT URLs for a single feed by its Transitland numeric ID.
 *
 * Steps:
 *   1. GET /feeds?id={numericId} to get the real onestop_id
 *   2. GET /feeds?spec=GTFS_RT&onestop_id={onestopId}~rt to find RT feed
 *   3. Extract realtime_vehicle_positions, realtime_trip_updates,
 *      realtime_alerts URLs from the response
 */
async function resolveRtUrlsForFeed(
  numericId: string,
  apiKey: string,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<GtfsRtUrl[]> {
  // Step 1: Resolve numeric ID to real onestop_id
  const feedUrl = `https://transit.land/api/v2/rest/feeds?apikey=${apiKey}&id=${encodeURIComponent(numericId)}&limit=1`
  const feedResponse = await fetchFn(feedUrl)
  if (!feedResponse.ok) return []

  const feedData = await feedResponse.json() as any
  const staticFeed = feedData.feeds?.[0]
  if (!staticFeed?.onestop_id) return []

  const realOnestopId = staticFeed.onestop_id as string

  // Step 2: Look up the RT feed using the {onestopId}~rt convention
  const rtOnestopId = `${realOnestopId}~rt`
  const rtUrl = `https://transit.land/api/v2/rest/feeds?apikey=${apiKey}&spec=GTFS_RT&onestop_id=${encodeURIComponent(rtOnestopId)}&limit=1`
  const rtResponse = await fetchFn(rtUrl)
  if (!rtResponse.ok) return []

  const rtData = await rtResponse.json() as any
  const rtFeed = rtData.feeds?.[0]
  if (!rtFeed) return []

  // Step 3: Extract RT URLs
  const urls = rtFeed.urls || {}
  const rtUrls: GtfsRtUrl[] = []

  for (const key of ['realtime_trip_updates', 'realtime_vehicle_positions', 'realtime_alerts'] as const) {
    const url = urls[key]
    if (url) {
      const headers: Record<string, string> = {}
      if (rtFeed.authorization?.type === 'header' && rtFeed.authorization?.param_name) {
        headers[rtFeed.authorization.param_name] = rtFeed.authorization.param_value || ''
      }
      rtUrls.push(Object.keys(headers).length ? { url, headers } : { url })
    }
  }

  return rtUrls
}

// ── GTFS ZIP parsing ────────────────────────────────────────────────

/**
 * Parse stops.txt from a GTFS ZIP buffer.
 * Returns an array of stop records ready for DB insert.
 */
export function parseStops(
  csvContent: string,
  feedId: string,
): Array<{
  stopId: string
  feedId: string
  stopName: string
  stopCode: string | null
  stopLat: number
  stopLon: number
  locationType: number
  parentStation: string | null
  wheelchairBoarding: number
  platformCode: string | null
}> {
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  return records
    .filter((r: any) => r.stop_lat && r.stop_lon)
    .map((r: any) => ({
      stopId: r.stop_id,
      feedId,
      stopName: r.stop_name || null,
      stopCode: r.stop_code || null,
      stopLat: parseFloat(r.stop_lat),
      stopLon: parseFloat(r.stop_lon),
      locationType: parseInt(r.location_type || '0', 10) || 0,
      parentStation: r.parent_station || null,
      wheelchairBoarding: parseInt(r.wheelchair_boarding || '0', 10) || 0,
      platformCode: r.platform_code || null,
    }))
}

/**
 * Parse routes.txt from a GTFS ZIP buffer.
 */
export function parseRoutes(
  csvContent: string,
  feedId: string,
  agencyMap: Map<string, string>,
): Array<{
  routeId: string
  feedId: string
  agencyId: string | null
  agencyName: string | null
  routeShortName: string | null
  routeLongName: string | null
  routeType: number
  routeColor: string | null
  routeTextColor: string | null
  routeUrl: string | null
}> {
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  return records.map((r: any) => ({
    routeId: r.route_id,
    feedId,
    agencyId: r.agency_id || null,
    agencyName: agencyMap.get(r.agency_id || '') || null,
    routeShortName: r.route_short_name || null,
    routeLongName: r.route_long_name || null,
    routeType: parseInt(r.route_type, 10) || 3,
    routeColor: r.route_color || null,
    routeTextColor: r.route_text_color || null,
    routeUrl: r.route_url || null,
  }))
}

/**
 * Parse agency.txt to build agency_id → agency_name map.
 */
export function parseAgencies(csvContent: string): Map<string, string> {
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  const map = new Map<string, string>()
  for (const r of records) {
    map.set(r.agency_id || '', r.agency_name || '')
  }
  return map
}

/**
 * Parse trips.txt and stop_times.txt to derive stop→route associations.
 *
 * Returns unique (stop_id, route_id) pairs. This is done by:
 * 1. Building a trip_id → route_id map from trips.txt
 * 2. For each stop_time, looking up the route_id via trip_id
 * 3. Collecting unique (stop_id, route_id) pairs
 */
type GtfsRecord = Record<string, string>

/** Parse a GTFS CSV file into row records. */
export function parseGtfsRecords(content: string): GtfsRecord[] {
  return parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })
}

/** Accept either raw CSV text or already-parsed records, so the importer can
 *  parse a large file (stop_times.txt) once and feed both derivers. */
function asRecords(content: string | GtfsRecord[]): GtfsRecord[] {
  return typeof content === 'string' ? parseGtfsRecords(content) : content
}

/** Parse a GTFS "HH:MM:SS" time (may exceed 24:00 for after-midnight) to
 *  seconds-since-midnight. Returns NaN if unparseable. */
export function gtfsTimeToSeconds(t: string | undefined): number {
  if (!t) return NaN
  const parts = t.split(':')
  if (parts.length < 3) return NaN
  const h = parseInt(parts[0], 10)
  const m = parseInt(parts[1], 10)
  const s = parseInt(parts[2], 10)
  if (Number.isNaN(h) || Number.isNaN(m) || Number.isNaN(s)) return NaN
  return h * 3600 + m * 60 + s
}

// ── Service-calendar resolution (representative-day counting) ───────
//
// GTFS service levels vary by concrete date: weekday vs weekend patterns,
// seasonal ranges, holiday exceptions, and headway-based (frequencies.txt)
// operation. Naively counting every trip of every weekday-flagged service_id
// breaks globally: calendar_dates-only services are invisible in mixed feeds,
// expired/seasonal services count forever, weekend-only stations vanish, and
// frequency-based feeds count 0–1 trips. Instead we expand, per feed, which
// service_ids run on which concrete dates over a bounded horizon, then pick
// three REPRESENTATIVE dates — the busiest Mon–Fri, Saturday and Sunday by
// total scheduled trips — and count service as it runs on those dates.

const DAY_MS = 86_400_000
const HORIZON_PAST_DAYS = 7
const HORIZON_FUTURE_DAYS = 60
/** Cap on the fallback horizon when the feed's validity doesn't cover today
 *  (expired or future feed) and we scan the feed's own window instead. */
const FALLBACK_HORIZON_DAYS = 90

const DOW_FIELDS = [
  'sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday',
] as const

/** GTFS YYYYMMDD → UTC ms at midnight, NaN if unparseable. */
function ymdToUtcMs(ymd: string | undefined): number {
  if (!ymd || ymd.length < 8) return NaN
  const y = parseInt(ymd.slice(0, 4), 10)
  const mo = parseInt(ymd.slice(4, 6), 10)
  const d = parseInt(ymd.slice(6, 8), 10)
  if (Number.isNaN(y) || Number.isNaN(mo) || Number.isNaN(d)) return NaN
  return Date.UTC(y, mo - 1, d)
}

const isoOf = (ms: number) => new Date(ms).toISOString().slice(0, 10)

export interface ServiceCalendarResolution {
  /** service_id → set of ISO dates (YYYY-MM-DD) it runs, within the horizon. */
  serviceDates: Map<string, Set<string>>
  /** Which inputs resolved service levels for this feed. */
  regime: 'calendar' | 'calendar_dates' | 'fail-open'
  /** Representative dates: busiest in-horizon Mon–Fri / Saturday / Sunday. */
  repWeekday: string | null
  repSaturday: string | null
  repSunday: string | null
  /** The horizon actually expanded (ISO), null when fail-open. */
  horizonStart: string | null
  horizonEnd: string | null
}

/**
 * Expand calendar.txt (weekday bits AND start_date/end_date) plus
 * calendar_dates.txt exceptions (1 = add, 2 = remove) into concrete
 * service dates over horizon = feed validity ∩ [today−7d, today+60d],
 * falling back to the feed's own validity window (capped) when it doesn't
 * overlap today. Then pick the representative Mon–Fri / Sat / Sun dates by
 * maximum total scheduled trips (trip totals per service_id from trips.txt).
 *
 * Returns the fail-open resolution (regime 'fail-open', no dates) when the
 * feed carries no calendar information at all — callers then treat every
 * trip as eligible rather than hiding everything.
 */
export function resolveServiceCalendar(
  calendarContent?: string | GtfsRecord[],
  calendarDatesContent?: string | GtfsRecord[],
  tripsContent?: string | GtfsRecord[],
  today: Date = new Date(),
): ServiceCalendarResolution {
  const cal = (calendarContent ? asRecords(calendarContent) : []).filter(r => r.service_id)
  const cd = (calendarDatesContent ? asRecords(calendarDatesContent) : [])
    .filter(r => r.service_id && r.date)

  const failOpen: ServiceCalendarResolution = {
    serviceDates: new Map(),
    regime: 'fail-open',
    repWeekday: null,
    repSaturday: null,
    repSunday: null,
    horizonStart: null,
    horizonEnd: null,
  }
  if (cal.length === 0 && cd.length === 0) return failOpen

  // Feed validity = union of calendar date ranges and calendar_dates dates.
  let feedMin = Infinity
  let feedMax = -Infinity
  for (const r of cal) {
    const s = ymdToUtcMs(r.start_date)
    const e = ymdToUtcMs(r.end_date)
    if (!Number.isNaN(s)) feedMin = Math.min(feedMin, s)
    if (!Number.isNaN(e)) feedMax = Math.max(feedMax, e)
  }
  for (const r of cd) {
    const d = ymdToUtcMs(r.date)
    if (Number.isNaN(d)) continue
    feedMin = Math.min(feedMin, d)
    feedMax = Math.max(feedMax, d)
  }
  const todayMs = Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate())
  if (!Number.isFinite(feedMin) || !Number.isFinite(feedMax) || feedMin > feedMax) {
    // Calendar rows exist but carry no parseable dates: assume currently valid.
    feedMin = todayMs - HORIZON_PAST_DAYS * DAY_MS
    feedMax = todayMs + HORIZON_FUTURE_DAYS * DAY_MS
  }

  let hStart = Math.max(feedMin, todayMs - HORIZON_PAST_DAYS * DAY_MS)
  let hEnd = Math.min(feedMax, todayMs + HORIZON_FUTURE_DAYS * DAY_MS)
  if (hStart > hEnd) {
    // Feed doesn't cover today (expired or not yet started): scan the feed's
    // own validity window instead so its service levels still resolve.
    hStart = feedMin
    hEnd = Math.min(feedMax, feedMin + (FALLBACK_HORIZON_DAYS - 1) * DAY_MS)
  }

  const serviceDates = new Map<string, Set<string>>()
  const addDate = (svc: string, iso: string) => {
    let s = serviceDates.get(svc)
    if (!s) {
      s = new Set()
      serviceDates.set(svc, s)
    }
    s.add(iso)
  }

  for (const r of cal) {
    const s = ymdToUtcMs(r.start_date)
    const e = ymdToUtcMs(r.end_date)
    const from = Math.max(hStart, Number.isNaN(s) ? hStart : s)
    const to = Math.min(hEnd, Number.isNaN(e) ? hEnd : e)
    for (let t = from; t <= to; t += DAY_MS) {
      if (r[DOW_FIELDS[new Date(t).getUTCDay()]] === '1') addDate(r.service_id, isoOf(t))
    }
  }
  for (const r of cd) {
    const d = ymdToUtcMs(r.date)
    if (Number.isNaN(d) || d < hStart || d > hEnd) continue
    if (r.exception_type === '1') addDate(r.service_id, isoOf(d))
    else if (r.exception_type === '2') serviceDates.get(r.service_id)?.delete(isoOf(d))
  }

  // Representative dates: per day class, the date with the most scheduled
  // trips (weighted by each active service's trip count from trips.txt).
  const tripTotals = new Map<string, number>()
  if (tripsContent) {
    for (const t of asRecords(tripsContent)) {
      if (!t.service_id) continue
      tripTotals.set(t.service_id, (tripTotals.get(t.service_id) ?? 0) + 1)
    }
  }
  const weight = (svc: string) => (tripsContent ? tripTotals.get(svc) ?? 0 : 1)

  const dateTotals = new Map<string, number>()
  for (const [svc, dates] of serviceDates) {
    const w = weight(svc)
    for (const iso of dates) dateTotals.set(iso, (dateTotals.get(iso) ?? 0) + w)
  }

  let repWeekday: string | null = null
  let repSaturday: string | null = null
  let repSunday: string | null = null
  let bestWk = -1
  let bestSat = -1
  let bestSun = -1
  for (const [iso, total] of [...dateTotals.entries()].sort()) {
    const dow = new Date(`${iso}T00:00:00Z`).getUTCDay()
    if (dow === 0) {
      if (total > bestSun) { bestSun = total; repSunday = iso }
    } else if (dow === 6) {
      if (total > bestSat) { bestSat = total; repSaturday = iso }
    } else if (total > bestWk) { bestWk = total; repWeekday = iso }
  }

  return {
    serviceDates,
    regime: cal.length > 0 ? 'calendar' : 'calendar_dates',
    repWeekday,
    repSaturday,
    repSunday,
    horizonStart: isoOf(hStart),
    horizonEnd: isoOf(hEnd),
  }
}

export interface RepresentativeServiceSets {
  /** service_ids active on the representative date; null = every trip is
   *  eligible (fail-open, feed has no calendar info at all). */
  weekday: Set<string> | null
  saturday: Set<string> | null
  sunday: Set<string> | null
}

/** service_ids active on each representative date. */
export function repServiceSets(res: ServiceCalendarResolution): RepresentativeServiceSets {
  if (res.regime === 'fail-open') return { weekday: null, saturday: null, sunday: null }
  const activeOn = (iso: string | null): Set<string> => {
    const s = new Set<string>()
    if (!iso) return s
    for (const [svc, dates] of res.serviceDates) if (dates.has(iso)) s.add(svc)
    return s
  }
  return {
    weekday: activeOn(res.repWeekday),
    saturday: activeOn(res.repSaturday),
    sunday: activeOn(res.repSunday),
  }
}

// ── Frequency-based (headway) service ────────────────────────────────

export interface FrequencyRow {
  startSec: number
  endSec: number
  headwaySecs: number
}

/** Parse frequencies.txt into trip_id → headway rows (invalid rows dropped). */
export function parseFrequencies(content: string | GtfsRecord[]): Map<string, FrequencyRow[]> {
  const map = new Map<string, FrequencyRow[]>()
  for (const r of asRecords(content)) {
    if (!r.trip_id) continue
    const startSec = gtfsTimeToSeconds(r.start_time)
    const endSec = gtfsTimeToSeconds(r.end_time)
    const headwaySecs = parseInt(r.headway_secs, 10)
    if (
      !Number.isFinite(startSec) || !Number.isFinite(endSec) ||
      !Number.isFinite(headwaySecs) || headwaySecs <= 0 || endSec <= startSec
    ) continue
    let rows = map.get(r.trip_id)
    if (!rows) {
      rows = []
      map.set(r.trip_id, rows)
    }
    rows.push({ startSec, endSec, headwaySecs })
  }
  return map
}

/** Sanity cap on departures expanded from one frequency row (a full service
 *  day at a 30 s headway is 2 880; anything far past that is bad data). */
const MAX_FREQ_DEPARTURES_PER_ROW = 5000

// Daytime window for "regular service": 06:00–22:00, tested on CLOCK seconds
// (raw % 86400) so GTFS 24:00+ after-midnight times classify by wall clock.
const DAYTIME_START_SEC = 6 * 3600
const DAYTIME_END_SEC = 22 * 3600

interface PairCounters {
  wkDay: number
  wkAny: number
  satDay: number
  satAny: number
  sunDay: number
  sunAny: number
}

export interface StopRouteServiceCounts {
  feedId: string
  stopId: string
  routeId: string
  /** Back-compat alias: identical to tripsWeekdayDay. */
  weekdayTrips: number
  /** Departures on the representative weekday, 06:00–22:00 clock window. */
  tripsWeekdayDay: number
  /** Departures on the representative weekday, all hours. */
  tripsWeekdayAny: number
  /** GREATEST of the representative Sat / Sun day-window departures. */
  tripsWeekendDay: number
  /** Max departures across the three representative days, all hours. */
  tripsAny: number
}

/**
 * Streaming-friendly per-(stop,route) service counter shared by the in-memory
 * deriver (deriveStopRoutes) and the streaming variant
 * (import/derive-rail-stop-routes.ts). Feed it one stop_times row at a time;
 * frequency-defined trips are buffered and expanded at finalize (departures =
 * headway multiples shifted by the stop's offset from the trip's first
 * departure). Every pair ever seen is emitted, 0 counts allowed — the table
 * must stay complete for routing lookups.
 */
export function createStopRouteAccumulator(opts: {
  tripToRoute: Map<string, string>
  tripToService: Map<string, string>
  frequencies?: Map<string, FrequencyRow[]>
  services: RepresentativeServiceSets
}) {
  const counters = new Map<string, PairCounters>()
  const order: string[] = [] // first-seen order, one entry per pair
  // Buffered stop events for frequency-defined trips, expanded at finalize
  // once the trip's first departure (= offset base) is known.
  const freqEvents = new Map<string, Array<{ key: string; depSec: number }>>()

  const eligibility = (tripId: string) => {
    const svc = opts.tripToService.get(tripId)
    const on = (s: Set<string> | null) => (s === null ? true : !!svc && s.has(svc))
    return {
      wk: on(opts.services.weekday),
      sat: on(opts.services.saturday),
      sun: on(opts.services.sunday),
    }
  }

  const bump = (
    c: PairCounters,
    el: { wk: boolean; sat: boolean; sun: boolean },
    rawSec: number,
    n = 1,
  ) => {
    const clock = ((rawSec % 86400) + 86400) % 86400
    const day = clock >= DAYTIME_START_SEC && clock < DAYTIME_END_SEC
    if (el.wk) {
      c.wkAny += n
      if (day) c.wkDay += n
    }
    if (el.sat) {
      c.satAny += n
      if (day) c.satDay += n
    }
    if (el.sun) {
      c.sunAny += n
      if (day) c.sunDay += n
    }
  }

  const entry = (key: string): PairCounters => {
    let c = counters.get(key)
    if (!c) {
      c = { wkDay: 0, wkAny: 0, satDay: 0, satAny: 0, sunDay: 0, sunAny: 0 }
      counters.set(key, c)
      order.push(key)
    }
    return c
  }

  return {
    /** One stop_times row: rawDepSec = gtfsTimeToSeconds(departure || arrival). */
    add(tripId: string, stopId: string, rawDepSec: number): void {
      const routeId = opts.tripToRoute.get(tripId)
      if (!routeId || !stopId) return
      const key = `${stopId}|${routeId}`
      const c = entry(key)
      const freq = opts.frequencies?.get(tripId)
      if (freq && freq.length > 0) {
        let evs = freqEvents.get(tripId)
        if (!evs) {
          evs = []
          freqEvents.set(tripId, evs)
        }
        if (Number.isFinite(rawDepSec)) evs.push({ key, depSec: rawDepSec })
        return
      }
      if (!Number.isFinite(rawDepSec)) return
      bump(c, eligibility(tripId), rawDepSec)
    },

    finalize(feedId: string): StopRouteServiceCounts[] {
      // Expand frequency-defined trips: each row yields
      // floor((end − start) / headway) departures at the trip's first stop;
      // a later stop sees them shifted by its scheduled offset from the
      // trip's first departure.
      for (const [tripId, evs] of freqEvents) {
        if (evs.length === 0) continue
        const el = eligibility(tripId)
        if (!el.wk && !el.sat && !el.sun) continue
        const first = Math.min(...evs.map(e => e.depSec))
        for (const row of opts.frequencies!.get(tripId)!) {
          const total = Math.min(
            Math.floor((row.endSec - row.startSec) / row.headwaySecs),
            MAX_FREQ_DEPARTURES_PER_ROW,
          )
          for (const ev of evs) {
            const c = counters.get(ev.key)!
            const offset = ev.depSec - first
            for (let k = 0; k < total; k++) {
              bump(c, el, row.startSec + k * row.headwaySecs + offset)
            }
          }
        }
      }

      return order.map(key => {
        const sep = key.lastIndexOf('|')
        const c = counters.get(key)!
        return {
          feedId,
          stopId: key.slice(0, sep),
          routeId: key.slice(sep + 1),
          weekdayTrips: c.wkDay,
          tripsWeekdayDay: c.wkDay,
          tripsWeekdayAny: c.wkAny,
          tripsWeekendDay: Math.max(c.satDay, c.sunDay),
          tripsAny: Math.max(c.wkAny, c.satAny, c.sunAny),
        }
      })
    },
  }
}

/**
 * Parse trips.txt + stop_times.txt (+ calendar + frequencies) to derive
 * stop→route associations annotated with representative-day service counts
 * (see resolveServiceCalendar / createStopRouteAccumulator).
 *
 * Every (stop_id, route_id) pair that appears anywhere in stop_times is still
 * returned (so the table stays complete for routing lookups); the counts let
 * the display layer filter out routes that only touch a station off-peak or on
 * a single "select" trip — the case the MTA diagrams / Apple omit (e.g. the
 * late-night-only 2 at a 1-line local stop, or the lone AM-rush 5 at Kingston
 * Av) — without erasing weekend-only or night-only stations. When calendar
 * info is missing entirely, all trips are treated as eligible (fail-open).
 */
export function deriveStopRoutes(
  tripsContent: string | GtfsRecord[],
  stopTimesContent: string | GtfsRecord[],
  feedId: string,
  calendarContent?: string | GtfsRecord[],
  calendarDatesContent?: string | GtfsRecord[],
  frequenciesContent?: string | GtfsRecord[],
  resolution?: ServiceCalendarResolution,
): StopRouteServiceCounts[] {
  const trips = asRecords(tripsContent)
  const res = resolution ?? resolveServiceCalendar(calendarContent, calendarDatesContent, trips)

  const tripToRoute = new Map<string, string>()
  const tripToService = new Map<string, string>()
  for (const trip of trips) {
    tripToRoute.set(trip.trip_id, trip.route_id)
    if (trip.service_id) tripToService.set(trip.trip_id, trip.service_id)
  }

  const acc = createStopRouteAccumulator({
    tripToRoute,
    tripToService,
    frequencies: frequenciesContent ? parseFrequencies(frequenciesContent) : undefined,
    services: repServiceSets(res),
  })
  for (const st of asRecords(stopTimesContent)) {
    acc.add(st.trip_id, st.stop_id, gtfsTimeToSeconds(st.departure_time || st.arrival_time))
  }
  return acc.finalize(feedId)
}

/**
 * Build a stop_id → normalised-station-id map from stops.txt.
 *
 * Normalised id = parent_station when the stop is a platform, else the stop
 * itself. Collapses platform-vs-station granularity so a subway leg (which
 * boards/alights a parent station) matches the platform ids in stop_times.
 */
export function parseStopParents(stopsContent: string): Map<string, string> {
  const records = parse(stopsContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })
  const map = new Map<string, string>()
  for (const r of records) {
    if (!r.stop_id) continue
    const parent = (r.parent_station || '').trim()
    map.set(r.stop_id, parent || r.stop_id)
  }
  return map
}

/**
 * Derive distinct trip patterns from trips.txt + stop_times.txt.
 *
 * One row per unique (route, direction, ordered station sequence). Stop ids are
 * normalised to their parent station via `stopParent` and the sequence is
 * stored comma-bounded (",a,b,c,") so a leg's board→alight run is a plain
 * substring match — see db.ts. Many trips collapse to a handful of patterns per
 * route, so the output is small even though stop_times is large.
 */
export function deriveTripPatterns(
  tripsContent: string | GtfsRecord[],
  stopTimesContent: string | GtfsRecord[],
  stopParent: Map<string, string>,
  feedId: string,
): Array<{ feedId: string; routeId: string; directionId: number | null; stopSeq: string }> {
  const trips = asRecords(tripsContent)
  const tripMeta = new Map<string, { routeId: string; directionId: number | null }>()
  for (const t of trips) {
    if (!t.trip_id || !t.route_id) continue
    const rawDir = t.direction_id
    const dir = rawDir != null && rawDir !== '' ? parseInt(rawDir, 10) : NaN
    tripMeta.set(t.trip_id, {
      routeId: t.route_id,
      directionId: Number.isNaN(dir) ? null : dir,
    })
  }

  // Group stop_times by trip, preserving order via stop_sequence.
  const stopTimes = asRecords(stopTimesContent)
  const byTrip = new Map<string, Array<{ seq: number; stopId: string }>>()
  for (const st of stopTimes) {
    if (!st.trip_id || !st.stop_id) continue
    let arr = byTrip.get(st.trip_id)
    if (!arr) {
      arr = []
      byTrip.set(st.trip_id, arr)
    }
    const seq = parseInt(st.stop_sequence ?? '', 10)
    arr.push({ seq: Number.isNaN(seq) ? arr.length : seq, stopId: st.stop_id })
  }

  // One normalised, comma-bounded sequence per trip; dedupe identical patterns.
  const seen = new Set<string>()
  const out: Array<{ feedId: string; routeId: string; directionId: number | null; stopSeq: string }> = []
  for (const [tripId, stops] of byTrip) {
    const meta = tripMeta.get(tripId)
    if (!meta) continue
    stops.sort((a, b) => a.seq - b.seq)
    const norm: string[] = []
    for (const s of stops) {
      const n = stopParent.get(s.stopId) ?? s.stopId
      if (norm[norm.length - 1] !== n) norm.push(n) // drop consecutive dups
    }
    // The comma-delimited scheme is ambiguous if a station id itself contains a
    // comma; skip such (vanishingly rare) ids rather than emit a pattern that
    // could false-match. The request side guards symmetrically.
    if (norm.length < 2 || norm.some((n) => n.includes(','))) continue
    const stopSeq = `,${norm.join(',')},`
    const key = `${meta.routeId}|${meta.directionId ?? ''}|${stopSeq}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push({ feedId, routeId: meta.routeId, directionId: meta.directionId, stopSeq })
  }
  return out
}

/**
 * Parse shapes.txt from a GTFS feed into shape coordinate arrays.
 *
 * Returns a Map of shape_id → [[lng, lat], ...] ordered by
 * shape_pt_sequence. The coordinates use [lng, lat] order to match
 * GeoJSON convention and Mapbox/Leaflet expectations.
 */
export function parseShapes(
  csvContent: string,
): Map<string, [number, number][]> {
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  // Group points by shape_id, then sort by sequence
  const raw = new Map<string, Array<{ seq: number; lat: number; lng: number }>>()
  for (const row of records) {
    const id = row.shape_id
    const lat = parseFloat(row.shape_pt_lat)
    const lng = parseFloat(row.shape_pt_lon)
    const seq = parseInt(row.shape_pt_sequence, 10)
    if (!id || isNaN(lat) || isNaN(lng) || isNaN(seq)) continue

    if (!raw.has(id)) raw.set(id, [])
    raw.get(id)!.push({ seq, lat, lng })
  }

  const result = new Map<string, [number, number][]>()
  for (const [id, points] of raw) {
    points.sort((a, b) => a.seq - b.seq)
    result.set(id, points.map(p => [p.lng, p.lat]))
  }

  return result
}

/**
 * Derive route → shape_id mapping from trips.txt.
 *
 * For each route, picks the shape_id that appears on the most trips.
 * This gives the "canonical" shape for display purposes.
 */
export function deriveRouteShapes(
  tripsContent: string,
): Map<string, string> {
  const records = parse(tripsContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  // Count shape_id occurrences per route_id
  const routeShapeCounts = new Map<string, Map<string, number>>()
  for (const row of records) {
    const routeId = row.route_id
    const shapeId = row.shape_id
    if (!routeId || !shapeId) continue

    if (!routeShapeCounts.has(routeId)) {
      routeShapeCounts.set(routeId, new Map())
    }
    const counts = routeShapeCounts.get(routeId)!
    counts.set(shapeId, (counts.get(shapeId) || 0) + 1)
  }

  // Pick the most common shape per route
  const result = new Map<string, string>()
  for (const [routeId, counts] of routeShapeCounts) {
    let bestShape = ''
    let bestCount = 0
    for (const [shapeId, count] of counts) {
      if (count > bestCount) {
        bestShape = shapeId
        bestCount = count
      }
    }
    if (bestShape) result.set(routeId, bestShape)
  }

  return result
}

/**
 * Derive route → bikes_allowed mapping from trips.txt.
 *
 * GTFS spec: bikes_allowed per trip: 0/empty=unknown, 1=allowed, 2=not allowed.
 * We aggregate to per-route:
 *   - If ANY trip on the route has bikes_allowed=1 → route gets 1
 *   - If ALL trips have bikes_allowed=1 → route gets 2
 *   - Otherwise → 0 (unknown/not allowed)
 */
export function deriveBikesAllowed(
  tripsContent: string,
): Map<string, number> {
  const records = parse(tripsContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  // Track per-route: total trips, trips with bikes_allowed=1
  const routeStats = new Map<string, { total: number; allowed: number }>()
  for (const row of records) {
    const routeId = row.route_id
    if (!routeId) continue

    const stats = routeStats.get(routeId) ?? { total: 0, allowed: 0 }
    stats.total++
    if (String(row.bikes_allowed) === '1') stats.allowed++
    routeStats.set(routeId, stats)
  }

  const result = new Map<string, number>()
  for (const [routeId, stats] of routeStats) {
    if (stats.allowed === 0) {
      result.set(routeId, 0) // unknown or not allowed
    } else if (stats.allowed === stats.total) {
      result.set(routeId, 2) // all trips allow bikes
    } else {
      result.set(routeId, 1) // some trips allow bikes
    }
  }

  return result
}

/**
 * Import shape coordinate arrays into gtfs_shapes table.
 */
export async function importShapes(
  shapes: Map<string, [number, number][]>,
  feedId: string,
): Promise<number> {
  if (shapes.size === 0) return 0

  // Clear existing shapes for this feed
  await db.execute(sql.raw(
    `DELETE FROM gtfs_shapes WHERE feed_id = '${feedId.replace(/'/g, "''")}'`,
  ))

  // Batch insert in chunks of 100 (shapes can be large)
  const entries = Array.from(shapes.entries())
  const chunkSize = 100
  let imported = 0

  for (let i = 0; i < entries.length; i += chunkSize) {
    const chunk = entries.slice(i, i + chunkSize)
    const values = chunk
      .map(([shapeId, coords]) => {
        const coordsJson = JSON.stringify(coords)
        return `('${feedId.replace(/'/g, "''")}', '${shapeId.replace(/'/g, "''")}', '${coordsJson.replace(/'/g, "''")}'::jsonb)`
      })
      .join(',\n')

    await db.execute(sql.raw(`
      INSERT INTO gtfs_shapes (feed_id, shape_id, coordinates)
      VALUES ${values}
      ON CONFLICT (feed_id, shape_id) DO UPDATE SET coordinates = EXCLUDED.coordinates
    `))
    imported += chunk.length
  }

  // Materialize LineString geometry from the coordinate arrays so PostGIS
  // spatial ops + ST_AsMVT tiles don't have to parse JSONB per query.
  // Degenerate shapes (<2 points) keep geom NULL.
  await populateShapeGeom(feedId)

  return imported
}

/**
 * (Re)compute the `geom` LineString column from the `coordinates` JSONB for a
 * feed (or all feeds when feedId is omitted). Idempotent. Degenerate shapes
 * (<2 points) are left NULL — a LineString needs at least two vertices.
 */
export async function populateShapeGeom(feedId?: string): Promise<void> {
  const where = feedId
    ? `feed_id = '${feedId.replace(/'/g, "''")}' AND `
    : ''
  await db.execute(sql.raw(`
    UPDATE gtfs_shapes
    SET geom = ST_SetSRID(
      ST_GeomFromGeoJSON(
        jsonb_build_object('type', 'LineString', 'coordinates', coordinates)::text
      ), 4326)
    WHERE ${where}jsonb_array_length(coordinates) >= 2
      AND geom IS NULL
  `))
}

/**
 * Update gtfs_routes with the canonical shape_id for each route.
 */
export async function updateRouteShapes(
  routeShapes: Map<string, string>,
  feedId: string,
): Promise<void> {
  for (const [routeId, shapeId] of routeShapes) {
    await db.execute(sql.raw(`
      UPDATE gtfs_routes
      SET shape_id = '${shapeId.replace(/'/g, "''")}'
      WHERE feed_id = '${feedId.replace(/'/g, "''")}' AND route_id = '${routeId.replace(/'/g, "''")}'
    `))
  }
}

/**
 * Batch lookup bikes_allowed for a list of (feedId, routeId) pairs.
 * Returns a map of "feedId_routeId" → bikes_allowed (0/1/2).
 * Routes not found in the DB return 0 (unknown).
 */
export async function getBikesAllowed(
  routes: Array<{ feedId: string; routeId: string }>,
): Promise<Record<string, number>> {
  if (routes.length === 0) return {}

  const conditions = routes.map(({ feedId, routeId }) =>
    `(feed_id = '${feedId.replace(/'/g, "''")}' AND route_id = '${routeId.replace(/'/g, "''")}')`
  ).join(' OR ')

  const rows = await db.execute(sql.raw(`
    SELECT feed_id, route_id, bikes_allowed
    FROM gtfs_routes
    WHERE ${conditions}
  `))

  const result: Record<string, number> = {}
  // Default all requested routes to 0
  for (const { feedId, routeId } of routes) {
    result[`${feedId}_${routeId}`] = 0
  }
  // Fill in from DB
  for (const row of rows as any[]) {
    result[`${row.feed_id}_${row.route_id}`] = parseInt(row.bikes_allowed, 10) || 0
  }

  return result
}

/**
 * Update the bikes_allowed column on gtfs_routes for a given feed.
 */
export async function updateBikesAllowed(
  bikesAllowed: Map<string, number>,
  feedId: string,
): Promise<void> {
  for (const [routeId, value] of bikesAllowed) {
    await db.execute(sql.raw(`
      UPDATE gtfs_routes
      SET bikes_allowed = ${value}
      WHERE feed_id = '${feedId.replace(/'/g, "''")}' AND route_id = '${routeId.replace(/'/g, "''")}'
    `))
  }
}

// ── Database import ─────────────────────────────────────────────────

/**
 * Import parsed stops into the gtfs_stops table.
 * Uses UPSERT to handle re-imports gracefully.
 */
export async function importStops(
  stops: ReturnType<typeof parseStops>,
): Promise<number> {
  if (stops.length === 0) return 0

  // Batch insert in chunks of 500
  const BATCH_SIZE = 500
  let imported = 0

  for (let i = 0; i < stops.length; i += BATCH_SIZE) {
    const batch = stops.slice(i, i + BATCH_SIZE)

    const validBatch = batch.filter(s => s.stopId && s.feedId)
    if (validBatch.length === 0) continue
    const values = validBatch.map(s => {
      const name = s.stopName ? `'${s.stopName.replace(/'/g, "''")}'` : 'NULL'
      const code = s.stopCode ? `'${s.stopCode.replace(/'/g, "''")}'` : 'NULL'
      const parent = s.parentStation ? `'${s.parentStation.replace(/'/g, "''")}'` : 'NULL'
      const platform = s.platformCode ? `'${s.platformCode.replace(/'/g, "''")}'` : 'NULL'

      return `(
        '${s.stopId.replace(/'/g, "''")}',
        '${s.feedId.replace(/'/g, "''")}',
        ${name},
        ${code},
        ${s.stopLat},
        ${s.stopLon},
        ${s.locationType},
        ${parent},
        ${s.wheelchairBoarding},
        ${platform},
        ST_SetSRID(ST_MakePoint(${s.stopLon}, ${s.stopLat}), 4326)
      )`
    }).join(',\n')

    await db.execute(sql.raw(`
      INSERT INTO gtfs_stops (
        stop_id, feed_id, stop_name, stop_code,
        stop_lat, stop_lon, location_type, parent_station,
        wheelchair_boarding, platform_code, geom
      )
      VALUES ${values}
      ON CONFLICT (feed_id, stop_id)
      DO UPDATE SET
        stop_name = EXCLUDED.stop_name,
        stop_lat = EXCLUDED.stop_lat,
        stop_lon = EXCLUDED.stop_lon,
        location_type = EXCLUDED.location_type,
        parent_station = EXCLUDED.parent_station,
        wheelchair_boarding = EXCLUDED.wheelchair_boarding,
        platform_code = EXCLUDED.platform_code,
        geom = EXCLUDED.geom
    `))

    imported += validBatch.length
  }

  return imported
}

/**
 * Import parsed routes into the gtfs_routes table.
 */
export async function importRoutes(
  routes: ReturnType<typeof parseRoutes>,
): Promise<number> {
  if (routes.length === 0) return 0

  const BATCH_SIZE = 500
  let imported = 0

  for (let i = 0; i < routes.length; i += BATCH_SIZE) {
    const batch = routes.slice(i, i + BATCH_SIZE)

    const values = batch.map(r => {
      const esc = (v: string | null) => v ? `'${v.replace(/'/g, "''")}'` : 'NULL'
      return `(
        ${esc(r.routeId)}, ${esc(r.feedId)}, ${esc(r.agencyId)}, ${esc(r.agencyName)},
        ${esc(r.routeShortName)}, ${esc(r.routeLongName)}, ${r.routeType},
        ${esc(r.routeColor)}, ${esc(r.routeTextColor)}, ${esc(r.routeUrl)}
      )`
    }).join(',\n')

    await db.execute(sql.raw(`
      INSERT INTO gtfs_routes (
        route_id, feed_id, agency_id, agency_name,
        route_short_name, route_long_name, route_type,
        route_color, route_text_color, route_url
      )
      VALUES ${values}
      ON CONFLICT (feed_id, route_id)
      DO UPDATE SET
        agency_id = EXCLUDED.agency_id,
        agency_name = EXCLUDED.agency_name,
        route_short_name = EXCLUDED.route_short_name,
        route_long_name = EXCLUDED.route_long_name,
        route_type = EXCLUDED.route_type,
        route_color = EXCLUDED.route_color,
        route_text_color = EXCLUDED.route_text_color,
        route_url = EXCLUDED.route_url
    `))

    imported += batch.length
  }

  return imported
}

/**
 * Import stop→route associations.
 */
export async function importStopRoutes(
  associations: ReturnType<typeof deriveStopRoutes>,
): Promise<number> {
  if (associations.length === 0) return 0

  const BATCH_SIZE = 500
  let imported = 0

  for (let i = 0; i < associations.length; i += BATCH_SIZE) {
    const batch = associations.slice(i, i + BATCH_SIZE)

    const values = batch.map(a => {
      const esc = (v: string) => `'${v.replace(/'/g, "''")}'`
      const num = (v: number | null | undefined) =>
        typeof v === 'number' && Number.isFinite(v) ? String(v) : 'NULL'
      // weekday_trips kept = trips_weekday_day for back-compat readers.
      return `(${esc(a.feedId)}, ${esc(a.stopId)}, ${esc(a.routeId)}, ` +
        `${num(a.tripsWeekdayDay)}, ${num(a.tripsWeekdayDay)}, ` +
        `${num(a.tripsWeekdayAny)}, ${num(a.tripsWeekendDay)}, ${num(a.tripsAny)})`
    }).join(',\n')

    await db.execute(sql.raw(`
      INSERT INTO gtfs_stop_routes
        (feed_id, stop_id, route_id, weekday_trips,
         trips_weekday_day, trips_weekday_any, trips_weekend_day, trips_any)
      VALUES ${values}
      ON CONFLICT (feed_id, stop_id, route_id)
      DO UPDATE SET
        weekday_trips = EXCLUDED.weekday_trips,
        trips_weekday_day = EXCLUDED.trips_weekday_day,
        trips_weekday_any = EXCLUDED.trips_weekday_any,
        trips_weekend_day = EXCLUDED.trips_weekend_day,
        trips_any = EXCLUDED.trips_any
    `))

    imported += batch.length
  }

  return imported
}

/**
 * Import trip patterns for a feed, replacing its existing rows. The feed is
 * cleared FIRST and unconditionally — so re-deriving to zero patterns (corrupt
 * or degenerate feed) leaves no stale rows behind, making the backfill (which,
 * unlike the full import, doesn't call clearFeed) truly idempotent.
 */
export async function importTripPatterns(
  feedId: string,
  patterns: ReturnType<typeof deriveTripPatterns>,
): Promise<number> {
  const feedEsc = feedId.replace(/'/g, "''")
  await db.execute(sql.raw(`DELETE FROM gtfs_trip_patterns WHERE feed_id = '${feedEsc}'`))
  if (patterns.length === 0) return 0

  const BATCH_SIZE = 500
  let imported = 0
  for (let i = 0; i < patterns.length; i += BATCH_SIZE) {
    const batch = patterns.slice(i, i + BATCH_SIZE)
    const values = batch.map(p => {
      const esc = (v: string) => `'${v.replace(/'/g, "''")}'`
      return `(${esc(p.feedId)}, ${esc(p.routeId)}, ${p.directionId ?? 'NULL'}, ${esc(p.stopSeq)})`
    }).join(',\n')

    await db.execute(sql.raw(`
      INSERT INTO gtfs_trip_patterns (feed_id, route_id, direction_id, stop_seq)
      VALUES ${values}
    `))

    imported += batch.length
  }

  return imported
}

/**
 * Parse transfers.txt — the agency's authoritative statement of which
 * stations connect inside one complex (e.g. Times Sq 1/2/3 ↔ N/Q/R/W)
 * and the minimum connection times.
 */
export function parseTransfers(
  csvContent: string,
  feedId: string,
): Array<{ feedId: string; fromStopId: string; toStopId: string; transferType: number; minTransferTime: number | null }> {
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })
  const out: Array<{ feedId: string; fromStopId: string; toStopId: string; transferType: number; minTransferTime: number | null }> = []
  for (const r of records) {
    if (!r.from_stop_id || !r.to_stop_id) continue
    out.push({
      feedId,
      fromStopId: r.from_stop_id,
      toStopId: r.to_stop_id,
      transferType: parseInt(r.transfer_type || '0', 10) || 0,
      minTransferTime: r.min_transfer_time ? parseInt(r.min_transfer_time, 10) : null,
    })
  }
  return out
}

/**
 * Import agency transfers, replacing the feed's existing rows.
 */
export async function importTransfers(
  transfers: ReturnType<typeof parseTransfers>,
): Promise<number> {
  if (transfers.length === 0) return 0
  const feedEsc = transfers[0].feedId.replace(/'/g, "''")
  await db.execute(sql.raw(`DELETE FROM gtfs_transfers WHERE feed_id = '${feedEsc}'`))

  const BATCH_SIZE = 500
  let imported = 0
  for (let i = 0; i < transfers.length; i += BATCH_SIZE) {
    const batch = transfers.slice(i, i + BATCH_SIZE)
    const values = batch.map(t => {
      const esc = (v: string) => `'${v.replace(/'/g, "''")}'`
      return `(${esc(t.feedId)}, ${esc(t.fromStopId)}, ${esc(t.toStopId)}, ${t.transferType}, ${t.minTransferTime ?? 'NULL'})`
    }).join(',\n')
    await db.execute(sql.raw(`
      INSERT INTO gtfs_transfers (feed_id, from_stop_id, to_stop_id, transfer_type, min_transfer_time)
      VALUES ${values}
      ON CONFLICT (feed_id, from_stop_id, to_stop_id) DO NOTHING
    `))
    imported += batch.length
  }
  return imported
}

/**
 * Record a feed import in the gtfs_feeds table.
 */
export async function recordFeed(feed: GtfsFeedInfo, stopCount: number, routeCount: number): Promise<void> {
  const esc = (v: string | null | undefined) => v ? `'${v.replace(/'/g, "''")}'` : 'NULL'
  const rtUrlsJson = feed.rtUrls?.length
    ? `'${JSON.stringify(feed.rtUrls).replace(/'/g, "''")}'::jsonb`
    : 'NULL'
  await db.execute(sql.raw(`
    INSERT INTO gtfs_feeds (feed_id, onestop_id, name, url, region, stop_count, route_count, rt_urls, imported_at)
    VALUES (${esc(feed.feedId)}, ${esc(feed.onestopId)}, ${esc(feed.name)}, ${esc(feed.url)}, ${esc(feed.region)}, ${stopCount}, ${routeCount}, ${rtUrlsJson}, NOW())
    ON CONFLICT (feed_id)
    DO UPDATE SET
      name = EXCLUDED.name,
      url = EXCLUDED.url,
      stop_count = EXCLUDED.stop_count,
      route_count = EXCLUDED.route_count,
      rt_urls = EXCLUDED.rt_urls,
      imported_at = NOW()
  `))
}

// ── Display overrides ───────────────────────────────────────────────
// Manual per-feed patches (config/transit-overrides.json) for data the agency
// publishes badly or not at all — e.g. CATS doesn't provide route_color for its
// Blue/Gold lines. Applied twice, once per consumer:
//   1. bakeDisplayOverridesIntoZip — rewrites routes.txt/stops.txt in the
//      PROCESSED zip (data/gtfs-processed) so MOTIS itineraries carry the
//      same colours/names as our display views. Raw zips stay pristine.
//   2. applyDisplayOverrides — patches the DB rows after import (idempotent;
//      also backfills feeds imported before the bake existed).

interface RouteOverride {
  route_color?: string
  route_text_color?: string
  route_long_name?: string
  route_short_name?: string
}
interface FeedOverride {
  routes?: Record<string, RouteOverride>
  stops?: Record<string, { stop_name?: string }>
}

let overridesCache: Record<string, FeedOverride> | null = null

/** Load and cache config/transit-overrides.json (its `feeds` map). */
function loadTransitOverrides(): Record<string, FeedOverride> {
  if (overridesCache) return overridesCache
  try {
    const path = join(import.meta.dir, '../../config/transit-overrides.json')
    const raw = JSON.parse(readFileSync(path, 'utf8'))
    overridesCache = (raw.feeds ?? {}) as Record<string, FeedOverride>
  } catch {
    overridesCache = {}
  }
  return overridesCache
}

/** Override block for a feed, matched by feed_id or onestop_id. */
function getFeedOverride(feed: GtfsFeedInfo): FeedOverride | undefined {
  const all = loadTransitOverrides()
  return all[feed.feedId] ?? all[feed.onestopId]
}

/**
 * Apply DISPLAY overrides (route colours/names, stop names) for a feed by
 * patching the DB rows. Idempotent; safe to re-run after every import. Returns
 * the number of route/stop rows patched.
 */
export async function applyDisplayOverrides(feed: GtfsFeedInfo): Promise<number> {
  const ov = getFeedOverride(feed)
  if (!ov) return 0
  const feedId = feed.feedId.replace(/'/g, "''")
  const esc = (v: string) => v.replace(/'/g, "''")
  let n = 0

  for (const [routeId, r] of Object.entries(ov.routes ?? {})) {
    const sets: string[] = []
    if (r.route_color != null) sets.push(`route_color = '${esc(r.route_color)}'`)
    if (r.route_text_color != null) sets.push(`route_text_color = '${esc(r.route_text_color)}'`)
    if (r.route_long_name != null) sets.push(`route_long_name = '${esc(r.route_long_name)}'`)
    if (r.route_short_name != null) sets.push(`route_short_name = '${esc(r.route_short_name)}'`)
    if (!sets.length) continue
    await db.execute(sql.raw(
      `UPDATE gtfs_routes SET ${sets.join(', ')} WHERE feed_id = '${feedId}' AND route_id = '${esc(routeId)}'`,
    ))
    n++
  }

  for (const [stopId, s] of Object.entries(ov.stops ?? {})) {
    if (s.stop_name == null) continue
    await db.execute(sql.raw(
      `UPDATE gtfs_stops SET stop_name = '${esc(s.stop_name)}' WHERE feed_id = '${feedId}' AND stop_id = '${esc(stopId)}'`,
    ))
    n++
  }
  return n
}

/** Feed ids that have a display-override block (for batch backfill). */
export function getOverriddenFeedIds(): string[] {
  return Object.keys(loadTransitOverrides())
}

/** Quote a CSV field if it contains commas, quotes, or newlines. */
function csvField(value: string): string {
  return /[",\r\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value
}

/** Serialize GTFS records back to CSV with the given column order. */
function serializeGtfsRecords(records: GtfsRecord[], columns: string[]): string {
  const lines = [columns.map(csvField).join(',')]
  for (const r of records) {
    lines.push(columns.map(c => csvField(r[c] ?? '')).join(','))
  }
  return lines.join('\n') + '\n'
}

/**
 * Patch rows of a CSV file inside a GTFS zip. `patches` maps a key-column
 * value (e.g. route_id) to the fields to overwrite on matching rows; columns
 * missing from the file are appended to the header. Returns rows patched
 * (0 = file untouched).
 */
async function patchZipCsv(
  zip: import('jszip'),
  filename: string,
  keyColumn: string,
  patches: Map<string, Record<string, string>>,
): Promise<number> {
  const entry = zip.file(filename)
  if (!entry || patches.size === 0) return 0

  const content = (await entry.async('string')).replace(/^﻿/, '')
  const records = parseGtfsRecords(content)
  if (records.length === 0) return 0

  // Derive columns from the header row, NOT Object.keys(records[0]):
  // relax_column_count means a ragged (short) first data row would yield a
  // record with only its own fields, silently dropping columns on rewrite.
  const headerRows = parse(content, { to_line: 1, trim: true }) as string[][]
  const columns = headerRows[0] ?? []
  for (const record of records) {
    for (const key of Object.keys(record)) {
      if (!columns.includes(key)) columns.push(key)
    }
  }
  if (!columns.includes(keyColumn)) return 0

  let patched = 0
  for (const record of records) {
    const patch = patches.get(record[keyColumn])
    if (!patch) continue
    for (const [field, value] of Object.entries(patch)) {
      if (!columns.includes(field)) columns.push(field)
      record[field] = value
    }
    patched++
  }

  if (patched > 0) {
    zip.file(filename, serializeGtfsRecords(records, columns))
  }
  return patched
}

/**
 * Bake DISPLAY overrides (route colours/names, stop names) into a GTFS zip's
 * routes.txt / stops.txt — the processed copy MOTIS ingests — so routing
 * responses carry the same colours/names as the DB display views. Patches the
 * exact fields applyDisplayOverrides patches in the DB. Rewrites the zip in
 * place; returns the number of route/stop rows patched (0 = zip untouched).
 */
export async function bakeDisplayOverridesIntoZip(
  feed: GtfsFeedInfo,
  zipPath: string,
): Promise<number> {
  const ov = getFeedOverride(feed)
  if (!ov) return 0

  const JSZip = (await import('jszip')).default
  const zip = await JSZip.loadAsync(readFileSync(zipPath))

  const routePatches = new Map<string, Record<string, string>>()
  for (const [routeId, r] of Object.entries(ov.routes ?? {})) {
    const patch: Record<string, string> = {}
    if (r.route_color != null) patch.route_color = r.route_color
    if (r.route_text_color != null) patch.route_text_color = r.route_text_color
    if (r.route_long_name != null) patch.route_long_name = r.route_long_name
    if (r.route_short_name != null) patch.route_short_name = r.route_short_name
    if (Object.keys(patch).length > 0) routePatches.set(routeId, patch)
  }

  const stopPatches = new Map<string, Record<string, string>>()
  for (const [stopId, s] of Object.entries(ov.stops ?? {})) {
    if (s.stop_name != null) stopPatches.set(stopId, { stop_name: s.stop_name })
  }

  const patched =
    (await patchZipCsv(zip, 'routes.txt', 'route_id', routePatches)) +
    (await patchZipCsv(zip, 'stops.txt', 'stop_id', stopPatches))

  if (patched > 0) {
    writeFileSync(zipPath, await zip.generateAsync({ type: 'nodebuffer' }))
  }
  return patched
}

/**
 * Remove all data for a specific feed (for re-import).
 */
export async function clearFeed(feedId: string): Promise<void> {
  const escaped = feedId.replace(/'/g, "''")
  await db.execute(sql.raw(`DELETE FROM gtfs_transfers WHERE feed_id = '${escaped}'`))
  await db.execute(sql.raw(`DELETE FROM gtfs_trip_patterns WHERE feed_id = '${escaped}'`))
  await db.execute(sql.raw(`DELETE FROM gtfs_stop_routes WHERE feed_id = '${escaped}'`))
  await db.execute(sql.raw(`DELETE FROM gtfs_routes WHERE feed_id = '${escaped}'`))
  await db.execute(sql.raw(`DELETE FROM gtfs_stops WHERE feed_id = '${escaped}'`))
  await db.execute(sql.raw(`DELETE FROM gtfs_feeds WHERE feed_id = '${escaped}'`))
}

// ── Transfer precomputation ─────────────────────────────────────────

/**
 * Find nearby stop pairs for transfer precomputation.
 *
 * Returns all pairs of stops within `maxDistance` meters of each other,
 * across all feeds (cross-feed transfers are important for multi-agency
 * cities). Uses PostGIS spatial index for efficiency.
 */
export async function findTransferPairs(
  maxDistance: number = 500,
): Promise<TransferPair[]> {
  const result = await db.execute(sql.raw(`
    SELECT
      a.stop_id AS from_stop_id,
      b.stop_id AS to_stop_id,
      a.feed_id AS from_feed_id,
      b.feed_id AS to_feed_id,
      a.stop_lat AS from_lat,
      a.stop_lon AS from_lng,
      b.stop_lat AS to_lat,
      b.stop_lon AS to_lng
    FROM gtfs_stops a
    JOIN gtfs_stops b
      ON a.id < b.id
      AND ST_DWithin(a.geom::geography, b.geom::geography, ${maxDistance})
    WHERE (a.location_type = 0 OR a.location_type IS NULL)
      AND (b.location_type = 0 OR b.location_type IS NULL)
  `))

  return (result as any[]).map((row: any) => ({
    fromStopId: row.from_stop_id,
    toStopId: row.to_stop_id,
    fromFeedId: row.from_feed_id,
    toFeedId: row.to_feed_id,
    fromLat: row.from_lat,
    fromLng: row.from_lng,
    toLat: row.to_lat,
    toLng: row.to_lng,
  }))
}

/**
 * Compute walking time between a single stop pair via GraphHopper.
 *
 * Uses point-to-point pedestrian routing (not matrix API, which is
 * unavailable in self-hosted GraphHopper).
 */
export async function computeWalkingTransfer(
  from: { lat: number; lng: number },
  to: { lat: number; lng: number },
  fetchFn: FetchFn = globalThis.fetch,
): Promise<{ walkTime: number; walkDistance: number } | null> {
  const ghUrl = process.env.GRAPHHOPPER_URL || 'http://barrelman-graphhopper:8989'

  try {
    const response = await fetchFn(`${ghUrl}/route`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        points: [[from.lng, from.lat], [to.lng, to.lat]],
        profile: 'foot',
        points_encoded: false,
        instructions: false,
      }),
    })

    if (!response.ok) return null

    const data = await response.json() as any
    const path = data.paths?.[0]
    if (!path) return null

    return {
      walkTime: Math.round(path.time / 1000), // ms → seconds
      walkDistance: Math.round(path.distance),
    }
  } catch {
    return null
  }
}

/**
 * Pre-compute walking transfers between all nearby stop pairs.
 *
 * Runs GraphHopper pedestrian routing for each pair to get accurate
 * walking times (instead of straight-line estimates). Results are used
 * to generate transfers.txt for MOTIS.
 *
 * Processes in parallel batches for performance. With ~500m max distance
 * and typical stop density, expect ~5ms per query.
 */
export async function computeAllTransfers(
  maxDistance: number = 500,
  concurrency: number = 8,
  fetchFn: FetchFn = globalThis.fetch,
  onProgress?: (completed: number, total: number) => void,
): Promise<ComputedTransfer[]> {
  const pairs = await findTransferPairs(maxDistance)
  const transfers: ComputedTransfer[] = []
  let completed = 0

  // Process in batches of `concurrency`
  for (let i = 0; i < pairs.length; i += concurrency) {
    const batch = pairs.slice(i, i + concurrency)

    const results = await Promise.all(
      batch.map(async (pair) => {
        const result = await computeWalkingTransfer(
          { lat: pair.fromLat, lng: pair.fromLng },
          { lat: pair.toLat, lng: pair.toLng },
          fetchFn,
        )

        if (result) {
          // Add both directions (A→B and B→A may differ due to one-way streets, stairs, etc.)
          return [
            {
              fromStopId: pair.fromStopId,
              toStopId: pair.toStopId,
              fromFeedId: pair.fromFeedId,
              toFeedId: pair.toFeedId,
              walkTime: result.walkTime,
              walkDistance: result.walkDistance,
            },
            {
              fromStopId: pair.toStopId,
              toStopId: pair.fromStopId,
              fromFeedId: pair.toFeedId,
              toFeedId: pair.fromFeedId,
              walkTime: result.walkTime,
              walkDistance: result.walkDistance,
            },
          ]
        }
        return []
      }),
    )

    for (const result of results) {
      transfers.push(...result)
    }

    completed += batch.length
    onProgress?.(completed, pairs.length)
  }

  return transfers
}

/**
 * Generate GTFS transfers.txt content from computed transfers.
 *
 * When feedId is provided, only includes transfers where BOTH stops
 * belong to that feed. This prevents stop ID collisions when injecting
 * into per-feed ZIPs (e.g. stop "1234" in feed A ≠ stop "1234" in feed B).
 *
 * Format: from_stop_id,to_stop_id,transfer_type,min_transfer_time
 * transfer_type=2 means timed transfer with min_transfer_time specified.
 */
export function generateTransfersTxt(
  transfers: ComputedTransfer[],
  feedId?: string,
): string {
  const filtered = feedId
    ? transfers.filter(t => t.fromFeedId === feedId && t.toFeedId === feedId)
    : transfers
  const header = 'from_stop_id,to_stop_id,transfer_type,min_transfer_time\n'
  const rows = filtered
    .map(t => `${t.fromStopId},${t.toStopId},2,${t.walkTime}`)
    .join('\n')
  return header + rows
}

// ── MOTIS config generation ────────────────────────────────────────

interface MotisConfigOptions {
  /** Directory containing GTFS zip files (relative to MOTIS data dir) */
  gtfsDir?: string
  /** Number of days to load */
  numDays?: number
  /** Max footpath length in minutes */
  maxFootpathLength?: number
  /**
   * Seconds between GTFS-RT poll cycles (MOTIS `timetable.update_interval`).
   * MOTIS's default is 60s, which means continuously re-fetching every feed's
   * realtime URLs — wasteful on a dev box that's only occasionally exercising
   * transit. Raise it (e.g. 600) for dev to keep realtime working but poll far
   * less often. Defaults to the MOTIS_RT_UPDATE_INTERVAL env var, else 60.
   */
  rtUpdateInterval?: number
  /** Enable OSM street routing for intermodal queries (default: false) */
  enableStreetRouting?: boolean
  /** Path to OSM PBF file (default: /osm-data/region.osm.pbf) */
  osmPath?: string
  /** Include GBFS feeds from gbfs_systems table (default: same as enableStreetRouting) */
  includeGbfs?: boolean
}

/**
 * Generate MOTIS config.yml from the gtfs_feeds table.
 *
 * Reads all imported feeds, builds dataset entries with RT feed URLs,
 * and returns the YAML string. Feeds with GTFS-RT URLs get `rt:` entries
 * so MOTIS automatically polls for realtime updates.
 *
 * Dataset paths ("gtfs/<feed_id>.zip") are relative to MOTIS's /data dir
 * (the barrelman-gtfs-data volume). scripts/rebuild-motis.sh populates that
 * gtfs/ dir from data/gtfs-processed — the fully preprocessed zips — so the
 * config must only ever be paired with those, never the raw downloads.
 */
export async function generateMotisConfig(options?: MotisConfigOptions): Promise<string> {
  const {
    gtfsDir = 'gtfs',
    numDays = 365,
    maxFootpathLength = 15,
    rtUpdateInterval = Number(process.env.MOTIS_RT_UPDATE_INTERVAL) || 60,
    enableStreetRouting = false,
    // MOTIS uses the platform-stripped extract (scripts/prepare-motis-osm.sh)
    // so underground subway stops stay street-reachable. region.osm.pbf is
    // untouched for GraphHopper / osm2pgsql / tiles.
    osmPath = '/osm-data/region-transit.osm.pbf',
    includeGbfs = enableStreetRouting,
  } = options || {}

  const result = await db.execute(sql.raw(`
    SELECT feed_id, rt_urls
    FROM gtfs_feeds
    ORDER BY feed_id
  `))

  const feeds = (result as any[]) as Array<{ feed_id: string; rt_urls: GtfsRtUrl[] | null }>

  // Build YAML manually (no dependency needed for this simple structure)
  const lines: string[] = [
    '# MOTIS config — auto-generated by Barrelman GTFS import',
    '#',
    `# ${feeds.length} feeds, generated ${new Date().toISOString()}`,
    `# street_routing: ${enableStreetRouting}`,
    '',
  ]

  // OSM file (required for street routing, geocoding, shapes)
  if (enableStreetRouting) {
    lines.push(`osm: ${osmPath}`)
    lines.push('')
  }

  lines.push('timetable:')
  lines.push('  first_day: TODAY')
  lines.push(`  num_days: ${numDays}`)
  lines.push('  with_shapes: true')
  lines.push('  adjust_footpaths: true')
  // How often MOTIS re-polls every feed's GTFS-RT URLs. MOTIS default is 60s;
  // dev can raise this (MOTIS_RT_UPDATE_INTERVAL) to cut continuous polling of
  // agencies it isn't testing while keeping realtime data present.
  lines.push(`  update_interval: ${rtUpdateInterval}`)
  lines.push(`  max_footpath_length: ${maxFootpathLength}`)
  // Import-time stop↔street matching radius used when generating
  // stop-to-stop transfer footpaths (osr_footpath). The MOTIS default of
  // 25m leaves off-street platforms (e.g. under Union Square Park) with
  // crow-fly transfer estimates instead of street-routed ones. Note the
  // QUERY-time equivalent (maxMatchingDistance on /api/v1/plan) is what
  // governs access/egress walks — transit.service.ts passes that per query.
  lines.push('  max_matching_distance: 250')
  lines.push('  datasets:')

  for (const feed of feeds) {
    lines.push(`    "${feed.feed_id}":`)
    lines.push(`      path: "${gtfsDir}/${feed.feed_id}.zip"`)

    // Add RT feeds if available
    const rtUrls = feed.rt_urls
    if (rtUrls && Array.isArray(rtUrls) && rtUrls.length > 0) {
      lines.push('      rt:')
      for (const rt of rtUrls) {
        lines.push(`        - url: "${rt.url}"`)
        if (rt.headers && Object.keys(rt.headers).length > 0) {
          lines.push('          headers:')
          for (const [key, value] of Object.entries(rt.headers)) {
            lines.push(`            "${key}": "${value}"`)
          }
        }
      }
    }
  }

  // GBFS feeds for shared mobility (bikeshare, scootershare)
  if (includeGbfs) {
    const gbfsSystems = await getGbfsFeedsForMotis()
    if (gbfsSystems.length > 0) {
      lines.push('')
      lines.push('gbfs:')
      lines.push(`  update_interval: 300`)
      lines.push(`  cache_size: ${Math.max(gbfsSystems.length, 256)}`)
      lines.push('  feeds:')
      for (const system of gbfsSystems) {
        lines.push(`    "${system.systemId}":`)
        lines.push(`      url: "${system.url}"`)
      }
    }
  }

  lines.push('')
  lines.push(`street_routing: ${enableStreetRouting}`)
  lines.push(`osr_footpath: ${enableStreetRouting}`)
  lines.push(`geocoding: ${enableStreetRouting}`)
  lines.push('reverse_geocoding: false')
  lines.push('')

  return lines.join('\n')
}

async function getGbfsFeedsForMotis(): Promise<Array<{ systemId: string; url: string }>> {
  const result = await db.execute(sql.raw(`
    SELECT system_id, url
    FROM gbfs_systems
    WHERE enabled = TRUE
    ORDER BY system_id
  `))
  return (result as any[]).map(row => ({
    systemId: row.system_id,
    url: row.url,
  }))
}

// ── GTFS-Flex sanitization ────────────────────────────────────────

/**
 * GTFS-Flex v2 extension files that crash MOTIS.
 *
 * These files define flex-route service areas, booking rules, and
 * GeoJSON location boundaries (including MultiPolygon geometries)
 * that MOTIS's GTFS parser cannot handle.
 */
export const FLEX_EXTENSION_FILES = [
  'areas.txt',
  'stop_areas.txt',
  'booking_rules.txt',
  'location_groups.txt',
  'location_group_stops.txt',
  'locations.geojson',
] as const

/**
 * Strip GTFS-Flex extension files from a GTFS ZIP buffer.
 *
 * MOTIS v2 crashes when it encounters Flex v2 extension files
 * (especially locations.geojson with MultiPolygon geometries).
 * This function removes those files while preserving all standard
 * GTFS data that MOTIS can process.
 *
 * Returns the sanitized buffer and a list of removed filenames.
 * If no flex files are found, returns the original buffer unchanged.
 */
export async function sanitizeGtfsZip(
  buffer: ArrayBuffer,
): Promise<{ buffer: ArrayBuffer; removedFiles: string[] }> {
  // Dynamic import to avoid requiring JSZip at module level in tests
  const JSZip = (await import('jszip')).default
  const zip = await JSZip.loadAsync(buffer)

  const removedFiles: string[] = []
  for (const flexFile of FLEX_EXTENSION_FILES) {
    if (zip.file(flexFile)) {
      zip.remove(flexFile)
      removedFiles.push(flexFile)
    }
  }

  if (removedFiles.length === 0) {
    return { buffer, removedFiles: [] }
  }

  const sanitized = await zip.generateAsync({ type: 'arraybuffer' })
  return { buffer: sanitized, removedFiles }
}
