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
export function deriveStopRoutes(
  tripsContent: string,
  stopTimesContent: string,
  feedId: string,
): Array<{ feedId: string; stopId: string; routeId: string }> {
  // Build trip → route map
  const trips = parse(tripsContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  const tripToRoute = new Map<string, string>()
  for (const trip of trips) {
    tripToRoute.set(trip.trip_id, trip.route_id)
  }

  // Scan stop_times for unique (stop_id, route_id) pairs
  const seen = new Set<string>()
  const result: Array<{ feedId: string; stopId: string; routeId: string }> = []

  const stopTimes = parse(stopTimesContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_column_count: true,
  })

  for (const st of stopTimes) {
    const routeId = tripToRoute.get(st.trip_id)
    if (!routeId || !st.stop_id) continue

    const key = `${st.stop_id}|${routeId}`
    if (seen.has(key)) continue
    seen.add(key)

    result.push({ feedId, stopId: st.stop_id, routeId })
  }

  return result
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
      return `(${esc(a.feedId)}, ${esc(a.stopId)}, ${esc(a.routeId)})`
    }).join(',\n')

    await db.execute(sql.raw(`
      INSERT INTO gtfs_stop_routes (feed_id, stop_id, route_id)
      VALUES ${values}
      ON CONFLICT (feed_id, stop_id, route_id) DO NOTHING
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

/**
 * Remove all data for a specific feed (for re-import).
 */
export async function clearFeed(feedId: string): Promise<void> {
  const escaped = feedId.replace(/'/g, "''")
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
}

/**
 * Generate MOTIS config.yml from the gtfs_feeds table.
 *
 * Reads all imported feeds, builds dataset entries with RT feed URLs,
 * and returns the YAML string. Feeds with GTFS-RT URLs get `rt:` entries
 * so MOTIS automatically polls for realtime updates.
 */
export async function generateMotisConfig(options?: MotisConfigOptions): Promise<string> {
  const { gtfsDir = 'gtfs', numDays = 365, maxFootpathLength = 15 } = options || {}

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
    '',
    'timetable:',
    '  first_day: TODAY',
    `  num_days: ${numDays}`,
    '  with_shapes: true',
    '  adjust_footpaths: true',
    `  max_footpath_length: ${maxFootpathLength}`,
    '  datasets:',
  ]

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

  lines.push('')
  lines.push('street_routing: false')
  lines.push('osr_footpath: false')
  lines.push('geocoding: false')
  lines.push('reverse_geocoding: false')
  lines.push('')

  return lines.join('\n')
}
