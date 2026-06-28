/**
 * Departures Service
 *
 * Queries the MOTIS stoptimes API to provide transit departure boards.
 * Enriches results with route colors from the GTFS database.
 *
 * This replaces the Transitland REST API as the departure data source.
 * The adapter pattern allows MOTIS to be swapped for another timetable
 * engine in the future without changing the external API contract.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'

// ── Types ───────────────────────────────────────────────────────────

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

export interface DepartureRequest {
  /** Coordinates to search near */
  lat: number
  lng: number
  /** Search radius in meters (default 150) */
  radius?: number
  /** ISO 8601 time to query from (default: now) */
  time?: string
  /** Max departures per stop (default 50) */
  n?: number
  /** Specific feedId (skip spatial search) */
  feedId?: string
  /** Specific stopId (skip spatial search) */
  stopId?: string
  /** Keep only departures whose route short name is in this set. Powers the
   *  merged "4 or 5" departure board — pass the interchangeable routes. */
  routeShortNames?: string[]
  /** Keep only departures in this GTFS direction ("0"/"1"). A platform stop
   *  can return both directions, so the board filters to the rider's way. */
  directionId?: string
}

export interface StopDepartures {
  stop: {
    stopId: string
    feedId: string
    name: string
    code?: string
    lat: number
    lng: number
    timezone: string
    distance?: number
  }
  departures: Departure[]
  nextPageCursor?: string
  previousPageCursor?: string
}

export interface Departure {
  tripId: string
  route: {
    id: string
    feedId: string
    shortName?: string
    longName?: string
    type: number
    color?: string
    textColor?: string
    agencyId?: string
    agencyName?: string
  }
  headsign?: string
  directionId?: string
  departureTime: string
  arrivalTime: string
  scheduledDepartureTime: string
  scheduledArrivalTime: string
  delay?: number
  realTime: boolean
  cancelled: boolean
  mode: string
  tripOrigin?: string
  tripDestination?: string
}

// ── MOTIS response types ────────────────────────────────────────────

interface MotisStopTimesResponse {
  stopTimes: MotisStopTime[]
  place: {
    name: string
    stopId: string
    lat: number
    lon: number
    stopCode?: string
    tz?: string
  }
  previousPageCursor?: string
  nextPageCursor?: string
}

interface MotisStopTime {
  place: {
    name: string
    stopId: string
    lat: number
    lon: number
    tz?: string
    arrival?: string
    departure?: string
    scheduledArrival?: string
    scheduledDeparture?: string
    stopCode?: string
    cancelled?: boolean
  }
  mode: string
  realTime: boolean
  headsign?: string
  tripFrom?: { name: string }
  tripTo?: { name: string }
  agencyId?: string
  agencyName?: string
  routeId: string
  routeShortName?: string
  routeLongName?: string
  routeType: number
  tripId: string
  directionId?: string
  cancelled?: boolean
  tripCancelled?: boolean
}

// ── Internals ───────────────────────────────────────────────────────

function getMotisUrl(): string {
  return process.env.MOTIS_URL || 'http://barrelman-motis:8080'
}

/**
 * Parse a MOTIS stop ID (format: `{feedId}_{stopId}`) into parts.
 */
function parseMotisId(motisId: string): { feedId: string; stopId: string } {
  const sep = motisId.indexOf('_')
  if (sep === -1) return { feedId: '', stopId: motisId }
  return { feedId: motisId.slice(0, sep), stopId: motisId.slice(sep + 1) }
}

/**
 * Compute delay in seconds from scheduled vs actual times.
 */
function computeDelay(actual?: string, scheduled?: string): number | undefined {
  if (!actual || !scheduled) return undefined
  const diff = new Date(actual).getTime() - new Date(scheduled).getTime()
  if (isNaN(diff)) return undefined
  return Math.round(diff / 1000)
}

/**
 * Find nearby GTFS stops using PostGIS spatial index.
 */
async function findNearbyStops(
  lat: number,
  lng: number,
  radius: number,
  limit: number,
): Promise<Array<{ feedId: string; stopId: string; name: string; code?: string; lat: number; lng: number; distance: number }>> {
  const result = await db.execute(sql.raw(`
    SELECT
      stop_id,
      feed_id,
      stop_name,
      stop_code,
      stop_lat,
      stop_lon,
      ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography
      ) AS distance
    FROM gtfs_stops
    WHERE ST_DWithin(
      geom::geography,
      ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography,
      ${radius}
    )
    AND (location_type = 0 OR location_type IS NULL)
    ORDER BY distance
    LIMIT ${limit}
  `))

  return (result as any[]).map((row: any) => ({
    feedId: row.feed_id,
    stopId: row.stop_id,
    name: row.stop_name || '',
    code: row.stop_code || undefined,
    lat: row.stop_lat,
    lng: row.stop_lon,
    distance: Math.round(row.distance * 10) / 10,
  }))
}

/**
 * Batch-fetch route colors from the database for a set of feed+route pairs.
 * Returns a map keyed by `{feedId}_{routeId}` → { color, textColor }.
 */
async function fetchRouteColors(
  pairs: Array<{ feedId: string; routeId: string }>,
): Promise<Map<string, { color?: string; textColor?: string }>> {
  const colorMap = new Map<string, { color?: string; textColor?: string }>()
  if (pairs.length === 0) return colorMap

  // Deduplicate
  const unique = [...new Map(pairs.map(p => [`${p.feedId}_${p.routeId}`, p])).values()]

  // Build WHERE clause for batch lookup
  const conditions = unique
    .map(p => `(feed_id = '${p.feedId.replace(/'/g, "''")}' AND route_id = '${p.routeId.replace(/'/g, "''")}')`)
    .join(' OR ')

  try {
    const result = await db.execute(sql.raw(`
      SELECT feed_id, route_id, route_color, route_text_color
      FROM gtfs_routes
      WHERE ${conditions}
    `))

    for (const row of result as any[]) {
      const key = `${row.feed_id}_${row.route_id}`
      colorMap.set(key, {
        color: row.route_color || undefined,
        textColor: row.route_text_color || undefined,
      })
    }
  } catch (err) {
    console.error('[Departures] Failed to fetch route colors:', err)
  }

  return colorMap
}

/**
 * Query the MOTIS stoptimes endpoint for a single stop.
 */
async function queryMotisStopTimes(
  stopId: string,
  n: number,
  time: string | undefined,
  fetchFn: FetchFn,
): Promise<MotisStopTimesResponse> {
  const motisUrl = getMotisUrl()
  const params = new URLSearchParams({ stopId, n: String(n) })
  if (time) params.set('time', time)

  const url = `${motisUrl}/api/v1/stoptimes?${params}`
  const response = await fetchFn(url, { signal: AbortSignal.timeout(10_000) })

  if (!response.ok) {
    const body = await response.text().catch(() => '')
    throw new Error(`MOTIS stoptimes returned ${response.status}: ${body}`)
  }

  return response.json() as Promise<MotisStopTimesResponse>
}

/**
 * Transform MOTIS stopTimes into our Departure format, with route colors.
 */
function transformDepartures(
  stopTimes: MotisStopTime[],
  colorMap: Map<string, { color?: string; textColor?: string }>,
): Departure[] {
  return stopTimes
    .filter(st => !st.tripCancelled)
    .map(st => {
      const { feedId, stopId: routeId } = parseMotisId(st.routeId)
      const colors = colorMap.get(st.routeId)

      const departureTime = st.place.departure || st.place.arrival || ''
      const arrivalTime = st.place.arrival || st.place.departure || ''
      const scheduledDep = st.place.scheduledDeparture || st.place.scheduledArrival || ''
      const scheduledArr = st.place.scheduledArrival || st.place.scheduledDeparture || ''

      return {
        tripId: st.tripId,
        route: {
          id: routeId,
          feedId,
          shortName: st.routeShortName || undefined,
          longName: st.routeLongName || undefined,
          type: st.routeType,
          color: colors?.color,
          textColor: colors?.textColor,
          agencyId: st.agencyId || undefined,
          agencyName: st.agencyName || undefined,
        },
        headsign: st.headsign || undefined,
        directionId: st.directionId || undefined,
        departureTime,
        arrivalTime,
        scheduledDepartureTime: scheduledDep,
        scheduledArrivalTime: scheduledArr,
        delay: st.realTime ? computeDelay(departureTime, scheduledDep) : undefined,
        realTime: st.realTime,
        cancelled: st.cancelled || st.place.cancelled || false,
        mode: st.mode,
        tripOrigin: st.tripFrom?.name,
        tripDestination: st.tripTo?.name,
      }
    })
}

// ── Public API ──────────────────────────────────────────────────────

/**
 * Get upcoming departures near a location or at a specific stop.
 *
 * When `feedId` + `stopId` are provided, queries that stop directly.
 * Otherwise, finds nearby stops via PostGIS and queries each.
 *
 * Results are enriched with route colors from the GTFS database,
 * which MOTIS doesn't return in its stoptimes response.
 */
export async function getDepartures(
  request: DepartureRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<StopDepartures[]> {
  const {
    lat,
    lng,
    radius = 150,
    time,
    n = 50,
    feedId,
    stopId,
    routeShortNames,
    directionId,
  } = request

  const routeFilter =
    routeShortNames && routeShortNames.length
      ? new Set(routeShortNames)
      : null

  // 1. Determine which stops to query
  let stops: Array<{ feedId: string; stopId: string; name: string; code?: string; lat: number; lng: number; distance?: number }>

  if (feedId && stopId) {
    // Direct stop query — skip spatial search
    stops = [{ feedId, stopId, name: '', lat, lng }]
  } else {
    stops = await findNearbyStops(lat, lng, radius, 5)
    if (stops.length === 0) return []
  }

  // 2. Query MOTIS stoptimes for each stop in parallel
  const motisResults = await Promise.allSettled(
    stops.map(async stop => {
      const motisStopId = `${stop.feedId}_${stop.stopId}`
      const result = await queryMotisStopTimes(motisStopId, n, time, fetchFn)
      return { stop, result }
    }),
  )

  // 3. Collect all route IDs for batch color lookup
  const routePairs: Array<{ feedId: string; routeId: string }> = []
  const successResults: Array<{ stop: typeof stops[0]; result: MotisStopTimesResponse }> = []

  for (const outcome of motisResults) {
    if (outcome.status !== 'fulfilled') continue
    const { stop, result } = outcome.value
    successResults.push({ stop, result })

    for (const st of result.stopTimes) {
      const { feedId: fid, stopId: rid } = parseMotisId(st.routeId)
      routePairs.push({ feedId: fid, routeId: rid })
    }
  }

  if (successResults.length === 0) return []

  // 4. Batch-fetch route colors
  const colorMap = await fetchRouteColors(routePairs)

  // 5. Transform and return
  return successResults.map(({ stop, result }) => {
    const motisPlace = result.place
    const timezone = motisPlace?.tz || result.stopTimes[0]?.place?.tz || 'UTC'

    return {
      stop: {
        stopId: stop.stopId,
        feedId: stop.feedId,
        name: motisPlace?.name || stop.name,
        code: motisPlace?.stopCode || stop.code,
        lat: motisPlace?.lat || stop.lat,
        lng: motisPlace?.lon || stop.lng,
        timezone,
        distance: stop.distance,
      },
      departures: transformDepartures(result.stopTimes, colorMap).filter(
        (d) =>
          (!routeFilter || routeFilter.has(d.route.shortName ?? '')) &&
          (directionId == null || d.directionId === directionId),
      ),
      nextPageCursor: result.nextPageCursor,
      previousPageCursor: result.previousPageCursor,
    }
  })
}
