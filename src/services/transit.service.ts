/**
 * Transit Routing Service (MOTIS)
 *
 * Calls the MOTIS transit router (OTPAPI v2 format) and adapts responses
 * into Barrelman's transit segment format. MOTIS runs in transit-only mode
 * — no street routing graph is loaded. Walking legs in MOTIS responses are
 * straight-line estimates; the Parchment server replaces them with actual
 * GraphHopper walking routes during trip composition.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'

// ── Types ───────────────────────────────────────────────────────────

export interface TransitRouteRequest {
  from: { lat: number; lng: number }
  to: { lat: number; lng: number }
  /** ISO 8601 datetime for departure or arrival */
  time?: string
  /** If true, `time` is interpreted as desired arrival time */
  arriveBy?: boolean
  /** Number of itinerary alternatives to return */
  numItineraries?: number
  /** Search window in minutes */
  searchWindow?: number
  /** Transit modes to include */
  transitModes?: TransitMode[]
  /** Maximum walking distance in meters (for MOTIS's internal access/egress) */
  maxWalkDistance?: number
  /** Maximum number of transfers */
  maxTransfers?: number
  /** Require wheelchair-accessible vehicles and stops */
  wheelchair?: boolean
}

export type TransitMode = 'BUS' | 'RAIL' | 'TRAM' | 'SUBWAY' | 'FERRY' | 'CABLE_CAR' | 'GONDOLA' | 'FUNICULAR'

export const ALL_TRANSIT_MODES: TransitMode[] = [
  'BUS', 'RAIL', 'TRAM', 'SUBWAY', 'FERRY', 'CABLE_CAR', 'GONDOLA', 'FUNICULAR',
]

export interface TransitRouteResponse {
  itineraries: TransitItinerary[]
  metadata?: {
    searchWindow: number
    nextPageCursor?: string
    prevPageCursor?: string
  }
}

export interface TransitItinerary {
  /** Total duration in seconds */
  duration: number
  /** ISO 8601 start time */
  startTime: string
  /** ISO 8601 end time */
  endTime: string
  /** Total walking time in seconds */
  walkTime: number
  /** Total transit time in seconds */
  transitTime: number
  /** Total waiting time in seconds */
  waitingTime: number
  /** Total walking distance in meters */
  walkDistance: number
  /** Number of transfers */
  transfers: number
  /** Ordered legs of this itinerary */
  legs: TransitLeg[]
}

export interface TransitLeg {
  /** WALK, BUS, RAIL, TRAM, SUBWAY, FERRY, etc. */
  mode: string
  /** Start location */
  from: TransitLegPlace
  /** End location */
  to: TransitLegPlace
  /** ISO 8601 departure time */
  startTime: string
  /** ISO 8601 arrival time */
  endTime: string
  /** Duration in seconds */
  duration: number
  /** Distance in meters */
  distance: number
  /** GeoJSON LineString geometry */
  geometry?: {
    type: 'LineString'
    coordinates: [number, number][]
  }
  /** True for transit legs, false for walking/cycling */
  transitLeg: boolean

  // Transit-only fields (undefined for walking legs)
  /** GTFS route short name (e.g. "9", "Blue Line") */
  routeShortName?: string
  /** GTFS route long name */
  routeLongName?: string
  /** Route color hex (without #) */
  routeColor?: string
  /** Route text color hex (without #) */
  routeTextColor?: string
  /** Agency name */
  agencyName?: string
  /** Agency ID */
  agencyId?: string
  /** GTFS trip ID */
  tripId?: string
  /** Trip headsign */
  headsign?: string
  /** GTFS route ID */
  routeId?: string
  /** Intermediate stops (between boarding and alighting) */
  intermediateStops?: TransitLegPlace[]
}

export interface TransitLegPlace {
  name: string
  lat: number
  lng: number
  /** GTFS stop ID (only for transit stops) */
  stopId?: string
  /** ISO 8601 arrival time at this place */
  arrival?: string
  /** ISO 8601 departure time from this place */
  departure?: string
  /** Platform/track code */
  platformCode?: string
}

export interface NearbyStopsRequest {
  lat: number
  lng: number
  /** Search radius in meters (default 1000) */
  radius?: number
  /** Maximum number of stops to return (default 20) */
  limit?: number
}

export interface NearbyStop {
  stopId: string
  feedId: string
  stopName: string
  stopCode: string | null
  lat: number
  lng: number
  distance: number
  locationType: number
  parentStation: string | null
  wheelchairBoarding: number
  platformCode: string | null
}

export interface StopRoutesResult {
  routeId: string
  feedId: string
  routeShortName: string | null
  routeLongName: string | null
  routeType: number
  routeColor: string | null
  routeTextColor: string | null
  agencyName: string | null
}

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

// ── MOTIS client ────────────────────────────────────────────────────

function getMotisUrl(): string {
  return process.env.MOTIS_URL || 'http://barrelman-motis:8080'
}

/**
 * Decode a Google Encoded Polyline into [lng, lat] coordinate pairs.
 * MOTIS returns geometry in the OTPAPI `legGeometry.points` format.
 */
function decodePolyline(encoded: string): [number, number][] {
  const coords: [number, number][] = []
  let index = 0
  let lat = 0
  let lng = 0

  while (index < encoded.length) {
    let shift = 0
    let result = 0
    let byte: number

    do {
      byte = encoded.charCodeAt(index++) - 63
      result |= (byte & 0x1f) << shift
      shift += 5
    } while (byte >= 0x20)

    lat += result & 1 ? ~(result >> 1) : result >> 1

    shift = 0
    result = 0

    do {
      byte = encoded.charCodeAt(index++) - 63
      result |= (byte & 0x1f) << shift
      shift += 5
    } while (byte >= 0x20)

    lng += result & 1 ? ~(result >> 1) : result >> 1

    coords.push([lng / 1e5, lat / 1e5])
  }

  return coords
}

/** Convert epoch milliseconds to ISO 8601 string */
function epochToIso(epoch: number): string {
  return new Date(epoch).toISOString()
}

/** Adapt a single MOTIS/OTPAPI leg into our format */
function adaptLeg(leg: any): TransitLeg {
  const isTransit = leg.mode !== 'WALK' && leg.mode !== 'BICYCLE'

  let geometry: TransitLeg['geometry'] | undefined
  if (leg.legGeometry?.points) {
    geometry = {
      type: 'LineString',
      coordinates: decodePolyline(leg.legGeometry.points),
    }
  }

  const adapted: TransitLeg = {
    mode: leg.mode,
    from: {
      name: leg.from?.name || '',
      lat: leg.from?.lat,
      lng: leg.from?.lon,
      stopId: leg.from?.stopId || undefined,
      arrival: leg.from?.arrival != null ? epochToIso(leg.from.arrival) : undefined,
      departure: leg.from?.departure != null ? epochToIso(leg.from.departure) : undefined,
      platformCode: leg.from?.platformCode || undefined,
    },
    to: {
      name: leg.to?.name || '',
      lat: leg.to?.lat,
      lng: leg.to?.lon,
      stopId: leg.to?.stopId || undefined,
      arrival: leg.to?.arrival != null ? epochToIso(leg.to.arrival) : undefined,
      departure: leg.to?.departure != null ? epochToIso(leg.to.departure) : undefined,
      platformCode: leg.to?.platformCode || undefined,
    },
    startTime: epochToIso(leg.startTime),
    endTime: epochToIso(leg.endTime),
    duration: leg.duration ?? Math.round((leg.endTime - leg.startTime) / 1000),
    distance: leg.distance ?? 0,
    geometry,
    transitLeg: isTransit,
  }

  if (isTransit) {
    adapted.routeShortName = leg.routeShortName || leg.route || undefined
    adapted.routeLongName = leg.routeLongName || undefined
    adapted.routeColor = leg.routeColor || undefined
    adapted.routeTextColor = leg.routeTextColor || undefined
    adapted.agencyName = leg.agencyName || undefined
    adapted.agencyId = leg.agencyId || undefined
    adapted.tripId = leg.tripId || undefined
    adapted.headsign = leg.headsign || undefined
    adapted.routeId = leg.routeId || undefined

    if (Array.isArray(leg.intermediateStops)) {
      adapted.intermediateStops = leg.intermediateStops.map((s: any) => ({
        name: s.name || '',
        lat: s.lat,
        lng: s.lon,
        stopId: s.stopId || undefined,
        arrival: s.arrival != null ? epochToIso(s.arrival) : undefined,
        departure: s.departure != null ? epochToIso(s.departure) : undefined,
        platformCode: s.platformCode || undefined,
      }))
    }
  }

  return adapted
}

/** Adapt a MOTIS/OTPAPI itinerary into our format */
function adaptItinerary(itin: any): TransitItinerary {
  return {
    duration: itin.duration,
    startTime: epochToIso(itin.startTime),
    endTime: epochToIso(itin.endTime),
    walkTime: itin.walkTime ?? 0,
    transitTime: itin.transitTime ?? 0,
    waitingTime: itin.waitingTime ?? 0,
    walkDistance: itin.walkDistance ?? 0,
    transfers: itin.transfers ?? 0,
    legs: (itin.legs || []).map(adaptLeg),
  }
}

/**
 * Query MOTIS for transit routes between two points.
 *
 * Sends a request to MOTIS's OTPAPI-compatible /api/v1/plan endpoint
 * and adapts the response into Barrelman's transit format.
 */
export async function getTransitRoute(
  request: TransitRouteRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  const motisUrl = getMotisUrl()
  const modes = request.transitModes || ALL_TRANSIT_MODES

  // Parse time or default to now
  const departureDate = request.time ? new Date(request.time) : new Date()

  const params = new URLSearchParams({
    fromPlace: `${request.from.lat},${request.from.lng}`,
    toPlace: `${request.to.lat},${request.to.lng}`,
    date: departureDate.toISOString().split('T')[0],
    time: departureDate.toTimeString().slice(0, 5),
    arriveBy: String(request.arriveBy ?? false),
    numItineraries: String(request.numItineraries ?? 5),
    transitModes: modes.join(','),
    preTransitModes: 'WALK',
    postTransitModes: 'WALK',
  })

  if (request.searchWindow != null) {
    params.set('searchWindow', String(request.searchWindow))
  }
  if (request.maxTransfers != null) {
    params.set('maxTransfers', String(request.maxTransfers))
  }
  if (request.wheelchair) {
    params.set('wheelchair', 'true')
  }

  const response = await fetchFn(`${motisUrl}/api/v1/plan?${params}`)

  if (!response.ok) {
    const errorText = await response.text()
    throw new MotisError(response.status, errorText)
  }

  const data = await response.json() as any

  if (!data.plan?.itineraries) {
    return { itineraries: [], metadata: { searchWindow: 0 } }
  }

  return {
    itineraries: data.plan.itineraries.map(adaptItinerary),
    metadata: {
      searchWindow: data.plan.searchWindowUsed ?? 0,
      nextPageCursor: data.nextPageCursor || undefined,
      prevPageCursor: data.previousPageCursor || undefined,
    },
  }
}

// ── Spatial stop queries ────────────────────────────────────────────

/**
 * Find transit stops near a given point.
 *
 * Uses the PostGIS spatial index on gtfs_stops.geom for efficient
 * radius queries. Returns stops ordered by distance.
 */
export async function getNearbyStops(
  request: NearbyStopsRequest,
): Promise<NearbyStop[]> {
  const { lat, lng, radius = 1000, limit = 20 } = request

  const result = await db.execute(sql.raw(`
    SELECT
      stop_id,
      feed_id,
      stop_name,
      stop_code,
      stop_lat,
      stop_lon,
      location_type,
      parent_station,
      wheelchair_boarding,
      platform_code,
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
    stopId: row.stop_id,
    feedId: row.feed_id,
    stopName: row.stop_name || '',
    stopCode: row.stop_code,
    lat: row.stop_lat,
    lng: row.stop_lon,
    distance: Math.round(row.distance * 10) / 10,
    locationType: row.location_type ?? 0,
    parentStation: row.parent_station,
    wheelchairBoarding: row.wheelchair_boarding ?? 0,
    platformCode: row.platform_code,
  }))
}

/**
 * Get routes that serve a given stop.
 *
 * Joins gtfs_stop_routes with gtfs_routes to return route details
 * for all lines passing through the specified stop.
 */
export async function getRoutesForStop(
  feedId: string,
  stopId: string,
): Promise<StopRoutesResult[]> {
  const result = await db.execute(sql.raw(`
    SELECT
      r.route_id,
      r.feed_id,
      r.route_short_name,
      r.route_long_name,
      r.route_type,
      r.route_color,
      r.route_text_color,
      r.agency_name
    FROM gtfs_stop_routes sr
    JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
    WHERE sr.feed_id = '${feedId.replace(/'/g, "''")}'
      AND sr.stop_id = '${stopId.replace(/'/g, "''")}'
    ORDER BY r.route_type, r.route_short_name
  `))

  return (result as any[]).map((row: any) => ({
    routeId: row.route_id,
    feedId: row.feed_id,
    routeShortName: row.route_short_name,
    routeLongName: row.route_long_name,
    routeType: row.route_type,
    routeColor: row.route_color,
    routeTextColor: row.route_text_color,
    agencyName: row.agency_name,
  }))
}

/**
 * Check if MOTIS is healthy and accepting requests.
 */
export async function checkMotisHealth(
  fetchFn: FetchFn = globalThis.fetch,
): Promise<{ status: 'ok' | 'unavailable'; message?: string }> {
  try {
    const motisUrl = getMotisUrl()
    const response = await fetchFn(`${motisUrl}/api/v1/health`, { signal: AbortSignal.timeout(3000) })
    if (response.ok) {
      return { status: 'ok' }
    }
    return { status: 'unavailable', message: `MOTIS returned ${response.status}` }
  } catch (err) {
    return {
      status: 'unavailable',
      message: err instanceof Error ? err.message : 'Connection failed',
    }
  }
}

// ── Errors ──────────────────────────────────────────────────────────

export class MotisError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly body: string,
  ) {
    super(`MOTIS returned ${statusCode}`)
    this.name = 'MotisError'
  }
}
