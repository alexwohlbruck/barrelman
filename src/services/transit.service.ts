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
  'BUS', 'RAIL', 'TRAM', 'SUBWAY', 'FERRY',
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
  /** Total fare from GTFS fare data (via MOTIS). Undefined if no fare data available. */
  fare?: {
    currency: string
    amount: number
  }
}

/** MOTIS street modes for intermodal routing */
export type MotisStreetMode = 'WALK' | 'BIKE' | 'CAR' | 'CAR_PARKING' | 'RENTAL'

/** MOTIS transit modes */
export type MotisTransitMode = 'TRANSIT' | 'BUS' | 'RAIL' | 'TRAM' | 'SUBWAY' | 'FERRY' | 'COACH' | 'REGIONAL_RAIL' | 'SUBURBAN' | 'HIGHSPEED_RAIL' | 'LONG_DISTANCE' | 'FUNICULAR' | 'AERIAL_LIFT'

/** GBFS rental vehicle form factors */
export type RentalFormFactor = 'BICYCLE' | 'CARGO_BICYCLE' | 'CAR' | 'MOPED' | 'SCOOTER_STANDING' | 'SCOOTER_SEATED' | 'OTHER'

export interface IntermodalRouteRequest extends TransitRouteRequest {
  /** Modes for first mile (coordinate → first transit stop). Default: ['WALK'] */
  preTransitModes?: MotisStreetMode[]
  /** Modes for last mile (last transit stop → coordinate). Default: ['WALK'] */
  postTransitModes?: MotisStreetMode[]
  /** Direct (non-transit) modes to also compute. Default: ['WALK'] */
  directModes?: MotisStreetMode[]
  /** Max first-mile time in seconds (default 900 = 15 min) */
  maxPreTransitTime?: number
  /** Max last-mile time in seconds (default 900 = 15 min) */
  maxPostTransitTime?: number
  /** Filter rental vehicles to specific form factors */
  preTransitRentalFormFactors?: RentalFormFactor[]
  postTransitRentalFormFactors?: RentalFormFactor[]
}

export interface TransitLeg {
  /** WALK, BIKE, CAR, CAR_PARKING, RENTAL, BUS, RAIL, TRAM, SUBWAY, FERRY, etc. */
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
  /** True for transit legs, false for walking/cycling/rental/car */
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

  // Realtime fields (present when MOTIS has GTFS-RT data)
  /** True if this leg has realtime data */
  realTime?: boolean
  /** Departure delay in seconds (positive = late, negative = early) */
  departureDelay?: number
  /** Arrival delay in seconds */
  arrivalDelay?: number

  // Rental/shared mobility fields (present for RENTAL legs from GBFS)
  /** GBFS rental provider name */
  rentalProvider?: string
  /** GBFS station name (for docked rentals) */
  rentalStationName?: string
  /** Vehicle form factor (BICYCLE, SCOOTER_STANDING, etc.) */
  rentalFormFactor?: string
  /** GBFS station ID */
  rentalStationId?: string
  /** Deep link URI for unlocking the rental vehicle */
  rentalUri?: string
  /** Propulsion type (HUMAN, ELECTRIC_ASSIST, ELECTRIC) */
  rentalPropulsionType?: string
  /** Destination station name (for docked returns) */
  rentalToStationName?: string
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
 * MOTIS v2 returns geometry in the OTPAPI `legGeometry.points` format
 * using precision 7 (1e7), not the Google Maps standard precision 5.
 */
function decodePolyline(encoded: string, precision = 7): [number, number][] {
  const factor = 10 ** precision
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

    coords.push([lng / factor, lat / factor])
  }

  return coords
}

/** Convert epoch milliseconds to ISO 8601 string */
function epochToIso(epoch: number): string {
  return new Date(epoch).toISOString()
}

const STREET_MODES = new Set(['WALK', 'BIKE', 'BICYCLE', 'CAR', 'CAR_PARKING', 'CAR_DROPOFF', 'RENTAL'])

/** Adapt a single MOTIS/OTPAPI leg into our format */
function adaptLeg(leg: any): TransitLeg {
  const isTransit = !STREET_MODES.has(leg.mode)

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

    // Pass through GTFS-RT realtime fields when available
    if (leg.realTime != null) adapted.realTime = leg.realTime
    if (leg.departureDelay != null) adapted.departureDelay = leg.departureDelay
    if (leg.arrivalDelay != null) adapted.arrivalDelay = leg.arrivalDelay
  }

  // Rental/shared mobility fields (GBFS legs from intermodal routing)
  if (leg.mode === 'RENTAL' && leg.rental) {
    adapted.rentalProvider = leg.rental.systemName || leg.rental.providerGroupId || undefined
    adapted.rentalStationName = leg.rental.fromStationName || leg.from?.name || undefined
    adapted.rentalFormFactor = leg.rental.formFactor || undefined
    adapted.rentalStationId = leg.from?.stopId || undefined
    adapted.rentalUri = leg.rental.rentalUriIOS || leg.rental.rentalUriAndroid || leg.rental.rentalUriWeb || undefined
    adapted.rentalPropulsionType = leg.rental.propulsionType || undefined
    adapted.rentalToStationName = leg.rental.toStationName || leg.to?.name || undefined
  }

  return adapted
}

/** Adapt a MOTIS/OTPAPI itinerary into our format */
function adaptItinerary(itin: any): TransitItinerary {
  const adapted: TransitItinerary = {
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

  // Extract fare data from MOTIS response.
  // MOTIS v2 OTPAPI may return fares in several formats:
  //   1. fareProducts[]: OTP2 format with amount.amount + amount.currency.code
  //   2. fare.fare.regular: older OTP2 format with cents + currency
  const fare = extractFare(itin)
  if (fare) adapted.fare = fare

  return adapted
}

/**
 * Extract fare from a MOTIS itinerary response.
 * Handles multiple OTP2 fare response formats.
 */
function extractFare(itin: any): { currency: string; amount: number } | undefined {
  // OTP2 fareProducts format (MOTIS v2)
  if (Array.isArray(itin.fareProducts) && itin.fareProducts.length > 0) {
    const product = itin.fareProducts[0]
    const amount = product.amount ?? product.price
    if (amount?.amount != null && amount?.currency?.code) {
      return { currency: amount.currency.code, amount: amount.amount }
    }
  }

  // Older OTP2 fare format
  if (itin.fare?.fare) {
    const regular = itin.fare.fare.regular
    if (regular?.cents != null && regular?.currency?.currency) {
      return {
        currency: regular.currency.currency,
        amount: regular.cents / 100,
      }
    }
  }

  return undefined
}

/**
 * Query MOTIS for transit routes between two points.
 *
 * Since MOTIS runs in transit-only mode (no street routing), it cannot
 * match raw coordinates to stops. This function first finds the closest
 * stops to origin and destination via PostGIS, then queries MOTIS with
 * stop IDs for accurate routing.
 *
 * If the origin or destination is already within ~50m of a stop, we use
 * that single stop. Otherwise, we try the closest stops and pick the
 * best itineraries across all combinations.
 */
export async function getTransitRoute(
  request: TransitRouteRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  // Find nearby stops for origin and destination
  const maxWalkDistance = request.maxWalkDistance || 1000
  const [originStops, destStops] = await Promise.all([
    getNearbyStops({ lat: request.from.lat, lng: request.from.lng, radius: maxWalkDistance, limit: 3 }),
    getNearbyStops({ lat: request.to.lat, lng: request.to.lng, radius: maxWalkDistance, limit: 3 }),
  ])

  if (originStops.length === 0 || destStops.length === 0) {
    return { itineraries: [], metadata: { searchWindow: 0 } }
  }

  // Build stop ID pairs to try (closest origin × closest destination)
  const pairs: Array<{ fromStopId: string; toStopId: string }> = []
  for (const from of originStops) {
    for (const to of destStops) {
      if (`${from.feedId}_${from.stopId}` !== `${to.feedId}_${to.stopId}`) {
        pairs.push({
          fromStopId: `${from.feedId}_${from.stopId}`,
          toStopId: `${to.feedId}_${to.stopId}`,
        })
      }
    }
  }

  if (pairs.length === 0) {
    return { itineraries: [], metadata: { searchWindow: 0 } }
  }

  // Query MOTIS for each pair in parallel, collect all itineraries
  const results = await Promise.all(
    pairs.slice(0, 4).map(pair =>
      queryMotis({ ...request, fromStopId: pair.fromStopId, toStopId: pair.toStopId }, fetchFn)
        .catch(() => null)
    ),
  )

  // Merge and deduplicate itineraries, sort by duration
  const allItineraries: TransitItinerary[] = []
  let metadata: TransitRouteResponse['metadata'] = { searchWindow: 0 }

  for (const result of results) {
    if (!result) continue
    allItineraries.push(...result.itineraries)
    if (result.metadata) metadata = result.metadata
  }

  // Sort by duration and take the requested number
  allItineraries.sort((a, b) => a.duration - b.duration)
  const numItineraries = request.numItineraries ?? 5
  const uniqueItineraries = deduplicateItineraries(allItineraries).slice(0, numItineraries)

  return { itineraries: uniqueItineraries, metadata }
}

/** Deduplicate itineraries by start time + duration + number of legs */
function deduplicateItineraries(itineraries: TransitItinerary[]): TransitItinerary[] {
  const seen = new Set<string>()
  return itineraries.filter(it => {
    const key = `${it.startTime}|${it.duration}|${it.legs.length}`
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
}

/**
 * Low-level MOTIS query using stop IDs.
 */
async function queryMotis(
  request: TransitRouteRequest & { fromStopId: string; toStopId: string },
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  const motisUrl = getMotisUrl()

  // Parse time or default to now
  const departureDate = request.time ? new Date(request.time) : new Date()

  const params = new URLSearchParams({
    fromPlace: request.fromStopId,
    toPlace: request.toStopId,
    time: departureDate.toISOString(),
    arriveBy: String(request.arriveBy ?? false),
    numItineraries: String(request.numItineraries ?? 5),
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

  // MOTIS v2 returns itineraries at the top level, not nested under `plan`
  const itineraries = data.itineraries || data.plan?.itineraries
  if (!itineraries || itineraries.length === 0) {
    return { itineraries: [], metadata: { searchWindow: 0 } }
  }

  return {
    itineraries: itineraries.map(adaptItinerary),
    metadata: {
      searchWindow: data.searchWindowUsed ?? data.plan?.searchWindowUsed ?? 0,
      nextPageCursor: data.nextPageCursor || undefined,
      prevPageCursor: data.previousPageCursor || undefined,
    },
  }
}

// ── Intermodal routing (coordinate-based) ─────────────────────────

/**
 * Query MOTIS with coordinate-based intermodal routing.
 *
 * Unlike queryMotis() which uses stop IDs, this uses lat,lng coordinates
 * directly. Requires MOTIS to have OSM street data loaded (street_routing: true).
 * Supports pre/post-transit mode selection (WALK, BIKE, CAR_PARKING, RENTAL).
 */
async function queryMotisIntermodal(
  request: IntermodalRouteRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  const motisUrl = getMotisUrl()
  const departureDate = request.time ? new Date(request.time) : new Date()

  const params = new URLSearchParams({
    fromPlace: `${request.from.lat},${request.from.lng}`,
    toPlace: `${request.to.lat},${request.to.lng}`,
    time: departureDate.toISOString(),
    arriveBy: String(request.arriveBy ?? false),
    numItineraries: String(request.numItineraries ?? 5),
  })

  // MOTIS defaults to 25m for matching coordinates/stops to the street
  // network, which strands off-street platforms (e.g. NYC subway stops
  // under Union Square Park): they become unreachable for access/egress
  // walks, so riders get absurd bus-first detours instead of boarding
  // the subway directly. 250m keeps every platform walkable; the walk is
  // still street-routed, so accuracy is unaffected. RENTAL queries keep
  // the MOTIS default — the wider radius multiplies GBFS candidate links
  // and blows those queries from ~2s to 20s+, and rental stations are
  // curbside anyway.
  const includesRental = [
    ...(request.preTransitModes ?? []),
    ...(request.postTransitModes ?? []),
  ].includes('RENTAL')
  if (!includesRental) {
    params.set('maxMatchingDistance', '250')
  }

  // Intermodal mode parameters
  if (request.preTransitModes?.length) {
    params.set('preTransitModes', request.preTransitModes.join(','))
  }
  if (request.postTransitModes?.length) {
    params.set('postTransitModes', request.postTransitModes.join(','))
  }
  // Explicitly pass directModes — MOTIS defaults to WALK which triggers
  // a slow direct walk computation. Pass empty to skip when not needed.
  if (request.directModes?.length) {
    params.set('directModes', request.directModes.join(','))
  } else {
    params.set('maxDirectTime', '0')
  }
  if (request.transitModes?.length) {
    params.set('transitModes', request.transitModes.join(','))
  }

  // Time limits for street legs
  if (request.maxPreTransitTime != null) {
    params.set('maxPreTransitTime', String(request.maxPreTransitTime))
  }
  if (request.maxPostTransitTime != null) {
    params.set('maxPostTransitTime', String(request.maxPostTransitTime))
  }

  // Rental vehicle filters
  if (request.preTransitRentalFormFactors?.length) {
    params.set('preTransitRentalFormFactors', request.preTransitRentalFormFactors.join(','))
  }
  if (request.postTransitRentalFormFactors?.length) {
    params.set('postTransitRentalFormFactors', request.postTransitRentalFormFactors.join(','))
  }

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

  const itineraries = data.itineraries || data.plan?.itineraries || []
  const direct = data.direct || []

  // Merge transit itineraries and direct connections
  const allItineraries = [
    ...itineraries.map(adaptItinerary),
    ...direct.map(adaptItinerary),
  ]

  if (allItineraries.length === 0) {
    return { itineraries: [], metadata: { searchWindow: 0 } }
  }

  return {
    itineraries: allItineraries,
    metadata: {
      searchWindow: data.searchWindowUsed ?? data.plan?.searchWindowUsed ?? 0,
      nextPageCursor: data.nextPageCursor || undefined,
      prevPageCursor: data.previousPageCursor || undefined,
    },
  }
}

/**
 * Intermodal routing using coordinates with mode selection.
 *
 * Simpler than getTransitRoute() — no stop ID cross-product needed.
 * MOTIS handles coordinate snapping internally when OSM is loaded.
 */
export async function getIntermodalRoute(
  request: IntermodalRouteRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  const result = await queryMotisIntermodal(request, fetchFn)

  // Sort by duration and deduplicate
  result.itineraries.sort((a, b) => a.duration - b.duration)
  const numItineraries = request.numItineraries ?? 5
  result.itineraries = deduplicateItineraries(result.itineraries).slice(0, numItineraries)

  return result
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
