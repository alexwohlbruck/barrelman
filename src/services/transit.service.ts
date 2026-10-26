/**
 * Transit Routing Service (MOTIS)
 *
 * Plans trips on the MOTIS transit router and adapts its responses into
 * Barrelman's itinerary format. Walking legs are straight-line estimates;
 * the Parchment server replaces them with GraphHopper walking routes during
 * trip composition. See the Planning section for how a trip is searched.
 */

import { db } from '../db'
import { haversine } from './route.service'
import { sql, type SQL } from 'drizzle-orm'
import {
  ensurePricing,
  pricingForLeg,
  type RentalPricing,
} from './rental-pricing.service'

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
    /**
     * Echoes that bike carriage was actually enforced on this search.
     *
     * A caller cannot tell "no carriage data, so no results" from "this
     * server is too old to know the flag and ignored it" — the second would
     * hand back rides the bike may not be allowed on. Echoing lets the
     * caller require the confirmation and fail safe without it.
     */
    requireBikeTransport?: boolean
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
  /**
   * How many separate fares this trip charges — how many times a rider
   * reaches for their card, which is not the same as the number of legs.
   *
   * MOTIS groups an itinerary's legs into FARE TRANSFERS: consecutive legs
   * joined by a matching GTFS fare_transfer_rule become one payment, legs
   * with no rule between them become two. So a subway change inside the
   * gates is one payment across two legs, while stepping out to another
   * operator is two. Undefined when the feeds carry no fare data and there
   * is nothing to group by.
   */
  farePayments?: number
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
  /** Max duration (s) for direct (non-transit) connections. MOTIS defaults
   *  to 1800, which silently drops e.g. a 31-minute shared-bike ride. */
  maxDirectTime?: number
  /** Max first-mile time in seconds (default 900 = 15 min) */
  maxPreTransitTime?: number
  /** Max last-mile time in seconds (default 900 = 15 min) */
  maxPostTransitTime?: number
  /** Filter rental vehicles to specific form factors */
  preTransitRentalFormFactors?: RentalFormFactor[]
  postTransitRentalFormFactors?: RentalFormFactor[]
  /**
   * Minutes reserved per interchange (MOTIS additionalTransferTime).
   * Default 3 — discourages marginal-gain transfer chains. Callers can
   * raise it (e.g. 15) to sweep for least-transfer itineraries that pure
   * time-optimal search would Pareto-dominate away.
   */
  additionalTransferTime?: number
  /**
   * Restrict to trips that allow bike carriage, for riding a bike onto
   * transit and off again at the far end.
   *
   * Answered from GTFS `bikes_allowed`, where "no information" counts as no —
   * so a feed that omits it yields nothing rather than a guess.
   */
  requireBikeTransport?: boolean
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
  /** GTFS direction_id ("0"/"1") — which way the trip runs; used to filter the
   *  merged departure board to the rider's direction. */
  directionId?: string
  /** Intermediate stops (between boarding and alighting) */
  intermediateStops?: TransitLegPlace[]
  /** Every route that runs this exact board→…→alight station sequence directly
   *  (interchangeable lines — the 4 and the 5, the N and the Q), derived from
   *  GTFS trip patterns rather than from what MOTIS returned. Includes this
   *  leg's own route. Present (length ≥ 2) only when a true alternate exists. */
  interchangeableRoutes?: InterchangeableRoute[]

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
  /** GBFS system_id (joins the leg to its operator's pricing). */
  rentalSystemId?: string
  /** Estimated fare from the operator's GBFS pricing feed, when published. */
  rentalPricing?: RentalPricing
}

/** A route that serves a leg's exact station sequence directly. */
export interface InterchangeableRoute {
  routeId: string
  shortName?: string
  longName?: string
  routeType?: number
  color?: string
  textColor?: string
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
  /**
   * How a rider reaches this line from the stop asked about.
   *
   * `station` — it calls at this station; a board here shows it departing.
   * `transfer` — it calls at another station the agency's `transfers.txt`
   * connects to this one, so it is reachable without leaving the paid area:
   * the J and Z at Chambers St from Brooklyn Bridge–City Hall. Useful to
   * offer, wrong to list as departing from here.
   * `nearby` — it calls at a stop within walking distance that the feed does
   * not join to this station. Usually another agency's: the M22 outside
   * Brooklyn Bridge–City Hall. Carries no promise about fare.
   */
  via: 'station' | 'transfer' | 'nearby'
  /** Metres to the nearest stop serving this line, on `nearby` rows only. */
  distanceM?: number
}

export type FetchFn = (url: string, init?: RequestInit) => Promise<Response>

// ── Activity tracking (for warm-up idle-gating) ─────────────────────

// When MOTIS last served a *real*, HTTP-driven transit query. The warm-up
// loop reads this so it can stand down while live traffic is already keeping
// MOTIS hot — warm-up only needs to fill genuine idle gaps, and firing it
// alongside real requests would just add MOTIS contention. Set by the route
// handlers (not by getIntermodalRoute itself, which the in-process warm-up
// calls directly and must not count as activity).
let lastTransitActivityAt = 0

/** Record a real transit query — call from HTTP route handlers only. */
export function markTransitActivity(): void {
  lastTransitActivityAt = Date.now()
}

/** Milliseconds since the last real transit query (huge before the first). */
export function transitIdleMs(): number {
  return Date.now() - lastTransitActivityAt
}

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
    adapted.routeShortName = leg.routeShortName || undefined
    adapted.routeLongName = leg.routeLongName || undefined
    adapted.routeColor = leg.routeColor || undefined
    adapted.routeTextColor = leg.routeTextColor || undefined
    adapted.agencyName = leg.agencyName || undefined
    adapted.agencyId = leg.agencyId || undefined
    adapted.tripId = leg.tripId || undefined
    adapted.headsign = leg.headsign || undefined
    adapted.routeId = leg.routeId || undefined
    adapted.directionId = leg.directionId != null ? String(leg.directionId) : undefined

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
    adapted.rentalSystemId = leg.rental.systemId || leg.rental.providerId || undefined
    // Pricing is read from a cache pre-warmed by the caller (ensurePricing);
    // absent until then, and absent for systems with no published fares.
    adapted.rentalPricing = pricingForLeg(
      adapted.rentalSystemId,
      adapted.duration,
      adapted.distance,
    )
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

  const fare = extractFare(itin)
  if (fare) adapted.fare = fare
  const payments = countFarePayments(itin)
  if (payments != null) adapted.farePayments = payments

  return adapted
}

/**
 * How many separate fares an itinerary charges, per MOTIS's own grouping.
 *
 * `fareTransfers` IS the answer: MOTIS emits one entry per fare payment,
 * having already applied the feeds' fare_transfer_rules. We only have to
 * refuse to report it when it means nothing — with no fare data at all
 * MOTIS still emits one empty entry per leg, which would read as "every
 * leg costs a fare" when the truth is that nobody knows.
 */
export function countFarePayments(itin: any): number | undefined {
  if (!Array.isArray(itin?.fareTransfers) || itin.fareTransfers.length === 0) {
    return undefined
  }
  const priced = itin.fareTransfers.some((ft: any) =>
    (ft?.effectiveFareLegProducts ?? []).some((alts: any[]) => alts?.length) ||
    (ft?.transferProducts ?? []).length > 0,
  )
  return priced ? itin.fareTransfers.length : undefined
}

/** Extract the total fare from a MOTIS itinerary's GTFS Fares v2 data. */
export function extractFare(itin: any): { currency: string; amount: number } | undefined {
  // GTFS Fares v2 (MOTIS withFares=true): fareTransfers[] groups the
  // itinerary's fare legs. Per fare leg, effectiveFareLegProducts lists
  // alternatives (rider categories); each alternative is a product set to
  // combine. Take the default rider category (else the first alternative)
  // and sum across legs, plus any explicit transfer products. If ANY fare
  // leg has no products (its agency publishes no fares — e.g. the MTA),
  // the itinerary's true cost is unknown: report nothing rather than a
  // misleading partial sum.
  if (Array.isArray(itin.fareTransfers) && itin.fareTransfers.length > 0) {
    let total = 0
    let currency: string | undefined
    let found = false
    const addProduct = (p: any) => {
      if (p?.amount == null) return
      total += p.amount
      currency ||= p.currency
      found = true
    }
    for (const ft of itin.fareTransfers) {
      for (const alternatives of ft.effectiveFareLegProducts ?? []) {
        if (!alternatives?.length) return undefined // unpriced fare leg
        const pick =
          alternatives.find((products: any[]) =>
            products?.some((p) => p?.riderCategory?.isDefaultFareCategory),
          ) ?? alternatives[0]
        for (const p of pick ?? []) addProduct(p)
      }
      for (const p of ft.transferProducts ?? []) addProduct(p)
    }
    if (found) {
      return { currency: currency || 'USD', amount: Math.round(total * 100) / 100 }
    }
  }

  return undefined
}

// ── Planning ────────────────────────────────────────────────────────
//
// Every query, whatever its shape, goes through `planParams` (what a
// MOTIS plan search means) and `motisPlan` (sending it and reading the
// answer). Two front doors sit on top:
//
// - `planViaStations` for the ordinary case, walking at both ends. We pick
//   nearby stations ourselves and route station to station. MOTIS's own
//   access walks need its street graph, which a large network can't have
//   (the US breaks its 16-ways-per-node limit and the build falls back to
//   timetable-only), and even with one they strand platforms whose entrances
//   OSM doesn't join to the sidewalk. The walk legs we add are straight
//   lines; Parchment re-routes them on GraphHopper.
// - `planFromCoordinates` for bike, car and rental access, which need MOTIS's
//   street graph and return nothing on a timetable-only build.
//
// Both hand their itineraries to `rankItineraries`.

/** Walking speed (m/s) for station reach and straight-line access times.
 *  Real geometry and timing come from GraphHopper downstream. */
const WALK_MPS = 1.25

/** Stations tried at each end. All origin × destination pairs are queried —
 *  16 MOTIS searches at most, each a few tens of milliseconds. */
const STATIONS_PER_END = 4

/** Search radius bounds (m) at each end, whatever walk time is asked for. */
const MIN_STATION_RADIUS = 500
const MAX_STATION_RADIUS = 2500

/**
 * How much longer a trip may take before a saved fare stops being worth it.
 *
 * A rider asked for directions, not for the cheapest possible routing, so a
 * one-fare trip that takes half an hour longer is not the answer to their
 * question. Ten minutes is about where the trade turns for a city fare:
 * below it, most riders take the cheaper trip; above it, they do not.
 */
const FARE_SAVING_TOLERANCE_SEC = 10 * 60

/** The parameters every MOTIS plan search shares, whatever its places are. */
function planParams(request: IntermodalRouteRequest, fromPlace: string, toPlace: string): URLSearchParams {
  const params = new URLSearchParams({
    fromPlace,
    toPlace,
    time: (request.time ? new Date(request.time) : new Date()).toISOString(),
    arriveBy: String(request.arriveBy ?? false),
    numItineraries: String(request.numItineraries ?? 5),
    // Fare computation from GTFS Fares v2 (native or synthesized from v1
    // by import/inject-fares-v2.ts). Cheap when a feed has no fare data.
    withFares: 'true',
    // Pad each interchange (default 3 min) so RAPTOR stops surfacing
    // marginal-gain transfer chains: a one-block bus hop that saves 90
    // seconds, three-vehicle relays. Genuinely faster transfers survive,
    // and itineraries tolerate a late connection better.
    additionalTransferTime: String(request.additionalTransferTime ?? 3),
  })
  if (request.searchWindow != null) params.set('searchWindow', String(request.searchWindow))
  if (request.maxTransfers != null) params.set('maxTransfers', String(request.maxTransfers))
  if (request.transitModes?.length) params.set('transitModes', request.transitModes.join(','))
  if (request.wheelchair) params.set('wheelchair', 'true')
  if (request.requireBikeTransport) params.set('requireBikeTransport', 'true')
  return params
}

/** Run one MOTIS plan search and adapt what comes back. */
async function motisPlan(
  params: URLSearchParams,
  request: IntermodalRouteRequest,
  fetchFn: FetchFn,
): Promise<TransitRouteResponse> {
  const response = await fetchFn(`${getMotisUrl()}/api/v1/plan?${params}`, {
    signal: AbortSignal.timeout(20_000),
  })
  if (!response.ok) throw new MotisError(response.status, await response.text())

  const data = await response.json() as any
  const raw: any[] = [...(data.itineraries ?? []), ...(data.direct ?? [])]

  // Fire-and-forget rental pricing warm-up — don't block the response.
  // pricingForLeg() returns nothing on a cache miss; the next request has it.
  const rentalSystemIds = raw.flatMap((it: any) =>
    (it.legs ?? [])
      .filter((l: any) => l.mode === 'RENTAL' && l.rental)
      .map((l: any) => l.rental.systemId || l.rental.providerId)
      .filter(Boolean),
  )
  if (rentalSystemIds.length) ensurePricing(rentalSystemIds).catch(() => {})

  return {
    itineraries: raw.map(adaptItinerary),
    metadata: {
      searchWindow: data.searchWindowUsed ?? 0,
      nextPageCursor: data.nextPageCursor || undefined,
      prevPageCursor: data.previousPageCursor || undefined,
      ...(request.requireBikeTransport && { requireBikeTransport: true }),
    },
  }
}

/** Bike, car and rental access: MOTIS routes the street legs itself. */
function planFromCoordinates(request: IntermodalRouteRequest, fetchFn: FetchFn): Promise<TransitRouteResponse> {
  const params = planParams(
    request,
    `${request.from.lat},${request.from.lng}`,
    `${request.to.lat},${request.to.lng}`,
  )
  const pre = request.preTransitModes ?? []
  const post = request.postTransitModes ?? []
  if (pre.length) params.set('preTransitModes', pre.join(','))
  if (post.length) params.set('postTransitModes', post.join(','))

  // MOTIS defaults to 25m for matching coordinates/stops to the street
  // network, which strands off-street platforms (e.g. NYC subway stops
  // under Union Square Park): they become unreachable for access/egress,
  // so riders get bus-first detours instead of boarding the subway. 250m
  // keeps every platform reachable; the walk is still street-routed.
  // RENTAL queries keep the default — the wider radius multiplies GBFS
  // candidate links and blows those queries from ~2s to 20s+, and rental
  // stations are curbside anyway.
  if (![...pre, ...post].includes('RENTAL')) params.set('maxMatchingDistance', '250')

  // MOTIS computes a direct walk unless told otherwise, which is slow.
  // Its 1800s cap on direct rides truncates legitimate ones (a 31-minute
  // shared bike across Brooklyn vanishes), so default to an hour.
  if (request.directModes?.length) {
    params.set('directModes', request.directModes.join(','))
    params.set('maxDirectTime', String(request.maxDirectTime ?? 3600))
  } else {
    params.set('maxDirectTime', '0')
  }

  if (request.maxPreTransitTime != null) params.set('maxPreTransitTime', String(request.maxPreTransitTime))
  if (request.maxPostTransitTime != null) params.set('maxPostTransitTime', String(request.maxPostTransitTime))
  if (request.preTransitRentalFormFactors?.length) {
    params.set('preTransitRentalFormFactors', request.preTransitRentalFormFactors.join(','))
  }
  if (request.postTransitRentalFormFactors?.length) {
    params.set('postTransitRentalFormFactors', request.postTransitRentalFormFactors.join(','))
  }

  return motisPlan(params, request, fetchFn)
}

interface Station {
  feedId: string
  stationId: string
  lat: number
  lng: number
}

/**
 * The stations to try near a point: rail, subway, tram and ferry first,
 * nearest within each tier.
 *
 * Without the tier, the handful of bus stops on the nearest corner would
 * crowd out a subway station 500 m away on any multi-km trip. Buses are
 * not lost by it: MOTIS walks from a station to the stops around it, so a
 * bus reachable from the chosen stations still comes back.
 *
 * A station counts as bus-only when every route it serves is a bus. The
 * test mirrors `gtfsModeClass` in src/lib/gtfs-modes.ts: 3 and 11 are
 * bus and trolleybus, and 700-899 are the extended bus and trolleybus
 * types. A station with no routes at all ranks with the buses.
 */
async function nearbyStations(at: { lat: number; lng: number }, radiusM: number): Promise<Station[]> {
  const rows = (await db.execute(sql`
    SELECT feed_id, station_id, stop_lat, stop_lon
    FROM (
      SELECT DISTINCT ON (st.feed_id, COALESCE(NULLIF(st.parent_station, ''), st.stop_id))
        st.feed_id,
        COALESCE(NULLIF(st.parent_station, ''), st.stop_id) AS station_id,
        st.stop_lat,
        st.stop_lon,
        ST_Distance(st.geom::geography, ST_SetSRID(ST_MakePoint(${at.lng}, ${at.lat}), 4326)::geography) AS distance,
        COALESCE((
          SELECT bool_and(r.route_type IN (3, 11) OR r.route_type BETWEEN 700 AND 899)
          FROM gtfs_stop_routes sr
          JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
          WHERE sr.feed_id = st.feed_id AND sr.stop_id = st.stop_id
        ), true) AS bus_only
      FROM gtfs_stops st
      WHERE ST_DWithin(st.geom::geography, ST_SetSRID(ST_MakePoint(${at.lng}, ${at.lat}), 4326)::geography, ${radiusM})
        AND (st.location_type = 0 OR st.location_type IS NULL)
      ORDER BY st.feed_id, COALESCE(NULLIF(st.parent_station, ''), st.stop_id), distance
    ) s
    ORDER BY bus_only, distance
    LIMIT ${STATIONS_PER_END}
  `)) as any[]
  return rows.map((r) => ({
    feedId: r.feed_id,
    stationId: r.station_id,
    lat: r.stop_lat,
    lng: r.stop_lon,
  }))
}

/** Straight-line walking time between two points, in whole seconds. */
function walkSeconds(a: { lat: number; lng: number }, b: { lat: number; lng: number }): number {
  return Math.max(1, Math.round(haversine(a.lat, a.lng, b.lat, b.lng) / WALK_MPS))
}

/** Wrap a station-to-station itinerary in straight-line walks from the
 *  origin and to the destination, adjusting its totals to match. */
function withAccessEgressWalks(
  it: TransitItinerary,
  from: { lat: number; lng: number },
  to: { lat: number; lng: number },
): TransitItinerary {
  const first = it.legs[0]
  const last = it.legs[it.legs.length - 1]
  if (!first || !last) return it

  const walk = (
    a: TransitLegPlace,
    b: TransitLegPlace,
    endsAt: Date,
    seconds: number,
  ): TransitLeg => ({
    mode: 'WALK',
    transitLeg: false,
    from: a,
    to: b,
    startTime: new Date(endsAt.getTime() - seconds * 1000).toISOString(),
    endTime: endsAt.toISOString(),
    duration: seconds,
    distance: Math.round(haversine(a.lat, a.lng, b.lat, b.lng)),
    geometry: { type: 'LineString', coordinates: [[a.lng, a.lat], [b.lng, b.lat]] },
  })

  const boardAt = { name: first.from.name, lat: first.from.lat, lng: first.from.lng, stopId: first.from.stopId }
  const alightAt = { name: last.to.name, lat: last.to.lat, lng: last.to.lng, stopId: last.to.stopId }
  const accessSec = walkSeconds(from, boardAt)
  const egressSec = walkSeconds(alightAt, to)
  const access = walk({ name: 'Origin', ...from }, boardAt, new Date(first.startTime), accessSec)
  const egress = walk(
    alightAt,
    { name: 'Destination', ...to },
    new Date(new Date(last.endTime).getTime() + egressSec * 1000),
    egressSec,
  )

  return {
    ...it,
    legs: [access, ...it.legs, egress],
    startTime: access.startTime,
    endTime: egress.endTime,
    duration: it.duration + accessSec + egressSec,
    walkTime: (it.walkTime || 0) + accessSec + egressSec,
    walkDistance: (it.walkDistance || 0) + access.distance + egress.distance,
  }
}

/** Feed pairs already warned about for MOTIS timetable misses (once per process). */
const motis404Warned = new Set<string>()

/**
 * Walking at both ends: route between nearby stations and walk to them.
 *
 * Every origin station is paired with every destination station. Dropping
 * pairs to save queries is how an obvious train goes missing: the fourth
 * station is as likely as the first to be the one with the direct line.
 */
async function planViaStations(request: IntermodalRouteRequest, fetchFn: FetchFn): Promise<TransitRouteResponse> {
  const radius = (seconds = 900) =>
    Math.min(Math.max(seconds * WALK_MPS, MIN_STATION_RADIUS), MAX_STATION_RADIUS)
  const [origins, dests] = await Promise.all([
    nearbyStations(request.from, radius(request.maxPreTransitTime)),
    nearbyStations(request.to, radius(request.maxPostTransitTime)),
  ])

  const pairs = origins.flatMap((o) =>
    dests
      .filter((d) => o.feedId !== d.feedId || o.stationId !== d.stationId)
      .map((d) => ({ o, d })),
  )

  const baseMs = request.time ? new Date(request.time).getTime() : Date.now()
  const settled = await Promise.allSettled(
    pairs.map(async ({ o, d }) => {
      // Shift the search so a departure it returns is one the rider can
      // reach: depart-by starts after the access walk, arrive-by ends early
      // enough to finish the egress walk in time.
      const queryMs = request.arriveBy
        ? baseMs - walkSeconds(d, request.to) * 1000
        : baseMs + walkSeconds(request.from, o) * 1000
      const params = planParams(
        { ...request, time: new Date(queryMs).toISOString() },
        `${o.feedId}_${o.stationId}`,
        `${d.feedId}_${d.stationId}`,
      )
      try {
        const { itineraries } = await motisPlan(params, request, fetchFn)
        return itineraries.map((it) => withAccessEgressWalks(it, request.from, request.to))
      } catch (err) {
        // A 404 means the stop is in PostGIS but not in the baked MOTIS
        // timetable (GTFS ahead of the bake). That pair has no answer, which
        // is not an outage. Warn once per feed pair so the drift is visible.
        if (err instanceof MotisError && err.statusCode === 404) {
          const key = `${o.feedId}:${d.feedId}`
          if (!motis404Warned.has(key)) {
            motis404Warned.add(key)
            console.warn(
              `[transit] MOTIS has no timetable location for ${o.feedId}_${o.stationId} → ` +
              `${d.feedId}_${d.stationId} — GTFS DB is ahead of the MOTIS bake for these feeds`,
            )
          }
          return []
        }
        throw err
      }
    }),
  )

  // One bad pair must not sink the rest. But if no pair answered and some
  // failed for real, MOTIS is down, and an empty 200 would pass that off as
  // "no route".
  const answered = settled.filter((s) => s.status === 'fulfilled')
  const failure = settled.find((s): s is PromiseRejectedResult => s.status === 'rejected')
  if (!answered.length && failure) throw failure.reason

  return {
    itineraries: answered.flatMap((s) => (s as PromiseFulfilledResult<TransitItinerary[]>).value),
    metadata: { searchWindow: 0 },
  }
}

/** What a rider sees as "the same trip": the vehicles boarded and where. */
function rideSignature(it: TransitItinerary): string {
  const rides = it.legs.filter((l) => l.transitLeg)
  if (!rides.length) {
    return `direct|${it.legs.map((l) => l.mode).join(',')}|${it.startTime}|${it.duration}`
  }
  return rides.map((l) => `${l.tripId ?? l.routeId}:${l.from.stopId}>${l.to.stopId}`).join('|')
}

/**
 * Order itineraries for the rider, drop repeats of the same ride, and keep
 * the best `limit`.
 *
 * Depart-at searches rank by arrival time, arrive-by searches by latest
 * departure. Ranking by duration instead let later trains with slightly
 * quicker rides fill the list and push out the first train.
 *
 * Each extra fare payment counts as FARE_SAVING_TOLERANCE_SEC, so a one-fare
 * trip beats a two-fare one that is less than ten minutes better. MOTIS is
 * Pareto over time and transfers, and an out-of-system walk is fewer
 * transfers, so without this it ranks a two-fare walk-between-stations trip
 * ahead of the one-fare trip arriving at the same minute. Unknown payment
 * counts cost nothing: a gap in fare coverage is not evidence of a price.
 *
 * The same ride reached from different stations (the station fan-out finds
 * it once per pair) is kept once, in its best-ranked form.
 */
export function rankItineraries(
  itineraries: TransitItinerary[],
  limit: number,
  arriveBy = false,
): TransitItinerary[] {
  const cost = (it: TransitItinerary) => {
    const extraFares = Math.max(0, (it.farePayments ?? 1) - 1)
    const anchor = arriveBy
      ? -new Date(it.startTime).getTime()
      : new Date(it.endTime).getTime()
    return anchor / 1000 + extraFares * FARE_SAVING_TOLERANCE_SEC
  }
  const ranked = itineraries
    .map((it) => ({ it, cost: cost(it) }))
    .sort((a, b) => a.cost - b.cost || a.it.duration - b.it.duration)

  const seen = new Set<string>()
  const kept: TransitItinerary[] = []
  for (const { it } of ranked) {
    const key = rideSignature(it)
    if (seen.has(key)) continue
    seen.add(key)
    kept.push(it)
    if (kept.length === limit) break
  }
  return kept
}

/**
 * Plan a transit trip between two coordinates.
 *
 * Walking at both ends goes station to station (`planViaStations`); any
 * other access mode needs MOTIS's street graph (`planFromCoordinates`).
 */
export async function getIntermodalRoute(
  request: IntermodalRouteRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  const walkOnly =
    !request.directModes?.length &&
    (request.preTransitModes ?? ['WALK']).every((m) => m === 'WALK') &&
    (request.postTransitModes ?? ['WALK']).every((m) => m === 'WALK')

  const result = walkOnly
    ? await planViaStations(request, fetchFn)
    : await planFromCoordinates(request, fetchFn)
  result.itineraries = rankItineraries(result.itineraries, request.numItineraries ?? 5, request.arriveBy)

  // Attach the full set of interchangeable lines to each transit leg, so the
  // rider sees every train that runs the segment directly — independent of
  // which ones MOTIS's time-optimal search happened to surface.
  await enrichInterchangeableRoutes(result.itineraries)

  return result
}

/**
 * Plan a walk-and-transit trip: `getIntermodalRoute` with walking at both
 * ends, reaching stations within `maxWalkDistance` (default 1000 m).
 */
export function getTransitRoute(
  request: TransitRouteRequest,
  fetchFn: FetchFn = globalThis.fetch,
): Promise<TransitRouteResponse> {
  const walkSec = (request.maxWalkDistance ?? 1000) / WALK_MPS
  return getIntermodalRoute(
    { ...request, maxPreTransitTime: walkSec, maxPostTransitTime: walkSec },
    fetchFn,
  )
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

  const result = await db.execute(sql`
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
  `)

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
 * Get routes reachable at a given stop, each marked with how.
 *
 * A station and its transfer complex are not the same list, and conflating
 * them is what puts the A and C on a Brooklyn Bridge–City Hall board. So the
 * answer separates them:
 *
 *   `via: 'station'`  — the line calls here. Its trains depart from this
 *     station's own platforms, which for a GTFS parent means every child
 *     stop_times references (640N/640S, not 640).
 *   `via: 'transfer'` — the line calls at a station the agency's
 *     transfers.txt joins to this one, so a rider reaches it without leaving
 *     the paid area. At Brooklyn Bridge–City Hall that is the J and Z, which
 *     stop at Chambers St one connection away.
 *
 * Both are worth showing and they mean different things, so the caller gets to
 * decide. Station lines sort first.
 *
 * `complex` collapses that distinction on purpose. A rider who taps the merged
 * label the map draws over an interchange has asked about the interchange, not
 * about whichever platform group happens to carry the stop id — so every line
 * in it is one the place they tapped runs, and Brooklyn Bridge–City Hall
 * answers 4 5 6 J Z rather than 4 5 6 with the J and Z filed under transfers.
 * Tapping one station of the group keeps the split.
 *
 * Only what the agency published counts as a transfer. A free bus-to-subway
 * ride bought by a fare rule rather than a walk between platforms is not in
 * transfers.txt and does not appear here.
 */
/**
 * Fold a station name for comparison inside one feed.
 *
 * Case and punctuation only — deliberately not the full `normalizeStationName`
 * fold, which also maps "street" to "st" and expands ordinals. That one exists
 * to reconcile names across sources (a GTFS feed against OSM), and its synonyms
 * are what let it match loosely. Here both names come from the SAME feed's
 * stops.txt, where an operator spells its own stations consistently, so the
 * loose half buys nothing and could only merge two stations that are not one.
 *
 * Erring conservative is the right direction: a name this misses is reported
 * as a transfer, which is the honest answer for anything that is not plainly
 * the same station.
 */
const nameFold = (expr: SQL) => sql`btrim(regexp_replace(lower(${expr}), '[^a-z0-9]+', ' ', 'g'))`

export async function getRoutesForStop(
  feedId: string,
  stopId: string,
  nearby?: { lat: number; lng: number; radius?: number; limit?: number },
  opts?: { complex?: boolean },
): Promise<StopRoutesResult[]> {
  // Whether a line reached across the interchange counts as one of this
  // place's own. Bound as a parameter, so it rides the same prepared plan.
  const complexIsStation = opts?.complex === true
  const result = await db.execute(sql`
    WITH seed AS (
      -- the stop itself, plus its parent station when it's a platform
      SELECT ${stopId}::text AS sid
      UNION
      SELECT parent_station FROM gtfs_stops
      WHERE feed_id = ${feedId} AND stop_id = ${stopId}
        AND parent_station IS NOT NULL AND parent_station <> ''
    ),
    station AS (
      -- this station alone: the seed and the platforms filed under it
      SELECT sid FROM seed
      UNION
      SELECT s.stop_id FROM gtfs_stops s JOIN seed ON s.parent_station = seed.sid
      WHERE s.feed_id = ${feedId}
    ),
    -- What the seed station is called, for the same-name test below.
    seed_name AS (
      SELECT DISTINCT s.stop_name AS nm
      FROM gtfs_stops s JOIN seed ON s.stop_id = seed.sid
      WHERE s.feed_id = ${feedId} AND s.stop_name IS NOT NULL
    ),
    complex AS (
      SELECT sid, TRUE AS same_name FROM seed
      UNION
      SELECT t.to_stop_id, EXISTS (
        SELECT 1 FROM seed_name n WHERE ${nameFold(sql`n.nm`)} = ${nameFold(sql`d.stop_name`)}
      )
      FROM gtfs_transfers t
      JOIN seed ON t.from_stop_id = seed.sid
      JOIN gtfs_stops d ON d.feed_id = ${feedId} AND d.stop_id = t.to_stop_id
      WHERE t.feed_id = ${feedId} AND t.to_stop_id <> t.from_stop_id
      UNION
      SELECT t.from_stop_id, EXISTS (
        SELECT 1 FROM seed_name n WHERE ${nameFold(sql`n.nm`)} = ${nameFold(sql`d.stop_name`)}
      )
      FROM gtfs_transfers t
      JOIN seed ON t.to_stop_id = seed.sid
      JOIN gtfs_stops d ON d.feed_id = ${feedId} AND d.stop_id = t.from_stop_id
      WHERE t.feed_id = ${feedId} AND t.to_stop_id <> t.from_stop_id
    ),
    members AS (
      SELECT sid, bool_or(at_station) AS at_station FROM (
        SELECT sid, TRUE AS at_station FROM station
        UNION ALL
        SELECT sid, ${complexIsStation} AND same_name FROM complex
        UNION ALL
        SELECT s.stop_id, ${complexIsStation} AND c.same_name
        FROM gtfs_stops s JOIN complex c ON s.parent_station = c.sid
        WHERE s.feed_id = ${feedId}
      ) reachable
      GROUP BY sid
    )
    SELECT
      r.route_id,
      r.feed_id,
      r.route_short_name,
      r.route_long_name,
      r.route_type,
      r.route_color,
      r.route_text_color,
      r.agency_name,
      -- a line calling anywhere in this station is served by it, even when it
      -- also calls across the complex
      bool_or(m.at_station) AS at_station
    FROM gtfs_stop_routes sr
    JOIN members m ON sr.stop_id = m.sid
    JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
    WHERE sr.feed_id = ${feedId}
    GROUP BY
      r.route_id, r.feed_id, r.route_short_name, r.route_long_name,
      r.route_type, r.route_color, r.route_text_color, r.agency_name
    ORDER BY bool_or(m.at_station) DESC, r.route_type, r.route_short_name
  `)

  const inSystem: StopRoutesResult[] = (result as any[]).map((row: any) => ({
    routeId: row.route_id,
    feedId: row.feed_id,
    routeShortName: row.route_short_name,
    routeLongName: row.route_long_name,
    routeType: row.route_type,
    routeColor: row.route_color,
    routeTextColor: row.route_text_color,
    agencyName: row.agency_name,
    via: row.at_station ? 'station' : 'transfer',
  }))

  if (!nearby) return inSystem
  return [...inSystem, ...(await routesNearby(nearby, inSystem))]
}

/**
 * How far a "nearby" line may be. Short on purpose: this list is meant to be
 * the stops a rider can see from the station door, and the radius is the only
 * thing standing between that and a page of every bus in the district. Two
 * hundred metres around Brooklyn Bridge–City Hall finds the M22 across the
 * street and nothing else; four hundred finds sixty stops across ten feeds.
 *
 * It is a judgement, not a fact, and a suburban park-and-ride would want more
 * — hence the caller's override.
 */
const NEARBY_RADIUS = 200

/**
 * How many nearby lines to report at most.
 *
 * The radius alone does not bound this. Two hundred metres of Lower Manhattan
 * holds twenty-five distinct lines — Rector St has the 1 fifty metres away and
 * then a wall of Staten Island express buses (SIM1, SIM1C, SIM2, SIM4C, SIM15,
 * SIM32…) and commuter coaches, none of which a rider standing on the N/R/W
 * platform is looking for. A list that long is not information.
 *
 * The cap applies after the sort, which is route type before distance, so what
 * survives is the nearest train rather than the nearest bus stop.
 */
const NEARBY_LIMIT = 6

/**
 * Lines calling at stops near the station that the feed does not connect to it.
 *
 * Deliberately cross-feed, which is the whole point: a subway station's bus
 * connections live in another agency's feed and no `transfers.txt` can reach
 * them. That also means the same service can arrive several times over — New
 * York is covered by a handful of overlapping bus feeds, so the M22 appears
 * under three different feed ids — so rows are folded to one per line, keeping
 * the closest, and anything already reported as `station` or `transfer` is
 * dropped rather than repeated.
 */
async function routesNearby(
  {
    lat,
    lng,
    radius = NEARBY_RADIUS,
    limit = NEARBY_LIMIT,
  }: { lat: number; lng: number; radius?: number; limit?: number },
  inSystem: StopRoutesResult[],
): Promise<StopRoutesResult[]> {
  const rows = (await db.execute(sql`
    SELECT DISTINCT ON (r.route_id, r.feed_id)
      r.route_id, r.feed_id, r.route_short_name, r.route_long_name,
      r.route_type, r.route_color, r.route_text_color, r.agency_name,
      ST_Distance(
        s.geom::geography,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography
      ) AS distance
    FROM gtfs_stops s
    JOIN gtfs_stop_routes sr ON sr.feed_id = s.feed_id AND sr.stop_id = s.stop_id
    JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
    WHERE ST_DWithin(
      s.geom::geography,
      ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography,
      ${radius}
    )
    AND (s.location_type = 0 OR s.location_type IS NULL)
    ORDER BY r.route_id, r.feed_id, distance
  `)) as any[]

  // One row per line as a rider names it. Agency is part of the key because
  // two operators may both run a "1"; the feed id is not, because one operator
  // arriving twice from two feeds is the duplication being removed.
  const seen = new Set(
    inSystem.map((r) => lineKey(r.routeShortName, r.routeLongName, r.agencyName)),
  )
  const nearby: StopRoutesResult[] = []

  for (const row of rows.sort((a, b) => a.distance - b.distance)) {
    const key = lineKey(row.route_short_name, row.route_long_name, row.agency_name)
    if (seen.has(key)) continue
    seen.add(key)
    nearby.push({
      routeId: row.route_id,
      feedId: row.feed_id,
      routeShortName: row.route_short_name,
      routeLongName: row.route_long_name,
      routeType: row.route_type,
      routeColor: row.route_color,
      routeTextColor: row.route_text_color,
      agencyName: row.agency_name,
      via: 'nearby',
      distanceM: Math.round(row.distance),
    })
  }

  return nearby
    .sort(
      (a, b) =>
        a.routeType - b.routeType ||
        (a.distanceM ?? 0) - (b.distanceM ?? 0) ||
        (a.routeShortName ?? '').localeCompare(b.routeShortName ?? ''),
    )
    .slice(0, Math.max(0, limit))
}

/** What makes two rows the same line to a rider: its name and who runs it. */
function lineKey(
  shortName: string | null,
  longName: string | null,
  agencyName: string | null,
): string {
  return [
    (shortName || longName || '').trim().toLowerCase(),
    (agencyName || '').trim().toLowerCase(),
  ].join('\u0000')
}

/** Split a MOTIS stop id ("feedId_stopId") into parts. The feed tag has no
 *  underscore, so split on the first one and keep the rest (the raw GTFS
 *  stop_id, which may itself contain underscores) intact. */
function splitMotisStopId(motisId: string): { feedId: string; stopId: string } {
  const sep = motisId.indexOf('_')
  if (sep === -1) return { feedId: '', stopId: motisId }
  return { feedId: motisId.slice(0, sep), stopId: motisId.slice(sep + 1) }
}

/**
 * Every route that runs a given board→…→alight station sequence directly.
 *
 * `rawStopIds` is the leg's ordered GTFS stop ids (feed prefix stripped). They
 * are normalised to parent stations (matching how patterns are stored), then
 * any route whose stored pattern contains that contiguous normalised run is
 * returned — the interchangeable lines (4/5, N/Q), including the leg's own
 * route. The contiguous match is inherently direction-correct, and an express
 * that skips either endpoint (or a route that branches away mid-segment) simply
 * has no pattern containing the run, so it is excluded.
 */
export async function getRoutesForStopSequence(
  feedId: string,
  rawStopIds: string[],
): Promise<InterchangeableRoute[]> {
  if (rawStopIds.length < 2) return []

  // Normalise each distinct leg stop to its parent station. The id list is a
  // variable-length IN, so it is built from one parametrized fragment per entry
  // rather than by quoting — `sql.join` keeps each value a bind parameter.
  const uniq = [...new Set(rawStopIds)]
  const inList = sql.join(
    uniq.map((s) => sql`${s}`),
    sql`, `,
  )
  const normRows = await db.execute(sql`
    SELECT stop_id, COALESCE(NULLIF(parent_station, ''), stop_id) AS norm
    FROM gtfs_stops
    WHERE feed_id = ${feedId} AND stop_id IN (${inList})
  `)
  const normMap = new Map<string, string>()
  for (const r of normRows as any[]) normMap.set(r.stop_id, r.norm)

  const seq: string[] = []
  for (const id of rawStopIds) {
    const n = normMap.get(id) ?? id
    if (seq[seq.length - 1] !== n) seq.push(n) // drop consecutive dups
  }
  // A comma in an id would make the delimiter ambiguous (it can't, for sane
  // GTFS, but guard symmetrically with the pattern derivation).
  if (seq.length < 2 || seq.some((n) => n.includes(','))) return []
  const needle = `,${seq.join(',')},`

  const rows = await db.execute(sql`
    SELECT DISTINCT
      r.route_id, r.route_short_name, r.route_long_name,
      r.route_type, r.route_color, r.route_text_color
    FROM gtfs_trip_patterns p
    JOIN gtfs_routes r ON r.feed_id = p.feed_id AND r.route_id = p.route_id
    WHERE p.feed_id = ${feedId}
      AND strpos(p.stop_seq, ${needle}) > 0
    ORDER BY r.route_type, r.route_short_name
  `)

  return (rows as any[]).map((row) => ({
    routeId: row.route_id,
    shortName: row.route_short_name ?? undefined,
    longName: row.route_long_name ?? undefined,
    routeType: row.route_type ?? undefined,
    color: row.route_color ?? undefined,
    textColor: row.route_text_color ?? undefined,
  }))
}

/** Ensure the leg's own (planned) route is in its interchangeable set. The GTFS
 *  pattern query should already include it, but feed-version skew between MOTIS's
 *  baked timetable and the patterns table could omit it — and the departure board
 *  filters strictly to this set, so a missing planned line would hide the actual
 *  planned departure. */
function withSelfRoute(routes: InterchangeableRoute[], leg: TransitLeg): InterchangeableRoute[] {
  // Match on shortName, not routeId: the GTFS set carries raw route ids ("4")
  // while a MOTIS leg's routeId is feed-prefixed ("5_4"), and the board both
  // filters and renders by shortName. Comparing ids would duplicate the line.
  const sn = leg.routeShortName
  if (!sn || routes.some((r) => r.shortName === sn)) return routes
  return [
    {
      routeId: leg.routeId ?? sn,
      shortName: sn,
      longName: leg.routeLongName,
      color: leg.routeColor,
      textColor: leg.routeTextColor,
    },
    ...routes,
  ]
}

/**
 * Annotate each transit leg with the full set of interchangeable routes that
 * run its board→…→alight sequence directly. Mutates the legs in place;
 * memoised per call so repeated sequences across itineraries hit the DB once,
 * and resilient — any failure leaves the leg unenriched (old behaviour).
 */
async function enrichInterchangeableRoutes(itineraries: TransitItinerary[]): Promise<void> {
  const cache = new Map<string, Promise<InterchangeableRoute[]>>()
  const tasks: Promise<void>[] = []

  for (const it of itineraries) {
    for (const leg of it.legs) {
      if (!leg.transitLeg) continue
      const ids = [leg.from, ...(leg.intermediateStops ?? []), leg.to]
        .map((p) => p.stopId)
        .filter((s): s is string => !!s)
      if (ids.length < 2) continue
      const parsed = ids.map(splitMotisStopId)
      const feedId = parsed[0].feedId
      // A transit leg shouldn't span feeds; skip if its stops disagree.
      if (!feedId || parsed.some((p) => p.feedId !== feedId)) continue
      const rawIds = parsed.map((p) => p.stopId)
      const key = `${feedId}|${rawIds.join(',')}`
      let pending = cache.get(key)
      if (!pending) {
        pending = getRoutesForStopSequence(feedId, rawIds).catch(() => [])
        cache.set(key, pending)
      }
      tasks.push(
        pending.then((routes) => {
          const merged = withSelfRoute(routes, leg)
          if (merged.length >= 2) leg.interchangeableRoutes = merged
        }),
      )
    }
  }

  await Promise.all(tasks)
}

/**
 * The subsystems a MOTIS health response reports as unhealthy, or null when the
 * body is not a subsystem report at all.
 *
 * MOTIS answers `/api/v1/health` with a flag per subsystem — `{"rt":true,
 * "gbfs":false}` — and only 200 when every one of them is true. The shape is
 * checked rather than assumed: an all-boolean object is a report, anything
 * else is some other error that happens to have a body.
 */
async function motisSubsystemsDown(response: Response): Promise<string[] | null> {
  try {
    const body: unknown = await response.json()
    if (!body || typeof body !== 'object' || Array.isArray(body)) return null
    const flags = Object.entries(body as Record<string, unknown>)
    if (!flags.length || flags.some(([, value]) => typeof value !== 'boolean')) return null
    return flags.filter(([, value]) => value === false).map(([name]) => name)
  } catch {
    return null
  }
}

/**
 * Check if MOTIS is healthy and accepting requests.
 *
 * A subsystem report is not an outage. MOTIS returns 400 the moment any one of
 * its updaters is unhealthy, so an instance whose GBFS feeds failed to load
 * answers `{"rt":true,"gbfs":false}` with a 400 while serving stoptimes
 * queries perfectly well. Reading that as "MOTIS is down" took the whole
 * transit endpoint group off `/health` — the API reporting an outage it did
 * not have. A partial answer therefore counts as up, and names what is
 * degraded. Anything that is not a subsystem report — another status, an
 * unparseable body, no answer at all — is still unavailable.
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

    const down = await motisSubsystemsDown(response)
    if (down) {
      return down.length
        ? { status: 'ok', message: `degraded: ${down.join(', ')}` }
        : { status: 'ok' }
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
