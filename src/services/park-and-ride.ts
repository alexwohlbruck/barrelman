// Park-and-ride access: drive on GraphHopper to a rail or ferry station,
// preferring one with a park_ride lot, then ride MOTIS station to station.
// MOTIS's own car_parking routing is not used: its street graph is clipped to
// bikeshare areas, and its path reconstruction fails on short footways at
// station entrances (`no connecting way … found 1 1`).

import { sql } from 'drizzle-orm'
import { db } from '../db'
import { graphHopperUrl } from '../lib/graphhopper'
import { haversine } from './route.service'
import type { FetchFn } from './transit.service'

export interface LatLng {
  lat: number
  lng: number
}

export interface ParkingLot extends LatLng {
  name: string
}

export interface ParkAndRideStation extends LatLng {
  feedId: string
  stationId: string
  /** Metres from the trip's origin, straight line. */
  distance: number
  /** The park_ride lot beside it, when there is one. */
  lot?: ParkingLot
}

export interface Drive {
  duration: number
  distance: number
  coordinates: [number, number][]
}

/** Stations driven to per trip; each is paired with every egress station. */
export const PARK_AND_RIDE_STATIONS = 4

/** Of those, how many may be taken by a station with a park_ride lot. */
const PARK_RIDE_SLOTS = 2

/** Nearer than this, the rider walks; driving to the station is not a trip. */
const MIN_DRIVE_M = 1200

/** Straight-line reach of a drive, per second of the access budget. */
const DRIVE_REACH_MPS = 10

const MAX_DRIVE_M = 25_000

/** The drive may cover at most this share of the trip, as the crow flies. */
const MAX_DRIVE_SHARE = 0.6

/** Origin → station → destination may be at most this much longer than direct. */
const MAX_DETOUR = 1.4

/** How far a lot may sit from its station's platforms. */
const LOT_TO_STATION_M = 400

/** Parking, locking up and leaving the lot. */
export const PARK_SEC = 300

/**
 * Route types that count as rail or ferry, by GTFS basic and extended types:
 * tram, metro, rail and monorail; ferry and other water transport. Coach
 * (200-299), air (1100) and the cable modes are left out.
 */
const RAIL_OR_FERRY_ROUTE = sql`(
  r.route_type IN (0, 1, 2, 4, 12)
  OR r.route_type BETWEEN 100 AND 199
  OR r.route_type BETWEEN 400 AND 499
  OR r.route_type BETWEEN 900 AND 999
  OR r.route_type BETWEEN 1000 AND 1099
  OR r.route_type BETWEEN 1200 AND 1299
)`

/** MOTIS leg modes a park-and-ride rider may board first. */
const RAIL_OR_FERRY_MODES = new Set([
  'TRAM', 'SUBWAY', 'METRO', 'FERRY', 'RAIL', 'HIGHSPEED_RAIL', 'LONG_DISTANCE',
  'NIGHT_RAIL', 'REGIONAL_FAST_RAIL', 'REGIONAL_RAIL', 'SUBURBAN',
])

export function isRailOrFerryMode(mode: string): boolean {
  return RAIL_OR_FERRY_MODES.has(mode)
}

/** The straight-line distance band a park-and-ride station must sit in. */
export function driveBand(from: LatLng, to: LatLng, maxDriveSec: number): { min: number; max: number; trip: number } {
  const trip = haversine(from.lat, from.lng, to.lat, to.lng)
  const max = Math.min(maxDriveSec * DRIVE_REACH_MPS, MAX_DRIVE_M, trip * MAX_DRIVE_SHARE)
  return { min: MIN_DRIVE_M, max, trip }
}

/**
 * The stations to drive to: up to two with a park_ride lot, nearest first,
 * then the nearest rail and ferry stations of any kind. A lot is a
 * preference, not a requirement: most city stations have none tagged, and a
 * park_ride lot far down the line is not always the better trip.
 */
export function pickParkAndRideStations(
  parkRide: ParkAndRideStation[],
  nearest: ParkAndRideStation[],
  limit = PARK_AND_RIDE_STATIONS,
): ParkAndRideStation[] {
  const byDistance = (a: ParkAndRideStation, b: ParkAndRideStation) => a.distance - b.distance
  const picked = new Map<string, ParkAndRideStation>()
  const take = (s: ParkAndRideStation) => {
    const key = `${s.feedId}:${s.stationId}`
    if (!picked.has(key) && picked.size < limit) picked.set(key, s)
  }
  ;[...parkRide].sort(byDistance).slice(0, PARK_RIDE_SLOTS).forEach(take)
  ;[...nearest].sort(byDistance).forEach(take)
  return [...picked.values()]
}

const point = (p: LatLng) => sql`ST_SetSRID(ST_MakePoint(${p.lng}, ${p.lat}), 4326)`

/** Ahead of the destination, inside the drive band, and not a detour. */
function onTheWay(from: LatLng, to: LatLng, band: ReturnType<typeof driveBand>) {
  return sql`
    ST_Distance(st.geom::geography, ${point(to)}::geography) < ${band.trip}
    AND ST_Distance(st.geom::geography, ${point(from)}::geography)
      + ST_Distance(st.geom::geography, ${point(to)}::geography) <= ${band.trip * MAX_DETOUR}
  `
}

const servesRailOrFerry = sql`EXISTS (
  SELECT 1 FROM gtfs_stop_routes sr
  JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
  WHERE sr.feed_id = st.feed_id AND sr.stop_id = st.stop_id AND ${RAIL_OR_FERRY_ROUTE}
)`

function toStation(r: any): ParkAndRideStation {
  return {
    feedId: r.feed_id,
    stationId: r.station_id,
    lat: r.stop_lat,
    lng: r.stop_lon,
    distance: Number(r.distance),
    ...(r.lot_lat != null && {
      lot: { name: r.lot_name || 'Park & Ride', lat: r.lot_lat, lng: r.lot_lon },
    }),
  }
}

/** The nearest rail and ferry stations in the drive band. */
async function nearestStations(from: LatLng, to: LatLng, band: ReturnType<typeof driveBand>) {
  const rows = (await db.execute(sql`
    SELECT * FROM (
      SELECT DISTINCT ON (st.feed_id, COALESCE(NULLIF(st.parent_station, ''), st.stop_id))
        st.feed_id,
        COALESCE(NULLIF(st.parent_station, ''), st.stop_id) AS station_id,
        st.stop_lat, st.stop_lon,
        ST_Distance(st.geom::geography, ${point(from)}::geography) AS distance
      FROM gtfs_stops st
      WHERE ST_DWithin(st.geom::geography, ${point(from)}::geography, ${band.max})
        AND NOT ST_DWithin(st.geom::geography, ${point(from)}::geography, ${band.min})
        AND (st.location_type = 0 OR st.location_type IS NULL)
        AND ${onTheWay(from, to, band)}
        AND ${servesRailOrFerry}
      ORDER BY st.feed_id, COALESCE(NULLIF(st.parent_station, ''), st.stop_id), distance
    ) s
    ORDER BY distance
    LIMIT ${PARK_AND_RIDE_STATIONS}
  `)) as any[]
  return rows.map(toStation)
}

/**
 * Rail and ferry stations in the drive band with a park_ride lot beside
 * them. Lots are OSM `amenity=parking` areas with any `park_ride` value but
 * `no`, and not private; the car is routed to the lot's corner nearest the
 * station.
 */
async function parkRideStations(from: LatLng, to: LatLng, band: ReturnType<typeof driveBand>) {
  const bboxDeg = band.max / (111_320 * Math.cos((from.lat * Math.PI) / 180))
  const rows = (await db.execute(sql`
    SELECT * FROM (
      SELECT DISTINCT ON (st.feed_id, st.station_id)
        st.feed_id, st.station_id, st.stop_lat, st.stop_lon, st.distance,
        lot.name AS lot_name,
        ST_Y(ST_ClosestPoint(lot.geom, st.geom)) AS lot_lat,
        ST_X(ST_ClosestPoint(lot.geom, st.geom)) AS lot_lon
      FROM geo_places lot
      CROSS JOIN LATERAL (
        SELECT
          st.feed_id,
          COALESCE(NULLIF(st.parent_station, ''), st.stop_id) AS station_id,
          st.stop_lat, st.stop_lon, st.geom,
          ST_Distance(st.geom::geography, ${point(from)}::geography) AS distance
        FROM gtfs_stops st
        WHERE st.geom && ST_Expand(lot.geom, 0.01)
          AND ST_DWithin(st.geom::geography, lot.geom::geography, ${LOT_TO_STATION_M})
          AND (st.location_type = 0 OR st.location_type IS NULL)
          AND ${servesRailOrFerry}
        ORDER BY ST_Distance(st.geom::geography, lot.geom::geography)
        LIMIT 1
      ) st
      WHERE lot.geom_type = 'area'
        AND lot.tags->>'amenity' = 'parking'
        AND COALESCE(lot.tags->>'park_ride', 'no') <> 'no'
        AND COALESCE(lot.tags->>'access', 'yes') NOT IN ('private', 'no', 'permit', 'customers', 'delivery')
        AND lot.geom && ST_Expand(${point(from)}, ${bboxDeg})
        AND ST_DWithin(lot.geom::geography, ${point(from)}::geography, ${band.max})
        AND NOT ST_DWithin(st.geom::geography, ${point(from)}::geography, ${band.min})
        AND ${onTheWay(from, to, band)}
      ORDER BY st.feed_id, st.station_id, ST_Distance(lot.geom::geography, st.geom::geography)
    ) s
    ORDER BY distance
    LIMIT ${PARK_RIDE_SLOTS}
  `)) as any[]
  return rows.map(toStation)
}

/** Rail and ferry stations worth driving to on the way from `from` to `to`. */
export async function parkAndRideStations(from: LatLng, to: LatLng, maxDriveSec: number): Promise<ParkAndRideStation[]> {
  const band = driveBand(from, to, maxDriveSec)
  if (band.max <= band.min) return []
  const [parkRide, nearest] = await Promise.all([
    parkRideStations(from, to, band),
    nearestStations(from, to, band),
  ])
  return pickParkAndRideStations(parkRide, nearest)
}

/** A car route on GraphHopper, or null when it has none. */
export async function routeDrive(from: LatLng, to: LatLng, fetchFn: FetchFn): Promise<Drive | null> {
  try {
    const response = await fetchFn(`${graphHopperUrl()}/route`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        profile: 'car',
        points: [[from.lng, from.lat], [to.lng, to.lat]],
        points_encoded: false,
        instructions: false,
      }),
      signal: AbortSignal.timeout(8_000),
    })
    if (!response.ok) return null
    const path = ((await response.json()) as any)?.paths?.[0]
    const coordinates = path?.points?.coordinates
    if (!path || !Array.isArray(coordinates) || coordinates.length < 2) return null
    return {
      duration: Math.max(1, Math.round(path.time / 1000)),
      distance: Math.round(path.distance),
      coordinates: coordinates.map((c: number[]) => [c[0], c[1]] as [number, number]),
    }
  } catch {
    return null
  }
}
