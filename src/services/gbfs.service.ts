/**
 * GBFS Shared Mobility Service
 *
 * Fetches and serves bikeshare/scootershare station data from GBFS
 * feeds. Uses on-demand fetch with LRU caching (same pattern as
 * vehicles.service.ts for GTFS-RT). Station info is cached for 1hr,
 * real-time availability for the feed's declared TTL (typically 30-300s).
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'

// ── Types ───────────────────────────────────────────────────────────

export interface GbfsSystem {
  systemId: string
  name: string | null
  operator: string | null
  url: string
  countryCode: string | null
  lat: number | null
  lon: number | null
  vehicleTypes: GbfsVehicleType[]
  hasStations: boolean
  hasFreeFloating: boolean
  feedUrls: Record<string, string>
  ttl: number
  enabled: boolean
}

export interface GbfsVehicleType {
  vehicleTypeId: string
  formFactor: string // bicycle, scooter, moped, car, other
  propulsionType: string // human, electric_assist, electric, combustion
  name?: string
}

export interface GbfsStation {
  systemId: string
  stationId: string
  name: string
  lat: number
  lon: number
  capacity: number | null
  numBikesAvailable: number
  numEbikesAvailable: number
  numScootersAvailable: number
  numDocksAvailable: number
  isRenting: boolean
  isReturning: boolean
  lastReported: string | null
  distance?: number // meters, populated by nearby queries
}

export interface GbfsFreeVehicle {
  systemId: string
  vehicleId: string
  type: 'free_floating'
  formFactor: string // bicycle, scooter, moped
  name: string // operator + vehicle type
  lat: number
  lon: number
  isReserved: boolean
  isDisabled: boolean
  batteryPercent: number | null
  distance?: number
}

export interface NearbyStationsRequest {
  lat: number
  lng: number
  radius?: number // meters, default 500
  vehicleType?: 'bike' | 'ebike' | 'scooter'
  limit?: number // default 5
}

export type NearbyResult = (GbfsStation & { type: 'station' }) | GbfsFreeVehicle

export interface NearbyStationsResponse {
  stations: NearbyResult[]
  systems: Array<{ systemId: string; name: string | null; operator: string | null }>
}

// ── Caches ──────────────────────────────────────────────────────────

/** Per-system station status (real-time availability). */
const stationStatusCache = new LRUCache<string, Map<string, StationStatusEntry>>({
  max: 200,
  ttl: 60_000, // default 60s, overridden per-system at set() time
})

interface StationStatusEntry {
  numBikesAvailable: number
  numEbikesAvailable: number
  numScootersAvailable: number
  numDocksAvailable: number
  isRenting: boolean
  isReturning: boolean
  lastReported: string | null
}

/** Per-system free-floating vehicle positions (in-memory only). */
const vehicleCache = new LRUCache<string, GbfsFreeVehicle[]>({
  max: 200,
  ttl: 30_000, // 30s default, overridden per-system
})

/** System metadata (discovery URLs, TTL). Rarely changes. */
const systemCache = new LRUCache<string, GbfsSystem>({
  max: 200,
  ttl: 3_600_000, // 1 hour
})

// ── Main query ──────────────────────────────────────────────────────

/**
 * Find nearby shared-mobility stations with real-time availability.
 *
 * 1. Spatial query on gbfs_stations for candidates within radius
 * 2. For each system hit, fetch/cache station_status for live counts
 * 3. Merge availability into station records
 * 4. Filter by vehicle type, sort by distance, apply limit
 */
export async function getNearbyStations(
  request: NearbyStationsRequest,
): Promise<NearbyStationsResponse> {
  const { lat, lng, radius = 500, vehicleType, limit = 5 } = request

  // Approximate degree offsets for the bounding box
  const dLat = radius / 111320
  const dLng = radius / (111320 * Math.cos((lat * Math.PI) / 180))

  // Spatial query: find stations within bounding box, compute distance
  const rows = await db.execute(sql.raw(`
    SELECT
      s.system_id, s.station_id, s.name, s.lat, s.lon, s.capacity,
      s.num_bikes_available, s.num_ebikes_available,
      s.num_scooters_available, s.num_docks_available,
      s.is_renting, s.is_returning, s.last_reported,
      (
        6371000 * acos(
          LEAST(1, GREATEST(-1,
            cos(radians(${lat})) * cos(radians(s.lat)) *
            cos(radians(s.lon) - radians(${lng})) +
            sin(radians(${lat})) * sin(radians(s.lat))
          ))
        )
      ) AS distance
    FROM gbfs_stations s
    JOIN gbfs_systems sys ON s.system_id = sys.system_id
    WHERE sys.enabled = TRUE
      AND s.lat BETWEEN ${lat - dLat} AND ${lat + dLat}
      AND s.lon BETWEEN ${lng - dLng} AND ${lng + dLng}
      AND s.is_renting = TRUE
    ORDER BY distance
    LIMIT ${limit * 3}
  `)) as any[]

  // ── Station-based results ──────────────────────────────────────────

  const results: NearbyResult[] = []
  const allSystemIds = new Set<string>()

  if (rows.length > 0) {
    const stationSystemIds = [...new Set(rows.map(r => r.system_id))]
    stationSystemIds.forEach(id => allSystemIds.add(id))
    await Promise.allSettled(
      stationSystemIds.map(id => refreshStationStatus(id)),
    )

    for (const row of rows) {
      const statusMap = stationStatusCache.get(row.system_id)
      const status = statusMap?.get(row.station_id)

      const station: GbfsStation & { type: 'station' } = {
        type: 'station',
        systemId: row.system_id,
        stationId: row.station_id,
        name: row.name || '',
        lat: row.lat,
        lon: row.lon,
        capacity: row.capacity,
        numBikesAvailable: status?.numBikesAvailable ?? row.num_bikes_available ?? 0,
        numEbikesAvailable: status?.numEbikesAvailable ?? row.num_ebikes_available ?? 0,
        numScootersAvailable: status?.numScootersAvailable ?? row.num_scooters_available ?? 0,
        numDocksAvailable: status?.numDocksAvailable ?? row.num_docks_available ?? 0,
        isRenting: status?.isRenting ?? row.is_renting ?? true,
        isReturning: status?.isReturning ?? row.is_returning ?? true,
        lastReported: status?.lastReported ?? row.last_reported,
        distance: Math.round(row.distance),
      }

      if (vehicleType === 'bike' && station.numBikesAvailable + station.numEbikesAvailable <= 0) continue
      if (vehicleType === 'ebike' && station.numEbikesAvailable <= 0) continue
      if (vehicleType === 'scooter' && station.numScootersAvailable <= 0) continue
      if (station.distance! > radius) continue

      results.push(station)
    }
  }

  // ── Free-floating vehicles ────────────────────────────────────────

  const freeFloatingSystems = await db.execute(sql.raw(`
    SELECT system_id FROM gbfs_systems
    WHERE enabled = TRUE AND has_free_floating = TRUE
      AND lat IS NOT NULL
      AND lat BETWEEN ${lat - 1} AND ${lat + 1}
      AND lon BETWEEN ${lng - 1} AND ${lng + 1}
  `)) as any[]

  if (freeFloatingSystems.length > 0) {
    const ffSystemIds = freeFloatingSystems.map((r: any) => r.system_id as string)
    ffSystemIds.forEach(id => allSystemIds.add(id))

    await Promise.allSettled(
      ffSystemIds.map(id => refreshVehicleStatus(id)),
    )

    for (const sysId of ffSystemIds) {
      const vehicles = vehicleCache.get(sysId)
      if (!vehicles) continue

      for (const v of vehicles) {
        if (v.isReserved || v.isDisabled) continue

        // Vehicle type filter
        if (vehicleType === 'bike' && v.formFactor !== 'bicycle') continue
        if (vehicleType === 'ebike' && v.formFactor !== 'bicycle') continue
        if (vehicleType === 'scooter' && !v.formFactor.includes('scooter')) continue

        // Haversine distance
        const dist = haversineM(lat, lng, v.lat, v.lon)
        if (dist > radius) continue

        results.push({ ...v, distance: Math.round(dist) })
      }
    }
  }

  // Sort all results (stations + vehicles) by distance, apply limit
  results.sort((a, b) => (a.distance ?? Infinity) - (b.distance ?? Infinity))
  const limited = results.slice(0, limit)

  const systemMeta = await getSystemsMeta([...allSystemIds])
  return { stations: limited, systems: systemMeta }
}

/**
 * List GBFS systems within a bounding box.
 */
export async function getSystemsInBounds(
  north: number, south: number, east: number, west: number,
): Promise<GbfsSystem[]> {
  const rows = await db.execute(sql.raw(`
    SELECT system_id, name, operator, url, country_code, lat, lon,
           vehicle_types, has_stations, has_free_floating, feed_urls, ttl, enabled
    FROM gbfs_systems
    WHERE enabled = TRUE
      AND lat BETWEEN ${south} AND ${north}
      AND lon BETWEEN ${west} AND ${east}
    ORDER BY name
    LIMIT 100
  `)) as any[]

  return rows.map(rowToSystem)
}

// ── Station status polling ──────────────────────────────────────────

/**
 * Fetch and cache station_status.json for a system.
 * Uses the system's declared TTL for cache duration.
 */
async function refreshStationStatus(systemId: string): Promise<void> {
  // Already cached?
  if (stationStatusCache.has(systemId)) return

  const system = await getSystem(systemId)
  if (!system?.feedUrls?.station_status) return

  try {
    const response = await fetch(system.feedUrls.station_status, {
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return

    const data = await response.json() as any
    const stationsData = data?.data?.stations
    if (!Array.isArray(stationsData)) return

    const statusMap = new Map<string, StationStatusEntry>()
    for (const s of stationsData) {
      const bikesAvail = s.num_bikes_available ?? 0
      const ebikesAvail = s.vehicle_types_available
        ?.find((vt: any) => vt.vehicle_type_id?.includes('electric'))
        ?.count ?? 0
      const scootersAvail = s.vehicle_types_available
        ?.find((vt: any) => vt.vehicle_type_id?.includes('scooter'))
        ?.count ?? 0

      statusMap.set(s.station_id, {
        numBikesAvailable: bikesAvail - ebikesAvail, // non-electric bikes
        numEbikesAvailable: ebikesAvail,
        numScootersAvailable: scootersAvail,
        numDocksAvailable: s.num_docks_available ?? 0,
        isRenting: s.is_renting !== false,
        isReturning: s.is_returning !== false,
        lastReported: s.last_reported
          ? new Date(s.last_reported * 1000).toISOString()
          : null,
      })
    }

    // Cache with system-specific TTL
    const ttlMs = (system.ttl || 60) * 1000
    stationStatusCache.set(systemId, statusMap, { ttl: ttlMs })
  } catch (err) {
    console.warn(`[GBFS] Failed to fetch station_status for ${systemId}:`,
      err instanceof Error ? err.message : err)
  }
}

// ── Free-floating vehicle polling ────────────────────────────────────

/**
 * Fetch and cache vehicle_status.json (v3) or free_bike_status.json (v2)
 * for a dockless system. Vehicles are kept in-memory only.
 */
async function refreshVehicleStatus(systemId: string): Promise<void> {
  if (vehicleCache.has(systemId)) return

  const system = await getSystem(systemId)
  if (!system) return

  const feedUrl = system.feedUrls.vehicle_status ?? system.feedUrls.free_bike_status
  if (!feedUrl) return

  try {
    const response = await fetch(feedUrl, {
      signal: AbortSignal.timeout(8000),
    })
    if (!response.ok) return

    const data = await response.json() as any
    // v3: data.vehicles, v2: data.bikes
    const rawVehicles = data?.data?.vehicles ?? data?.data?.bikes ?? []

    // Build a vehicle_type_id → form_factor map from the system's cached types
    const typeMap = new Map<string, string>()
    for (const vt of system.vehicleTypes) {
      typeMap.set(vt.vehicleTypeId, vt.formFactor || 'bicycle')
    }

    const systemName = system.name || system.operator || systemId

    const vehicles: GbfsFreeVehicle[] = []
    for (const v of rawVehicles) {
      if (v.is_reserved || v.is_disabled) continue
      const vLat = v.lat ?? v.latitude
      const vLon = v.lon ?? v.longitude
      if (!vLat || !vLon) continue

      const formFactor = typeMap.get(v.vehicle_type_id) || 'bicycle'
      const label = formFactor.includes('scooter') ? 'Scooter' : 'Bike'

      vehicles.push({
        systemId,
        vehicleId: v.vehicle_id || v.bike_id || '',
        type: 'free_floating',
        formFactor,
        name: `${systemName} ${label}`,
        lat: vLat,
        lon: vLon,
        isReserved: false,
        isDisabled: false,
        batteryPercent: v.current_fuel_percent != null
          ? Math.round(v.current_fuel_percent * 100)
          : null,
      })
    }

    const ttlMs = Math.max((system.ttl || 30) * 1000, 10_000)
    vehicleCache.set(systemId, vehicles, { ttl: ttlMs })
  } catch (err) {
    console.warn(`[GBFS] Failed to fetch vehicle_status for ${systemId}:`,
      err instanceof Error ? err.message : err)
  }
}

// ── Helpers ─────────────────────────────────────────────────────────

/** Haversine distance in meters between two lat/lng points. */
function haversineM(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371000
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLon = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

async function getSystem(systemId: string): Promise<GbfsSystem | null> {
  const cached = systemCache.get(systemId)
  if (cached) return cached

  const rows = await db.execute(sql.raw(`
    SELECT system_id, name, operator, url, country_code, lat, lon,
           vehicle_types, has_stations, has_free_floating, feed_urls, ttl, enabled
    FROM gbfs_systems
    WHERE system_id = '${systemId.replace(/'/g, "''")}'
    LIMIT 1
  `)) as any[]

  if (rows.length === 0) return null
  const system = rowToSystem(rows[0])
  systemCache.set(systemId, system)
  return system
}

async function getSystemsMeta(
  systemIds: string[],
): Promise<Array<{ systemId: string; name: string | null; operator: string | null }>> {
  if (systemIds.length === 0) return []

  const inList = systemIds.map(id => `'${id.replace(/'/g, "''")}'`).join(',')
  const rows = await db.execute(sql.raw(`
    SELECT system_id, name, operator FROM gbfs_systems
    WHERE system_id IN (${inList})
  `)) as any[]

  return rows.map(r => ({
    systemId: r.system_id,
    name: r.name,
    operator: r.operator,
  }))
}

function rowToSystem(row: any): GbfsSystem {
  return {
    systemId: row.system_id,
    name: row.name,
    operator: row.operator,
    url: row.url,
    countryCode: row.country_code,
    lat: row.lat,
    lon: row.lon,
    vehicleTypes: typeof row.vehicle_types === 'string'
      ? JSON.parse(row.vehicle_types)
      : (row.vehicle_types ?? []),
    hasStations: row.has_stations ?? true,
    hasFreeFloating: row.has_free_floating ?? false,
    feedUrls: typeof row.feed_urls === 'string'
      ? JSON.parse(row.feed_urls)
      : (row.feed_urls ?? {}),
    ttl: row.ttl ?? 300,
    enabled: row.enabled ?? true,
  }
}
