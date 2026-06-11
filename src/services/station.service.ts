import { db } from '../db'
import { sql } from 'drizzle-orm'

export interface PlatformAccessPoint {
  osmId: string
  name: string | null
  description: string | null
  wheelchair: string | null
  level: string | null
  /** What kind of access point: subway_entrance, train_station_entrance, railway_crossing, highway_crossing, platform_edge */
  accessType: string
  lat: number
  lon: number
  distanceM: number
}

/** @deprecated Use PlatformAccessPoint */
export type StationEntrance = PlatformAccessPoint

export interface StationBuilding {
  osmId: string
  name: string | null
  stationType: string | null
  geometry: GeoJSON.Geometry
}

export interface StationDetail {
  stopId: string
  feedId: string
  stopName: string
  lat: number
  lon: number
  entrances: StationEntrance[]
  buildings: StationBuilding[]
}

/**
 * Get detailed station info including OSM-linked entrances and building geometry.
 */
export async function getStationDetail(
  feedId: string,
  stopId: string,
): Promise<StationDetail | null> {
  // Get the GTFS station
  const stationRows = await db.execute(sql.raw(`
    SELECT id, stop_id, feed_id, stop_name, stop_lat, stop_lon
    FROM gtfs_stops
    WHERE feed_id = '${feedId}' AND stop_id = '${stopId}' AND location_type = 1
    LIMIT 1
  `))

  const stations = stationRows as any[]
  if (stations.length === 0) return null
  const station = stations[0]

  // Get linked entrances
  const entrances = (await db.execute(sql.raw(`
    SELECT
      osm_entrance_id as osm_id,
      entrance_name as name,
      entrance_description as description,
      entrance_wheelchair as wheelchair,
      entrance_level as level,
      railway_type,
      ST_Y(entrance_geom) as lat,
      ST_X(entrance_geom) as lon,
      distance_m
    FROM station_entrances
    WHERE feed_id = '${feedId}' AND stop_id = '${stopId}'
    ORDER BY distance_m
  `))) as any[]

  // Get linked buildings
  const buildings = (await db.execute(sql.raw(`
    SELECT
      osm_building_id as osm_id,
      building_name as name,
      station_type,
      ST_AsGeoJSON(building_geom)::jsonb as geometry
    FROM station_buildings
    WHERE feed_id = '${feedId}' AND stop_id = '${stopId}'
  `))) as any[]

  return {
    stopId: station.stop_id,
    feedId: station.feed_id,
    stopName: station.stop_name,
    lat: station.stop_lat,
    lon: station.stop_lon,
    entrances: entrances.map((r: any) => ({
      osmId: r.osm_id,
      name: r.name || null,
      description: r.description || null,
      wheelchair: r.wheelchair || null,
      level: r.level || null,
      accessType: r.railway_type,
      lat: parseFloat(r.lat),
      lon: parseFloat(r.lon),
      distanceM: parseFloat(r.distance_m),
    })),
    buildings: buildings.map((r: any) => ({
      osmId: r.osm_id,
      name: r.name || null,
      stationType: r.station_type || null,
      geometry: r.geometry,
    })),
  }
}

/**
 * Find the best platform access point near a coordinate.
 *
 * Uses a tiered search strategy to handle all station types:
 *
 * Tier 1 — Explicit transit entrances (subway, commuter rail):
 *   railway=subway_entrance, railway=train_station_entrance
 *   These are purpose-mapped entrance nodes with names, wheelchair info, etc.
 *
 * Tier 2 — Track crossings (at-grade tram/light rail):
 *   railway=crossing nodes where pedestrians cross tracks to reach a platform.
 *   Only used if no Tier 1 entrance is found within the search radius.
 *
 * Tier 3 — Pedestrian crossings near platforms (any at-grade station):
 *   highway=crossing nodes within 100m of a public_transport=platform.
 *   Captures signalized crossings leading to median tram platforms.
 *
 * Each tier returns the access point nearest to the given coordinate.
 * The first tier with results wins — we don't mix tiers.
 */
export async function getNearestEntrance(
  lat: number,
  lon: number,
  maxDistanceM: number = 500,
): Promise<PlatformAccessPoint | null> {
  const degRadius = maxDistanceM / 111000
  const point = `ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)`

  // Single query with tiered UNION ALL — Postgres evaluates all branches
  // but we pick the best result by tier priority then distance.
  const rows = (await db.execute(sql.raw(`
    WITH candidates AS (
      -- Tier 1: Explicit transit entrances
      SELECT
        id as osm_id,
        name,
        COALESCE(tags->>'description', '') as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        COALESCE(tags->>'level', '') as level,
        COALESCE(tags->>'railway', 'entrance') as access_type,
        ST_Y(centroid) as lat,
        ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        1 as tier
      FROM geo_places
      WHERE (tags->>'railway' IN ('subway_entrance', 'train_station_entrance')
             OR (tags->>'entrance' IS NOT NULL AND tags->>'entrance' != 'no'
                 AND (tags->>'building' = 'train_station' OR tags->>'public_transport' = 'station')))
        AND centroid && ST_Expand(${point}, ${degRadius})

      UNION ALL

      -- Tier 2: Railway crossings (at-grade track crossings near platforms)
      SELECT
        id as osm_id,
        name,
        '' as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        '0' as level,
        'railway_crossing' as access_type,
        ST_Y(centroid) as lat,
        ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        2 as tier
      FROM geo_places
      WHERE tags->>'railway' = 'crossing'
        AND centroid && ST_Expand(${point}, ${degRadius})

      UNION ALL

      -- Tier 3: Pedestrian crossings near platforms
      SELECT
        id as osm_id,
        name,
        '' as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        '0' as level,
        'highway_crossing' as access_type,
        ST_Y(centroid) as lat,
        ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        3 as tier
      FROM geo_places
      WHERE tags->>'highway' = 'crossing'
        AND centroid && ST_Expand(${point}, ${degRadius * 0.5})
        -- Only include crossings that are near a platform
        AND EXISTS (
          SELECT 1 FROM geo_places p
          WHERE (p.tags->>'public_transport' = 'platform'
                 OR p.tags->>'railway' = 'platform')
            AND ST_DWithin(p.centroid, geo_places.centroid, 0.001)
        )
    )
    -- Pick the best tier that has results, then nearest within that tier
    SELECT * FROM candidates
    WHERE tier = (SELECT MIN(tier) FROM candidates)
    ORDER BY distance_m
    LIMIT 1
  `))) as any[]

  if (rows.length === 0) return null
  const r = rows[0]
  return {
    osmId: r.osm_id,
    name: r.name || null,
    description: r.description || null,
    wheelchair: r.wheelchair || null,
    level: r.level || null,
    accessType: r.access_type,
    lat: parseFloat(r.lat),
    lon: parseFloat(r.lon),
    distanceM: parseFloat(r.distance_m),
  }
}
