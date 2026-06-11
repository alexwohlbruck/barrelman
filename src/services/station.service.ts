import { db } from '../db'
import { sql } from 'drizzle-orm'

export interface StationEntrance {
  osmId: string
  name: string | null
  description: string | null
  wheelchair: string | null
  level: string | null
  railwayType: string
  lat: number
  lon: number
  distanceM: number
}

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
      railwayType: r.railway_type,
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
 * Get entrances near a coordinate (for routing — find the nearest entrance
 * to use as a walk target instead of the station centroid).
 */
export async function getNearestEntrance(
  lat: number,
  lon: number,
  maxDistanceM: number = 500,
): Promise<StationEntrance | null> {
  const degRadius = maxDistanceM / 111000

  // Query geo_places directly to use the spatial index on centroid
  const rows = (await db.execute(sql.raw(`
    SELECT
      id as osm_id,
      name,
      COALESCE(tags->>'description', '') as description,
      COALESCE(tags->>'wheelchair', '') as wheelchair,
      COALESCE(tags->>'level', '') as level,
      tags->>'railway' as "railwayType",
      ST_Y(centroid) as lat,
      ST_X(centroid) as lon,
      ST_Distance(
        centroid::geography,
        ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)::geography
      ) as distance_m
    FROM geo_places
    WHERE tags->>'railway' IN ('subway_entrance', 'train_station_entrance')
      AND centroid && ST_Expand(ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326), ${degRadius})
    ORDER BY centroid <-> ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)
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
    railwayType: r.railwayType,
    lat: parseFloat(r.lat),
    lon: parseFloat(r.lon),
    distanceM: parseFloat(r.distance_m),
  }
}
