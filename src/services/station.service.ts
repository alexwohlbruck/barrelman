import { db } from '../db'
import { sql } from 'drizzle-orm'
import { getRoutesForStop, type StopRoutesResult } from './transit.service'

// Whether the stop_area_members table exists (loaded by
// scripts/import-stop-areas.sh). Checked once and cached for the process.
let stopAreasAvailable: boolean | null = null

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

/**
 * A nearby station the rider can transfer to on foot. Now that same-name,
 * close-but-distinct parent_stations render as SEPARATE map markers (Jackson
 * Blue vs Jackson Red), the detail page cross-references them here — the
 * Apple-Maps "Connections" list. Sourced from GTFS transfers.txt links and
 * from parent_station proximity (~200 m parent-to-parent).
 */
export interface StationConnection {
  feedId: string
  stopId: string
  name: string
  distanceM: number
  /** Lines serving the connected station (its route-bullet row). */
  routes: StopRoutesResult[]
}

export interface StationDetail {
  stopId: string
  feedId: string
  stopName: string
  lat: number
  lon: number
  entrances: StationEntrance[]
  buildings: StationBuilding[]
  /** Lines serving the station, aggregated across its transfer complex. */
  routes: StopRoutesResult[]
  /** Nearby parent_stations reachable on foot (walk-transfer / transfers.txt). */
  connections: StationConnection[]
}

/** Max parent-to-parent walking distance for a proximity connection (metres). */
const CONNECTION_RADIUS_M = 200

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

  // Connections: other parent_stations reachable on foot. Union of GTFS
  // transfers.txt links (agency-declared) and parent_station proximity
  // (~200 m parent-to-parent). Each row is a distinct connected parent; we
  // keep the shortest distance when a station shows up via both sources.
  const feedEsc = feedId.replace(/'/g, "''")
  const stopEsc = stopId.replace(/'/g, "''")
  const connectionRows = (await db.execute(sql.raw(`
    WITH me AS (
      SELECT stop_id, geom, parent_station
      FROM gtfs_stops
      WHERE feed_id = '${feedEsc}' AND stop_id = '${stopEsc}' AND location_type = 1
      LIMIT 1
    ),
    -- transfers.txt: to/from links from this parent (or its child platforms)
    -- resolved up to the connected station's parent.
    seed AS (
      SELECT stop_id AS sid FROM me
      UNION
      SELECT stop_id FROM gtfs_stops
      WHERE feed_id = '${feedEsc}' AND parent_station = '${stopEsc}'
    ),
    transfer_targets AS (
      SELECT t.to_stop_id AS sid FROM gtfs_transfers t JOIN seed ON t.from_stop_id = seed.sid
      WHERE t.feed_id = '${feedEsc}' AND t.to_stop_id <> t.from_stop_id
      UNION
      SELECT t.from_stop_id AS sid FROM gtfs_transfers t JOIN seed ON t.to_stop_id = seed.sid
      WHERE t.feed_id = '${feedEsc}' AND t.to_stop_id <> t.from_stop_id
    ),
    transfer_parents AS (
      SELECT DISTINCT COALESCE(NULLIF(g.parent_station, ''), g.stop_id) AS parent_id
      FROM transfer_targets tt
      JOIN gtfs_stops g ON g.feed_id = '${feedEsc}' AND g.stop_id = tt.sid
    ),
    candidates AS (
      -- Proximity: other parent stations within CONNECTION_RADIUS_M.
      SELECT s.stop_id AS parent_id, s.stop_name,
             ST_Distance(s.geom::geography, me.geom::geography) AS distance_m
      FROM gtfs_stops s, me
      WHERE s.feed_id = '${feedEsc}' AND s.location_type = 1
        AND s.stop_id <> '${stopEsc}'
        AND ST_DWithin(s.geom::geography, me.geom::geography, ${CONNECTION_RADIUS_M})
      UNION
      -- transfers.txt parents (any distance — the agency declared the link).
      SELECT s.stop_id AS parent_id, s.stop_name,
             ST_Distance(s.geom::geography, me.geom::geography) AS distance_m
      FROM transfer_parents tp
      JOIN gtfs_stops s ON s.feed_id = '${feedEsc}' AND s.stop_id = tp.parent_id
                       AND s.location_type = 1
      CROSS JOIN me
      WHERE s.stop_id <> '${stopEsc}'
    )
    SELECT parent_id AS stop_id, stop_name, min(distance_m) AS distance_m
    FROM candidates
    GROUP BY parent_id, stop_name
    ORDER BY min(distance_m)
  `))) as any[]

  // Attach each connection's route bullets. Bounded (typically 0–3 rows).
  const connections: StationConnection[] = await Promise.all(
    connectionRows.map(async (r: any) => ({
      feedId,
      stopId: r.stop_id,
      name: r.stop_name,
      distanceM: Math.round(parseFloat(r.distance_m)),
      routes: await getRoutesForStop(feedId, r.stop_id),
    })),
  )

  return {
    stopId: station.stop_id,
    feedId: station.feed_id,
    stopName: station.stop_name,
    lat: station.stop_lat,
    lon: station.stop_lon,
    connections,
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
    routes: await getRoutesForStop(feedId, stopId),
  }
}

/**
 * Find the best platform access point near a coordinate.
 *
 * Uses a tiered search strategy to handle all station types:
 *
 * Tier 1 — Explicit transit entrances (subway, commuter rail):
 *   railway=subway_entrance, railway=train_station_entrance
 *   Purpose-mapped entrance nodes with names, wheelchair info, level.
 *
 * Tier 2 — Generic entrances near transit platforms:
 *   entrance=yes/main/secondary nodes within 100m of a platform.
 *   Catches station building doors that aren't tagged as rail entrances.
 *
 * Tier 3 — Vertical access near platforms (elevated/underground):
 *   highway=steps or highway=elevator within 150m of a platform.
 *   The physical stairs/elevator connecting street level to platform level.
 *   Uses the street-level end (centroid) of stairways as the access point.
 *
 * Tier 4 — Track crossings (at-grade tram/light rail):
 *   railway=crossing nodes. Pedestrian crossings across tracks to a platform.
 *
 * Tier 5 — Pedestrian crossings near platforms (fallback at-grade):
 *   highway=crossing nodes within 80m of a platform.
 *   Signalized crossings leading to median tram platforms.
 *
 * The first tier with results wins. Within a tier, nearest to coordinate wins.
 */
export async function getNearestEntrance(
  lat: number,
  lon: number,
  maxDistanceM: number = 500,
  wheelchair: boolean = false,
): Promise<PlatformAccessPoint | null> {
  const degRadius = maxDistanceM / 111000
  // Accessible mode: drop anything explicitly wheelchair=no (unknown is
  // allowed — most entrances are untagged), require elevators rather than
  // stairs for vertical access, and prefer confirmed wheelchair=yes.
  const accessFilter = wheelchair
    ? "AND COALESCE(tags->>'wheelchair', '') <> 'no'"
    : ''
  const point = `ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)`
  // Tighter radius for proximity-to-platform checks (~ 100m in degrees)
  const platformProximity = 0.001

  // First, check if there are any platforms in the search area.
  // This lets us skip the expensive EXISTS subqueries for tiers 2/3/5
  // when no platform is nearby (most places on earth).
  const platformCheck = (await db.execute(sql.raw(`
    SELECT 1 FROM geo_places
    WHERE (tags->>'public_transport' = 'platform' OR tags->>'railway' = 'platform')
      AND centroid && ST_Expand(${point}, ${degRadius})
    LIMIT 1
  `))) as any[]
  const hasPlatformNearby = platformCheck.length > 0

  // stop_area relations are loaded by scripts/import-stop-areas.sh —
  // skip Tier 0 gracefully on databases that haven't run it yet.
  if (stopAreasAvailable === null) {
    const reg = (await db.execute(
      sql.raw(`SELECT to_regclass('stop_area_members') IS NOT NULL AS ok`),
    )) as any[]
    stopAreasAvailable = reg[0]?.ok === true
  }

  const accessFilterE = wheelchair
    ? "AND COALESCE(e.tags->>'wheelchair', '') <> 'no'"
    : ''

  const rows = (await db.execute(sql.raw(`
    WITH candidates AS (
      ${stopAreasAvailable ? `
      -- Tier 0: Relation-linked entrances — the mapper's authoritative
      -- public_transport=stop_area grouping. An entrance (or elevator) that
      -- shares a stop_area with the platform/stop near the query point wins
      -- over any purely geometric candidate; proximity is the fallback.
      SELECT
        e.id as osm_id, e.name,
        COALESCE(e.tags->>'description', '') as description,
        COALESCE(e.tags->>'wheelchair', '') as wheelchair,
        COALESCE(e.tags->>'level', '') as level,
        COALESCE(e.tags->>'railway',
          CASE WHEN e.tags->>'highway' = 'elevator' THEN 'elevator' ELSE 'entrance' END
        ) as access_type,
        ST_Y(e.centroid) as lat, ST_X(e.centroid) as lon,
        ST_Distance(e.centroid::geography, ${point}::geography) as distance_m,
        0 as tier
      FROM geo_places near_member
      JOIN stop_area_members mp
        ON mp.member_type = near_member.osm_type AND mp.member_ref = near_member.osm_id
      JOIN stop_area_members me ON me.relation_id = mp.relation_id
      JOIN geo_places e
        ON e.osm_type = me.member_type AND e.osm_id = me.member_ref
      WHERE near_member.centroid && ST_Expand(${point}, ${platformProximity})
        AND (
          near_member.tags->>'public_transport' IN ('platform', 'stop_position', 'station')
          OR near_member.tags->>'railway' IN ('platform', 'station', 'halt', 'stop')
        )
        AND (
          e.tags->>'railway' IN ('subway_entrance', 'train_station_entrance')
          OR e.tags->>'entrance' IS NOT NULL
          OR e.tags->>'highway' = 'elevator'
          OR me.member_role = 'entrance'
        )
        AND COALESCE(e.tags->>'entrance', '') NOT IN ('no', 'service', 'emergency')
        ${wheelchair ? "AND e.tags->>'highway' IS DISTINCT FROM 'steps'" : ''}
        ${accessFilterE}
        AND e.centroid && ST_Expand(${point}, ${degRadius})

      UNION ALL
      ` : ''}

      -- Tier 1: Explicit transit entrances (subway, train station)
      SELECT
        id as osm_id, name,
        COALESCE(tags->>'description', '') as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        COALESCE(tags->>'level', '') as level,
        COALESCE(tags->>'railway', 'entrance') as access_type,
        ST_Y(centroid) as lat, ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        1 as tier
      FROM geo_places
      WHERE tags->>'railway' IN ('subway_entrance', 'train_station_entrance')
        ${accessFilter}
        AND centroid && ST_Expand(${point}, ${degRadius})

      ${hasPlatformNearby ? `
      UNION ALL

      -- Tier 2: Generic entrance nodes near a transit platform
      SELECT
        id as osm_id, name,
        '' as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        COALESCE(tags->>'level', '0') as level,
        'entrance' as access_type,
        ST_Y(centroid) as lat, ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        2 as tier
      FROM geo_places
      WHERE tags->>'entrance' IS NOT NULL
        ${accessFilter}
        AND tags->>'entrance' NOT IN ('no', 'service', 'emergency')
        AND centroid && ST_Expand(${point}, ${degRadius})
        AND EXISTS (
          SELECT 1 FROM geo_places p
          WHERE (p.tags->>'public_transport' = 'platform' OR p.tags->>'railway' = 'platform')
            AND p.centroid && ST_Expand(geo_places.centroid, ${platformProximity})
        )

      UNION ALL

      -- Tier 3: Stairs and elevators near a platform (elevated/underground)
      SELECT
        id as osm_id, name,
        CASE WHEN tags->>'highway' = 'elevator' THEN 'elevator'
             WHEN tags->>'conveying' IS NOT NULL THEN 'escalator'
             ELSE 'stairs' END as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        COALESCE(tags->>'level', '') as level,
        tags->>'highway' as access_type,
        ST_Y(centroid) as lat, ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        3 as tier
      FROM geo_places
      WHERE tags->>'highway' IN ${wheelchair ? "('elevator')" : "('steps', 'elevator')"}
        ${accessFilter}
        AND centroid && ST_Expand(${point}, ${degRadius})
        AND EXISTS (
          SELECT 1 FROM geo_places p
          WHERE (p.tags->>'public_transport' = 'platform' OR p.tags->>'railway' = 'platform')
            AND p.centroid && ST_Expand(geo_places.centroid, ${platformProximity * 1.5})
        )
      ` : ''}

      UNION ALL

      -- Tier 4: Railway crossings (at-grade track crossings)
      SELECT
        id as osm_id, name,
        '' as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        '0' as level,
        'railway_crossing' as access_type,
        ST_Y(centroid) as lat, ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        4 as tier
      FROM geo_places
      WHERE tags->>'railway' = 'crossing'
        ${accessFilter}
        AND centroid && ST_Expand(${point}, ${degRadius})

      ${hasPlatformNearby ? `
      UNION ALL

      -- Tier 5: Pedestrian crossings near platforms
      SELECT
        id as osm_id, name,
        '' as description,
        COALESCE(tags->>'wheelchair', '') as wheelchair,
        '0' as level,
        'highway_crossing' as access_type,
        ST_Y(centroid) as lat, ST_X(centroid) as lon,
        ST_Distance(centroid::geography, ${point}::geography) as distance_m,
        5 as tier
      FROM geo_places
      WHERE tags->>'highway' = 'crossing'
        ${accessFilter}
        AND centroid && ST_Expand(${point}, ${degRadius * 0.4})
        AND EXISTS (
          SELECT 1 FROM geo_places p
          WHERE (p.tags->>'public_transport' = 'platform' OR p.tags->>'railway' = 'platform')
            AND p.centroid && ST_Expand(geo_places.centroid, ${platformProximity})
        )
      ` : ''}
    )
    SELECT * FROM candidates
    WHERE tier = (SELECT MIN(tier) FROM candidates)
    ORDER BY ${wheelchair ? "(wheelchair = 'yes') DESC, " : ''}distance_m
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
