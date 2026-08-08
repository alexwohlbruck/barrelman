/**
 * Station infrastructure: GTFS stations joined to the OSM entrances, elevators
 * and building outlines that let a router put someone at the right door.
 *
 * Every value that reaches Postgres from here is a bind parameter — through the
 * query builder for the plain lookups, through a `sql` tagged template for the
 * PostGIS work the builder cannot express. Neither form is a suggestion: with a
 * parameter present the driver uses Postgres's extended protocol, which refuses
 * multiple statements, so a `'; UPDATE …` in a path segment cannot execute even
 * if this file is edited carelessly later. The `sql.raw` string concatenation
 * this replaced had no such floor — `feedId` and `stopId` arrive straight from
 * `/transit/station/:feedId/:stopId` and were spliced in unescaped.
 */
import { db } from '../db'
import { and, eq, sql } from 'drizzle-orm'
import { gtfsStops, stationBuildings, stationEntrances } from '../schema/gtfs'
import { getRoutesForStop, type StopRoutesResult } from './transit.service'

/**
 * The OSM-linked relations this module reads are all built by import steps that
 * are separate from the GTFS import: `station_entrances` and `station_buildings`
 * by `import/create-station-links.sql`, `stop_area_members` by
 * `scripts/import-stop-areas.sh`. A GTFS-only instance has stations but none of
 * these, and that is a normal state, not an error — so their absence degrades to
 * "no entrances found" rather than a 500.
 *
 * Probed once per relation and cached for the process. `to_regclass` returns
 * NULL instead of raising, so this is a cheap lookup rather than a failed query.
 */
const relationCache = new Map<string, boolean>()

async function relationAvailable(name: string): Promise<boolean> {
  const cached = relationCache.get(name)
  if (cached !== undefined) return cached

  const [row] = (await db.execute(
    sql`SELECT to_regclass(${name}) IS NOT NULL AS ok`,
  )) as Array<{ ok: boolean }>
  const present = row?.ok === true
  relationCache.set(name, present)
  return present
}

/** Test seam: forget what was probed, so a fixture can change under the process. */
export function __resetRelationCacheForTests(): void {
  relationCache.clear()
}

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
  /** Lines serving the station, aggregated across its transfer complex. */
  routes: StopRoutesResult[]
}

/**
 * Get detailed station info including OSM-linked entrances and building geometry.
 */
export async function getStationDetail(
  feedId: string,
  stopId: string,
): Promise<StationDetail | null> {
  // Get the GTFS station
  const [station] = await db
    .select({
      stopId: gtfsStops.stopId,
      feedId: gtfsStops.feedId,
      stopName: gtfsStops.stopName,
      stopLat: gtfsStops.stopLat,
      stopLon: gtfsStops.stopLon,
    })
    .from(gtfsStops)
    .where(
      and(eq(gtfsStops.feedId, feedId), eq(gtfsStops.stopId, stopId), eq(gtfsStops.locationType, 1)),
    )
    .limit(1)

  if (!station) return null

  // Get linked entrances
  const entrances = !(await relationAvailable('station_entrances')) ? [] : await db
    .select({
      osmId: stationEntrances.osmEntranceId,
      name: stationEntrances.entranceName,
      description: stationEntrances.entranceDescription,
      wheelchair: stationEntrances.entranceWheelchair,
      level: stationEntrances.entranceLevel,
      railwayType: stationEntrances.railwayType,
      lat: sql<number>`ST_Y(${stationEntrances.entranceGeom})`,
      lon: sql<number>`ST_X(${stationEntrances.entranceGeom})`,
      distanceM: stationEntrances.distanceM,
    })
    .from(stationEntrances)
    .where(and(eq(stationEntrances.feedId, feedId), eq(stationEntrances.stopId, stopId)))
    .orderBy(stationEntrances.distanceM)

  // Get linked buildings
  const buildings = !(await relationAvailable('station_buildings')) ? [] : await db
    .select({
      osmId: stationBuildings.osmBuildingId,
      name: stationBuildings.buildingName,
      stationType: stationBuildings.stationType,
      geometry: sql<GeoJSON.Geometry>`ST_AsGeoJSON(${stationBuildings.buildingGeom})::jsonb`,
    })
    .from(stationBuildings)
    .where(and(eq(stationBuildings.feedId, feedId), eq(stationBuildings.stopId, stopId)))

  return {
    stopId: station.stopId,
    feedId: station.feedId,
    stopName: station.stopName as string,
    lat: station.stopLat,
    lon: station.stopLon,
    entrances: entrances.map((r) => ({
      osmId: r.osmId as string,
      name: r.name || null,
      description: r.description || null,
      wheelchair: r.wheelchair || null,
      level: r.level || null,
      accessType: r.railwayType as string,
      lat: Number(r.lat),
      lon: Number(r.lon),
      distanceM: Number(r.distanceM),
    })),
    buildings: buildings.map((r) => ({
      osmId: r.osmId as string,
      name: r.name || null,
      stationType: r.stationType || null,
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
  //
  // The conditional pieces below are SQL *fragments*, not values: they select
  // between fixed clauses this module wrote, and carry nothing a caller
  // supplied. Only `lat`, `lon` and the radii come from outside, and those go
  // through `${}` in a `sql` template, which binds them as parameters.
  const accessFilter = wheelchair
    ? sql`AND COALESCE(tags->>'wheelchair', '') <> 'no'`
    : sql.empty()
  const point = sql`ST_SetSRID(ST_MakePoint(${lon}, ${lat}), 4326)`
  // Tighter radius for proximity-to-platform checks (~ 100m in degrees)
  const platformProximity = 0.001

  // First, check if there are any platforms in the search area.
  // This lets us skip the expensive EXISTS subqueries for tiers 2/3/5
  // when no platform is nearby (most places on earth).
  const platformCheck = (await db.execute(sql`
    SELECT 1 FROM geo_places
    WHERE (tags->>'public_transport' = 'platform' OR tags->>'railway' = 'platform')
      AND centroid && ST_Expand(${point}, ${degRadius})
    LIMIT 1
  `)) as any[]
  const hasPlatformNearby = platformCheck.length > 0

  // stop_area relations are loaded by scripts/import-stop-areas.sh —
  // skip Tier 0 gracefully on databases that haven't run it yet.
  const stopAreasAvailable = await relationAvailable('stop_area_members')

  const accessFilterE = wheelchair
    ? sql`AND COALESCE(e.tags->>'wheelchair', '') <> 'no'`
    : sql.empty()

  const tier0 = stopAreasAvailable
    ? sql`
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
        ${wheelchair ? sql`AND e.tags->>'highway' IS DISTINCT FROM 'steps'` : sql.empty()}
        ${accessFilterE}
        AND e.centroid && ST_Expand(${point}, ${degRadius})

      UNION ALL
      `
    : sql.empty()

  // Tiers 2, 3 and 5 all require a platform nearby, so they are omitted
  // entirely rather than evaluated and discarded — their EXISTS subqueries are
  // the expensive part, and most coordinates on earth have no platform.
  const tiers23 = hasPlatformNearby
    ? sql`
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
      WHERE tags->>'highway' IN ${wheelchair ? sql`('elevator')` : sql`('steps', 'elevator')`}
        ${accessFilter}
        AND centroid && ST_Expand(${point}, ${degRadius})
        AND EXISTS (
          SELECT 1 FROM geo_places p
          WHERE (p.tags->>'public_transport' = 'platform' OR p.tags->>'railway' = 'platform')
            AND p.centroid && ST_Expand(geo_places.centroid, ${platformProximity * 1.5})
        )
      `
    : sql.empty()

  const tier5 = hasPlatformNearby
    ? sql`
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
      `
    : sql.empty()

  const rows = (await db.execute(sql`
    WITH candidates AS (
      ${tier0}

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

      ${tiers23}

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

      ${tier5}
    )
    SELECT * FROM candidates
    WHERE tier = (SELECT MIN(tier) FROM candidates)
    ORDER BY ${wheelchair ? sql`(wheelchair = 'yes') DESC,` : sql.empty()} distance_m
    LIMIT 1
  `)) as any[]

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
