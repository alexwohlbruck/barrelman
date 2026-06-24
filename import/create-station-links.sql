-- Materialized links between GTFS stations and nearby OSM infrastructure.
-- Consumed by station.service.ts getStationDetail() — the /transit/station
-- endpoint. Run after OSM + GTFS imports; re-run (or REFRESH MATERIALIZED
-- VIEW) whenever either dataset is re-imported.
--
-- These are intentionally built straight from geo_places rather than from
-- the transit_* tile views (create-transit-views.sql): the tile views get
-- dropped/recreated with CASCADE, which would silently destroy any
-- dependent matview (that's how station_entrances was lost once — this
-- file is its committed reconstruction).

-- ── Station ↔ entrance links ────────────────────────────────────────────
-- One row per (station, entrance) within 250m, nearest-first via distance_m.
-- The && ST_Expand pre-filter keeps the join on the partial centroid index
-- (geo_places_transit_entrances_centroid_idx); the geography distance gives
-- exact meters. 0.003° ≈ 330m, comfortably wider than the 250m cutoff.
DROP MATERIALIZED VIEW IF EXISTS station_entrances;
CREATE MATERIALIZED VIEW station_entrances AS
SELECT
  s.feed_id,
  s.stop_id,
  e.id AS osm_entrance_id,
  e.name AS entrance_name,
  e.tags->>'description' AS entrance_description,
  e.tags->>'wheelchair' AS entrance_wheelchair,
  e.tags->>'level' AS entrance_level,
  e.tags->>'railway' AS railway_type,
  e.centroid AS entrance_geom,
  ST_Distance(
    e.centroid::geography,
    ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326)::geography
  ) AS distance_m
FROM gtfs_stops s
JOIN geo_places e
  ON e.tags->>'railway' IN ('subway_entrance', 'train_station_entrance')
 AND e.centroid && ST_Expand(
       ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326), 0.003)
WHERE s.location_type = 1
  AND ST_Distance(
        e.centroid::geography,
        ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326)::geography
      ) <= 250;

CREATE INDEX station_entrances_stop_idx
  ON station_entrances (feed_id, stop_id);

-- ── Station ↔ building links ────────────────────────────────────────────
-- Station buildings that contain the station point or sit within ~100m.
DROP MATERIALIZED VIEW IF EXISTS station_buildings;
CREATE MATERIALIZED VIEW station_buildings AS
SELECT
  s.feed_id,
  s.stop_id,
  b.id AS osm_building_id,
  b.name AS building_name,
  b.tags->>'station' AS station_type,
  b.geom AS building_geom
FROM gtfs_stops s
JOIN geo_places b
  ON b.geom_type = 'area'
 AND (
   b.tags->>'building' = 'train_station'
   OR b.tags->>'public_transport' = 'station'
   OR b.tags->>'railway' = 'station'
 )
 AND b.geom && ST_Expand(
       ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326), 0.0012)
WHERE s.location_type = 1
  AND (
    ST_Contains(b.geom, ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326))
    OR ST_Distance(
         b.geom::geography,
         ST_SetSRID(ST_MakePoint(s.stop_lon, s.stop_lat), 4326)::geography
       ) <= 100
  );

CREATE INDEX station_buildings_stop_idx
  ON station_buildings (feed_id, stop_id);
