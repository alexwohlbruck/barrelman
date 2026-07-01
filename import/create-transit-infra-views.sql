-- OSM station-navigation infrastructure views for Martin tile serving.
-- Complements create-transit-views.sql (station_buildings / platforms /
-- entrances) with elevators and stairs, so the display layer can draw the
-- inside-the-station detail Apple shows at high zoom. Same pushdown-safe
-- pattern: no window functions, synthetic fid from (osm_type, osm_id).

-- Elevators (accessibility): highway=elevator points.
DROP VIEW IF EXISTS transit_elevators CASCADE;
CREATE VIEW transit_elevators AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) AS fid,
       id, name, centroid, 'point'::text AS geom_type,
       COALESCE(tags->>'level', '')      AS level,
       COALESCE(tags->>'wheelchair', '') AS wheelchair
FROM geo_places
WHERE tags->>'highway' = 'elevator';

-- Stairs: highway=steps lines. Drawn only at very high zoom (they're numerous),
-- to help users find station stairwells / street stairs.
DROP VIEW IF EXISTS transit_stairs CASCADE;
CREATE VIEW transit_stairs AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) AS fid,
       id, name, geom, geom_type,
       COALESCE(tags->>'incline', '') AS incline
FROM geo_places
WHERE tags->>'highway' = 'steps'
  AND geom_type = 'line';

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_transit_elevator_centroid_idx
  ON geo_places USING gist (centroid)
  WHERE tags->>'highway' = 'elevator';

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_transit_steps_geom_idx
  ON geo_places USING gist (geom)
  WHERE tags->>'highway' = 'steps';
