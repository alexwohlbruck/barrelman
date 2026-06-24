-- Create transit infrastructure views for Martin tile serving
-- These views filter OSM transit data from geo_places for use as vector tile sources
-- Run after OSM import (post-import.sql)
--
-- IMPORTANT: the views must stay free of window functions (e.g.
-- ROW_NUMBER() OVER ()). A window function is an optimization fence: Martin's
-- per-tile envelope filter can't be pushed down into the geo_places scan, so
-- every tile request degrades to a full-table scan (observed: 6+ minute tile
-- queries on a 21M-row table). The fid is instead derived deterministically
-- from (osm_type, osm_id), which is unique per row and pushdown-safe.

-- Station buildings: polygonal outlines of transit stations
DROP VIEW IF EXISTS transit_station_buildings CASCADE;
CREATE VIEW transit_station_buildings AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, name, geom, geom_type,
       COALESCE(tags->>'railway', '') as railway,
       COALESCE(tags->>'public_transport', '') as public_transport,
       COALESCE(tags->>'building', '') as building_type,
       COALESCE(tags->>'station', '') as station_type,
       COALESCE(tags->>'network', '') as network,
       COALESCE(tags->>'operator', '') as operator
FROM geo_places
WHERE geom_type = 'area'
  AND (
    tags->>'building' = 'train_station'
    OR tags->>'public_transport' = 'station'
    OR tags->>'railway' = 'station'
  );

-- Platforms: platform shapes (polygons and lines)
DROP VIEW IF EXISTS transit_platforms CASCADE;
CREATE VIEW transit_platforms AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, name, geom, geom_type,
       COALESCE(tags->>'railway', '') as railway,
       COALESCE(tags->>'public_transport', '') as public_transport,
       COALESCE(tags->>'ref', '') as ref,
       COALESCE(tags->>'wheelchair', '') as wheelchair,
       COALESCE(tags->>'level', '') as level
FROM geo_places
WHERE (tags->>'public_transport' = 'platform' OR tags->>'railway' = 'platform')
  AND geom_type IN ('area', 'line');

-- Entrances: subway and train station entrance points with metadata
DROP VIEW IF EXISTS transit_entrances CASCADE;
CREATE VIEW transit_entrances AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, name, centroid, 'point'::text as geom_type,
       COALESCE(tags->>'railway', '') as railway_type,
       COALESCE(tags->>'entrance', '') as entrance_type,
       COALESCE(tags->>'wheelchair', '') as wheelchair,
       COALESCE(tags->>'level', '') as level,
       COALESCE(tags->>'description', '') as description,
       COALESCE(tags->>'ref', '') as ref,
       COALESCE(tags->>'network', '') as network,
       COALESCE(tags->>'operator', '') as operator
FROM geo_places
WHERE tags->>'railway' IN ('subway_entrance', 'train_station_entrance');

-- Partial spatial indexes matching each view's predicate. With pushdown
-- restored (no window fence), the planner answers a tile request with an
-- index scan over just the transit features instead of touching the rest
-- of geo_places. CONCURRENTLY keeps the live tile service unblocked; note
-- CONCURRENTLY cannot run inside a transaction block.
CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_transit_buildings_geom_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'area'
    AND (
      tags->>'building' = 'train_station'
      OR tags->>'public_transport' = 'station'
      OR tags->>'railway' = 'station'
    );

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_transit_platforms_geom_idx
  ON geo_places USING gist (geom)
  WHERE (tags->>'public_transport' = 'platform' OR tags->>'railway' = 'platform')
    AND geom_type IN ('area', 'line');

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_transit_entrances_centroid_idx
  ON geo_places USING gist (centroid)
  WHERE tags->>'railway' IN ('subway_entrance', 'train_station_entrance');
