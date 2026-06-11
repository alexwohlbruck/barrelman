-- Create transit infrastructure views for Martin tile serving
-- These views filter OSM transit data from geo_places for use as vector tile sources
-- Run after OSM import (post-import.sql)

-- Station buildings: polygonal outlines of transit stations
DROP VIEW IF EXISTS transit_station_buildings CASCADE;
CREATE VIEW transit_station_buildings AS
SELECT ROW_NUMBER() OVER () as fid,
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
SELECT ROW_NUMBER() OVER () as fid,
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
SELECT ROW_NUMBER() OVER () as fid,
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
