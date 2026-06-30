-- Conflate GTFS stop names with OSM transit-stop names.
--
-- Many agencies publish ugly/abbreviated stop names ("BWAY/W 42 ST"). OSM
-- usually has a cleaner, locally-correct name for the same physical stop. This
-- populates gtfs_stops.osm_name with the name of the nearest matching OSM
-- transit stop within 25 m; the transit_stops display view then prefers it via
-- COALESCE(NULLIF(osm_name,''), stop_name). Manual overrides (applied to
-- stop_name directly) still win where set. Safe to re-run.
--
-- Run: docker exec -i barrelman-db psql -U barrelman -d barrelman < import/backfill-osm-stop-names.sql

ALTER TABLE gtfs_stops ADD COLUMN IF NOT EXISTS osm_name TEXT;

-- Candidate OSM transit stops that carry a usable name. Materialised + indexed
-- so the per-stop nearest-neighbour lookup is a fast GiST KNN scan rather than
-- a repeated filter over the 21M-row geo_places table.
DROP TABLE IF EXISTS osm_transit_stop_candidates;
CREATE TEMP TABLE osm_transit_stop_candidates AS
SELECT name, centroid AS geom
FROM geo_places
WHERE name IS NOT NULL AND name <> ''
  AND centroid IS NOT NULL
  AND (
    tags->>'highway' = 'bus_stop'
    OR tags->>'public_transport' IN ('platform', 'stop_position', 'station')
    OR tags->>'railway' IN ('station', 'tram_stop', 'halt')
  );
CREATE INDEX osm_tsc_geom_idx ON osm_transit_stop_candidates USING GIST (geom);
ANALYZE osm_transit_stop_candidates;

-- Match each GTFS stop to the nearest candidate within 25 m.
UPDATE gtfs_stops s
SET osm_name = (
  SELECT c.name
  FROM osm_transit_stop_candidates c
  WHERE ST_DWithin(c.geom::geography, s.geom::geography, 25)
  ORDER BY c.geom <-> s.geom
  LIMIT 1
)
WHERE s.geom IS NOT NULL;

-- Report match rate.
SELECT
  count(*) FILTER (WHERE osm_name IS NOT NULL) AS matched,
  count(*) AS total,
  round(100.0 * count(*) FILTER (WHERE osm_name IS NOT NULL) / NULLIF(count(*), 0), 1) AS pct
FROM gtfs_stops;
