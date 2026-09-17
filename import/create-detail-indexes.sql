-- Partial spatial indexes for the map detail views
--
-- Split out of create-detail-views.sql, and the split is the point: that file
-- is run synchronously on every API startup, and building an index over
-- geo_places means scanning all of it. On a continental import that is minutes
-- per index with the API not yet listening — a deploy that looked like a
-- crash. Shipping these was a four-minute outage before the scan was killed.
--
-- So they live here, run out of band: `scripts/import-osm.sh` after an import,
-- and the "Map Detail Indexes" console task otherwise. Missing them costs
-- speed, never correctness — a tile without them is the same tile, read the
-- slow way.
--
-- CONCURRENTLY, unlike the indexes left in create-detail-views.sql, because
-- nothing here runs inside an implicit transaction: psql sends these one
-- statement at a time. That is also why this is a separate file rather than a
-- flag on the other one.

-- Same reasoning as create-transit-views.sql, and the same omission it already
-- fixed there: without these the planner answers a tile request from the plain
-- `geo_places_geom_idx`, gets every feature in the tile envelope, and then
-- throws away everything that is not a car park or a tree. Measured over a
-- Tucson z14 tile, `parking_areas` read 18,866 rows to return 436 — 11,823
-- buffers, which is nothing once they are cached and 650 ms of random reads
-- when they are not. A partial index over exactly the view's predicate turns
-- that into a scan of the 436.
--
-- Trees and furniture are already fast at the zooms they draw at (z16-17 tiles
-- are small), so these are about the cold case and about the planner having an
-- honest row estimate for a jsonb predicate it cannot otherwise guess.
--
CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_parking_geom_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'area' AND tags->>'amenity' = 'parking';

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_trees_centroid_idx
  ON geo_places USING gist (centroid)
  WHERE geom_type = 'point' AND tags->>'natural' = 'tree';

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_tree_rows_geom_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'line' AND tags->>'natural' = 'tree_row';

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_street_furniture_centroid_idx
  ON geo_places USING gist (centroid)
  WHERE geom_type = 'point'
    AND tags->>'amenity' IN ('bench', 'waste_basket', 'recycling', 'waste_disposal');
