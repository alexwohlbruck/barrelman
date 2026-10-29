-- Partial spatial indexes for the route context /route adds to each path
--
-- A crosswalk's road is read from named roads, overpasses from bridges and
-- raised ways along the route, parks from park polygons. Through the plain
-- geo_places_geom_idx each probe reads every building, sidewalk and polygon in
-- its box; over these predicates it reads only the candidates. States go
-- through geo_places_admin_geom_idx, which the import already builds.
--
-- The WHERE clauses must stay identical to NAMED_ROAD_PREDICATE,
-- OVERPASS_PREDICATE and PARK_PREDICATE in src/services/route-context.service.ts;
-- the planner uses a partial index only when the query implies its predicate.
-- A test holds them together.
--
-- Run out of band, CONCURRENTLY: each one scans the whole of geo_places.
-- Without them a route's context is slower and may run out of its budget and
-- be left off, never wrong.

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_route_named_road_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'line' AND name IS NOT NULL AND tags ? 'highway'
    AND tags->>'highway' NOT IN ('footway', 'path', 'cycleway', 'steps', 'pedestrian', 'corridor', 'bridleway', 'platform', 'proposed', 'construction', 'elevator');

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_route_overpass_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'line'
    AND (tags ? 'highway' OR tags ? 'railway')
    AND (tags->>'bridge' <> 'no' OR tags->>'layer' IN ('1', '2', '3', '4', '5'));

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_route_park_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'area' AND tags->>'leisure' = 'park';
