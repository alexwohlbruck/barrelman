-- Partial spatial indexes for POST /route/hazards
--
-- The endpoint asks for the hazard-bearing nodes and ways within metres of a
-- route, piece by piece. Through the plain geo_places_geom_idx each piece reads
-- every building, address and polygon in its box and throws nearly all away:
-- about a second for a 20 km city route. Over these predicates it reads only
-- the candidates.
--
-- The WHERE clauses must stay identical to POINT_PREDICATE and WAY_PREDICATE in
-- src/services/route-hazards.service.ts; the planner uses a partial index only
-- when the query implies its predicate. A test holds the two together.
--
-- Run out of band, CONCURRENTLY, like create-detail-indexes.sql: each one scans
-- the whole of geo_places. Without them the endpoint is slower, never wrong.

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_route_hazard_points_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'point'
    AND (tags->>'highway' IN ('traffic_signals', 'stop', 'give_way', 'speed_camera')
         OR tags->>'barrier' = 'toll_booth'
         OR tags ?| array['hazard', 'traffic_calming', 'maxheight']);

CREATE INDEX CONCURRENTLY IF NOT EXISTS geo_places_route_hazard_ways_idx
  ON geo_places USING gist (geom)
  WHERE geom_type = 'line'
    AND (tags->>'railway' = 'tram'
         OR (tags ? 'highway'
             AND (tags->>'lit' = 'no' OR tags->>'flood_prone' = 'yes' OR tags->>'toll' = 'yes'
                  OR tags->>'bridge' = 'movable' OR tags->>'sidewalk' IN ('no', 'none')
                  OR tags->>'sidewalk:both' = 'no'
                  OR (tags->>'sidewalk:left' = 'no' AND tags->>'sidewalk:right' = 'no')
                  OR tags ?| array['hazard', 'maxheight', 'bridge:movable'])));
