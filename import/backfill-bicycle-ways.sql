-- Brings an existing bicycle_ways up to the import rules for lifecycle-prefix
-- bikeways and the streets signed routes follow, without a full reimport.
-- Mirrors `derive_bicycle_infra_type` and `is_route_street` in
-- osm2pgsql-flex.lua. Idempotent. Run through scripts/backfill-bicycle-ways.sh.

BEGIN;

-- `proposed:highway=cycleway` with no `highway` at all.
INSERT INTO bicycle_ways
  (osm_id, name, infra_type, highway, bicycle, surface, state, oneway, bridge, tunnel, geom)
SELECT
  g.osm_id,
  g.name,
  'cycleway',
  NULL,
  g.tags->>'bicycle',
  g.tags->>'surface',
  CASE WHEN g.tags->>'proposed:highway' = 'cycleway' THEN 'proposed' ELSE 'construction' END,
  CASE g.tags->>'oneway' WHEN 'yes' THEN 1 WHEN '1' THEN 1 WHEN '-1' THEN -1 ELSE 0 END,
  coalesce(g.tags->>'bridge', 'no') <> 'no',
  coalesce(g.tags->>'tunnel', 'no') <> 'no',
  g.geom
FROM geo_places g
WHERE g.osm_type = 'W'
  AND g.geom_type = 'line'
  AND NOT g.tags ? 'highway'
  -- `@>` rather than `->>`, so the tags index is used instead of a full scan.
  AND (g.tags @> '{"proposed:highway": "cycleway"}' OR g.tags @> '{"construction:highway": "cycleway"}')
  AND NOT EXISTS (SELECT 1 FROM bicycle_ways b WHERE b.osm_id = g.osm_id);

-- `route_ways` holds the way members of built bicycle routes; see
-- scripts/backfill-bicycle-ways.sh, which reads them from the source PBF.
CREATE TEMP TABLE route_streets ON COMMIT DROP AS
SELECT g.*
FROM route_ways rw
JOIN geo_places g ON g.osm_type = 'W' AND g.osm_id = rw.osm_id
WHERE g.geom_type = 'line'
  AND g.tags ? 'highway'
  AND coalesce(g.tags->>'bicycle', '') NOT IN ('no', 'use_sidepath')
  AND 'separate' NOT IN (
    coalesce(g.tags->>'cycleway', ''), coalesce(g.tags->>'cycleway:both', ''),
    coalesce(g.tags->>'cycleway:left', ''), coalesce(g.tags->>'cycleway:right', '')
  );

UPDATE bicycle_ways b
SET infra_type = 'bicycle_route'
FROM route_streets s
WHERE b.osm_id = s.osm_id AND b.infra_type = 'bicycle_yes' AND b.state IS NULL;

INSERT INTO bicycle_ways
  (osm_id, name, infra_type, highway, cycleway, cycleway_left, cycleway_right,
   bicycle, surface, state, oneway, bridge, tunnel, geom)
SELECT
  s.osm_id,
  s.name,
  'bicycle_route',
  s.tags->>'highway',
  s.tags->>'cycleway',
  coalesce(s.tags->>'cycleway:left', s.tags->>'cycleway:both'),
  coalesce(s.tags->>'cycleway:right', s.tags->>'cycleway:both'),
  s.tags->>'bicycle',
  s.tags->>'surface',
  NULL,
  CASE s.tags->>'oneway' WHEN 'yes' THEN 1 WHEN '1' THEN 1 WHEN '-1' THEN -1 ELSE 0 END,
  coalesce(s.tags->>'bridge', 'no') <> 'no',
  coalesce(s.tags->>'tunnel', 'no') <> 'no',
  s.geom
FROM route_streets s
WHERE NOT EXISTS (SELECT 1 FROM bicycle_ways b WHERE b.osm_id = s.osm_id);

COMMIT;

ANALYZE bicycle_ways;
