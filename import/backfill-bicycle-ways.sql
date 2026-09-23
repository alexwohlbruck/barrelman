-- Brings an existing bicycle_ways up to the import rules for lifecycle-prefix
-- bikeways and the streets signed routes follow, without a full reimport.
-- Mirrors `derive_bicycle_infra_type` and `is_route_street` in
-- osm2pgsql-flex.lua. Idempotent; replication replaces these rows as usual.

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
  AND (g.tags->>'proposed:highway' = 'cycleway' OR g.tags->>'construction:highway' = 'cycleway')
  AND NOT EXISTS (SELECT 1 FROM bicycle_ways b WHERE b.osm_id = g.osm_id);

-- Way members of built bicycle routes, from the osm2pgsql middle table.
CREATE TEMP TABLE route_ways ON COMMIT DROP AS
SELECT DISTINCT unnest(r.parts[r.way_off + 1 : r.rel_off]) AS osm_id
FROM planet_osm_rels r,
  -- The middle table stores tags as a flat key, value, key, value array.
  LATERAL (
    SELECT jsonb_object_agg(r.tags[i], r.tags[i + 1]) AS t
    FROM generate_series(1, array_length(r.tags, 1), 2) i
  ) tags
WHERE tags.t->>'type' = 'route'
  AND tags.t->>'route' = 'bicycle'
  AND coalesce(tags.t->>'state', '') NOT IN ('proposed', 'construction')
  AND lower(coalesce(tags.t->>'name', '')) !~ '(future|proposed|planned|construction)';

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
