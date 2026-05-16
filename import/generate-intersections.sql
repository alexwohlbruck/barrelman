-- =============================================================================
-- Generate road intersection records in geo_places
-- =============================================================================
-- Finds points where differently-named roads cross, clusters nearby points,
-- and inserts synthetic "intersection" records into geo_places so they appear
-- in the search pipeline alongside regular OSM objects.
--
-- Run AFTER post-import.sql + codes + abbreviations, BEFORE parent context
-- resolution and tsvector rebuild (so intersections get context + FTS).
-- =============================================================================

-- Remove stale intersection rows from a previous run
DELETE FROM geo_places WHERE osm_type = 'X';

-- Step 1: Materialize named roads into a temp table with a spatial index.
-- CTEs lose the GIST index, causing a nested-loop over 450K×450K rows.
DROP TABLE IF EXISTS _tmp_named_roads;
CREATE TEMP TABLE _tmp_named_roads AS
  SELECT id, name, geom
  FROM geo_places
  WHERE geom_type = 'line'
    AND name IS NOT NULL
    AND categories && ARRAY[
      'highway/motorway', 'highway/motorway_link',
      'highway/trunk', 'highway/trunk_link',
      'highway/primary', 'highway/primary_link',
      'highway/secondary', 'highway/secondary_link',
      'highway/tertiary', 'highway/tertiary_link',
      'highway/residential',
      'highway/unclassified',
      'highway/living_street',
      'highway/cycleway',
      'highway/footway'
    ];
CREATE INDEX ON _tmp_named_roads USING gist (geom);
ANALYZE _tmp_named_roads;

-- Step 2: Find intersection points, cluster, and insert
WITH raw_points AS (
  SELECT
    a.name AS name_a,
    b.name AS name_b,
    (ST_Dump(ST_Intersection(a.geom, b.geom))).geom AS point
  FROM _tmp_named_roads a
  JOIN _tmp_named_roads b
    ON a.geom && b.geom
    AND ST_Intersects(a.geom, b.geom)
    AND a.name < b.name
),
point_only AS (
  SELECT name_a, name_b, point
  FROM raw_points
  WHERE ST_GeometryType(point) = 'ST_Point'
),
snapped AS (
  SELECT
    name_a,
    name_b,
    ST_SnapToGrid(point, 0.00001) AS snapped_point
  FROM point_only
),
all_names AS (
  SELECT snapped_point, name_a AS road_name FROM snapped
  UNION
  SELECT snapped_point, name_b AS road_name FROM snapped
),
clustered AS (
  SELECT
    snapped_point,
    array_agg(DISTINCT road_name ORDER BY road_name) AS road_names
  FROM all_names
  GROUP BY snapped_point
)
INSERT INTO geo_places (
  id, osm_type, osm_id, name, names, tags, categories,
  centroid, geom, geom_type
)
SELECT
  'intersection/' || row_number() OVER (ORDER BY ST_Y(snapped_point) DESC, ST_X(snapped_point)),
  'X',
  row_number() OVER (ORDER BY ST_Y(snapped_point) DESC, ST_X(snapped_point)),
  array_to_string(road_names, ' & '),
  road_names,
  '{}'::jsonb,
  ARRAY['highway/intersection']::text[],
  ST_SetSRID(snapped_point, 4326),
  ST_SetSRID(snapped_point, 4326),
  'point'
FROM clustered;

DROP TABLE IF EXISTS _tmp_named_roads;
