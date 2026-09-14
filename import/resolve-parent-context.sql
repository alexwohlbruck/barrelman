-- Resolve parent boundary context for all named places.
-- Populates parent_context with admin boundary names + address fields
-- so searches like "starbucks pineville nc" match via tsvector/embeddings.
--
-- Run AFTER post-import.sql (which creates the column, computes area_m2 and
-- defines build_ts()). Joins against admin boundaries and
-- neighbourhood/suburb area polygons.
--
-- This file also writes ts. parent_context exists to be searched, so every
-- row it touches needs its tsvector rebuilt anyway — doing both in one
-- statement halves the row versions on the widest UPDATE of the import.
-- rebuild-tsvectors.sql remains the entry point for rebuilding ts *without*
-- re-resolving context (daily updates), and its change guard makes running it
-- after this file nearly free.

-- Pass 0: Subdivide the boundary polygons.
--
-- The containment join used to probe raw boundary geometries, so every point
-- in a state was tested against that state's full outline — ST_Contains
-- against multi-million-vertex multipolygons, once per named place. Cutting
-- the boundaries into ≤255-vertex pieces first turns each probe into a
-- bbox-indexed test against a small convex-ish fragment; the classic
-- point-in-polygon-at-scale fix. ST_MakeValid because planet-scale extracts
-- always carry a few self-intersecting boundary relations, and one invalid
-- geometry would otherwise abort the whole pass; CollectionExtract(3) keeps
-- only the polygonal parts MakeValid may split off.
DROP TABLE IF EXISTS _tmp_boundary_pieces;
CREATE TEMP TABLE _tmp_boundary_pieces AS
SELECT id, name, area_m2,
       ST_Subdivide(ST_CollectionExtract(ST_MakeValid(geom), 3), 255) AS geom
FROM geo_places
WHERE geom_type = 'area'
  AND name IS NOT NULL
  AND (admin_level IS NOT NULL
       OR categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[]);
CREATE INDEX ON _tmp_boundary_pieces USING gist (geom);
ANALYZE _tmp_boundary_pieces;

-- Pass 1: Spatial join — find containing admin boundaries for each named POI.
-- Result example: "Providence Road Charlotte NC 28277 Elizabeth Charlotte Mecklenburg County North Carolina United States"
--
-- The DISTINCT inner select dedupes pieces: a point sits in exactly one
-- fragment of a subdivided polygon in the typical case, but a point on a
-- shared fragment edge would otherwise count its parent twice.
UPDATE geo_places p
SET parent_context = trim(
      coalesce(p.address->>'street', '') || ' ' ||
      coalesce(p.address->>'city', '') || ' ' ||
      coalesce(p.address->>'state', '') || ' ' ||
      coalesce(p.address->>'postcode', '') || ' ' ||
      coalesce(sub.boundary_names, '')),
    ts = build_ts(p.osm_type, p.name, p.names, p.name_abbrev, p.categories, trim(
      coalesce(p.address->>'street', '') || ' ' ||
      coalesce(p.address->>'city', '') || ' ' ||
      coalesce(p.address->>'state', '') || ' ' ||
      coalesce(p.address->>'postcode', '') || ' ' ||
      coalesce(sub.boundary_names, '')))
FROM (
  SELECT poi_id, string_agg(bname, ' ' ORDER BY barea ASC) AS boundary_names
  FROM (
    SELECT DISTINCT poi.id AS poi_id, b.id AS bid, b.name AS bname, b.area_m2 AS barea
    FROM geo_places poi
    JOIN _tmp_boundary_pieces b ON ST_Contains(b.geom, poi.centroid)
    WHERE poi.name IS NOT NULL
  ) pieces
  GROUP BY poi_id
) sub
WHERE p.id = sub.poi_id;

DROP TABLE IF EXISTS _tmp_boundary_pieces;

-- Pass 2: POIs that have address tags but fall outside any admin boundary
UPDATE geo_places
SET parent_context = trim(
      coalesce(address->>'street', '') || ' ' ||
      coalesce(address->>'city', '') || ' ' ||
      coalesce(address->>'state', '') || ' ' ||
      coalesce(address->>'postcode', '')),
    ts = build_ts(osm_type, name, names, name_abbrev, categories, trim(
      coalesce(address->>'street', '') || ' ' ||
      coalesce(address->>'city', '') || ' ' ||
      coalesce(address->>'state', '') || ' ' ||
      coalesce(address->>'postcode', '')))
WHERE name IS NOT NULL
  AND parent_context IS NULL
  AND address IS NOT NULL;

-- Pass 3: every named row the first two passes did not reach still needs a
-- tsvector (no boundaries, no address — the document is just name, aliases
-- and categories). Only rows still NULL, so the three passes write each named
-- row exactly once between them.
UPDATE geo_places
SET ts = build_ts(osm_type, name, names, name_abbrev, categories, parent_context)
WHERE name IS NOT NULL
  AND ts IS NULL;

-- Stats
DO $$
DECLARE
  ctx_count BIGINT;
BEGIN
  SELECT count(*) INTO ctx_count FROM geo_places WHERE parent_context IS NOT NULL;
  RAISE NOTICE 'Parent context resolved for % places', ctx_count;
END $$;
