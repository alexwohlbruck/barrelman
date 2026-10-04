-- Re-derives the post-import columns for the rows replicate-extract.sh just
-- swapped into geo_places, and for nothing else.
--
-- The full pipeline (post-import.sql, generate-codes-abbrevs.sql,
-- generate-intersections.sql, resolve-parent-context.sql) scans the whole
-- table. That is fine once per import and far too slow per update: on a US
-- import it is days of work to absorb one day of edits. Each statement here
-- does the same derivation as its full-pipeline counterpart, restricted to the
-- swapped rows plus whatever those rows invalidate. Keep the expressions in
-- step with the files named in each section.
--
-- Runs inside the swap transaction, so a failure here rolls the swap back and
-- the next run retries the same diffs. Inputs, in schema osm_replay:
--   changed     ids of the rows just inserted
--   old_places  the rows they replaced, and the rows of deleted objects

-- ── Tag columns (post-import.sql) ─────────────────────────────────────────
-- The rows are fresh, so every column is NULL and needs no IS DISTINCT FROM guard.
UPDATE geo_places g SET
    address = CASE WHEN (tags ? 'addr:street' OR tags ? 'addr:housenumber')
        THEN jsonb_build_object(
            'housenumber', tags->>'addr:housenumber',
            'street', tags->>'addr:street',
            'unit', tags->>'addr:unit',
            'city', tags->>'addr:city',
            'state', tags->>'addr:state',
            'postcode', tags->>'addr:postcode',
            'country', tags->>'addr:country') END,
    hours = tags->>'opening_hours',
    phones = CASE WHEN (tags ? 'phone' OR tags ? 'contact:phone' OR tags ? 'contact:mobile')
        THEN ARRAY(SELECT unnest FROM unnest(ARRAY[
            tags->>'phone', tags->>'contact:phone', tags->>'contact:mobile'
        ]) WHERE unnest IS NOT NULL) END,
    websites = CASE WHEN (tags ? 'website' OR tags ? 'contact:website' OR tags ? 'url')
        THEN ARRAY(SELECT unnest FROM unnest(ARRAY[
            tags->>'website', tags->>'contact:website', tags->>'url'
        ]) WHERE unnest IS NOT NULL) END
FROM osm_replay.changed c
WHERE g.id = c.id
  AND g.tags ?| ARRAY['addr:street', 'addr:housenumber', 'opening_hours', 'phone',
                      'contact:phone', 'contact:mobile', 'website', 'contact:website', 'url'];

-- ── area_m2 (post-import.sql) ─────────────────────────────────────────────
-- A generated column on databases imported since it became one; Postgres
-- already filled it. Older databases have a plain column to fill here.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_name = 'geo_places' AND column_name = 'area_m2'
               AND is_generated = 'NEVER') THEN
    UPDATE geo_places g SET area_m2 = ST_Area(g.geom::geography)::real
    FROM osm_replay.changed c
    WHERE g.id = c.id AND g.geom_type = 'area';
  END IF;
END $$;

-- ── Codes and abbreviations (generate-codes-abbrevs.sql) ──────────────────
UPDATE geo_places g SET codes = s.codes
FROM (
  SELECT p.id,
    array_agg(DISTINCT lower(trim(code))) FILTER (WHERE trim(code) <> '') AS codes
  FROM osm_replay.changed c
  JOIN geo_places p ON p.id = c.id,
  LATERAL unnest(
    string_to_array(coalesce(p.tags->>'iata', ''), ';') ||
    string_to_array(coalesce(p.tags->>'icao', ''), ';') ||
    string_to_array(coalesce(p.tags->>'ref', ''), ';') ||
    string_to_array(coalesce(p.tags->>'short_name', ''), ';') ||
    string_to_array(coalesce(p.tags->>'abbreviation', ''), ';') ||
    string_to_array(coalesce(p.tags->>'alt_name', ''), ';')
  ) AS code
  WHERE p.tags ?| ARRAY['iata', 'icao', 'ref', 'short_name', 'abbreviation', 'alt_name']
  GROUP BY p.id
) s
WHERE g.id = s.id AND s.codes IS NOT NULL;

UPDATE geo_places g SET name_abbrev = s.abbrev
FROM (
  SELECT id, lower(string_agg(left(word, 1), '' ORDER BY ord)) AS abbrev
  FROM (
    SELECT p.id, word, ord
    FROM osm_replay.changed c
    JOIN geo_places p ON p.id = c.id,
    LATERAL unnest(regexp_split_to_array(p.name, '\s+')) WITH ORDINALITY AS t(word, ord)
    WHERE p.name IS NOT NULL
      AND p.name ~ '^[\w\s\d\-''\.&]+$'
  ) words
  WHERE lower(word) NOT IN (
    'of','the','and','at','in','for','a','an',
    'de','la','le','les','du','des','et','au',
    'der','die','das','von','und','im','am',
    'del','los','las','el','dos','e',
    'di','della','dei','degli'
  )
  AND length(word) > 0
  GROUP BY id
  HAVING count(*) >= 2
) s
WHERE g.id = s.id;

-- ── Intersections (generate-intersections.sql) ────────────────────────────
-- An intersection row is a point where two differently named roads cross. The
-- full pass deletes every one and recomputes. Here only the ones near a road
-- the swap touched are redone:
--   1. Delete intersection rows within TOL of a touched road, old or new shape.
--   2. Gather the named roads near those deleted points or a touched road's
--      new shape. Every road through a point that needs
--      redoing is among them, so pairing them with each other is enough.
--   3. Recompute crossings among those roads, keeping only points within TOL
--      of the area that was cleared, so crossings elsewhere along a long road
--      are not added twice.
-- TOL is 2e-5 degrees (about 2 m), twice the 1e-5 grid the points snap to.
CREATE TEMP TABLE _named_road_kinds (category text);
INSERT INTO _named_road_kinds VALUES
  ('highway/motorway'), ('highway/motorway_link'),
  ('highway/trunk'), ('highway/trunk_link'),
  ('highway/primary'), ('highway/primary_link'),
  ('highway/secondary'), ('highway/secondary_link'),
  ('highway/tertiary'), ('highway/tertiary_link'),
  ('highway/residential'),
  ('highway/unclassified'),
  ('highway/living_street'),
  ('highway/cycleway'),
  ('highway/footway');

CREATE TEMP TABLE _touched_roads AS
SELECT geom, false AS is_new FROM osm_replay.old_places
WHERE geom_type = 'line' AND name IS NOT NULL
  AND categories && (SELECT array_agg(category) FROM _named_road_kinds)
UNION ALL
SELECT g.geom, true FROM osm_replay.changed c
JOIN geo_places g ON g.id = c.id
WHERE g.geom_type = 'line' AND g.name IS NOT NULL
  AND g.categories && (SELECT array_agg(category) FROM _named_road_kinds);
CREATE INDEX ON _touched_roads USING gist (geom);
ANALYZE _touched_roads;

-- A data-modifying WITH is only allowed at the top level, hence the separate
-- CREATE and INSERT.
CREATE TEMP TABLE _cleared (geom geometry);
WITH hit AS (
  SELECT DISTINCT x.id
  FROM _touched_roads r
  JOIN geo_places x
    ON x.centroid && ST_Expand(r.geom, 0.00002)
   AND x.osm_type = 'X'
   AND ST_DWithin(x.centroid, r.geom, 0.00002)
),
gone AS (
  DELETE FROM geo_places g USING hit
  WHERE g.id = hit.id AND g.osm_type = 'X'
  RETURNING g.centroid
)
INSERT INTO _cleared SELECT centroid FROM gone;

-- What step 3 may add points within: the cleared points and the new shapes.
CREATE TEMP TABLE _redo_zone AS
SELECT geom FROM _cleared
UNION ALL
SELECT geom FROM _touched_roads WHERE is_new;
CREATE INDEX ON _redo_zone USING gist (geom);
ANALYZE _redo_zone;

-- 3e-5 rather than TOL: a kept point may sit TOL from the zone, and snapping
-- may have moved it up to ~0.71e-5 from where its roads actually cross.
CREATE TEMP TABLE _redo_roads AS
SELECT DISTINCT ON (g.id) g.id, g.name, g.geom
FROM _redo_zone z
JOIN geo_places g
  ON g.geom && ST_Expand(z.geom, 0.00003)
 AND g.geom_type = 'line'
 AND g.name IS NOT NULL
 AND g.categories && (SELECT array_agg(category) FROM _named_road_kinds)
 AND ST_DWithin(g.geom, z.geom, 0.00003);
CREATE INDEX ON _redo_roads USING gist (geom);
ANALYZE _redo_roads;

CREATE TEMP TABLE _new_intersections AS
WITH raw_points AS (
  SELECT a.name AS name_a, b.name AS name_b,
    (ST_Dump(ST_Intersection(a.geom, b.geom))).geom AS point
  FROM _redo_roads a
  JOIN _redo_roads b
    ON a.geom && b.geom
   AND ST_Intersects(a.geom, b.geom)
   AND a.name < b.name
),
snapped AS (
  SELECT name_a, name_b, ST_SnapToGrid(point, 0.00001) AS snapped_point
  FROM raw_points
  WHERE ST_GeometryType(point) = 'ST_Point'
),
all_names AS (
  SELECT snapped_point, name_a AS road_name FROM snapped
  UNION
  SELECT snapped_point, name_b AS road_name FROM snapped
),
clustered AS (
  SELECT snapped_point, array_agg(DISTINCT road_name ORDER BY road_name) AS road_names
  FROM all_names
  GROUP BY snapped_point
)
SELECT ST_SetSRID(c.snapped_point, 4326) AS point, c.road_names
FROM clustered c
WHERE EXISTS (
  SELECT 1 FROM _redo_zone z
  WHERE ST_DWithin(ST_SetSRID(c.snapped_point, 4326), z.geom, 0.00002)
)
-- A point that survived step 1 is already right; never add it twice.
AND NOT EXISTS (
  SELECT 1 FROM geo_places x
  WHERE x.osm_type = 'X' AND x.centroid && ST_SetSRID(c.snapped_point, 4326)
    AND ST_Equals(x.centroid, ST_SetSRID(c.snapped_point, 4326))
);

-- The full pass numbers intersections 1..n. New ones continue after the
-- highest id in use rather than reusing a deleted one.
WITH base AS (
  SELECT coalesce(max(osm_id), 0) AS n FROM geo_places WHERE osm_type = 'X'
),
numbered AS (
  SELECT base.n + row_number() OVER (ORDER BY ST_Y(point) DESC, ST_X(point)) AS osm_id,
         point, road_names
  FROM _new_intersections, base
),
added AS (
  INSERT INTO geo_places (id, osm_type, osm_id, name, names, tags, categories,
                          centroid, geom, geom_type)
  SELECT 'intersection/' || osm_id, 'X', osm_id,
         array_to_string(road_names, ' & '), road_names, '{}'::jsonb,
         ARRAY['highway/intersection']::text[], point, point, 'point'
  FROM numbered
  RETURNING id
)
INSERT INTO osm_replay.changed SELECT id FROM added;

-- ── Parent context (resolve-parent-context.sql) ───────────────────────────
-- Every named row the swap inserted needs it, plus every named row whose
-- containing boundaries the swap changed. For a boundary that kept its name
-- only the area between its old and new outline changed hands; anything that
-- was added, removed or renamed affects everything inside it.
CREATE TEMP TABLE _old_boundaries AS
SELECT osm_type, osm_id, name, geom FROM osm_replay.old_places
WHERE geom_type = 'area' AND name IS NOT NULL
  AND (admin_level IS NOT NULL
       OR categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[]);

CREATE TEMP TABLE _new_boundaries AS
SELECT g.osm_type, g.osm_id, g.name, g.geom FROM osm_replay.changed c
JOIN geo_places g ON g.id = c.id
WHERE g.geom_type = 'area' AND g.name IS NOT NULL
  AND (g.admin_level IS NOT NULL
       OR g.categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[]);

-- ST_SymDifference can raise on an invalid outline, so both sides are made
-- valid first, as resolve-parent-context.sql does. If it still raises, both
-- outlines whole are redone: slower, still correct, and one bad polygon cannot
-- fail every update until someone fixes it.
CREATE OR REPLACE FUNCTION pg_temp.changed_area(old_geom geometry, new_geom geometry)
RETURNS geometry LANGUAGE plpgsql AS $$
BEGIN
  RETURN ST_SymDifference(ST_MakeValid(old_geom), ST_MakeValid(new_geom));
EXCEPTION WHEN OTHERS THEN
  RETURN ST_Collect(old_geom, new_geom);
END $$;

CREATE TEMP TABLE _context_zone AS
SELECT pg_temp.changed_area(o.geom, n.geom) AS geom
FROM _old_boundaries o
JOIN _new_boundaries n ON n.osm_type = o.osm_type AND n.osm_id = o.osm_id AND n.name = o.name
-- Byte comparison, not ST_Equals: it needs no GEOS call, so an invalid
-- outline cannot raise here.
WHERE ST_AsBinary(o.geom) <> ST_AsBinary(n.geom)
UNION ALL
SELECT o.geom FROM _old_boundaries o
WHERE NOT EXISTS (SELECT 1 FROM _new_boundaries n
                  WHERE n.osm_type = o.osm_type AND n.osm_id = o.osm_id AND n.name = o.name)
UNION ALL
SELECT n.geom FROM _new_boundaries n
WHERE NOT EXISTS (SELECT 1 FROM _old_boundaries o
                  WHERE o.osm_type = n.osm_type AND o.osm_id = n.osm_id AND o.name = n.name);

CREATE TEMP TABLE _recontext AS
SELECT c.id FROM osm_replay.changed c
UNION
SELECT p.id FROM _context_zone z
JOIN geo_places p
  ON p.centroid && z.geom
 AND p.name IS NOT NULL
 AND ST_Intersects(z.geom, p.centroid)
WHERE NOT ST_IsEmpty(z.geom);
CREATE INDEX ON _recontext (id);
ANALYZE _recontext;

-- Containment is tested against subdivided boundaries, as the full pass does.
-- A raw country-scale outline has tens of thousands of vertices, and testing
-- points against it directly ran at about 160 rows a second. Only the
-- boundaries whose box holds one of the rows are subdivided.
CREATE TEMP TABLE _context_points AS
SELECT p.id, p.centroid FROM _recontext r
JOIN geo_places p ON p.id = r.id AND p.name IS NOT NULL;
CREATE INDEX ON _context_points USING gist (centroid);
ANALYZE _context_points;

CREATE TEMP TABLE _boundary_pieces AS
SELECT b.id, b.name, b.area_m2,
       ST_Subdivide(ST_CollectionExtract(ST_MakeValid(b.geom), 3), 255) AS geom
FROM geo_places b
WHERE b.id IN (
  SELECT DISTINCT b2.id
  FROM _context_points pt
  JOIN geo_places b2
    ON b2.geom && pt.centroid
   AND b2.geom_type = 'area'
   AND b2.name IS NOT NULL
   AND (b2.admin_level IS NOT NULL
        OR b2.categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[])
);
CREATE INDEX ON _boundary_pieces USING gist (geom);
ANALYZE _boundary_pieces;

-- One known gap: a boundary that changed shape but not name only redoes the
-- rows in the area that changed hands. If its new area_m2 reorders it against
-- another boundary nested with it, rows outside that area keep the old order
-- until they are next touched. Both names are still there.
--
-- Same result as resolve-parent-context.sql's two passes: the address parts
-- and the containing boundaries, smallest first; just the address when no
-- boundary contains the row; NULL when it has neither.
UPDATE geo_places p
SET parent_context = CASE
      WHEN sub.boundary_names IS NOT NULL OR p.address IS NOT NULL THEN trim(
        coalesce(p.address->>'street', '') || ' ' ||
        coalesce(p.address->>'city', '') || ' ' ||
        coalesce(p.address->>'state', '') || ' ' ||
        coalesce(p.address->>'postcode', '') || ' ' ||
        coalesce(sub.boundary_names, ''))
    END
FROM (
  SELECT pt.id, names.boundary_names
  FROM _context_points pt
  LEFT JOIN (
    SELECT poi_id, string_agg(bname, ' ' ORDER BY barea ASC) AS boundary_names
    FROM (
      SELECT DISTINCT pt2.id AS poi_id, b.id AS bid, b.name AS bname, b.area_m2 AS barea
      FROM _context_points pt2
      JOIN _boundary_pieces b ON ST_Contains(b.geom, pt2.centroid)
    ) pieces
    GROUP BY poi_id
  ) names ON names.poi_id = pt.id
) sub
WHERE p.id = sub.id;

-- ── Search document (rebuild-tsvectors.sql) ───────────────────────────────
-- Last, because it reads name_abbrev and parent_context.
UPDATE geo_places g
SET ts = build_ts(g.osm_type, g.name, g.names, g.name_abbrev, g.categories, g.parent_context)
FROM _recontext r
WHERE g.id = r.id AND g.name IS NOT NULL;

DROP TABLE _named_road_kinds, _touched_roads, _cleared, _redo_zone, _redo_roads,
  _new_intersections, _old_boundaries, _new_boundaries, _context_zone, _recontext,
  _context_points, _boundary_pieces;
