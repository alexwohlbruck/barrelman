-- Map detail views for Martin tile serving
-- Trees, parking surfaces and street furniture, filtered out of geo_places
-- Run after OSM import (post-import.sql), alongside create-transit-views.sql
--
-- These exist because our basemap is a stock OpenMapTiles build, and that
-- schema carries no parking polygons (parking survives only as a poi point and
-- as service=parking_aisle centrelines) and no individual trees at all. The OSM
-- import already keeps every tagged node and way with its full tag set, so the
-- features are sitting in geo_places — they only need to be served as their own
-- sources rather than waiting on a custom Planetiler profile and a full pmtiles
-- rebuild, which is a manual job measured in hours.
--
-- IMPORTANT: same rule as create-transit-views.sql — no window functions. A
-- window function is an optimization fence: Martin's per-tile envelope filter
-- can't be pushed down into the geo_places scan, so every tile request degrades
-- to a full-table scan. The fid is derived deterministically from
-- (osm_type, osm_id), which is unique per row and pushdown-safe.
--
-- Columns are deliberately limited to what the client actually reads. These are
-- the densest sources we serve — a quarter of a million trees, drawn at z16-17
-- — so an unused column is paid for on every tile. Tag values stay text rather
-- than being cast: OSM heights are written "12", "12 m" and "~10" alike, and a
-- cast turns one bad value into a failed tile. The client parses leniently.

-- Parking: the paved surface, as a polygon
DROP VIEW IF EXISTS parking_areas CASCADE;
CREATE VIEW parking_areas AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, geom,
       -- Multi-storey, underground and rooftop parking are not ground you can
       -- see from above, and the client drops them on this tag. It has to be
       -- served raw for that filter to work.
       COALESCE(tags->>'parking', '') as parking
FROM geo_places
WHERE geom_type = 'area'
  AND tags->>'amenity' = 'parking';

-- Street trees: one node per tree, with whatever the surveyor recorded
DROP VIEW IF EXISTS street_trees CASCADE;
CREATE VIEW street_trees AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, centroid,
       -- Which of the three families the model comes from. leaf_type is the one
       -- tag with real coverage; genus and species carry the palms, which
       -- leaf_type has no value for.
       COALESCE(tags->>'leaf_type', '') as leaf_type,
       COALESCE(tags->>'genus', '') as genus,
       COALESCE(tags->>'species', '') as species,
       COALESCE(tags->>'taxon', '') as taxon,
       -- Size. A measured height is rare (well under 1% of trees), so the
       -- estimate and the trunk girth both matter: girth correlates strongly
       -- enough with height to narrow a guess.
       COALESCE(tags->>'height', '') as height,
       COALESCE(tags->>'est_height', '') as est_height,
       COALESCE(tags->>'diameter_crown', '') as diameter_crown,
       COALESCE(tags->>'circumference', '') as circumference,
       -- A street tree stands differently from one in a wood.
       COALESCE(tags->>'denotation', '') as denotation
FROM geo_places
WHERE geom_type = 'point'
  AND tags->>'natural' = 'tree';

-- Tree rows: an avenue is one line in OSM, not a tree per node. The client
-- plants along it, so it takes the same attributes a single tree does.
DROP VIEW IF EXISTS tree_rows CASCADE;
CREATE VIEW tree_rows AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, geom,
       COALESCE(tags->>'leaf_type', '') as leaf_type,
       COALESCE(tags->>'genus', '') as genus,
       COALESCE(tags->>'species', '') as species,
       COALESCE(tags->>'taxon', '') as taxon,
       COALESCE(tags->>'height', '') as height,
       COALESCE(tags->>'est_height', '') as est_height,
       COALESCE(tags->>'diameter_crown', '') as diameter_crown,
       COALESCE(tags->>'circumference', '') as circumference,
       COALESCE(tags->>'denotation', '') as denotation
FROM geo_places
WHERE geom_type = 'line'
  AND tags->>'natural' = 'tree_row';

-- Street furniture: bins, recycling containers and benches
DROP VIEW IF EXISTS street_furniture CASCADE;
CREATE VIEW street_furniture AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, centroid,
       -- Which model to draw. The client maps waste_disposal onto the same
       -- container as recycling.
       tags->>'amenity' as kind,
       -- The compass bearing a bench's seat faces. Only about one bench in
       -- thirty carries it, and the client leaves the rest out — a bench
       -- pointed the wrong way reads worse than no bench at all.
       COALESCE(tags->>'direction', '') as direction
FROM geo_places
WHERE geom_type = 'point'
  AND tags->>'amenity' IN ('bench', 'waste_basket', 'recycling', 'waste_disposal')
  -- A recycling *centre* is a depot you drive to, not a container on the
  -- pavement. Both are amenity=recycling, and only recycling_type separates
  -- them, so without this a civic amenity site draws as a single wheelie bin.
  AND COALESCE(tags->>'recycling_type', 'container') <> 'centre';

-- ─── 3D buildings ────────────────────────────────────────────────────────────
--
-- Building outlines and building:parts, with the flag that tells them apart.
--
-- OpenStreetMap's Simple 3D Buildings scheme maps a detailed building twice: an
-- outline tagged `building=*` covering the whole footprint, and one or more
-- `building:part=*` polygons inside it carrying the real heights and colours.
-- A 3D renderer must draw the parts and NOT the outline, or it draws both — two
-- solids in the same place, z-fighting, one at the outline's default height and
-- default colour and one at the part's own.
--
-- OpenMapTiles has a field for exactly this, `hide_3d`, set on the outline. Our
-- basemap is a stock OpenMapTiles build whose building layer carries only
-- `colour`, `render_height` and `render_min_height` — no `hide_3d` — so the
-- client's `["!has", "hide_3d"]` filter has nothing to bite on and every
-- part-mapped building renders doubled. Same reasoning as the views above: the
-- features are already in geo_places with their full tag set, so serving them
-- ourselves beats waiting on a custom Planetiler profile and a pmtiles rebuild.
--
-- MATERIALIZED, unlike everything else in this file, because `hide_3d` is a
-- spatial self-join — which outlines contain a part — and a join cannot be
-- pushed down into Martin's per-tile envelope filter. Computed once here,
-- indexed, and read like a table.
--
-- IF NOT EXISTS and WITH NO DATA, unlike the DROP/CREATE above, because this one
-- holds rows: the API runs this same file on every startup, and a DROP there
-- would empty the layer on each restart until something refreshed it again.
-- Created empty and populated out of band — `scripts/import-osm.sh` refreshes it
-- after an import, and the API refreshes it in the background on startup if it
-- is still empty. Same shape as the brand catalog in `src/db.ts`.
--
-- Changing the SELECT below therefore needs the view dropped by hand, since
-- IF NOT EXISTS will not redefine one that is already there:
--   DROP MATERIALIZED VIEW buildings_3d;  -- then re-run this file
CREATE MATERIALIZED VIEW IF NOT EXISTS buildings_3d AS
WITH shapes AS (
  SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
         id, geom, tags,
         COALESCE(tags->>'building:part', 'no') <> 'no' as is_part
  FROM geo_places
  WHERE geom_type = 'area'
    -- A basement or a subway concourse is not something you can see from above.
    -- OpenMapTiles drops these too.
    AND COALESCE(tags->>'location', '') <> 'underground'
    AND (COALESCE(tags->>'building', 'no') <> 'no'
      OR COALESCE(tags->>'building:part', 'no') <> 'no')
),
-- How much of each outline its own parts cover.
--
-- Two separate questions, and conflating them is what makes a naive
-- implementation eat buildings.
--
-- *Which parts belong to this outline.* S3DB says a part belongs to the
-- outline it is inside, so this is containment — but not a bare
-- `ST_Contains`, because a part clipped by an import boundary or drawn a
-- fraction outside its own outline would then belong to nothing. 99% of the
-- part's area is OSM2World's threshold and it is the right one: generous
-- enough for hand-drawn geometry, tight enough that a row house does not
-- adopt its neighbour's parts through a shared wall. A representative point
-- would be cheaper, and was what this did before, but a point says nothing
-- about how much of the part is actually inside — an L-shaped part lying
-- mostly over the building next door can still put its centre here.
--
-- *Whether they cover enough of it to stand in for it.* The wiki says the
-- parts should fill the outline and that a filled outline is not rendered,
-- but plenty of real buildings carry one small part — a rooftop plant room,
-- a lift overrun — on an otherwise unmodelled footprint. Dropping the outline
-- there deletes the building and leaves a box floating where its roof was.
-- So the outline goes only when the parts cover 90% of it, again OSM2World's
-- number, and otherwise it draws in full with the parts on top of it.
--
-- Not F4Map's approach, which subtracts the parts from the outline and draws
-- the remainder. That is the prettier answer on clean data and a worse one on
-- real data: an entrance node added to the part but not to the outline leaves
-- a sliver of building behind, and OSM2World dropped subtraction for exactly
-- that reason. Whole or nothing has no slivers.
--
-- The union is clipped to the outline before measuring, since parts may
-- overlap each other and a plain sum would read as covered when it is not.
covered AS (
  SELECT o.fid,
         ST_Area(ST_Intersection(ST_Union(p.geom), o.geom)) as covered_area,
         ST_Area(o.geom) as outline_area
  FROM shapes o
  JOIN shapes p
    ON p.is_part
   AND p.geom && o.geom
   AND (ST_Contains(o.geom, p.geom)
        -- Only for the parts a plain containment misses, since ST_Intersection
        -- is the expensive half of this join.
        OR ST_Area(ST_Intersection(o.geom, p.geom)) >= 0.99 * ST_Area(p.geom))
  WHERE NOT o.is_part
  GROUP BY o.fid, o.geom
),
-- Heights arrive as free text — "12", "12 m", "~10", "3,5" are all in use — and
-- a plain cast turns one bad value into a failed tile for the whole area. The
-- leading number is taken where there is one and the row falls through to the
-- level count otherwise, which is what OpenMapTiles does and what keeps these
-- columns numeric like the basemap's own.
measured AS (
  SELECT s.*,
         NULLIF(substring(s.tags->>'height'            from '^\s*([0-9]+(?:\.[0-9]+)?)'), '')::real as h,
         NULLIF(substring(s.tags->>'min_height'        from '^\s*([0-9]+(?:\.[0-9]+)?)'), '')::real as min_h,
         NULLIF(substring(s.tags->>'building:levels'   from '^\s*([0-9]+(?:\.[0-9]+)?)'), '')::real as levels,
         NULLIF(substring(s.tags->>'building:min_level' from '^\s*([0-9]+(?:\.[0-9]+)?)'), '')::real as min_levels
  FROM shapes s
)
SELECT m.fid, m.id, m.geom,
       -- 3.66m a storey and 5m for a building that records neither, both
       -- OpenMapTiles' numbers, so a building served from here and one served
       -- from the basemap stand the same height.
       COALESCE(m.h, m.levels * 3.66, 5)::real          as render_height,
       COALESCE(m.min_h, m.min_levels * 3.66, 0)::real  as render_min_height,
       -- Both spellings: OSM accepts either and both are in the data. NULL
       -- rather than '' where there is none, for the same reason as `hide_3d`
       -- below — a vector tile has no null, so the key is simply absent and a
       -- client can ask `["has", "colour"]` rather than testing for an empty
       -- string it would otherwise have to know about.
       NULLIF(COALESCE(m.tags->>'building:colour', m.tags->>'building:color', ''), '') as colour,
       -- The roof, separately. OpenMapTiles has no field for this at all, which
       -- is half the reason for serving buildings ourselves.
       NULLIF(COALESCE(m.tags->>'roof:colour', m.tags->>'roof:color', ''), '')         as roof_colour,
       -- An outline its parts have replaced is the one thing that must not draw;
       -- see `covered` above for when that is and is not the case.
       --
       -- TRUE or NULL, never FALSE, which is deliberate on both counts. A vector
       -- tile has no null, so Martin drops the key entirely for the ordinary
       -- building and only the hidden outlines carry it — which is how
       -- OpenMapTiles emits `hide_3d`, so a client's `["!has", "hide_3d"]`
       -- filter works unchanged, and the flag costs nothing on the 99% of
       -- buildings that are not part-mapped.
       CASE WHEN c.covered_area >= 0.9 * c.outline_area THEN true END as hide_3d
FROM measured m
LEFT JOIN covered c ON c.fid = m.fid
WITH NO DATA;

-- Unique on fid so the view can be refreshed CONCURRENTLY, and GIST on geom so
-- Martin's envelope filter is an index scan rather than a walk of every
-- building in the region.
CREATE UNIQUE INDEX IF NOT EXISTS buildings_3d_fid_idx ON buildings_3d (fid);
CREATE INDEX IF NOT EXISTS buildings_3d_geom_idx ON buildings_3d USING GIST (geom);
