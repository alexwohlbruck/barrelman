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
