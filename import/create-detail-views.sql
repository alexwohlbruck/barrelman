-- Map detail views for Martin tile serving
-- Trees, parking surfaces, street furniture and coaster tracks, filtered out of
-- geo_places
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

-- Administrative boundaries, the one geo_places layer that genuinely belongs at
-- LOW zoom.
--
-- `parchment_boundaries` used to read geo_places unfiltered from z4, which meant
-- a tile serialised every feature inside it rather than the boundaries: at z8
-- that measured 83.6 MB in 10.1 s, and z4/z6 died with a db error after a 10 s
-- statement timeout. The other unfiltered sources (roads, water, landuse, pois)
-- were given a z14 floor instead, because low-zoom roads need GENERALISATION —
-- dropping minor features and simplifying geometry — which a live table query
-- cannot do and which the basemap already does.
--
-- Boundaries are different: there are few of them, and `geo_places_admin_geom_idx`
-- already indexes exactly this predicate, so a filter is both correct and fast
-- where a zoom floor would simply remove the feature. The same query the tile
-- makes, over the whole US Southwest, plans to that index and runs in 52 ms.
DROP VIEW IF EXISTS admin_boundaries CASCADE;
CREATE VIEW admin_boundaries AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, name, geom,
       -- Carried because martin-config lists it as a tile property; the filter
       -- already pins it to 'area', so it is constant, but dropping it here
       -- would stop the source configuring at startup.
       geom_type,
       admin_level
FROM geo_places
WHERE geom_type = 'area'
  AND admin_level IS NOT NULL;

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

-- ─── Street furniture ────────────────────────────────────────────────────────
--
-- One point per object, with the model to draw (`kind`) and the compass bearing
-- it faces (`direction`, degrees as text, same meaning as the OSM tag).
--
-- OSM's own `direction` wins. Under one bench in thirty carries it, so anything
-- with a front is otherwise turned to face its nearest way: benches, tables,
-- racks and fountains face a road or path within 14 m; lamps and billboards
-- face a carriageway (25 m and 60 m), lamps falling back to a path. An object
-- standing on its way turns side-on to it. Bins and bollards have no front and
-- no bearing.
--
-- MATERIALIZED for the same reason as buildings_3d below: the nearest-way
-- lookup is a join, which Martin cannot push its tile envelope into. Created
-- empty, filled by scripts/refresh-view.sh after an import or update and by the
-- API on startup if still empty. A change to the SELECT needs
--   DROP MATERIALIZED VIEW street_furniture;  -- then re-run this file
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'street_furniture' AND relkind = 'v') THEN
    DROP VIEW street_furniture;
  END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS street_furniture AS
WITH furniture AS MATERIALIZED (
  SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
         osm_id, id, centroid,
         CASE
           WHEN tags->>'leisure' = 'picnic_table' THEN 'picnic_table'
           WHEN tags->>'highway' = 'street_lamp' THEN 'street_lamp'
           WHEN tags->>'barrier' = 'bollard' THEN 'bollard'
           WHEN tags->>'advertising' = 'billboard' THEN 'billboard'
           ELSE tags->>'amenity'
         END as kind,
         NULLIF(tags->>'direction', '') as direction
  FROM geo_places
  WHERE geom_type = 'point'
    AND (tags->>'amenity' IN ('bench', 'waste_basket', 'recycling', 'waste_disposal',
                              'drinking_water', 'bicycle_parking', 'fountain')
         OR tags->>'leisure' = 'picnic_table'
         OR tags->>'highway' = 'street_lamp'
         OR tags->>'barrier' = 'bollard'
         OR tags->>'advertising' = 'billboard')
    -- A recycling *centre* is a depot you drive to, not a container on the
    -- pavement; only recycling_type tells the two apart.
    AND COALESCE(tags->>'recycling_type', 'container') <> 'centre'
    -- Bike parking is drawn as a rack, so sheds, lockers and garages are left out.
    AND COALESCE(tags->>'bicycle_parking', 'stands') NOT IN ('building', 'shed', 'lockers', 'floor')
    AND COALESCE(tags->>'indoor', 'no') = 'no'
    AND COALESCE(tags->>'location', '') NOT IN ('indoor', 'underground')
),
facing AS (
  SELECT f.*,
         CASE
           WHEN f.direction IS NOT NULL THEN NULL
           WHEN f.kind IN ('bench', 'picnic_table', 'bicycle_parking', 'drinking_water') THEN 'path'
           WHEN f.kind IN ('street_lamp', 'billboard') THEN 'road'
         END as faces,
         CASE f.kind WHEN 'billboard' THEN 60 WHEN 'street_lamp' THEN 25 ELSE 14 END as reach
  FROM furniture f
)
SELECT f.fid, f.id, f.centroid, f.kind,
       COALESCE(f.direction, nearest.bearing::text, '') as direction
FROM facing f
LEFT JOIN LATERAL (
  SELECT (round(CASE
            WHEN m.dist < 0.5
              THEN degrees(ST_Azimuth(m.behind, m.ahead)) + CASE WHEN f.osm_id % 2 = 0 THEN 90 ELSE 270 END
            ELSE degrees(ST_Azimuth(g.p, m.closest))
          END)::int + 360) % 360 as bearing
  FROM (
    SELECT r.geom,
           r.tags->>'highway' IN ('motorway', 'motorway_link', 'trunk', 'trunk_link',
             'primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary',
             'tertiary_link', 'unclassified', 'residential', 'living_street', 'service',
             'road', 'busway') as carriageway
    FROM geo_places r
    WHERE f.faces IS NOT NULL
      AND r.geom_type = 'line' AND r.tags ? 'highway'
      AND r.tags->>'highway' NOT IN ('construction', 'proposed', 'abandoned', 'razed',
                                     'platform', 'corridor', 'elevator', 'raceway')
      AND ST_DWithin(r.geom, f.centroid, f.reach / 111320.0 / cos(radians(ST_Y(f.centroid))))
    ORDER BY r.geom <-> f.centroid
    LIMIT 8
  ) r
  -- Web Mercator is conformal, so bearings measured in it are true bearings.
  CROSS JOIN LATERAL (
    SELECT ST_Transform(r.geom, 3857) as line, ST_Transform(f.centroid, 3857) as p
  ) g
  CROSS JOIN LATERAL (
    SELECT ST_ClosestPoint(g.line, g.p) as closest,
           ST_Distance(g.line, g.p) * cos(radians(ST_Y(f.centroid))) as dist,
           ST_LineInterpolatePoint(g.line, greatest(ST_LineLocatePoint(g.line, g.p) - 1 / ST_Length(g.line), 0)) as behind,
           ST_LineInterpolatePoint(g.line, least(ST_LineLocatePoint(g.line, g.p) + 1 / ST_Length(g.line), 1)) as ahead
  ) m
  WHERE m.dist <= CASE WHEN f.faces = 'road' AND r.carriageway THEN f.reach ELSE 14 END
  ORDER BY (f.faces = 'road' AND NOT r.carriageway), m.dist
  LIMIT 1
) nearest ON true
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS street_furniture_fid_idx ON street_furniture (fid);
CREATE INDEX IF NOT EXISTS street_furniture_centroid_idx ON street_furniture USING GIST (centroid);

-- Lines a client draws as standing objects: fences, walls, hedges and guard
-- rails, power lines on their towers and poles, and the overhead wire over
-- electrified track. A closed barrier is stored as an area and comes through
-- as its polygon, so the client walks its ring; serving ST_Boundary instead
-- would hide the geometry from the tile envelope's index. `height` is the
-- tagged height in metres where there is one.
DROP VIEW IF EXISTS object_lines CASCADE;
CREATE VIEW object_lines AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, geom,
       CASE
         WHEN tags->>'power' = 'line' THEN 'power_line'
         WHEN tags->>'power' = 'minor_line' THEN 'power_minor_line'
         WHEN tags ? 'railway' THEN 'catenary'
         ELSE tags->>'barrier'
       END as kind,
       substring(tags->>'height' from '^[0-9]+(?:\.[0-9]+)?')::real as height
FROM geo_places
WHERE geom_type IN ('line', 'area')
  AND (tags->>'barrier' IN ('fence', 'wall', 'retaining_wall', 'hedge', 'guard_rail', 'city_wall')
       OR (geom_type = 'line' AND tags->>'power' IN ('line', 'minor_line'))
       OR (geom_type = 'line' AND tags->>'electrified' = 'contact_line'
           AND tags->>'railway' IN ('rail', 'light_rail', 'tram', 'narrow_gauge', 'subway')
           AND COALESCE(tags->>'tunnel', 'no') = 'no'));

-- Roller coaster tracks: `roller_coaster=track`, and the older
-- `railway=roller_coaster` that some parks still carry.
--
-- A closed track is stored as an area, since the import turns every closed way
-- into a polygon unless it says area=no, and nobody tags a coaster that way. It
-- is served as its ring, so the layer is lines all the way through and a
-- client draws the rails rather than a filled loop.
--
-- `id` is served because a 3D landmark lists the track ways it stands in for
-- among its `replaces`, and a client hides exactly those, matched on `id`, the
-- way it hides buildings.
--
-- Driven off the tags GIN index, not the spatial one. `@>` is what
-- `jsonb_path_ops` can answer, where `tags->>'roller_coaster' = 'track'` would
-- leave the planner nothing but the envelope: every feature in the tile, read
-- and thrown away. There are only a few hundred tracks in a continent, so
-- fetching them all and dropping those outside the tile is cheaper than any
-- spatial index, and needs no new one. It also sidesteps the CASE below, which
-- no index on `geom` could answer anyway. A Coney Island z14 tile: 7 ms on
-- production, 11 features, 895 bytes.
DROP VIEW IF EXISTS coaster_tracks CASCADE;
CREATE VIEW coaster_tracks AS
SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
       id, name,
       CASE WHEN geom_type = 'area' THEN ST_Boundary(geom) ELSE geom END as geom,
       -- Both spellings, NULL where there is none, as in buildings_3d below.
       NULLIF(COALESCE(tags->>'colour', tags->>'color', ''), '') as colour,
       -- Which stretch passes over which, for draw order. A small integer or
       -- nothing: a pattern match rather than a cast, so a stray "1;2" drops
       -- the key instead of failing the tile.
       substring(tags->>'layer' from '^\s*([+-]?[0-9]{1,2})\s*$')::smallint as layer
FROM geo_places
WHERE geom_type IN ('line', 'area')
  AND (tags @> '{"roller_coaster": "track"}' OR tags @> '{"railway": "roller_coaster"}');

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
--
-- Order matters on a deployment that does not have this view yet. Reading a
-- materialized view that has never been refreshed raises rather than returning
-- no rows, so between this statement and the first refresh the view exists and
-- is unreadable — and Martin validates every source at startup. Create it,
-- refresh it, and only then let Martin see it; `--on-invalid warn` in
-- docker-compose.yml is what keeps a mistake here from taking the whole tile
-- server down rather than just this layer.
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
-- *Which parts belong to this outline.* S3DB says a part belongs to the
-- outline it is inside, so this is containment — but not a bare
-- `ST_Contains`, because a part clipped by an import boundary or drawn a
-- fraction outside its own outline would then belong to nothing. 99% of the
-- part's area is OSM2World's threshold: generous enough for hand-drawn
-- geometry, tight enough that a row house does not adopt its neighbour's
-- parts through a shared wall.
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
-- the remainder: an entrance node added to the part but not to the outline
-- leaves a sliver of building behind, and OSM2World dropped subtraction for
-- exactly that reason. Whole or nothing has no slivers.
--
-- The union is clipped to the outline before measuring, since parts may
-- overlap each other and a plain sum would read as covered when it is not.
--
-- Both sides read `geo_places` directly rather than sharing the `shapes` CTE
-- below, and that is load-bearing rather than repetition. A CTE referenced
-- more than once is materialised, a materialised CTE carries no indexes, and
-- without an index the planner cannot turn `&&` into an index probe — it falls
-- back to a nested loop that evaluates the spatial predicate over every pair.
-- Sharing `shapes` here planned at cost 9e14 over ~36 trillion pairs and ran
-- 40 minutes on production without writing a row before it was killed. Driven
-- off `parts` against the GIST index on `geo_places.geom` it is one scan and
-- a few hundred thousand index probes.
parts AS MATERIALIZED (
  SELECT id, geom
  FROM geo_places
  WHERE geom_type = 'area'
    AND COALESCE(tags->>'location', '') <> 'underground'
    AND COALESCE(tags->>'building:part', 'no') <> 'no'
),
covered AS (
  SELECT o.id as id,
         ST_Area(ST_Intersection(ST_Union(p.geom), o.geom)) as covered_area,
         ST_Area(o.geom) as outline_area
  FROM parts p
  JOIN geo_places o
    ON o.geom && p.geom
   AND o.geom_type = 'area'
   AND COALESCE(o.tags->>'location', '') <> 'underground'
   AND COALESCE(o.tags->>'building', 'no') <> 'no'
   AND COALESCE(o.tags->>'building:part', 'no') = 'no'
   -- ST_Intersection is the expensive half, so it only runs for the parts a
   -- plain containment misses.
   AND (ST_Contains(o.geom, p.geom)
        OR ST_Area(ST_Intersection(o.geom, p.geom)) >= 0.99 * ST_Area(p.geom))
  GROUP BY o.id, o.geom
),
-- The outline each part belongs to, by the same containment rule, so the
-- client can treat a part-mapped building as one building — one colour, not a
-- patchwork. The smallest containing outline wins where outlines nest.
owner AS (
  SELECT DISTINCT ON (p.id) p.id as part_id,
         (o.osm_id * 4 + CASE o.osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as outline_fid
  FROM parts p
  JOIN geo_places o
    ON o.geom && p.geom
   AND o.geom_type = 'area'
   AND COALESCE(o.tags->>'location', '') <> 'underground'
   AND COALESCE(o.tags->>'building', 'no') <> 'no'
   AND COALESCE(o.tags->>'building:part', 'no') = 'no'
   AND (ST_Contains(o.geom, p.geom)
        OR ST_Area(ST_Intersection(o.geom, p.geom)) >= 0.99 * ST_Area(p.geom))
  ORDER BY p.id, ST_Area(o.geom)
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
       CASE WHEN c.covered_area >= 0.9 * c.outline_area THEN true END as hide_3d,
       -- The building this shape belongs to: its outline for a part, itself
       -- otherwise. Shared by every part of one building.
       COALESCE(w.outline_fid, m.fid) as group_id
FROM measured m
LEFT JOIN covered c ON c.id = m.id
LEFT JOIN owner w ON w.part_id = m.id
WITH NO DATA;

-- Unique on fid so the view can be refreshed CONCURRENTLY, and GIST on geom so
-- Martin's envelope filter is an index scan rather than a walk of every
-- building in the region.
-- Parts are 334k rows out of 27M, but the planner cannot estimate a jsonb
-- expression and guesses 12M — which is why, left alone, it refuses to drive
-- the join from this side and scans every outline instead. A partial index
-- over exactly the predicate both makes the scan cheap and gives ANALYZE
-- something honest to read, and it is small because it only covers the parts.
CREATE INDEX IF NOT EXISTS geo_places_building_part_geom_idx
  ON geo_places USING GIST (geom)
  WHERE COALESCE(tags->>'building:part', 'no') <> 'no';

CREATE UNIQUE INDEX IF NOT EXISTS buildings_3d_fid_idx ON buildings_3d (fid);
CREATE INDEX IF NOT EXISTS buildings_3d_geom_idx ON buildings_3d USING GIST (geom);

-- ─── Sport pitches ───────────────────────────────────────────────────────────
--
-- Every leisure=pitch as its surface, plus regulation markings and the nets,
-- hoops and goals that stand on them, fitted to the pitch's oriented bounding
-- box. One row per surface, one MultiLineString of markings, and a point per
-- prop; `kind` tells them apart. Props carry `direction` (compass degrees, as
-- street_furniture) and `width` in metres.
--
-- Layouts are drawn in local metres, x along the pitch's long axis and y across
-- it, centred on the origin, then rotated and placed by pitch_place.

CREATE OR REPLACE FUNCTION pitch_seg(x0 float8, y0 float8, x1 float8, y1 float8)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_MakeLine(ST_MakePoint(x0, y0), ST_MakePoint(x1, y1))
$$;

CREATE OR REPLACE FUNCTION pitch_rect(x0 float8, y0 float8, x1 float8, y1 float8)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_ExteriorRing(ST_MakeEnvelope(least(x0, x1), least(y0, y1), greatest(x0, x1), greatest(y0, y1)))
$$;

-- A circle's outline, optionally kept only inside a box.
CREATE OR REPLACE FUNCTION pitch_arc(x float8, y float8, r float8,
  x0 float8 DEFAULT NULL, y0 float8 DEFAULT NULL, x1 float8 DEFAULT NULL, y1 float8 DEFAULT NULL)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN x0 IS NULL THEN ring
              ELSE ST_Intersection(ring, ST_MakeEnvelope(least(x0, x1), least(y0, y1), greatest(x0, x1), greatest(y0, y1))) END
  FROM (SELECT ST_ExteriorRing(ST_Buffer(ST_MakePoint(x, y), r, 12)) AS ring) c
$$;

-- One court or field at regulation size in local metres. `prop` is NULL on
-- marking rows; on prop rows `facing` is degrees clockwise from local +x.
CREATE OR REPLACE FUNCTION pitch_template(sport text, len float8, wid float8, half boolean DEFAULT false)
RETURNS TABLE(prop text, geom geometry, facing float8, size float8)
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  s float8;
  hl float8;
  hw float8;
  side int;
BEGIN
  IF sport = 'tennis' THEN
    RETURN QUERY VALUES
      (NULL::text, pitch_rect(-11.885, -5.485, 11.885, 5.485), NULL::float8, NULL::float8),
      (NULL, pitch_seg(-11.885, -4.115, 11.885, -4.115), NULL, NULL),
      (NULL, pitch_seg(-11.885, 4.115, 11.885, 4.115), NULL, NULL),
      (NULL, pitch_seg(-6.4, -4.115, -6.4, 4.115), NULL, NULL),
      (NULL, pitch_seg(6.4, -4.115, 6.4, 4.115), NULL, NULL),
      (NULL, pitch_seg(-6.4, 0, 6.4, 0), NULL, NULL),
      (NULL, pitch_seg(-11.885, 0, -11.785, 0), NULL, NULL),
      (NULL, pitch_seg(11.885, 0, 11.785, 0), NULL, NULL),
      ('tennis-net', ST_MakePoint(0, 0), 0::float8, 12.8::float8);
  ELSIF sport = 'pickleball' THEN
    RETURN QUERY VALUES
      (NULL::text, pitch_rect(-6.705, -3.05, 6.705, 3.05), NULL::float8, NULL::float8),
      (NULL, pitch_seg(-2.13, -3.05, -2.13, 3.05), NULL, NULL),
      (NULL, pitch_seg(2.13, -3.05, 2.13, 3.05), NULL, NULL),
      (NULL, pitch_seg(-6.705, 0, -2.13, 0), NULL, NULL),
      (NULL, pitch_seg(2.13, 0, 6.705, 0), NULL, NULL),
      ('pickleball-net', ST_MakePoint(0, 0), 0::float8, 6.7::float8);
  ELSIF sport = 'volleyball' THEN
    RETURN QUERY VALUES
      (NULL::text, pitch_rect(-9, -4.5, 9, 4.5), NULL::float8, NULL::float8),
      (NULL, pitch_seg(0, -4.5, 0, 4.5), NULL, NULL),
      (NULL, pitch_seg(-3, -4.5, -3, 4.5), NULL, NULL),
      (NULL, pitch_seg(3, -4.5, 3, 4.5), NULL, NULL),
      ('volleyball-net', ST_MakePoint(0, 0), 0::float8, 10::float8);
  ELSIF sport = 'beachvolleyball' OR sport = 'beach_volleyball' THEN
    RETURN QUERY VALUES
      (NULL::text, pitch_rect(-8, -4, 8, 4), NULL::float8, NULL::float8),
      ('volleyball-net', ST_MakePoint(0, 0), 0::float8, 9.5::float8);
  ELSIF sport = 'basketball' THEN
    -- A half court is 14 m long, its basket at +x and the centre circle cut
    -- by the half-court line at -x.
    hl := CASE WHEN half THEN 7 ELSE 14 END;
    RETURN QUERY VALUES
      (NULL::text, pitch_rect(-hl, -7.5, hl, 7.5), NULL::float8, NULL::float8),
      (NULL, CASE WHEN half THEN pitch_arc(-7, 0, 1.8, -7, -2, -5, 2) ELSE pitch_seg(0, -7.5, 0, 7.5) END, NULL, NULL);
    IF NOT half THEN
      RETURN QUERY VALUES (NULL::text, pitch_arc(0, 0, 1.8), NULL::float8, NULL::float8);
    END IF;
    FOREACH side IN ARRAY CASE WHEN half THEN ARRAY[1] ELSE ARRAY[-1, 1] END LOOP
      RETURN QUERY VALUES
        (NULL::text, pitch_rect(side * hl, -2.45, side * (hl - 5.8), 2.45), NULL::float8, NULL::float8),
        (NULL, pitch_arc(side * (hl - 5.8), 0, 1.8), NULL, NULL),
        (NULL, pitch_arc(side * (hl - 1.575), 0, 6.75, side * (hl - 1.575 - 6.75), -6.6, side * (hl - 1.575 - 1.416), 6.6), NULL, NULL),
        (NULL, pitch_seg(side * hl, -6.6, side * (hl - 2.991), -6.6), NULL, NULL),
        (NULL, pitch_seg(side * hl, 6.6, side * (hl - 2.991), 6.6), NULL, NULL),
        ('basketball-hoop', ST_MakePoint(side * (hl - 1.2), 0), CASE WHEN side = 1 THEN 180 ELSE 0 END::float8, NULL::float8);
    END LOOP;
  ELSIF sport = 'soccer' THEN
    -- Fields vary, so the boundary is the pitch itself and the boxes and
    -- circles scale with it, capped at regulation size.
    hl := len / 2;
    hw := wid / 2;
    s := least(len / 105, wid / 68, 1);
    RETURN QUERY VALUES
      (NULL::text, pitch_rect(-hl, -hw, hl, hw), NULL::float8, NULL::float8),
      (NULL, pitch_seg(0, -hw, 0, hw), NULL, NULL),
      (NULL, pitch_arc(0, 0, 9.15 * s), NULL, NULL);
    FOREACH side IN ARRAY ARRAY[-1, 1] LOOP
      RETURN QUERY VALUES
        (NULL::text, pitch_rect(side * hl, -20.16 * s, side * (hl - 16.5 * s), 20.16 * s), NULL::float8, NULL::float8),
        (NULL, pitch_rect(side * hl, -9.16 * s, side * (hl - 5.5 * s), 9.16 * s), NULL, NULL),
        (NULL, pitch_arc(side * (hl - 11 * s), 0, 9.15 * s, side * (hl - 16.5 * s), -hw, -side * hl, hw), NULL, NULL),
        ('soccer-goal', ST_MakePoint(side * hl, 0), CASE WHEN side = 1 THEN 180 ELSE 0 END::float8, 7.32 * s);
    END LOOP;
  ELSIF sport = 'american_football' THEN
    hl := len / 2;
    hw := wid / 2;
    s := len / 109.73;
    RETURN QUERY
      SELECT NULL::text, pitch_rect(-hl, -hw, hl, hw), NULL::float8, NULL::float8
      UNION ALL
      SELECT NULL, pitch_seg(i * 4.572 * s, -hw, i * 4.572 * s, hw), NULL, NULL FROM generate_series(-10, 10) i
      UNION ALL
      SELECT 'football-goalpost', ST_MakePoint(side_ * hl, 0), CASE WHEN side_ = 1 THEN 180 ELSE 0 END::float8, 5.64::float8
      FROM unnest(ARRAY[-1, 1]) side_;
  END IF;
END
$$;

-- Courts repeat across a pitch mapped as a block of them; fields fill it.
-- Each court is regulation size, shrunk only if its share of the pitch is
-- smaller, and nothing is drawn below 60% of regulation.
CREATE OR REPLACE FUNCTION pitch_layout(sport text, len float8, wid float8)
RETURNS TABLE(prop text, geom geometry, facing float8, size float8)
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  court_len float8;
  court_wid float8;
  cell_len float8;
  cell_wid float8;
  nx int;
  ny int;
  s float8;
  half boolean := false;
BEGIN
  IF sport IN ('soccer', 'american_football') THEN
    IF (sport = 'soccer' AND len BETWEEN 40 AND 130 AND wid BETWEEN 25 AND 100)
       OR (sport = 'american_football' AND len BETWEEN 80 AND 125 AND wid BETWEEN 40 AND 60) THEN
      RETURN QUERY SELECT * FROM pitch_template(sport, len, wid);
    END IF;
    RETURN;
  END IF;

  SELECT c.l, c.w, c.cl, c.cw INTO court_len, court_wid, cell_len, cell_wid
  FROM (VALUES
    ('tennis', 23.77, 10.97, 35.0, 16.5),
    ('pickleball', 13.41, 6.1, 18.0, 9.0),
    ('basketball', 28.0, 15.0, 30.0, 17.0),
    ('volleyball', 18.0, 9.0, 20.0, 11.0),
    ('beachvolleyball', 16.0, 8.0, 22.0, 13.0),
    ('beach_volleyball', 16.0, 8.0, 22.0, 13.0)
  ) c(sport, l, w, cl, cw)
  WHERE c.sport = pitch_layout.sport;
  IF court_len IS NULL THEN
    RETURN;
  END IF;

  nx := least(6, greatest(1, floor(len / cell_len)::int));
  ny := least(6, greatest(1, floor(wid / cell_wid)::int));
  IF sport = 'basketball' AND nx * ny = 1 AND len < 20 THEN
    half := true;
    court_len := 14;
  END IF;
  s := least(1, (len / nx) / court_len, (wid / ny) / court_wid);
  IF s < 0.6 THEN
    RETURN;
  END IF;

  RETURN QUERY
    SELECT t.prop,
           ST_Affine(t.geom, s, 0, 0, s, (i - (nx - 1) / 2.0) * len / nx, (j - (ny - 1) / 2.0) * wid / ny),
           t.facing,
           t.size * s
    FROM generate_series(0, nx - 1) i
    CROSS JOIN generate_series(0, ny - 1) j
    CROSS JOIN pitch_template(sport, len, wid, half) t;
END
$$;

-- Local metres onto a pitch: x along `azimuth` (radians clockwise from north),
-- scaled into Web Mercator, which is conformal, so angles come through true.
CREATE OR REPLACE FUNCTION pitch_place(local geometry, center geometry, azimuth float8)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_Transform(ST_SetSRID(ST_Affine(local,
           s * sin(azimuth), -s * cos(azimuth),
           s * cos(azimuth), s * sin(azimuth),
           ST_X(center), ST_Y(center)), 3857), 4326)
  FROM (SELECT 1 / cos(radians(ST_Y(ST_Transform(center, 4326)))) AS s) k
$$;

-- MATERIALIZED like buildings_3d: the layout is computed per pitch, and its
-- output geometry is not one Martin's tile envelope can find in geo_places.
CREATE MATERIALIZED VIEW IF NOT EXISTS sport_pitches AS
WITH pitch AS MATERIALIZED (
  SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) as fid,
         id, geom,
         NULLIF(lower(trim(split_part(tags->>'sport', ';', 1))), '') as sport,
         NULLIF(tags->>'surface', '') as surface
  FROM geo_places
  WHERE geom_type = 'area' AND tags->>'leisure' = 'pitch'
),
fitted AS (
  SELECT p.*, f.*
  FROM pitch p
  CROSS JOIN LATERAL (
    SELECT ST_Transform(p.geom, 3857) as merc,
           ST_OrientedEnvelope(ST_Transform(p.geom, 3857)) as box,
           cos(radians(ST_Y(ST_Centroid(p.geom)))) as k
  ) b
  CROSS JOIN LATERAL (
    SELECT ST_Centroid(b.box) as center,
           greatest(e1, e2) as len,
           least(e1, e2) as wid,
           CASE WHEN e1 >= e2 THEN ST_Azimuth(c1, c2) ELSE ST_Azimuth(c2, c3) END as azimuth,
           ST_Area(b.merc) / nullif(ST_Area(b.box), 0) as fill
    FROM (
      SELECT ST_PointN(ST_ExteriorRing(b.box), 1) as c1,
             ST_PointN(ST_ExteriorRing(b.box), 2) as c2,
             ST_PointN(ST_ExteriorRing(b.box), 3) as c3
    ) c
    CROSS JOIN LATERAL (
      SELECT ST_Distance(c.c1, c.c2) * b.k as e1, ST_Distance(c.c2, c.c3) * b.k as e2
    ) e
  ) f
  WHERE GeometryType(b.box) = 'POLYGON'
),
-- Only near-rectangular pitches get markings; a baseball diamond or a park
-- lawn tagged as a pitch keeps just its surface. So does a pitch drawn around
-- other pitches, a block of courts mapped both as one and one by one.
layout AS (
  SELECT f.fid, f.id, f.sport, f.center, f.azimuth, l.prop, l.geom, l.facing, l.size,
         row_number() OVER (PARTITION BY f.fid ORDER BY l.prop NULLS FIRST) as n
  FROM fitted f
  CROSS JOIN LATERAL pitch_layout(f.sport, f.len, f.wid) l
  WHERE f.fill >= 0.8
    AND NOT EXISTS (
      SELECT 1 FROM geo_places q
      WHERE q.geom_type = 'area' AND q.tags->>'leisure' = 'pitch'
        AND q.geom && f.geom AND q.id <> f.id
        AND ST_Contains(f.geom, ST_PointOnSurface(q.geom))
    )
)
SELECT fid * 64 as fid, id, 'surface' as kind, sport, surface,
       NULL::text as direction, NULL::real as width, geom
FROM pitch
UNION ALL
SELECT fid * 64 + 1, id, 'lines', sport, NULL, NULL, NULL,
       pitch_place(ST_CollectionExtract(ST_Collect(geom), 2), min(center), min(azimuth))
FROM layout
WHERE prop IS NULL
GROUP BY fid, id, sport
UNION ALL
SELECT fid * 64 + 1 + n, id, prop, sport, NULL,
       ((round(degrees(azimuth) + facing)::int % 360 + 360) % 360)::text,
       size::real,
       pitch_place(geom, center, azimuth)
FROM layout
WHERE prop IS NOT NULL AND n < 63
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS sport_pitches_fid_idx ON sport_pitches (fid);
CREATE INDEX IF NOT EXISTS sport_pitches_geom_idx ON sport_pitches USING GIST (geom);
