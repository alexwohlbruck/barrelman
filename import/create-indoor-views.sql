-- ─── Indoor maps ─────────────────────────────────────────────────────────────
--
-- Building and station interiors from Simple Indoor Tagging, one row per
-- feature per level it stands on, so a client shows a floor with a single
-- `level == n` filter. Rows come in these kinds:
--
--   venue   one per indoor complex (a terminal, a mall, a station's concourses):
--           its outline, `levels` ("-1;0;1"), `level_names` ("-1=B1;0=G"),
--           `default_level`, and `buildings`, the buildings_3d ids it stands in
--   floor   the walkable outline of one level of a venue
--   wall    walls with real thickness, door openings cut out, per level
--   area    rooms, areas, corridors, platforms, columns, stairs and lift shafts
--   line    steps, escalators and indoor footways
--   poi     shops, amenities, gates, doors, lifts, and named rooms' labels
--
-- Every row carries `venue` and the venue's `default_level`, so a style can
-- show one venue at a chosen level and every other at its own default.
--
-- Stored, not computed per tile: walls and floors union whole levels, which
-- is too slow for a tile request. Refreshed after each OSM update like
-- buildings_3d (scripts/refresh-view.sh). Changing the SELECT needs the view
-- dropped first; the version comment below does that.

-- `level` and `repeat_on` values as a sorted list of levels. Handles lists
-- ("0;1"), ranges ("0-2", "-4--2"), decimals ("-0.5") and any order. Junk is
-- skipped rather than failing the row, as is anything past 200 floors either way.
CREATE OR REPLACE FUNCTION indoor_levels(spec text) RETURNS real[]
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  part text;
  m text[];
  lo real;
  hi real;
  found real[] := '{}';
BEGIN
  IF spec IS NULL THEN RETURN NULL; END IF;
  FOREACH part IN ARRAY string_to_array(replace(spec, ' ', ''), ';') LOOP
    m := regexp_match(part, '^(-?\d{1,3}(?:\.\d+)?)(?:-(-?\d{1,3}(?:\.\d+)?))?$');
    CONTINUE WHEN m IS NULL OR abs(m[1]::real) > 200 OR abs(COALESCE(m[2], m[1])::real) > 200;
    lo := least(m[1]::real, COALESCE(m[2], m[1])::real);
    hi := greatest(m[1]::real, COALESCE(m[2], m[1])::real);
    found := found || lo || hi;
    IF hi - lo <= 40 THEN
      FOR i IN ceil(lo)::int .. floor(hi)::int LOOP
        found := found || i::real;
      END LOOP;
    END IF;
  END LOOP;
  RETURN (SELECT array_agg(DISTINCT v ORDER BY v) FROM unnest(found) v);
END $$;

-- A buffer of `metres`, measured on the ground at the shape's latitude.
CREATE OR REPLACE FUNCTION indoor_buffer(g geometry, metres float8, style text DEFAULT 'join=mitre mitre_limit=2')
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_Transform(
           ST_Buffer(ST_Transform(g, 3857), metres / cos(radians(ST_Y(ST_Centroid(g)))), style),
           4326)
$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'indoor_features' AND relkind = 'm')
    AND COALESCE(obj_description(to_regclass('indoor_features'), 'pg_class'), '') <> 'v7' THEN
    DROP MATERIALIZED VIEW indoor_features CASCADE;
  END IF;
END $$;

CREATE MATERIALIZED VIEW IF NOT EXISTS indoor_features AS
WITH tagged AS MATERIALIZED (
  SELECT (osm_id * 4 + CASE osm_type WHEN 'N' THEN 0 WHEN 'W' THEN 1 ELSE 2 END) AS osm_fid,
         id, name, tags, geom_type,
         CASE WHEN geom_type = 'area' THEN ST_MakeValid(geom) ELSE geom END AS geom,
         NULLIF(tags->>'indoor', '') AS indoor,
         COALESCE(tags->>'public_transport' = 'platform' OR tags->>'railway' = 'platform', false) AS platform
  FROM geo_places
  WHERE (tags ? 'level' OR tags ? 'repeat_on' OR tags ? 'indoor'
         OR tags->>'railway' = 'platform' OR tags->>'public_transport' = 'platform'
         OR tags->>'aeroway' = 'gate')
    AND COALESCE(tags->>'indoor', '') <> 'no'
    AND NOT (tags ? 'building' AND NOT tags ? 'indoor')
),
-- What each shape is to a floor plan. Platforms count only where they are
-- indoors or underground; an open-air platform is the basemap's to draw.
classed AS (
  SELECT t.*,
         CASE
           WHEN geom_type = 'area' AND (indoor = 'wall') THEN 'wall'
           WHEN geom_type = 'area' AND platform THEN 'platform'
           WHEN geom_type = 'area' AND (tags->>'highway' = 'elevator' OR tags->>'room' IN ('elevator', 'lift')) THEN 'elevator'
           WHEN geom_type = 'area' AND (tags->>'stairs' = 'yes' OR tags->>'room' = 'stairs') THEN 'stairs'
           WHEN geom_type = 'area' AND indoor IN ('room', 'area', 'corridor', 'level', 'column') THEN indoor
           WHEN geom_type = 'area' AND indoor IS NOT NULL THEN 'area'
           WHEN geom_type = 'line' AND indoor = 'wall' THEN 'wall'
           WHEN geom_type = 'line' AND tags->>'highway' = 'steps' THEN
             CASE WHEN COALESCE(tags->>'conveying', 'no') <> 'no' THEN 'escalator' ELSE 'steps' END
           WHEN geom_type = 'line' AND platform THEN 'platform'
           WHEN geom_type = 'line' AND tags->>'highway' IN ('footway', 'corridor', 'path', 'pedestrian')
                AND (indoor IS NOT NULL OR tags->>'highway' = 'corridor') THEN 'corridor'
           WHEN geom_type = 'point' AND tags->>'highway' = 'elevator' THEN 'elevator'
           WHEN geom_type = 'point' AND (indoor = 'door' OR tags ? 'door' OR tags ? 'entrance') THEN 'door'
           WHEN geom_type = 'point' AND (tags ? 'shop' OR tags ? 'amenity' OR tags ? 'tourism' OR tags ? 'office'
                OR tags ? 'healthcare' OR tags ? 'craft' OR tags->>'aeroway' = 'gate' OR tags ? 'leisure'
                OR tags->>'railway' IN ('subway_entrance', 'train_station_entrance', 'ticket_validator')) THEN 'poi'
         END AS class,
         COALESCE(
           indoor_levels(tags->>'level'),
           -- A platform or passage with only `layer`: usually underground, one storey per layer.
           CASE WHEN (platform OR indoor IS NOT NULL) AND (tags->>'layer') ~ '^-[1-9]$' THEN indoor_levels(tags->>'layer') END,
           CASE WHEN indoor IN ('room', 'area', 'corridor', 'wall', 'column', 'level') THEN ARRAY[0::real] END
         ) AS own_levels
  FROM tagged t
),
leveled AS (
  SELECT c.*,
         (SELECT array_agg(DISTINCT v ORDER BY v)
          FROM unnest(c.own_levels || COALESCE(indoor_levels(c.tags->>'repeat_on'), '{}')) v) AS levels
  FROM classed c
  -- A gate is rarely given a level; it goes on its terminal's default one.
  WHERE class IS NOT NULL AND (own_levels IS NOT NULL OR (geom_type = 'point' AND tags->>'aeroway' = 'gate'))
    AND (class <> 'platform' OR indoor IS NOT NULL OR tags->>'level' ~ '^-|;-'
         OR (tags->>'layer') ~ '^-' OR tags->>'location' = 'underground' OR tags->>'tunnel' IS NOT NULL)
),
-- The bones of a venue: everything but its points, which join a venue only if
-- they stand inside one.
structure AS (
  SELECT * FROM leveled WHERE geom_type <> 'point'
),
-- Shapes within ~25 m of each other belong to one venue. Partitioned on a
-- coarse grid so the clustering never sorts the planet in one window; a venue
-- straddling a grid line splits in two, which costs only a second outline.
clustered AS (
  SELECT s.*,
         ST_ClusterDBSCAN(geom, 0.00025, 1) OVER (
           PARTITION BY floor(ST_X(ST_Centroid(geom)) * 4), floor(ST_Y(ST_Centroid(geom)) * 4)
         ) AS cluster,
         floor(ST_X(ST_Centroid(geom)) * 4)::int AS cell_x,
         floor(ST_Y(ST_Centroid(geom)) * 4)::int AS cell_y
  FROM structure s
),
venue_ids AS (
  SELECT cell_x, cell_y, cluster, min(osm_fid) AS venue
  FROM clustered
  GROUP BY cell_x, cell_y, cluster
  -- A lone indoor=yes passage is not a building to step into.
  HAVING count(*) FILTER (WHERE geom_type = 'area' AND class IN ('room', 'area', 'corridor', 'platform', 'level')) >= 3
),
members AS (
  SELECT c.*, v.venue
  FROM clustered c
  JOIN venue_ids v USING (cell_x, cell_y, cluster)
),
venue_shapes AS (
  SELECT venue, ST_ConvexHull(ST_Collect(geom)) AS hull
  FROM members
  GROUP BY venue
),
-- Buildings a venue stands in: any above-ground outline whose footprint its
-- rooms at ground level or above cover a fair share of. Not the venue's
-- outline, which for a station runs under whole blocks of towers. A client
-- lowers these so the floor plan shows instead.
above_ground AS MATERIALIZED (
  SELECT venue, ST_Union(geom) AS geom
  FROM members
  WHERE geom_type = 'area' AND class IN ('room', 'area', 'corridor', 'level', 'stairs', 'elevator')
    AND levels[array_length(levels, 1)] >= 0
  GROUP BY venue
),
venue_buildings AS MATERIALIZED (
  SELECT ag.venue,
         string_agg(DISTINCT b.id, ';') AS buildings,
         (array_agg(b.name ORDER BY ST_Area(b.geom) DESC) FILTER (WHERE b.name IS NOT NULL))[1] AS building_name
  FROM above_ground ag
  JOIN geo_places b
    ON b.geom_type = 'area' AND b.geom && ag.geom
   AND COALESCE(b.tags->>'building', 'no') <> 'no'
   AND COALESCE(b.tags->>'location', '') <> 'underground'
   AND ST_Area(ST_Intersection(ST_MakeValid(b.geom), ag.geom)) > 0.3 * ST_Area(b.geom)
  GROUP BY ag.venue
),
-- A station's name for a venue with no building: a station complex underground.
venue_stations AS MATERIALIZED (
  SELECT DISTINCT ON (vs.venue) vs.venue, s.name
  FROM venue_shapes vs
  JOIN geo_places s
    ON s.centroid && ST_Expand(vs.hull, 0.002) AND s.name IS NOT NULL
   AND (s.tags->>'public_transport' = 'station' OR s.tags->>'railway' = 'station')
  ORDER BY vs.venue, ST_Distance(s.centroid, vs.hull)
),
member_levels AS (
  SELECT m.*, l AS level
  FROM members m, unnest(m.levels) WITH ORDINALITY AS u(l, n)
  WHERE n <= 64
),
-- What each level is called on the signs, by the commonest `level:ref` (or an
-- indoor=level outline's name) among single-level shapes on it.
level_names AS MATERIALIZED (
  SELECT DISTINCT ON (venue, level) venue, level, label
  FROM (
    SELECT venue, level, COALESCE(NULLIF(tags->>'level:ref', ''), CASE WHEN class = 'level' THEN name END) AS label
    FROM member_levels
    WHERE array_length(levels, 1) = 1
  ) named
  WHERE label IS NOT NULL AND length(label) <= 12
  GROUP BY venue, level, label
  ORDER BY venue, level, count(*) DESC
),
venue_levels AS (
  SELECT ml.venue,
         array_agg(DISTINCT ml.level ORDER BY ml.level) AS levels,
         -- Ground if it has one, else the busiest level: a station that is all
         -- below ground opens on its concourse rather than on nothing.
         COALESCE(
           max(ml.level) FILTER (WHERE ml.level = 0),
           mode() WITHIN GROUP (ORDER BY ml.level)
         ) AS default_level
  FROM member_levels ml
  WHERE ml.geom_type = 'area' AND ml.class IN ('room', 'area', 'corridor', 'platform', 'level')
  GROUP BY ml.venue
),
venues AS MATERIALIZED (
  SELECT vl.venue, row_number() OVER (ORDER BY vl.venue) AS ord, vl.levels, vl.default_level, vb.buildings,
         COALESCE(vb.building_name, st.name) AS name,
         vs.hull,
         (SELECT string_agg(ln.level::text || '=' || ln.label, ';' ORDER BY ln.level)
          FROM level_names ln WHERE ln.venue = vl.venue) AS level_names
  FROM venue_levels vl
  JOIN venue_shapes vs USING (venue)
  LEFT JOIN venue_buildings vb USING (venue)
  LEFT JOIN venue_stations st USING (venue)
),
-- Each level's walkable outline. Above ground it also covers the buildings the
-- venue stands in, so a sparsely mapped floor still reads as the building.
floors AS (
  SELECT ml.venue, ml.level,
         ST_Union(indoor_buffer(ml.geom, 0.25)) AS geom
  FROM member_levels ml
  WHERE ml.geom_type = 'area' AND ml.class IN ('room', 'area', 'corridor', 'platform', 'level', 'stairs', 'elevator')
  GROUP BY ml.venue, ml.level
),
floors_with_buildings AS (
  SELECT f.venue, f.level,
         CASE WHEN f.level >= 0 AND v.buildings IS NOT NULL THEN
           ST_Union(f.geom, (SELECT ST_Union(ST_MakeValid(b.geom)) FROM geo_places b
                             WHERE b.id = ANY(string_to_array(v.buildings, ';'))))
         ELSE f.geom END AS geom
  FROM floors f JOIN venues v USING (venue)
),
-- Doors, as the gaps they open in whatever wall they sit on.
doors AS (
  SELECT l.osm_fid, l.levels, v.venue, l.tags, l.geom, l.class, l.name, l.id, l.geom_type, lvl AS level
  FROM leveled l
  JOIN venue_shapes v ON l.geom && v.hull AND ST_Intersects(l.geom, v.hull)
  CROSS JOIN LATERAL unnest(l.levels) lvl
  WHERE l.geom_type = 'point' AND l.class = 'door'
),
gaps AS (
  SELECT venue, level,
         ST_Union(indoor_buffer(geom,
           least(greatest(COALESCE(substring(tags->>'width' from '^[0-9]+(?:\.[0-9]+)?')::float8, 0.9), 0.7), 4) / 2,
           'quad_segs=4')) AS geom
  FROM doors
  GROUP BY venue, level
),
-- Walls: every room's outline, every mapped wall, and each floor's edge, as
-- one shape per level so crossing walls merge instead of overlapping. Drawn
-- thicker than built (60 cm inside, a metre outside) so a wall still reads at
-- the zooms a plan is looked at.
wall_lines AS (
  SELECT venue, level, ST_Boundary(geom) AS geom, 0.3 AS half
  FROM member_levels
  WHERE geom_type = 'area' AND class IN ('room', 'wall', 'stairs', 'elevator')
  UNION ALL
  SELECT venue, level, geom, 0.3 FROM member_levels WHERE geom_type = 'line' AND class = 'wall'
  UNION ALL
  SELECT venue, level, ST_Boundary(geom), 0.5 FROM floors_with_buildings
),
walls AS (
  SELECT w.venue, w.level,
         ST_Difference(ST_Union(indoor_buffer(w.geom, w.half, 'endcap=flat join=mitre mitre_limit=2')),
                       COALESCE(g.geom, ST_SetSRID('GEOMETRYCOLLECTION EMPTY'::geometry, 4326))) AS geom
  FROM wall_lines w
  LEFT JOIN gaps g USING (venue, level)
  GROUP BY w.venue, w.level, g.geom
),
-- A point inside two venues' outlines belongs to the smaller, so it is drawn once.
points AS (
  SELECT l.*, own.venue, lvl AS level
  FROM leveled l
  CROSS JOIN LATERAL (
    SELECT v.venue FROM venue_shapes v
    WHERE l.geom && v.hull AND ST_Intersects(l.geom, v.hull)
    ORDER BY ST_Area(v.hull), v.venue LIMIT 1
  ) own
  JOIN venue_levels vl ON vl.venue = own.venue
  CROSS JOIN LATERAL unnest(COALESCE(l.levels, ARRAY[vl.default_level])) WITH ORDINALITY AS u(lvl, n)
  WHERE l.geom_type = 'point' AND n <= 64
),
-- A point's kind of place, for its icon: the first of these keys it carries.
poi_kinds AS (
  SELECT p.*,
         CASE
           WHEN p.tags ? 'aeroway' THEN 'aeroway'
           WHEN p.tags ? 'railway' THEN 'railway'
           WHEN p.tags ? 'shop' THEN 'shop'
           WHEN p.tags ? 'amenity' THEN 'amenity'
           WHEN p.tags ? 'tourism' THEN 'tourism'
           WHEN p.tags ? 'healthcare' THEN 'healthcare'
           WHEN p.tags ? 'leisure' THEN 'leisure'
           WHEN p.tags ? 'office' THEN 'office'
           WHEN p.tags ? 'craft' THEN 'craft'
         END AS poi_key
  FROM points p
),
-- Feature ids: even for a shape's own rows (its id, level and whether it is a
-- room's label), odd for what is derived per venue. A tile id cannot be negative.
rows AS MATERIALIZED (
  SELECT (v.ord * 1048576) * 2 + 1 AS fid, v.venue, NULL::real AS level, 'venue' AS kind, NULL AS class, NULL AS subclass,
         v.name, NULL AS ref, NULL AS id, v.hull AS geom,
         array_to_string(v.levels, ';') AS levels, v.level_names, v.buildings
  FROM venues v
  UNION ALL
  SELECT (v.ord * 1048576 + 262144 + round(f.level * 100)::int) * 2 + 1, f.venue, f.level, 'floor', NULL, NULL,
         NULL, NULL, NULL, f.geom, NULL, NULL, NULL
  FROM floors_with_buildings f JOIN venues v USING (venue)
  UNION ALL
  SELECT (v.ord * 1048576 + 786432 + round(w.level * 100)::int) * 2 + 1, w.venue, w.level, 'wall', NULL, NULL,
         NULL, NULL, NULL, w.geom, NULL, NULL, NULL
  FROM walls w JOIN venues v USING (venue)
  WHERE NOT ST_IsEmpty(w.geom)
  UNION ALL
  SELECT (ml.osm_fid * 256 + n * 2) * 2, ml.venue, ml.level,
         CASE WHEN ml.geom_type = 'area' THEN 'area' ELSE 'line' END,
         ml.class,
         CASE ml.class
           WHEN 'escalator' THEN ml.tags->>'conveying'
           -- What a room is for, for its tint: any shop is just `shop`.
           WHEN 'room' THEN COALESCE(CASE WHEN ml.tags ? 'shop' THEN 'shop' END, ml.tags->>'amenity', ml.tags->>'room')
         END,
         ml.name, ml.tags->>'ref', ml.id, ml.geom, NULL, NULL, NULL
  FROM member_levels ml
  CROSS JOIN LATERAL (SELECT array_position(ml.levels, ml.level) AS n) pos
  WHERE ml.class <> 'wall' OR ml.geom_type = 'area'
  UNION ALL
  -- Each flight's middle, for its icon: a line is too short to carry one along it.
  SELECT (ml.osm_fid * 256 + n * 2 + 1) * 2, ml.venue, ml.level, 'poi', ml.class,
         CASE WHEN ml.class = 'escalator' THEN ml.tags->>'conveying' END,
         NULL, NULL, ml.id, ST_LineInterpolatePoint(ST_LineMerge(ml.geom), 0.5), NULL, NULL, NULL
  FROM member_levels ml
  CROSS JOIN LATERAL (SELECT array_position(ml.levels, ml.level) AS n) pos
  WHERE ml.geom_type = 'line' AND ml.class IN ('steps', 'escalator')
    AND ST_GeometryType(ST_LineMerge(ml.geom)) = 'ST_LineString'
  UNION ALL
  -- Named rooms get a label point; one carrying shop or amenity tags is that
  -- place, so it is iconed and tappable like a mapped point.
  SELECT (ml.osm_fid * 256 + n * 2 + 1) * 2, ml.venue, ml.level, 'poi',
         CASE WHEN ml.tags ? 'shop' THEN 'shop' WHEN ml.tags ? 'amenity' THEN 'amenity'
              WHEN ml.tags ? 'aeroway' THEN 'aeroway' ELSE 'room' END,
         COALESCE(ml.tags->>'shop', ml.tags->>'amenity', ml.tags->>'aeroway', ml.tags->>'room'),
         ml.name, ml.tags->>'ref', ml.id, ST_PointOnSurface(ml.geom), NULL, NULL, NULL
  FROM member_levels ml
  CROSS JOIN LATERAL (SELECT array_position(ml.levels, ml.level) AS n) pos
  WHERE ml.geom_type = 'area' AND ml.class IN ('room', 'area', 'platform')
    AND (ml.name IS NOT NULL OR ml.tags ? 'ref' OR ml.tags ? 'shop' OR ml.tags ? 'amenity')
  UNION ALL
  SELECT (p.osm_fid * 256 + n * 2) * 2, p.venue, p.level, 'poi',
         CASE WHEN p.class = 'poi' THEN p.poi_key ELSE p.class END,
         CASE
           WHEN p.class = 'door' THEN COALESCE(NULLIF(p.tags->>'entrance', 'yes'), CASE WHEN p.tags ? 'entrance' THEN 'entrance' END, 'door')
           WHEN p.class = 'elevator' THEN 'elevator'
           ELSE p.tags->>p.poi_key
         END,
         p.name, p.tags->>'ref', p.id, p.geom, NULL, NULL, NULL
  FROM poi_kinds p
  CROSS JOIN LATERAL (SELECT COALESCE(array_position(p.levels, p.level), 1) AS n) pos
)
SELECT r.fid, r.venue, r.level, v.default_level, r.kind, r.class, r.subclass,
       r.name, r.ref, r.id, r.levels, r.level_names, r.buildings,
       ST_Multi(CASE WHEN r.kind IN ('poi') THEN r.geom ELSE ST_CollectionExtract(ST_MakeValid(r.geom)) END) AS geom
FROM rows r
JOIN venues v USING (venue)
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW indoor_features IS 'v7';
CREATE UNIQUE INDEX IF NOT EXISTS indoor_features_fid_idx ON indoor_features (fid);
CREATE INDEX IF NOT EXISTS indoor_features_geom_idx ON indoor_features USING GIST (geom);

-- Martin serves each geometry kind as its own source; a view per kind keeps
-- the tile's layers small and lets venues come in at lower zooms.
CREATE OR REPLACE VIEW indoor_venues AS
SELECT fid, venue, default_level, name, levels, level_names, buildings, geom
FROM indoor_features WHERE kind = 'venue';

CREATE OR REPLACE VIEW indoor_map AS
SELECT fid, venue, level, default_level, kind, class, subclass, name, ref, id, geom
FROM indoor_features WHERE kind <> 'venue';
