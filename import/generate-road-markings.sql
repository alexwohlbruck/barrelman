-- =============================================================================
-- Road surfaces and markings
-- =============================================================================
-- Lane-level road geometry from OSM's lane tags, for a client to draw with
-- plain fill, line and symbol layers:
--
--   road_surfaces   carriageways at their real width, with kerb corners
--                   rounded where roads meet
--   road_markings   lines: centre, lane, edge, bike lane, stop and crosswalk,
--                   each with a colour and a pattern
--   road_glyphs     points: turn arrows, bike symbols and sharrows, each with
--                   the bearing it reads along
--
-- Everything is built in Web Mercator, where `s` is mercator units per metre at
-- the road's latitude, then written back in 4326. Built into *_next tables and
-- swapped in at the end, so tiles keep serving the old geometry meanwhile.
-- =============================================================================

-- Default lane width in metres by class.
CREATE OR REPLACE FUNCTION road_lane_width(class text) RETURNS float8
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN class IN ('motorway', 'trunk') THEN 3.6
    WHEN class IN ('motorway_link', 'trunk_link', 'primary') THEN 3.4
    WHEN class IN ('primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link') THEN 3.3
    WHEN class = 'service' THEN 2.8
    ELSE 3.0
  END
$$;

-- A whole number from the front of a tag, or NULL.
CREATE OR REPLACE FUNCTION road_int(value text) RETURNS int
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NULLIF(substring(value from '^\s*([0-9]{1,2})'), '')::int
$$;

-- A line offset sideways (positive to the left of its direction), easing from
-- `from_offset` to `to_offset` over its first `ease` units, then holding. This
-- is a lane taper: a lane line drifting across as a lane opens or closes.
CREATE OR REPLACE FUNCTION road_taper(line geometry, from_offset float8, to_offset float8, ease float8)
RETURNS geometry LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  split float8 := least(1, ease / nullif(ST_Length(line), 0));
  head geometry := ST_LineSubstring(line, 0, split);
  pts geometry[] := ARRAY(SELECT geom FROM ST_DumpPoints(ST_Segmentize(head, greatest(ease / 8, 0.5))) ORDER BY path);
  n int := array_length(pts, 1);
  out geometry[] := '{}';
  along float8 := 0;
  rest geometry;
  i int;
  dx float8;
  dy float8;
  len float8;
  t float8;
  d float8;
BEGIN
  -- Only the eased stretch needs walking point by point; past it the line holds
  -- its offset and GEOS can draw it.
  FOR i IN 1..n LOOP
    IF i > 1 THEN along := along + ST_Distance(pts[i - 1], pts[i]); END IF;
    dx := ST_X(pts[least(i + 1, n)]) - ST_X(pts[greatest(i - 1, 1)]);
    dy := ST_Y(pts[least(i + 1, n)]) - ST_Y(pts[greatest(i - 1, 1)]);
    len := sqrt(dx * dx + dy * dy);
    IF len = 0 THEN CONTINUE; END IF;
    t := least(1, along / nullif(ease, 0));
    -- Smoothstep, so the line leaves and joins its lane without a kink.
    d := from_offset + (to_offset - from_offset) * (t * t * (3 - 2 * t));
    out := out || ST_MakePoint(ST_X(pts[i]) - dy / len * d, ST_Y(pts[i]) + dx / len * d);
  END LOOP;
  IF split < 1 THEN
    rest := road_offset(ST_LineSubstring(line, split, 1), to_offset);
    IF rest IS NOT NULL AND GeometryType(rest) = 'LINESTRING' THEN
      out := out || ARRAY(SELECT ST_SetSRID(geom, 0) FROM ST_DumpPoints(rest) ORDER BY path);
    END IF;
  END IF;
  RETURN CASE WHEN array_length(out, 1) >= 2 THEN ST_SetSRID(ST_MakeLine(out), ST_SRID(line)) END;
END
$$;

-- ST_OffsetCurve, or NULL for the self-crossing ways GEOS refuses to offset.
CREATE OR REPLACE FUNCTION road_offset(line geometry, distance float8) RETURNS geometry
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN ST_OffsetCurve(line, distance, 'join=round quad_segs=2');
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
$$;

-- The carriageway between two kerb lines, rounded off at its far end; NULL if
-- the kerbs do not make a polygon.
CREATE OR REPLACE FUNCTION road_body(left_kerb geometry, right_kerb geometry, far_end geometry, radius float8)
RETURNS geometry LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN ST_Union(
    ST_CollectionExtract(ST_MakeValid(ST_MakePolygon(ST_AddPoint(ST_MakeLine(left_kerb, ST_Reverse(right_kerb)), ST_StartPoint(left_kerb)))), 3),
    ST_Buffer(far_end, radius, 4));
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
$$;

-- ─── Roads and their lanes ───────────────────────────────────────────────────
DROP TABLE IF EXISTS _rm_roads;
CREATE TEMP TABLE _rm_roads AS
WITH raw AS (
  SELECT osm_id, tags, tags->>'highway' as class,
         CASE WHEN tags->>'oneway' = '-1' THEN ST_Reverse(ST_Transform(geom, 3857)) ELSE ST_Transform(geom, 3857) END as g,
         1 / cos(radians(ST_Y(ST_Centroid(geom)))) as s,
         ST_X(ST_Centroid(geom)) < -30 as americas,
         COALESCE(tags->>'oneway' IN ('yes', 'true', '1', '-1')
           OR tags->>'junction' IN ('roundabout', 'circular')
           OR (tags->>'highway' IN ('motorway', 'motorway_link') AND COALESCE(tags->>'oneway', '') <> 'no'), false) as oneway,
         road_int(tags->>'lanes') as lanes_tag,
         COALESCE(road_int(tags->>'lanes:both_ways'), 0) as both_ways,
         substring(tags->>'width' from '^\s*([0-9]+(?:\.[0-9]+)?)\s*(?:m)?\s*$')::float8 as width_tag,
         COALESCE(tags->>'cycleway:right', tags->>'cycleway:both', tags->>'cycleway') as bike_right,
         COALESCE(tags->>'cycleway:left', tags->>'cycleway:both',
                  CASE WHEN COALESCE(tags->>'oneway', 'no') = 'no' THEN tags->>'cycleway' END) as bike_left
  FROM geo_places
  WHERE geom_type = 'line'
    AND tags->>'highway' IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link',
                             'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified',
                             'residential', 'living_street', 'service', 'busway')
    AND COALESCE(tags->>'tunnel', 'no') = 'no'
    AND COALESCE(tags->>'area', 'no') <> 'yes'
    AND COALESCE(tags->>'service', '') NOT IN ('driveway', 'drive-through', 'parking_aisle', 'emergency_access')
),
counted AS (
  SELECT r.*,
         COALESCE(r.lanes_tag, CASE
           WHEN r.class IN ('motorway', 'trunk') AND r.oneway THEN 2
           WHEN r.oneway THEN 1
           WHEN r.class = 'service' THEN 1
           ELSE 2 END) as lanes
  FROM raw r
),
split AS (
  SELECT c.*,
         CASE WHEN c.oneway THEN c.lanes - c.both_ways
              ELSE COALESCE(road_int(c.tags->>'lanes:forward'), ceil((c.lanes - c.both_ways) / 2.0)::int) END as fwd,
         CASE WHEN c.oneway THEN 0
              ELSE COALESCE(road_int(c.tags->>'lanes:backward'),
                            c.lanes - c.both_ways - COALESCE(road_int(c.tags->>'lanes:forward'), ceil((c.lanes - c.both_ways) / 2.0)::int)) END as bwd,
         CASE WHEN c.bike_right IN ('lane', 'exclusive_lane', 'opposite_lane') THEN 1.6 ELSE 0 END as bike_r,
         CASE WHEN c.bike_left IN ('lane', 'exclusive_lane', 'opposite_lane') AND NOT c.oneway THEN 1.6 ELSE 0 END as bike_l
  FROM counted c
)
SELECT osm_id, class, g, s, americas, oneway, tags, both_ways,
       -- Above the ground: a bridge, or anything mapped on a layer over it.
       COALESCE(tags->>'bridge', 'no') NOT IN ('no') OR COALESCE(road_int(tags->>'layer'), 0) > 0 as bridge,
       greatest(fwd, 0) as fwd, greatest(bwd, 0) as bwd, bike_r, bike_l,
       -- Width: tagged, else lanes at the class's lane width plus bike lanes.
       COALESCE(CASE WHEN width_tag BETWEEN 2.5 AND 60 THEN width_tag END,
                (greatest(fwd, 0) + greatest(bwd, 0) + both_ways) * road_lane_width(class) + bike_r + bike_l) as width,
       -- Marked: a lane count was mapped, or the class is one that is striped
       -- anyway, and nobody tagged it unmarked.
       COALESCE(tags->>'lane_markings', 'yes') <> 'no'
         AND class NOT IN ('service', 'living_street')
         AND (lanes_tag IS NOT NULL OR class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary',
                                                 'primary_link', 'secondary', 'secondary_link', 'tertiary')) as marked,
       COALESCE(tags->>'turn:lanes:forward', CASE WHEN oneway THEN tags->>'turn:lanes' END) as turn_forward,
       tags->>'turn:lanes:backward' as turn_backward,
       COALESCE(tags->>'cycleway', tags->>'cycleway:both', tags->>'cycleway:right') = 'shared_lane' as sharrow
FROM split;
-- Across the carriageway, left of the way's direction is positive: the left
-- kerb, the backward lanes, any centre turn lane, the forward lanes, the right
-- kerb. `split_b` and `split_f` bound the turn lane; without one they meet.
ALTER TABLE _rm_roads ADD COLUMN lane_w float8, ADD COLUMN left_edge float8, ADD COLUMN right_edge float8,
  ADD COLUMN split_b float8, ADD COLUMN split_f float8;
UPDATE _rm_roads SET lane_w = (width - bike_r - bike_l) / greatest(fwd + bwd + both_ways, 1);
UPDATE _rm_roads SET left_edge = width / 2 - bike_l, right_edge = -width / 2 + bike_r;
UPDATE _rm_roads SET split_b = left_edge - bwd * lane_w;
UPDATE _rm_roads SET split_f = split_b - both_ways * lane_w;
CREATE INDEX ON _rm_roads USING gist (g);
ANALYZE _rm_roads;

-- ─── Tapers ──────────────────────────────────────────────────────────────────
-- Where one road simply continues into the next, the next starts from the
-- first's lane layout and eases into its own: a lane opening or closing drifts
-- across over a taper rather than jumping sideways at the join. The taper runs
-- to the MUTCD's length for the road's speed (L = WS²/60 ft up to 40 mph,
-- WS above), short of the road's own length.
ALTER TABLE _rm_roads
  ADD COLUMN prev_left float8, ADD COLUMN prev_right float8, ADD COLUMN prev_split_b float8,
  ADD COLUMN prev_split_f float8, ADD COLUMN prev_lane_w float8, ADD COLUMN ease float8,
  ADD COLUMN opens_kerb_f boolean, ADD COLUMN opens_kerb_b boolean, ADD COLUMN body geometry;
WITH ends AS (
  SELECT osm_id, round(ST_X(ST_EndPoint(g))::numeric, 2) as x, round(ST_Y(ST_EndPoint(g))::numeric, 2) as y FROM _rm_roads
),
starts AS (
  SELECT osm_id, round(ST_X(ST_StartPoint(g))::numeric, 2) as x, round(ST_Y(ST_StartPoint(g))::numeric, 2) as y FROM _rm_roads
),
pairs AS (
  SELECT e.osm_id as prev_id, st.osm_id as next_id
  FROM ends e JOIN starts st USING (x, y)
  JOIN (SELECT x, y FROM ends GROUP BY x, y HAVING count(*) = 1) ue USING (x, y)
  JOIN (SELECT x, y FROM starts GROUP BY x, y HAVING count(*) = 1) us USING (x, y)
  WHERE e.osm_id <> st.osm_id
)
UPDATE _rm_roads b SET
  prev_left = a.left_edge, prev_right = a.right_edge, prev_split_b = a.split_b,
  prev_split_f = a.split_f, prev_lane_w = a.lane_w
FROM pairs JOIN _rm_roads a ON a.osm_id = pairs.prev_id
WHERE b.osm_id = pairs.next_id AND a.marked = b.marked
  AND NOT (a.oneway <> b.oneway)
  AND (abs(a.left_edge - b.left_edge) > 0.2 OR abs(a.right_edge - b.right_edge) > 0.2
       OR abs(a.split_b - b.split_b) > 0.2 OR abs(a.lane_w - b.lane_w) > 0.2);
UPDATE _rm_roads SET
  ease = least(0.6 * ST_Length(g) / s, greatest(15,
           CASE WHEN mph >= 45 THEN lane_w * 3.281 * mph ELSE lane_w * 3.281 * mph * mph / 60 END / 3.281)),
  opens_kerb_f = COALESCE(fwd > 0 AND split_part(turn_forward, '|', greatest(fwd, 1)) LIKE '%right%' AND split_part(turn_forward, '|', 1) NOT LIKE '%left%', false),
  opens_kerb_b = COALESCE(bwd > 0 AND split_part(turn_backward, '|', greatest(bwd, 1)) LIKE '%right%' AND split_part(turn_backward, '|', 1) NOT LIKE '%left%', false)
FROM (SELECT osm_id as id, COALESCE(
        CASE WHEN tags->>'maxspeed' ~ 'mph' THEN substring(tags->>'maxspeed' from '^\s*([0-9]+)')::float8
             ELSE substring(tags->>'maxspeed' from '^\s*([0-9]+)\s*$')::float8 / 1.609 END,
        CASE WHEN class IN ('motorway', 'motorway_link') THEN 60 WHEN class IN ('trunk', 'trunk_link') THEN 50
             WHEN class IN ('primary', 'primary_link') THEN 45 WHEN class IN ('secondary', 'secondary_link') THEN 40
             WHEN class IN ('tertiary', 'tertiary_link') THEN 35 ELSE 25 END) as mph FROM _rm_roads) speed
WHERE osm_id = speed.id AND prev_left IS NOT NULL;
-- A tapering road's carriageway, between its two eased kerbs.
UPDATE _rm_roads SET body = road_body(l, r, ST_EndPoint(g), width / 2 * s)
FROM (SELECT osm_id as id,
             road_taper(g, prev_left * s, left_edge * s, ease * s) as l,
             road_taper(g, prev_right * s, right_edge * s, ease * s) as r
      FROM _rm_roads WHERE prev_left IS NOT NULL) e
WHERE osm_id = e.id AND e.l IS NOT NULL AND e.r IS NOT NULL;

-- ─── Junctions ───────────────────────────────────────────────────────────────
-- A vertex where three or more road ends meet, counting a road that passes
-- through as two. Markings stop short of it and stop lines stand at the gap.
DROP TABLE IF EXISTS _rm_vertices;
CREATE TEMP TABLE _rm_vertices AS
SELECT p, sum(degree)::int as degree, max(width) as width, max(s) as s
FROM (
  SELECT ST_SnapToGrid(dp.geom, 0.01) as p,
         CASE WHEN dp.path[1] IN (1, ST_NPoints(r.g)) THEN 1 ELSE 2 END as degree,
         r.width, r.s
  FROM _rm_roads r, ST_DumpPoints(r.g) dp
) v
GROUP BY p;
DROP TABLE IF EXISTS _rm_nodes;
CREATE TEMP TABLE _rm_nodes AS SELECT * FROM _rm_vertices WHERE degree >= 3;
CREATE INDEX ON _rm_nodes USING gist (p);
ANALYZE _rm_nodes;

-- Signals and stop signs near a junction earn its approaches a stop line.
DROP TABLE IF EXISTS _rm_signals;
CREATE TEMP TABLE _rm_signals AS
SELECT ST_Transform(geom, 3857) as g FROM geo_places
WHERE geom_type = 'point' AND (tags @> '{"highway": "traffic_signals"}' OR tags @> '{"highway": "stop"}');
CREATE INDEX ON _rm_signals USING gist (g);
ANALYZE _rm_signals;
UPDATE _rm_nodes n SET degree = -degree
WHERE EXISTS (SELECT 1 FROM _rm_signals c WHERE ST_DWithin(c.g, n.p, 30 * n.s));

-- Painted crossings: lane lines break for them, and stop lines and arrows
-- stand back from them. `style` is what the paint looks like: zebra bars, two
-- lines, or both.
DROP TABLE IF EXISTS _rm_crossings;
CREATE TEMP TABLE _rm_crossings AS
SELECT ST_Transform(geom, 3857) as g, geom_type, 1 / cos(radians(ST_Y(ST_Centroid(geom)))) as s,
       CASE
         WHEN tags->>'crossing:markings' IN ('zebra', 'zebra:double', 'zebra:paired', 'zebra:bicolour', 'yes') THEN 'zebra'
         WHEN tags->>'crossing:markings' IN ('lines', 'lines:paired', 'dashes', 'dots') THEN 'lines'
         WHEN tags->>'crossing:markings' IN ('ladder', 'ladder:paired', 'ladder:skewed') THEN 'ladder'
         WHEN tags->>'crossing:markings' IN ('no', 'surface') THEN NULL
         WHEN tags->>'crossing' IN ('zebra', 'marked', 'uncontrolled') OR tags->>'crossing_ref' = 'zebra' THEN 'zebra'
         WHEN tags->>'crossing' = 'traffic_signals' THEN 'lines'
       END as style
FROM geo_places
WHERE (geom_type = 'line' AND tags->>'footway' = 'crossing')
   OR (geom_type = 'point' AND tags->>'highway' = 'crossing');
DELETE FROM _rm_crossings WHERE style IS NULL;
CREATE INDEX ON _rm_crossings USING gist (g);
ANALYZE _rm_crossings;

-- ─── Surfaces ────────────────────────────────────────────────────────────────
-- Each road widened to its carriageway, unioned per grid cell and closed by a
-- kerb radius, which rounds every inside corner where two roads meet. A cell
-- unions every road within reach of it and keeps only its own square, so the
-- seams between cells match.
DROP TABLE IF EXISTS _rm_surfaces;
CREATE TEMP TABLE _rm_surfaces (bridge boolean, g geometry);
WITH cells AS (
  SELECT DISTINCT floor(ST_X(c) / 800) as cx, floor(ST_Y(c) / 800) as cy
  FROM _rm_roads r, LATERAL (SELECT (ST_DumpPoints(ST_Segmentize(r.g, 400))).geom as c) d
),
merged AS (
  SELECT cells.cx, cells.cy, r.bridge,
         ST_MakeEnvelope(cells.cx * 800, cells.cy * 800, (cells.cx + 1) * 800, (cells.cy + 1) * 800, 3857) as box,
         ST_Union(COALESCE(
           ST_Intersection(r.body, ST_Expand(ST_MakeEnvelope(cells.cx * 800, cells.cy * 800, (cells.cx + 1) * 800, (cells.cy + 1) * 800, 3857), 60)),
           ST_Buffer(ST_Intersection(r.g, ST_Expand(ST_MakeEnvelope(cells.cx * 800, cells.cy * 800, (cells.cx + 1) * 800, (cells.cy + 1) * 800, 3857), 60)),
                     r.width / 2 * r.s, 'endcap=round join=round quad_segs=4'))) as u,
         avg(r.s) as s
  FROM cells
  JOIN _rm_roads r ON r.g && ST_Expand(ST_MakeEnvelope(cells.cx * 800, cells.cy * 800, (cells.cx + 1) * 800, (cells.cy + 1) * 800, 3857), 60)
  GROUP BY cells.cx, cells.cy, r.bridge
)
INSERT INTO _rm_surfaces (bridge, g)
SELECT bridge, ST_CollectionExtract(ST_MakeValid(ST_Intersection(
         ST_Buffer(ST_Buffer(u, 5 * s, 'join=round quad_segs=4'), -5 * s, 'join=round quad_segs=4'), box)), 3)
FROM merged;
DELETE FROM _rm_surfaces WHERE ST_IsEmpty(g);
CREATE INDEX ON _rm_surfaces USING gist (g);
DROP TABLE IF EXISTS road_surfaces_next;
CREATE TABLE road_surfaces_next (fid bigserial CONSTRAINT road_surfaces_next_pk PRIMARY KEY, bridge boolean, geom geometry(MultiPolygon, 4326));
INSERT INTO road_surfaces_next (bridge, geom) SELECT bridge, ST_Multi(ST_Transform(g, 4326)) FROM _rm_surfaces;

-- What lane lines break for: junctions, and the crosswalks across them,
-- unioned once per grid cell rather than once per line.
DROP TABLE IF EXISTS _rm_cuts;
CREATE TEMP TABLE _rm_cuts AS
SELECT ST_MakeEnvelope(cx * 400, cy * 400, (cx + 1) * 400, (cy + 1) * 400, 3857) as box, ST_Union(cut) as cut
FROM (
  SELECT floor(ST_X(n.p) / 400) as cx, floor(ST_Y(n.p) / 400) as cy, ST_Buffer(n.p, (abs(n.width) / 2 + 2) * n.s, 4) as cut FROM _rm_nodes n
  UNION ALL
  SELECT floor(ST_X(c.cxy) / 400), floor(ST_Y(c.cxy) / 400), ST_Buffer(c.g, 2 * c.s, 2)
  FROM (SELECT g, s, ST_PointOnSurface(g) as cxy FROM _rm_crossings) c
) cuts
GROUP BY cx, cy;
-- A cut spills a little past its cell; widen each box to hold it.
UPDATE _rm_cuts SET box = ST_Envelope(ST_Collect(box, cut));
CREATE INDEX ON _rm_cuts USING gist (box);

-- ─── Lane lines ──────────────────────────────────────────────────────────────
-- Offsets are to the left of the way's direction. Backward lanes run on the
-- left and forward lanes on the right; `split` is the line between them.
DROP TABLE IF EXISTS road_markings_next;
CREATE TABLE road_markings_next (
  fid bigserial CONSTRAINT road_markings_next_pk PRIMARY KEY, kind text, pattern text, color text, style text, bridge boolean,
  geom geometry(Geometry, 4326));

DROP TABLE IF EXISTS _rm_lines;
CREATE TEMP TABLE _rm_lines AS
WITH lanes AS (SELECT * FROM _rm_roads WHERE marked)
-- The centre line between the two directions, doubled on main roads; a centre
-- turn lane is bounded by a line on each side. `start_m` is where a line
-- begins when it eases in from the road before; see Tapers.
SELECT osm_id, g, s, bridge, ease, 'centre' as kind,
       CASE WHEN class IN ('primary', 'secondary', 'trunk') OR fwd + bwd >= 4 OR both_ways > 0 THEN 'double' ELSE 'dashed' END as pattern,
       CASE WHEN americas THEN 'yellow' ELSE 'white' END as color,
       split_b as offset_m, prev_split_b as start_m
FROM lanes WHERE NOT oneway AND fwd > 0 AND bwd > 0
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'centre', 'double', CASE WHEN americas THEN 'yellow' ELSE 'white' END, split_f, prev_split_f
FROM lanes WHERE NOT oneway AND both_ways > 0
UNION ALL
-- Lane dividers within each direction. A lane opening at the centre keeps the
-- dividers counted from the kerb where they were; one opening at the kerb
-- keeps those counted from the centre. A divider with no counterpart on the
-- road before emerges from the line it is clamped to.
SELECT osm_id, g, s, bridge, ease, 'lane', 'dashed', 'white', split_f - i * lane_w,
       CASE WHEN prev_left IS NULL THEN NULL
            WHEN opens_kerb_f THEN greatest(prev_right, least(prev_split_f, prev_split_f - i * prev_lane_w))
            ELSE greatest(prev_right, least(prev_split_f, prev_right + (fwd - i) * prev_lane_w)) END
FROM lanes, generate_series(1, 8) i WHERE i < fwd
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'lane', 'dashed', 'white', split_b + i * lane_w,
       CASE WHEN prev_left IS NULL THEN NULL
            WHEN opens_kerb_b THEN least(prev_left, greatest(prev_split_b, prev_split_b + i * prev_lane_w))
            ELSE least(prev_left, greatest(prev_split_b, prev_left - (bwd - i) * prev_lane_w)) END
FROM lanes, generate_series(1, 8) i WHERE i < bwd
UNION ALL
-- Edge lines on roads without kerbs: motorways, trunks and their links.
SELECT osm_id, g, s, bridge, ease, 'edge', 'solid', 'white', right_edge + 0.3, prev_right + 0.3
FROM lanes WHERE class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'edge', 'solid', CASE WHEN americas AND oneway THEN 'yellow' ELSE 'white' END, left_edge - 0.3, prev_left - 0.3
FROM lanes WHERE class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')
UNION ALL
-- Bike lanes, separated from traffic by a solid line.
SELECT osm_id, g, s, bridge, ease, 'bike', 'solid', 'white', right_edge, prev_right
FROM lanes WHERE bike_r > 0
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'bike', 'solid', 'white', left_edge, prev_left
FROM lanes WHERE bike_l > 0;

-- Lane lines stop short of junctions, where the crossing traffic runs.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT l.kind, l.pattern, l.color, l.bridge, ST_Transform(clipped, 4326)
FROM _rm_lines l
CROSS JOIN LATERAL (SELECT CASE
  WHEN l.start_m IS NOT NULL AND abs(l.start_m - l.offset_m) > 0.2
    THEN road_taper(ST_Simplify(l.g, 0.2 * l.s), l.start_m * l.s, l.offset_m * l.s, l.ease * l.s)
  ELSE road_offset(ST_Simplify(l.g, 0.2 * l.s), l.offset_m * l.s) END as line) o
CROSS JOIN LATERAL (
  SELECT COALESCE((SELECT ST_LineMerge(ST_Union(ST_Difference(ST_Intersection(o.line, k.box), k.cut)))
                   FROM _rm_cuts k WHERE k.box && o.line), o.line) as clipped
) c
WHERE o.line IS NOT NULL AND NOT ST_IsEmpty(c.clipped)
  AND ST_Length(c.clipped) > 2 * l.s;

-- ─── Stop lines and turn arrows ──────────────────────────────────────────────
-- At each end of a marked road that meets a junction: across the lanes that
-- arrive there, a stop line if the junction is controlled, and an arrow in
-- each lane its turn:lanes describes.
DROP TABLE IF EXISTS _rm_ends;
CREATE TEMP TABLE _rm_ends AS
SELECT r.osm_id, r.g, r.s, r.americas, r.bridge, n.degree < 0 as controlled, e.at_end,
       r.split_b, r.split_f, r.left_edge, r.right_edge, r.lane_w, r.fwd, r.bwd,
       CASE WHEN e.at_end THEN r.turn_forward ELSE r.turn_backward END as turns,
       -- Back from the junction centre to the stop line: clear of the junction,
       -- and of any crosswalk across the approach.
       greatest((abs(n.width) / 2 + 2) * r.s, COALESCE((
         SELECT max(abs(CASE WHEN e.at_end THEN 1 - ST_LineLocatePoint(r.g, ST_ClosestPoint(c.g, e.pt))
                                              ELSE ST_LineLocatePoint(r.g, ST_ClosestPoint(c.g, e.pt)) END) * ST_Length(r.g))
         FROM _rm_crossings c
         WHERE c.g && ST_Expand(e.pt, 30 * r.s) AND ST_DWithin(c.g, r.g, 0.5 * r.s)
           AND ST_DWithin(c.g, e.pt, 30 * r.s)
       ) + 2.5 * r.s, 0)) as setback
FROM _rm_roads r
CROSS JOIN LATERAL (VALUES (true, ST_EndPoint(r.g)), (false, ST_StartPoint(r.g))) e(at_end, pt)
JOIN _rm_nodes n ON n.p = ST_SnapToGrid(e.pt, 0.01)
WHERE r.marked AND ST_Length(r.g) > 40 * r.s;

-- A frame at a point `back` units from a road end: the point, and the unit
-- vectors along the arriving traffic and to its left.
DROP TABLE IF EXISTS _rm_frames;
CREATE TEMP TABLE _rm_frames AS
SELECT e.*, f.back, p,
       (ST_X(ahead) - ST_X(p)) / nullif(ST_Distance(ahead, p), 0) as ux,
       (ST_Y(ahead) - ST_Y(p)) / nullif(ST_Distance(ahead, p), 0) as uy
FROM _rm_ends e
CROSS JOIN LATERAL (VALUES (e.setback), (e.setback + 6 * e.s)) f(back)
CROSS JOIN LATERAL (
  SELECT ST_LineInterpolatePoint(e.g, CASE WHEN e.at_end THEN greatest(0, 1 - f.back / ST_Length(e.g)) ELSE least(1, f.back / ST_Length(e.g)) END) as p,
         ST_LineInterpolatePoint(e.g, CASE WHEN e.at_end THEN greatest(0, 1 - (f.back - e.s) / ST_Length(e.g)) ELSE least(1, (f.back - e.s) / ST_Length(e.g)) END) as ahead
) pts
WHERE f.back < ST_Length(e.g) / 2;

-- Stop lines span the arriving lanes. At a way's end those are the forward
-- lanes; at its start the backward ones, and the frame there faces the other
-- way, so the way's offsets flip sign.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'stop', 'solid', 'white', bridge, ST_Transform(ST_SetSRID(ST_MakeLine(
         ST_MakePoint(ST_X(p) - uy * a * s, ST_Y(p) + ux * a * s),
         ST_MakePoint(ST_X(p) - uy * b * s, ST_Y(p) + ux * b * s)), 3857), 4326)
FROM _rm_frames
CROSS JOIN LATERAL (SELECT
  CASE WHEN at_end THEN split_f ELSE -split_b END as a,
  CASE WHEN at_end THEN right_edge ELSE -left_edge END as b) o
WHERE controlled AND back = setback AND ux IS NOT NULL
  AND ((at_end AND fwd > 0) OR (NOT at_end AND bwd > 0));

-- Turn arrows, one per lane, when turn:lanes names every lane that arrives.
DROP TABLE IF EXISTS road_glyphs_next;
CREATE TABLE road_glyphs_next (fid bigserial CONSTRAINT road_glyphs_next_pk PRIMARY KEY, glyph text, direction int, bridge boolean, geom geometry(Point, 4326));
INSERT INTO road_glyphs_next (glyph, direction, bridge, geom)
SELECT glyph, ((round(degrees(atan2(ux, uy)))::int % 360) + 360) % 360,
       bridge,
       ST_Transform(ST_SetSRID(ST_MakePoint(ST_X(p) - uy * lane_off * s, ST_Y(p) + ux * lane_off * s), 3857), 4326)
FROM (
  SELECT f.*, t.ord, t.value,
         -- Lanes are listed left to right as traffic sees them, from the split outwards.
         CASE WHEN f.at_end THEN f.split_f - (t.ord - 0.5) * f.lane_w
              ELSE -f.split_b - (t.ord - 0.5) * f.lane_w END as lane_off,
         CASE regexp_replace(t.value, '\s', '', 'g')
           WHEN 'left' THEN 'road-arrow-left'
           WHEN 'through' THEN 'road-arrow-through'
           WHEN 'right' THEN 'road-arrow-right'
           WHEN 'left;through' THEN 'road-arrow-left-through'
           WHEN 'through;left' THEN 'road-arrow-left-through'
           WHEN 'through;right' THEN 'road-arrow-through-right'
           WHEN 'right;through' THEN 'road-arrow-through-right'
           WHEN 'left;right' THEN 'road-arrow-left-right'
           WHEN 'slight_left' THEN 'road-arrow-slight-left'
           WHEN 'slight_right' THEN 'road-arrow-slight-right'
           WHEN 'reverse' THEN 'road-arrow-uturn'
           WHEN 'left;reverse' THEN 'road-arrow-uturn'
           WHEN 'reverse;left' THEN 'road-arrow-uturn'
         END as glyph
  FROM _rm_frames f
  CROSS JOIN LATERAL unnest(string_to_array(f.turns, '|')) WITH ORDINALITY t(value, ord)
  WHERE f.back > f.setback AND f.turns IS NOT NULL AND f.ux IS NOT NULL
    AND array_length(string_to_array(f.turns, '|'), 1) = CASE WHEN f.at_end THEN f.fwd ELSE f.bwd END
) a
WHERE glyph IS NOT NULL;

-- Bike symbols along bike lanes, and sharrows along shared lanes, every 60 m.
INSERT INTO road_glyphs_next (glyph, direction, bridge, geom)
SELECT CASE WHEN r.sharrow THEN 'road-sharrow' ELSE 'road-bike' END,
       ((round(degrees(atan2(ST_X(b) - ST_X(a), ST_Y(b) - ST_Y(a))))::int + CASE WHEN side.forward THEN 0 ELSE 180 END) % 360 + 360) % 360,
       r.bridge,
       ST_Transform(ST_SetSRID(ST_MakePoint(ST_X(a) - (ST_Y(b) - ST_Y(a)) / d * side.off * r.s,
                                            ST_Y(a) + (ST_X(b) - ST_X(a)) / d * side.off * r.s), 3857), 4326)
FROM _rm_roads r
CROSS JOIN LATERAL (VALUES
  (true, CASE WHEN r.sharrow THEN r.right_edge + r.lane_w * 0.6 ELSE r.right_edge - r.bike_r / 2 END, r.bike_r > 0 OR r.sharrow),
  (false, CASE WHEN r.sharrow THEN r.left_edge - r.lane_w * 0.6 ELSE r.left_edge + r.bike_l / 2 END, (r.bike_l > 0 OR r.sharrow) AND NOT r.oneway)
) side(forward, off, present)
CROSS JOIN LATERAL generate_series(1, floor(ST_Length(r.g) / r.s / 60)::int) k
CROSS JOIN LATERAL (SELECT
  ST_LineInterpolatePoint(r.g, (k * 60 - 30) * r.s / ST_Length(r.g)) as a,
  ST_LineInterpolatePoint(r.g, least(1, ((k * 60 - 30) + 1) * r.s / ST_Length(r.g))) as b) pts
CROSS JOIN LATERAL (SELECT nullif(ST_Distance(pts.a, pts.b), 0) as d) dist
WHERE side.present AND d IS NOT NULL AND (r.bike_r > 0 OR r.bike_l > 0 OR r.sharrow);

-- ─── Crosswalks ──────────────────────────────────────────────────────────────
-- A mapped crossing way, trimmed to the kerbs it runs between; or, for a
-- crossing mapped only as a node, a line straight across the road there.


INSERT INTO road_markings_next (kind, pattern, color, style, bridge, geom)
SELECT 'crosswalk', 'solid', 'white', c.style, false, ST_Transform(seg, 4326)
FROM _rm_crossings c
CROSS JOIN LATERAL (
  SELECT ST_Intersection(s.g, c.g) as seg
  FROM _rm_surfaces s
  WHERE c.geom_type = 'line' AND s.g && c.g
) x
WHERE c.geom_type = 'line' AND GeometryType(seg) IN ('LINESTRING', 'MULTILINESTRING') AND ST_Length(seg) > 1;

INSERT INTO road_markings_next (kind, pattern, color, style, bridge, geom)
SELECT 'crosswalk', 'solid', 'white', c.style, false, ST_Transform(ST_SetSRID(ST_MakeLine(
         ST_MakePoint(ST_X(c.g) - uy * r.width / 2 * r.s, ST_Y(c.g) + ux * r.width / 2 * r.s),
         ST_MakePoint(ST_X(c.g) + uy * r.width / 2 * r.s, ST_Y(c.g) - ux * r.width / 2 * r.s)), 3857), 4326)
FROM _rm_crossings c
CROSS JOIN LATERAL (
  SELECT r.* FROM _rm_roads r WHERE r.g && ST_Expand(c.g, 2) ORDER BY r.g <-> c.g LIMIT 1
) r
CROSS JOIN LATERAL (SELECT ST_LineLocatePoint(r.g, c.g) as t) loc
CROSS JOIN LATERAL (SELECT ST_LineInterpolatePoint(r.g, greatest(0, loc.t - 0.5 * r.s / ST_Length(r.g))) as a,
                           ST_LineInterpolatePoint(r.g, least(1, loc.t + 0.5 * r.s / ST_Length(r.g))) as b) e
CROSS JOIN LATERAL (SELECT (ST_X(b) - ST_X(a)) / nullif(ST_Distance(a, b), 0) as ux,
                           (ST_Y(b) - ST_Y(a)) / nullif(ST_Distance(a, b), 0) as uy) u
WHERE c.geom_type = 'point' AND ux IS NOT NULL
  AND ST_DWithin(r.g, c.g, 1 * r.s)
  AND NOT EXISTS (SELECT 1 FROM _rm_crossings w WHERE w.geom_type = 'line' AND ST_DWithin(w.g, c.g, 1 * r.s));

-- ─── Swap ────────────────────────────────────────────────────────────────────
CREATE INDEX road_surfaces_next_geom_idx ON road_surfaces_next USING gist (geom);
CREATE INDEX road_markings_next_geom_idx ON road_markings_next USING gist (geom);
CREATE INDEX road_glyphs_next_geom_idx ON road_glyphs_next USING gist (geom);
BEGIN;
DROP TABLE IF EXISTS road_surfaces;
DROP TABLE IF EXISTS road_markings;
DROP TABLE IF EXISTS road_glyphs;
ALTER TABLE road_surfaces_next RENAME TO road_surfaces;
ALTER TABLE road_markings_next RENAME TO road_markings;
ALTER TABLE road_glyphs_next RENAME TO road_glyphs;
ALTER INDEX road_surfaces_next_geom_idx RENAME TO road_surfaces_geom_idx;
ALTER INDEX road_markings_next_geom_idx RENAME TO road_markings_geom_idx;
ALTER INDEX road_glyphs_next_geom_idx RENAME TO road_glyphs_geom_idx;
ALTER TABLE road_surfaces RENAME CONSTRAINT road_surfaces_next_pk TO road_surfaces_pkey;
ALTER TABLE road_markings RENAME CONSTRAINT road_markings_next_pk TO road_markings_pkey;
ALTER TABLE road_glyphs RENAME CONSTRAINT road_glyphs_next_pk TO road_glyphs_pkey;
ALTER SEQUENCE road_surfaces_next_fid_seq RENAME TO road_surfaces_fid_seq;
ALTER SEQUENCE road_markings_next_fid_seq RENAME TO road_markings_fid_seq;
ALTER SEQUENCE road_glyphs_next_fid_seq RENAME TO road_glyphs_fid_seq;
COMMIT;
ANALYZE road_surfaces;
ANALYZE road_markings;
ANALYZE road_glyphs;
