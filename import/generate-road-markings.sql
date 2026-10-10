-- =============================================================================
-- Road surfaces and markings
-- =============================================================================
-- Lane-level road geometry from OSM's lane tags, for a client to draw with
-- plain fill, line and symbol layers:
--
--   road_surfaces   carriageways at their real width, with kerb corners
--                   rounded where roads meet, and the islands turning loops
--                   run round (`kind` 'island')
--   road_markings   lines: centre, lane, edge, bike lane, stop and crosswalk,
--                   each with a colour and a pattern
--   road_glyphs     points: turn arrows, bike symbols and sharrows, each with
--                   the bearing it reads along
--
-- Everything is built in Web Mercator, where `s` is mercator units per metre at
-- the road's latitude, then written back in 4326. Built into *_next tables and
-- swapped in at the end, so tiles keep serving the old geometry meanwhile.
-- =============================================================================

-- ─── One build at a time ─────────────────────────────────────────────────────
-- Full and scoped builds (see Scope below) write the same road_*_next tables,
-- so every build holds advisory lock (5393739, 0), "RMK" in ASCII, and a build
-- started during another waits for it. Run as one transaction, the way the
-- console sends this file or `psql -1`, the lock belongs to the transaction and
-- goes with it, failure included. Run statement by statement (psql \i), it
-- belongs to the session and is let go at the end of this file, or when psql
-- exits. scripts/update-road-markings.sh checks it before each cell, so a
-- nightly update steps aside for a full build instead of queueing behind it.
CREATE TEMP TABLE IF NOT EXISTS _rm_lock (xid xid8);
TRUNCATE _rm_lock;
INSERT INTO _rm_lock VALUES (pg_current_xact_id());
DO $$
BEGIN
  IF (SELECT xid FROM _rm_lock) = pg_current_xact_id() THEN
    PERFORM pg_advisory_xact_lock(5393739, 0);
  -- An interactive session can still hold it from a run that stopped partway.
  ELSIF NOT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()
                    AND classid = 5393739 AND objid = 0 AND objsubid = 2 AND granted) THEN
    PERFORM pg_advisory_lock(5393739, 0);
  END IF;
END
$$;

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

-- A way's corners rounded off, so a curve mapped as a few straight pieces
-- draws as a curve: each run between pinned vertices (`pins`, where something
-- else meets the way) is cut into pieces of at most `step` and corner-cut,
-- which bends it within half a step of each corner. Pins and ends stay put.
CREATE OR REPLACE FUNCTION road_smooth(line geometry, pins geometry, step float8) RETURNS geometry
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  pts geometry[] := ARRAY(SELECT geom FROM ST_DumpPoints(line) ORDER BY path);
  n int := array_length(pts, 1);
  out geometry[] := pts[1:1];
  run geometry[] := pts[1:1];
  i int;
BEGIN
  IF n IS NULL OR n < 3 OR GeometryType(line) <> 'LINESTRING' THEN RETURN line; END IF;
  FOR i IN 2..n LOOP
    run := run || pts[i];
    IF i = n OR (pins IS NOT NULL AND ST_DWithin(pts[i], pins, step * 1e-4)) THEN
      out := out || CASE WHEN array_length(run, 1) < 3 THEN pts[i:i] ELSE (ARRAY(
        SELECT geom FROM ST_DumpPoints(ST_Simplify(ST_ChaikinSmoothing(ST_Segmentize(ST_MakeLine(run), step), 3, true), step * 1e-3))
        ORDER BY path))[2:] END;
      run := pts[i:i];
    END IF;
  END LOOP;
  IF array_length(out, 1) < 2 THEN RETURN line; END IF;
  RETURN ST_SetSRID(ST_MakeLine(out), ST_SRID(line));
EXCEPTION WHEN OTHERS THEN
  RETURN line;
END
$$;

-- The area between two lines drawn the same way; NULL if they do not make a
-- polygon.
CREATE OR REPLACE FUNCTION road_between(l geometry, r geometry) RETURNS geometry
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN ST_CollectionExtract(ST_MakeValid(ST_MakePolygon(ST_AddPoint(ST_MakeLine(l, ST_Reverse(r)), ST_StartPoint(l)))), 3);
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
$$;

-- The carriageway between two kerb lines, rounded off at its far end.
CREATE OR REPLACE FUNCTION road_body(left_kerb geometry, right_kerb geometry, far_end geometry, radius float8)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_Union(road_between(left_kerb, right_kerb), ST_Buffer(far_end, radius, 8))
$$;

-- A length in metres from a width-style tag ("3.5", "3.5 m", "12 ft"), or NULL.
CREATE OR REPLACE FUNCTION road_metres(value text) RETURNS float8
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN value ~ '^\s*[0-9]+(\.[0-9]+)?\s*m?\s*$' THEN substring(value from '[0-9]+(?:\.[0-9]+)?')::float8
    WHEN value ~ '^\s*[0-9]+(\.[0-9]+)?\s*(ft|'')\s*$' THEN substring(value from '[0-9]+(?:\.[0-9]+)?')::float8 * 0.3048
  END
$$;

-- How far up the road hierarchy a class sits; a stop sign on a junction node
-- binds the approaches below the top.
CREATE OR REPLACE FUNCTION road_rank(class text) RETURNS int
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN class IN ('motorway', 'motorway_link') THEN 7
    WHEN class IN ('trunk', 'trunk_link') THEN 6
    WHEN class IN ('primary', 'primary_link') THEN 5
    WHEN class IN ('secondary', 'secondary_link') THEN 4
    WHEN class IN ('tertiary', 'tertiary_link') THEN 3
    WHEN class IN ('unclassified', 'residential', 'busway') THEN 2
    ELSE 1
  END
$$;

-- Metres of carriageway a parking lane takes on one side (OSM's left/right).
CREATE OR REPLACE FUNCTION road_parking_width(tags jsonb, side text) RETURNS float8
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN v IN ('lane', 'parallel', 'diagonal', 'perpendicular', 'marked') THEN
      CASE COALESCE(o, v) WHEN 'diagonal' THEN 4.5 WHEN 'perpendicular' THEN 5.0 ELSE 2.4 END
    WHEN v = 'half_on_kerb' THEN 1.2
    ELSE 0
  END
  FROM (SELECT COALESCE(tags->>('parking:' || side), tags->>'parking:both',
                        tags->>('parking:lane:' || side), tags->>'parking:lane:both') as v,
               COALESCE(tags->>('parking:' || side || ':orientation'), tags->>'parking:both:orientation') as o) t
$$;

-- A bike lane on one side (OSM's left/right): {metres including any painted
-- buffer, metres of buffer, 1 if a track}. In the Americas a track on a street
-- is a parking-protected lane on the carriageway, so it is paved and painted.
CREATE OR REPLACE FUNCTION road_bike(tags jsonb, side text, oneway boolean, americas boolean) RETURNS float8[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN v IN ('lane', 'exclusive_lane', 'opposite_lane') THEN ARRAY[COALESCE(w, 1.6) + b, b, 0]
    WHEN v IN ('track', 'opposite_track') AND americas THEN ARRAY[COALESCE(w, 1.8) + greatest(b, 0.9), greatest(b, 0.9), 1]
    ELSE ARRAY[0, 0, 0]::float8[]
  END
  FROM (
    SELECT v, road_metres(COALESCE(tags->>('cycleway:' || side || ':width'), tags->>'cycleway:both:width')) as w,
           COALESCE(road_metres(bv), CASE WHEN bv = 'yes' THEN 0.9 END, 0) as b
    FROM (SELECT COALESCE(tags->>('cycleway:' || side), tags->>'cycleway:both',
                   CASE WHEN side = 'right' AND tags->>'cycleway' NOT IN ('opposite_lane', 'opposite_track') THEN tags->>'cycleway'
                        WHEN side = 'left' AND (NOT oneway OR tags->>'cycleway' IN ('opposite_lane', 'opposite_track')) THEN tags->>'cycleway' END) as v,
                 COALESCE(tags->>('cycleway:' || side || ':buffer'), tags->>'cycleway:both:buffer', tags->>'cycleway:buffer') as bv) a
  ) t
$$;

-- The bus lanes among `n` lanes, numbered left to right as traffic sees them:
-- from a per-lane `bus:lanes` list, else a count of them at the kerb.
CREATE OR REPLACE FUNCTION road_bus_lanes(per_lane text, kerb_count int, n int) RETURNS int[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN n > 0 AND per_lane IS NOT NULL AND array_length(string_to_array(per_lane, '|'), 1) = n
      THEN ARRAY(SELECT i::int FROM unnest(string_to_array(per_lane, '|')) WITH ORDINALITY u(v, i) WHERE trim(v) = 'designated')
    WHEN kerb_count BETWEEN 1 AND n THEN ARRAY(SELECT generate_series(n - kerb_count + 1, n))
    ELSE '{}'::int[]
  END
$$;

-- A band of road `from_offset`..`to_offset` to the left of the line.
CREATE OR REPLACE FUNCTION road_strip(line geometry, from_offset float8, to_offset float8) RETURNS geometry
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN ST_Buffer(road_offset(line, (from_offset + to_offset) / 2), abs(to_offset - from_offset) / 2, 'endcap=flat join=mitre mitre_limit=2');
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
$$;

-- The band between two lines that ease across from offsets `a0`, `b0` to `a1`,
-- `b1` over `ease`: a lane that follows a tapering kerb.
CREATE OR REPLACE FUNCTION road_taper_band(line geometry, a0 float8, a1 float8, b0 float8, b1 float8, ease float8)
RETURNS geometry LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN road_between(road_taper(line, a0, a1, ease), road_taper(line, b0, b1, ease));
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
$$;

-- Where lane divider `i` of `n` (0 the split, `n` the kerb-side edge) stood on
-- the road before, lanes counted outward from `split` toward `edge` at `lw`. A
-- lane opening at the centre keeps the dividers counted from the kerb where they
-- were; one opening at the kerb keeps those counted from the centre.
CREATE OR REPLACE FUNCTION road_divider_start(i int, n int, split float8, edge float8, lw float8, opens_kerb boolean)
RETURNS float8 LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN edge <= split
    THEN greatest(edge, least(split, CASE WHEN opens_kerb THEN split - i * lw ELSE edge + (n - i) * lw END))
    ELSE least(edge, greatest(split, CASE WHEN opens_kerb THEN split + i * lw ELSE edge - (n - i) * lw END)) END
$$;

-- The part of `g` inside the box (or outside it), as `dim` (2 lines, 3
-- polygons). A scoped build cuts live rows it did not make, and GEOS refuses
-- some invalid polygons outright; those are repaired and cut again rather
-- than failing the whole box.
CREATE OR REPLACE FUNCTION road_clip(g geometry, box geometry, inside boolean, dim int) RETURNS geometry
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN ST_CollectionExtract(CASE WHEN inside THEN ST_Intersection(g, box) ELSE ST_Difference(g, box) END, dim);
EXCEPTION WHEN OTHERS THEN
  g := ST_MakeValid(g);
  RETURN ST_CollectionExtract(CASE WHEN inside THEN ST_Intersection(g, box) ELSE ST_Difference(g, box) END, dim);
END
$$;

-- Where a `placement` value ("left_of:2", "middle_of:1", "right_of:3") puts the
-- way, as an offset: lanes counted from `edge`, the left edge of the first lane
-- as its traffic sees it, stepping `dir` (-1 rightward along the way, +1 for
-- lanes running against it). NULL for "transition", a lane past the `lanes`
-- there are, or anything else.
CREATE OR REPLACE FUNCTION road_place(value text, edge float8, lane_w float8, dir int, lanes int) RETURNS float8
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT edge + dir * lane_w * CASE split_part(v, ':', 1)
    WHEN 'left_of' THEN n - 1 WHEN 'right_of' THEN n WHEN 'middle_of' THEN n - 0.5 END
  FROM (SELECT trim(value) as v) t, LATERAL (SELECT road_int(split_part(t.v, ':', 2)) as n) c
  WHERE t.v ~ '^(left_of|right_of|middle_of):[0-9]+$' AND n BETWEEN 1 AND lanes
$$;

-- Turn bays in a turn:lanes list of `n` lanes: {lanes at the left that only
-- turn left, lanes at the right that only turn right}. Through traffic keeps
-- to the lanes between, and those are what line up across a junction.
CREATE OR REPLACE FUNCTION road_turn_bays(turns text, n int) RETURNS int[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  WITH l AS (
    SELECT i, v ~ '(^|;)\s*(left|sharp_left|slight_left|reverse)\s*(;|$)' AND v !~ 'through|right' as lo,
              v ~ '(^|;)\s*(right|sharp_right|slight_right)\s*(;|$)' AND v !~ 'through|left|reverse' as ro
    FROM unnest(string_to_array(turns, '|')) WITH ORDINALITY u(v, i)
  ),
  b AS (
    SELECT COALESCE((SELECT min(i) FROM l WHERE NOT lo), n + 1)::int - 1 as lb,
           n - COALESCE((SELECT max(i) FROM l WHERE NOT ro), 0)::int as rb
  )
  SELECT CASE WHEN turns IS NULL OR n < 2 OR array_length(string_to_array(turns, '|'), 1) <> n OR lb + rb >= n
              THEN ARRAY[0, 0] ELSE ARRAY[lb, rb] END
  FROM b
$$;

-- A way road markings are built from: a street or road class above a track,
-- at grade, and a service road only where someone mapped its lanes (there are
-- more of those than of every other class together).
CREATE OR REPLACE FUNCTION road_is_marked_way(tags jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT tags->>'highway' IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link',
                              'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified',
                              'residential', 'living_street', 'service', 'busway')
    AND COALESCE(tags->>'tunnel', 'no') = 'no'
    AND COALESCE(tags->>'area', 'no') <> 'yes'
    AND (tags->>'highway' <> 'service' OR tags ?| ARRAY['lanes', 'lane_markings', 'width', 'width:carriageway'])
$$;

-- The unit direction {x, y} of a line at fraction `t`, over `reach` units
-- either side.
CREATE OR REPLACE FUNCTION road_dir(line geometry, t float8, reach float8) RETURNS float8[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN d > 0 THEN ARRAY[(ST_X(b) - ST_X(a)) / d, (ST_Y(b) - ST_Y(a)) / d] END
  FROM (SELECT ST_LineInterpolatePoint(line, greatest(0, t - reach / ST_Length(line))) as a,
               ST_LineInterpolatePoint(line, least(1, t + reach / ST_Length(line))) as b) e,
       LATERAL (SELECT ST_Distance(e.a, e.b) as d) dist
  WHERE ST_Length(line) > 0
$$;

-- ─── Scope ───────────────────────────────────────────────────────────────────
-- To rebuild one box rather than everything, create _rm_scope in the same
-- session first and put the box in it (EPSG:4326). Roads are read from a
-- margin around it, so junctions at the edge come out whole; what is written
-- is cut to the box and merged into the live tables, so neighbouring boxes
-- meet without seams; the live tables are created if this is the first box.
-- Without a scope the tables are rebuilt and swapped.
CREATE TEMP TABLE IF NOT EXISTS _rm_scope (box geometry(Polygon, 4326) NOT NULL);
DROP TABLE IF EXISTS _rm_area;
CREATE TEMP TABLE _rm_area AS
SELECT COALESCE((SELECT ST_Expand(box, 0.003) FROM _rm_scope LIMIT 1), ST_MakeEnvelope(-180, -90, 180, 90, 4326)) as area;

-- ─── Roads and their lanes ───────────────────────────────────────────────────
-- A way tagged oneway=-1 is turned to run with its traffic, so its OSM left
-- and right swap: `l` and `r` below name the sides of the geometry.
-- A road's place can come from the two ways before it (see Where the way
-- runs), so a scoped build also reads the ways meeting the ends of its roads
-- outside the area, and the ways meeting theirs. Those only inform; nothing is
-- drawn from them, and each way comes out the same whichever box builds it.
-- A full build reads geo_places as it goes rather than copying it here.
DROP TABLE IF EXISTS _rm_source;
CREATE TEMP TABLE _rm_source AS
SELECT osm_id, tags, geom, true as local
FROM geo_places
WHERE EXISTS (SELECT 1 FROM _rm_scope)
  AND geom_type = 'line' AND osm_type = 'W' AND tags ? 'highway' AND geom && (SELECT area FROM _rm_area) AND road_is_marked_way(tags);
CREATE INDEX ON _rm_source (osm_id);
DO $$
DECLARE
  -- The carry reads one way back, and the taper after it reads one more.
  reach constant int := 2;
  frontier geometry[];
BEGIN
  IF NOT EXISTS (SELECT 1 FROM _rm_scope) THEN RETURN; END IF;
  SELECT array_agg(pt) INTO frontier
  FROM _rm_source, LATERAL (VALUES (ST_StartPoint(geom)), (ST_EndPoint(geom))) e(pt)
  WHERE NOT ST_Intersects(pt, (SELECT area FROM _rm_area));
  FOR depth IN 1..reach LOOP
    EXIT WHEN frontier IS NULL;
    WITH found AS (
      INSERT INTO _rm_source
      SELECT DISTINCT ON (g.osm_id) g.osm_id, g.tags, g.geom, false
      FROM unnest(frontier) f(pt)
      JOIN geo_places g ON g.geom && ST_Expand(f.pt, 1e-7)
       AND g.geom_type = 'line' AND g.osm_type = 'W' AND g.tags ? 'highway'
       AND (ST_DWithin(ST_StartPoint(g.geom), f.pt, 1e-7) OR ST_DWithin(ST_EndPoint(g.geom), f.pt, 1e-7))
      WHERE road_is_marked_way(g.tags) AND NOT EXISTS (SELECT 1 FROM _rm_source x WHERE x.osm_id = g.osm_id)
      RETURNING geom
    )
    -- Onward only from the far end of what was found.
    SELECT array_agg(pt) INTO frontier
    FROM found, LATERAL (VALUES (ST_StartPoint(geom)), (ST_EndPoint(geom))) e(pt)
    WHERE NOT pt = ANY (frontier);
  END LOOP;
END
$$;

DROP TABLE IF EXISTS _rm_roads;
CREATE TEMP TABLE _rm_roads AS
WITH raw AS (
  SELECT osm_id, tags, tags->>'highway' as class,
         tags->>'oneway' = '-1' as flip,
         CASE WHEN tags->>'oneway' = '-1' THEN ST_Reverse(ST_Transform(geom, 3857)) ELSE ST_Transform(geom, 3857) END as g,
         1 / cos(radians(ST_Y(ST_Centroid(geom)))) as s,
         ST_X(ST_Centroid(geom)) < -30 as americas,
         COALESCE(tags->>'oneway' IN ('yes', 'true', '1', '-1')
           OR tags->>'junction' IN ('roundabout', 'circular')
           OR (tags->>'highway' IN ('motorway', 'motorway_link') AND COALESCE(tags->>'oneway', '') <> 'no'), false) as oneway,
         road_int(tags->>'lanes') as lanes_tag,
         COALESCE(road_int(tags->>'lanes:both_ways'), 0) as both_ways,
         COALESCE(road_metres(tags->>'width:carriageway'), road_metres(tags->>'width')) as width_tag,
         COALESCE(
           CASE WHEN tags->>'maxspeed' ~ 'mph' THEN substring(tags->>'maxspeed' from '^\s*([0-9]+)')::float8
                ELSE substring(tags->>'maxspeed' from '^\s*([0-9]+)\s*$')::float8 / 1.609 END,
           CASE WHEN tags->>'highway' IN ('motorway', 'motorway_link') THEN 60 WHEN tags->>'highway' IN ('trunk', 'trunk_link') THEN 50
                WHEN tags->>'highway' IN ('primary', 'primary_link') THEN 45 WHEN tags->>'highway' IN ('secondary', 'secondary_link') THEN 40
                WHEN tags->>'highway' IN ('tertiary', 'tertiary_link') THEN 35 ELSE 25 END) as mph,
         local
  FROM (SELECT osm_id, tags, geom, true as local FROM geo_places
        WHERE NOT EXISTS (SELECT 1 FROM _rm_scope) AND geom_type = 'line' AND osm_type = 'W' AND tags ? 'highway' AND road_is_marked_way(tags)
        UNION ALL
        SELECT osm_id, tags, geom, local FROM _rm_source) src
),
sided AS (
  SELECT r.*,
         road_bike(tags, CASE WHEN flip THEN 'left' ELSE 'right' END, oneway, americas) as bike_rt,
         road_bike(tags, CASE WHEN flip THEN 'right' ELSE 'left' END, oneway, americas) as bike_lt,
         road_parking_width(tags, CASE WHEN flip THEN 'left' ELSE 'right' END) as park_r,
         road_parking_width(tags, CASE WHEN flip THEN 'right' ELSE 'left' END) as park_l,
         COALESCE(tags->>CASE WHEN flip THEN 'busway:left' ELSE 'busway:right' END, tags->>'busway:both', tags->>'busway') = 'lane' as busway_r,
         COALESCE(tags->>CASE WHEN flip THEN 'busway:right' ELSE 'busway:left' END, tags->>'busway:both',
                  CASE WHEN NOT oneway THEN tags->>'busway' END) = 'lane' as busway_l,
         COALESCE(tags->>'lanes:bus:forward', tags->>'lanes:psv:forward') as bus_count_f,
         COALESCE(tags->>'lanes:bus:backward', tags->>'lanes:psv:backward') as bus_count_b
  FROM raw r
),
counted AS (
  SELECT r.*,
         COALESCE(r.lanes_tag, CASE
           WHEN r.class IN ('motorway', 'trunk') AND r.oneway THEN 2
           WHEN r.oneway THEN 1
           WHEN r.class = 'service' THEN 1
           ELSE 2 END) as lanes
  FROM sided r
),
split AS (
  SELECT c.*,
         greatest(CASE WHEN c.oneway THEN c.lanes - c.both_ways
              ELSE COALESCE(road_int(c.tags->>'lanes:forward'), ceil((c.lanes - c.both_ways) / 2.0)::int) END, 0) as fwd,
         greatest(CASE WHEN c.oneway THEN 0
              ELSE COALESCE(road_int(c.tags->>'lanes:backward'),
                            c.lanes - c.both_ways - COALESCE(road_int(c.tags->>'lanes:forward'), ceil((c.lanes - c.both_ways) / 2.0)::int)) END, 0) as bwd
  FROM counted c
)
SELECT osm_id, local, class, g, s, americas, oneway, flip, tags, both_ways, mph, fwd, bwd,
       -- Above the ground: a bridge, or anything mapped on a layer over it.
       COALESCE(tags->>'bridge', 'no') NOT IN ('no') OR COALESCE(road_int(tags->>'layer'), 0) > 0 as bridge,
       bike_rt[1] as bike_r, bike_rt[2] as buf_r, bike_rt[3] = 1 as track_r,
       bike_lt[1] as bike_l, bike_lt[2] as buf_l, bike_lt[3] = 1 as track_l,
       park_r, park_l,
       -- Width: tagged, else lanes at the class's lane width, bike lanes and parking.
       COALESCE(CASE WHEN width_tag BETWEEN 2.5 AND 60 THEN width_tag END,
                (fwd + bwd + both_ways) * road_lane_width(class) + bike_rt[1] + bike_lt[1] + park_r + park_l) as width,
       -- Marked: a lane count was mapped, or the class is one that is striped
       -- anyway, and nobody tagged it unmarked.
       COALESCE(tags->>'lane_markings', 'yes') <> 'no'
         AND class NOT IN ('service', 'living_street')
         AND (lanes_tag IS NOT NULL OR class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary',
                                                 'primary_link', 'secondary', 'secondary_link', 'tertiary')) as marked,
       COALESCE(tags->>'turn:lanes:forward', CASE WHEN oneway THEN tags->>'turn:lanes' END) as turn_forward,
       tags->>'turn:lanes:backward' as turn_backward,
       COALESCE(NULLIF(road_bus_lanes(
           COALESCE(tags->>'bus:lanes:forward', tags->>'psv:lanes:forward', CASE WHEN oneway THEN COALESCE(tags->>'bus:lanes', tags->>'psv:lanes') END),
           COALESCE(road_int(bus_count_f), CASE WHEN oneway THEN road_int(COALESCE(tags->>'lanes:bus', tags->>'lanes:psv')) ELSE road_int(tags->>'lanes:bus') / 2 END),
           fwd), '{}'),
         ARRAY(SELECT x FROM unnest(ARRAY[CASE WHEN busway_r THEN fwd END, CASE WHEN busway_l AND oneway THEN 1 END]) u(x) WHERE x > 0)) as bus_f,
       COALESCE(NULLIF(road_bus_lanes(
           COALESCE(tags->>'bus:lanes:backward', tags->>'psv:lanes:backward'),
           COALESCE(road_int(bus_count_b), CASE WHEN NOT oneway THEN road_int(tags->>'lanes:bus') / 2 END),
           bwd), '{}'),
         ARRAY(SELECT x FROM unnest(ARRAY[CASE WHEN busway_l AND NOT oneway THEN bwd END]) u(x) WHERE x > 0)) as bus_b
FROM split;
-- Indexed before anything is updated: an index built over rows updated in
-- the same transaction (a console build is one) is invisible to it.
CREATE INDEX ON _rm_roads USING gist (g);
CREATE INDEX ON _rm_roads (osm_id);
-- Curves drawn smooth. A vertex another road shares, or a crossing, signal,
-- stop sign or turning circle stands on, is where junctions and paint are
-- matched up, so it stays where it was mapped. Bridges keep their mapped line:
-- bridge decks are fitted against it.
DROP TABLE IF EXISTS _rm_pins;
CREATE TEMP TABLE _rm_pins AS
SELECT round(ST_X(dp.geom)::numeric, 2) as x, round(ST_Y(dp.geom)::numeric, 2) as y
FROM _rm_roads r, ST_DumpPoints(r.g) dp
GROUP BY 1, 2 HAVING count(*) > 1
UNION
SELECT round(ST_X(p)::numeric, 2), round(ST_Y(p)::numeric, 2)
FROM (SELECT ST_Transform(geom, 3857) as p FROM geo_places
      WHERE geom && (SELECT area FROM _rm_area) AND geom_type = 'point'
        AND tags->>'highway' IN ('crossing', 'traffic_signals', 'stop', 'turning_circle', 'turning_loop')) pts;
CREATE INDEX ON _rm_pins (x, y);
ANALYZE _rm_pins;
UPDATE _rm_roads r SET g = road_smooth(r.g, p.pins, 20 * r.s)
FROM (
  SELECT r2.osm_id, ST_Collect(dp.geom) FILTER (WHERE k.x IS NOT NULL) as pins
  FROM _rm_roads r2
  CROSS JOIN LATERAL ST_DumpPoints(r2.g) dp
  LEFT JOIN _rm_pins k ON k.x = round(ST_X(dp.geom)::numeric, 2) AND k.y = round(ST_Y(dp.geom)::numeric, 2)
  WHERE ST_NPoints(r2.g) > 2 AND NOT r2.bridge
  GROUP BY r2.osm_id
) p
WHERE r.osm_id = p.osm_id;
-- A tagged width too narrow for its lanes beside parking counts the parking out.
UPDATE _rm_roads SET park_r = 0, park_l = 0
WHERE park_r + park_l > 0 AND (width - bike_r - bike_l - park_r - park_l) / greatest(fwd + bwd + both_ways, 1) < 2.6;
-- Across the carriageway, left of the way's direction is positive: the left
-- kerb, its parking and bike lane, the backward lanes, any centre turn lane,
-- the forward lanes, then the right side's bike lane, parking and kerb.
-- `split_b` and `split_f` bound the turn lane; without one they meet. A bike
-- lane's paint runs from `*_a` at the kerb side to `*_b`; a track sits at the
-- kerb with the parking outside it, a lane outside the parking.
ALTER TABLE _rm_roads ADD COLUMN lane_w float8, ADD COLUMN left_edge float8, ADD COLUMN right_edge float8,
  ADD COLUMN split_b float8, ADD COLUMN split_f float8,
  ADD COLUMN br_a float8, ADD COLUMN br_b float8, ADD COLUMN bl_a float8, ADD COLUMN bl_b float8;
UPDATE _rm_roads SET lane_w = (width - bike_r - bike_l - park_r - park_l) / greatest(fwd + bwd + both_ways, 1);
UPDATE _rm_roads SET left_edge = width / 2 - bike_l - park_l, right_edge = -width / 2 + bike_r + park_r,
  br_a = -width / 2 + CASE WHEN track_r THEN 0 ELSE park_r END,
  bl_a = width / 2 - CASE WHEN track_l THEN 0 ELSE park_l END;
UPDATE _rm_roads SET br_b = br_a + bike_r - buf_r, bl_b = bl_a - bike_l + buf_l;
UPDATE _rm_roads SET split_b = left_edge - bwd * lane_w;
UPDATE _rm_roads SET split_f = split_b - both_ways * lane_w;
ANALYZE _rm_roads;

-- Ways that simply carry on into the next: one way ends where one other starts.
DROP TABLE IF EXISTS _rm_pairs;
CREATE TEMP TABLE _rm_pairs AS
WITH ends AS (
  SELECT osm_id, round(ST_X(ST_EndPoint(g))::numeric, 2) as x, round(ST_Y(ST_EndPoint(g))::numeric, 2) as y FROM _rm_roads
),
starts AS (
  SELECT osm_id, round(ST_X(ST_StartPoint(g))::numeric, 2) as x, round(ST_Y(ST_StartPoint(g))::numeric, 2) as y FROM _rm_roads
)
SELECT e.osm_id as prev_id, st.osm_id as next_id
FROM ends e JOIN starts st USING (x, y)
JOIN (SELECT x, y FROM ends GROUP BY x, y HAVING count(*) = 1) ue USING (x, y)
JOIN (SELECT x, y FROM starts GROUP BY x, y HAVING count(*) = 1) us USING (x, y)
WHERE e.osm_id <> st.osm_id;
CREATE INDEX ON _rm_pairs (next_id);

-- ─── Where the way runs ──────────────────────────────────────────────────────
-- The carriageway is centred on its way unless something says otherwise, and
-- `shift` moves it across. `placement` says so outright. A turn bay hangs off
-- its own side, so the lanes that carry on through a junction stay centred and
-- line up with the lanes beyond it; on a two-way road the centre line steps
-- across for a left bay, as built. A way where the lane count changes keeps the
-- kerb of the way before on the side that did not change (a one-way its left),
-- and the way after it eases back to centre (see Tapers): ways downstream are
-- drawn down the middle of the road. So a way's place depends on its own tags
-- and the way before it, and nothing further.
ALTER TABLE _rm_roads ADD COLUMN placed boolean NOT NULL DEFAULT false, ADD COLUMN base float8 NOT NULL DEFAULT 0,
  ADD COLUMN shift float8, ADD COLUMN kerb_l float8, ADD COLUMN kerb_r float8, ADD COLUMN axis geometry,
  ADD COLUMN bays_f int[], ADD COLUMN bays_b int[];
-- Lane offsets are in the geometry's direction, which for oneway=-1 runs
-- against the OSM way: its forward lanes are the tagged backward ones.
UPDATE _rm_roads SET bays_f = bf, bays_b = bb, placed = x IS NOT NULL, base = COALESCE(-x, CASE
    WHEN oneway THEN (bf[1] - bf[2]) * lane_w / 2
    ELSE (bf[1] - bf[2] - bb[1] + bb[2]) * lane_w / 2 END)
FROM (
  SELECT osm_id as id, road_turn_bays(turn_forward, fwd) as bf, road_turn_bays(turn_backward, bwd) as bb, CASE WHEN flip
    THEN COALESCE(road_place(tags->>'placement:backward', left_edge, lane_w, -1, fwd),
                  road_place(tags->>'placement', left_edge, lane_w, -1, fwd))
    ELSE COALESCE(road_place(tags->>'placement:forward', CASE WHEN oneway THEN left_edge ELSE split_f END, lane_w, -1, fwd),
                  road_place(tags->>'placement:backward', split_b, lane_w, 1, bwd),
                  road_place(tags->>'placement', left_edge, lane_w, -1, fwd + bwd + both_ways)) END as x
  FROM _rm_roads
) p
WHERE osm_id = p.id;
UPDATE _rm_roads b SET shift = COALESCE(c.kept, b.base)
FROM _rm_roads b2
LEFT JOIN LATERAL (
  -- The kerb of the lanes that carry on: the way before less its turn bays.
  SELECT CASE WHEN b2.oneway THEN a.left_edge + a.base - a.bays_f[1] * a.lane_w - b2.left_edge
              WHEN b2.bwd = a.bwd THEN a.left_edge + a.base - (a.bays_f[1] + a.bays_b[2]) * a.lane_w - b2.left_edge
              ELSE a.right_edge + a.base + (a.bays_b[1] + a.bays_f[2]) * a.lane_w - b2.right_edge END as kept
  FROM _rm_pairs p JOIN _rm_roads a ON a.osm_id = p.prev_id
  WHERE p.next_id = b2.osm_id AND b2.oneway = a.oneway AND NOT b2.placed
    AND b2.bays_f = ARRAY[0, 0] AND b2.bays_b = ARRAY[0, 0]
    AND (b2.fwd <> a.fwd OR b2.bwd <> a.bwd) AND (b2.oneway OR b2.fwd = a.fwd OR b2.bwd = a.bwd)
) c ON true
WHERE b.osm_id = b2.osm_id;
-- The carriageway's own centreline, where it is not the way; a way GEOS cannot
-- offset stays on its way.
UPDATE _rm_roads SET axis = road_offset(g, shift * s) WHERE abs(shift) >= 0.05;
UPDATE _rm_roads SET shift = 0, axis = NULL WHERE abs(shift) >= 0.05 AND COALESCE(GeometryType(axis), '') <> 'LINESTRING';
UPDATE _rm_roads SET left_edge = left_edge + shift, right_edge = right_edge + shift,
  split_b = split_b + shift, split_f = split_f + shift,
  br_a = br_a + shift, br_b = br_b + shift, bl_a = bl_a + shift, bl_b = bl_b + shift,
  kerb_l = width / 2 + shift, kerb_r = -width / 2 + shift,
  axis = CASE WHEN local THEN axis END;

-- ─── Tapers ──────────────────────────────────────────────────────────────────
-- Where one road simply continues into the next, the next starts from the
-- first's lane layout and eases into its own: a lane opening or closing drifts
-- across over a taper rather than jumping sideways at the join. The taper runs
-- to the MUTCD's length for the road's speed (L = WS²/60 ft up to 40 mph,
-- WS above), short of the road's own length.
ALTER TABLE _rm_roads
  ADD COLUMN prev_left float8, ADD COLUMN prev_right float8, ADD COLUMN prev_kerb_l float8, ADD COLUMN prev_kerb_r float8,
  ADD COLUMN prev_split_b float8, ADD COLUMN prev_split_f float8, ADD COLUMN prev_lane_w float8, ADD COLUMN ease float8,
  ADD COLUMN opens_kerb_f boolean, ADD COLUMN opens_kerb_b boolean, ADD COLUMN body geometry;
UPDATE _rm_roads b SET
  prev_left = a.left_edge, prev_right = a.right_edge, prev_kerb_l = a.kerb_l, prev_kerb_r = a.kerb_r,
  prev_split_b = a.split_b, prev_split_f = a.split_f, prev_lane_w = a.lane_w
FROM _rm_pairs p JOIN _rm_roads a ON a.osm_id = p.prev_id
WHERE b.osm_id = p.next_id AND a.marked = b.marked
  AND NOT (a.oneway <> b.oneway)
  AND (abs(a.kerb_l - b.kerb_l) > 0.2 OR abs(a.kerb_r - b.kerb_r) > 0.2
       OR abs(a.left_edge - b.left_edge) > 0.2 OR abs(a.right_edge - b.right_edge) > 0.2
       OR abs(a.split_b - b.split_b) > 0.2 OR abs(a.lane_w - b.lane_w) > 0.2);
UPDATE _rm_roads SET
  ease = least(0.6 * ST_Length(g) / s, greatest(15,
           CASE WHEN mph >= 45 THEN lane_w * 3.281 * mph ELSE lane_w * 3.281 * mph * mph / 60 END / 3.281)),
  opens_kerb_f = COALESCE(fwd > 0 AND split_part(turn_forward, '|', greatest(fwd, 1)) LIKE '%right%' AND split_part(turn_forward, '|', 1) NOT LIKE '%left%', false),
  opens_kerb_b = COALESCE(bwd > 0 AND split_part(turn_backward, '|', greatest(bwd, 1)) LIKE '%right%' AND split_part(turn_backward, '|', 1) NOT LIKE '%left%', false)
WHERE prev_left IS NOT NULL;
-- A tapering road's carriageway, between its two eased kerbs; a kerb that
-- stays put does not move.
UPDATE _rm_roads SET body = road_body(l, r, ST_EndPoint(COALESCE(axis, g)), width / 2 * s)
FROM (SELECT osm_id as id,
             road_taper(g, prev_kerb_l * s, kerb_l * s, ease * s) as l,
             road_taper(g, prev_kerb_r * s, kerb_r * s, ease * s) as r
      FROM _rm_roads WHERE prev_left IS NOT NULL AND local) e
WHERE osm_id = e.id AND e.l IS NOT NULL AND e.r IS NOT NULL;

-- Ways read only to place the ones in the area have done their part.
DELETE FROM _rm_roads WHERE NOT local;
ANALYZE _rm_roads;

-- ─── Junctions ───────────────────────────────────────────────────────────────
-- Every road vertex shared with another road, or where a road ends into
-- another: `degree` counts a road passing through as two. Service roads are
-- driveways and alleys to the lanes on a street, so a junction for the
-- markings needs three street ends without them.
DROP TABLE IF EXISTS _rm_incid;
CREATE TEMP TABLE _rm_incid AS
SELECT osm_id, class, width, s, bridge, p, is_end,
       round(ST_X(p)::numeric, 2) as x, round(ST_Y(p)::numeric, 2) as y,
       degrees(ST_Azimuth(prev_p, next_p)) as bearing
FROM (
  SELECT r.osm_id, r.class, r.width, r.s, r.bridge, dp.geom as p,
         dp.path[1] IN (1, ST_NPoints(r.g)) as is_end,
         COALESCE(lag(dp.geom) OVER w, dp.geom) as prev_p, COALESCE(lead(dp.geom) OVER w, dp.geom) as next_p
  FROM _rm_roads r, ST_DumpPoints(r.g) dp
  WINDOW w AS (PARTITION BY r.osm_id ORDER BY dp.path[1])
) v;
CREATE INDEX ON _rm_incid (x, y);
ANALYZE _rm_incid;

-- A road ending against a bridge, or a bridge itself, ends square at the
-- abutment rather than rounding out under or over the other.
ALTER TABLE _rm_roads ADD COLUMN cap text NOT NULL DEFAULT 'endcap=round join=round quad_segs=8';
UPDATE _rm_roads r SET cap = 'endcap=flat join=round quad_segs=8'
WHERE r.bridge OR EXISTS (
  SELECT 1 FROM _rm_incid a JOIN _rm_incid b ON b.x = a.x AND b.y = a.y AND b.bridge <> a.bridge
  WHERE a.osm_id = r.osm_id AND a.is_end);

DROP TABLE IF EXISTS _rm_vertices;
CREATE TEMP TABLE _rm_vertices AS
SELECT x, y, (array_agg(p))[1] as p, max(width) as width, max(s) as s,
       COALESCE(sum(CASE WHEN is_end THEN 1 ELSE 2 END) FILTER (WHERE class <> 'service'), 0) as street_degree,
       COALESCE(max(road_rank(class)) FILTER (WHERE class <> 'service'), 0) as top_rank
FROM _rm_incid
GROUP BY x, y
HAVING count(*) > 1;
CREATE INDEX ON _rm_vertices (x, y);
DROP TABLE IF EXISTS _rm_nodes;
CREATE TEMP TABLE _rm_nodes AS SELECT * FROM _rm_vertices WHERE street_degree >= 3;
CREATE INDEX ON _rm_nodes (x, y);
CREATE INDEX ON _rm_nodes USING gist (p);
ANALYZE _rm_nodes;

-- The roads that cross each road at a junction, rather than carry on from it:
-- its lines break for their carriageways, and its stop line stands clear of
-- the widest of them.
DROP TABLE IF EXISTS _rm_cross;
CREATE TEMP TABLE _rm_cross AS
SELECT a.osm_id, n.x, n.y, n.p, b.osm_id as other_id, b.width as other_w,
       abs(sin(radians(a.bearing - b.bearing))) as sine
FROM _rm_nodes n
JOIN _rm_incid a ON a.x = n.x AND a.y = n.y
JOIN _rm_incid b ON b.x = n.x AND b.y = n.y AND b.osm_id <> a.osm_id
WHERE b.class <> 'service' AND abs(sin(radians(a.bearing - b.bearing))) > 0.42;

DROP TABLE IF EXISTS _rm_approach;
CREATE TEMP TABLE _rm_approach AS
SELECT DISTINCT ON (osm_id, x, y) osm_id, x, y, other_w as cross_w, sine
FROM _rm_cross ORDER BY osm_id, x, y, other_w DESC;
CREATE INDEX ON _rm_approach (osm_id, x, y);

DROP TABLE IF EXISTS _rm_road_cuts;
CREATE TEMP TABLE _rm_road_cuts AS
SELECT c.osm_id, ST_Union(ST_Buffer(ST_Intersection(COALESCE(o.axis, o.g), ST_Expand(c.p, (r.width + 6) * r.s)),
                                    (o.width / 2 + 1) * o.s, 'endcap=flat join=round quad_segs=4')) as cut
FROM _rm_cross c
JOIN _rm_roads r ON r.osm_id = c.osm_id
JOIN _rm_roads o ON o.osm_id = c.other_id
WHERE r.marked
GROUP BY c.osm_id;
CREATE INDEX ON _rm_road_cuts (osm_id);

-- Signals control every approach to their junction. A stop sign binds the
-- approach it stands on, facing its traffic; on the junction node itself, the
-- approaches below the top road, or all of them at an all-way stop.
DROP TABLE IF EXISTS _rm_signals;
CREATE TEMP TABLE _rm_signals AS
SELECT ST_Transform(geom, 3857) as g, tags->>'highway' as kind,
       COALESCE(tags->>'direction', tags->>'traffic_signals:direction') as direction,
       COALESCE(tags->>'stop' = 'all', false) as all_way
FROM geo_places
WHERE geom && (SELECT area FROM _rm_area) AND geom_type = 'point'
  AND (tags @> '{"highway": "traffic_signals"}' OR tags @> '{"highway": "stop"}');
CREATE INDEX ON _rm_signals USING gist (g);
ANALYZE _rm_signals;

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
WHERE geom && (SELECT area FROM _rm_area)
  AND ((geom_type = 'line' AND tags->>'footway' = 'crossing') OR (geom_type = 'point' AND tags->>'highway' = 'crossing'));
DELETE FROM _rm_crossings WHERE style IS NULL;
CREATE INDEX ON _rm_crossings USING gist (g);
ANALYZE _rm_crossings;

-- ─── Turning circles ────────────────────────────────────────────────────────
-- A round bulb of carriageway centred on the node, its radius half the tagged
-- `diameter` (6 to 20 m), else 11 m on a street and 8 m on a service road, and
-- never inside the road's own kerbs. The kerb flares into it on 8 m fillets
-- rather than meeting it at a notch. A turning loop runs round an island left
-- unpaved, the small area mapped under its node or else a disc 5.5 m inside a
-- 14 m kerb. Nodes on a bridge or at a junction are left as they are.
DROP TABLE IF EXISTS _rm_turns;
CREATE TEMP TABLE _rm_turns AS
WITH nodes AS MATERIALIZED (
  SELECT ST_Transform(g.geom, 3857) as p, g.tags->>'highway' = 'turning_loop' as loop, road_metres(g.tags->>'diameter') as diameter,
         -- The part holding the node, within 25 m of it all round: a long
         -- strip or a far-flung multipolygon would pave a field.
         CASE WHEN g.tags->>'highway' = 'turning_loop' THEN (
           SELECT ST_CollectionExtract(ST_MakeValid(ST_Transform(d.geom, 3857)), 3)
           FROM geo_places a, LATERAL (SELECT geom FROM ST_Dump(a.geom) WHERE ST_Contains(geom, g.geom) LIMIT 1) d
           WHERE a.geom && g.geom AND a.geom_type = 'area' AND NOT a.tags ?| ARRAY['building', 'highway', 'area:highway', 'amenity']
             AND ST_XMax(a.geom) - ST_XMin(a.geom) < 0.001 AND ST_YMax(a.geom) - ST_YMin(a.geom) < 0.001
             AND ST_Area(a.geom::geography) < 1200
             AND ST_MaxDistance(ST_Transform(d.geom, 3857), ST_Transform(g.geom, 3857)) < 25 / cos(radians(ST_Y(g.geom)))
           ORDER BY ST_Area(a.geom), a.osm_type, a.osm_id LIMIT 1) END as mapped_island
  FROM geo_places g
  WHERE g.geom && (SELECT area FROM _rm_area) AND g.geom_type = 'point' AND g.tags->>'highway' IN ('turning_circle', 'turning_loop')
),
sized AS (
  SELECT t.*, r.ids, r.s, r.width, r.edged, 8.0::float8 as fillet,
         greatest(CASE WHEN t.diameter > 0 THEN least(greatest(t.diameter / 2, 6), 20)
                       WHEN r.class = 'service' THEN 8 WHEN t.loop THEN 14 ELSE 11 END,
                  ST_MaxDistance(t.p, t.mapped_island) / r.s + 5.5, r.width / 2 + 2) as radius
  FROM nodes t
  CROSS JOIN LATERAL (
    SELECT array_agg(r.osm_id) as ids, max(r.s) as s, max(r.width) as width, bool_or(r.bridge) as bridge,
           (array_agg(r.class ORDER BY road_rank(r.class) DESC, r.class))[1] as class,
           bool_or(r.marked AND r.class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')) as edged
    FROM _rm_roads r WHERE r.g && ST_Expand(t.p, 1) AND ST_DWithin(r.g, t.p, 0.05)
  ) r
  WHERE r.ids IS NOT NULL AND NOT r.bridge
    AND NOT EXISTS (SELECT 1 FROM _rm_nodes n WHERE n.x = round(ST_X(t.p)::numeric, 2) AND n.y = round(ST_Y(t.p)::numeric, 2))
)
-- The bulb and its road closed over the fillet, kept within reach of the node.
-- Paint ends where the kerb starts to flare: `paint_end` from the node.
SELECT t.p, t.ids, t.s, t.edged, t.radius, (t.radius + 2 * t.fillet) * t.s as reach,
       (sqrt(power(t.radius + t.fillet, 2) - power(t.width / 2 + t.fillet, 2)) + 2) * t.s as paint_end,
       COALESCE(CASE WHEN NOT ST_IsEmpty(t.mapped_island) THEN t.mapped_island END, CASE WHEN t.loop AND t.radius - 5.5 >= 2.5 THEN ST_Buffer(t.p, (t.radius - 5.5) * t.s, 16) END) as island,
       ST_Intersection(
         ST_Buffer(ST_Buffer(ST_Union(u.road, ST_Buffer(t.p, t.radius * t.s, 16)), t.fillet * t.s, 'quad_segs=16'), -t.fillet * t.s, 'quad_segs=16'),
         ST_Buffer(t.p, (t.radius + 2 * t.fillet) * t.s, 32)) as g
FROM sized t
CROSS JOIN LATERAL (
  SELECT ST_Union(COALESCE(ST_Intersection(r.body, box), ST_Buffer(ST_Intersection(COALESCE(r.axis, r.g), box), r.width / 2 * r.s, r.cap))) as road
  FROM _rm_roads r, LATERAL (SELECT ST_Expand(t.p, (t.radius + 2 * t.fillet) * t.s * 2) as box) b
  WHERE r.osm_id = ANY (t.ids)
) u;
DELETE FROM _rm_turns WHERE g IS NULL OR ST_IsEmpty(g);
CREATE INDEX ON _rm_turns USING gist (g);
ANALYZE _rm_turns;

-- ─── Surfaces ────────────────────────────────────────────────────────────────
-- Each road widened to its carriageway, and at every vertex roads share, the
-- corners between just those roads filled to a kerb radius. Rounding only
-- what meets keeps a median, an island or a plaza between two roads that
-- never join open. Unioned per grid cell, each keeping only its own square,
-- so the seams between cells match.
DROP TABLE IF EXISTS _rm_fillets;
CREATE TEMP TABLE _rm_fillets AS
SELECT bridge, p, ST_Intersection(
         -- Where the reach cuts across a corner's fill, the kerb turns off
         -- round rather than at a notch.
         ST_Buffer(ST_Buffer(ST_Union(u, ST_Intersection(
           ST_Buffer(ST_Buffer(u, 4 * s, 'quad_segs=8'), -4 * s, 'quad_segs=8'),
           ST_Buffer(p, reach, 16))), 2 * s, 'quad_segs=8'), -2 * s, 'quad_segs=8'),
         ST_Buffer(p, reach + 4 * s, 16)) as g
FROM (
  SELECT i.bridge, v.p, v.s, (v.width * 0.75 + 10) * v.s as reach,
         ST_Union(COALESCE(ST_Intersection(r.body, ST_Expand(v.p, (v.width + 10 + r.width) * v.s)),
                  ST_Buffer(ST_Intersection(COALESCE(r.axis, r.g), ST_Expand(v.p, (v.width + 10 + r.width) * v.s)), r.width / 2 * r.s,
                            r.cap))) as u
  FROM _rm_vertices v
  JOIN _rm_incid i ON i.x = v.x AND i.y = v.y
  JOIN _rm_roads r ON r.osm_id = i.osm_id
  GROUP BY i.bridge, v.p, v.s, v.width
) pieces;
DELETE FROM _rm_fillets WHERE g IS NULL OR ST_IsEmpty(g);
CREATE INDEX ON _rm_fillets USING gist (g);

DROP TABLE IF EXISTS _rm_surfaces;
CREATE TEMP TABLE _rm_surfaces (bridge boolean, g geometry);
-- Every cell a carriageway reaches, not only those its centreline passes.
WITH cells AS (
  SELECT DISTINCT cx, cy
  FROM _rm_roads r,
       LATERAL (SELECT (ST_DumpPoints(ST_Segmentize(r.g, 200))).geom as c) d,
       LATERAL (SELECT (r.width / 2 + abs(r.shift) + 15) * r.s as reach) m,
       generate_series(floor((ST_X(d.c) - m.reach) / 800)::int, floor((ST_X(d.c) + m.reach) / 800)::int) cx,
       generate_series(floor((ST_Y(d.c) - m.reach) / 800)::int, floor((ST_Y(d.c) + m.reach) / 800)::int) cy
  UNION
  SELECT cx, cy
  FROM _rm_turns t,
       generate_series(floor(ST_XMin(t.g) / 800)::int, floor(ST_XMax(t.g) / 800)::int) cx,
       generate_series(floor(ST_YMin(t.g) / 800)::int, floor(ST_YMax(t.g) / 800)::int) cy
),
boxes AS (
  SELECT cx, cy, ST_MakeEnvelope(cx * 800, cy * 800, (cx + 1) * 800, (cy + 1) * 800, 3857) as box FROM cells
),
pieces AS (
  SELECT b.cx, b.cy, b.box, r.bridge,
         COALESCE(ST_Intersection(r.body, ST_Expand(b.box, 60)),
                  ST_Buffer(ST_Intersection(COALESCE(r.axis, r.g), ST_Expand(b.box, 60)), r.width / 2 * r.s, r.cap)) as g
  FROM boxes b JOIN _rm_roads r ON r.g && ST_Expand(b.box, 60)
  UNION ALL
  SELECT b.cx, b.cy, b.box, f.bridge, f.g
  FROM boxes b JOIN _rm_fillets f ON f.g && b.box
  UNION ALL
  SELECT b.cx, b.cy, b.box, false, t.g
  FROM boxes b JOIN _rm_turns t ON t.g && b.box
)
-- Rounded kerbs carry far more vertices than a tile can show; 3 cm is well
-- under a pixel at any zoom the tiles are drawn at.
INSERT INTO _rm_surfaces (bridge, g)
SELECT bridge, ST_CollectionExtract(ST_MakeValid(ST_Intersection(ST_CollectionExtract(ST_MakeValid(
         ST_Simplify(ST_Union(g), 0.03 / cos(radians(ST_Y(ST_Transform(ST_Centroid(box), 4326)))))), 3), box)), 3)
FROM pieces
GROUP BY cx, cy, box, bridge;
-- Indexed before the update below, so a one-transaction build can still use it.
CREATE INDEX ON _rm_surfaces USING gist (g);
-- Turning loops ring their islands.
UPDATE _rm_surfaces su SET g = ST_CollectionExtract(ST_Difference(su.g, i.g), 3)
FROM (SELECT su2.ctid as row, ST_Union(t.island) as g
      FROM _rm_turns t JOIN _rm_surfaces su2 ON su2.g && t.island AND NOT su2.bridge
      WHERE t.island IS NOT NULL
      GROUP BY su2.ctid) i
WHERE su.ctid = i.row;
DELETE FROM _rm_surfaces WHERE ST_IsEmpty(g);
DROP TABLE IF EXISTS road_surfaces_next;
CREATE TABLE road_surfaces_next (fid bigserial CONSTRAINT road_surfaces_next_pk PRIMARY KEY, bridge boolean, kind text, geom geometry(MultiPolygon, 4326));
INSERT INTO road_surfaces_next (bridge, geom) SELECT bridge, ST_Multi(ST_Transform(g, 4326)) FROM _rm_surfaces;
-- An island is drawn over whatever the basemap runs into it.
INSERT INTO road_surfaces_next (bridge, kind, geom)
SELECT false, 'island', ST_Multi(ST_Transform(island, 4326)) FROM _rm_turns WHERE island IS NOT NULL;
CREATE INDEX road_surfaces_next_geom_idx ON road_surfaces_next USING gist (geom);

-- What lane lines break for besides crossing roads: the crosswalks over them,
-- unioned once per grid cell rather than once per line.
DROP TABLE IF EXISTS _rm_cuts;
CREATE TEMP TABLE _rm_cuts AS
SELECT ST_Union(cut) as cut
FROM (
  SELECT floor(ST_X(c.cxy) / 400) as cx, floor(ST_Y(c.cxy) / 400) as cy, ST_Buffer(c.g, 2 * c.s, 2) as cut
  FROM (SELECT g, s, ST_PointOnSurface(g) as cxy FROM _rm_crossings) c
) cuts
GROUP BY cx, cy;
CREATE INDEX ON _rm_cuts USING gist (cut);
-- Lane and centre lines stop short of a turning circle; edge lines run on round
-- its kerb, so theirs are cut only where the kerb line around it takes over.
DROP TABLE IF EXISTS _rm_turn_cuts;
CREATE TEMP TABLE _rm_turn_cuts AS
SELECT ids, ST_Buffer(p, paint_end, 16) as cut, false as edge FROM _rm_turns
UNION ALL
SELECT ids, ST_Buffer(p, reach - 0.5 * s, 32), true FROM _rm_turns WHERE edged;
CREATE INDEX ON _rm_turn_cuts USING gist (cut);

-- ─── Lane lines ──────────────────────────────────────────────────────────────
-- Offsets are to the left of the way's direction. Backward lanes run on the
-- left and forward lanes on the right; `split` is the line between them.
DROP TABLE IF EXISTS road_markings_next;
CREATE TABLE road_markings_next (
  fid bigserial CONSTRAINT road_markings_next_pk PRIMARY KEY, kind text, pattern text, color text, style text, bridge boolean,
  geom geometry(Geometry, 4326));

DROP TABLE IF EXISTS _rm_lines;
CREATE TEMP TABLE _rm_lines AS
-- Motorways keep the long highway dash; streets take a shorter one.
WITH lanes AS (
  SELECT *, CASE WHEN class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') THEN 'dashed_long' ELSE 'dashed' END as dash
  FROM _rm_roads WHERE marked
),
centre AS (
  -- Two directions are kept apart by a double line unless passing is allowed:
  -- in the Americas only where it is tagged, elsewhere off the main roads.
  SELECT *, CASE
    WHEN tags->>'overtaking' = 'no' OR both_ways > 0 OR fwd + bwd >= 4 THEN 'double'
    WHEN 'yes' IN (tags->>'overtaking', tags->>'overtaking:forward', tags->>'overtaking:backward') THEN dash
    WHEN americas OR class IN ('primary', 'secondary', 'trunk') THEN 'double'
    ELSE dash END as centre_pattern,
    CASE WHEN americas THEN 'yellow' ELSE 'white' END as centre_color
  FROM lanes
)
-- The centre line between the two directions; a centre turn lane is bounded
-- by a line on each side. `start_m` is where a line begins when it eases in
-- from the road before; see Tapers.
SELECT osm_id, g, s, bridge, ease, 'centre' as kind, centre_pattern as pattern, centre_color as color,
       split_b as offset_m, prev_split_b as start_m
FROM centre WHERE NOT oneway AND fwd > 0 AND bwd > 0
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'centre', 'double', centre_color, split_f, prev_split_f
FROM centre WHERE NOT oneway AND both_ways > 0
UNION ALL
-- Lane dividers within each direction, easing in from where they stood on the
-- road before (see road_divider_start). A divider with no counterpart there
-- emerges from the line it is clamped to.
SELECT osm_id, g, s, bridge, ease, 'lane', dash, 'white', split_f - i * lane_w,
       CASE WHEN prev_left IS NOT NULL THEN road_divider_start(i, fwd, prev_split_f, prev_right, prev_lane_w, opens_kerb_f) END
FROM lanes, generate_series(1, 8) i WHERE i < fwd
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'lane', dash, 'white', split_b + i * lane_w,
       CASE WHEN prev_left IS NOT NULL THEN road_divider_start(i, bwd, prev_split_b, prev_left, prev_lane_w, opens_kerb_b) END
FROM lanes, generate_series(1, 8) i WHERE i < bwd
UNION ALL
-- Edge lines on roads without kerbs: motorways, trunks and their links.
SELECT osm_id, g, s, bridge, ease, 'edge', 'solid', 'white', right_edge + 0.3, prev_right + 0.3
FROM lanes WHERE class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'edge', 'solid', CASE WHEN americas AND oneway THEN 'yellow' ELSE 'white' END, left_edge - 0.3, prev_left - 0.3
FROM lanes WHERE class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')
UNION ALL
-- Bike lanes: a solid line on the traffic side of the paint, another past any
-- buffer, and one against parking on the kerb side. They keep their distance
-- from a kerb that tapers.
SELECT osm_id, g, s, bridge, ease, 'bike', 'solid', 'white', o, prev_kerb_r + o - kerb_r
FROM _rm_roads, LATERAL (VALUES (br_b), (CASE WHEN buf_r > 0 THEN br_b + buf_r END),
                                (CASE WHEN park_r > 0 AND NOT track_r THEN br_a END)) v(o)
WHERE bike_r > 0 AND o IS NOT NULL
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'bike', 'solid', 'white', o, prev_kerb_l + o - kerb_l
FROM _rm_roads, LATERAL (VALUES (bl_b), (CASE WHEN buf_l > 0 THEN bl_b - buf_l END),
                                (CASE WHEN park_l > 0 AND NOT track_l THEN bl_a END)) v(o)
WHERE bike_l > 0 AND o IS NOT NULL;

-- Lane lines stop where another road's carriageway crosses, and at crosswalks.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT l.kind, l.pattern, l.color, l.bridge, ST_Transform(clipped, 4326)
FROM _rm_lines l
CROSS JOIN LATERAL (SELECT CASE
  WHEN l.start_m IS NOT NULL AND abs(l.start_m - l.offset_m) > 0.2
    THEN road_taper(ST_Simplify(l.g, 0.05 * l.s), l.start_m * l.s, l.offset_m * l.s, l.ease * l.s)
  ELSE road_offset(ST_Simplify(l.g, 0.05 * l.s), l.offset_m * l.s) END as line) o
CROSS JOIN LATERAL (
  SELECT ST_LineMerge(ST_CollectionExtract(COALESCE(ST_Difference(o.line, ST_Union(cut)), o.line), 2)) as clipped
  FROM (SELECT k.cut FROM _rm_cuts k WHERE k.cut && o.line
        UNION ALL SELECT rc.cut FROM _rm_road_cuts rc WHERE rc.osm_id = l.osm_id
        UNION ALL SELECT tc.cut FROM _rm_turn_cuts tc WHERE l.osm_id = ANY (tc.ids) AND tc.cut && o.line AND tc.edge = (l.kind = 'edge')) cuts
) c
WHERE o.line IS NOT NULL AND NOT ST_IsEmpty(c.clipped)
  AND ST_Length(c.clipped) > 2 * l.s;

-- Edge lines on round a turning circle's kerb.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'edge', 'solid', 'white', false, ST_Transform(ST_LineMerge(ST_CollectionExtract(
         ST_Intersection(ST_Boundary(ST_Buffer(g, -0.3 * s, 'quad_segs=8')), ST_Buffer(p, reach - 0.5 * s, 32)), 2)), 4326)
FROM _rm_turns WHERE edged;

-- Coloured lanes: bike lanes green, bus lanes red, as bands of paint that
-- stop where lane lines do.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT b.kind, 'fill', b.color, b.bridge, ST_Transform(ST_Multi(clipped), 4326)
FROM (
  SELECT osm_id, g, s, bridge, 'bike_lane' as kind, 'green' as color, br_a as a, br_b as b,
         prev_kerb_r - kerb_r as d, ease, NULL::float8 as d2 FROM _rm_roads WHERE bike_r > 0
  UNION ALL
  SELECT osm_id, g, s, bridge, 'bike_lane', 'green', bl_a, bl_b, prev_kerb_l - kerb_l, ease, NULL FROM _rm_roads WHERE bike_l > 0
  UNION ALL
  -- A bus lane eases in with the dividers either side of it.
  SELECT osm_id, g, s, bridge, 'bus_lane', 'red', split_f - (i - 1) * lane_w, split_f - i * lane_w,
         road_divider_start(i - 1, fwd, prev_split_f, prev_right, prev_lane_w, opens_kerb_f) - (split_f - (i - 1) * lane_w),
         ease, road_divider_start(i, fwd, prev_split_f, prev_right, prev_lane_w, opens_kerb_f) - (split_f - i * lane_w)
  FROM _rm_roads, unnest(bus_f) i
  UNION ALL
  SELECT osm_id, g, s, bridge, 'bus_lane', 'red', split_b + (i - 1) * lane_w, split_b + i * lane_w,
         road_divider_start(i - 1, bwd, prev_split_b, prev_left, prev_lane_w, opens_kerb_b) - (split_b + (i - 1) * lane_w),
         ease, road_divider_start(i, bwd, prev_split_b, prev_left, prev_lane_w, opens_kerb_b) - (split_b + i * lane_w)
  FROM _rm_roads, unnest(bus_b) i
) b
CROSS JOIN LATERAL (SELECT COALESCE(CASE
  WHEN abs(COALESCE(b.d, 0)) > 0.2 OR abs(COALESCE(b.d2, 0)) > 0.2
    THEN road_taper_band(ST_Simplify(b.g, 0.05 * b.s), (b.a + b.d) * b.s, b.a * b.s, (b.b + COALESCE(b.d2, b.d)) * b.s, b.b * b.s, b.ease * b.s) END,
  road_strip(ST_Simplify(b.g, 0.05 * b.s), b.a * b.s, b.b * b.s)) as band) o
CROSS JOIN LATERAL (
  SELECT ST_CollectionExtract(COALESCE(ST_Difference(o.band, ST_Union(cut)), o.band), 3) as clipped
  FROM (SELECT k.cut FROM _rm_cuts k WHERE k.cut && o.band
        UNION ALL SELECT rc.cut FROM _rm_road_cuts rc WHERE rc.osm_id = b.osm_id
        UNION ALL SELECT tc.cut FROM _rm_turn_cuts tc WHERE b.osm_id = ANY (tc.ids) AND tc.cut && o.band AND NOT tc.edge) cuts
) c
WHERE o.band IS NOT NULL AND NOT ST_IsEmpty(c.clipped) AND ST_Area(c.clipped) > 4 * b.s * b.s;

-- ─── Bike lanes through junctions ────────────────────────────────────────────
-- A bike lane arriving at a junction that carries straight on into a bike lane
-- on the far side, on the same side of the traffic, is painted across it: green
-- blocks between dashed lines, as US conflict markings are. Each lane is a
-- track in its direction of travel, offset to the left of travel (a right-hand
-- lane is negative). A lane that ends, or turns, is left alone.
DROP TABLE IF EXISTS _rm_bike_tracks;
CREATE TEMP TABLE _rm_bike_tracks AS
SELECT osm_id, s, bridge, dir, line, off, w, x, y, p, node_w, arrives,
       degrees(ST_Azimuth(
         ST_LineInterpolatePoint(line, CASE WHEN arrives THEN greatest(0, 1 - 8 * s / ST_Length(line)) ELSE 0 END),
         ST_LineInterpolatePoint(line, CASE WHEN arrives THEN 1 ELSE least(1, 8 * s / ST_Length(line)) END))) as bearing
FROM (
  -- At each junction on the lane, through the way or at its end: the stretch
  -- arriving and the stretch leaving.
  SELECT r.osm_id, r.s, r.bridge, t.dir, t.off, t.w, n.x, n.y, n.p, n.width as node_w, e.arrives,
         CASE WHEN e.arrives THEN ST_LineSubstring(t.line, 0, at.f) ELSE ST_LineSubstring(t.line, at.f, 1) END as line
  FROM _rm_roads r
  CROSS JOIN LATERAL (VALUES
    (r.bike_r > 0, 1, r.g, (r.br_a + r.br_b) / 2, abs(r.br_b - r.br_a)),
    (r.bike_l > 0 AND r.oneway, 1, r.g, (r.bl_a + r.bl_b) / 2, abs(r.bl_b - r.bl_a)),
    (r.bike_l > 0 AND NOT r.oneway, -1, ST_Reverse(r.g), -(r.bl_a + r.bl_b) / 2, abs(r.bl_b - r.bl_a))
  ) t(present, dir, line, off, w)
  JOIN _rm_incid i ON i.osm_id = r.osm_id
  JOIN _rm_nodes n ON n.x = i.x AND n.y = i.y
  CROSS JOIN LATERAL (SELECT ST_LineLocatePoint(t.line, n.p) as f) at
  CROSS JOIN LATERAL (VALUES (true), (false)) e(arrives)
  WHERE t.present AND ((e.arrives AND at.f > 0) OR (NOT e.arrives AND at.f < 1))
) pieces
WHERE ST_Length(line) > 10 * s;

INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT CASE WHEN piece.fill THEN 'bike_lane' ELSE 'bike' END, CASE WHEN piece.fill THEN 'fill' ELSE 'dashed' END,
       CASE WHEN piece.fill THEN 'green' ELSE 'white' END, m.bridge, ST_Transform(piece.g, 4326)
FROM (
  SELECT DISTINCT ON (a.osm_id, a.dir, a.off, a.x, a.y) a.bridge, a.s, a.x, a.y, a.p, a.node_w,
         least(a.w, d.w) as w, (a.off + d.off) / 2 as off,
         ST_MakeLine(ST_LineSubstring(a.line, greatest(0, 1 - 30 * a.s / ST_Length(a.line)), 1),
                     ST_LineSubstring(d.line, 0, least(1, 30 * a.s / ST_Length(d.line)))) as path,
         a.osm_id as a_id, d.osm_id as d_id
  FROM _rm_bike_tracks a
  JOIN _rm_bike_tracks d ON d.x = a.x AND d.y = a.y AND NOT d.arrives
                        AND sign(d.off) = sign(a.off) AND d.bridge = a.bridge
  WHERE a.arrives AND abs(((d.bearing - a.bearing + 540)::numeric % 360) - 180) < 35
  ORDER BY a.osm_id, a.dir, a.off, a.x, a.y, abs(((d.bearing - a.bearing + 540)::numeric % 360) - 180)
) m
CROSS JOIN LATERAL (SELECT road_offset(ST_RemoveRepeatedPoints(m.path), m.off * m.s) as centre) c
-- Only across the junction itself, and not over its crosswalks.
CROSS JOIN LATERAL (
  SELECT ST_LineMerge(ST_CollectionExtract(ST_Difference(
           ST_Intersection(c.centre, ST_Union(zone.g)),
           COALESCE((SELECT ST_Union(k.cut) FROM _rm_cuts k WHERE k.cut && c.centre), ST_SetSRID('POLYGON EMPTY'::geometry, 3857))), 2)) as span
  FROM (SELECT ST_Buffer(m.p, (m.node_w / 2 + 1) * m.s, 8) as g
        UNION ALL SELECT ST_Intersection(rc.cut, ST_Buffer(m.p, (m.node_w / 2 + 10) * m.s, 8))
        FROM _rm_road_cuts rc WHERE rc.osm_id IN (m.a_id, m.d_id)) zone
) gap
CROSS JOIN LATERAL (
  SELECT true as fill, ST_Buffer(ST_LineSubstring(part.geom, f0, f1), m.w / 2 * m.s, 'endcap=flat') as g
  FROM ST_Dump(gap.span) part,
       LATERAL (SELECT ST_Length(part.geom) / m.s as len) l,
       LATERAL generate_series(0, floor(l.len / 3)::int) k,
       LATERAL (SELECT least(1, k * 3 / l.len) as f0, least(1, (k * 3 + 1.8) / l.len) as f1) f
  WHERE f1 > f0
  UNION ALL
  SELECT false, road_offset(part.geom, side * m.w / 2 * m.s)
  FROM ST_Dump(gap.span) part, (VALUES (1), (-1)) v(side)
) piece
WHERE c.centre IS NOT NULL AND gap.span IS NOT NULL AND NOT ST_IsEmpty(gap.span) AND piece.g IS NOT NULL
  AND NOT ST_IsEmpty(piece.g);

-- ─── Stop lines and turn arrows ──────────────────────────────────────────────
-- At each end of a marked road that meets a junction: across the lanes that
-- arrive there, a stop line if the junction is controlled, and an arrow in
-- each lane its turn:lanes describes.
DROP TABLE IF EXISTS _rm_ends;
CREATE TEMP TABLE _rm_ends AS
SELECT r.osm_id, r.g, r.s, r.americas, r.bridge, e.at_end, e.pt, n.x, n.y, n.top_rank, road_rank(r.class) as rank,
       r.split_b, r.split_f, r.left_edge, r.right_edge, r.lane_w, r.fwd, r.bwd,
       CASE WHEN e.at_end THEN r.turn_forward ELSE r.turn_backward END as turns,
       -- Back from the junction centre to the stop line: clear of the widest
       -- road crossing here, and of any crosswalk across the approach.
       greatest((COALESCE(a.cross_w, 0) / 2 / greatest(COALESCE(a.sine, 1), 0.5) + 1.5) * r.s, COALESCE((
         SELECT max(abs(CASE WHEN e.at_end THEN 1 - ST_LineLocatePoint(r.g, ST_ClosestPoint(c.g, e.pt))
                                              ELSE ST_LineLocatePoint(r.g, ST_ClosestPoint(c.g, e.pt)) END) * ST_Length(r.g))
         FROM _rm_crossings c
         WHERE c.g && ST_Expand(e.pt, 30 * r.s) AND ST_DWithin(c.g, r.g, 0.5 * r.s)
           AND ST_DWithin(c.g, e.pt, 30 * r.s)
       ) + 2.5 * r.s, 0)) as setback,
       COALESCE(a.cross_w, 0) as cross_w
FROM _rm_roads r
CROSS JOIN LATERAL (VALUES (true, ST_EndPoint(r.g)), (false, ST_StartPoint(r.g))) e(at_end, pt)
JOIN _rm_nodes n ON n.x = round(ST_X(e.pt)::numeric, 2) AND n.y = round(ST_Y(e.pt)::numeric, 2)
LEFT JOIN _rm_approach a ON a.osm_id = r.osm_id AND a.x = n.x AND a.y = n.y
WHERE r.marked AND ST_Length(r.g) > 25 * r.s;

ALTER TABLE _rm_ends ADD COLUMN controlled boolean;
UPDATE _rm_ends e SET controlled =
  -- A signal at or just off the junction.
  EXISTS (SELECT 1 FROM _rm_signals c WHERE c.kind = 'traffic_signals' AND ST_DWithin(c.g, e.pt, (e.cross_w / 2 + 20) * e.s))
  -- A stop sign on the node: an all-way stop, or a minor road meeting a bigger one.
  OR EXISTS (SELECT 1 FROM _rm_signals c WHERE c.kind = 'stop' AND ST_DWithin(c.g, e.pt, 0.5 * e.s)
             AND (c.all_way OR e.rank < e.top_rank))
  -- A stop sign on this approach, facing the traffic arriving here.
  OR EXISTS (SELECT 1 FROM _rm_signals c WHERE c.kind = 'stop' AND ST_DWithin(c.g, e.pt, 30 * e.s)
             AND NOT ST_DWithin(c.g, e.pt, 0.5 * e.s) AND ST_DWithin(c.g, e.g, 0.5 * e.s)
             AND COALESCE(c.direction, '') <> CASE WHEN e.at_end THEN 'backward' ELSE 'forward' END);

-- A frame at a point `back` units from a road end: the point, and the unit
-- vectors along the arriving traffic and to its left.
DROP TABLE IF EXISTS _rm_frames;
CREATE TEMP TABLE _rm_frames AS
SELECT e.*, f.back, p,
       (ST_X(ahead) - ST_X(p)) / nullif(ST_Distance(ahead, p), 0) as ux,
       (ST_Y(ahead) - ST_Y(p)) / nullif(ST_Distance(ahead, p), 0) as uy
FROM _rm_ends e
CROSS JOIN LATERAL (VALUES (e.setback), (e.setback + 6 * e.s), (e.setback + 36 * e.s)) f(back)
CROSS JOIN LATERAL (
  SELECT ST_LineInterpolatePoint(e.g, CASE WHEN e.at_end THEN greatest(0, 1 - f.back / ST_Length(e.g)) ELSE least(1, f.back / ST_Length(e.g)) END) as p,
         ST_LineInterpolatePoint(e.g, CASE WHEN e.at_end THEN greatest(0, 1 - (f.back - e.s) / ST_Length(e.g)) ELSE least(1, (f.back - e.s) / ST_Length(e.g)) END) as ahead
) pts
WHERE f.back < ST_Length(e.g) / 2;

-- Stop lines span the arriving lanes. At a way's end those are the forward
-- lanes; at its start the backward ones, and the frame there faces the other
-- way, so the way's offsets flip sign.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'stop', 'fill', 'white', bridge, ST_Transform(ST_Buffer(ST_SetSRID(ST_MakeLine(
         ST_MakePoint(ST_X(p) - uy * a * s, ST_Y(p) + ux * a * s),
         ST_MakePoint(ST_X(p) - uy * b * s, ST_Y(p) + ux * b * s)), 3857), 0.225 * s, 'endcap=flat'), 4326)
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
-- On a one-way street both read with the traffic.
INSERT INTO road_glyphs_next (glyph, direction, bridge, geom)
SELECT CASE WHEN side.lane THEN 'road-bike' ELSE 'road-sharrow' END,
       ((round(degrees(atan2(ST_X(b) - ST_X(a), ST_Y(b) - ST_Y(a))))::int + CASE WHEN side.forward THEN 0 ELSE 180 END) % 360 + 360) % 360,
       r.bridge,
       ST_Transform(ST_SetSRID(ST_MakePoint(ST_X(a) - (ST_Y(b) - ST_Y(a)) / d * side.off * r.s,
                                            ST_Y(a) + (ST_X(b) - ST_X(a)) / d * side.off * r.s), 3857), 4326)
FROM (
  SELECT r.*, 'shared_lane' IN (COALESCE(tags->>CASE WHEN flip THEN 'cycleway:left' ELSE 'cycleway:right' END, tags->>'cycleway:both', tags->>'cycleway', '')) as sharrow_r,
              'shared_lane' IN (COALESCE(tags->>CASE WHEN flip THEN 'cycleway:right' ELSE 'cycleway:left' END, tags->>'cycleway:both',
                                         CASE WHEN NOT oneway THEN tags->>'cycleway' END, '')) as sharrow_l
  FROM _rm_roads r
) r
CROSS JOIN LATERAL (VALUES
  (true, r.bike_r > 0, CASE WHEN r.bike_r > 0 THEN (r.br_a + r.br_b) / 2 ELSE r.right_edge + r.lane_w * 0.4 END, r.bike_r > 0 OR r.sharrow_r),
  (r.oneway, r.bike_l > 0, CASE WHEN r.bike_l > 0 THEN (r.bl_a + r.bl_b) / 2 ELSE r.left_edge - r.lane_w * 0.4 END, r.bike_l > 0 OR r.sharrow_l)
) side(forward, lane, off, present)
CROSS JOIN LATERAL generate_series(1, floor(ST_Length(r.g) / r.s / 60)::int) k
CROSS JOIN LATERAL (SELECT
  ST_LineInterpolatePoint(r.g, (k * 60 - 30) * r.s / ST_Length(r.g)) as a,
  ST_LineInterpolatePoint(r.g, least(1, ((k * 60 - 30) + 1) * r.s / ST_Length(r.g))) as b) pts
CROSS JOIN LATERAL (SELECT nullif(ST_Distance(pts.a, pts.b), 0) as d) dist
WHERE side.present AND d IS NOT NULL;

-- ─── Crosswalks ──────────────────────────────────────────────────────────────
-- A mapped crossing way, trimmed to the kerbs it runs between; or, for a
-- crossing mapped only as a node, a line straight across the road there.
-- Painted as bars: zebra stripes 0.6 m wide at 1.2 m, two edge lines, or both.
-- The band follows the crossing; each bar runs with the traffic on the road it
-- crosses, so a crossing drawn askew gets slanted bar ends, not slanted bars.
DROP TABLE IF EXISTS _rm_walk_segs;
CREATE TEMP TABLE _rm_walk_segs AS
SELECT c.style, c.s, (ST_Dump(ST_LineMerge(ST_CollectionExtract(ST_Intersection(s.g, c.g), 2)))).geom as seg
FROM _rm_crossings c
JOIN _rm_surfaces s ON s.g && c.g AND NOT s.bridge
WHERE c.geom_type = 'line'
UNION ALL
SELECT c.style, c.s, ST_SetSRID(ST_MakeLine(
         ST_MakePoint(ST_X(c.g) - u[2] * r.kerb_l * r.s, ST_Y(c.g) + u[1] * r.kerb_l * r.s),
         ST_MakePoint(ST_X(c.g) - u[2] * r.kerb_r * r.s, ST_Y(c.g) + u[1] * r.kerb_r * r.s)), 3857)
FROM _rm_crossings c
CROSS JOIN LATERAL (
  SELECT r.* FROM _rm_roads r WHERE r.g && ST_Expand(c.g, 2) ORDER BY r.g <-> c.g LIMIT 1
) r
CROSS JOIN LATERAL (SELECT road_dir(r.g, ST_LineLocatePoint(r.g, c.g), 0.5 * r.s) as u) d
WHERE c.geom_type = 'point' AND u IS NOT NULL
  AND ST_DWithin(r.g, c.g, 1 * r.s)
  AND NOT EXISTS (SELECT 1 FROM _rm_crossings w WHERE w.geom_type = 'line' AND ST_DWithin(w.g, c.g, 1 * r.s));
-- With the crossed road's direction where the crossing meets it, and the sine
-- of the angle between the two: a bar's footprint along the band grows as
-- 1 / sine. Nearly along the road is a crossing mapped oddly; it keeps square
-- bars.
DROP TABLE IF EXISTS _rm_walks;
CREATE TEMP TABLE _rm_walks AS
SELECT w.style, w.s, w.seg, CASE WHEN d.sine >= 0.4 THEN d.ux END as ux, CASE WHEN d.sine >= 0.4 THEN d.uy END as uy,
       CASE WHEN d.sine >= 0.4 THEN d.sine ELSE 1 END as sine
FROM _rm_walk_segs w
LEFT JOIN LATERAL (
  SELECT u[1] as ux, u[2] as uy,
         abs(u[1] * (ST_Y(ST_EndPoint(w.seg)) - ST_Y(ST_StartPoint(w.seg))) - u[2] * (ST_X(ST_EndPoint(w.seg)) - ST_X(ST_StartPoint(w.seg))))
           / nullif(ST_Length(w.seg), 0) as sine
  FROM (SELECT r.g FROM _rm_roads r
        WHERE r.g && ST_Expand(w.seg, 10 * w.s) AND ST_Intersects(r.g, ST_Buffer(w.seg, w.s))
        ORDER BY r.g <-> ST_LineInterpolatePoint(w.seg, 0.5) LIMIT 1) r
  CROSS JOIN LATERAL (SELECT road_dir(r.g, ST_LineLocatePoint(r.g, ST_LineInterpolatePoint(w.seg, 0.5)), w.s) as u) d
  WHERE u IS NOT NULL
) d ON true
WHERE GeometryType(w.seg) = 'LINESTRING' AND ST_Length(w.seg) >= 1 * w.s;

INSERT INTO road_markings_next (kind, pattern, color, style, bridge, geom)
SELECT 'crosswalk', 'fill', 'white', w.style, false, ST_Transform(ST_Multi(ST_Union(bar)), 4326)
FROM _rm_walks w
CROSS JOIN LATERAL (SELECT ST_Length(w.seg) / w.s as len) l
CROSS JOIN LATERAL (
  SELECT CASE WHEN w.ux IS NULL
           THEN ST_Buffer(ST_LineSubstring(w.seg, o.a / l.len, least(1, (o.a + sz.d) / l.len)), 1.5 * w.s, 'endcap=flat')
           ELSE ST_MakePolygon(ST_MakeLine(ARRAY[
             ST_Translate(p0, w.ux * sz.h, w.uy * sz.h), ST_Translate(p1, w.ux * sz.h, w.uy * sz.h),
             ST_Translate(p1, -w.ux * sz.h, -w.uy * sz.h), ST_Translate(p0, -w.ux * sz.h, -w.uy * sz.h),
             ST_Translate(p0, w.ux * sz.h, w.uy * sz.h)])) END as bar
  FROM (SELECT 1.2 / w.sine as step, 0.6 / w.sine as d, 1.5 / w.sine * w.s as h) sz
  CROSS JOIN LATERAL generate_series(0, greatest(floor((l.len - sz.d) / sz.step)::int, 0)) k
  CROSS JOIN LATERAL (SELECT (l.len - (floor((l.len - sz.d) / sz.step) * sz.step + sz.d)) / 2 + k * sz.step as a) o
  CROSS JOIN LATERAL (SELECT ST_LineInterpolatePoint(w.seg, greatest(0, least(1, o.a / l.len))) as p0,
                             ST_LineInterpolatePoint(w.seg, greatest(0, least(1, (o.a + sz.d) / l.len))) as p1) pts
  -- A crossing too short for one bar gets none.
  WHERE w.style IN ('zebra', 'ladder') AND l.len >= sz.d
  UNION ALL
  SELECT ST_Buffer(road_offset(w.seg, side * 1.5 * w.s), 0.15 * w.s, 'endcap=flat')
  FROM (VALUES (1), (-1)) v(side)
  WHERE w.style IN ('lines', 'ladder')
) b
GROUP BY w.style, w.seg;

-- ─── Paint stays on the road ─────────────────────────────────────────────────
-- Everything painted is cut to the carriageway it lies on, so a bar, a line or
-- a symbol never runs past a kerb.
-- Built in Web Mercator, a sliver brought back to degrees can cross itself,
-- which GEOS refuses to cut.
UPDATE road_surfaces_next SET geom = ST_Multi(ST_CollectionExtract(ST_MakeValid(geom), 3)) WHERE NOT ST_IsValid(geom);
DELETE FROM road_surfaces_next WHERE ST_IsEmpty(geom);
UPDATE road_markings_next SET geom = ST_CollectionExtract(ST_MakeValid(geom), 3) WHERE pattern = 'fill' AND NOT ST_IsValid(geom);
UPDATE road_markings_next m SET geom = COALESCE(ST_CollectionExtract(clip.g, CASE WHEN m.pattern = 'fill' THEN 3 ELSE 2 END),
                                                 'GEOMETRYCOLLECTION EMPTY'::geometry)
FROM (
  SELECT m2.fid, ST_Intersection(m2.geom, ST_Union(s.geom)) as g
  FROM road_markings_next m2
  JOIN road_surfaces_next s ON s.geom && m2.geom AND s.kind IS NULL AND (s.bridge = m2.bridge OR m2.kind = 'crosswalk')
  GROUP BY m2.fid, m2.geom
) clip
WHERE m.fid = clip.fid;
DELETE FROM road_markings_next m
WHERE ST_IsEmpty(geom)
   OR NOT EXISTS (SELECT 1 FROM road_surfaces_next s WHERE s.geom && m.geom AND s.kind IS NULL AND (s.bridge = m.bridge OR m.kind = 'crosswalk'));
DELETE FROM road_glyphs_next g
WHERE EXISTS (SELECT 1 FROM _rm_turns t WHERE t.g && ST_Transform(g.geom, 3857) AND ST_DWithin(t.p, ST_Transform(g.geom, 3857), t.radius * t.s))
   OR NOT EXISTS (SELECT 1 FROM road_surfaces_next s WHERE s.bridge = g.bridge AND s.kind IS NULL AND ST_Intersects(s.geom, g.geom));

-- ─── Swap ────────────────────────────────────────────────────────────────────
CREATE INDEX road_markings_next_geom_idx ON road_markings_next USING gist (geom);
CREATE INDEX road_glyphs_next_geom_idx ON road_glyphs_next USING gist (geom);
DO $$
DECLARE
  box geometry := (SELECT box FROM _rm_scope LIMIT 1);
BEGIN
  IF box IS NULL THEN
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
    RETURN;
  END IF;

  -- The first box on a database that never had a full build. Same shape as
  -- the swapped-in tables, so a later full build replaces them cleanly.
  CREATE TABLE IF NOT EXISTS road_surfaces (
    fid bigserial CONSTRAINT road_surfaces_pkey PRIMARY KEY, bridge boolean, kind text, geom geometry(MultiPolygon, 4326));
  -- Altered only when it lacks the column: even a no-op ALTER would lock tile
  -- reads out of the table until this build commits.
  IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'road_surfaces'::regclass AND attname = 'kind' AND NOT attisdropped) THEN
    -- Fails the build rather than queue tile reads behind it.
    PERFORM set_config('lock_timeout', '5s', true);
    ALTER TABLE road_surfaces ADD COLUMN kind text;
  END IF;
  CREATE TABLE IF NOT EXISTS road_markings (
    fid bigserial CONSTRAINT road_markings_pkey PRIMARY KEY, kind text, pattern text, color text, style text, bridge boolean,
    geom geometry(Geometry, 4326));
  CREATE TABLE IF NOT EXISTS road_glyphs (
    fid bigserial CONSTRAINT road_glyphs_pkey PRIMARY KEY, glyph text, direction int, bridge boolean, geom geometry(Point, 4326));
  CREATE INDEX IF NOT EXISTS road_surfaces_geom_idx ON road_surfaces USING gist (geom);
  CREATE INDEX IF NOT EXISTS road_markings_geom_idx ON road_markings USING gist (geom);
  CREATE INDEX IF NOT EXISTS road_glyphs_geom_idx ON road_glyphs USING gist (geom);

  -- Live rows straddling the box keep only their part outside it; the new
  -- rows, cut to the box, fill the inside. The box is a rectangle, so a row
  -- whose bounding box lies inside it (@) lies inside it, with no GEOS call.
  UPDATE road_surfaces SET geom = ST_Multi(road_clip(geom, box, false, 3))
  WHERE geom && box AND NOT geom @ box;
  DELETE FROM road_surfaces WHERE geom && box AND (geom @ box OR ST_IsEmpty(geom));
  UPDATE road_markings SET geom = road_clip(geom, box, false, CASE WHEN pattern = 'fill' THEN 3 ELSE 2 END)
  WHERE geom && box AND NOT geom @ box;
  DELETE FROM road_markings WHERE geom && box AND (geom @ box OR ST_IsEmpty(geom));
  DELETE FROM road_glyphs WHERE ST_Intersects(geom, box);

  INSERT INTO road_surfaces (bridge, kind, geom)
  SELECT bridge, kind, ST_Multi(road_clip(geom, box, true, 3)) FROM road_surfaces_next WHERE geom && box;
  INSERT INTO road_markings (kind, pattern, color, style, bridge, geom)
  SELECT kind, pattern, color, style, bridge, road_clip(geom, box, true, CASE WHEN pattern = 'fill' THEN 3 ELSE 2 END)
  FROM road_markings_next WHERE geom && box;
  INSERT INTO road_glyphs (glyph, direction, bridge, geom)
  SELECT glyph, direction, bridge, geom FROM road_glyphs_next WHERE ST_Intersects(geom, box);
  DELETE FROM road_surfaces WHERE geom && box AND ST_IsEmpty(geom);
  DELETE FROM road_markings WHERE geom && box AND ST_IsEmpty(geom);

  DROP TABLE road_surfaces_next;
  DROP TABLE road_markings_next;
  DROP TABLE road_glyphs_next;
END
$$;
ANALYZE road_surfaces;
ANALYZE road_markings;
ANALYZE road_glyphs;

-- Let go of a session's lock (see "One build at a time"); a transaction's goes
-- at its commit.
SELECT pg_advisory_unlock(5393739, 0) FROM _rm_lock WHERE xid <> pg_current_xact_id();
