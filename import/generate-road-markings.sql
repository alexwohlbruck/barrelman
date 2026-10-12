-- =============================================================================
-- Road surfaces and markings
-- =============================================================================
-- Lane-level road geometry from OSM's lane tags, for a client to draw with
-- plain fill, line and symbol layers:
--
--   road_surfaces   carriageways at their real width, with kerb corners
--                   rounded where roads meet, and the islands turning loops
--                   run round (`kind` 'island')
--   road_markings   lines and areas of paint: centre, lane, edge, bike, bus,
--                   parking, stop, yield, crosswalk, hump, and the outline and
--                   hatching of painted areas, each with a colour and a pattern
--                   (docs/content/docs/self-hosting/road-markings.mdx)
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

-- 0 before 0, 1 after 1, and an S-curve between with level ends.
CREATE OR REPLACE FUNCTION road_smoothstep(t float8) RETURNS float8
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN t IS NULL OR t >= 1 THEN 1 WHEN t <= 0 THEN 0 ELSE t * t * (3 - 2 * t) END
$$;

-- A line offset sideways (positive to the left of its direction) by a profile
-- along it, `x` metres from its start of `len_m` (the way's mapped length):
--
--   target - (target - start) * (1 - S(x / ease))          its own taper
--          - res * (1 - S((x + res_at) / res_len))         the way before's, run on
--          + tail * S((x - len_m + tail_ease) / tail_ease) its far end's taper
--
-- S is a smoothstep, so a line leaves and joins its place without a kink, and
-- tapers that overlap add up to one curve. Offsets are in metres, `s` mercator
-- units to the metre. Where nothing eases the line is a plain offset curve.
-- `dir_s` and `dir_e`, where given, are the unit directions to offset square
-- to at the ends, halfway between this way's and the next's at a join, so
-- the lines of two ways meet rather than gap on the outside of a bend.
CREATE OR REPLACE FUNCTION road_profile(line geometry, s float8, len_m float8, target float8, start float8, ease float8,
                                        res float8, res_at float8, res_len float8, tail float8, tail_ease float8,
                                        dir_s float8[] DEFAULT NULL, dir_e float8[] DEFAULT NULL)
RETURNS geometry LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  d1 float8 := CASE WHEN start IS NOT NULL AND ease > 0 AND abs(target - start) > 0.01 THEN target - start ELSE 0 END;
  d2 float8 := CASE WHEN res IS NOT NULL AND res_len > res_at AND abs(res) > 0.01 THEN res ELSE 0 END;
  d3 float8 := CASE WHEN tail IS NOT NULL AND tail_ease > 0 AND abs(tail) > 0.05 THEN tail ELSE 0 END;
  -- The stretch, as fractions of the line, where anything eases.
  e1 float8;
  t0 float8;
  step float8;
  pts geometry[];
  out geometry[] := '{}';
  piece geometry;
  n int;
  i int;
  f float8;
  dx float8;
  dy float8;
  dl float8;
  a float8;
  b float8;
  u float8[];
  k float8;
BEGIN
  IF line IS NULL OR len_m IS NULL OR len_m <= 0 OR ST_Length(line) = 0 OR GeometryType(line) <> 'LINESTRING' THEN RETURN NULL; END IF;
  -- Arithmetic leaves 1e-16 where an offset is nil, which GEOS fails to offset by.
  IF abs(target) < 1e-6 THEN target := 0; END IF;
  e1 := least(1, greatest(CASE WHEN d1 <> 0 THEN ease ELSE 0 END, CASE WHEN d2 <> 0 THEN res_len - res_at ELSE 0 END) / len_m);
  t0 := CASE WHEN d3 <> 0 THEN greatest(0, 1 - tail_ease / len_m) ELSE 1 END;
  step := greatest(0.5, least(CASE WHEN d1 <> 0 THEN ease ELSE 'Infinity' END, CASE WHEN d2 <> 0 THEN res_len ELSE 'Infinity' END,
                              CASE WHEN d3 <> 0 THEN tail_ease ELSE 'Infinity' END, 'Infinity') / 12) / len_m;
  -- Head [0, e1], middle [e1, t0], tail [t0, 1]; where head and tail meet, one.
  FOR a, b IN SELECT * FROM (VALUES (0::float8, CASE WHEN e1 >= t0 THEN 1 ELSE e1 END), (e1, t0), (t0, CASE WHEN e1 >= t0 THEN 0 ELSE 1 END)) v(va, vb)
              WHERE vb - va > 1e-9 LOOP
    piece := ST_LineSubstring(line, a, b);
    IF a >= e1 AND b <= t0 THEN
      -- Nothing eases here: GEOS draws it.
      piece := road_offset(piece, target * s);
      IF piece IS NULL OR GeometryType(piece) <> 'LINESTRING' THEN RETURN NULL; END IF;
      out := out || ARRAY(SELECT ST_SetSRID(geom, 0) FROM ST_DumpPoints(piece) ORDER BY path);
      CONTINUE;
    END IF;
    pts := ARRAY(SELECT geom FROM ST_DumpPoints(ST_Segmentize(piece, step * ST_Length(line))) ORDER BY path);
    n := array_length(pts, 1);
    dl := 0;
    FOR i IN 1..n LOOP
      IF i > 1 THEN dl := dl + ST_Distance(pts[i - 1], pts[i]); END IF;
      dx := ST_X(pts[least(i + 1, n)]) - ST_X(pts[greatest(i - 1, 1)]);
      dy := ST_Y(pts[least(i + 1, n)]) - ST_Y(pts[greatest(i - 1, 1)]);
      IF dx = 0 AND dy = 0 THEN CONTINUE; END IF;
      f := COALESCE(a + (b - a) * dl / nullif(ST_Length(piece), 0), a);
      k := road_profile_at(f * len_m, len_m, target, d1, ease, d2, res_at, res_len, d3, tail_ease) * s / sqrt(dx * dx + dy * dy);
      out := out || ST_MakePoint(ST_X(pts[i]) - dy * k, ST_Y(pts[i]) + dx * k);
    END LOOP;
  END LOOP;
  IF array_length(out, 1) < 2 THEN RETURN NULL; END IF;
  -- Square to the join, at the same distance from both ways.
  FOREACH f IN ARRAY ARRAY[0, 1]::float8[] LOOP
    u := CASE WHEN f = 0 THEN dir_s ELSE dir_e END;
    CONTINUE WHEN u IS NULL;
    pts := ARRAY[ST_LineInterpolatePoint(line, f), ST_LineInterpolatePoint(line, CASE WHEN f = 0 THEN least(1, 0.5 * s / ST_Length(line)) ELSE greatest(0, 1 - 0.5 * s / ST_Length(line)) END)];
    dx := abs(ST_X(pts[2]) - ST_X(pts[1])) + abs(ST_Y(pts[2]) - ST_Y(pts[1]));
    CONTINUE WHEN dx = 0;
    -- cos of the angle between the way's own end and the shared direction
    k := (u[1] * (ST_X(pts[2]) - ST_X(pts[1])) + u[2] * (ST_Y(pts[2]) - ST_Y(pts[1]))) / ST_Distance(pts[1], pts[2]) * CASE WHEN f = 0 THEN 1 ELSE -1 END;
    CONTINUE WHEN k < 0.7;
    k := road_profile_at(f * len_m, len_m, target, d1, ease, d2, res_at, res_len, d3, tail_ease) * s / k;
    IF f = 0 THEN out[1] := ST_MakePoint(ST_X(pts[1]) - u[2] * k, ST_Y(pts[1]) + u[1] * k);
    ELSE out[array_length(out, 1)] := ST_MakePoint(ST_X(pts[1]) - u[2] * k, ST_Y(pts[1]) + u[1] * k); END IF;
  END LOOP;
  RETURN ST_RemoveRepeatedPoints(ST_SetSRID(ST_MakeLine(out), ST_SRID(line)));
END
$$;

-- The offset road_profile draws at `x` metres, given its terms.
CREATE OR REPLACE FUNCTION road_profile_at(x float8, len_m float8, target float8, d1 float8, ease float8, d2 float8, res_at float8,
                                           res_len float8, d3 float8, tail_ease float8)
RETURNS float8 LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT target - CASE WHEN d1 <> 0 THEN d1 * (1 - road_smoothstep(x / ease)) ELSE 0 END
                - CASE WHEN d2 <> 0 THEN d2 * (1 - road_smoothstep((x + res_at) / res_len)) ELSE 0 END
                + CASE WHEN d3 <> 0 THEN d3 * road_smoothstep((x - len_m + tail_ease) / tail_ease) ELSE 0 END
$$;

-- How far a way's taper moved the line that ends up at offset `p`, read
-- between the lines whose moves are known (`pos`, `disp`), and held past the
-- outermost.
CREATE OR REPLACE FUNCTION road_field(pos float8[], disp float8[], p float8) RETURNS float8
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  lo int;
  hi int;
  i int;
BEGIN
  IF pos IS NULL OR p IS NULL THEN RETURN 0; END IF;
  -- `pos` is sorted; the nearest at or below `p` and at or above it.
  FOR i IN 1..array_length(pos, 1) LOOP
    IF pos[i] <= p THEN lo := i; END IF;
    IF pos[i] >= p AND hi IS NULL THEN hi := i; END IF;
  END LOOP;
  IF lo IS NULL THEN RETURN COALESCE(disp[hi], 0); END IF;
  IF hi IS NULL OR pos[hi] = pos[lo] THEN RETURN COALESCE(disp[lo], 0); END IF;
  RETURN COALESCE(disp[lo] + (disp[hi] - disp[lo]) * (p - pos[lo]) / (pos[hi] - pos[lo]), 0);
END
$$;

-- How far the tapers have the line headed for offset `p` still to go, `x`
-- metres along a way (see road_profile), from its own and the way before's.
CREATE OR REPLACE FUNCTION road_moved(pos float8[], disp float8[], ease float8, res_pos float8[], res_disp float8[],
                                      res_at float8, res_len float8, p float8, x float8)
RETURNS float8 LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  d float8;
BEGIN
  IF pos IS NULL AND res_pos IS NULL THEN RETURN 0; END IF;
  d := road_field(pos, disp, p);
  RETURN -d * (1 - road_smoothstep(x / nullif(ease, 0)))
         - road_field(res_pos, res_disp, p - d) * (1 - road_smoothstep((x + res_at) / nullif(res_len, 0)));
END
$$;

-- A line cut into dashes `dash` metres long, a whole number of `period`s
-- along the `len_m` metres of its way, so a dash starts at each end of every
-- way and the pattern runs on across a join; none outside fractions `from_f`
-- to `to_f`.
DROP FUNCTION IF EXISTS road_dashes(geometry, float8, float8, float8, float8);
CREATE OR REPLACE FUNCTION road_dashes(line geometry, len_m float8, period float8, dash float8, from_f float8, to_f float8)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_Collect(ST_LineSubstring(line, greatest(f0, from_f), least(f1, to_f)) ORDER BY k)
  FROM (SELECT greatest(1, round(len_m / period))::int as n WHERE len_m > 0 AND line IS NOT NULL) c,
       generate_series(0, c.n - 1) k,
       LATERAL (SELECT k::float8 / c.n as f0, least(1, k::float8 / c.n + dash / len_m) as f1) f
  WHERE least(f1, to_f) > greatest(f0, from_f)
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

-- The carriageway between two kerb lines, rounded off at its far end, where
-- the kerbs end at `far_l` and `far_r`.
CREATE OR REPLACE FUNCTION road_body(left_kerb geometry, right_kerb geometry, far_l geometry, far_r geometry)
RETURNS geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT ST_Union(road_between(left_kerb, right_kerb),
                  ST_Buffer(ST_LineInterpolatePoint(ST_MakeLine(far_l, far_r), 0.5), ST_Distance(far_l, far_r) / 2, 8))
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

-- The stall marks of a parking lane on one side (OSM's left/right), as
-- {metres between them, cotangent of their angle to the kerb, 1 for short
-- ticks only}, or NULL where none are painted: angled stalls unless
-- markings=no, parallel ones in full where marked, and short ticks a car
-- length apart along a parallel lane nobody said is unmarked.
CREATE OR REPLACE FUNCTION road_parking_marks(tags jsonb, side text) RETURNS float8[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN m = 'no' THEN NULL
    WHEN COALESCE(o, v) = 'diagonal' THEN ARRAY[3.1, 0.577, 0]
    WHEN COALESCE(o, v) = 'perpendicular' THEN ARRAY[2.7, 0, 0]
    WHEN m = 'yes' OR v = 'marked' THEN ARRAY[6.1, 0, 0]
    WHEN v IN ('lane', 'parallel') AND COALESCE(o, 'parallel') = 'parallel' THEN ARRAY[6.1, 0, 1]
  END
  FROM (SELECT COALESCE(tags->>('parking:' || side), tags->>'parking:both', tags->>('parking:lane:' || side), tags->>'parking:lane:both') as v,
               COALESCE(tags->>('parking:' || side || ':orientation'), tags->>'parking:both:orientation') as o,
               COALESCE(tags->>('parking:' || side || ':markings'), tags->>'parking:both:markings') as m) t
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

-- A bike lane's tags with a buffer added where none is tagged but a barred
-- area is marked on its traffic side as its rider sees it: the right when the
-- kerb is on their left. The rider runs against the way in a contraflow lane,
-- and on the left of a two-way street.
CREATE OR REPLACE FUNCTION road_bike_barred(tags jsonb, side text, oneway boolean) RETURNS jsonb
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN NOT tags ?| ARRAY['cycleway:' || side || ':marking:left', 'cycleway:' || side || ':marking:right',
                                     'cycleway:both:marking:left', 'cycleway:both:marking:right']
                OR tags ?| ARRAY['cycleway:' || side || ':buffer', 'cycleway:both:buffer', 'cycleway:buffer'] THEN tags
              WHEN CASE WHEN (side = 'left') = NOT (COALESCE(COALESCE(tags->>('cycleway:' || side), tags->>'cycleway:both', tags->>'cycleway')
                                                               IN ('opposite_lane', 'opposite_track'), false)
                                                   OR COALESCE(tags->>('cycleway:' || side || ':oneway'), '') = '-1'
                                                   OR (side = 'left' AND NOT oneway AND COALESCE(tags->>'cycleway:left:oneway', '-1') = '-1'))
                        THEN 'barred_area' IN (tags->>('cycleway:' || side || ':marking:right'), tags->>'cycleway:both:marking:right')
                        ELSE 'barred_area' IN (tags->>('cycleway:' || side || ':marking:left'), tags->>'cycleway:both:marking:left') END
                THEN tags || jsonb_build_object('cycleway:' || side || ':buffer', 'yes')
              ELSE tags END
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

-- The band between two lines drawn by road_profile, from offsets `a` and `b`
-- (each eased from `a0`, `b0`, moved by `ra`, `rb` from the way before, and
-- by `ta`, `tb` toward its end): a lane that follows a tapering kerb.
DROP FUNCTION IF EXISTS road_profile_band(geometry, float8, float8, float8, float8, float8, float8, float8, float8, float8, float8, float8,
                                           float8, float8, float8[], float8[]);
CREATE OR REPLACE FUNCTION road_profile_band(line geometry, s float8, len_m float8, a float8, a0 float8, b float8, b0 float8, ease float8,
                                             ra float8, rb float8, res_at float8, res_len float8, ta float8, tb float8, tail_ease float8,
                                             dir_s float8[] DEFAULT NULL, dir_e float8[] DEFAULT NULL)
RETURNS geometry LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
  RETURN road_between(road_profile(line, s, len_m, a, a0, ease, ra, res_at, res_len, ta, tail_ease, dir_s, dir_e),
                      road_profile(line, s, len_m, b, b0, ease, rb, res_at, res_len, tb, tail_ease, dir_s, dir_e));
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
$$;

-- Where a line at offset `o` beside the traffic lanes (a bike lane's, or a
-- parking lane's) starts when its road eases in from the way before: a bike
-- lane keeps its place beside the traffic (`at_kerb` false) and a track or a
-- parking lane its place at the kerb, held between the kerb and the traffic
-- lanes there, so neither squeezes those lanes nor steps across at the join.
CREATE OR REPLACE FUNCTION road_side_start(o float8, kerb float8, edge float8, prev_kerb float8, prev_edge float8, at_kerb boolean)
RETURNS float8 LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE WHEN kerb <= edge THEN least(prev_edge, greatest(prev_kerb, st)) ELSE greatest(prev_edge, least(prev_kerb, st)) END
  FROM (SELECT CASE WHEN at_kerb THEN prev_kerb + o - kerb ELSE prev_edge + o - edge END as st) t
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

-- A centre lane that traffic both ways turns from: a two-way left-turn lane,
-- unless turn:lanes:both_ways says it is something else.
CREATE OR REPLACE FUNCTION road_twltl(tags jsonb, oneway boolean, both_ways int) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT NOT oneway AND both_ways > 0 AND COALESCE(tags->>'turn:lanes:both_ways', 'left') ~ '(left|right)'
$$;

-- A change:lanes list for `n` lanes, or NULL when it does not name them all;
-- a single change value holds for every lane.
CREATE OR REPLACE FUNCTION road_change(per_lane text, whole text, n int) RETURNS text[]
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT CASE
    WHEN n > 0 AND per_lane IS NOT NULL AND array_length(string_to_array(per_lane, '|'), 1) = n
      THEN ARRAY(SELECT COALESCE(NULLIF(trim(v), ''), 'yes') FROM unnest(string_to_array(per_lane, '|')) WITH ORDINALITY u(v, i) ORDER BY i)
    WHEN n > 0 AND whole IS NOT NULL THEN array_fill(trim(whole), ARRAY[n])
  END
$$;

-- The paint between lanes `i` and `i + 1` of a change list, numbered left to
-- right as traffic sees them: dashed where either may cross, solid where
-- neither may, and where only one may, a solid line on the side that may not
-- beside a dashed one. Each line is `d` to the left of the divider, `side` +1
-- where the traffic's left is the way's left and -1 where it runs against it.
CREATE OR REPLACE FUNCTION road_lane_divider(change text[], i int, dash text, side int)
RETURNS TABLE (pattern text, d float8) LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT v.pattern, v.d * side
  FROM (SELECT COALESCE(change[i] IN ('no', 'not_right', 'only_left'), false) as l,
               COALESCE(change[i + 1] IN ('no', 'not_left', 'only_right'), false) as r) b,
  LATERAL (VALUES (CASE WHEN b.l = b.r THEN CASE WHEN b.l THEN 'solid' ELSE dash END END, 0.0),
                  (CASE WHEN b.l <> b.r THEN 'solid' END, CASE WHEN b.l THEN 0.1 ELSE -0.1 END),
                  (CASE WHEN b.l <> b.r THEN dash END, CASE WHEN b.l THEN -0.1 ELSE 0.1 END)) v(pattern, d)
  WHERE v.pattern IS NOT NULL
$$;

-- Bars across a band `half` either side of a line, one every `step`, each
-- leaning `lean` times `half` forward on its left: a hatched buffer at 1, the
-- stalls of a parking lane at their angle's cotangent.
CREATE OR REPLACE FUNCTION road_hatch(line geometry, half float8, step float8, lean float8) RETURNS geometry
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  len float8 := ST_Length(line);
  -- Points every half step from the start: bars stand on the odd ones, and
  -- the even ones either side give the way's direction there.
  pts geometry[];
  bars geometry[] := '{}';
  p geometry;
  dx float8;
  dy float8;
  d float8;
  i int;
BEGIN
  IF GeometryType(line) <> 'LINESTRING' OR len < step THEN RETURN NULL; END IF;
  pts := ST_StartPoint(line) || ARRAY(SELECT geom FROM ST_Dump(ST_LineInterpolatePoints(line, least(1, step / 2 / len), true)) ORDER BY path);
  i := 2;
  WHILE i + 1 <= array_length(pts, 1) LOOP
    p := pts[i];
    dx := ST_X(pts[i + 1]) - ST_X(pts[i - 1]);
    dy := ST_Y(pts[i + 1]) - ST_Y(pts[i - 1]);
    d := sqrt(dx * dx + dy * dy);
    IF d > 0 THEN
      dx := dx / d; dy := dy / d;
      bars := bars || ST_MakeLine(ST_MakePoint(ST_X(p) - (dx * lean - dy) * half, ST_Y(p) - (dy * lean + dx) * half),
                                  ST_MakePoint(ST_X(p) + (dx * lean - dy) * half, ST_Y(p) + (dy * lean + dx) * half));
    END IF;
    i := i + 2;
  END LOOP;
  RETURN ST_SetSRID(ST_Collect(bars), ST_SRID(line));
END
$$;

-- The bars painted across an area, `step` apart along its length and kept
-- `inset` inside its outline: stripes lean 45° off its long axis, crosshatch
-- both ways, and chevrons point back along `u` (the traffic's direction),
-- toward the traffic arriving. Laid out from the area's own shape alone, so
-- every box that builds a piece of it draws the same bars.
CREATE OR REPLACE FUNCTION road_area_hatch(area geometry, pattern text, step float8, ux float8, uy float8, inset float8)
RETURNS geometry LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
DECLARE
  env geometry := ST_OrientedEnvelope(area);
  c geometry := ST_Centroid(env);
  p geometry[] := ARRAY(SELECT geom FROM ST_DumpPoints(ST_ExteriorRing(env)) ORDER BY path);
  e1 float8;
  e2 float8;
  ax float8;
  ay float8;
  len float8;
  reach float8;
  bars geometry[] := '{}';
  t float8;
  cx float8;
  cy float8;
BEGIN
  IF GeometryType(env) <> 'POLYGON' OR array_length(p, 1) < 4 THEN RETURN NULL; END IF;
  e1 := ST_Distance(p[1], p[2]);
  e2 := ST_Distance(p[2], p[3]);
  IF least(e1, e2) <= 0 THEN RETURN NULL; END IF;
  -- The long axis, turned to run with the traffic where that is known.
  IF e1 >= e2 THEN
    ax := (ST_X(p[2]) - ST_X(p[1])) / e1; ay := (ST_Y(p[2]) - ST_Y(p[1])) / e1; len := e1;
  ELSE
    ax := (ST_X(p[3]) - ST_X(p[2])) / e2; ay := (ST_Y(p[3]) - ST_Y(p[2])) / e2; len := e2;
  END IF;
  IF ux IS NOT NULL AND ax * ux + ay * uy < 0 THEN ax := -ax; ay := -ay; END IF;
  reach := len + least(e1, e2);
  t := -reach;
  WHILE t <= reach LOOP
    cx := ST_X(c) + ax * t;
    cy := ST_Y(c) + ay * t;
    -- (ax - ay, ay + ax) leans 45° left of the axis, (ax + ay, ay - ax) right.
    IF pattern IN ('stripes', 'crosshatch') THEN
      bars := bars || ST_MakeLine(ST_MakePoint(cx - (ax - ay) * reach, cy - (ay + ax) * reach), ST_MakePoint(cx + (ax - ay) * reach, cy + (ay + ax) * reach));
    END IF;
    IF pattern = 'crosshatch' THEN
      bars := bars || ST_MakeLine(ST_MakePoint(cx - (ax + ay) * reach, cy - (ay - ax) * reach), ST_MakePoint(cx + (ax + ay) * reach, cy + (ay - ax) * reach));
    END IF;
    IF pattern = 'chevron' THEN
      bars := bars || ST_MakeLine(ARRAY[ST_MakePoint(cx + (ax - ay) * reach, cy + (ay + ax) * reach), ST_MakePoint(cx, cy),
                                        ST_MakePoint(cx + (ax + ay) * reach, cy + (ay - ax) * reach)]);
    END IF;
    t := t + step;
  END LOOP;
  RETURN ST_CollectionExtract(ST_Intersection(ST_SetSRID(ST_Collect(bars), ST_SRID(area)), ST_Buffer(area, -inset)), 2);
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END
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
  next_frontier geometry[];
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
    SELECT array_agg(pt) INTO next_frontier
    FROM found, LATERAL (VALUES (ST_StartPoint(geom)), (ST_EndPoint(geom))) e(pt)
    WHERE NOT pt = ANY (frontier);
    -- A street running through one of the first of these points makes it a
    -- junction rather than a fork (see Forks): read it too, but go no further.
    IF depth = 1 THEN
    INSERT INTO _rm_source
    SELECT DISTINCT ON (g.osm_id) g.osm_id, g.tags, g.geom, false
    FROM unnest(frontier) f(pt)
    JOIN geo_places g ON g.geom && ST_Expand(f.pt, 1e-7)
     AND g.geom_type = 'line' AND g.osm_type = 'W' AND g.tags ? 'highway' AND ST_DWithin(g.geom, f.pt, 1e-7)
    WHERE road_is_marked_way(g.tags) AND NOT EXISTS (SELECT 1 FROM _rm_source x WHERE x.osm_id = g.osm_id);
    END IF;
    frontier := next_frontier;
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
         road_bike(road_bike_barred(tags, CASE WHEN flip THEN 'left' ELSE 'right' END, oneway), CASE WHEN flip THEN 'left' ELSE 'right' END, oneway, americas) as bike_rt,
         road_bike(road_bike_barred(tags, CASE WHEN flip THEN 'right' ELSE 'left' END, oneway), CASE WHEN flip THEN 'right' ELSE 'left' END, oneway, americas) as bike_lt,
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
       -- Metres along the way as mapped: tapers are laid out on it, the same
       -- whichever box builds them, though smoothing differs.
       ST_Length(g) / s as len_m,
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
-- Kerb to kerb from the sidewalks mapped either side, where no width is
-- tagged: a sidewalk's line runs down its middle, `inset` (half its tagged
-- width, else 2.25 m) from the kerb. Read square across the way at a quarter,
-- half and three quarters along, each side needs a sidewalk as the nearest
-- path or road at two of the three, and the nearer side sets both kerbs. Only
-- ever wider than the lanes and their bike lanes and parking, by at most 8 m:
-- the rest is unmarked `sh` either side, so lanes keep their width. It lies
-- outside a track and any parking, where a car parks, and inside a bike lane,
-- which runs beside the traffic.
-- Read from the mapped line, so every box gets the same width.
ALTER TABLE _rm_roads ADD COLUMN sh float8 NOT NULL DEFAULT 0;
-- Square across the way 20 m to each side, `side` +1 to the left.
DROP TABLE IF EXISTS _rm_rays;
CREATE TEMP TABLE _rm_rays AS
SELECT r.osm_id, r.s, v.side, at.p, ST_MakeLine(at.p, ST_Translate(at.p, -v.side * at.u[2] * 20 * r.s, v.side * at.u[1] * 20 * r.s)) as line
FROM _rm_roads r
CROSS JOIN LATERAL (VALUES (0.25), (0.5), (0.75)) f(t)
CROSS JOIN LATERAL (SELECT ST_LineInterpolatePoint(r.g, f.t) as p, road_dir(r.g, f.t, 5 * r.s) as u) at
CROSS JOIN LATERAL (VALUES (1), (-1)) v(side)
WHERE NOT r.bridge AND at.u IS NOT NULL
  AND COALESCE(road_metres(r.tags->>'width:carriageway'), road_metres(r.tags->>'width')) IS NULL
  AND r.class NOT IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'service')
  AND NOT r.tags ?| ARRAY['placement', 'placement:forward', 'placement:backward'];
ALTER TABLE _rm_rays ADD COLUMN line_ll geometry;
UPDATE _rm_rays SET line_ll = ST_Transform(line, 4326);
-- The ground-level paths and roads those cross, in pieces for the index.
-- The ids first: the filters below hide how many rows match, and joined in one
-- go the planner scans geo_places once per hit.
DROP TABLE IF EXISTS _rm_hits;
CREATE TEMP TABLE _rm_hits AS
SELECT DISTINCT h.id FROM _rm_rays y
CROSS JOIN LATERAL (SELECT g.id FROM geo_places g WHERE g.geom && y.line_ll AND g.geom_type = 'line' AND g.tags ? 'highway') h;
ANALYZE _rm_hits;
DROP TABLE IF EXISTS _rm_beside;
CREATE TEMP TABLE _rm_beside AS
SELECT c.osm_id, ST_Subdivide(ST_Transform(c.geom, 3857), 8) as g,
       COALESCE(c.tags->>'footway' = 'sidewalk' AND c.tags->>'highway' IN ('footway', 'path', 'cycleway'), false) as sidewalk,
       road_metres(c.tags->>'width') as walk_w
FROM _rm_hits hit
JOIN geo_places c ON c.id = hit.id
WHERE c.geom_type = 'line' AND COALESCE(c.tags->>'footway', '') <> 'crossing'
  AND c.tags->>'highway' NOT IN ('proposed', 'construction', 'abandoned', 'razed', 'platform')
  AND COALESCE(c.tags->>'bridge', 'no') = 'no' AND COALESCE(c.tags->>'tunnel', 'no') = 'no'
  AND COALESCE(road_int(c.tags->>'layer'), 0) = 0;
CREATE INDEX ON _rm_beside USING gist (g);
ANALYZE _rm_beside;
UPDATE _rm_roads r SET sh = (k.kerb_w - r.width) / 2, width = k.kerb_w
FROM (
  SELECT h.osm_id, 2 * least(percentile_cont(0.5) WITHIN GROUP (ORDER BY h.d) FILTER (WHERE h.side = 1),
                             percentile_cont(0.5) WITHIN GROUP (ORDER BY h.d) FILTER (WHERE h.side = -1)) as kerb_w
  FROM (
    -- The nearest path or road along each, if it is a sidewalk.
    SELECT y.osm_id, y.side, hit.d - least(3, greatest(0.75, COALESCE(hit.walk_w / 2, 2.25))) as d
    FROM _rm_rays y
    CROSS JOIN LATERAL (
      SELECT ST_Distance(y.p, ST_Intersection(c.g, y.line)) / y.s as d, c.sidewalk, c.walk_w
      FROM _rm_beside c
      WHERE c.g && y.line AND c.osm_id <> y.osm_id AND ST_Intersects(c.g, y.line)
      ORDER BY 1 LIMIT 1
    ) hit
    WHERE hit.sidewalk
  ) h
  GROUP BY h.osm_id
  HAVING count(*) FILTER (WHERE h.side = 1) >= 2 AND count(*) FILTER (WHERE h.side = -1) >= 2
) k
WHERE r.osm_id = k.osm_id AND k.kerb_w > r.width + 0.5 AND k.kerb_w <= r.width + 8;
-- Where each way ends, and its heading there, read from the line as mapped:
-- smoothing depends on what else a build read, and these decide pairs and
-- forks (below), which must come out the same in every box.
DROP TABLE IF EXISTS _rm_ends_at;
CREATE TEMP TABLE _rm_ends_at AS
SELECT osm_id, service, two_way, at_start, pt, round(ST_X(pt)::numeric, 2) as x, round(ST_Y(pt)::numeric, 2) as y,
       -- The way's heading where it meets the node, in its direction of travel.
       degrees(ST_Azimuth(a, b)) as heading
FROM (
  SELECT osm_id, class = 'service' as service, NOT oneway as two_way, e.at_start, e.pt,
         CASE WHEN e.at_start THEN ST_StartPoint(g) ELSE ST_LineInterpolatePoint(g, greatest(0, 1 - 10 * s / ST_Length(g))) END as a,
         CASE WHEN e.at_start THEN ST_LineInterpolatePoint(g, least(1, 10 * s / ST_Length(g))) ELSE ST_EndPoint(g) END as b
  FROM _rm_roads, LATERAL (VALUES (true, ST_StartPoint(g)), (false, ST_EndPoint(g))) e(at_start, pt)
  WHERE ST_Length(g) > 0
) h;
CREATE INDEX ON _rm_ends_at (x, y);
ANALYZE _rm_ends_at;
-- Ways that carry on into the next: one way ends where another starts, within
-- 35° of straight on, and nothing else leaving or arriving there is within 50°
-- of either (that is a fork; see Forks). Side streets and driveways meeting
-- the join do not break it, nor does a one-way ramp leaving or joining a
-- two-way road, but where streets cross there, two legs or more of them at
-- 25° or more, the join is a `junction`: the road changes inside the junction
-- rather than easing across the block beyond it (see Tapers). Only the ways
-- with an end at the join, and the streets through it, decide, so a scoped
-- build reading those (see Scope) pairs them as a full build does.
-- Two-way ways carry on into each other whichever way they were drawn: two
-- starting at a join, or two ending there, pair too (`_rm_flips`, below).
DROP TABLE IF EXISTS _rm_turns_at;
CREATE TEMP TABLE _rm_turns_at AS
SELECT e.osm_id as a_id, e.at_start as a_start, st.osm_id as b_id, st.at_start as b_start, e.x, e.y, e.pt, e.heading,
       e.two_way AND st.two_way as two_way,
       -- arriving along a (against it from its start), leaving along b (against it to its end)
       abs(((st.heading + CASE WHEN st.at_start THEN 0 ELSE 180 END - e.heading - CASE WHEN e.at_start THEN 180 ELSE 0 END + 900)::numeric % 360) - 180) as turn
FROM _rm_ends_at e JOIN _rm_ends_at st ON st.x = e.x AND st.y = e.y AND st.osm_id <> e.osm_id
WHERE e.service = st.service
  AND ((NOT e.at_start AND st.at_start) OR (e.at_start = st.at_start AND e.two_way AND st.two_way));
CREATE INDEX ON _rm_turns_at (x, y);
DROP TABLE IF EXISTS _rm_joins;
CREATE TEMP TABLE _rm_joins AS
SELECT t.a_id, t.a_start, t.b_id, t.b_start, COALESCE((
         SELECT sum(CASE WHEN ST_DWithin(ST_StartPoint(o.g), t.pt, 0.05) OR ST_DWithin(ST_EndPoint(o.g), t.pt, 0.05) THEN 1 ELSE 2 END)
         FROM _rm_roads o
         CROSS JOIN LATERAL (SELECT road_dir(o.g, ST_LineLocatePoint(o.g, t.pt), 2 * o.s) as u) d
         WHERE o.class <> 'service' AND o.osm_id <> t.a_id AND o.osm_id <> t.b_id
           AND o.g && ST_Expand(t.pt, 0.05) AND ST_DWithin(o.g, t.pt, 0.05)
           -- crossing, as the streets that cut its lines do (see Junctions), not a ramp alongside
           AND abs(d.u[1] * cos(radians(t.heading)) - d.u[2] * sin(radians(t.heading))) > 0.42), 0) >= 2 as junction
FROM _rm_turns_at t
WHERE t.turn < 35
  AND NOT EXISTS (SELECT 1 FROM _rm_turns_at o WHERE o.x = t.x AND o.y = t.y AND o.turn < 50 AND (o.two_way OR NOT t.two_way)
                  AND NOT (o.a_id IN (t.a_id, t.b_id) AND o.b_id IN (t.a_id, t.b_id))
                  AND ((o.a_id, o.a_start) IN ((t.a_id, t.a_start), (t.b_id, t.b_start))
                       OR (o.b_id, o.b_start) IN ((t.a_id, t.a_start), (t.b_id, t.b_start))));
DROP TABLE IF EXISTS _rm_pairs;
CREATE TEMP TABLE _rm_pairs AS
SELECT a_id as prev_id, b_id as next_id, junction FROM _rm_joins WHERE NOT a_start AND b_start;
CREATE INDEX ON _rm_pairs (next_id);
CREATE INDEX ON _rm_pairs (prev_id);
-- Two two-way ways drawn head to head (`at_start`) or tail to tail: the longer,
-- `id`, eases from the other's layout seen from its own side, at its start or
-- its end; the other, `other`, keeps its own.
DROP TABLE IF EXISTS _rm_flips;
CREATE TEMP TABLE _rm_flips AS
SELECT j.a_id as id, j.b_id as other, j.a_start as at_start, j.junction
FROM _rm_joins j JOIN _rm_roads a ON a.osm_id = j.a_id JOIN _rm_roads b ON b.osm_id = j.b_id
WHERE j.a_start = j.b_start AND (a.len_m, a.osm_id) > (b.len_m, b.osm_id);
CREATE INDEX ON _rm_flips (id);
-- A street keeps its kerbs where only its lanes are shared out differently: a
-- short way (under 100 m) narrower than the ways it runs on from and into
-- along the block, or than the one of them it has at the end of the block,
-- takes the narrower of their widths, and the extra is unmarked `sh` either
-- side. Streets only, off bridges: a ramp or highway narrower between two
-- wider stretches is built that way. Read from the widths before this, so
-- every box agrees. `held`: kerbs known to stand where they are, from the
-- sidewalks or along such a block.
ALTER TABLE _rm_roads ADD COLUMN held boolean NOT NULL DEFAULT false;
UPDATE _rm_roads SET held = true WHERE sh > 0;
DROP TABLE IF EXISTS _rm_links;
CREATE TEMP TABLE _rm_links AS
SELECT p.prev_id, p.next_id, a.width as prev_w, b.width as next_w
FROM _rm_pairs p JOIN _rm_roads a ON a.osm_id = p.prev_id JOIN _rm_roads b ON b.osm_id = p.next_id
WHERE NOT p.junction AND a.oneway = b.oneway AND NOT a.bridge AND NOT b.bridge
  AND a.class NOT IN ('motorway', 'trunk', 'service') AND a.class NOT LIKE '%\_link'
  AND b.class NOT IN ('motorway', 'trunk', 'service') AND b.class NOT LIKE '%\_link';
CREATE INDEX ON _rm_links (prev_id);
CREATE INDEX ON _rm_links (next_id);
DROP TABLE IF EXISTS _rm_held;
CREATE TEMP TABLE _rm_held AS
SELECT w.osm_id, w.width, h.w
FROM _rm_roads w
LEFT JOIN _rm_links lp ON lp.next_id = w.osm_id
LEFT JOIN _rm_links ln ON ln.prev_id = w.osm_id
CROSS JOIN LATERAL (SELECT CASE WHEN lp.prev_w IS NOT NULL AND ln.next_w IS NOT NULL THEN least(lp.prev_w, ln.next_w)
                                WHEN EXISTS (SELECT 1 FROM _rm_pairs j WHERE j.junction AND (j.next_id = w.osm_id OR j.prev_id = w.osm_id))
                                  THEN COALESCE(lp.prev_w, ln.next_w) END as w) h
WHERE w.len_m < 100 AND NOT w.bridge AND h.w > w.width + 0.2 AND h.w <= w.width + 8
  AND COALESCE(road_metres(w.tags->>'width:carriageway'), road_metres(w.tags->>'width')) IS NULL
  AND NOT w.tags ?| ARRAY['placement', 'placement:forward', 'placement:backward'];
UPDATE _rm_roads r SET sh = r.sh + (h.w - h.width) / 2, width = h.w, held = true FROM _rm_held h WHERE r.osm_id = h.osm_id;
UPDATE _rm_roads r SET held = true FROM _rm_held h JOIN _rm_links l ON l.prev_id = h.osm_id WHERE r.osm_id = l.next_id;
UPDATE _rm_roads r SET held = true FROM _rm_held h JOIN _rm_links l ON l.next_id = h.osm_id WHERE r.osm_id = l.prev_id;
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
WHERE park_r + park_l > 0 AND (width - 2 * sh - bike_r - bike_l - park_r - park_l) / greatest(fwd + bwd + both_ways, 1) < 2.6;
-- Across the carriageway, left of the way's direction is positive: the left
-- kerb, its parking and bike lane, the backward lanes, any centre turn lane,
-- the forward lanes, then the right side's bike lane, parking and kerb.
-- `split_b` and `split_f` bound the turn lane; without one they meet. A bike
-- lane's paint runs from `*_a` at the kerb side to `*_b`; a track sits at the
-- kerb with the parking outside it, a lane outside the parking.
ALTER TABLE _rm_roads ADD COLUMN lane_w float8, ADD COLUMN left_edge float8, ADD COLUMN right_edge float8,
  ADD COLUMN split_b float8, ADD COLUMN split_f float8,
  ADD COLUMN br_a float8, ADD COLUMN br_b float8, ADD COLUMN bl_a float8, ADD COLUMN bl_b float8;
UPDATE _rm_roads SET lane_w = (width - 2 * sh - bike_r - bike_l - park_r - park_l) / greatest(fwd + bwd + both_ways, 1);
UPDATE _rm_roads SET left_edge = width / 2 - sh - bike_l - park_l, right_edge = -width / 2 + sh + bike_r + park_r,
  br_a = -width / 2 + CASE WHEN track_r THEN 0 ELSE park_r + sh END,
  bl_a = width / 2 - CASE WHEN track_l THEN 0 ELSE park_l + sh END;
UPDATE _rm_roads SET br_b = br_a + bike_r - buf_r, bl_b = bl_a - bike_l + buf_l;
UPDATE _rm_roads SET split_b = left_edge - bwd * lane_w;
UPDATE _rm_roads SET split_f = split_b - both_ways * lane_w;
ANALYZE _rm_roads;

-- At each join, the direction halfway between the two ways: both offset their
-- lines square to it there, so they meet.
ALTER TABLE _rm_roads ADD COLUMN dir_s float8[], ADD COLUMN dir_e float8[];
WITH j AS (
  -- through the join from a to b: arriving along a, leaving along b
  SELECT j.a_id, j.a_start, j.b_id, j.b_start, sa * ua[1] + sb * ub[1] as x, sa * ua[2] + sb * ub[2] as y
  FROM _rm_joins j JOIN _rm_roads a ON a.osm_id = j.a_id JOIN _rm_roads b ON b.osm_id = j.b_id,
       LATERAL (SELECT road_dir(a.g, CASE WHEN j.a_start THEN 0 ELSE 1 END, 2 * a.s) as ua, CASE WHEN j.a_start THEN -1 ELSE 1 END as sa,
                       road_dir(b.g, CASE WHEN j.b_start THEN 0 ELSE 1 END, 2 * b.s) as ub, CASE WHEN j.b_start THEN 1 ELSE -1 END as sb) d
  WHERE ua IS NOT NULL AND ub IS NOT NULL
), u AS (
  -- each way's end, turned to run with the way
  SELECT DISTINCT ON (id, at_start) id, at_start, ARRAY[sg * x / n, sg * y / n] as u
  FROM j, LATERAL (SELECT sqrt(x * x + y * y) as n) l,
       LATERAL (VALUES (a_id, a_start, CASE WHEN a_start THEN -1 ELSE 1 END), (b_id, b_start, CASE WHEN b_start THEN 1 ELSE -1 END)) v(id, at_start, sg)
  WHERE n > 0
  ORDER BY id, at_start
)
UPDATE _rm_roads r SET dir_s = s.u, dir_e = e.u
FROM _rm_roads r2
LEFT JOIN u s ON s.id = r2.osm_id AND s.at_start
LEFT JOIN u e ON e.id = r2.osm_id AND NOT e.at_start
WHERE r.osm_id = r2.osm_id AND (s.u IS NOT NULL OR e.u IS NOT NULL);

-- ─── Where the way runs ──────────────────────────────────────────────────────
-- The carriageway is centred on its way unless something says otherwise:
-- `kshift` moves it across, and `shift` its lanes. `placement` says so
-- outright. A turn bay hangs off its own side, so the lanes that carry on
-- through a junction stay centred and line up with the lanes beyond it; on a
-- two-way road the centre line steps across for a left bay, as built. On a
-- street the lanes step across within any room beside them (`sh`) first, and
-- the kerbs take the rest, unless they are `held`. A way's
-- place depends on its own tags only; how it eases from the ways before it is
-- up to Tapers.
ALTER TABLE _rm_roads ADD COLUMN placed boolean NOT NULL DEFAULT false, ADD COLUMN base float8 NOT NULL DEFAULT 0,
  ADD COLUMN shift float8, ADD COLUMN kshift float8, ADD COLUMN kerb_l float8, ADD COLUMN kerb_r float8, ADD COLUMN axis geometry,
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
UPDATE _rm_roads SET shift = CASE WHEN placed OR class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') THEN base ELSE greatest(-sh, least(sh, base)) END;
UPDATE _rm_roads SET kshift = CASE WHEN placed OR class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') THEN base WHEN held THEN 0 ELSE base - shift END;
-- The carriageway's own centreline, where it is not the way; a way GEOS cannot
-- offset stays on its way.
UPDATE _rm_roads SET axis = road_offset(g, kshift * s) WHERE abs(kshift) >= 0.05;
UPDATE _rm_roads SET shift = 0, kshift = 0, axis = NULL WHERE abs(kshift) >= 0.05 AND COALESCE(GeometryType(axis), '') <> 'LINESTRING';
-- A track keeps to its kerb.
UPDATE _rm_roads SET left_edge = left_edge + shift, right_edge = right_edge + shift,
  split_b = split_b + shift, split_f = split_f + shift,
  br_a = br_a + CASE WHEN track_r THEN kshift ELSE shift END, br_b = br_b + CASE WHEN track_r THEN kshift ELSE shift END,
  bl_a = bl_a + CASE WHEN track_l THEN kshift ELSE shift END, bl_b = bl_b + CASE WHEN track_l THEN kshift ELSE shift END,
  kerb_l = width / 2 + kshift, kerb_r = -width / 2 + kshift,
  axis = CASE WHEN local THEN axis END;

-- ─── Forks ───────────────────────────────────────────────────────────────────
-- Where a road splits or ways merge into one (a ramp leaving or joining, a
-- street dividing into two one-ways) the branches share the stem's
-- carriageway at the node: the leftmost branch keeps the stem's left kerb,
-- the rightmost its right kerb, and any between are spaced across it by
-- width. So lanes carry on from a fixed edge rather than each branch running
-- down the middle of the stem. Each branch eases back to its own place away
-- from the node (see Tapers); `fork_s` and `fork_e` are how far it stands off
-- that at its start and end. A fork is a node where three to five street ways
-- end and none runs through (service roads aside), the branches one-way,
-- within 50° of the stem's line and flowing the same way.
ALTER TABLE _rm_roads ADD COLUMN fork_s float8 NOT NULL DEFAULT 0, ADD COLUMN fork_e float8 NOT NULL DEFAULT 0,
  ADD COLUMN fork_ease_s float8, ADD COLUMN fork_ease_e float8;
DROP TABLE IF EXISTS _rm_fork_members;
CREATE TEMP TABLE _rm_fork_members AS
WITH node AS (
  SELECT x, y, (array_agg(pt))[1] as pt, array_agg(osm_id) as ids FROM _rm_ends_at WHERE NOT service GROUP BY x, y
  HAVING count(*) BETWEEN 3 AND 5
)
-- `out` is +1 for traffic leaving the node, -1 arriving, 0 both ways; `ux`,
-- `uy` the way's direction away from the node.
SELECT n.x, n.y, r.osm_id, r.oneway, e.at_start, r.kerb_l, r.kerb_r, r.width, r.bridge,
       CASE WHEN NOT r.oneway THEN 0 WHEN e.at_start THEN 1 ELSE -1 END as out,
       sin(radians(e.heading)) * CASE WHEN e.at_start THEN 1 ELSE -1 END as ux,
       cos(radians(e.heading)) * CASE WHEN e.at_start THEN 1 ELSE -1 END as uy
FROM node n
JOIN _rm_ends_at e ON e.x = n.x AND e.y = n.y AND NOT e.service
JOIN _rm_roads r ON r.osm_id = e.osm_id
-- and no other street running through it
WHERE NOT EXISTS (SELECT 1 FROM _rm_roads o WHERE o.class <> 'service' AND o.g && ST_Expand(n.pt, 0.05)
                  AND ST_DWithin(o.g, n.pt, 0.05) AND o.osm_id <> ALL (n.ids));
CREATE INDEX ON _rm_fork_members (x, y);
ANALYZE _rm_fork_members;
WITH fork AS (
  -- The stem: the one way whose traffic all the others divide or join. `u`
  -- points from the node along the branches; kl, kr are the stem's kerbs to
  -- the left and right of it.
  SELECT m.x, m.y, m.osm_id as stem, -m.ux as ux, -m.uy as uy, m.width as stem_w,
         CASE WHEN m.at_start THEN -m.kerb_r ELSE m.kerb_l END as kl,
         CASE WHEN m.at_start THEN -m.kerb_l ELSE m.kerb_r END as kr
  FROM _rm_fork_members m
  JOIN _rm_fork_members b ON b.x = m.x AND b.y = m.y AND b.osm_id <> m.osm_id
  GROUP BY m.x, m.y, m.osm_id, m.ux, m.uy, m.width, m.at_start, m.kerb_l, m.kerb_r, m.out
  HAVING bool_and(b.oneway AND (b.out = -m.out OR m.out = 0) AND b.ux * -m.ux + b.uy * -m.uy > cos(radians(50)))
     AND (m.out <> 0 OR sum(b.out) = 0)
),
side AS (
  SELECT f.*, b.osm_id, b.at_start, b.width, b.bridge, (b.kerb_l + b.kerb_r) / 2 as mid, abs(f.ux * b.uy - f.uy * b.ux) as sine,
         dense_rank() OVER w as k, count(*) OVER (PARTITION BY f.x, f.y) as n,
         sum(b.width) OVER w - b.width as wl, sum(b.width) OVER (PARTITION BY f.x, f.y) as wt
  FROM fork f JOIN _rm_fork_members b ON b.x = f.x AND b.y = f.y AND b.osm_id <> f.stem
  WINDOW w AS (PARTITION BY f.x, f.y ORDER BY f.ux * b.uy - f.uy * b.ux DESC)
),
place AS (
  -- Where each branch's middle belongs across `u`, left positive; branches
  -- side by side only (no two at one bearing).
  SELECT *, CASE WHEN k = 1 THEN kl - width / 2 WHEN k = n THEN kr + width / 2
                 ELSE kl - (wl + width / 2) * stem_w / wt END as c
  FROM (SELECT *, max(k) OVER (PARTITION BY x, y) as top FROM side) t
  WHERE n >= 2 AND top = n
),
delta AS (
  -- A branch arriving at the node runs against `u`, so its sides swap. It
  -- eases back over about as far as its mapped line takes to drift that far
  -- off the stem's, so a branch leaving at an angle does not swing wide.
  -- Bridges keep their mapped line: decks are fitted against it.
  SELECT osm_id, at_start, d, abs(d) / greatest(sine, 0.03) as ease
  FROM place, LATERAL (SELECT CASE WHEN at_start THEN c - mid ELSE -c - mid END as d) dd
  WHERE abs(d) < stem_w AND NOT bridge
)
UPDATE _rm_roads r SET fork_s = COALESCE(s.d, 0), fork_e = COALESCE(e.d, 0), fork_ease_s = s.ease, fork_ease_e = e.ease
FROM (SELECT osm_id FROM delta GROUP BY osm_id) m
LEFT JOIN delta s ON s.osm_id = m.osm_id AND s.at_start
LEFT JOIN delta e ON e.osm_id = m.osm_id AND NOT e.at_start
WHERE r.osm_id = m.osm_id;

-- ─── Tapers ──────────────────────────────────────────────────────────────────
-- Where one road simply continues into the next, the next starts from the
-- first's lane layout and eases into its own: a lane opening or closing drifts
-- across over a taper rather than jumping sideways at the join. The prev_*
-- columns are where the way's lines stood on the way before. A taper runs to
-- the MUTCD's length for the road's speed (L = WS²/60 ft up to 40 mph, WS
-- above), at most 150 m, and on into the next way where its own is short; a
-- run of short ways each adding a lane reaches into the next one's taper, so
-- they flare out as one. A taper ends within the way after its own: what a way
-- shows depends on the two ways before it (and their neighbours' widths), and
-- only near its start. Two two-way ways drawn tail to tail meet the same way,
-- the longer easing at its end (`nxt_*`, `ease_e`) into the other's layout.
ALTER TABLE _rm_roads
  ADD COLUMN prev_left float8, ADD COLUMN prev_right float8, ADD COLUMN prev_kerb_l float8, ADD COLUMN prev_kerb_r float8,
  ADD COLUMN prev_split_b float8, ADD COLUMN prev_split_f float8, ADD COLUMN prev_lane_w float8, ADD COLUMN ease float8,
  ADD COLUMN opens_kerb_f boolean, ADD COLUMN opens_kerb_b boolean, ADD COLUMN body geometry, ADD COLUMN ease_e float8,
  ADD COLUMN carries boolean NOT NULL DEFAULT false, ADD COLUMN next_len float8, ADD COLUMN next_grows float8,
  ADD COLUMN pos float8[], ADD COLUMN disp float8[], ADD COLUMN emerge float8[],
  ADD COLUMN res_pos float8[], ADD COLUMN res_disp float8[], ADD COLUMN res_emerge float8[], ADD COLUMN res_at float8, ADD COLUMN res_len float8,
  ADD COLUMN res_close float8;
-- The layout of the way each way carries on from at its start (`at_start`),
-- or into at its end, seen from its own side: a way drawn the other way round
-- has its sides and directions swapped. Only where tags agree: `carries`, the
-- way carries on from the one before it in the same layout. A way's end eases
-- only from one drawn head to head with it (see _rm_flips); the way after a
-- way it runs on into eases from it instead.
DROP TABLE IF EXISTS _rm_from;
CREATE TEMP TABLE _rm_from AS
SELECT p.next_id as id, true as at_start, a.left_edge as l, a.right_edge as r, a.kerb_l as kl, a.kerb_r as kr,
       a.split_b as sb, a.split_f as sf, a.lane_w as lw
FROM _rm_pairs p JOIN _rm_roads a ON a.osm_id = p.prev_id JOIN _rm_roads b ON b.osm_id = p.next_id
WHERE NOT p.junction AND a.marked = b.marked AND a.oneway = b.oneway
UNION ALL
SELECT f.id, f.at_start, -o.right_edge, -o.left_edge, -o.kerb_r, -o.kerb_l, -o.split_f, -o.split_b, o.lane_w
FROM _rm_flips f JOIN _rm_roads o ON o.osm_id = f.other JOIN _rm_roads t ON t.osm_id = f.id
WHERE NOT f.junction AND o.marked = t.marked;
CREATE INDEX ON _rm_from (id);
UPDATE _rm_roads b SET carries = true FROM _rm_from a WHERE b.osm_id = a.id AND a.at_start;
-- Only where a line would otherwise step at the join.
DELETE FROM _rm_from a USING _rm_roads b
WHERE b.osm_id = a.id AND abs(a.kl - b.kerb_l) <= 0.05 AND abs(a.kr - b.kerb_r) <= 0.05 AND abs(a.l - b.left_edge) <= 0.05
  AND abs(a.r - b.right_edge) <= 0.05 AND abs(a.sb - b.split_b) <= 0.05 AND abs(a.sf - b.split_f) <= 0.05 AND abs(a.lw - b.lane_w) <= 0.05;
UPDATE _rm_roads b SET
  prev_left = a.l, prev_right = a.r, prev_kerb_l = a.kl, prev_kerb_r = a.kr, prev_split_b = a.sb, prev_split_f = a.sf, prev_lane_w = a.lw
FROM _rm_from a WHERE b.osm_id = a.id AND a.at_start;
ALTER TABLE _rm_roads
  ADD COLUMN nxt_left float8, ADD COLUMN nxt_right float8, ADD COLUMN nxt_kerb_l float8, ADD COLUMN nxt_kerb_r float8,
  ADD COLUMN nxt_split_b float8, ADD COLUMN nxt_split_f float8, ADD COLUMN nxt_lane_w float8;
UPDATE _rm_roads b SET
  nxt_left = a.l, nxt_right = a.r, nxt_kerb_l = a.kl, nxt_kerb_r = a.kr, nxt_split_b = a.sb, nxt_split_f = a.sf, nxt_lane_w = a.lw
FROM _rm_from a WHERE b.osm_id = a.id AND NOT a.at_start;
-- How long the way it carries on into is, and how much wider (or, below
-- zero, narrower) it gets there.
UPDATE _rm_roads a SET next_len = b.len_m, next_grows = (b.kerb_l - b.kerb_r) - (a.kerb_l - a.kerb_r)
FROM _rm_pairs p JOIN _rm_roads b ON b.osm_id = p.next_id
WHERE a.osm_id = p.prev_id AND b.carries;
UPDATE _rm_roads SET
  ease = least(len_m + COALESCE(next_len, 0), greatest(least(30, CASE WHEN next_len IS NULL THEN 0.6 * len_m ELSE len_m + next_len END),
           least(0.6 * (len_m + COALESCE(next_len, 0)), t.mutcd),
           CASE WHEN sign(next_grows) = sign((kerb_l - kerb_r) - (prev_kerb_l - prev_kerb_r)) AND abs(next_grows) > 0.2
                THEN least(t.mutcd, len_m + 0.5 * next_len) END)),
  opens_kerb_f = COALESCE(fwd > 0 AND split_part(turn_forward, '|', greatest(fwd, 1)) LIKE '%right%' AND split_part(turn_forward, '|', 1) NOT LIKE '%left%', false),
  opens_kerb_b = COALESCE(bwd > 0 AND split_part(turn_backward, '|', greatest(bwd, 1)) LIKE '%right%' AND split_part(turn_backward, '|', 1) NOT LIKE '%left%', false)
FROM (SELECT osm_id as id, least(150, CASE WHEN mph >= 45 THEN lane_w * mph ELSE lane_w * mph * mph / 60 END) as mutcd FROM _rm_roads) t
WHERE osm_id = t.id AND prev_left IS NOT NULL;
-- A branch starting at a fork eases in from its place in the stem, within
-- its own length.
UPDATE _rm_roads SET prev_left = left_edge + fork_s, prev_right = right_edge + fork_s,
  prev_kerb_l = kerb_l + fork_s, prev_kerb_r = kerb_r + fork_s,
  prev_split_b = split_b + fork_s, prev_split_f = split_f + fork_s, prev_lane_w = lane_w,
  ease = least(0.6 * len_m, greatest(10, least(fork_ease_s, CASE WHEN mph >= 45 THEN lane_w * mph ELSE lane_w * mph * mph / 60 END))),
  opens_kerb_f = false, opens_kerb_b = false
WHERE abs(fork_s) > 0.05 AND prev_left IS NULL;
UPDATE _rm_roads SET ease_e = least(0.4 * len_m, greatest(10, least(fork_ease_e,
           CASE WHEN mph >= 45 THEN lane_w * mph ELSE lane_w * mph * mph / 60 END)))
WHERE abs(fork_e) > 0.05;
-- A way easing into the layout of one drawn tail to tail with it, over the
-- same length it would ease in from one, within its own length.
UPDATE _rm_roads SET ease_e = least(len_m, greatest(least(30, 0.6 * len_m),
                                    least(0.6 * len_m, least(150, CASE WHEN mph >= 45 THEN lane_w * mph ELSE lane_w * mph * mph / 60 END))))
WHERE nxt_left IS NOT NULL;
-- Where each of the way's lines ends up (`pos`) and how far its taper moves
-- it there (`disp`): kerbs, edges, the centre and the dividers between, each
-- divider from where road_divider_start puts it on the way before. `emerge`:
-- the dividers of lanes that open here, which start once their lane has.
UPDATE _rm_roads SET pos = d.pos, disp = d.disp, emerge = d.emerge
FROM (
  SELECT osm_id as id, array_agg(p ORDER BY p, st) as pos, array_agg(p - st ORDER BY p, st) as disp,
         array_agg(p ORDER BY p) FILTER (WHERE opens) as emerge
  FROM _rm_roads r,
       LATERAL (VALUES (r.kerb_l, r.prev_kerb_l, false), (r.left_edge, r.prev_left, false), (r.split_b, r.prev_split_b, false),
                       (r.split_f, r.prev_split_f, false), (r.right_edge, r.prev_right, false), (r.kerb_r, r.prev_kerb_r, false),
                       (r.left_edge - 0.3, r.prev_left - 0.3, false), (r.right_edge + 0.3, r.prev_right + 0.3, false)
               UNION ALL
               SELECT o, road_side_start(o, r.kerb_r, r.right_edge, r.prev_kerb_r, r.prev_right, r.track_r), false
               FROM unnest(ARRAY[r.br_b, r.br_b + r.buf_r, r.br_a]) o WHERE r.bike_r > 0
               UNION ALL
               SELECT o, road_side_start(o, r.kerb_l, r.left_edge, r.prev_kerb_l, r.prev_left, r.track_l), false
               FROM unnest(ARRAY[r.bl_b, r.bl_b - r.buf_l, r.bl_a]) o WHERE r.bike_l > 0
               UNION ALL
               SELECT r.split_f - i * r.lane_w, st, abs(st - r.prev_split_f) < 0.05 OR abs(st - r.prev_right) < 0.05
               FROM generate_series(1, r.fwd - 1) i,
                    LATERAL (SELECT road_divider_start(i, r.fwd, r.prev_split_f, r.prev_right, r.prev_lane_w, r.opens_kerb_f) as st) d
               UNION ALL
               SELECT r.split_b + i * r.lane_w, st, abs(st - r.prev_split_b) < 0.05 OR abs(st - r.prev_left) < 0.05
               FROM generate_series(1, r.bwd - 1) i,
                    LATERAL (SELECT road_divider_start(i, r.bwd, r.prev_split_b, r.prev_left, r.prev_lane_w, r.opens_kerb_b) as st) d) v(p, st, opens)
  WHERE r.prev_left IS NOT NULL
  GROUP BY osm_id
) d
WHERE osm_id = d.id;
-- The taper of the way before, where it runs on into this one.
-- `res_close`: how far the line closing in on the centre (see Lane lines)
-- still had to go where the way before ended.
UPDATE _rm_roads b SET res_pos = a.pos, res_disp = a.disp, res_emerge = a.emerge, res_at = a.len_m, res_len = a.ease,
  res_close = CASE WHEN NOT a.oneway AND a.fwd > 0 AND a.bwd > 0 AND a.both_ways = 0 AND abs(a.prev_split_f - a.prev_split_b) > 0.2
                   THEN a.split_f - a.prev_split_f END
FROM _rm_pairs p JOIN _rm_roads a ON a.osm_id = p.prev_id
WHERE b.osm_id = p.next_id AND b.carries AND a.carries AND a.prev_left IS NOT NULL AND a.ease > a.len_m;
-- A tapering road's carriageway, between its two eased kerbs; a kerb that
-- stays put does not move.
UPDATE _rm_roads SET body = CASE WHEN prev_left IS NULL AND res_len IS NULL AND cap_round
    THEN ST_Union(road_body(l, r, ST_EndPoint(l), ST_EndPoint(r)), ST_Buffer(ST_LineInterpolatePoint(ST_MakeLine(ST_StartPoint(l), ST_StartPoint(r)), 0.5), width / 2 * s, 8))
    ELSE road_body(l, r, ST_EndPoint(l), ST_EndPoint(r)) END
FROM (SELECT osm_id as id, COALESCE(tags->>'bridge', 'no') = 'no' AND COALESCE(road_int(tags->>'layer'), 0) <= 0 as cap_round,
             road_profile(g, s, len_m, kerb_l, prev_kerb_l, ease, road_field(res_pos, res_disp, COALESCE(prev_kerb_l, kerb_l)),
                          res_at, res_len, COALESCE(nxt_kerb_l - kerb_l, fork_e), ease_e, dir_s, dir_e) as l,
             road_profile(g, s, len_m, kerb_r, prev_kerb_r, ease, road_field(res_pos, res_disp, COALESCE(prev_kerb_r, kerb_r)),
                          res_at, res_len, COALESCE(nxt_kerb_r - kerb_r, fork_e), ease_e, dir_s, dir_e) as r
      FROM _rm_roads WHERE (prev_left IS NOT NULL OR ease_e IS NOT NULL OR res_len IS NOT NULL) AND local) e
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
-- approaches below the top road, or all of them at an all-way stop. A give way
-- sign binds the same way, short of a stop.
DROP TABLE IF EXISTS _rm_signals;
CREATE TEMP TABLE _rm_signals AS
SELECT ST_Transform(geom, 3857) as g, tags->>'highway' as kind,
       COALESCE(tags->>'direction', tags->>'traffic_signals:direction', tags->>'stop:direction', tags->>'give_way:direction') as direction,
       COALESCE(tags->>'stop' = 'all', false) as all_way
FROM geo_places
WHERE geom && (SELECT area FROM _rm_area) AND geom_type = 'point'
  AND (tags @> '{"highway": "traffic_signals"}' OR tags @> '{"highway": "stop"}' OR tags @> '{"highway": "give_way"}');
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
         WHEN tags->>'crossing:markings' IN ('lines', 'lines:paired') THEN 'lines'
         WHEN tags->>'crossing:markings' IN ('dashes', 'dots') THEN tags->>'crossing:markings'
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

-- ─── Painted areas ──────────────────────────────────────────────────────────
-- Hatched areas traffic keeps off, a gore where lanes part or a painted median,
-- mapped as an area of their own: road_marking=restriction with its `pattern`,
-- `colour` and outline `stroke`, or the older area:highway=prohibited and
-- traffic_calming=painted_island. They lie on the carriageway, so they are
-- paved too. Untagged, paint between opposing traffic is yellow in the
-- Americas (a two-way road running through it, or one-ways either side running
-- opposite ways) and white elsewhere. Chevrons need the traffic's direction
-- from a one-way beside them; without one they are drawn as stripes. Only
-- areas under 200 m across are read, so every box that reaches one reads all
-- the roads it touches.
DROP TABLE IF EXISTS _rm_paint_mapped;
CREATE TEMP TABLE _rm_paint_mapped AS
SELECT a.osm_type, a.osm_id, ST_CollectionExtract(ST_MakeValid(ST_Transform(a.geom, 3857)), 3) as g,
       1 / cos(radians(ST_Y(ST_Centroid(a.geom)))) as s, ST_X(ST_Centroid(a.geom)) < -30 as americas,
       lower(a.tags->>'colour') as colour,
       CASE WHEN a.tags->>'road_marking' = 'restriction' THEN COALESCE(a.tags->>'pattern', 'stripes') ELSE 'stripes' END as pattern,
       COALESCE(a.tags->>'stroke', 'solid') as stroke
FROM geo_places a
WHERE a.geom && (SELECT area FROM _rm_area) AND a.geom_type = 'area'
  -- Rare tags: the tags index finds them faster than the box does.
  AND (a.tags @> '{"road_marking": "restriction"}' OR a.tags @> '{"area:highway": "prohibited"}' OR a.tags @> '{"traffic_calming": "painted_island"}')
  AND COALESCE(a.tags->>'bridge', 'no') = 'no' AND COALESCE(a.tags->>'tunnel', 'no') = 'no' AND COALESCE(road_int(a.tags->>'layer'), 0) = 0
  AND ST_XMax(a.geom) - ST_XMin(a.geom) < 0.002 AND ST_YMax(a.geom) - ST_YMin(a.geom) < 0.002
  AND COALESCE(lower(a.tags->>'colour'), 'white') IN ('white', 'yellow');
DELETE FROM _rm_paint_mapped WHERE ST_IsEmpty(g);
CREATE INDEX ON _rm_paint_mapped (osm_type, osm_id);
ANALYZE _rm_paint_mapped;
-- The roads each area lies on or beside, and their direction there.
DROP TABLE IF EXISTS _rm_paint_near;
CREATE TEMP TABLE _rm_paint_near AS
SELECT m.osm_type, m.osm_id, r.osm_id as road, r.oneway, ST_Intersects(r.g, m.g) as through, ST_Distance(r.g, m.g) as dist,
       road_dir(r.g, ST_LineLocatePoint(r.g, ST_Centroid(m.g)), 5 * r.s) as u
FROM _rm_paint_mapped m
JOIN _rm_roads r ON r.g && ST_Expand(m.g, 50 * m.s)
 AND ST_DWithin(r.g, m.g, (r.width / 2 + abs(r.kshift) + 2) * r.s) AND NOT r.bridge;
CREATE INDEX ON _rm_paint_near (osm_type, osm_id);
ANALYZE _rm_paint_near;
-- The nearest one-way's direction (the lowest id among equals), and whether
-- the area lies between opposing traffic.
DROP TABLE IF EXISTS _rm_paint_flow;
CREATE TEMP TABLE _rm_paint_flow AS
SELECT osm_type, osm_id, bool_or(NOT oneway AND through) as two_way,
       (array_agg(u[1] ORDER BY dist, road) FILTER (WHERE oneway AND u IS NOT NULL))[1] as ux,
       (array_agg(u[2] ORDER BY dist, road) FILTER (WHERE oneway AND u IS NOT NULL))[1] as uy
FROM _rm_paint_near GROUP BY osm_type, osm_id;
CREATE INDEX ON _rm_paint_flow (osm_type, osm_id);
ANALYZE _rm_paint_flow;
DROP TABLE IF EXISTS _rm_paint_areas;
CREATE TEMP TABLE _rm_paint_areas AS
SELECT m.g, m.s, m.stroke,
       CASE WHEN m.pattern = 'chevron' AND f.ux IS NULL THEN 'stripes' ELSE m.pattern END as pattern,
       COALESCE(m.colour, CASE WHEN m.americas AND (f.two_way OR o.opposed) THEN 'yellow' ELSE 'white' END) as color,
       f.ux, f.uy,
       -- Bars about one and a half times the area's width (2 A / P for a
       -- strip) apart, 3 to 8 m.
       least(greatest(1.5 * 2 * ST_Area(m.g) / greatest(ST_Perimeter(m.g), 1e-6) / m.s, 3), 8) as step
FROM _rm_paint_mapped m
JOIN _rm_paint_flow f ON f.osm_type = m.osm_type AND f.osm_id = m.osm_id
CROSS JOIN LATERAL (
  SELECT EXISTS (SELECT 1 FROM _rm_paint_near a JOIN _rm_paint_near b ON b.osm_type = a.osm_type AND b.osm_id = a.osm_id
                 WHERE a.osm_type = m.osm_type AND a.osm_id = m.osm_id AND a.oneway AND b.oneway
                   AND a.u[1] * b.u[1] + a.u[2] * b.u[2] < -0.5) as opposed
) o;
CREATE INDEX ON _rm_paint_areas USING gist (g);
ANALYZE _rm_paint_areas;

-- ─── Surfaces ────────────────────────────────────────────────────────────────
-- Each road widened to its carriageway, and at every vertex roads share, the
-- corners between just those roads filled to a kerb radius. Rounding only
-- what meets keeps a median, an island or a plaza between two roads that
-- never join open. A median's end is rounded off from both of its sides, so
-- beside one (another carriageway running alongside, `gap` metres of kerb
-- apart) its corner takes half the gap rather than the full radius, and the
-- nose comes out round rather than drawn to a point. Unioned per grid cell,
-- each keeping only its own square, so the seams between cells match.
DROP TABLE IF EXISTS _rm_fillets;
CREATE TEMP TABLE _rm_fillets AS
SELECT bridge, p, ST_Intersection(
         -- Where the reach cuts across a corner's fill, the kerb turns off
         -- round rather than at a notch.
         ST_Buffer(ST_Buffer(ST_Union(u, ST_Intersection(
           CASE WHEN m.near IS NULL THEN ST_Buffer(ST_Buffer(u, 4 * s, 'quad_segs=8'), -4 * s, 'quad_segs=8')
                ELSE ST_Union(ST_Difference(ST_Buffer(ST_Buffer(u, 4 * s, 'quad_segs=8'), -4 * s, 'quad_segs=8'), m.near),
                              ST_Buffer(ST_Buffer(u, m.r * s, 'quad_segs=8'), -m.r * s, 'quad_segs=8')) END,
           ST_Buffer(p, reach, 16))), least(2, COALESCE(m.r, 2)) * s, 'quad_segs=8'), -least(2, COALESCE(m.r, 2)) * s, 'quad_segs=8'),
         ST_Buffer(p, reach + 4 * s, 16)) as g
FROM (
  SELECT i.bridge, v.p, v.s, (v.width * 0.75 + 10) * v.s as reach, array_agg(i.osm_id) as ids,
         array_agg(i.bearing) as bearings, array_agg(r.width) as widths, array_agg(r.len_m) as lens,
         ST_Union(COALESCE(ST_Intersection(r.body, ST_Expand(v.p, (v.width + 10 + r.width) * v.s)),
                  ST_Buffer(ST_Intersection(COALESCE(r.axis, r.g), ST_Expand(v.p, (v.width + 10 + r.width) * v.s)), r.width / 2 * r.s,
                            r.cap))) as u
  FROM _rm_vertices v
  JOIN _rm_incid i ON i.x = v.x AND i.y = v.y
  JOIN _rm_roads r ON r.osm_id = i.osm_id
  GROUP BY i.bridge, v.p, v.s, v.width
) pieces
-- The carriageways alongside, 2 to 8 m of kerb away from one of the roads
-- here running the same line, and the median between: from each one's
-- middle out to the near kerb of ours. Both run on for a block (40 m): short
-- ways are the pieces of a junction, whose gaps the full radius closes, as it
-- does a sliver under 2 m.
LEFT JOIN LATERAL (
  SELECT ST_Union(ST_Buffer(ST_Intersection(COALESCE(o.axis, o.g), ST_Expand(pieces.p, pieces.reach + 10 * o.s)),
                            (o.width / 2 + q.gap + 0.25) * o.s, 'quad_segs=4')) as near,
         CASE WHEN count(*) > 0 THEN min(q.gap) / 2 - 0.1 END as r
  FROM _rm_roads o
  CROSS JOIN LATERAL (SELECT road_dir(o.g, ST_LineLocatePoint(o.g, pieces.p), 5 * o.s) as u,
                             ST_ClosestPoint(COALESCE(o.axis, o.g), pieces.p) as c) d
  CROSS JOIN LATERAL (
    SELECT ST_Distance(pieces.p, COALESCE(o.axis, o.g)) / o.s - o.width / 2 - max(w.width) / 2 as gap
    FROM unnest(pieces.bearings, pieces.widths, pieces.lens) w(bearing, width, len)
    WHERE abs(d.u[1] * sin(radians(w.bearing)) + d.u[2] * cos(radians(w.bearing))) > 0.9 AND w.len > 40
  ) q
  WHERE o.g && ST_Expand(pieces.p, 25 * pieces.s) AND o.bridge = pieces.bridge AND o.osm_id <> ALL (pieces.ids) AND o.len_m > 40
    AND d.u IS NOT NULL AND q.gap > 2 AND q.gap < 8
    -- square across from here, not further along the same line
    AND abs((ST_X(d.c) - ST_X(pieces.p)) * d.u[1] + (ST_Y(d.c) - ST_Y(pieces.p)) * d.u[2]) < 0.5 * ST_Distance(d.c, pieces.p)
) m ON true;
DELETE FROM _rm_fillets WHERE g IS NULL OR ST_IsEmpty(g);
CREATE INDEX ON _rm_fillets USING gist (g);

DROP TABLE IF EXISTS _rm_surfaces;
CREATE TEMP TABLE _rm_surfaces (bridge boolean, g geometry);
-- Every cell a carriageway reaches, not only those its centreline passes.
WITH cells AS (
  SELECT DISTINCT cx, cy
  FROM _rm_roads r,
       LATERAL (SELECT (ST_DumpPoints(ST_Segmentize(r.g, 200))).geom as c) d,
       LATERAL (SELECT (r.width / 2 + abs(r.kshift) + greatest(abs(r.fork_s), abs(r.fork_e)) + 15) * r.s as reach) m,
       generate_series(floor((ST_X(d.c) - m.reach) / 800)::int, floor((ST_X(d.c) + m.reach) / 800)::int) cx,
       generate_series(floor((ST_Y(d.c) - m.reach) / 800)::int, floor((ST_Y(d.c) + m.reach) / 800)::int) cy
  UNION
  SELECT cx, cy
  FROM _rm_turns t,
       generate_series(floor(ST_XMin(t.g) / 800)::int, floor(ST_XMax(t.g) / 800)::int) cx,
       generate_series(floor(ST_YMin(t.g) / 800)::int, floor(ST_YMax(t.g) / 800)::int) cy
  UNION
  SELECT cx, cy
  FROM _rm_paint_areas a,
       generate_series(floor(ST_XMin(a.g) / 800)::int, floor(ST_XMax(a.g) / 800)::int) cx,
       generate_series(floor(ST_YMin(a.g) / 800)::int, floor(ST_YMax(a.g) / 800)::int) cy
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
  UNION ALL
  SELECT b.cx, b.cy, b.box, false, a.g
  FROM boxes b JOIN _rm_paint_areas a ON a.g && b.box
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
  -- and painted areas, inside their outline: a line along one's edge runs on.
  UNION ALL
  SELECT floor(ST_X(ST_PointOnSurface(g)) / 400), floor(ST_Y(ST_PointOnSurface(g)) / 400), ST_Buffer(g, -0.5 * s)
  FROM _rm_paint_areas
) cuts
GROUP BY cx, cy;
-- And the inside of a junction: the hull of the crosswalks round a node some
-- road crosses, so lanes end at the crosswalks rather than run on between
-- them. A crosswalk counts within the junction's width and 10 m of its node.
-- Only the roads meeting there break for it, and those carrying on from them
-- (`ids`): a bridge over the junction or a road beside it runs on.
DROP TABLE IF EXISTS _rm_boxes;
CREATE TEMP TABLE _rm_boxes AS
SELECT ST_Intersection(h.hull, ST_Buffer(n.p, (n.width + 10) * n.s, 16)) as cut,
       ARRAY(SELECT i.osm_id FROM _rm_incid i WHERE i.x = n.x AND i.y = n.y AND NOT i.bridge
             UNION SELECT p.next_id FROM _rm_pairs p JOIN _rm_incid i ON i.osm_id = p.prev_id AND i.x = n.x AND i.y = n.y AND NOT i.bridge
             UNION SELECT p.prev_id FROM _rm_pairs p JOIN _rm_incid i ON i.osm_id = p.next_id AND i.x = n.x AND i.y = n.y AND NOT i.bridge) as ids
FROM _rm_nodes n
CROSS JOIN LATERAL (
  SELECT ST_ConvexHull(ST_Collect(n.p, ST_Collect(c.g))) as hull
  FROM _rm_crossings c
  WHERE c.g && ST_Expand(n.p, (n.width + 10) * n.s) AND ST_DWithin(c.g, n.p, (n.width + 10) * n.s)
) h
WHERE GeometryType(h.hull) = 'POLYGON' AND EXISTS (SELECT 1 FROM _rm_cross x WHERE x.x = n.x AND x.y = n.y);
CREATE INDEX ON _rm_cuts USING gist (cut);
CREATE INDEX ON _rm_boxes USING gist (cut);
-- Centre and lane lines never run on inside a painted area's outline.
DROP TABLE IF EXISTS _rm_paint_cuts;
CREATE TEMP TABLE _rm_paint_cuts AS SELECT ST_Buffer(g, -0.15 * s) as cut FROM _rm_paint_areas;
CREATE INDEX ON _rm_paint_cuts USING gist (cut);
-- Lane and centre lines stop short of a turning circle; edge lines run on round
-- its kerb, so theirs are cut only where the kerb line around it takes over.
DROP TABLE IF EXISTS _rm_turn_cuts;
CREATE TEMP TABLE _rm_turn_cuts AS
SELECT ids, ST_Buffer(p, paint_end, 16) as cut, false as edge FROM _rm_turns
UNION ALL
SELECT ids, ST_Buffer(p, reach - 0.5 * s, 32), true FROM _rm_turns WHERE edged;
CREATE INDEX ON _rm_turn_cuts USING gist (cut);

-- ─── Junction approaches ─────────────────────────────────────────────────────
-- At each end of a marked road that meets a junction: where its stop line
-- stands (`setback`), whether one is painted, and the lanes that arrive. Lane
-- lines end there too, wherever a road crosses.
DROP TABLE IF EXISTS _rm_ends;
CREATE TEMP TABLE _rm_ends AS
SELECT r.osm_id, r.g, r.s, r.americas, r.bridge, e.at_end, e.pt, n.x, n.y, n.top_rank, road_rank(r.class) as rank,
       r.split_b, r.split_f, r.left_edge, r.right_edge, r.lane_w, r.fwd, r.bwd, f.d as fork_d, f.ease as fork_ease,
       r.len_m, r.ease, r.pos, r.disp, r.res_pos, r.res_disp, r.res_at, r.res_len,
       CASE WHEN e.at_end THEN r.turn_forward ELSE r.turn_backward END as turns,
       -- Back from the junction centre to the stop line: clear of the widest
       -- road crossing here, and of any crosswalk across the approach, all
       -- the way across the lanes arriving where the crosswalk runs askew.
       greatest((COALESCE(a.cross_w, 0) / 2 / greatest(COALESCE(a.sine, 1), 0.5) + 1.5) * r.s, COALESCE((
         SELECT max(abs(CASE WHEN e.at_end THEN 1 - ST_LineLocatePoint(r.g, dp.geom)
                                              ELSE ST_LineLocatePoint(r.g, dp.geom) END) * ST_Length(r.g))
         FROM _rm_crossings c
         CROSS JOIN LATERAL ST_DumpPoints(ST_Collect(ST_ClosestPoint(c.g, e.pt),
                                                     ST_Intersection(c.g, road_strip(r.g, CASE WHEN e.at_end THEN r.right_edge ELSE r.split_b END * r.s,
                                                                                     CASE WHEN e.at_end THEN r.split_f ELSE r.left_edge END * r.s)))) dp
         WHERE c.g && ST_Expand(e.pt, 30 * r.s) AND ST_DWithin(c.g, r.g, 0.5 * r.s)
           AND ST_DWithin(c.g, e.pt, 30 * r.s) AND ST_DWithin(dp.geom, e.pt, (30 + r.width / 2) * r.s)
       ) + 2.5 * r.s, 0)) as setback,
       COALESCE(a.cross_w, 0) as cross_w
FROM _rm_roads r
CROSS JOIN LATERAL (VALUES (true, ST_EndPoint(r.g)), (false, ST_StartPoint(r.g))) e(at_end, pt)
CROSS JOIN LATERAL (SELECT CASE WHEN e.at_end THEN r.fork_e ELSE r.fork_s END as d,
                           CASE WHEN e.at_end THEN r.ease_e ELSE r.ease END as ease) f
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
-- A give way sign binds an approach as a stop sign does; one mapped further
-- along a road stands where its line is (see Yield lines).
ALTER TABLE _rm_ends ADD COLUMN yields boolean;
UPDATE _rm_ends e SET yields = NOT e.controlled AND (
  EXISTS (SELECT 1 FROM _rm_signals c WHERE c.kind = 'give_way' AND ST_DWithin(c.g, e.pt, 0.5 * e.s) AND e.rank < e.top_rank)
  OR EXISTS (SELECT 1 FROM _rm_signals c WHERE c.kind = 'give_way' AND ST_DWithin(c.g, e.pt, 30 * e.s)
             AND NOT ST_DWithin(c.g, e.pt, 0.5 * e.s) AND ST_DWithin(c.g, e.g, 0.5 * e.s)
             AND COALESCE(c.direction, '') <> CASE WHEN e.at_end THEN 'backward' ELSE 'forward' END));

-- The stretch of each approach between its stop line and the junction.
DROP TABLE IF EXISTS _rm_approach_cuts;
CREATE TEMP TABLE _rm_approach_cuts AS
SELECT e.osm_id, ST_Buffer(ST_LineSubstring(e.g, CASE WHEN e.at_end THEN 1 - least(1, e.setback / ST_Length(e.g)) ELSE 0 END,
                                                  CASE WHEN e.at_end THEN 1 ELSE least(1, e.setback / ST_Length(e.g)) END),
                           (r.width / 2 + abs(r.kshift) + greatest(abs(r.fork_s), abs(r.fork_e)) + 2) * r.s, 'endcap=flat') as cut
FROM _rm_ends e JOIN _rm_roads r ON r.osm_id = e.osm_id
WHERE e.cross_w > 0;
CREATE INDEX ON _rm_approach_cuts (osm_id);

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
-- change:lanes says where a lane may not be left (see road_lane_divider).
WITH lanes AS (
  SELECT *, CASE WHEN class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') THEN 'dashed_long' ELSE 'dashed' END as dash,
         road_twltl(tags, oneway, both_ways) as twltl,
         road_change(COALESCE(tags->>'change:lanes:forward', CASE WHEN oneway THEN tags->>'change:lanes' END),
                     COALESCE(tags->>'change:forward', tags->>'change'), fwd) as change_f,
         road_change(tags->>'change:lanes:backward', COALESCE(tags->>'change:backward', tags->>'change'), bwd) as change_b
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
-- from the road before, `end_m` where it ends when it eases into the one after
-- (see Tapers). `part` keeps a line to its taper at the 'head' or 'tail' only,
-- or to the way before's running on ('res', moved by `res_m` rather than as
-- the line it starts from), and `closes` ends a lane's divider where its lane
-- has closed.
SELECT osm_id, g, s, bridge, ease, 'centre' as kind, centre_pattern as pattern, centre_color as color,
       split_b as offset_m, prev_split_b as start_m, nxt_split_b as end_m, 'whole' as part, false as closes, NULL::float8 as res_m
FROM centre WHERE NOT oneway AND fwd > 0 AND bwd > 0 AND NOT twltl
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'centre', 'double', centre_color, split_f, prev_split_f, nxt_split_f, 'whole', false, NULL
FROM centre WHERE NOT oneway AND both_ways > 0 AND NOT twltl
UNION ALL
-- Where a centre turn lane ends, the line along its far side closes in on the
-- centre line over the taper rather than stopping short.
SELECT osm_id, g, s, bridge, ease, 'centre', 'double', centre_color, split_f, v.start, v.fin, v.part, false, NULL
FROM centre, LATERAL (VALUES (prev_split_f, NULL::float8, 'head', abs(prev_split_f - prev_split_b)),
                             (NULL, nxt_split_f, 'tail', abs(nxt_split_f - nxt_split_b))) v(start, fin, part, apart)
WHERE NOT oneway AND fwd > 0 AND bwd > 0 AND both_ways = 0 AND v.apart > 0.2
UNION ALL
-- and on into the next way where the taper does.
SELECT osm_id, g, s, bridge, ease, 'centre', 'double', centre_color, split_b, prev_split_b, nxt_split_b, 'res', false, res_close
FROM centre WHERE res_close IS NOT NULL AND NOT oneway AND fwd > 0 AND bwd > 0 AND NOT twltl
UNION ALL
-- A two-way left-turn lane is edged on each side by a solid line toward the
-- through lanes and a dashed one toward the turn lane.
SELECT osm_id, g, s, bridge, ease, 'centre', v.pattern, centre_color, v.o, v.start, v.fin, 'whole', false, NULL
FROM centre, LATERAL (VALUES ('solid', split_b + 0.14, prev_split_b + 0.14, nxt_split_b + 0.14), (dash, split_b - 0.14, prev_split_b - 0.14, nxt_split_b - 0.14),
                             ('solid', split_f - 0.14, prev_split_f - 0.14, nxt_split_f - 0.14), (dash, split_f + 0.14, prev_split_f + 0.14, nxt_split_f + 0.14)) v(pattern, o, start, fin)
WHERE twltl
UNION ALL
-- Lane dividers within each direction, easing in from where they stood on the
-- road before (see road_divider_start), and out to where they stand on the
-- one after. A divider with no counterpart there emerges from, or closes
-- into, the line it is clamped to.
SELECT osm_id, g, s, bridge, ease, 'lane', v.pattern, 'white', split_f - i * lane_w + v.d,
       CASE WHEN prev_left IS NOT NULL THEN road_divider_start(i, fwd, prev_split_f, prev_right, prev_lane_w, opens_kerb_f) + v.d END,
       e.fin + v.d, 'whole', COALESCE(abs(e.fin - nxt_split_f) < 0.05 OR abs(e.fin - nxt_right) < 0.05, false), NULL
FROM lanes, generate_series(1, least(fwd, 9) - 1) i, LATERAL road_lane_divider(change_f, i, dash, 1) v,
     LATERAL (SELECT road_divider_start(i, fwd, nxt_split_f, nxt_right, nxt_lane_w, opens_kerb_f) as fin) e
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'lane', v.pattern, 'white', split_b + i * lane_w + v.d,
       CASE WHEN prev_left IS NOT NULL THEN road_divider_start(i, bwd, prev_split_b, prev_left, prev_lane_w, opens_kerb_b) + v.d END,
       e.fin + v.d, 'whole', COALESCE(abs(e.fin - nxt_split_b) < 0.05 OR abs(e.fin - nxt_left) < 0.05, false), NULL
FROM lanes, generate_series(1, least(bwd, 9) - 1) i, LATERAL road_lane_divider(change_b, i, dash, -1) v,
     LATERAL (SELECT road_divider_start(i, bwd, nxt_split_b, nxt_left, nxt_lane_w, opens_kerb_b) as fin) e
UNION ALL
-- Edge lines on roads without kerbs: motorways, trunks and their links.
SELECT osm_id, g, s, bridge, ease, 'edge', 'solid', 'white', right_edge + 0.3, prev_right + 0.3, nxt_right + 0.3, 'whole', false, NULL
FROM lanes WHERE class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'edge', 'solid', CASE WHEN americas AND oneway THEN 'yellow' ELSE 'white' END, left_edge - 0.3, prev_left - 0.3,
       nxt_left - 0.3, 'whole', false, NULL
FROM lanes WHERE class IN ('motorway', 'motorway_link', 'trunk', 'trunk_link')
UNION ALL
-- Bike lanes: a solid line on the traffic side of the paint, another past any
-- buffer, and one against parking on the kerb side, easing in and out as
-- road_side_start says.
SELECT osm_id, g, s, bridge, ease, 'bike', 'solid', 'white', o, road_side_start(o, kerb_r, right_edge, prev_kerb_r, prev_right, track_r),
       road_side_start(o, kerb_r, right_edge, nxt_kerb_r, nxt_right, track_r), 'whole', false, NULL
FROM _rm_roads, LATERAL (VALUES (br_b), (CASE WHEN buf_r > 0 THEN br_b + buf_r END),
                                (CASE WHEN park_r > 0 AND NOT track_r THEN br_a END)) v(o)
WHERE bike_r > 0 AND o IS NOT NULL
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'bike', 'solid', 'white', o, road_side_start(o, kerb_l, left_edge, prev_kerb_l, prev_left, track_l),
       road_side_start(o, kerb_l, left_edge, nxt_kerb_l, nxt_left, track_l), 'whole', false, NULL
FROM _rm_roads, LATERAL (VALUES (bl_b), (CASE WHEN buf_l > 0 THEN bl_b - buf_l END),
                                (CASE WHEN park_l > 0 AND NOT track_l THEN bl_a END)) v(o)
WHERE bike_l > 0 AND o IS NOT NULL;

-- Paint along the kerb sides drawn as bars across a band, `hatch` either side
-- of its middle, one every `step`, leaning `lean` (see road_hatch): a bike
-- lane's painted buffer where it is wide enough to take diagonal bars, and the
-- stalls of a parking lane (see road_parking_marks), angled so a car pulls in
-- forward, with a line along the traffic side of marked parallel ones where no
-- bike lane already draws it. `side` is +1 for the right kerb, -1 the left.
DROP TABLE IF EXISTS _rm_side_lines;
CREATE TEMP TABLE _rm_side_lines AS
SELECT osm_id, g, s, bridge, ease, 'hatch' as kind, 'solid' as pattern, 'white' as color,
       br_b + buf_r / 2 as offset_m, road_side_start(br_b + buf_r / 2, kerb_r, right_edge, prev_kerb_r, prev_right, track_r) as start_m,
       road_side_start(br_b + buf_r / 2, kerb_r, right_edge, nxt_kerb_r, nxt_right, track_r) as end_m, 'whole' as part, false as closes, NULL::float8 as res_m,
       buf_r / 2 - 0.15 as hatch, 3.5::float8 as step, 1::float8 as lean
FROM _rm_roads WHERE bike_r > 0 AND buf_r >= 0.6
UNION ALL
SELECT osm_id, g, s, bridge, ease, 'hatch', 'solid', 'white', bl_b - buf_l / 2,
       road_side_start(bl_b - buf_l / 2, kerb_l, left_edge, prev_kerb_l, prev_left, track_l),
       road_side_start(bl_b - buf_l / 2, kerb_l, left_edge, nxt_kerb_l, nxt_left, track_l), 'whole', false, NULL, buf_l / 2 - 0.15, 3.5, 1
FROM _rm_roads WHERE bike_l > 0 AND buf_l >= 0.6
UNION ALL
SELECT r.osm_id, r.g, r.s, r.bridge, r.ease, 'parking', 'solid', 'white', o.mid + v.at,
       road_side_start(o.mid + v.at, o.kerb, o.edge, o.prev_kerb, o.prev_edge, true),
       e.fin, 'whole', abs(e.fin - (o.mid + v.at)) > 0.2, NULL, v.half, m.marks[1], m.marks[2] * CASE WHEN o.side = -1 AND r.oneway THEN 1 ELSE -1 END
FROM _rm_roads r
CROSS JOIN LATERAL (VALUES
  (1, r.park_r, r.kerb_r + CASE WHEN r.track_r THEN r.bike_r ELSE 0 END + r.park_r / 2, r.bike_r > 0 AND NOT r.track_r,
   CASE WHEN r.flip THEN 'left' ELSE 'right' END, r.kerb_r, r.right_edge, r.prev_kerb_r, r.prev_right, r.nxt_kerb_r, r.nxt_right),
  (-1, r.park_l, r.kerb_l - CASE WHEN r.track_l THEN r.bike_l ELSE 0 END - r.park_l / 2, r.bike_l > 0 AND NOT r.track_l,
   CASE WHEN r.flip THEN 'right' ELSE 'left' END, r.kerb_l, r.left_edge, r.prev_kerb_l, r.prev_left, r.nxt_kerb_l, r.nxt_left)
) o(side, park, mid, edged, osm_side, kerb, edge, prev_kerb, prev_edge, nxt_kerb, nxt_edge)
CROSS JOIN LATERAL (SELECT road_parking_marks(r.tags, o.osm_side) as marks) m
-- The stalls, or a metre-long tick off the traffic side of each, and the line
-- along a marked parallel lane.
CROSS JOIN LATERAL (VALUES (CASE WHEN m.marks[3] = 0 THEN 0.0 END, o.park / 2),
                           (CASE WHEN m.marks[3] = 1 THEN o.side * (o.park / 2 - 0.5) END, 0.5),
                           (CASE WHEN m.marks[3] = 0 AND m.marks[2] = 0 AND m.marks[1] > 5 AND NOT o.edged THEN o.side * o.park / 2 END, NULL)) v(at, half)
CROSS JOIN LATERAL (SELECT road_side_start(o.mid + v.at, o.kerb, o.edge, o.nxt_kerb, o.nxt_edge, true) as fin) e
WHERE o.park > 0 AND m.marks IS NOT NULL AND v.at IS NOT NULL;

-- Lane lines stop where another road's carriageway crosses, and at crosswalks.
-- A divider of a lane that opens starts where its taper ends, and so do the
-- stalls of a parking lane that moves over. Dashes are cut
-- here rather than by the client, a whole number to each way, so they run on
-- across a join without starting over.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT l.kind, l.pattern, l.color, l.bridge, ST_Transform(h.paint, 4326)
FROM (SELECT *, NULL::float8 as hatch, NULL::float8 as step, NULL::float8 as lean FROM _rm_lines UNION ALL SELECT * FROM _rm_side_lines) l
JOIN _rm_roads r ON r.osm_id = l.osm_id
CROSS JOIN LATERAL (SELECT COALESCE(l.start_m, l.offset_m) as at_prev) ap
CROSS JOIN LATERAL (SELECT road_profile(ST_Simplify(l.g, 0.05 * l.s), l.s, r.len_m, l.offset_m, l.start_m, l.ease,
                                        COALESCE(l.res_m, road_field(r.res_pos, r.res_disp, ap.at_prev)), r.res_at, r.res_len,
                                        CASE WHEN r.nxt_left IS NOT NULL THEN COALESCE(l.end_m - l.offset_m, 0) ELSE r.fork_e END, r.ease_e,
                                        r.dir_s, r.dir_e) as line,
                           CASE WHEN l.part = 'tail' THEN greatest(0, 1 - r.ease_e / r.len_m) ELSE least(1, greatest(0,
                             CASE WHEN l.kind = 'lane' AND EXISTS (SELECT 1 FROM unnest(r.emerge) e WHERE abs(e - l.offset_m) < 0.05) THEN r.ease END,
                             CASE WHEN l.kind = 'lane' AND EXISTS (SELECT 1 FROM unnest(r.res_emerge) e WHERE abs(e - ap.at_prev) < 0.05)
                                  THEN r.res_len - r.res_at END,
                             CASE WHEN l.kind = 'parking' AND abs(l.start_m - l.offset_m) > 0.2 THEN r.ease END) / nullif(r.len_m, 0)) END as from_f,
                           CASE WHEN l.part = 'head' THEN least(1, r.ease / r.len_m)
                                WHEN l.part = 'res' THEN least(1, (r.res_len - r.res_at) / r.len_m)
                                WHEN l.closes AND r.nxt_left IS NOT NULL THEN greatest(0, 1 - r.ease_e / r.len_m) ELSE 1 END as to_f) o
CROSS JOIN LATERAL (SELECT CASE
  WHEN l.pattern = 'dashed' THEN road_dashes(o.line, r.len_m, 9, 3, o.from_f, o.to_f)
  WHEN l.pattern = 'dashed_long' THEN road_dashes(o.line, r.len_m, 12, 3, o.from_f, o.to_f)
  WHEN o.from_f < o.to_f AND (o.from_f > 0 OR o.to_f < 1) THEN ST_LineSubstring(o.line, o.from_f, o.to_f)
  WHEN o.from_f < o.to_f THEN o.line END as paint) d
CROSS JOIN LATERAL (
  SELECT ST_LineMerge(ST_CollectionExtract(COALESCE(ST_Difference(d.paint, ST_Union(cut)), d.paint), 2)) as merged, ST_Union(cut) as cuts
  FROM (SELECT k.cut FROM _rm_cuts k WHERE k.cut && o.line
        UNION ALL SELECT k.cut FROM _rm_boxes k WHERE k.cut && o.line AND l.osm_id = ANY (k.ids)
        UNION ALL SELECT a.cut FROM _rm_approach_cuts a WHERE a.osm_id = l.osm_id
        UNION ALL SELECT rc.cut FROM _rm_road_cuts rc WHERE rc.osm_id = l.osm_id
        UNION ALL SELECT tc.cut FROM _rm_turn_cuts tc WHERE l.osm_id = ANY (tc.ids) AND tc.cut && o.line AND tc.edge = (l.kind = 'edge')
        UNION ALL SELECT pc.cut FROM _rm_paint_cuts pc WHERE pc.cut && o.line AND l.kind IN ('centre', 'lane')) cuts
) m
-- A dash a cut leaves a stub of is left out.
CROSS JOIN LATERAL (SELECT CASE WHEN l.pattern LIKE 'dashed%'
  THEN (SELECT ST_Collect(p.geom) FROM ST_Dump(m.merged) p WHERE ST_Length(p.geom) > 1 * l.s) ELSE m.merged END as clipped) c
-- Bars are spaced along the whole line, which every box draws alike, and kept
-- where the line is not cut.
CROSS JOIN LATERAL (SELECT CASE WHEN l.hatch IS NULL THEN c.clipped
  ELSE (SELECT ST_CollectionExtract(ST_Collect(b.geom), 2) FROM ST_Dump(road_hatch(o.line, l.hatch * l.s, l.step * l.s, l.lean)) b
        WHERE (m.cuts IS NULL OR NOT ST_Intersects(ST_LineInterpolatePoint(b.geom, 0.5), m.cuts))
          AND ST_LineLocatePoint(o.line, ST_LineInterpolatePoint(b.geom, 0.5)) BETWEEN o.from_f AND o.to_f) END as paint) h
WHERE d.paint IS NOT NULL AND c.clipped IS NOT NULL AND NOT ST_IsEmpty(c.clipped)
  AND ST_Length(c.clipped) > CASE WHEN l.pattern LIKE 'dashed%' THEN 0.5 ELSE 2 END * l.s
  AND h.paint IS NOT NULL AND NOT ST_IsEmpty(h.paint);

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
         road_side_start(br_a, kerb_r, right_edge, prev_kerb_r, prev_right, track_r) - br_a as d, ease,
         road_side_start(br_b, kerb_r, right_edge, prev_kerb_r, prev_right, track_r) - br_b as d2,
         road_side_start(br_a, kerb_r, right_edge, nxt_kerb_r, nxt_right, track_r) - br_a as e,
         road_side_start(br_b, kerb_r, right_edge, nxt_kerb_r, nxt_right, track_r) - br_b as e2 FROM _rm_roads WHERE bike_r > 0
  UNION ALL
  SELECT osm_id, g, s, bridge, 'bike_lane', 'green', bl_a, bl_b, road_side_start(bl_a, kerb_l, left_edge, prev_kerb_l, prev_left, track_l) - bl_a,
         ease, road_side_start(bl_b, kerb_l, left_edge, prev_kerb_l, prev_left, track_l) - bl_b,
         road_side_start(bl_a, kerb_l, left_edge, nxt_kerb_l, nxt_left, track_l) - bl_a,
         road_side_start(bl_b, kerb_l, left_edge, nxt_kerb_l, nxt_left, track_l) - bl_b FROM _rm_roads WHERE bike_l > 0
  UNION ALL
  -- A bus lane eases in and out with the dividers either side of it.
  SELECT osm_id, g, s, bridge, 'bus_lane', 'red', split_f - (i - 1) * lane_w, split_f - i * lane_w,
         road_divider_start(i - 1, fwd, prev_split_f, prev_right, prev_lane_w, opens_kerb_f) - (split_f - (i - 1) * lane_w),
         ease, road_divider_start(i, fwd, prev_split_f, prev_right, prev_lane_w, opens_kerb_f) - (split_f - i * lane_w),
         road_divider_start(i - 1, fwd, nxt_split_f, nxt_right, nxt_lane_w, opens_kerb_f) - (split_f - (i - 1) * lane_w),
         road_divider_start(i, fwd, nxt_split_f, nxt_right, nxt_lane_w, opens_kerb_f) - (split_f - i * lane_w)
  FROM _rm_roads, unnest(bus_f) i
  UNION ALL
  SELECT osm_id, g, s, bridge, 'bus_lane', 'red', split_b + (i - 1) * lane_w, split_b + i * lane_w,
         road_divider_start(i - 1, bwd, prev_split_b, prev_left, prev_lane_w, opens_kerb_b) - (split_b + (i - 1) * lane_w),
         ease, road_divider_start(i, bwd, prev_split_b, prev_left, prev_lane_w, opens_kerb_b) - (split_b + i * lane_w),
         road_divider_start(i - 1, bwd, nxt_split_b, nxt_left, nxt_lane_w, opens_kerb_b) - (split_b + (i - 1) * lane_w),
         road_divider_start(i, bwd, nxt_split_b, nxt_left, nxt_lane_w, opens_kerb_b) - (split_b + i * lane_w)
  FROM _rm_roads, unnest(bus_b) i
) b
JOIN _rm_roads r ON r.osm_id = b.osm_id
CROSS JOIN LATERAL (SELECT b.a + COALESCE(b.d, 0) as a0, b.b + COALESCE(b.d2, b.d, 0) as b0) st
CROSS JOIN LATERAL (SELECT COALESCE(CASE
  WHEN abs(COALESCE(b.d, 0)) > 0.05 OR abs(COALESCE(b.d2, 0)) > 0.05 OR r.ease_e IS NOT NULL OR r.res_len IS NOT NULL
    THEN road_profile_band(ST_Simplify(b.g, 0.05 * b.s), b.s, r.len_m, b.a, st.a0, b.b, st.b0, b.ease,
                           road_field(r.res_pos, r.res_disp, st.a0), road_field(r.res_pos, r.res_disp, st.b0),
                           r.res_at, r.res_len, COALESCE(b.e, r.fork_e), COALESCE(b.e2, b.e, r.fork_e), r.ease_e, r.dir_s, r.dir_e) END,
  road_strip(ST_Simplify(b.g, 0.05 * b.s), b.a * b.s, b.b * b.s)) as band) o
CROSS JOIN LATERAL (
  SELECT ST_CollectionExtract(COALESCE(ST_Difference(o.band, ST_Union(cut)), o.band), 3) as cut_band
  FROM (SELECT k.cut FROM _rm_cuts k WHERE k.cut && o.band
        UNION ALL SELECT k.cut FROM _rm_boxes k WHERE k.cut && o.band AND b.osm_id = ANY (k.ids)
        UNION ALL SELECT a.cut FROM _rm_approach_cuts a WHERE a.osm_id = b.osm_id
        UNION ALL SELECT rc.cut FROM _rm_road_cuts rc WHERE rc.osm_id = b.osm_id
        UNION ALL SELECT tc.cut FROM _rm_turn_cuts tc WHERE b.osm_id = ANY (tc.ids) AND tc.cut && o.band AND NOT tc.edge) cuts
) k
-- A scrap under 5 m of lane long, cut off between a crosswalk and the edge
-- of a junction, is left out.
CROSS JOIN LATERAL (SELECT ST_Collect(p.geom) as clipped FROM ST_Dump(k.cut_band) p
                    WHERE ST_Area(p.geom) > greatest(4, CASE WHEN ST_NumGeometries(k.cut_band) > 1 THEN 5 * abs(b.b - b.a) END) * b.s * b.s) c
WHERE o.band IS NOT NULL AND c.clipped IS NOT NULL;

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
    -- A junction some road crosses, not a fork where two simply merge.
    AND EXISTS (SELECT 1 FROM _rm_cross c WHERE c.osm_id = r.osm_id AND c.x = n.x AND c.y = n.y)
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
-- A frame at a point `back` units from a road end: the point, and the unit
-- vectors along the arriving traffic and to its left.
DROP TABLE IF EXISTS _rm_frames;
CREATE TEMP TABLE _rm_frames AS
SELECT e.*, f.back, p,
       CASE WHEN e.at_end THEN e.len_m - f.back / e.s ELSE f.back / e.s END as x_m,
       -- How far a fork at this end still holds the lanes off the way here.
       CASE WHEN abs(e.fork_d) > 0.05 AND e.fork_ease > 0
            THEN e.fork_d * (1 - t * t * (3 - 2 * t)) ELSE 0 END as shift_here,
       (ST_X(ahead) - ST_X(p)) / nullif(ST_Distance(ahead, p), 0) as ux,
       (ST_Y(ahead) - ST_Y(p)) / nullif(ST_Distance(ahead, p), 0) as uy
FROM _rm_ends e
CROSS JOIN LATERAL (VALUES (e.setback), (e.setback + 6 * e.s), (e.setback + 36 * e.s)) f(back)
CROSS JOIN LATERAL (SELECT least(1, f.back / e.s / nullif(e.fork_ease, 0)) as t) ft
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
  CASE WHEN at_end THEN split_f + shift_here + road_moved(pos, disp, ease, res_pos, res_disp, res_at, res_len, split_f, x_m)
       ELSE -(split_b + shift_here + road_moved(pos, disp, ease, res_pos, res_disp, res_at, res_len, split_b, x_m)) END as a,
  CASE WHEN at_end THEN right_edge + shift_here + road_moved(pos, disp, ease, res_pos, res_disp, res_at, res_len, right_edge, x_m)
       ELSE -(left_edge + shift_here + road_moved(pos, disp, ease, res_pos, res_disp, res_at, res_len, left_edge, x_m)) END as b) o
WHERE controlled AND back = setback AND ux IS NOT NULL
  AND ((at_end AND fwd > 0) OR (NOT at_end AND bwd > 0));

-- ─── Yield lines ─────────────────────────────────────────────────────────────
-- A row of white triangles across the arriving lanes, each 0.45 m at the base
-- and 0.68 m long, pointing back at the traffic: at the stop line's place on
-- an approach a give way sign binds, and where one is mapped further along a
-- road than that, across the lanes running the way it faces (as tagged, else
-- with a one-way, else toward the nearer end of the way).
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'yield', 'fill', 'white', r.bridge, ST_Transform(ST_Multi(ST_Collect(ST_SetSRID(ST_MakePolygon(ST_MakeLine(ARRAY[
         ST_MakePoint(ST_X(d.at) - d.u[2] * t.o0 * r.s, ST_Y(d.at) + d.u[1] * t.o0 * r.s),
         ST_MakePoint(ST_X(d.at) - d.u[2] * t.o1 * r.s, ST_Y(d.at) + d.u[1] * t.o1 * r.s),
         ST_MakePoint(ST_X(d.at) - (d.u[2] * (t.o0 + t.o1) / 2 + d.u[1] * 0.68 * o.dir) * r.s,
                      ST_Y(d.at) + (d.u[1] * (t.o0 + t.o1) / 2 - d.u[2] * 0.68 * o.dir) * r.s),
         ST_MakePoint(ST_X(d.at) - d.u[2] * t.o0 * r.s, ST_Y(d.at) + d.u[1] * t.o0 * r.s)])), 3857))), 4326)
FROM _rm_signals c
CROSS JOIN LATERAL (
  SELECT r.* FROM _rm_roads r WHERE r.marked AND r.g && ST_Expand(c.g, 2) ORDER BY r.g <-> c.g LIMIT 1
) r
CROSS JOIN LATERAL (SELECT ST_LineLocatePoint(r.g, c.g) as f) l
CROSS JOIN LATERAL (SELECT road_dir(r.g, l.f, 2 * r.s) as u, ST_ClosestPoint(r.g, c.g) as at) d
-- `dir` +1 for traffic along the way's line, -1 against it; then the lanes it arrives in.
CROSS JOIN LATERAL (SELECT CASE
  WHEN c.direction IN ('forward', 'backward') THEN CASE WHEN (c.direction = 'forward') <> r.flip THEN 1 ELSE -1 END
  WHEN r.oneway OR l.f >= 0.5 THEN 1 ELSE -1 END as dir) o0
CROSS JOIN LATERAL (SELECT o0.dir, CASE WHEN o0.dir = 1 THEN r.split_f ELSE r.split_b END as a,
                           CASE WHEN o0.dir = 1 THEN r.right_edge ELSE r.left_edge END as b) o
CROSS JOIN LATERAL (SELECT floor(abs(o.b - o.a) / 0.65)::int as n, sign(o.b - o.a) as side) k0
CROSS JOIN LATERAL generate_series(0, k0.n - 1) k
CROSS JOIN LATERAL (SELECT o.a + k0.side * ((abs(o.b - o.a) - k0.n * 0.65) / 2 + 0.1 + k * 0.65) as o0,
                           o.a + k0.side * ((abs(o.b - o.a) - k0.n * 0.65) / 2 + 0.55 + k * 0.65) as o1) t
WHERE c.kind = 'give_way' AND d.u IS NOT NULL AND k0.n > 0 AND ST_DWithin(r.g, c.g, 0.5 * r.s)
  AND ((o.dir = 1 AND r.fwd > 0) OR (o.dir = -1 AND r.bwd > 0))
  AND NOT EXISTS (SELECT 1 FROM _rm_nodes n WHERE n.x = round(ST_X(c.g)::numeric, 2) AND n.y = round(ST_Y(c.g)::numeric, 2))
  AND NOT EXISTS (SELECT 1 FROM _rm_ends e WHERE e.osm_id = r.osm_id AND e.yields AND ST_DWithin(c.g, e.pt, 30 * e.s)
                  AND ST_DWithin(c.g, e.g, 0.5 * e.s) AND COALESCE(c.direction, '') <> CASE WHEN e.at_end THEN 'backward' ELSE 'forward' END)
GROUP BY c.g, r.bridge;
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'yield', 'fill', 'white', f.bridge, ST_Transform(ST_Multi(ST_Collect(ST_SetSRID(ST_MakePolygon(ST_MakeLine(ARRAY[
         ST_MakePoint(ST_X(f.p) - f.uy * t.o0 * f.s, ST_Y(f.p) + f.ux * t.o0 * f.s),
         ST_MakePoint(ST_X(f.p) - f.uy * t.o1 * f.s, ST_Y(f.p) + f.ux * t.o1 * f.s),
         ST_MakePoint(ST_X(f.p) - (f.uy * (t.o0 + t.o1) / 2 + f.ux * 0.68) * f.s, ST_Y(f.p) + (f.ux * (t.o0 + t.o1) / 2 - f.uy * 0.68) * f.s),
         ST_MakePoint(ST_X(f.p) - f.uy * t.o0 * f.s, ST_Y(f.p) + f.ux * t.o0 * f.s)])), 3857))), 4326)
FROM _rm_frames f
CROSS JOIN LATERAL (SELECT
  CASE WHEN f.at_end THEN f.split_f + f.shift_here ELSE -(f.split_b + f.shift_here) END as a,
  CASE WHEN f.at_end THEN f.right_edge + f.shift_here ELSE -(f.left_edge + f.shift_here) END as b) o
CROSS JOIN LATERAL (SELECT floor(abs(o.b - o.a) / 0.65)::int as n, sign(o.b - o.a) as dir) c
CROSS JOIN LATERAL generate_series(0, c.n - 1) k
CROSS JOIN LATERAL (SELECT o.a + c.dir * ((abs(o.b - o.a) - c.n * 0.65) / 2 + 0.1 + k * 0.65) as o0,
                           o.a + c.dir * ((abs(o.b - o.a) - c.n * 0.65) / 2 + 0.55 + k * 0.65) as o1) t
WHERE f.yields AND f.back = f.setback AND f.ux IS NOT NULL AND c.n > 0
  AND ((f.at_end AND f.fwd > 0) OR (NOT f.at_end AND f.bwd > 0))
GROUP BY f.osm_id, f.at_end, f.x, f.y, f.bridge;

-- Turn arrows, one per lane, when turn:lanes names every lane that arrives.
DROP TABLE IF EXISTS road_glyphs_next;
CREATE TABLE road_glyphs_next (fid bigserial CONSTRAINT road_glyphs_next_pk PRIMARY KEY, glyph text, direction int, bridge boolean, geom geometry(Point, 4326));
INSERT INTO road_glyphs_next (glyph, direction, bridge, geom)
SELECT glyph, ((round(degrees(atan2(ux, uy)))::int % 360) + 360) % 360,
       bridge,
       ST_Transform(ST_SetSRID(ST_MakePoint(ST_X(p) - uy * lane_off * s, ST_Y(p) + ux * lane_off * s), 3857), 4326)
FROM (
  SELECT f.*, t.ord, t.value,
         -- Lanes are listed left to right as traffic sees them, from the split
         -- outwards, wherever the tapers have them by here.
         CASE WHEN f.at_end THEN c.mid + f.shift_here + road_moved(f.pos, f.disp, f.ease, f.res_pos, f.res_disp, f.res_at, f.res_len, c.mid, f.x_m)
              ELSE -(c.mid + f.shift_here + road_moved(f.pos, f.disp, f.ease, f.res_pos, f.res_disp, f.res_at, f.res_len, c.mid, f.x_m)) END as lane_off,
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
  CROSS JOIN LATERAL (SELECT CASE WHEN f.at_end THEN f.split_f - (t.ord - 0.5) * f.lane_w ELSE f.split_b + (t.ord - 0.5) * f.lane_w END as mid) c
  WHERE f.back > f.setback AND f.turns IS NOT NULL AND f.ux IS NOT NULL
    AND array_length(string_to_array(f.turns, '|'), 1) = CASE WHEN f.at_end THEN f.fwd ELSE f.bwd END
) a
WHERE glyph IS NOT NULL;

-- Down the middle of a two-way left-turn lane, pairs of left arrows back to
-- back, one for each direction, about every 90 m of way.
INSERT INTO road_glyphs_next (glyph, direction, bridge, geom)
SELECT 'road-arrow-left', ((round(degrees(atan2(d.u[1], d.u[2])))::int + CASE WHEN v.ahead > 0 THEN 0 ELSE 180 END) % 360 + 360) % 360,
       r.bridge,
       ST_Transform(ST_SetSRID(ST_MakePoint(ST_X(d.p) + (-d.u[2] * m.mid + d.u[1] * v.ahead) * r.s,
                                            ST_Y(d.p) + (d.u[1] * m.mid + d.u[2] * v.ahead) * r.s), 3857), 4326)
FROM _rm_roads r
CROSS JOIN LATERAL (SELECT ST_Length(r.g) / r.s as len, (r.split_b + r.split_f) / 2 as mid,
                           greatest(1, round(ST_Length(r.g) / r.s / 90))::int as n) m
CROSS JOIN LATERAL generate_series(1, m.n) k
CROSS JOIN LATERAL (SELECT ST_LineInterpolatePoint(r.g, (k - 0.5) / m.n) as p, road_dir(r.g, (k - 0.5) / m.n, 2 * r.s) as u) d
CROSS JOIN (VALUES (4.0), (-4.0)) v(ahead)
WHERE r.marked AND road_twltl(r.tags, r.oneway, r.both_ways) AND m.len >= 40 AND d.u IS NOT NULL;

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

-- ─── Painted areas: paint ────────────────────────────────────────────────────
-- Each painted area's outline, as its `stroke` says, and the bars across it.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
-- Unstroked, it is still written, in a pattern no layer draws, so the queue can
-- find where it was.
SELECT 'restriction', CASE WHEN a.stroke IN ('no', 'none') OR ST_IsEmpty(i.g) THEN 'none' WHEN a.stroke = 'double_solid' THEN 'double'
                           WHEN a.stroke = 'dashed' THEN 'dashed' ELSE 'solid' END, a.color, false,
       ST_Transform(ST_Boundary(CASE WHEN ST_IsEmpty(i.g) THEN a.g ELSE i.g END), 4326)
FROM _rm_paint_areas a
CROSS JOIN LATERAL (SELECT ST_Buffer(a.g, (CASE WHEN a.stroke = 'double_solid' THEN -0.3 ELSE -0.15 END) * a.s) as g) i;
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'hatch', 'solid', a.color, false, ST_Transform(h.bars, 4326)
FROM _rm_paint_areas a
CROSS JOIN LATERAL (SELECT road_area_hatch(a.g, a.pattern, a.step * a.s, a.ux, a.uy,
                                           (CASE WHEN a.stroke IN ('no', 'none') THEN 0.1 ELSE 0.45 END) * a.s) as bars) h
WHERE a.pattern IN ('stripes', 'crosshatch', 'chevron') AND h.bars IS NOT NULL AND NOT ST_IsEmpty(h.bars);

-- ─── Speed humps ─────────────────────────────────────────────────────────────
-- A hump, speed cushion or speed table on a road gets the MUTCD's chevron in
-- each lane: 1.8 m across at its base on the hump's near edge, its point at
-- the middle of the hump, pointing with the traffic.
INSERT INTO road_markings_next (kind, pattern, color, bridge, geom)
SELECT 'hump', 'fill', 'white', false, ST_Transform(ST_Multi(ST_Union(ST_Buffer(ST_SetSRID(ST_MakeLine(ARRAY[
         ST_MakePoint(ST_X(c.p) - c.tx * 1.8 * r.s - c.ty * c.w / 2 * r.s, ST_Y(c.p) - c.ty * 1.8 * r.s + c.tx * c.w / 2 * r.s),
         c.p,
         ST_MakePoint(ST_X(c.p) - c.tx * 1.8 * r.s + c.ty * c.w / 2 * r.s, ST_Y(c.p) - c.ty * 1.8 * r.s - c.tx * c.w / 2 * r.s)]), 3857),
       0.15 * r.s, 'endcap=flat join=mitre mitre_limit=3'))), 4326)
FROM (
  SELECT ST_Transform(geom, 3857) as p, osm_id
  FROM geo_places
  WHERE geom && (SELECT area FROM _rm_area) AND geom_type = 'point'
    AND (tags @> '{"traffic_calming": "hump"}' OR tags @> '{"traffic_calming": "table"}' OR tags @> '{"traffic_calming": "cushion"}')
) h
CROSS JOIN LATERAL (
  SELECT r.* FROM _rm_roads r WHERE r.g && ST_Expand(h.p, 2) ORDER BY r.g <-> h.p LIMIT 1
) r
CROSS JOIN LATERAL (SELECT road_dir(r.g, ST_LineLocatePoint(r.g, h.p), 2 * r.s) as u, ST_ClosestPoint(r.g, h.p) as at) d
-- Each lane's middle, and the way its traffic runs.
CROSS JOIN LATERAL (
  SELECT r.split_f - (i - 0.5) * r.lane_w as o, 1 as dir FROM generate_series(1, r.fwd) i
  UNION ALL
  SELECT r.split_b + (i - 0.5) * r.lane_w, -1 FROM generate_series(1, r.bwd) i
) l
CROSS JOIN LATERAL (SELECT ST_SetSRID(ST_MakePoint(ST_X(d.at) - d.u[2] * l.o * r.s, ST_Y(d.at) + d.u[1] * l.o * r.s), 0) as p,
                           d.u[1] * l.dir as tx, d.u[2] * l.dir as ty, least(1.8, 0.75 * r.lane_w) as w) c
WHERE NOT r.bridge AND d.u IS NOT NULL AND ST_DWithin(r.g, h.p, 0.5 * r.s)
  -- A raised crosswalk keeps its crosswalk.
  AND NOT EXISTS (SELECT 1 FROM _rm_crossings x WHERE x.g && ST_Expand(h.p, 4 * r.s) AND ST_DWithin(x.g, h.p, 4 * r.s) AND ST_DWithin(x.g, r.g, 0.5 * r.s))
GROUP BY h.osm_id;

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
  -- Edge lines whole, or broken into 0.6 m dashes or 0.3 m dots.
  SELECT ST_Buffer(CASE WHEN w.style IN ('lines', 'ladder') THEN e.line
                        ELSE ST_LineSubstring(e.line, least(1, k * e.period / e.len), least(1, (k * e.period + e.paint) / e.len)) END,
                   0.15 * w.s, 'endcap=flat')
  FROM (VALUES (1), (-1)) v(side)
  CROSS JOIN LATERAL (SELECT road_offset(w.seg, side * 1.5 * w.s) as line) o
  CROSS JOIN LATERAL (SELECT o.line, ST_Length(o.line) / w.s as len, CASE WHEN w.style = 'dots' THEN 0.6 ELSE 1.2 END as period,
                             CASE WHEN w.style = 'dots' THEN 0.3 ELSE 0.6 END as paint) e
  CROSS JOIN LATERAL generate_series(0, CASE WHEN w.style IN ('lines', 'ladder') THEN 0 ELSE floor(e.len / e.period)::int END) k
  WHERE w.style IN ('lines', 'ladder', 'dashes', 'dots') AND o.line IS NOT NULL AND e.len > 0
    AND (w.style IN ('lines', 'ladder') OR k * e.period + e.paint <= e.len)
) b
GROUP BY w.style, w.seg;

-- Crosswalks meeting at a skewed corner, or mapped twice, would stack their
-- bars: the smaller gives way to the larger (then the one further west, so
-- every box picks the same) wherever they overlap.
-- Indexed on their own: road_markings_next has no index until the swap.
DROP TABLE IF EXISTS _rm_walk_paint;
CREATE TEMP TABLE _rm_walk_paint AS
SELECT fid, geom, ST_Area(geom) as area FROM road_markings_next WHERE kind = 'crosswalk';
CREATE INDEX ON _rm_walk_paint USING gist (geom);
ANALYZE _rm_walk_paint;
UPDATE road_markings_next m SET geom = ST_Multi(ST_CollectionExtract(ST_Difference(m.geom, w.cover), 3))
FROM (
  SELECT m2.fid, ST_Union(ST_ConvexHull(o.geom)) as cover
  FROM _rm_walk_paint m2
  JOIN _rm_walk_paint o ON o.fid <> m2.fid AND o.geom && m2.geom AND ST_Intersects(o.geom, m2.geom)
   AND (o.area, -ST_XMin(o.geom), -ST_YMin(o.geom)) > (m2.area, -ST_XMin(m2.geom), -ST_YMin(m2.geom))
  GROUP BY m2.fid
) w
WHERE m.fid = w.fid;
DELETE FROM road_markings_next WHERE kind = 'crosswalk' AND ST_IsEmpty(geom);

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
