-- Martin function source for the v3 SEMANTIC transit segments (stage 6, phase B).
--
-- Reads transit_line_segments (written by segments/build.py --emit): per-line
-- features pre-cut by the pipeline into
--   steady      -> junction-to-junction, constant composition+slot; offset_px
--                  (screen px, constant along the feature),
--   transition  -> fixed ground length (~60 m) straddling a junction node;
--                  off_from_px / off_to_px in the feature's travel frame.
-- Unlike transit_lines_rt (v2, kept untouched), feature boundaries here are
-- SEMANTIC: they exist only where bundle membership or slot changes (v3
-- contract C1), so the client's line-progress interpolation between
-- off_from_px and off_to_px lands exactly on the junction transitions.
--
-- Clip fractions: MapLibre's line bucket only enables per-vertex line-progress
-- when a feature carries mapbox_clip_start / mapbox_clip_end. We compute them
-- per tile-clipped part as LOCAL fractions of the FEATURE (0..1 across the
-- whole segment feature) via ST_LineLocatePoint, so progress is continuous
-- across tile seams. Direction-normalise with ST_Reverse + LEAST/GREATEST:
-- ST_Intersection does not guarantee it preserves the parent line's direction,
-- and geometry + fractions must flip together (same proven machinery as
-- create-transit-lines-runtime.sql, repointed at segment features).
--
-- Legacy compat (v2 client / stock Mapbox degradation): every feature also
-- carries slot / line_count such that the v2 constant-offset formula
--   offset = (slot - (line_count - 1) / 2) * 4.4
-- reproduces offset_px on steady features and off_from_px on transitions
-- (the from-side offset, so a non-fork client draws transitions as a constant
-- continuation of the approaching steady ribbon). Stored transition slots can
-- carry an interior bundle's slot (see segments/emit.py), so for transitions
-- we derive a synthetic (slot, line_count) pair from off_from_px instead of
-- trusting the stored columns.
--
-- Zoom guard: transition features are ~60 m of ground — sub-pixel below z11 —
-- so the function emits them only at z >= 11 (steady only below). Tune here;
-- martin-config.yaml registers the source for z8-22 either way.
--
-- Apply: docker exec -i barrelman-db psql -U barrelman -d barrelman \
--          < import/create-transit-lines-rt2.sql
-- Registered as function source transit_lines_rt2 in martin-config.yaml
-- (docker restart barrelman-martin once after registering; SQL replacements
-- are picked up live afterwards).

CREATE OR REPLACE FUNCTION transit_lines_rt2(z integer, x integer, y integer)
RETURNS bytea AS $$
DECLARE
  mvt bytea;
  env  geometry := ST_TileEnvelope(z, x, y);                                  -- 3857
  -- Match ST_AsMVTGeom's 128/4096 render buffer so clipped parts (and their
  -- fractions) extend a hair past the tile edge — no hairline seams.
  bw   double precision := (128.0 / 4096.0) * (2 * 20037508.342789 / power(2, z));
  envb geometry := ST_Expand(env, bw);
  envb4326 geometry := ST_Transform(envb, 4326);   -- segment geom is 4326
BEGIN
  SELECT INTO mvt ST_AsMVT(tile, 'transit_lines', 4096, 'geom', 'id') FROM (
    SELECT s.id,               -- MVT feature id: stable across tiles/build_keys
           s.kind, s.feed_id, s.route_ids, s.route_short_names,
           s.route_type, s.route_color, s.route_text_color, s.color_key,
           lg.slot, lg.line_count,
           s.offset_px, s.off_from_px, s.off_to_px,
           -- Fraction of the FULL segment feature at this clipped part's
           -- endpoints -> line-progress continuous across tile seams. Emit the
           -- geometry in the direction of increasing fraction (the bucket
           -- walks the emitted vertex order): reverse geometry + fractions
           -- together when ST_Intersection flipped the part.
           LEAST(fr.f0, fr.f1)    AS mapbox_clip_start,
           GREATEST(fr.f0, fr.f1) AS mapbox_clip_end,
           ST_AsMVTGeom(CASE WHEN fr.f0 <= fr.f1 THEN part.g ELSE ST_Reverse(part.g) END,
                        env, 4096, 128, true) AS geom
    FROM transit_line_segments s
    CROSS JOIN LATERAL (SELECT ST_Transform(s.geom, 3857) AS geom3857) m
    CROSS JOIN LATERAL (
      SELECT (ST_Dump(ST_Intersection(m.geom3857, envb))).geom AS g
    ) part
    CROSS JOIN LATERAL (
      SELECT ST_LineLocatePoint(m.geom3857, ST_StartPoint(part.g)) AS f0,
             ST_LineLocatePoint(m.geom3857, ST_EndPoint(part.g))   AS f1
    ) fr
    CROSS JOIN LATERAL (
      -- Legacy slot / line_count (see header). k = from-side offset in
      -- half-gap (2.2 px) units; h = (line_count-1)/2 picked with the parity
      -- that makes slot integral and >= 0.
      SELECT CASE WHEN s.kind = 'steady' THEN s.slot ELSE lc.slot END AS slot,
             CASE WHEN s.kind = 'steady' THEN s.line_count ELSE lc.line_count END AS line_count
      FROM (
        SELECT (k.k / 2.0 + h.h)::int AS slot, (2 * h.h + 1)::int AS line_count
        FROM (SELECT round(COALESCE(s.off_from_px, 0) / 2.2)::int AS k) k
        CROSS JOIN LATERAL (
          SELECT GREATEST((s.line_count - 1) / 2.0, -k.k / 2.0, 0) AS h0
        ) h0
        CROSS JOIN LATERAL (
          SELECT CASE WHEN k.k % 2 = 0 THEN ceil(h0.h0)
                      ELSE ceil(h0.h0 - 0.5) + 0.5 END AS h
        ) h
      ) lc
    ) lg
    WHERE s.geom && envb4326
      AND (s.kind = 'steady' OR z >= 11)   -- zoom guard, see header
      AND ST_GeometryType(part.g) = 'ST_LineString'
      AND ST_NPoints(part.g) >= 2
  ) AS tile
  WHERE geom IS NOT NULL;
  RETURN mvt;
END
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
