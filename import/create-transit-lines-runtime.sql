-- Runtime-offset transit line ribbons (MapLibre variable line-offset).
--
-- Unlike create-transit-lines-offset-zoom.sql (which BAKES a parallel offset into
-- the geometry, 9 per-zoom copies per ribbon), this serves each bundle's SHARED
-- CENTRELINE ONCE and lets the renderer offset per-vertex at draw time:
--
--   * Constant offset  ->  line-offset = (slot - (line_count-1)/2) * SPACING_px.
--       line-offset is in SCREEN PIXELS, so the on-screen gap is constant at every
--       zoom automatically — no per-zoom baking. Works on Mapbox AND MapLibre.
--   * Junction taper   ->  multiply that by a `line-progress` ramp that goes 0 at
--       each run's ends and 1 in the middle. Needs MapLibre + the variable
--       line-offset fork (line-offset accepting a line-progress expression).
--       Ribbons then converge to the centreline at junctions and fan back out
--       (Apple/Transit look). Mapbox ignores this (constant offset only).
--
-- WHY per-run tapering lands the junctions for free: the bundle is grouped by
-- (build_key, color_key, slot, line_count). A merged centreline therefore spans a
-- MAXIMAL run over which the bundle composition is constant; its endpoints are
-- exactly the nodes where a route joins/leaves (line_count/slot change). Taper at
-- progress 0/1 == taper at those junctions.
--
-- line-progress on VECTOR tiles: MapLibre's line bucket only enables per-vertex
-- progress when a feature carries `mapbox_clip_start`/`mapbox_clip_end` (geojson
-- gets these free from lineMetrics:true; MVT does not). We compute them per
-- tile-clipped segment as the segment's fraction of the FULL run via
-- ST_LineLocatePoint, so progress is continuous across tile seams (a naive 0/1
-- per tile would restart the ramp every tile).
--
-- Run after import/load-transit-graph.ts. Registered as the Martin function
-- source transit_lines_rt(z,x,y) in martin-config.yaml.

DROP MATERIALIZED VIEW IF EXISTS transit_lines_centerline CASCADE;
CREATE MATERIALIZED VIEW transit_lines_centerline AS
WITH params AS (
  SELECT 30.0::double precision AS seg   -- densify step (EPSG:3857 metres) so the
                                         -- per-vertex offset + taper have vertices
),
edge_colors AS (
  -- Colour-merge routes sharing a trunk colour on an edge (Apple-style), same as
  -- the baked view so bundling is identical.
  SELECT edge_id,
    COALESCE(NULLIF(route_color, ''), 'rid:' || route_id) AS color_key,
    MIN(slot) AS first_slot, MAX(route_color) AS route_color, MIN(route_id) AS route_id,
    string_agg(DISTINCT route_short_name, ',') AS route_short_names,
    MIN(route_short_name) AS route_short_name, MIN(route_type) AS route_type,
    MIN(feed_id) AS feed_id
  FROM transit_graph_edge_lines
  GROUP BY edge_id, COALESCE(NULLIF(route_color, ''), 'rid:' || route_id)
),
ranked AS (
  SELECT ec.*,
    (dense_rank() OVER (PARTITION BY edge_id ORDER BY first_slot, color_key) - 1) AS slot,
    count(*) OVER (PARTITION BY edge_id) AS line_count
  FROM edge_colors ec
),
merged AS (
  SELECT e.build_key, r.color_key, r.slot, r.line_count,
    MIN(r.feed_id) AS feed_id, MIN(r.route_id) AS route_id,
    MIN(r.route_short_name) AS route_short_name,
    string_agg(DISTINCT r.route_short_names, ',') AS route_short_names,
    MIN(r.route_type) AS route_type, MAX(r.route_color) AS route_color,
    ST_Transform(ST_LineMerge(ST_Collect(e.geom)), 3857) AS geom3857
  FROM ranked r JOIN transit_graph_edges e ON e.id = r.edge_id
  WHERE e.geom IS NOT NULL
  GROUP BY e.build_key, r.color_key, r.slot, r.line_count
)
-- One row per connected run (ST_LineMerge may leave disjoint parts); each is a
-- single LineString so ST_LineLocatePoint (used for clip fractions) is well-defined.
SELECT
  row_number() OVER () AS fid,
  m.build_key, m.color_key, m.slot, m.line_count, m.feed_id, m.route_id,
  m.route_short_name, m.route_short_names, m.route_type, m.route_color,
  part.geom AS geom3857,
  ST_Length(part.geom) AS len3857
FROM merged m
CROSS JOIN params p
CROSS JOIN LATERAL (
  -- Densify (add collinear vertices on straight runs so the taper can vary there),
  -- THEN Chaikin-smooth the junction corners (endpoints preserved -> run termini,
  -- hence station positions, stay put).
  SELECT ST_ChaikinSmoothing(ST_Segmentize((d).geom, p.seg), 2, true) AS geom
  FROM ST_Dump(m.geom3857) d
  WHERE ST_GeometryType((d).geom) = 'ST_LineString' AND ST_NPoints((d).geom) >= 2
) part
WHERE ST_Length(part.geom) > 0;

CREATE UNIQUE INDEX transit_lines_centerline_fid_idx ON transit_lines_centerline (fid);
CREATE INDEX transit_lines_centerline_geom_idx ON transit_lines_centerline USING GIST (geom3857);

-- Martin function source. Layer name 'transit_lines' matches the client's existing
-- source-layer, so pointing LINES_SOURCE at this function needs no client filter
-- edits. Emits mapbox_clip_start/end per clipped segment for cross-tile line-progress.
CREATE OR REPLACE FUNCTION transit_lines_rt(z integer, x integer, y integer)
RETURNS bytea AS $$
DECLARE
  mvt bytea;
  env  geometry := ST_TileEnvelope(z, x, y);                                  -- 3857
  -- Match ST_AsMVTGeom's 128/4096 render buffer so clipped segments (and their
  -- fractions) extend a hair past the tile edge — no hairline seams at boundaries.
  bw   double precision := (128.0 / 4096.0) * (2 * 20037508.342789 / power(2, z));
  envb geometry := ST_Expand(env, bw);
BEGIN
  SELECT INTO mvt ST_AsMVT(tile, 'transit_lines', 4096, 'geom') FROM (
    SELECT c.feed_id, c.route_id, c.route_short_name, c.route_short_names,
           c.route_type, c.route_color, c.slot, c.line_count,
           -- Fraction of the connected run (c.geom3857 is a single LineString) at
           -- this clipped segment's endpoints -> line-progress continuous across
           -- tile seams. Normalise so the EMITTED geometry runs in the direction of
           -- increasing fraction and clip_start <= clip_end: ST_Intersection does
           -- not guarantee it preserves the parent line's direction, and the bucket
           -- walks the emitted geometry's own vertex order, so geometry + fractions
           -- must be reversed together (reversing only the fractions would invert
           -- the taper). NOTE: progress is per connected part; ST_LineMerge already
           -- joins all degree-2 chains, so a part boundary is a real branch/terminus
           -- (where a converge/diverge taper is wanted), not a spurious mid-run cut.
           LEAST(fr.f0, fr.f1)    AS mapbox_clip_start,
           GREATEST(fr.f0, fr.f1) AS mapbox_clip_end,
           ST_AsMVTGeom(CASE WHEN fr.f0 <= fr.f1 THEN part.g ELSE ST_Reverse(part.g) END,
                        env, 4096, 128, true) AS geom
    FROM transit_lines_centerline c
    CROSS JOIN LATERAL (
      SELECT (ST_Dump(ST_Intersection(c.geom3857, envb))).geom AS g
    ) part
    CROSS JOIN LATERAL (
      SELECT ST_LineLocatePoint(c.geom3857, ST_StartPoint(part.g)) AS f0,
             ST_LineLocatePoint(c.geom3857, ST_EndPoint(part.g))   AS f1
    ) fr
    WHERE c.geom3857 && envb
      AND ST_GeometryType(part.g) = 'ST_LineString'
      AND ST_NPoints(part.g) >= 2
  ) AS tile
  WHERE geom IS NOT NULL;
  RETURN mvt;
END
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
