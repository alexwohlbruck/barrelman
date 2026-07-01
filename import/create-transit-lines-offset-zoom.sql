-- Zoom-dependent, pre-baked, junction-smoothed transit line ribbons.
--
-- Combines three things so the display is Apple-like AND cross-platform:
--   1. Colour-merge bundling (routes sharing a trunk + colour = one line).
--   2. Junction smoothing: ST_ChaikinSmoothing on the merged centreline (2 iters,
--      endpoints preserved so stations stay on the line) rounds the sharp kinks
--      LOOM leaves where edges meet at nodes — baked, no custom GL layer needed.
--   3. Per-zoom baked parallel offset: ST_OffsetCurve in Web Mercator by
--      (slot - (line_count-1)/2) * PX * (78271.51696 / 2^z). MapLibre/Mapbox
--      render vector tiles at 512px, so the on-screen resolution at integer zoom
--      z is 78271.51696/2^z metres/pixel (NOT the 256px 156543/2^z — using that
--      made ribbons 2x too far apart, "off by one zoom"). This makes each ribbon
--      render at a CONSTANT PX-pixel gap at every integer zoom.
--
-- One row per (ribbon × integer zoom 10..18). Served by the Martin FUNCTION
-- source transit_lines_zoom(z,x,y) below, which returns only the rows for the
-- requested (clamped) zoom. Martin does NOT auto-discover functions when a
-- postgres.tables: block is present, so the function is registered explicitly in
-- martin-config.yaml (postgres.functions:).
--
-- Run after import/load-transit-graph.ts (rebuilds fully; DROP + CREATE).

DROP MATERIALIZED VIEW IF EXISTS transit_lines_offset_zoom CASCADE;
CREATE MATERIALIZED VIEW transit_lines_offset_zoom AS
WITH params AS (
  SELECT 4.4::double precision AS px          -- on-screen gap between ribbons (px)
),
edge_colors AS (
  SELECT edge_id,
    COALESCE(NULLIF(route_color, ''), 'rid:' || route_id) AS color_key,
    MIN(slot) AS first_slot, MAX(route_color) AS route_color, MIN(route_id) AS route_id,
    string_agg(DISTINCT route_short_name, ',') AS route_short_names,
    MIN(route_short_name) AS route_short_name, MIN(route_type) AS route_type, MIN(feed_id) AS feed_id
  FROM transit_graph_edge_lines
  GROUP BY edge_id, COALESCE(NULLIF(route_color, ''), 'rid:' || route_id)
),
ranked AS (
  SELECT ec.*, (dense_rank() OVER (PARTITION BY edge_id ORDER BY first_slot, color_key) - 1) AS slot,
    count(*) OVER (PARTITION BY edge_id) AS line_count
  FROM edge_colors ec
),
merged AS (
  SELECT row_number() OVER () AS fid, e.build_key, r.color_key, r.slot, r.line_count,
    MIN(r.feed_id) AS feed_id, MIN(r.route_id) AS route_id, MIN(r.route_short_name) AS route_short_name,
    string_agg(DISTINCT r.route_short_names, ',') AS route_short_names,
    MIN(r.route_type) AS route_type, MAX(r.route_color) AS route_color,
    ST_Transform(ST_LineMerge(ST_Collect(e.geom)), 3857) AS geom3857
  FROM ranked r JOIN transit_graph_edges e ON e.id = r.edge_id
  WHERE e.geom IS NOT NULL
  GROUP BY e.build_key, r.color_key, r.slot, r.line_count
)
SELECT (m.fid * 100 + z) AS fid, z AS zlvl,
  m.build_key, m.color_key, m.slot, m.line_count, m.feed_id, m.route_id,
  m.route_short_name, m.route_short_names, m.route_type, m.route_color,
  ST_Transform(ST_Collect(off.geom), 4326) AS geom
FROM merged m
CROSS JOIN generate_series(10, 18) AS z
CROSS JOIN params p
CROSS JOIN LATERAL (
  -- Round junction corners (Chaikin, endpoints preserved), then offset the
  -- SMOOTHED centreline by the zoom-appropriate ground distance. Keep the
  -- smoothed line if ST_OffsetCurve degenerates (tiny stubs).
  SELECT COALESCE(
           ST_OffsetCurve(
             ST_ChaikinSmoothing((d).geom, 2, true),
             (m.slot - (m.line_count - 1) / 2.0) * p.px * (78271.51696 / power(2, z))
           ),
           ST_ChaikinSmoothing((d).geom, 2, true)
         ) AS geom
  FROM ST_Dump(m.geom3857) d
  WHERE ST_GeometryType((d).geom) = 'ST_LineString' AND ST_NPoints((d).geom) >= 2
) off
GROUP BY m.fid, z, m.build_key, m.color_key, m.slot, m.line_count, m.feed_id,
         m.route_id, m.route_short_name, m.route_short_names, m.route_type, m.route_color;

CREATE UNIQUE INDEX transit_lines_offset_zoom_fid_idx ON transit_lines_offset_zoom (fid);
CREATE INDEX transit_lines_offset_zoom_geom_idx ON transit_lines_offset_zoom USING GIST (geom);
CREATE INDEX transit_lines_offset_zoom_z_idx ON transit_lines_offset_zoom (zlvl);

-- Martin function source: serve the rows for the requested (clamped) zoom as MVT.
-- Layer name 'transit_lines' matches the client's existing source-layer, so no
-- client layer edits are needed beyond pointing the source at this function.
CREATE OR REPLACE FUNCTION transit_lines_zoom(z integer, x integer, y integer)
RETURNS bytea AS $$
DECLARE
  mvt bytea;
  zc  integer := LEAST(GREATEST(z, 10), 18);
BEGIN
  SELECT INTO mvt ST_AsMVT(tile, 'transit_lines', 4096, 'geom') FROM (
    SELECT t.feed_id, t.route_id, t.route_short_name, t.route_short_names,
           t.route_type, t.route_color, t.slot, t.line_count,
           ST_AsMVTGeom(ST_Transform(t.geom, 3857), ST_TileEnvelope(z, x, y), 4096, 128, true) AS geom
    FROM transit_lines_offset_zoom t
    WHERE t.zlvl = zc AND t.geom && ST_Transform(ST_TileEnvelope(z, x, y), 4326)
  ) AS tile WHERE geom IS NOT NULL;
  RETURN mvt;
END
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
