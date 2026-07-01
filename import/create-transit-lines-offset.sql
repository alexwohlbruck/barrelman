-- Bundled transit lines from the LOOM line graph, grouped by COLOUR, merged into
-- continuous geometry, then PRE-OFFSET (baked) into parallel ribbons.
--
-- Apple-style bundling: routes sharing a trunk AND a colour render as one line
-- (N/Q/R/W = one yellow Broadway line); different colours run as parallel
-- ribbons. We collapse each edge's line list to DISTINCT colours, assign a
-- per-edge colour slot, MERGE all edges of the same (colour, slot, line_count)
-- into one continuous line, then bake the parallel offset directly into the
-- geometry with ST_OffsetCurve (in Web Mercator metres, transformed back to
-- 4326).
--
-- OFFSET OPTION B (baked): unlike the client-side line-offset variant, the
-- ribbons are drawn at their real ground positions — a fixed GROUND separation,
-- so they SPREAD apart as you zoom in (Apple-style) and get proper geometric
-- joins at bends, at the cost of the constant-pixel zoom feel.
--
-- Run after import/load-transit-graph.ts.

DROP MATERIALIZED VIEW IF EXISTS transit_lines_offset CASCADE;
CREATE MATERIALIZED VIEW transit_lines_offset AS
WITH edge_colors AS (
  SELECT
    edge_id,
    COALESCE(NULLIF(route_color, ''), 'rid:' || route_id) AS color_key,
    MIN(slot)                                    AS first_slot,
    MAX(route_color)                             AS route_color,
    MIN(route_id)                                AS route_id,
    string_agg(DISTINCT route_short_name, ',')   AS route_short_names,
    MIN(route_short_name)                        AS route_short_name,
    MIN(route_type)                              AS route_type,
    MIN(feed_id)                                 AS feed_id
  FROM transit_graph_edge_lines
  GROUP BY edge_id, COALESCE(NULLIF(route_color, ''), 'rid:' || route_id)
),
ranked AS (
  SELECT
    ec.*,
    (dense_rank() OVER (PARTITION BY edge_id ORDER BY first_slot, color_key) - 1) AS slot,
    count(*) OVER (PARTITION BY edge_id) AS line_count
  FROM edge_colors ec
),
merged AS (
  SELECT
    row_number() OVER ()                    AS fid,
    e.build_key,
    r.color_key,
    r.slot,
    r.line_count,
    MIN(r.feed_id)                          AS feed_id,
    MIN(r.route_id)                          AS route_id,
    MIN(r.route_short_name)                 AS route_short_name,
    string_agg(DISTINCT r.route_short_names, ',') AS route_short_names,
    MIN(r.route_type)                       AS route_type,
    MAX(r.route_color)                      AS route_color,
    ST_LineMerge(ST_Collect(e.geom))        AS geom
  FROM ranked r
  JOIN transit_graph_edges e ON e.id = r.edge_id
  WHERE e.geom IS NOT NULL
  GROUP BY e.build_key, r.color_key, r.slot, r.line_count
)
SELECT
  m.fid, m.build_key, m.color_key, m.slot, m.line_count, m.feed_id, m.route_id,
  m.route_short_name, m.route_short_names, m.route_type, m.route_color,
  ST_Transform(ST_Collect(off.geom), 4326) AS geom
FROM merged m
CROSS JOIN LATERAL (
  -- Dump the (possibly multi-)line to components, offset each in Web Mercator
  -- metres, keeping the original if ST_OffsetCurve degenerates (tiny stubs).
  SELECT COALESCE(
           ST_OffsetCurve((d).geom, (m.slot - (m.line_count - 1) / 2.0) * 18.0),
           (d).geom
         ) AS geom
  FROM ST_Dump(ST_Transform(m.geom, 3857)) d
  WHERE ST_GeometryType((d).geom) = 'ST_LineString'
    AND ST_NPoints((d).geom) >= 2
) off
GROUP BY m.fid, m.build_key, m.color_key, m.slot, m.line_count, m.feed_id,
         m.route_id, m.route_short_name, m.route_short_names, m.route_type, m.route_color;

CREATE INDEX transit_lines_offset_geom_idx
  ON transit_lines_offset USING GIST (geom);
CREATE UNIQUE INDEX transit_lines_offset_fid_idx
  ON transit_lines_offset (fid);
