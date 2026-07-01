-- Bundled transit lines from the LOOM line graph, grouped by COLOUR and merged
-- into continuous geometry for smooth junctions.
--
-- Apple-style bundling: routes sharing a trunk AND a colour render as one line
-- (N/Q/R/W = one yellow Broadway line); different colours run as parallel
-- ribbons. We collapse each edge's ordered line list to its DISTINCT colours,
-- assign a per-edge colour slot, then MERGE all edges of the same
-- (colour, slot, line_count) into one continuous (multi)line via ST_LineMerge.
-- Merging removes the per-edge feature boundaries that made client-side
-- line-offset leave micro-gaps at every intermediate node — offset now renders
-- continuously along each ribbon, with a break only where the bundle width
-- actually changes (a route joins/leaves).
--
-- Offset is applied CLIENT-SIDE via MapLibre line-offset (constant pixels,
-- zoom-independent, mobile-safe), keyed on slot + line_count.
--
-- Run after import/load-transit-graph.ts. Window functions are fine (materialized).

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
)
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
GROUP BY e.build_key, r.color_key, r.slot, r.line_count;

CREATE INDEX transit_lines_offset_geom_idx
  ON transit_lines_offset USING GIST (geom);
CREATE UNIQUE INDEX transit_lines_offset_fid_idx
  ON transit_lines_offset (fid);
