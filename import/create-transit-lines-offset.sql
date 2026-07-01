-- Bundled transit lines from the LOOM line graph, grouped by COLOUR.
--
-- Apple-style bundling: routes that share a trunk AND a colour render as ONE
-- line (e.g. N/Q/R/W = one yellow Broadway line), while different colours run
-- as parallel ribbons. So we collapse each edge's ordered line list down to its
-- DISTINCT colours (keeping LOOM's ordering via first appearance), and emit one
-- row per (edge x colour) carrying the CENTRELINE geometry plus a slot index.
--
-- The perpendicular offset is applied CLIENT-SIDE via MapLibre `line-offset`
-- (constant pixels, zoom-independent, mobile-safe) rather than baked into the
-- geometry — so ribbons keep a fixed on-screen separation at every zoom instead
-- of spreading apart. Window functions are fine here (materialized, not per-tile).

DROP MATERIALIZED VIEW IF EXISTS transit_lines_offset CASCADE;
CREATE MATERIALIZED VIEW transit_lines_offset AS
WITH edge_colors AS (
  SELECT
    edge_id,
    -- group same-colour routes; keep uncoloured routes separate by route_id
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
    count(*)      OVER (PARTITION BY edge_id) AS line_count
  FROM edge_colors ec
)
SELECT
  (e.id * 32 + r.slot)      AS fid,
  r.edge_id,
  r.slot,
  r.line_count,
  r.feed_id,
  r.route_id,
  r.route_short_name,
  r.route_short_names,
  r.route_type,
  r.route_color,
  e.geom                    AS geom      -- centreline; offset applied client-side
FROM ranked r
JOIN transit_graph_edges e ON e.id = r.edge_id
WHERE e.geom IS NOT NULL;

CREATE INDEX transit_lines_offset_geom_idx
  ON transit_lines_offset USING GIST (geom);
CREATE UNIQUE INDEX transit_lines_offset_fid_idx
  ON transit_lines_offset (fid);
