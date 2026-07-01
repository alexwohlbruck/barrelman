-- Bundled parallel offset geometry from the LOOM line graph.
--
-- For each (edge x line) we shift the edge centreline sideways by its slot so
-- interlined routes render as separated parallel ribbons instead of overlapping
-- (Apple/Transit-app style). Offset is applied in Web Mercator (metres-ish) so
-- the separation is a fixed ground distance; ST_OffsetCurve distance is signed
-- (left/right of direction), and centring on (line_count-1)/2 fans the bundle
-- out symmetrically about the centreline.
--
-- SPACING is in Web-Mercator units (~1.3x metres at NYC latitude). Tune for
-- visual separation. This is the pre-baked/mobile-safe offset; continuous-zoom
-- equidistance is a later render-time refinement.
--
-- Run after import/load-transit-graph.ts. Refreshable via REFRESH MATERIALIZED VIEW.

DROP MATERIALIZED VIEW IF EXISTS transit_lines_offset CASCADE;
CREATE MATERIALIZED VIEW transit_lines_offset AS
SELECT
  (e.id * 64 + el.slot)                 AS fid,
  el.edge_id,
  el.slot,
  el.feed_id,
  el.route_id,
  el.route_short_name,
  el.route_type,
  el.route_color,
  el.route_text_color,
  e.line_count,
  e.build_key,
  ST_Transform(
    ST_OffsetCurve(
      ST_Transform(e.geom, 3857),
      (el.slot - (e.line_count - 1) / 2.0) * 22.0
    ),
    4326
  ) AS geom
FROM transit_graph_edge_lines el
JOIN transit_graph_edges e ON e.id = el.edge_id
WHERE e.geom IS NOT NULL;

CREATE INDEX transit_lines_offset_geom_idx
  ON transit_lines_offset USING GIST (geom);
CREATE UNIQUE INDEX transit_lines_offset_fid_idx
  ON transit_lines_offset (fid);
