-- Grouped station route bullets (Apple-style rows: N Q R W at a station).
--
-- For each LOOM station node, one point per serving route, laid out in a
-- horizontal row: each bullet's point is projected east from the station by its
-- row index so the client draws them side-by-side. (Ground-projected rather
-- than a client pixel-offset because MVT can't carry the per-feature 2-vector
-- an icon-offset expression would need; bullets only show at high zoom where a
-- ~13 m step reads as a tight row.)
--
-- Run after import/load-transit-graph.ts.

DROP MATERIALIZED VIEW IF EXISTS transit_station_bullets CASCADE;
CREATE MATERIALIZED VIEW transit_station_bullets AS
WITH node_routes AS (
  SELECT DISTINCT ON (n.id, el.route_short_name)
    n.id            AS node_id,
    n.geom          AS node_geom,
    el.route_short_name,
    el.route_color,
    el.route_id,
    el.route_type
  FROM transit_graph_nodes n
  JOIN transit_graph_edges e
    ON e.build_key = n.build_key
   AND ST_DWithin(e.geom::geography, n.geom::geography, 15)
  JOIN transit_graph_edge_lines el ON el.edge_id = e.id
  WHERE n.station_id IS NOT NULL
    AND COALESCE(el.route_short_name, '') <> ''
  ORDER BY n.id, el.route_short_name, el.route_color
),
ranked AS (
  SELECT
    nr.*,
    (row_number() OVER (PARTITION BY node_id ORDER BY route_short_name) - 1) AS idx,
    count(*)      OVER (PARTITION BY node_id) AS cnt
  FROM node_routes nr
)
SELECT
  (node_id * 32 + idx)          AS fid,
  node_id,
  route_short_name,
  route_color,
  route_id,
  route_type,
  idx,
  cnt,
  ST_Project(
    node_geom::geography,
    (idx - (cnt - 1) / 2.0) * 13.0,  -- metres east per row position
    radians(90)
  )::geometry(Point, 4326)      AS geom
FROM ranked;

CREATE INDEX transit_station_bullets_geom_idx
  ON transit_station_bullets USING GIST (geom);
CREATE UNIQUE INDEX transit_station_bullets_fid_idx
  ON transit_station_bullets (fid);
