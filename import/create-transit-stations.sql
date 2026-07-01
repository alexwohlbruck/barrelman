-- Per-station data for the client's HTML station-label markers.
--
-- One row per station COMPLEX: the station name plus the JSON list of routes
-- serving it (short name + colour + type). The client renders each as an
-- Apple-style DOM label (station name + a row of coloured route bullets).
--
-- Routes are derived from ACTUAL GTFS SERVICE, not track geometry: each LOOM
-- station node is matched to nearby rail GTFS stops (within 150 m) and we read
-- the routes that actually STOP there from gtfs_stop_routes. Express trains that
-- merely pass through a local stop have no stop record there, so they are
-- correctly excluded (e.g. Houston St on the 1 line shows only 1/2, never the
-- express 3 whose tracks physically run through it).
--
-- LOOM often splits a big interchange into several nodes; we cluster same-named
-- nodes within ~330 m back into one complex so the client shows a single label
-- carrying every route (e.g. Times Sq-42 St = one row, all ~10 routes).
--
-- Run after import/load-transit-graph.ts (and after gtfs_stop_routes is imported).

DROP MATERIALIZED VIEW IF EXISTS transit_station_bullets CASCADE;
DROP MATERIALIZED VIEW IF EXISTS transit_stations CASCADE;
CREATE MATERIALIZED VIEW transit_stations AS
WITH node_routes AS (
  -- Bullet label: prefer route_short_name, fall back to route_id for agencies
  -- that leave short_name blank and name lines in route_long_name (e.g. CTA's
  -- 'Red'/'Brn'/'Org' L lines).
  SELECT DISTINCT ON (n.id, COALESCE(NULLIF(r.route_short_name, ''), r.route_id))
    n.id            AS node_id,
    n.geom          AS geom,
    n.station_label AS station_label,
    COALESCE(NULLIF(r.route_short_name, ''), r.route_id) AS route_short_name,
    r.route_color,
    r.route_type
  FROM transit_graph_nodes n
  JOIN transit_stops ts
    ON ts.is_rail
   AND ST_DWithin(ts.geom::geography, n.geom::geography, 150)
  JOIN gtfs_stop_routes sr
    ON sr.feed_id = ts.feed_id AND sr.stop_id = ts.stop_id
  JOIN gtfs_routes r
    ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
  WHERE n.station_id IS NOT NULL
    AND COALESCE(n.station_label, '') <> ''
    AND COALESCE(NULLIF(r.route_short_name, ''), r.route_id) <> ''
    -- Regular-service filter: the route must make at least 2 weekday-daytime
    -- trips at some platform of this complex. Excludes late-night-only reroutes
    -- (e.g. the 2 at a 1-line local stop = 0 daytime trips) and single "select"
    -- trips (the lone AM-rush 5 at Kingston Av = 1) — matching the MTA diagrams /
    -- Apple. weekday_trips is populated by import/backfill-stop-route-service.ts;
    -- NULL (feed not yet backfilled) fails open so those stations still show.
    AND COALESCE(sr.weekday_trips, 999) >= 2
  ORDER BY n.id, COALESCE(NULLIF(r.route_short_name, ''), r.route_id), r.route_color
),
-- Cluster same-named nodes within ~330 m into one station complex.
clustered AS (
  SELECT *,
    ST_ClusterDBSCAN(geom, eps := 0.003, minpoints := 1)
      OVER (PARTITION BY station_label) AS cid
  FROM node_routes
),
-- Dedupe routes shared across the merged nodes: one row per (complex, route).
cluster_routes AS (
  SELECT DISTINCT ON (station_label, cid, route_short_name)
    station_label, cid, route_short_name, route_color, route_type
  FROM clustered
  ORDER BY station_label, cid, route_short_name, route_color
),
cluster_geom AS (
  SELECT station_label, cid, ST_Centroid(ST_Collect(geom)) AS geom
  FROM clustered
  GROUP BY station_label, cid
)
SELECT
  (row_number() OVER ())::int             AS fid,
  cr.station_label                        AS name,
  count(*)::int                           AS route_count,
  jsonb_agg(
    jsonb_build_object('n', cr.route_short_name, 'c', cr.route_color, 't', cr.route_type)
    ORDER BY cr.route_short_name
  )::text                                 AS routes,
  cg.geom
FROM cluster_routes cr
JOIN cluster_geom cg USING (station_label, cid)
GROUP BY cr.station_label, cr.cid, cg.geom;

CREATE INDEX transit_stations_geom_idx ON transit_stations USING GIST (geom);
CREATE UNIQUE INDEX transit_stations_fid_idx ON transit_stations (fid);
