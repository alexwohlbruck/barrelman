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
-- Service gating happens PER COMPLEX, after clustering: counts are summed
-- across every matched platform stop of the complex (per-platform gating
-- halves counts — each direction platform sees only half the trips), then a
-- tiered gate keeps regular weekday routes when the complex has them, else
-- weekend routes, else anything with real service — so weekend-only and
-- night-only stations never vanish from the map.
--
-- Run after import/load-transit-graph.ts (and after gtfs_stop_routes is imported).

DROP MATERIALIZED VIEW IF EXISTS transit_station_bullets CASCADE;
DROP MATERIALIZED VIEW IF EXISTS transit_stations CASCADE;
CREATE MATERIALIZED VIEW transit_stations AS
-- (a) Cluster same-named LOOM nodes within ~330 m into one station complex
--     FIRST, before any service gating.
WITH station_nodes AS (
  SELECT
    n.id,
    n.geom,
    n.station_label,
    ST_ClusterDBSCAN(n.geom, eps := 0.003, minpoints := 1)
      OVER (PARTITION BY n.station_label) AS cid
  FROM transit_graph_nodes n
  WHERE n.station_id IS NOT NULL
    AND COALESCE(n.station_label, '') <> ''
),
-- Every (complex, platform stop, route) with its service counts. DISTINCT on
-- the underlying gtfs_stop_routes row so a platform matched by several nodes
-- of the same complex is counted once. Bullet label: prefer route_short_name,
-- fall back to route_id for agencies that leave short_name blank and name
-- lines in route_long_name (e.g. CTA's 'Red'/'Brn'/'Org' L lines).
complex_stop_routes AS (
  SELECT DISTINCT
    sn.station_label,
    sn.cid,
    COALESCE(NULLIF(r.route_short_name, ''), r.route_id) AS route_short_name,
    r.route_color,
    r.route_type,
    sr.feed_id,
    sr.stop_id,
    sr.route_id,
    -- trips_weekday_day is populated by import/backfill-stop-route-service.ts
    -- (representative-day counts); weekday_trips is the legacy column kept in
    -- sync — used as fallback for feeds not yet re-backfilled.
    COALESCE(sr.trips_weekday_day, sr.weekday_trips) AS trips_weekday_day,
    sr.trips_weekend_day,
    sr.trips_any
  FROM station_nodes sn
  JOIN transit_stops ts
    ON ts.is_rail
   AND ST_DWithin(ts.geom::geography, sn.geom::geography, 150)
  JOIN gtfs_stop_routes sr
    ON sr.feed_id = ts.feed_id AND sr.stop_id = ts.stop_id
  JOIN gtfs_routes r
    ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
  WHERE COALESCE(NULLIF(r.route_short_name, ''), r.route_id) <> ''
),
-- (b) SUM service across all matched platform stops of the complex, per
--     bullet label. NULL counts (feed not yet backfilled) count as passing —
--     fail-open, those stations still show everything.
--     Grouping by route_short_name (not route_id) is deliberate: the rendered
--     unit is one bullet per label, so same-named route_id variants (peak
--     branches, feed splits) pool their counts toward that bullet's gate.
complex_routes AS (
  SELECT
    station_label,
    cid,
    route_short_name,
    min(route_color) AS route_color,
    min(route_type)  AS route_type,
    bool_or(trips_weekday_day IS NULL)
      OR COALESCE(sum(trips_weekday_day), 0) >= 2 AS pass_weekday,
    bool_or(trips_weekend_day IS NULL)
      OR COALESCE(sum(trips_weekend_day), 0) >= 2 AS pass_weekend,
    bool_or(trips_any IS NULL)
      OR COALESCE(sum(trips_any), 0) >= 1         AS pass_any
  FROM complex_stop_routes
  GROUP BY station_label, cid, route_short_name
),
complex_tiers AS (
  SELECT
    station_label,
    cid,
    bool_or(pass_weekday) AS has_weekday,
    bool_or(pass_weekend) AS has_weekend
  FROM complex_routes
  GROUP BY station_label, cid
),
-- (c) Tiered regular-service gate per complex:
--       tier 1: summed trips_weekday_day >= 2 (regular weekday service —
--               matches the MTA diagrams / Apple),
--       tier 2 (only if tier 1 is empty for the complex): summed
--               trips_weekend_day >= 2 (weekend-only stations keep labels),
--       tier 3 (only if still empty): summed trips_any >= 1 (night-only /
--               once-daily intercity stations keep labels).
--     A station with any real service must never end up label-less.
--
-- Anchors (NYC subway) that must hold:
--   • Christopher St–Sheridan Sq: the late-night-only 2 has summed
--     trips_weekday_day = 0; the 1 passes tier 1, so the complex resolves at
--     tier 1 and the 2 stays hidden.
--   • Kingston Av: the lone AM-rush 5 sums to 1 (< 2) across the complex; the
--     3 passes tier 1, so the complex resolves at tier 1 and the 5 stays
--     hidden.
gated_routes AS (
  SELECT cr.station_label, cr.cid, cr.route_short_name, cr.route_color, cr.route_type
  FROM complex_routes cr
  JOIN complex_tiers ct USING (station_label, cid)
  WHERE (ct.has_weekday AND cr.pass_weekday)
     OR (NOT ct.has_weekday AND ct.has_weekend AND cr.pass_weekend)
     OR (NOT ct.has_weekday AND NOT ct.has_weekend AND cr.pass_any)
),
cluster_geom AS (
  SELECT station_label, cid, ST_Centroid(ST_Collect(geom)) AS geom
  FROM station_nodes
  GROUP BY station_label, cid
)
SELECT
  (row_number() OVER ())::int             AS fid,
  gr.station_label                        AS name,
  count(*)::int                           AS route_count,
  jsonb_agg(
    jsonb_build_object('n', gr.route_short_name, 'c', gr.route_color, 't', gr.route_type)
    ORDER BY gr.route_short_name
  )::text                                 AS routes,
  cg.geom
FROM gated_routes gr
JOIN cluster_geom cg USING (station_label, cid)
GROUP BY gr.station_label, gr.cid, cg.geom;

CREATE INDEX transit_stations_geom_idx ON transit_stations USING GIST (geom);
CREATE UNIQUE INDEX transit_stations_fid_idx ON transit_stations (fid);
