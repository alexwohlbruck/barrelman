-- GTFS display views for Martin tile serving.
-- These expose the imported GTFS routes/stops as vector-tile sources for the
-- transit DISPLAY layer (distinct from the OSM-derived transit_* views in
-- create-transit-views.sql). Run after a GTFS import + backfill-shape-geom.
--
-- IMPORTANT: keep these views free of window functions (e.g. ROW_NUMBER()
-- OVER ()). A window function is an optimization fence that blocks Martin's
-- per-tile envelope filter from pushing down into the GiST index scan, turning
-- every tile request into a full scan. The fid comes straight from the source
-- table's unique serial PK (pushdown-safe and stable per feature).

-- Ledger of LOOM-bundled graphs (also created by ensureTransitGraphSchema);
-- declared here so transit_routes can exclude bundled feeds even on a fresh DB.
CREATE TABLE IF NOT EXISTS transit_graph_builds (
  build_key TEXT PRIMARY KEY,
  feed_id TEXT,
  mode TEXT,
  route_type INTEGER,
  built_at TIMESTAMPTZ DEFAULT NOW()
);

-- Routes: each route drawn along its canonical shape geometry. One row per
-- route (gtfs_routes.id is unique; the canonical shape_id joins to exactly one
-- gtfs_shapes row). Feeds/modes that have a LOOM-bundled graph are EXCLUDED
-- here and served instead from transit_lines_offset (parallel offset ribbons),
-- so they aren't drawn twice.
DROP VIEW IF EXISTS transit_routes CASCADE;
CREATE VIEW transit_routes AS
SELECT
  r.id                                  AS fid,
  r.feed_id,
  r.route_id,
  r.route_type,
  COALESCE(r.route_color, '')           AS route_color,
  COALESCE(r.route_text_color, '')      AS route_text_color,
  COALESCE(r.route_short_name, '')      AS route_short_name,
  COALESCE(r.route_long_name, '')       AS route_long_name,
  s.geom
FROM gtfs_routes r
JOIN gtfs_shapes s
  ON s.feed_id = r.feed_id AND s.shape_id = r.shape_id
WHERE s.geom IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM transit_graph_builds b
    WHERE b.feed_id = r.feed_id AND b.route_type = r.route_type
  );

-- Stops: boardable stops (location_type 0) and stations (location_type 1),
-- enriched with the serving routes so the client can colour each stop:
--   route_count = 1 ⇒ colour the dot by route_color
--   route_count > 1 ⇒ interchange ⇒ white connecting bar (route_color empty)
-- The hosted Transitland stop tiles never carried route colour; ours do.
-- LATERAL (not a window function) keeps the per-tile envelope filter pushable
-- into the gtfs_stops GiST scan.
--
-- route_count/route_color use the same tiered service preference as the
-- station labels (import/create-transit-stations.sql): prefer routes with
-- regular weekday service (trips_weekday_day >= 2, NULL = not yet backfilled
-- fails open), else weekend service (trips_weekend_day >= 2), else everything
-- — so the dot never disagrees with the label (e.g. Christopher St stays a
-- single-route dot even though the late-night-only 2 has a stop record).
-- route_count_all keeps the ungated total for anything relying on the old
-- semantics. Implemented with FILTERed aggregates (no window functions).
DROP VIEW IF EXISTS transit_stops CASCADE;
CREATE VIEW transit_stops AS
SELECT
  st.id                                 AS fid,
  st.feed_id,
  st.stop_id,
  -- Prefer the conflated OSM name when matched, else the GTFS name (which
  -- already carries any manual override applied by applyDisplayOverrides).
  COALESCE(NULLIF(st.osm_name, ''), st.stop_name, '') AS stop_name,
  COALESCE(st.location_type, 0)         AS location_type,
  COALESCE(st.parent_station, '')       AS parent_station,
  COALESCE(st.wheelchair_boarding, 0)   AS wheelchair_boarding,
  COALESCE(rc.route_count, 0)           AS route_count,
  COALESCE(rc.route_color, '')          AS route_color,
  COALESCE(rc.route_text_color, '')     AS route_text_color,
  -- is_rail: served by any non-bus route (tram/subway/rail/ferry/etc), gate-
  -- free on purpose — a night-only rail stop is still a rail stop. Lets the
  -- client show rail stops prominently and bus-only stops faint + high-zoom.
  COALESCE(rc.is_rail, false)           AS is_rail,
  COALESCE(rc.route_count_all, 0)       AS route_count_all,
  st.geom
FROM gtfs_stops st
LEFT JOIN LATERAL (
  SELECT
    CASE WHEN x.n_weekday > 0 THEN x.n_weekday
         WHEN x.n_weekend > 0 THEN x.n_weekend
         ELSE x.n_all END AS route_count,
    CASE WHEN x.n_weekday > 0 THEN x.color_weekday
         WHEN x.n_weekend > 0 THEN x.color_weekend
         ELSE x.color_all END AS route_color,
    CASE WHEN x.n_weekday > 0 THEN x.text_weekday
         WHEN x.n_weekend > 0 THEN x.text_weekend
         ELSE x.text_all END AS route_text_color,
    x.n_all AS route_count_all,
    x.is_rail
  FROM (
    SELECT
      count(DISTINCT r.route_id) FILTER
        (WHERE COALESCE(sr.trips_weekday_day, sr.weekday_trips, 999) >= 2) AS n_weekday,
      count(DISTINCT r.route_id) FILTER
        (WHERE COALESCE(sr.trips_weekend_day, 999) >= 2) AS n_weekend,
      count(DISTINCT r.route_id) AS n_all,
      bool_or(r.route_type NOT IN (3, 11)) AS is_rail,
      CASE WHEN count(DISTINCT r.route_id) FILTER
             (WHERE COALESCE(sr.trips_weekday_day, sr.weekday_trips, 999) >= 2) = 1
           THEN max(COALESCE(r.route_color, '')) FILTER
             (WHERE COALESCE(sr.trips_weekday_day, sr.weekday_trips, 999) >= 2)
           ELSE '' END AS color_weekday,
      CASE WHEN count(DISTINCT r.route_id) FILTER
             (WHERE COALESCE(sr.trips_weekend_day, 999) >= 2) = 1
           THEN max(COALESCE(r.route_color, '')) FILTER
             (WHERE COALESCE(sr.trips_weekend_day, 999) >= 2)
           ELSE '' END AS color_weekend,
      CASE WHEN count(DISTINCT r.route_id) = 1
           THEN max(COALESCE(r.route_color, '')) ELSE '' END AS color_all,
      CASE WHEN count(DISTINCT r.route_id) FILTER
             (WHERE COALESCE(sr.trips_weekday_day, sr.weekday_trips, 999) >= 2) = 1
           THEN max(COALESCE(r.route_text_color, '')) FILTER
             (WHERE COALESCE(sr.trips_weekday_day, sr.weekday_trips, 999) >= 2)
           ELSE '' END AS text_weekday,
      CASE WHEN count(DISTINCT r.route_id) FILTER
             (WHERE COALESCE(sr.trips_weekend_day, 999) >= 2) = 1
           THEN max(COALESCE(r.route_text_color, '')) FILTER
             (WHERE COALESCE(sr.trips_weekend_day, 999) >= 2)
           ELSE '' END AS text_weekend,
      CASE WHEN count(DISTINCT r.route_id) = 1
           THEN max(COALESCE(r.route_text_color, '')) ELSE '' END AS text_all
    FROM gtfs_stop_routes sr
    JOIN gtfs_routes r ON r.feed_id = sr.feed_id AND r.route_id = sr.route_id
    WHERE sr.feed_id = st.feed_id AND sr.stop_id = st.stop_id
  ) x
) rc ON TRUE
WHERE st.geom IS NOT NULL
  AND COALESCE(st.location_type, 0) IN (0, 1);
