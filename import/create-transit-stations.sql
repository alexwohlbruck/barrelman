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
-- BUILD SCOPING: only the CURRENTLY-RENDERED line builds contribute nodes — the
-- v3 build_keys that transit_line_segments (the drawn ribbons) carry. The
-- station graph tables still hold the superseded pre-v3 builds (chicago:l,
-- nyc:subway) side-by-side with their v3 replacements, and mixing them rendered
-- the SAME physical complex from two generations at once, a few metres apart
-- with slightly different node names (Clark/Lake from chicago:l-v3 +
-- Clark/Lake (Subway) from chicago:l = two markers). Scoping to the rendered
-- builds is what a marker source must do to agree with the lines it labels.
--
-- CLUSTERING (two levels). LOOM often splits a big interchange into several
-- nodes:
--   (1) same-name nodes within ~330 m fuse (Times Sq-42 St = one row, all
--       ~10 routes) — but same-name nodes KILOMETRES apart stay separate
--       (`86 St` appears on several unrelated lines), so this pass is
--       per-station_label.
--   (2) a proximity post-merge fuses adjacent complexes with DIFFERENT platform
--       names that are one physical transfer (74 St-Broadway / Jackson
--       Heights-Roosevelt Av; 59 St / Lexington Av-59 St; Brooklyn
--       Bridge-City Hall / Chambers St): preliminary complexes whose centroids
--       lie within STATION_MERGE_M of each other collapse to one marker whose
--       routes are the UNION of both. The threshold is tuned to catch real
--       transfer complexes (≤~80 m) without over-merging genuinely distinct
--       neighbours (8 St-NYU vs Astor Place at ~109 m stay two markers).
-- The merged complex takes a single canonical name (cleanest label: no
-- parenthetical qualifier such as "(Subway)"/"(Blue Line)", no directional
-- suffix, then shortest, then alphabetical for determinism).
--
-- Service gating happens PER COMPLEX, after clustering: counts are summed
-- across every matched platform stop of the complex (per-platform gating
-- halves counts — each direction platform sees only half the trips), then a
-- tiered gate keeps regular weekday routes when the complex has them, else
-- weekend routes, else anything with real service — so weekend-only and
-- night-only stations never vanish from the map.
--
-- SNAP: the marker sits on the rendered track ribbon. Conflation moved rail
-- stops onto OSM platform positions, which sit a few metres off the line
-- centreline (O'Hare: the Blue line ends and the dot floats beside it). Each
-- complex geom is snapped to the nearest point on its serving build's edge
-- centreline (transit_graph_edges — the geometry the ribbons are drawn from),
-- capped at STATION_SNAP_MAX_M; a complex further than the cap keeps its
-- conflated position (a mis-clustered or genuinely-off node is not dragged
-- across the map onto an unrelated line).
--
-- Run after import/load-transit-graph.ts (and after gtfs_stop_routes is imported).

DROP MATERIALIZED VIEW IF EXISTS transit_station_bullets CASCADE;
DROP MATERIALIZED VIEW IF EXISTS transit_stations CASCADE;
CREATE MATERIALIZED VIEW transit_stations AS
-- Tunables (inlined as literals below; documented here):
--   RENDERED_BUILDS   = ('chicago:l-v3','nyc:subway-v3') — the build_keys
--                       transit_line_segments actually draws.
--   STATION_MERGE_M   = 80  — proximity post-merge radius (level-2 clustering).
--   STATION_SNAP_MAX_M= 60  — max distance a marker is dragged onto its line.
--
-- (a1) Cluster same-named nodes within ~330 m into preliminary complexes.
WITH station_nodes AS (
  SELECT
    n.id,
    n.geom,
    n.station_label,
    ST_ClusterDBSCAN(n.geom, eps := 0.003, minpoints := 1)
      OVER (PARTITION BY n.station_label) AS lcid
  FROM transit_graph_nodes n
  WHERE n.build_key IN ('chicago:l-v3', 'nyc:subway-v3')
    AND n.station_id IS NOT NULL
    AND COALESCE(n.station_label, '') <> ''
),
-- (a2) Preliminary complex = one (station_label, lcid) with its centroid.
prelim AS (
  SELECT
    station_label,
    lcid,
    ST_Centroid(ST_Collect(geom)) AS geom
  FROM station_nodes
  GROUP BY station_label, lcid
),
-- (a3) Proximity post-merge: fuse preliminary complexes whose centroids lie
--      within STATION_MERGE_M (80 m). Clustered in Web-Mercator with the
--      per-latitude scale factor so the metre threshold is honest. minpoints:=1
--      so isolated complexes keep their own gid. gid is the final complex key.
merged AS (
  SELECT
    station_label,
    lcid,
    geom,
    ST_ClusterDBSCAN(
      ST_Transform(geom, 3857),
      eps := 80.0 / cos(radians(41)),
      minpoints := 1
    ) OVER () AS gid
  FROM prelim
),
-- Map every source node to its final complex gid (for stop matching + geom).
node_gid AS (
  SELECT sn.id, sn.geom, m.gid
  FROM station_nodes sn
  JOIN merged m
    ON m.station_label = sn.station_label AND m.lcid = sn.lcid
),
-- Every (complex, platform stop, route) with its service counts. DISTINCT on
-- the underlying gtfs_stop_routes row so a platform matched by several nodes
-- of the same complex is counted once. Bullet label: prefer route_short_name,
-- fall back to route_id for agencies that leave short_name blank and name
-- lines in route_long_name (e.g. CTA's 'Red'/'Brn'/'Org' L lines).
complex_stop_routes AS (
  SELECT DISTINCT
    ng.gid,
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
  FROM node_gid ng
  JOIN transit_stops ts
    ON ts.is_rail
   AND ST_DWithin(ts.geom::geography, ng.geom::geography, 150)
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
    gid,
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
  GROUP BY gid, route_short_name
),
complex_tiers AS (
  SELECT
    gid,
    bool_or(pass_weekday) AS has_weekday,
    bool_or(pass_weekend) AS has_weekend
  FROM complex_routes
  GROUP BY gid
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
  SELECT cr.gid, cr.route_short_name, cr.route_color, cr.route_type
  FROM complex_routes cr
  JOIN complex_tiers ct USING (gid)
  WHERE (ct.has_weekday AND cr.pass_weekday)
     OR (NOT ct.has_weekday AND ct.has_weekend AND cr.pass_weekend)
     OR (NOT ct.has_weekday AND NOT ct.has_weekend AND cr.pass_any)
),
-- Canonical name per complex: prefer the cleanest platform label. Score each
-- distinct label of the complex and keep the best. Lower score = cleaner:
--   +100 has a parenthetical qualifier   "(Subway)" / "(Blue Line)"
--   + 40 has a directional / qualifier token (N/S/E/W-bound, Uptown, etc.)
--   then shorter label wins, then alphabetical (deterministic).
complex_labels AS (
  SELECT DISTINCT gid, station_label FROM merged
),
canonical_name AS (
  SELECT DISTINCT ON (gid)
    gid,
    station_label AS name
  FROM complex_labels
  ORDER BY
    gid,
    (CASE WHEN station_label ~ '\(' THEN 100 ELSE 0 END)
    + (CASE WHEN station_label ~* '\y(uptown|downtown|northbound|southbound|eastbound|westbound|inbound|outbound)\y'
            THEN 40 ELSE 0 END),
    length(station_label),
    station_label
),
-- Raw complex centroid (pre-snap).
complex_geom AS (
  SELECT gid, ST_Centroid(ST_Collect(geom)) AS geom
  FROM node_gid
  GROUP BY gid
),
-- (d) Snap each complex onto its serving build's edge centreline (the geometry
--     the ribbons are drawn from). Cap at STATION_SNAP_MAX_M (60 m): beyond the
--     cap keep the conflated position (never drag a marker across the map).
snapped_geom AS (
  SELECT
    cg.gid,
    CASE
      WHEN nb.snap IS NULL THEN cg.geom
      WHEN ST_Distance(cg.geom::geography, nb.snap::geography) <= 60 THEN nb.snap
      ELSE cg.geom
    END AS geom
  FROM complex_geom cg
  LEFT JOIN LATERAL (
    SELECT ST_ClosestPoint(e.geom, cg.geom) AS snap
    FROM transit_graph_edges e
    WHERE e.build_key IN ('chicago:l-v3', 'nyc:subway-v3')
    ORDER BY e.geom <-> cg.geom
    LIMIT 1
  ) nb ON true
)
SELECT
  (row_number() OVER ())::int             AS fid,
  cn.name                                 AS name,
  count(*)::int                           AS route_count,
  jsonb_agg(
    jsonb_build_object('n', gr.route_short_name, 'c', gr.route_color, 't', gr.route_type)
    ORDER BY gr.route_short_name
  )::text                                 AS routes,
  sg.geom
FROM gated_routes gr
JOIN canonical_name cn USING (gid)
JOIN snapped_geom   sg USING (gid)
GROUP BY gr.gid, cn.name, sg.geom;

CREATE INDEX transit_stations_geom_idx ON transit_stations USING GIST (geom);
CREATE UNIQUE INDEX transit_stations_fid_idx ON transit_stations (fid);
