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
-- CLUSTERING — by GTFS parent_station (NOT proximity). One marker per
-- parent_station. The v3 LOOM build already stamped each station node with the
-- GTFS parent_station id it matched (transit_graph_nodes.station_id resolves to
-- a location_type=1 parent in both rendered feeds), so grouping nodes by
-- (feed, station_id) reproduces the agency's own station-complex grouping:
--   • Clark/Lake  → parent 40380 with four child platform stops → ONE marker.
--   • Jackson (Chicago) → TWO parents, 40070 (Blue) and 40560 (Red), ~130 m
--     apart → TWO markers. Same physical street name, genuinely different
--     stations; the cross-reference between them is the /transit/station
--     "connections" list, not a merge.
--   • Monroe (Chicago) → likewise two parents (Blue 40790, Red 41090).
--   • Fulton Street (NYC) → five distinct "Fulton Street" parents (plus the
--     separately-named Fulton St & X stops) → the real per-parent marker count.
-- The previous 80 m PROXIMITY post-merge is REMOVED: it fused same-name but
-- DIFFERENT parent_stations (both Jacksons, both Monroes) into one marker.
-- parent_station is the agency's authoritative complex key; proximity is not.
--
-- FALLBACK for feeds without parent_station metadata (station_id unresolved):
-- per-station_label DBSCAN (same-name nodes within ~330 m fuse, same-name nodes
-- kilometres apart stay separate). Never proximity across different names.
--
-- Service gating happens PER COMPLEX, after clustering: counts are summed
-- across every matched platform stop of the complex (per-platform gating
-- halves counts — each direction platform sees only half the trips), then a
-- tiered gate keeps regular weekday routes when the complex has them, else
-- weekend routes, else anything with real service — so weekend-only and
-- night-only stations never vanish from the map.
--
-- CENTERING — the marker sits at the CENTRE of the platform, on the rendered
-- track ribbon. Conflation moved rail stops onto OSM platform positions; the
-- old code snapped the node centroid to the nearest point on the line, which
-- lands the dot at whatever bit of platform is closest, not its middle. Instead
-- we project the station's OSM platform extent (transit_platforms —
-- railway=platform geometry within STATION_PLATFORM_M of the complex) onto the
-- serving build's edge centreline and put the marker at the MIDPOINT of that
-- projected span.
--   • TERMINAL exception: if a line endpoint falls inside the projected span
--     (end-of-line, e.g. O'Hare), the natural marker position is the platform
--     end, not a forced mid-span point that would sit off the platform — so we
--     clamp the midpoint toward the in-span endpoint.
--   • FALLBACK: complexes with no matching platform (all of Chicago today has
--     no OSM railway=platform geometry) keep the round-9 nearest-point snap,
--     capped at STATION_SNAP_MAX_M.
--   • Drift cap: STATION_CENTER_MAX_M — never drag a marker across the map onto
--     an unrelated line; beyond the cap keep the conflated centroid.
--
-- Run after import/load-transit-graph.ts (and after gtfs_stop_routes is imported).

DROP MATERIALIZED VIEW IF EXISTS transit_station_bullets CASCADE;
DROP MATERIALIZED VIEW IF EXISTS transit_stations CASCADE;
CREATE MATERIALIZED VIEW transit_stations AS
-- Tunables (inlined as literals below; documented here):
--   RENDERED_BUILDS     = ('chicago:l-v3','nyc:subway-v3') — the build_keys
--                         transit_line_segments actually draws.
--   STATION_PLATFORM_M  = 90  — max distance from the complex centroid to an
--                         OSM platform that centres the marker.
--   STATION_CENTER_MAX_M= 90  — max distance the platform-centre may sit from
--                         the conflated centroid (drift cap).
--   STATION_SNAP_MAX_M  = 60  — max distance the nearest-point-snap fallback
--                         may drag a marker onto its line.
--
-- (a) Every rendered station node, tagged with its GTFS parent id (station_id).
WITH station_nodes AS (
  SELECT
    n.id,
    n.geom,
    n.station_id,
    n.station_label,
    CASE WHEN n.build_key LIKE 'chicago:%' THEN '29' ELSE '5' END AS feed_id
  FROM transit_graph_nodes n
  WHERE n.build_key IN ('chicago:l-v3', 'nyc:subway-v3')
    AND n.station_id IS NOT NULL
    AND COALESCE(n.station_label, '') <> ''
),
-- (a2) FALLBACK clustering for nodes whose station_id does not resolve to a
--      GTFS parent (feeds without parent_station metadata). Per-label DBSCAN so
--      same-name nodes within ~330 m fuse but kilometres-apart same-name nodes
--      stay separate. minpoints:=1 so isolated nodes keep a cluster.
node_parent AS (
  SELECT
    sn.id, sn.geom, sn.station_label, sn.feed_id,
    CASE WHEN gp.stop_id IS NOT NULL THEN true ELSE false END AS has_parent,
    -- Final complex key. Parents keyed by (feed, parent_id); fallback nodes
    -- keyed by (feed, label, dbscan-cluster) with a marker prefix so the two
    -- keyspaces never collide.
    CASE
      WHEN gp.stop_id IS NOT NULL
        THEN 'p:' || sn.feed_id || ':' || sn.station_id
      ELSE 'l:' || sn.feed_id || ':' || sn.station_label || ':' ||
           ST_ClusterDBSCAN(sn.geom, eps := 0.003, minpoints := 1)
             OVER (PARTITION BY sn.feed_id, sn.station_label)
    END AS gid,
    COALESCE(gp.stop_name, sn.station_label) AS parent_name
  FROM station_nodes sn
  LEFT JOIN gtfs_stops gp
    ON gp.feed_id = sn.feed_id
   AND gp.stop_id = sn.station_id
   AND gp.location_type = 1
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
  FROM node_parent ng
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
-- Canonical name per complex. For a resolved parent every node carries the
-- parent's stop_name, so parent_name is already canonical; the scoring only
-- matters for the fallback keyspace where several platform labels share a gid.
-- Lower score = cleaner:
--   +100 has a parenthetical qualifier   "(Subway)" / "(Blue Line)"
--   + 40 has a directional / qualifier token (N/S/E/W-bound, Uptown, etc.)
--   then shorter label wins, then alphabetical (deterministic).
complex_labels AS (
  SELECT DISTINCT gid, parent_name AS station_label FROM node_parent
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
-- Raw complex centroid (pre-centering) + the build the complex belongs to.
complex_geom AS (
  SELECT
    gid,
    min(feed_id) AS feed_id,
    ST_Centroid(ST_Collect(geom)) AS geom
  FROM node_parent
  GROUP BY gid
),
-- (d1) PLATFORM CENTERING. For each complex, collect OSM platform geometry
--      within STATION_PLATFORM_M of the centroid, and the single nearest
--      serving edge (the rendered ribbon centreline). Project the platform
--      extent's boundary points onto the edge, take the min/max line fraction
--      (the projected span), and place the marker at its MIDPOINT.
--      TERMINAL: if the edge endpoint (fraction 0 or 1) lies inside the span,
--      clamp the midpoint toward that endpoint so the dot stays on the platform
--      end (natural end-of-line position) rather than a forced centre off it.
platform_center AS (
  SELECT
    cg.gid,
    ST_LineInterpolatePoint(nb.eg, gc.f_marker) AS geom,
    ST_Distance(
      ST_LineInterpolatePoint(nb.eg, gc.f_marker)::geography,
      cg.geom::geography
    ) AS drift_m
  FROM complex_geom cg
  -- serving edge (nearest ribbon centreline of the build)
  JOIN LATERAL (
    SELECT e.geom AS eg
    FROM transit_graph_edges e
    WHERE e.build_key IN ('chicago:l-v3', 'nyc:subway-v3')
    ORDER BY e.geom <-> cg.geom
    LIMIT 1
  ) nb ON true
  -- merged nearby platform geometry
  JOIN LATERAL (
    SELECT ST_Collect(p.geom) AS pg
    FROM transit_platforms p
    WHERE (p.public_transport = 'platform' OR p.railway = 'platform')
      AND ST_DWithin(p.geom::geography, cg.geom::geography, 90)
  ) pl ON pl.pg IS NOT NULL
  -- projected span of the platform onto the edge, then the (possibly clamped)
  -- marker fraction
  JOIN LATERAL (
    SELECT
      f_lo, f_hi,
      -- terminal clamp: if an edge endpoint sits within the span, snap the
      -- marker to that end; else use the span midpoint.
      CASE
        WHEN f_lo <= 0.0001 THEN f_lo         -- start-of-line within span
        WHEN f_hi >= 0.9999 THEN f_hi         -- end-of-line within span
        ELSE (f_lo + f_hi) / 2.0
      END AS f_marker
    FROM (
      SELECT
        min(ST_LineLocatePoint(nb.eg, dp.geom)) AS f_lo,
        max(ST_LineLocatePoint(nb.eg, dp.geom)) AS f_hi
      FROM ST_DumpPoints(ST_Boundary(pl.pg)) dp
    ) span
  ) gc ON true
),
-- (d2) Fallback nearest-point snap (round-9) for complexes with no platform
--      match. Cap at STATION_SNAP_MAX_M (60 m): beyond the cap keep the
--      conflated position (never drag a marker across the map).
snapped_geom AS (
  SELECT
    cg.gid,
    CASE
      -- platform centre wins when found and within the drift cap (90 m)
      WHEN pc.geom IS NOT NULL AND pc.drift_m <= 90 THEN pc.geom
      -- else nearest-point snap within cap
      WHEN nb.snap IS NULL THEN cg.geom
      WHEN ST_Distance(cg.geom::geography, nb.snap::geography) <= 60 THEN nb.snap
      ELSE cg.geom
    END AS geom
  FROM complex_geom cg
  LEFT JOIN platform_center pc ON pc.gid = cg.gid
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
