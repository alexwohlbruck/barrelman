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
-- CENTERING — the marker sits at the CENTROID of the station's OSM platform
-- geometry (as OSM-Carto places the station name), snapped onto the rendered
-- ribbon. We collect this complex's own platform ways (transit_platforms —
-- railway=platform geometry within STATION_PLATFORM_M of the complex), take
-- ST_Centroid, then snap that centroid perpendicular onto the nearest edge so
-- the dot rides the drawn line. For island/side platform pairs the centroid
-- lands in the track centre between them, then on the ribbon.
--   • TERMINAL: at a route end the terminal platform's projection lands near a
--     degree-1 line end (an edge endpoint touched by only one edge), so we clamp
--     the marker to that tip — the last stop sits at the very end of the line.
--     Junctions (degree >= 2) are excluded so transfers never jump to a vertex.
--   • FALLBACK: complexes with no matching platform (all of Chicago today has
--     no OSM railway=platform geometry in geo_places) keep the round-9
--     nearest-point snap, capped at STATION_SNAP_MAX_M.
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
-- Materialize the ~2.2k rail platforms ONCE. transit_platforms is a VIEW over
-- geo_places (21.7M rows); a correlated LATERAL over the view re-scans geo_places
-- per station (600×) — that is the platform-centering matview-build runaway. One
-- materialized pass + a tiny per-station distance scan over 2.2k rows is the fix.
WITH platforms AS MATERIALIZED (
  SELECT geom
  FROM transit_platforms
  WHERE public_transport = 'platform' OR railway = 'platform'
),
-- (a) Every rendered station node, tagged with its GTFS parent id (station_id).
station_nodes AS (
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
    sn.id, sn.geom, sn.station_label, sn.feed_id, sn.station_id,
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
-- Every (complex, platform stop, route) with its service counts. Bullets show
-- ONLY the lines that stop AT this station — the routes of the parent_station's
-- OWN child platform stops (gtfs_stops.parent_station = the parent's stop_id),
-- NOT a spatial match that would sweep in adjacent stations' routes (the 4
-- separate Fulton St stations sit within 150 m of each other, so a radius match
-- oversampled every one with all of A/C + 2/3 + 4/5 + J/Z). Connections to
-- nearby stations belong on the station detail card, not the label bullets.
-- Fallback (feeds with no parent hierarchy): the node's own matched stop.
-- DISTINCT so a stop reached by several nodes of the same complex counts once.
-- Bullet label: prefer route_short_name, fall back to route_id for agencies
-- that leave short_name blank (e.g. CTA's 'Red'/'Brn'/'Org' L lines).
complex_stop_routes AS (
  SELECT DISTINCT
    m.gid,
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
  FROM (
    -- Member platform stops of each complex. Split into two index-friendly
    -- branches (an OR across parent_station/stop_id can't use an index and
    -- seq-scanned all of gtfs_stops per node):
    --   parent-keyed → the parent_station's child platforms
    --     (gtfs_stops_feed_parent_idx);
    --   fallback (no parent metadata) → the node's own matched stop.
    SELECT ng.gid, ng.feed_id, cs.stop_id
    FROM node_parent ng
    JOIN gtfs_stops cs
      ON cs.feed_id = ng.feed_id AND cs.parent_station = ng.station_id
    WHERE ng.has_parent
    UNION
    SELECT ng.gid, ng.feed_id, ng.station_id
    FROM node_parent ng
    WHERE NOT ng.has_parent
  ) m
  JOIN gtfs_stop_routes sr
    ON sr.feed_id = m.feed_id AND sr.stop_id = m.stop_id
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
-- (d0) TRUE LINE TERMINI — edge endpoints touched by exactly one edge end
--      (graph degree 1). A terminal station's marker snaps to one of these so it
--      sits at the very tip of the ribbon; junctions (degree >= 2) are excluded
--      so a transfer station never jumps to an interior corridor vertex.
line_ends AS (
  SELECT (array_agg(pt))[1] AS pt
  FROM (
    SELECT ST_StartPoint(geom) AS pt FROM transit_graph_edges
      WHERE build_key IN ('chicago:l-v3', 'nyc:subway-v3')
    UNION ALL
    SELECT ST_EndPoint(geom)   AS pt FROM transit_graph_edges
      WHERE build_key IN ('chicago:l-v3', 'nyc:subway-v3')
  ) ep
  GROUP BY ST_SnapToGrid(pt, 0.00001)
  HAVING count(*) = 1
),
-- (d1) PLATFORM CENTERING (OSM-Carto style, snapped to the ribbon). Take the
--      centroid of this complex's own OSM platform ways, then snap it
--      perpendicular onto the nearest rendered edge so the dot rides the drawn
--      line. At a route end the terminal platform's projection lands near a
--      degree-1 line end, so clamp to that tip (last stop sits at the very end).
platform_center AS (
  SELECT
    cg.gid,
    sp.snapped AS geom,
    ST_Distance(sp.snapped::geography, cg.geom::geography) AS drift_m
  FROM complex_geom cg
  -- (1) centroid of this complex's own platform ways. Tight planar radius
  --     (~0.0007 deg ≈ 60-75 m); the conflated stop already sits on its platform
  --     so this grabs just this station without pulling an adjacent complex.
  --     Planar (no ::geography) keeps the scan over materialized `platforms`
  --     cheap; drift_m below is the only ellipsoidal measure, once per complex.
  JOIN LATERAL (
    SELECT ST_Centroid(ST_Collect(p.geom)) AS c
    FROM platforms p
    WHERE ST_DWithin(p.geom, cg.geom, 0.0007)
  ) pc ON pc.c IS NOT NULL
  -- (2) nearest rendered ribbon centreline to that centroid (gist KNN).
  JOIN LATERAL (
    SELECT e.geom AS eg
    FROM transit_graph_edges e
    WHERE e.build_key IN ('chicago:l-v3', 'nyc:subway-v3')
    ORDER BY e.geom <-> pc.c
    LIMIT 1
  ) nb ON true
  -- (3) perpendicular snap onto that edge; if the projection lands within
  --     ~0.001 deg (~85-110 m) of a true line terminus, clamp to the tip.
  JOIN LATERAL (
    SELECT COALESCE(le.pt, q.proj) AS snapped
    FROM (SELECT ST_ClosestPoint(nb.eg, pc.c) AS proj) q
    LEFT JOIN LATERAL (
      SELECT pt FROM line_ends le
      WHERE ST_DWithin(le.pt, q.proj, 0.001)
      ORDER BY le.pt <-> q.proj
      LIMIT 1
    ) le ON true
  ) sp ON true
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
