#!/usr/bin/env python3
"""Chicago Loop exam (stage 4, phase C) — 'chicago:l-v3' vs the LOOM baseline.

Compares the raster-skeleton build (build_key chicago:l-v3) against the
LOOM baseline (chicago:l) that it replaces, entirely in PostGIS, plus the
mm_edges QA table (Chicago OSM rail ways with tunnel/bridge tags):

  1  no fabricated geometry — every sample point of every elevated-family
     (Brn/P/Org/G/Pink) edge lies within 25 m of a surface/elevated OSM
     rail way (literal, no escape hatch); same probe run on LOOM as the
     before/after receipt (LOOM's Tower 18 over-merge fails it)
  2  subway integrity — Blue/Red Loop-interior corridors hug tunnel
     ways and stay single-route; Red never shares an edge with an
     elevated route; Blue does so only on the Lake leg, where each
     co-attributed edge must prove the Dearborn subway runs beneath it
  3  bundle sanity per Loop leg (Lake/Wabash/Van Buren/Wells) — edge
     route sets match ground truth derived from matched_shapes
  4  station coverage — every Loop-window GTFS parent station labels a
     v3 node within 100 m
  5  junction inventory — degree>=3 nodes with incident route sets;
     Tower 18 must be a junction, mid-block Dearborn must not
  6  visual evidence — side-by-side PNG render + GeoJSON dumps of both
     graphs' Loop windows

Read-only against the graph tables; writes only PNG/GeoJSON artifacts.
Exits non-zero if any check fails. Run:

  uv run --with-requirements linegraph/requirements.txt \
      python linegraph/exam/loop_exam.py [--out data/exam]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)
BUILD = "chicago:l-v3"
BASELINE = "chicago:l"

# elevated Loop + interior subways (same window as tests/test_real_emit.py)
LOOP_WINDOW = (-87.6355, 41.8755, -87.6245, 41.8875)
# strictly inside the legs: only the Dearborn (Blue) / State (Red) subways
LOOP_INTERIOR = (-87.6330, 41.8772, -87.6266, 41.8852)

# elevated-ONLY routes: never in a tunnel anywhere on the system
ELEVATED = ("Brn", "P", "Org", "G", "Pink")
# LOOM edge_lines carry only route_color — CTA hex -> route_id
COLOR_TO_ROUTE = {
    "00a1de": "Blue", "c60c30": "Red", "62361b": "Brn", "009b3a": "G",
    "f9461c": "Org", "522398": "P", "e27ea6": "Pink", "f9e300": "Y",
}
ELEVATED_COLORS = tuple(c for c, r in COLOR_TO_ROUTE.items() if r in ELEVATED)

TOL_M = 25.0          # spec tolerance: every point within 25 m of a way
SAMPLE_M = 10.0       # edge sampling step for point probes
GAP_VERIFY_M = 10.0   # failure diagnostic: QA-table hole vs true fabrication
MERGE_WIDTH_M = 18.0  # build default (--merge-width); fused centerline sits
                      # within one merge width of every source track

# Loop legs, trimmed clear of the corners; a route is "on" a leg when its
# clipped length >= 0.5 x the leg's long dimension (perpendicular subway
# crossings clip to ~the leg's width and fall well under).
LEGS = {
    "Lake": (-87.6332, 41.8852, -87.6270, 41.8862),
    "Wabash": (-87.6266, 41.8778, -87.6256, 41.8848),
    "Van Buren": (-87.6328, 41.8763, -87.6296, 41.8772),
    "Wells": (-87.6344, 41.8780, -87.6334, 41.8845),
}

TOWER18 = (-87.63395, 41.88575)   # Lake/Wells interlocking
DEARBORN_LON = -87.62930          # Dearborn subway alignment
# mid-block band: between the Lake and Van Buren corners, with margin
DEARBORN_MIDBLOCK_LAT = (41.8785, 41.8845)

# tunnel classification: tag present and not no/building_passage
# (building_passage is at-grade — the Orange line threads one at Midway)
IS_TUNNEL = "(m.tags ? 'tunnel' AND m.tags->>'tunnel' NOT IN ('no','building_passage'))"

FAILURES: list[str] = []


def report(check: str, ok: bool, detail: str = "") -> None:
    print(f"  -> {'PASS' if ok else 'FAIL'}{': ' + detail if detail else ''}")
    if not ok:
        FAILURES.append(f"{check}: {detail}")


def envelope(box) -> str:
    w, s, e, n = box
    return f"ST_MakeEnvelope({w}, {s}, {e}, {n}, 4326)"


def elevated_edges_cte(build_key: str) -> str:
    """Edges carrying any elevated-only route, with a route list column."""
    if build_key == BASELINE:  # LOOM rows only have route_color
        colors = ", ".join(f"'{c}'" for c in ELEVATED_COLORS)
        return f"""
          SELECT e.id, e.geom,
                 (SELECT string_agg(DISTINCT lower(el.route_color), ',')
                    FROM transit_graph_edge_lines el WHERE el.edge_id = e.id) AS routes
          FROM transit_graph_edges e
          WHERE e.build_key = '{build_key}' AND EXISTS (
            SELECT 1 FROM transit_graph_edge_lines el
            WHERE el.edge_id = e.id AND lower(el.route_color) IN ({colors}))"""
    routes = ", ".join(f"'{r}'" for r in ELEVATED)
    return f"""
      SELECT e.id, e.geom,
             (SELECT string_agg(DISTINCT el.route_id, ',' ORDER BY el.route_id)
                FROM transit_graph_edge_lines el WHERE el.edge_id = e.id) AS routes
      FROM transit_graph_edges e
      WHERE e.build_key = '{build_key}' AND EXISTS (
        SELECT 1 FROM transit_graph_edge_lines el
        WHERE el.edge_id = e.id AND el.route_id IN ({routes}))"""


def fabrication_probe(cur, build_key: str):
    """Per-edge: points >TOL from every surface way, split into 'rides a
    tunnel alignment' (fabrication) vs 'uncovered' (no reference nearby)."""
    cur.execute(f"""
      WITH target AS ({elevated_edges_cte(build_key)}),
      pts AS (
        SELECT t.id, t.routes,
               (ST_DumpPoints(ST_Segmentize(ST_Transform(t.geom, 32616), {SAMPLE_M}))).geom AS p
        FROM target t
      ),
      scored AS (
        SELECT id, routes, p,
          (SELECT min(ST_Distance(ST_Transform(m.geom, 32616), pts.p)) FROM mm_edges m
            WHERE m.mode = 'rail' AND NOT {IS_TUNNEL}
              AND ST_DWithin(m.geom, ST_Transform(pts.p, 4326), 0.004)) AS d_surface,
          (SELECT min(ST_Distance(ST_Transform(m.geom, 32616), pts.p)) FROM mm_edges m
            WHERE m.mode = 'rail' AND {IS_TUNNEL}
              AND ST_DWithin(m.geom, ST_Transform(pts.p, 4326), 0.004)) AS d_tunnel
        FROM pts
      )
      SELECT id, routes, count(*) AS n_pts,
        count(*) FILTER (WHERE (d_surface IS NULL OR d_surface > {TOL_M})
                           AND d_tunnel <= {TOL_M}) AS n_rides_tunnel,
        count(*) FILTER (WHERE (d_surface IS NULL OR d_surface > {TOL_M})
                           AND (d_tunnel IS NULL OR d_tunnel > {TOL_M})) AS n_uncovered,
        ST_AsText(ST_Centroid(ST_Collect(ST_Transform(p, 4326))), 5) AS center
      FROM scored GROUP BY id, routes ORDER BY id""")
    return cur.fetchall()


def check1_no_fabricated_geometry(cur) -> None:
    print(f"\nCHECK 1 — no fabricated geometry (Tower 18 kill-shot), tol {TOL_M:.0f} m")
    print(f"  elevated-only routes {list(ELEVATED)}: every {SAMPLE_M:.0f} m-sample point"
          " within tol of a non-tunnel OSM rail way")

    results = {}
    for key in (BUILD, BASELINE):
        rows = fabrication_probe(cur, key)
        riders = [r for r in rows if r[3] > 0]
        uncovered = [r for r in rows if r[4] > 0]
        results[key] = (rows, riders, uncovered)
        print(f"  [{key}] {len(rows)} elevated-family edges: "
              f"{len(riders)} ride a tunnel alignment, {len(uncovered)} uncovered")
        for eid, routes, n_pts, n_rt, n_unc, center in rows:
            if not n_rt and not n_unc:
                continue
            kind = (["RIDES-TUNNEL"] if n_rt else []) + (["uncovered"] if n_unc else [])
            print(f"    edge {eid} [{routes}] {'+'.join(kind)}: "
                  f"{n_rt + n_unc}/{n_pts} pts @ {center}")

    # v3 kill-shot: zero points on tunnel alignments
    v3_riders = results[BUILD][1]
    report("check1.v3-fabrication", not v3_riders,
           f"{len(v3_riders)} v3 elevated edges ride a subway alignment"
           if v3_riders else "0 v3 elevated edges ride a subway alignment")

    # literal spec assertion: zero uncovered points — mm_edges holes are
    # graph-crop bugs (the Linden terminal sat north of the old 42.07 bbox
    # edge), fixed by widening config/regions.json + re-dumping mm_edges
    # (shapesnap.graph --postgis), never by relaxing the exam. On failure,
    # measure the span against the route's OWN matched shape to tell a
    # reference-data hole (hugs it) from true fabrication (doesn't).
    v3_uncovered = results[BUILD][2]
    for eid, routes, _, _, n_unc, _ in v3_uncovered:
        cur.execute(f"""
          WITH pts AS (
            SELECT (ST_DumpPoints(ST_Segmentize(ST_Transform(e.geom, 32616), {SAMPLE_M}))).geom AS p
            FROM transit_graph_edges e WHERE e.id = %s
          ),
          unc AS (
            SELECT p FROM pts
            WHERE NOT EXISTS (SELECT 1 FROM mm_edges m
              WHERE m.mode = 'rail' AND NOT {IS_TUNNEL}
                AND ST_DWithin(m.geom, ST_Transform(pts.p, 4326), 0.004)
                AND ST_Distance(ST_Transform(m.geom, 32616), pts.p) <= {TOL_M})
          )
          SELECT max((SELECT min(ST_Distance(ST_Transform(ms.geom, 32616), unc.p))
                      FROM matched_shapes ms
                      WHERE ms.feed_id = '29' AND ms.route_id = ANY(%s)))
          FROM unc""", (eid, routes.split(",")))
        worst = cur.fetchone()[0]
        print(f"    edge {eid} [{routes}] uncovered span sits "
              f"{'?' if worst is None else f'{worst:.1f} m'} from its own matched "
              f"shape ({'reference-data hole — fix the graph bbox' if worst is not None and worst <= GAP_VERIFY_M else 'FABRICATED GEOMETRY'})")
    report("check1.v3-coverage", not v3_uncovered,
           f"{len(v3_uncovered)} v3 elevated edges have points >{TOL_M:.0f} m from "
           "every surface rail way" if v3_uncovered else
           "every v3 elevated sample point within tol of a surface rail way")

    # the receipt: LOOM's over-merge fails the same probe
    loom_riders = results[BASELINE][1]
    report("check1.loom-receipt", len(loom_riders) > 0,
           f"LOOM baseline: {len(loom_riders)} elevated edges ride the Dearborn/State "
           f"subway alignments ({len(results[BUILD][1])} in v3)")


def check2_subway_integrity(cur) -> None:
    print(f"\nCHECK 2 — subway integrity (Loop interior {LOOP_INTERIOR})")
    cur.execute(f"""
      WITH corridor AS (
        SELECT e.id,
          (SELECT string_agg(DISTINCT el.route_id, ',' ORDER BY el.route_id)
             FROM transit_graph_edge_lines el WHERE el.edge_id = e.id) AS routes,
          ST_Length(ST_Transform(ST_Intersection(e.geom, {envelope(LOOP_INTERIOR)}), 32616)) AS len_in,
          e.geom
        FROM transit_graph_edges e
        WHERE e.build_key = '{BUILD}' AND ST_Intersects(e.geom, {envelope(LOOP_INTERIOR)})
      )
      SELECT c.id, c.routes, round(c.len_in::numeric, 0),
        (SELECT max(sub.d) FROM (
           SELECT (SELECT min(ST_Distance(ST_Transform(m.geom, 32616), p.geom))
                   FROM mm_edges m
                   WHERE m.mode = 'rail' AND {IS_TUNNEL}
                     AND ST_DWithin(m.geom, ST_Transform(p.geom, 4326), 0.004)) AS d
           FROM (SELECT (ST_DumpPoints(ST_Segmentize(ST_Transform(
                   ST_Intersection(c.geom, {envelope(LOOP_INTERIOR)}), 32616), {SAMPLE_M}))).geom) p(geom)
        ) sub) AS worst_tunnel_m
      FROM corridor c WHERE c.len_in > 30 ORDER BY c.routes, c.id""")
    rows = cur.fetchall()
    sets = {r[1] for r in rows}
    for eid, routes, len_in, worst in rows:
        print(f"  interior edge {eid} [{routes}] {len_in} m in-window, "
              f"worst point {worst:.1f} m from a tunnel way")
    report("check2.interior-pure", sets == {"Blue", "Red"} and len(rows) >= 2,
           f"interior corridor route sets = {sorted(sets)} (want pure Blue + pure Red)")
    worst_all = max(r[3] for r in rows) if rows else float("inf")
    report("check2.interior-tunnel", worst_all <= TOL_M,
           f"worst interior point {worst_all:.1f} m from a tunnel way (tol {TOL_M:.0f})")

    cur.execute(f"""
      SELECT count(*) FROM transit_graph_edges e
      WHERE e.build_key = '{BUILD}'
        AND EXISTS (SELECT 1 FROM transit_graph_edge_lines el
                    WHERE el.edge_id = e.id AND el.route_id = 'Blue')
        AND EXISTS (SELECT 1 FROM transit_graph_edge_lines el
                    WHERE el.edge_id = e.id AND el.route_id = 'Red')""")
    n_bluered = cur.fetchone()[0]
    report("check2.blue-red-distinct", n_bluered == 0,
           f"{n_bluered} edges carry both Blue and Red (build-wide)")

    # subway/elevated co-attribution in the Loop window: Red never; Blue only
    # on the Lake leg, where the Milwaukee–Dearborn Blue subway genuinely runs
    # beneath the Lake elevated (plan-view parallel within MERGE_WIDTH fuses,
    # by design — see tests/test_real_emit.py; spec amendment in
    # docs/transit-pipeline-v3.md). Never on the Dearborn/State interior, and
    # each allowed edge must PROVE the subway is beneath it (below).
    cur.execute(f"""
      SELECT e.id,
        (SELECT string_agg(DISTINCT el.route_id, ',' ORDER BY el.route_id)
           FROM transit_graph_edge_lines el WHERE el.edge_id = e.id),
        ST_YMin(Box2D(e.geom)),
        EXISTS (SELECT 1 FROM transit_graph_edge_lines el
                WHERE el.edge_id = e.id AND el.route_id = 'Red')
      FROM transit_graph_edges e
      WHERE e.build_key = '{BUILD}' AND e.geom && {envelope(LOOP_WINDOW)}
        AND EXISTS (SELECT 1 FROM transit_graph_edge_lines el
                    WHERE el.edge_id = e.id AND el.route_id IN ('Blue', 'Red'))
        AND EXISTS (SELECT 1 FROM transit_graph_edge_lines el
                    WHERE el.edge_id = e.id
                      AND el.route_id IN {ELEVATED})""")
    co = cur.fetchall()
    red_co = [r for r in co if r[3]]
    off_lake = [r for r in co if not r[3] and r[2] < 41.8845]
    for eid, routes, min_lat, _ in co:
        print(f"  co-attributed window edge {eid} [{routes}] min_lat {min_lat:.5f}")
    report("check2.red-never-coattributed", not red_co,
           f"{len(red_co)} window edges carry Red + an elevated route")
    report("check2.blue-coattribution-lake-only", not off_lake,
           f"{len(co) - len(red_co)} Blue+elevated edges, all on the Lake leg "
           f"(subway under the elevated)" if not off_lake else
           f"{len(off_lake)} Blue+elevated edges stray off the Lake leg")

    # the allowance is physical, not geographic: each Blue+elevated edge must
    # sit over the Blue subway — a tunnel-tagged rail way within TOL_M of
    # every sample point, and Blue's OWN matched shape within one merge width
    # of the centerline (Blue is inside the fused stroke, not crossing bleed)
    for eid, routes, _, is_red in co:
        if is_red:
            continue
        cur.execute(f"""
          WITH pts AS (
            SELECT (ST_DumpPoints(ST_Segmentize(ST_Transform(e.geom, 32616), {SAMPLE_M}))).geom AS p
            FROM transit_graph_edges e WHERE e.id = %s
          )
          SELECT
            max((SELECT min(ST_Distance(ST_Transform(m.geom, 32616), pts.p))
                 FROM mm_edges m
                 WHERE m.mode = 'rail' AND {IS_TUNNEL}
                   AND ST_DWithin(m.geom, ST_Transform(pts.p, 4326), 0.004))),
            max((SELECT min(ST_Distance(ST_Transform(ms.geom, 32616), pts.p))
                 FROM matched_shapes ms
                 WHERE ms.feed_id = '29' AND ms.route_id = 'Blue'))
          FROM pts""", (eid,))
        worst_tunnel, worst_blue = cur.fetchone()
        ok = (worst_tunnel is not None and worst_tunnel <= TOL_M
              and worst_blue is not None and worst_blue <= MERGE_WIDTH_M)
        fmt = lambda v: "no way found" if v is None else f"{v:.1f} m"
        report("check2.blue-subway-beneath", ok,
               f"edge {eid} [{routes}]: tunnel way within {fmt(worst_tunnel)} of "
               f"every point (tol {TOL_M:.0f}), Blue's matched shape within "
               f"{fmt(worst_blue)} (tol {MERGE_WIDTH_M:.0f})")


def check3_leg_bundles(cur) -> None:
    print("\nCHECK 3 — bundle sanity per Loop leg (truth = matched_shapes traversals)")
    for name, box in LEGS.items():
        cur.execute(f"""
          WITH box AS (SELECT {envelope(box)} AS g),
          lm AS (
            SELECT GREATEST(ST_XMax(t.b) - ST_XMin(t.b), ST_YMax(t.b) - ST_YMin(t.b)) AS long_m
            FROM box, LATERAL (SELECT Box2D(ST_Transform(box.g, 32616)) AS b) t
          ),
          edge_side AS (
            SELECT el.route_id,
                   sum(ST_Length(ST_Transform(ST_Intersection(e.geom, box.g), 32616))) AS len_m
            FROM box, transit_graph_edges e
            JOIN transit_graph_edge_lines el ON el.edge_id = e.id
            WHERE e.build_key = '{BUILD}' AND e.geom && box.g
            GROUP BY el.route_id
          ),
          truth_side AS (
            SELECT ms.route_id,
                   ST_Length(ST_Transform(ST_Intersection(ST_Union(ms.geom), box.g), 32616)) AS len_m
            FROM box, matched_shapes ms
            WHERE ms.feed_id = '29' AND ms.geom && box.g
            GROUP BY ms.route_id, box.g
          )
          SELECT (SELECT round(long_m::numeric, 0) FROM lm),
            (SELECT array_agg(route_id ORDER BY route_id) FROM edge_side
              WHERE len_m >= 0.5 * (SELECT long_m FROM lm)),
            (SELECT array_agg(route_id ORDER BY route_id) FROM truth_side
              WHERE len_m >= 0.5 * (SELECT long_m FROM lm)),
            (SELECT string_agg(route_id || ':' || round(len_m::numeric, 0), ',' ORDER BY route_id)
               FROM edge_side WHERE len_m < 0.5 * (SELECT long_m FROM lm)),
            (SELECT string_agg(route_id || ':' || round(len_m::numeric, 0), ',' ORDER BY route_id)
               FROM truth_side WHERE len_m < 0.5 * (SELECT long_m FROM lm))""")
        long_m, edge_set, truth_set, edge_sub, truth_sub = cur.fetchone()
        edge_set, truth_set = edge_set or [], truth_set or []
        print(f"  {name} ({long_m} m): edges {{{','.join(edge_set)}}} "
              f"vs shapes {{{','.join(truth_set)}}}"
              + (f"  [sub-threshold edges: {edge_sub}]" if edge_sub else "")
              + (f" [sub-threshold shapes: {truth_sub}]" if truth_sub else ""))
        report(f"check3.{name}-match", edge_set == truth_set and edge_set != [],
               f"{name}: edge routes == matched-shape routes")
        report(f"check3.{name}-count", 2 <= len(edge_set) <= 6,
               f"{name}: {len(edge_set)} routes (want 2..6)")


def check4_station_coverage(cur) -> None:
    print("\nCHECK 4 — station coverage (Loop-window GTFS parent stations, 100 m)")
    cur.execute(f"""
      SELECT s.stop_id, s.stop_name,
        (SELECT round(min(ST_Distance(n.geom::geography, s.geom::geography))::numeric, 1)
         FROM transit_graph_nodes n
         WHERE n.build_key = '{BUILD}' AND n.station_id = s.stop_id) AS dist_m
      FROM gtfs_stops s
      WHERE s.feed_id = '29' AND s.location_type = 1
        AND s.geom && {envelope(LOOP_WINDOW)}
      ORDER BY s.stop_name""")
    rows = cur.fetchall()
    for stop_id, name, dist in rows:
        print(f"  {stop_id} {name}: "
              + (f"labeled node {dist} m away" if dist is not None else "NO LABELED NODE"))
    missing = [r for r in rows if r[2] is None or r[2] > 100.0]
    report("check4.stations", rows != [] and not missing,
           f"{len(rows) - len(missing)}/{len(rows)} Loop-window stations labeled within "
           f"100 m (feed omits State/Lake — closed for reconstruction)")


def junctions(cur, build_key: str, tol_deg: float):
    cur.execute(f"""
      WITH ends AS (
        SELECT e.id AS edge_id, ST_StartPoint(e.geom) AS p
        FROM transit_graph_edges e WHERE e.build_key = '{build_key}'
        UNION ALL
        SELECT e.id, ST_EndPoint(e.geom)
        FROM transit_graph_edges e WHERE e.build_key = '{build_key}'
      )
      SELECT n.loom_id, n.station_label, ST_X(n.geom), ST_Y(n.geom),
             count(en.edge_id) AS degree, array_agg(en.edge_id) AS edge_ids
      FROM transit_graph_nodes n
      LEFT JOIN ends en ON ST_DWithin(n.geom, en.p, {tol_deg})
      WHERE n.build_key = '{build_key}' AND n.geom && {envelope(LOOP_WINDOW)}
      GROUP BY n.id, n.loom_id, n.station_label, n.geom
      HAVING count(en.edge_id) >= 3
      ORDER BY count(en.edge_id) DESC, n.loom_id""")
    return cur.fetchall()


def check5_junction_inventory(cur) -> None:
    print("\nCHECK 5 — v3 junction inventory (degree >= 3) in the Loop window")
    rows = junctions(cur, BUILD, 0.0000015)  # v3 endpoints match nodes exactly
    for loom_id, label, lon, lat, degree, edge_ids in rows:
        cur.execute(
            """SELECT string_agg(DISTINCT el.route_id, ',' ORDER BY el.route_id)
               FROM transit_graph_edge_lines el WHERE el.edge_id = ANY(%s)""",
            (edge_ids,))
        routes = cur.fetchone()[0]
        print(f"  node {loom_id} deg={degree} ({lon:.5f}, {lat:.5f})"
              f"{' [' + label + ']' if label else ''} routes {{{routes}}}")

    # Way-graph-era calibration (PAR-12 v3 stage-4 rebuild): junctions are
    # the REAL switch nodes, and Tower 18 is a grand-union interlocking —
    # a ladder of switches spread over ~60 m, not one point. The raster
    # skeleton collapsed it to a single blob node (the old `== 1` pin);
    # the exact build must show the interlocking CORE (a degree>=4 node
    # where the Lake and Wells bundles meet) plus a small switch cluster,
    # and nothing more — a smeared/duplicated junction would inflate the
    # cluster beyond any physical ladder.
    t18 = [r for r in rows
           if abs(r[2] - TOWER18[0]) < 0.0005 and abs(r[3] - TOWER18[1]) < 0.0005]
    core = [r for r in t18 if r[4] >= 4]
    report("check5.tower18", 1 <= len(t18) <= 6 and len(core) >= 1,
           f"Tower 18 (Lake/Wells) interlocking: {len(t18)} switch node(s), "
           f"core {core[0][0] if core else '—'} deg={core[0][4] if core else 0}")

    lat_lo, lat_hi = DEARBORN_MIDBLOCK_LAT
    midblock = [r for r in rows
                if abs(r[2] - DEARBORN_LON) < 0.00042 and lat_lo < r[3] < lat_hi]
    report("check5.no-midblock-dearborn", not midblock,
           "no junction mid-block on the Dearborn subway"
           if not midblock else f"{len(midblock)} mid-block Dearborn junctions: "
           f"{[r[0] for r in midblock]}")

    # contrast probe (not asserted): the LOOM baseline's mid-block junctions
    loom = junctions(cur, BASELINE, 0.00001)
    loom_mid = [r for r in loom
                if abs(r[2] - DEARBORN_LON) < 0.00042 and lat_lo < r[3] < lat_hi]
    print(f"  [contrast] LOOM baseline: {len(loom)} window junctions, "
          f"{len(loom_mid)} mid-block on Dearborn "
          f"{[(r[0], round(r[3], 5)) for r in loom_mid]}")


def fetch_window_features(cur, build_key: str, pad: float = 0.0012):
    w, s, e, n = LOOP_WINDOW
    box = (w - pad, s - pad, e + pad, n + pad)
    route_col = ("string_agg(DISTINCT lower(el.route_color), ',')"
                 if build_key == BASELINE else
                 "string_agg(DISTINCT el.route_id, ',' ORDER BY el.route_id)")
    cur.execute(f"""
      SELECT e.id, e.loom_id, e.line_count, ST_AsGeoJSON(e.geom, 6),
        (SELECT {route_col} FROM transit_graph_edge_lines el WHERE el.edge_id = e.id)
      FROM transit_graph_edges e
      WHERE e.build_key = '{build_key}' AND e.geom && {envelope(box)}""")
    edges = cur.fetchall()
    cur.execute(f"""
      SELECT n.loom_id, n.station_label, ST_X(n.geom), ST_Y(n.geom)
      FROM transit_graph_nodes n
      WHERE n.build_key = '{build_key}' AND n.geom && {envelope(box)}""")
    nodes = cur.fetchall()
    juncs = junctions(cur, build_key, 0.0000015 if build_key == BUILD else 0.00001)
    return edges, nodes, juncs


def geojson_dump(edges, nodes, juncs, build_key: str, path: Path) -> None:
    feats = [
        {"type": "Feature",
         "geometry": json.loads(gj),
         "properties": {"build": build_key, "edge_id": eid, "loom_id": loom_id,
                        "line_count": lc, "routes": routes}}
        for eid, loom_id, lc, gj, routes in edges
    ]
    junc_ids = {j[0] for j in juncs}
    feats += [
        {"type": "Feature",
         "geometry": {"type": "Point", "coordinates": [lon, lat]},
         "properties": {"build": build_key, "loom_id": loom_id,
                        "station_label": label,
                        "junction": loom_id in junc_ids}}
        for loom_id, label, lon, lat in nodes
    ]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    print(f"  wrote {path} ({len(feats)} features)")


def render(cur, out_dir: Path) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    w, s, e, n = LOOP_WINDOW
    fig, axes = plt.subplots(1, 2, figsize=(20, 11), dpi=100)
    cmap = plt.get_cmap("viridis")

    for ax, key in zip(axes, (BUILD, BASELINE)):
        edges, nodes, juncs = fetch_window_features(cur, key)
        geojson_dump(edges, nodes, juncs, key,
                     out_dir / f"loop-{key.replace(':', '-')}.geojson")
        max_lc = max((lc for _, _, lc, _, _ in edges), default=1)
        for _, _, lc, gj, _ in edges:
            coords = json.loads(gj)["coordinates"]
            xs, ys = zip(*coords)
            ax.plot(xs, ys, color=cmap((lc - 1) / max(max_lc - 1, 1)),
                    linewidth=1.0 + 1.1 * lc, solid_capstyle="round", zorder=2)
        for loom_id, label, lon, lat in nodes:
            if label:
                ax.plot(lon, lat, "s", color="white", markeredgecolor="black",
                        markersize=5, zorder=4)
        for loom_id, label, lon, lat, deg, _ in juncs:
            ax.plot(lon, lat, "o", color="black", markersize=9, zorder=5)
        ax.set_xlim(w - 0.0012, e + 0.0012)
        ax.set_ylim(s - 0.0012, n + 0.0012)
        ax.set_aspect(1 / 0.745)  # cos(41.88 deg)
        ax.set_title(f"{key} — {len(edges)} edges, {len(juncs)} junctions "
                     f"(Loop window)", fontsize=13)
        ax.tick_params(labelsize=7)
        handles = [Line2D([], [], color=cmap((c - 1) / max(max_lc - 1, 1)),
                          linewidth=1.0 + 1.1 * c, label=f"{c} line{'s' if c > 1 else ''}")
                   for c in sorted({lc for _, _, lc, _, _ in edges})]
        handles += [Line2D([], [], marker="o", linestyle="", color="black",
                           markersize=9, label="junction (deg>=3)"),
                    Line2D([], [], marker="s", linestyle="", color="white",
                           markeredgecolor="black", markersize=5, label="station node")]
        ax.legend(handles=handles, loc="lower right", fontsize=8)

    fig.suptitle("Chicago Loop — raster-skeleton v3 vs LOOM baseline, "
                 "edges colored by line_count", fontsize=15)
    fig.tight_layout()
    png = out_dir / "loop-v3-vs-loom.png"
    fig.savefig(png)
    plt.close(fig)
    return png


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dsn", default=DSN)
    ap.add_argument("--out", default="data/exam", help="artifact directory")
    ap.add_argument("--no-render", action="store_true",
                    help="skip PNG/GeoJSON artifacts (checks only)")
    args = ap.parse_args()

    import psycopg
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT build_key, count(*) FROM transit_graph_edges "
            "WHERE build_key IN (%s, %s) GROUP BY 1 ORDER BY 1", (BUILD, BASELINE))
        counts = dict(cur.fetchall())
        print(f"Loop exam: {BUILD} ({counts.get(BUILD, 0)} edges) vs "
              f"{BASELINE} ({counts.get(BASELINE, 0)} edges)")
        if not counts.get(BUILD) or not counts.get(BASELINE):
            print("missing build(s) — aborting"); return 2

        check1_no_fabricated_geometry(cur)
        check2_subway_integrity(cur)
        check3_leg_bundles(cur)
        check4_station_coverage(cur)
        check5_junction_inventory(cur)

        if not args.no_render:
            print("\nCHECK 6 — visual evidence")
            png = render(cur, out_dir)
            print(f"  wrote {png}")

    print(f"\n{'=' * 60}")
    if FAILURES:
        print(f"LOOP EXAM: {len(FAILURES)} FAILURE(S)")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("LOOP EXAM: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
