#!/usr/bin/env python3
"""NYC acceptance exam (milestone 5 — feed 5 through the v3 pipeline).

Validates the nyc:subway-v3 build against the NYC-specific contracts
Chicago could not test — colour collapse on trunk corridors and the
DeKalb junction complex — on top of the generic checks the companion
exams already enforce:

  1  BROADWAY YELLOW TRUNK — between Times Sq and Canal St the
     N/Q/R/W family (route_color F6BC26 in feed 5; the MTA's nominal
     FCCC0A after display normalization) renders as ONE ribbon:
     perpendicular cross-sections along Broadway each hit exactly one
     yellow feature whose route list is a co-running subset of
     {N,Q,R,W}; a window-wide sweep asserts no two distinct yellow
     steady features run side-by-side (parallel within 20 m for
     longer than 100 m) — the two-parallel-centerlines failure mode
     the raster stage's MERGE_WIDTH fusion exists to prevent
  2  TRUNK FAMILY CHECK — cross-sections through the major trunks
     (Broadway N/Q/R/W, 7th Av 1/2/3, 8th Av A/C/E, Lexington 4/5/6,
     Queens Blvd E + F/M + R) print the per-trunk ribbon table and
     assert each single-colour family is exactly one ribbon, with the
     expected co-running route set
  3  DEKALB JUNCTION — the transition inventory of the DeKalb Av /
     Flatbush Av complex: every transition feature in the window is
     anchored to graph junctions / composition-change nodes (the C1
     human receipt), meets its fillet floor measured on the emitted
     DB geometry, and no emitted row in the window self-intersects

Companion commands (both must also pass for the milestone):

  uv run --with-requirements lineorder/requirements.txt \
      python lineorder/exam/stability_exam.py --build-key nyc:subway-v3
  uv run --with-requirements segments/requirements.txt \
      python segments/exam/segments_exam.py --build-key nyc:subway-v3

Read-only. Exits non-zero if any check fails. Run:

  uv run --with-requirements segments/requirements.txt \
      python segments/exam/nyc_exam.py
"""

from __future__ import annotations

import math
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.build import load_shapes                     # noqa: E402
from segments.corridors import DEFAULT_DSN, load_graph     # noqa: E402
from segments.segment import (LocalProj, SegmentConfig,    # noqa: E402
                              _circumradius, build_segments,
                              transition_sites)

BUILD = "nyc:subway-v3"
CFG = SegmentConfig()
# receipts run against the z15 band (the default transition length);
# the in-memory rebuild below uses CFG.transition_len_m which IS that
# band's length, and the emitted-row queries filter band_minzoom to it
DEFAULT_BAND = max(mz for mz, _ in CFG.bands)

# Broadway trunk window, Times Sq (-73.987, 40.755) -> Canal St
# (-74.001, 40.719), padded to hold the full corridor
BROADWAY_WINDOW = (-74.005, 40.716, -73.983, 40.757)
# DeKalb Av / Flatbush Av junction complex
DEKALB_WINDOW = (-73.990, 40.683, -73.9745, 40.697)

# Broadway cross-section points (mid-block / station, away from the
# junction transition zones)
BROADWAY_POINTS = [
    ("38 St (Times Sq approach)", (-73.9868, 40.7524)),
    ("34 St-Herald Sq",           (-73.9879, 40.7496)),
    ("28 St",                     (-73.9885, 40.7454)),
    ("18 St mid-block",           (-73.9899, 40.7392)),
    ("8 St-NYU",                  (-73.9925, 40.7304)),
    ("Prince St",                 (-73.9977, 40.7241)),
]

# trunk -> (cross-section point, {family route set, ...}); each family
# must collapse to EXACTLY one ribbon at the cross-section. Colours are
# resolved from the build's own route attributes, not hardcoded.
TRUNKS = [
    ("Broadway",     (-73.9885, 40.7454), [{"N", "Q", "R", "W"}]),
    ("7th Ave",      (-73.9977, 40.7409), [{"1", "2", "3"}]),
    ("8th Ave",      (-73.9979, 40.7459), [{"A", "C", "E"}]),
    ("Lexington Av", (-73.9822, 40.7462), [{"4", "5", "6", "6X"}]),
    ("Queens Blvd",  (-73.8823, 40.7423), [{"E"}, {"F", "FX", "M"},
                                           {"R"}]),
]

YELLOW_ROUTES = {"N", "Q", "R", "W"}
XSECT_HALF_M = 30.0          # cross-section half length
PARALLEL_BUF_M = 20.0        # side-by-side sweep: proximity ...
PARALLEL_MAX_M = 100.0       # ... and max tolerated parallel length

FAILURES: list[str] = []


def report(check: str, ok: bool, detail: str = "") -> None:
    print(f"  -> {'PASS' if ok else 'FAIL'}{': ' + detail if detail else ''}")
    if not ok:
        FAILURES.append(f"{check}: {detail}")


def rebuild():
    g = load_graph(BUILD, DEFAULT_DSN)
    shapes = load_shapes(g, DEFAULT_DSN)
    segments, info = build_segments(g, CFG, shapes=shapes)
    lon0 = sum(n.lon for n in g.nodes.values()) / len(g.nodes)
    lat0 = sum(n.lat for n in g.nodes.values()) / len(g.nodes)
    proj = LocalProj(lon0, lat0)
    for s in segments:
        s.xy = proj.to_xy(s.coords)
    return g, proj, segments


def family_color(g, routes: set) -> str:
    """The single color_key shared by a route family — colour collapse
    is only meaningful if the family shares one key."""
    keys = {ln.color_key for e in g.edges.values() for ln in e.lines
            if ln.short_name in routes}
    if len(keys) != 1:
        raise AssertionError(f"family {sorted(routes)} spans colour "
                             f"keys {sorted(keys)}")
    return keys.pop()


def window_geom(proj, window):
    from shapely.geometry import box
    w, s, e, n = window
    (x0, y0), (x1, y1) = proj.to_xy([(w, s), (e, n)])
    return box(x0, y0, x1, y1)


def cross_section(proj, segments, pt_ll, colors=None, half=XSECT_HALF_M):
    """Perpendicular cross-line at pt: centred on the nearest feature
    centerline, aligned to its local tangent. Returns (hits, line) —
    features (optionally colour-filtered) intersecting the cross-line.
    Ribbons of a bundle share the corridor centerline geometry, so one
    hit == one ribbon."""
    from shapely.geometry import LineString, Point

    pt = Point(proj.to_xy([pt_ll])[0])
    pool = [s for s in segments
            if colors is None or s.color_key in colors]
    lines = [(s, LineString(s.xy)) for s in pool
             if Point(s.xy[0]).distance(pt) < 5000]  # cheap prefilter
    near = [(ls.distance(pt), s, ls) for s, ls in lines
            if ls.distance(pt) <= 100.0]
    if not near:
        return [], None
    _, s0, ls0 = min(near, key=lambda t: t[0])
    d = ls0.project(pt)
    a = ls0.interpolate(max(0.0, d - 5.0))
    b = ls0.interpolate(min(ls0.length, d + 5.0))
    t = (b.x - a.x, b.y - a.y)
    tl = math.hypot(*t) or 1.0
    nx, ny = -t[1] / tl, t[0] / tl
    c = ls0.interpolate(d)
    xline = LineString([(c.x - nx * half, c.y - ny * half),
                        (c.x + nx * half, c.y + ny * half)])
    hits = [s for s, ls in lines if ls.intersects(xline)]
    return hits, xline


def fmt_off(v):
    return "  --  " if v is None else f"{v:+6.1f}"


def seg_off(s):
    if s.kind == "steady":
        return f"offset {fmt_off(s.offset_px)} px"
    return f"offset {fmt_off(s.off_from_px)} -> {fmt_off(s.off_to_px)} px"


# ------------------------------------------------- 1: Broadway yellow

def check1_broadway(g, proj, segments):
    print("\nCHECK 1 — Broadway yellow trunk: N/Q/R/W = ONE ribbon")
    yellow = family_color(g, YELLOW_ROUTES)
    print(f"  yellow family colour key: {yellow} "
          f"(routes {','.join(sorted(YELLOW_ROUTES))})")

    bad_pts = []
    for name, pt in BROADWAY_POINTS:
        hits, _ = cross_section(proj, segments, pt, colors={yellow})
        routes = sorted({r for s in hits
                         for r in s.route_short_names.split(",")})
        ok = (len(hits) == 1 and set(routes) <= YELLOW_ROUTES
              and len(routes) >= 2)
        mark = "ok" if ok else "BAD"
        det = "; ".join(f"seg {s.seg_id} {s.kind} [{s.route_short_names}] "
                        f"slot {s.slot}/{s.line_count}" for s in hits)
        print(f"  {mark:>3} {name:<26} {len(hits)} yellow ribbon(s): {det}")
        if not ok:
            bad_pts.append((name, len(hits), routes))
    report("check1.one-yellow-ribbon", not bad_pts,
           f"{len(bad_pts)} cross-sections without exactly one "
           f"co-running N/Q/R/W ribbon: {bad_pts}")

    # side-by-side sweep: two distinct yellow steady features parallel
    # within PARALLEL_BUF_M for > PARALLEL_MAX_M means the raster kept
    # two centerlines for one trunk (duplicate-ribbon failure mode);
    # junction divergence cones stay far below the threshold
    from shapely.geometry import LineString
    win = window_geom(proj, BROADWAY_WINDOW)
    feats = [s for s in segments
             if s.kind == "steady" and s.color_key == yellow
             and LineString(s.xy).intersects(win)]
    worst = (0.0, None)
    bad_pairs = []
    for i, a in enumerate(feats):
        la = LineString(a.xy)
        for b in feats[i + 1:]:
            lb = LineString(b.xy)
            if a.corridor_id == b.corridor_id:
                continue
            par = la.intersection(lb.buffer(PARALLEL_BUF_M)).length
            if par > worst[0]:
                worst = (par, (a.seg_id, b.seg_id))
            if par > PARALLEL_MAX_M:
                bad_pairs.append((a.seg_id, b.seg_id, round(par, 1)))
    print(f"  {len(feats)} yellow steady features in the window; worst "
          f"parallel proximity {worst[0]:.1f} m (segs {worst[1]}, "
          f"limit {PARALLEL_MAX_M:.0f} m within {PARALLEL_BUF_M:.0f} m)")
    report("check1.no-side-by-side-duplicates", not bad_pairs,
           f"{len(bad_pairs)} yellow steady pairs parallel beyond the "
           f"duplicate threshold: {bad_pairs}")


# --------------------------------------------- 2: trunk family table

def check2_trunk_families(g, proj, segments):
    print("\nCHECK 2 — trunk families: one ribbon per colour family")
    bad = []
    for name, pt, families in TRUNKS:
        expected = {family_color(g, fam): fam for fam in families}
        hits, _ = cross_section(proj, segments, pt,
                                colors=set(expected))
        by_color = defaultdict(list)
        for s in hits:
            by_color[s.color_key].append(s)
        print(f"  {name} @ ({pt[0]:.4f}, {pt[1]:.4f}) — "
              f"{len(hits)} ribbon(s) across the trunk:")
        for ck in sorted(by_color):
            for s in by_color[ck]:
                print(f"    #{ck} seg {s.seg_id:4d} {s.kind:<10} "
                      f"[{s.route_short_names:<10}] slot "
                      f"{s.slot}/{s.line_count} {seg_off(s)}")
        for ck, fam in sorted(expected.items()):
            got = by_color.get(ck, [])
            routes = {r for s in got
                      for r in s.route_short_names.split(",")}
            if len(got) != 1 or routes != fam:
                bad.append((name, ck, sorted(fam), len(got),
                            sorted(routes)))
    for b in bad:
        print(f"  VIOLATION {b[0]} #{b[1]}: expected 1 ribbon with "
              f"routes {b[2]}, got {b[3]} ribbon(s) with {b[4]}")
    report("check2.one-ribbon-per-family", not bad,
           f"{len(bad)} (trunk, family) cells not a single "
           f"expected-route ribbon")


# --------------------------------------------------- 3: DeKalb junction

def check3_dekalb(g, proj, segments):
    print("\nCHECK 3 — DeKalb junction: transition inventory + receipts")
    import json

    import psycopg
    from shapely.geometry import LineString

    sites = transition_sites(g)
    win = window_geom(proj, DEKALB_WINDOW)
    trs = [s for s in segments if s.kind == "transition"
           and LineString(s.xy).intersects(win)]
    trs.sort(key=lambda s: (s.color_key, s.seg_id))

    def site_str(s):
        parts = []
        for nid in s.sites:
            n = g.nodes[nid]
            deg = g.degree(nid)
            kind = sites.get(nid, "NOT-A-SITE")
            parts.append(f"{nid} deg{deg} {kind}"
                         + (f" [{n.label}]" if n.label else ""))
        return "; ".join(parts)

    print(f"  {len(trs)} transition features in the DeKalb window:")
    bad_anchor = []
    for s in trs:
        print(f"    seg {s.seg_id:4d} #{s.color_key} "
              f"[{s.route_short_names:<8}] slot {s.slot}/{s.line_count} "
              f"{seg_off(s)}  {s.len_m:5.0f} m  site {site_str(s)}")
        if not s.sites or any(nid not in sites for nid in s.sites):
            bad_anchor.append(s.seg_id)
    report("check3.window-populated", len(trs) >= 6,
           f"{len(trs)} transitions in the junction window (>= 6)")
    report("check3.anchored-to-graph-junctions", not bad_anchor,
           f"{len(bad_anchor)} transitions not anchored to a "
           f"junction/composition site: {bad_anchor}")

    # fillet floors + self-intersections, measured on the emitted rows
    w, s_, e, n = DEKALB_WINDOW
    with psycopg.connect(DEFAULT_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT seg_id, ST_AsGeoJSON(geom, 15), ST_IsSimple(geom)
               FROM transit_line_segments
               WHERE build_key = %s AND band_minzoom = %s
                 AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)""",
            (BUILD, DEFAULT_BAND, w, s_, e, n))
        rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    not_simple = [sid for sid, (_, simple) in rows.items() if not simple]
    report("check3.no-self-intersections", not not_simple,
           f"{len(not_simple)} self-intersecting emitted rows in the "
           f"window: {not_simple}")

    inf = float("inf")
    bad_fillet = []
    for t in trs:
        row = rows.get(t.seg_id)
        if row is None:
            bad_fillet.append((t.seg_id, "missing from DB"))
            continue
        xy = proj.to_xy(json.loads(row[0])["coordinates"])
        measured = min((_circumradius(a, b, c)
                        for a, b, c in zip(xy, xy[1:], xy[2:])),
                       default=inf)
        target = (t.fillet_target_m if t.fillet_target_m is not None
                  else t.line_count * CFG.gap_px * CFG.fillet_radius_factor)
        raw = t.raw_min_radius_m if t.raw_min_radius_m is not None else inf
        ach = t.fillet_radius_m if t.fillet_radius_m is not None else inf
        floor = min(ach, raw) if t.fillet_clamped else min(target, raw)
        if measured < 0.9 * floor:
            bad_fillet.append((t.seg_id, round(measured, 1),
                               round(floor, 1)))
    report("check3.fillet-floors-hold", not bad_fillet,
           f"{len(bad_fillet)} window transitions under their curvature "
           f"floor: {bad_fillet}")


def main() -> int:
    print(f"NYC acceptance exam — build {BUILD}\ndsn {DEFAULT_DSN}")
    g, proj, segments = rebuild()
    print(f"rebuilt {len(segments)} features "
          f"({sum(1 for s in segments if s.kind == 'steady')} steady, "
          f"{sum(1 for s in segments if s.kind == 'transition')} "
          f"transition) from {len(g.edges)} edges")

    check1_broadway(g, proj, segments)
    check2_trunk_families(g, proj, segments)
    check3_dekalb(g, proj, segments)

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"EXAM FAILED — {len(FAILURES)} failing check(s):")
        for f in FAILURES:
            print(f"  * {f}")
        return 1
    print("EXAM PASSED — all checks green")
    return 0


if __name__ == "__main__":
    sys.exit(main())
