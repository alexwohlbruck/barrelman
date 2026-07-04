"""tools.sandbox.bundle_audit — network-wide missed-bundle sweep (Part C).

The acceptance metric for the corridor bundling fix. Reads the EMITTED
pre-offset corridor geometry (transit_graph_edges + transit_graph_edge_lines)
and finds every pair of DIFFERENT-FAMILY steady corridors that

  * run within `cross_family_gap_m` of each other, and
  * stay PARALLEL (low relative bearing) for a sustained contiguous length
    (> min_len_m), and
  * do NOT share a centerline (they are two geometrically distinct edges,
    not one bundled edge carrying both families).

Each such pair is CLASSIFIED:

  * missed-bundle (FAIL) — the two corridors run parallel and NON-CROSSING
    for the whole co-run: they are the same physical corridor rendered as
    two crowded independent lines and SHOULD have merged onto one shared
    centerline. This is the bug.
  * legit (OK)          — the co-run contains a real crossing / kiss /
    divergence (the geometries intersect mid-run, or the parallel stretch
    is a transient valley), so keeping them separate is correct.

"Sharing a centerline" is exact at the corridor level: a real bundle is ONE
edge carrying both families (line_count folds them together, DeKalb-style),
or two edges whose geometry is coincident (median gap <= coincident_tol_m).
Two edges of different families that stay a track-gap apart in parallel are
the missed bundle.

Prints, per city: the missed-bundle COUNT + total METERS + worst offenders,
and the legit (correctly-separate) count. Run before/after a merge change:
the missed-bundle count must drop to ~0 (only real crossings remain), with
no labeled kiss site flipped into a bundle.

  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.bundle_audit
  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.bundle_audit --build-key nyc:subway-v3 --json
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DEFAULT_DSN = "postgresql://barrelman:barrelman@localhost:5434/barrelman"
BUILD_KEYS = ("nyc:subway-v3", "chicago:l-v3")

# thresholds — mirror the corridor builder's cross-family bundle criteria so
# the sweep flags exactly what the merge SHOULD have caught.
GAP_M = 12.0            # "close together": within this in plan
MIN_LEN_M = 150.0       # sustained parallel co-run longer than this
BEARING_TOL_DEG = 20.0  # parallel (not crossing) — same as cross_family bearing
COINCIDENT_TOL_M = 6.0  # centerlines within this = already bundled (one ribbon)
SAMPLE_STEP_M = 10.0    # co-run sampling resolution


# ── geometry helpers (projected metres) ──────────────────────────────────


def _to_xy(coords, lat0):
    """Equirectangular local projection (metres), good at city scale."""
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(lat0))
    return [(lon * m_per_deg_lon, lat * m_per_deg_lat) for lon, lat in coords]


def _densify(xy, step):
    import numpy as np
    pts = np.asarray(xy, dtype=float)
    if len(pts) < 2:
        return pts
    seg = np.hypot(*(pts[1:] - pts[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    total = float(cum[-1])
    if total <= 0:
        return pts[:1]
    n = max(1, int(total / step))
    t = np.linspace(0.0, total, n + 1)
    return np.column_stack([np.interp(t, cum, pts[:, 0]),
                            np.interp(t, cum, pts[:, 1])])


def _bearing_at(pts, i, cum, h=12.0):
    import numpy as np
    t = cum[i]
    lo = np.searchsorted(cum, max(0.0, t - h))
    hi = min(len(pts) - 1, np.searchsorted(cum, min(cum[-1], t + h)))
    lo = min(lo, len(pts) - 1)
    d = pts[hi] - pts[lo]
    return math.atan2(d[1], d[0])


def _bearing_diff(b1, b2):
    return math.degrees(abs((b1 - b2 + math.pi / 2) % math.pi - math.pi / 2))


def analyze_pair(a_xy, b_xy, gap_m, bearing_tol):
    """Return the co-run analysis of corridor A against corridor B.

    Walks A sample-by-sample; a sample is "co-running" when it lies within
    gap_m of B AND the local bearings are parallel (< bearing_tol). Returns
    the longest CONTIGUOUS co-run (metres), the median gap over it, whether
    the two geometries CROSS inside that run, and the co-run's midpoint.
    """
    import numpy as np
    from shapely.geometry import LineString, Point

    a = _densify(a_xy, SAMPLE_STEP_M)
    if len(a) < 2:
        return None
    b = _densify(b_xy, SAMPLE_STEP_M)
    if len(b) < 2:
        return None
    lb = LineString(b)
    bcum = np.concatenate([[0.0], np.cumsum(np.hypot(*(b[1:] - b[:-1]).T))])
    acum = np.concatenate([[0.0], np.cumsum(np.hypot(*(a[1:] - a[:-1]).T))])

    best = None            # (length_m, i0, i1, gaps)
    cur_len = 0.0
    cur_start = None
    cur_gaps = []
    for i in range(len(a)):
        p = Point(a[i])
        d = lb.distance(p)
        parallel = False
        if d <= gap_m:
            s = lb.project(p)
            bi = int(np.searchsorted(bcum, s))
            ba = _bearing_at(a, i, acum)
            bb = _bearing_at(b, min(bi, len(b) - 1), bcum)
            parallel = _bearing_diff(ba, bb) <= bearing_tol
        if parallel:
            if cur_start is None:
                cur_start = i
                cur_len = 0.0
                cur_gaps = []
            else:
                cur_len += acum[i] - acum[i - 1]
            cur_gaps.append(d)
        else:
            if cur_start is not None:
                if best is None or cur_len > best[0]:
                    best = (cur_len, cur_start, i - 1, cur_gaps)
            cur_start = None
    if cur_start is not None:
        if best is None or cur_len > best[0]:
            best = (cur_len, cur_start, len(a) - 1, cur_gaps)
    if best is None or best[0] <= 0:
        return None

    length_m, i0, i1, gaps = best
    gaps_sorted = sorted(gaps)
    med_gap = gaps_sorted[len(gaps_sorted) // 2]
    min_gap = gaps_sorted[0]
    # crossing test: do the two geometries intersect INSIDE the co-run
    # (trimmed a little off each end so a shared switch at the ends is not a
    # crossing)? A mid-run intersection is the definitive "not a bundle".
    crosses = False
    if i1 - i0 >= 3:
        trim = max(1, (i1 - i0) // 10)
        sub = LineString(a[i0 + trim:i1 - trim + 1])
        if sub.length > 1.0:
            inter = sub.intersection(lb)
            crosses = (not inter.is_empty) and inter.geom_type in (
                "Point", "MultiPoint", "GeometryCollection")
    mid = a[(i0 + i1) // 2]
    return {"co_run_m": length_m, "median_gap_m": med_gap,
            "min_gap_m": min_gap, "crosses": crosses, "mid_xy": mid,
            "i0": i0, "i1": i1}


# ── DB access ─────────────────────────────────────────────────────────────


def load_corridors(cur, build_key):
    """[(edge_id, families:set, routes:set, coords_lonlat)] — one per emitted
    corridor edge. families = distinct route_colors carried by the edge."""
    cur.execute(
        """SELECT e.id, ST_AsGeoJSON(e.geom),
                  array_agg(DISTINCT el.route_color),
                  array_agg(DISTINCT el.route_short_name)
           FROM transit_graph_edges e
           JOIN transit_graph_edge_lines el ON el.edge_id = e.id
           WHERE e.build_key = %s
           GROUP BY e.id, e.geom""", (build_key,))
    out = []
    for eid, gj, colors, names in cur.fetchall():
        g = json.loads(gj)
        if g["type"] != "LineString" or len(g["coordinates"]) < 2:
            continue
        fams = frozenset(c for c in colors if c)
        out.append((eid, fams, frozenset(names), g["coordinates"]))
    return out


def _coincident_m(a_xy, b_xy):
    """Median gap over the overlapping run (<=40 m) between two centerlines —
    the 'already one physical corridor' test."""
    from shapely.geometry import LineString, Point
    lb = LineString(b_xy)
    ds = [lb.distance(Point(p)) for p in a_xy if lb.distance(Point(p)) <= 40.0]
    if not ds:
        return None
    ds.sort()
    return ds[len(ds) // 2]


# ── sweep ─────────────────────────────────────────────────────────────────


def sweep(cur, build_key, gap_m=GAP_M, min_len_m=MIN_LEN_M,
          bearing_tol=BEARING_TOL_DEG):
    """Find & classify every different-family close-parallel corridor pair."""
    from shapely.geometry import LineString
    from shapely import STRtree

    corrs = load_corridors(cur, build_key)
    if not corrs:
        return {"build_key": build_key, "corridors": 0, "missed": [],
                "legit": []}
    lat0 = corrs[0][3][0][1]
    xy = [_to_xy(c[3], lat0) for c in corrs]
    lines = [LineString(p) for p in xy]
    tree = STRtree(lines)

    missed = []
    legit = []
    seen = set()
    for i, (eid_a, fam_a, names_a, _c) in enumerate(corrs):
        for j in tree.query(lines[i], predicate="dwithin", distance=gap_m):
            j = int(j)
            if j == i:
                continue
            key = frozenset((i, j))
            if key in seen:
                continue
            seen.add(key)
            eid_b, fam_b, names_b, _cb = corrs[j]
            # cross-family only: the two corridors carry DISJOINT families
            if fam_a & fam_b:
                continue
            res = analyze_pair(xy[i], xy[j], gap_m, bearing_tol)
            if res is None or res["co_run_m"] < min_len_m:
                continue
            # already one physical corridor? (coincident centerlines — the
            # DeKalb 0.0 m case is a single edge so it never reaches here,
            # but a duplicate-geometry pair would)
            coin = _coincident_m(xy[i], xy[j])
            if coin is not None and coin <= COINCIDENT_TOL_M:
                continue
            lon = res["mid_xy"][0] / (111320.0 * math.cos(math.radians(lat0)))
            lat = res["mid_xy"][1] / 111320.0
            rec = {
                "edge_a": eid_a, "edge_b": eid_b,
                "routes_a": sorted(names_a), "routes_b": sorted(names_b),
                "families_a": sorted(fam_a), "families_b": sorted(fam_b),
                "co_run_m": round(res["co_run_m"], 1),
                "median_gap_m": round(res["median_gap_m"], 2),
                "min_gap_m": round(res["min_gap_m"], 2),
                "crosses": res["crosses"],
                "at": [round(lon, 5), round(lat, 5)],
            }
            # classification: non-crossing sustained parallel => missed
            # bundle (FAIL); a crossing in the co-run => legit separate.
            if res["crosses"]:
                legit.append(rec)
            else:
                missed.append(rec)
    missed.sort(key=lambda r: -r["co_run_m"])
    legit.sort(key=lambda r: -r["co_run_m"])
    return {"build_key": build_key, "corridors": len(corrs),
            "missed": missed, "legit": legit,
            "missed_count": len(missed),
            "missed_total_m": round(sum(r["co_run_m"] for r in missed), 1),
            "legit_count": len(legit)}


def render(card):
    L = []
    bk = card["build_key"]
    L.append("=" * 68)
    L.append(f" BUNDLE AUDIT — {bk}   ({card['corridors']} corridors)")
    L.append("=" * 68)
    L.append(f" MISSED bundles (parallel, non-crossing, unmerged): "
             f"{card['missed_count']}")
    L.append(f"   total co-run meters: {card['missed_total_m']} m")
    L.append(f" legit (crossing/kiss, correctly separate): "
             f"{card['legit_count']}")
    L.append("-" * 68)
    if card["missed"]:
        L.append(" worst offenders (should merge):")
        for r in card["missed"][:16]:
            L.append(f"   {'/'.join(r['routes_a']):<10} <-> "
                     f"{'/'.join(r['routes_b']):<10} "
                     f"{r['co_run_m']:>7.1f} m  gap {r['median_gap_m']:>5.1f}"
                     f"/{r['min_gap_m']:.1f} m  @ {r['at']}")
    else:
        L.append(" (no missed bundles — every close parallel pair is a real "
                 "crossing)")
    L.append("=" * 68)
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Missed-bundle network sweep")
    ap.add_argument("--build-key", action="append",
                    help="default: nyc:subway-v3 and chicago:l-v3")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--gap-m", type=float, default=GAP_M)
    ap.add_argument("--min-len-m", type=float, default=MIN_LEN_M)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    import psycopg
    keys = args.build_key or list(BUILD_KEYS)
    cards = []
    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        for bk in keys:
            cards.append(sweep(cur, bk, gap_m=args.gap_m,
                               min_len_m=args.min_len_m))
    if args.json:
        print(json.dumps(cards, indent=2, default=float))
    else:
        for c in cards:
            print(render(c))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
