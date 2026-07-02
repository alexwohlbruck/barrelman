#!/usr/bin/env python3
"""Junction through-ribbon straightness exam (PAR-12 v3 refit metric).

THE METRIC: for every junction (degree >= 3 node) of the skeleton graph
and every ribbon passing THROUGH it — a pair of incident edges with
TRAVERSAL EVIDENCE: some attributed shape rides arm -> junction -> arm
in order (probes 30 m into each arm, projected onto a single pass of
the shape near the node, segments' _shape_evidence rule) — measure the
max deviation of the centerline from the chord connecting the corridor
points ~100 m either side of the junction along it. The walk follows
the corridor through intermediate nodes while the pair's shared shapes
identify exactly one continuation, so the short rungs a shallow X
skeletonizes into are measured as one through-path.

Printed as a before/after distribution (count, mean, p50, p90, max):
BEFORE = raw skeleton geometry (what shipped pre-refit), AFTER = the
shape-evidence refit geometry (linegraph.refit) — station splitting and
line-less pruning never move geometry, so these ARE the final
centerlines of the two pipeline variants. The subset whose OWN shape
evidence is straight (truth < 2 m) is printed separately: that is the
X/T-crossing receipt, where the skeleton's medial-axis bow lived and
must disappear.

Every pair also carries a TRUTH allowance: the max chord deviation of
the supporting shapes' own +-100 m windows at the junction. Genuine
corners keep large deviations (the shapes bend there too — Chicago's
Loop); fused flying-junction corridors are compromises of several
tracks and may exceed any single shape by up to the merge criterion
(each edge's refit is capped at merge width from its evidence).
Deviations may GROW only where that allowance covers the result:
growth beyond half a merge width with the after-value NOT within
truth + one merge width fails the exam, as do a non-improving overall
mean and a non-improving straight-subset mean.

Read-only on the DB (touches only the local skeleton cache). Run:

  uv run --with-requirements linegraph/requirements.txt \
      python linegraph/exam/junction_exam.py            # NYC (feed 5)
  uv run --with-requirements linegraph/requirements.txt \
      python linegraph/exam/junction_exam.py --feed 29  # Chicago
"""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyproj import Transformer                             # noqa: E402
from shapely.geometry import LineString, Point             # noqa: E402
from shapely.ops import substring                          # noqa: E402

from linegraph.attribute import (EdgeSnapIndex,            # noqa: E402
                                 attribute_shape_xy, sample_polyline_xy)
from linegraph.build import collect_shapes, resolve_feed_zip  # noqa: E402
from linegraph.model import (default_cache_path,           # noqa: E402
                             input_digest, load_linegraph, save_linegraph)
from linegraph.refit import refit_geometry                 # noqa: E402

ARM_M = 100.0        # corridor walk length either side of the junction
DENSIFY_M = 2.0      # deviation sampling step along the walked path
SHAPE_DENSIFY_M = 3.0
MIN_ARM_M = 25.0     # pairs with a stubbier arm than this are skipped
PROBE_M = 30.0       # traversal-evidence probe distance into each arm
EVIDENCE_TOL_M = 25.0
NODE_VISIT_M = 30.0  # a pass supports a junction only if it comes this close
STRAIGHT_TRUTH_M = 2.0   # truth below this = straight-through subset
GROW_FLAG_M = 1.0    # per-pair growth worth flagging
GROW_FAIL_FRAC = 0.5  # x merge_width: growth beyond this needs truth cover
COVER_FRAC = 1.0     # x merge_width: the truth cover a grower may use —
                     # per-edge refits are capped at one merge width from
                     # their own evidence, so a fused composite may bend
                     # up to that beyond any single supporting shape


def load_or_build(feed: str, mode: str, merge: float, res: float,
                  zip_override=None):
    zip_path = resolve_feed_zip(feed, zip_override)
    shapes, n_patterns, n_skipped = collect_shapes(zip_path, mode)
    if not shapes:
        raise SystemExit(f"feed {feed} mode {mode}: no shapes")
    digest = input_digest(shapes, merge, res)
    cache = default_cache_path(feed, mode)
    try:
        lg = load_linegraph(cache, expect_digest=digest)
        src = f"cache {cache.name}"
    except (FileNotFoundError, ValueError):
        from linegraph.build import build_linegraph

        lg = build_linegraph(shapes, merge, res, build_key=f"exam:{feed}",
                             feed_id=feed, mode=mode, verbose=True)
        save_linegraph(lg, cache)
        src = "fresh build"
    print(f"[junction] {n_patterns} patterns -> {len(shapes)} shapes "
          f"({n_skipped} shapeless), skeleton {len(lg.nodes)} nodes / "
          f"{len(lg.edges)} edges ({src})")
    return lg, shapes


def coarse_edge_shapes(lg, shapes_lonlat) -> dict:
    """{edge position: {shape index riding it}} via the standard core."""
    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    index = EdgeSnapIndex(lg)
    out: dict = {}
    for si, coords in enumerate(shapes_lonlat):
        xs, ys = to_xy.transform([c[0] for c in coords], [c[1] for c in coords])
        ridden, _, _, _ = attribute_shape_xy(index, list(zip(xs, ys)))
        for eid in ridden:
            out.setdefault(eid, set()).add(si)
    return out


def densify_shapes(lg, shapes_lonlat) -> list:
    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    dense = []
    for coords in shapes_lonlat:
        xs, ys = to_xy.transform([c[0] for c in coords], [c[1] for c in coords])
        dense.append(sample_polyline_xy(list(zip(xs, ys)), SHAPE_DENSIFY_M))
    return dense


def build_incidence(lg) -> dict:
    inc: dict = {}
    for pos, e in enumerate(lg.edges):
        inc.setdefault(e.from_node, []).append(pos)
        if e.to_node != e.from_node:
            inc.setdefault(e.to_node, []).append(pos)
    return inc


def node_passes(shape_dense: np.ndarray, node: np.ndarray, window: float):
    """Single passes of one shape near a node: contiguous within-window
    runs that actually visit the junction (come within NODE_VISIT_M)."""
    d = np.hypot(*(shape_dense - node).T)
    idx = np.flatnonzero(d < window)
    if idx.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(idx) > 1)
    out = []
    for run in np.split(idx, breaks + 1):
        if len(run) >= 2 and d[run].min() < NODE_VISIT_M:
            out.append(LineString(shape_dense[run]))
    return out


def probe_point(edge, nid: int, dist: float) -> Point:
    coords = (edge.coords_xy if edge.from_node == nid
              else list(reversed(edge.coords_xy)))
    line = LineString(coords)
    return line.interpolate(min(dist, 0.9 * line.length))


def walk_arm(lg, incidence, edge_shapes, start_node: int, first_edge: int,
             shared: set, arm_m: float) -> np.ndarray:
    """Corridor points from start_node outward, <= arm_m of arc length.

    Continues through a node only when the pair's shared shapes select
    exactly ONE other incident edge there (unambiguous continuation)."""
    pts: list = []
    node, edge = start_node, first_edge
    remaining = arm_m
    for _ in range(64):
        e = lg.edges[edge]
        coords = (e.coords_xy if e.from_node == node
                  else list(reversed(e.coords_xy)))
        line = LineString(coords)
        take = min(remaining, line.length)
        piece = substring(line, 0.0, take) if take < line.length else line
        dense = sample_polyline_xy(list(piece.coords), DENSIFY_M)
        pts.append(dense if not pts else dense[1:])
        remaining -= take
        if remaining <= 1e-6 or take < line.length:
            break
        nxt = e.to_node if e.from_node == node else e.from_node
        cands = [i for i in incidence.get(nxt, [])
                 if i != edge and edge_shapes.get(i, set()) & shared]
        if len(cands) != 1:
            break
        node, edge = nxt, cands[0]
    return np.vstack([p for p in pts if len(p)]) if pts else np.empty((0, 2))


def max_dev_from_chord(pts: np.ndarray) -> float:
    a, b = pts[0], pts[-1]
    ab = b - a
    norm2 = float(ab @ ab)
    if norm2 < 1e-12:
        return 0.0
    t = np.clip(((pts - a) @ ab) / norm2, 0.0, 1.0)
    proj = a[None, :] + t[:, None] * ab[None, :]
    return float(np.hypot(*(pts - proj).T).max())


def through_pairs(lg, incidence, edge_shapes, dense, arm_m: float):
    """Evidence-backed pairs: [(nid, arm, arm, shared)] + truth allowance
    {(nid, arm, arm): max chord dev of the supporting shape windows}."""
    node_xy = {n.node_id: np.array([n.x, n.y]) for n in lg.nodes}
    pairs, truth = [], {}
    for nid in sorted(incidence):
        if len(incidence[nid]) < 3:
            continue
        arms = [p for p in incidence[nid]
                if lg.edges[p].from_node != lg.edges[p].to_node]
        c = node_xy[nid]
        node_pt = Point(c[0], c[1])
        passes_of: dict = {}
        for i in range(len(arms)):
            for j in range(i + 1, len(arms)):
                ea, eb = arms[i], arms[j]
                shared = (edge_shapes.get(ea, set())
                          & edge_shapes.get(eb, set()))
                if not shared:
                    continue
                pi = probe_point(lg.edges[ea], nid, PROBE_M)
                pj = probe_point(lg.edges[eb], nid, PROBE_M)
                t_best = None
                for si in sorted(shared):
                    if si not in passes_of:
                        passes_of[si] = node_passes(dense[si], c,
                                                    arm_m + 60.0)
                    for ls in passes_of[si]:
                        if (ls.distance(node_pt) > EVIDENCE_TOL_M
                                or ls.distance(pi) > EVIDENCE_TOL_M
                                or ls.distance(pj) > EVIDENCE_TOL_M):
                            continue
                        fi, fn, fj = (ls.project(pi), ls.project(node_pt),
                                      ls.project(pj))
                        if not (fi < fn < fj or fj < fn < fi):
                            continue  # a fork pair, not a through-ribbon
                        sub = substring(ls, max(0.0, fn - arm_m),
                                        min(ls.length, fn + arm_m))
                        sxy = np.asarray(sub.coords)
                        if len(sxy) < 2 or LineString(sxy).length < 2 * MIN_ARM_M:
                            continue
                        v = max_dev_from_chord(sxy)
                        t_best = v if t_best is None else max(t_best, v)
                if t_best is not None:
                    pairs.append((nid, ea, eb, shared))
                    truth[(nid, ea, eb)] = t_best
    return pairs, truth


def measure(lg, incidence, edge_shapes, pairs, arm_m: float) -> dict:
    """{(nid, ea, eb): max chord deviation} — None when an arm is too
    stubby to measure."""
    out = {}
    for nid, ea, eb, shared in pairs:
        pa = walk_arm(lg, incidence, edge_shapes, nid, ea, shared, arm_m)
        pb = walk_arm(lg, incidence, edge_shapes, nid, eb, shared, arm_m)
        if len(pa) < 2 or len(pb) < 2:
            out[(nid, ea, eb)] = None
            continue

        def arclen(pts):
            return float(np.hypot(*(pts[1:] - pts[:-1]).T).sum())

        if arclen(pa) < MIN_ARM_M or arclen(pb) < MIN_ARM_M:
            out[(nid, ea, eb)] = None
            continue
        path = np.vstack([pa[::-1], pb[1:]])
        out[(nid, ea, eb)] = max_dev_from_chord(path)
    return out


def dist_row(name: str, vals) -> str:
    v = np.asarray(vals)
    if v.size == 0:
        return f"  {name:<8} {0:>6}"
    return (f"  {name:<8} {len(v):>6}  {v.mean():7.2f} {np.median(v):7.2f} "
            f"{np.percentile(v, 90):7.2f} {v.max():7.2f}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--feed", default="5", help="GTFS feed id (default NYC)")
    ap.add_argument("--mode", default="rail")
    ap.add_argument("--merge-width", type=float, default=18.0)
    ap.add_argument("--res", type=float, default=2.0)
    ap.add_argument("--zip", type=Path, default=None)
    ap.add_argument("--arm", type=float, default=ARM_M,
                    help="corridor walk length either side (m)")
    args = ap.parse_args(argv)

    lg_before, shapes = load_or_build(args.feed, args.mode,
                                      args.merge_width, args.res, args.zip)
    edge_shapes = coarse_edge_shapes(lg_before, shapes)
    dense = densify_shapes(lg_before, shapes)
    incidence = build_incidence(lg_before)
    pairs, truth = through_pairs(lg_before, incidence, edge_shapes, dense,
                                 args.arm)
    n_junctions = len({nid for nid, _, _, _ in pairs})
    print(f"[junction] {len(pairs)} evidence-backed through-ribbon pairs "
          f"across {n_junctions} junctions (deg>=3)")

    lg_after = copy.deepcopy(lg_before)
    rs = refit_geometry(lg_after, shapes)
    print(f"[junction] refit: {rs.n_refit}/{rs.n_edges} edges, "
          f"{rs.n_contributions} sub-polylines, {rs.n_no_evidence} "
          f"no-evidence, {rs.n_capped} capped, max node move "
          f"{rs.max_node_move_m:.1f} m, {rs.n_node_fallback} LSQ fallbacks, "
          f"{rs.n_floor_pairs} floor pairs")

    before = measure(lg_before, incidence, edge_shapes, pairs, args.arm)
    after = measure(lg_after, incidence, edge_shapes, pairs, args.arm)

    keys = [k for k in before
            if before[k] is not None and after[k] is not None]
    skipped = len(before) - len(keys)
    b = np.array([before[k] for k in keys])
    a = np.array([after[k] for k in keys])

    print(f"\nthrough-ribbon max deviation from the ±{args.arm:.0f} m chord "
          f"(m); {skipped} stub pairs skipped")
    print(f"  {'':<8} {'count':>6}  {'mean':>7} {'p50':>7} {'p90':>7} {'max':>7}")
    print(dist_row("before", b))
    print(dist_row("after", a))

    straight = [i for i, k in enumerate(keys)
                if truth[k] < STRAIGHT_TRUTH_M]
    print(f"\nstraight-through subset (own shape evidence bends < "
          f"{STRAIGHT_TRUTH_M:.0f} m — the X/T crossing receipt)")
    print(f"  {'':<8} {'count':>6}  {'mean':>7} {'p50':>7} {'p90':>7} {'max':>7}")
    print(dist_row("before", b[straight]))
    print(dist_row("after", a[straight]))

    node_at = {n.node_id: (n.lon, n.lat) for n in lg_before.nodes}
    worst = sorted(range(len(keys)), key=lambda i: b[i], reverse=True)[:10]
    print("\n  worst pre-refit pairs (before -> after, truth allowance):")
    for i in worst:
        nid, ea, eb = keys[i]
        lon, lat = node_at[nid]
        print(f"    node {nid:>5} ({lon:.5f},{lat:.5f}) edges {ea}+{eb}: "
              f"{b[i]:6.2f} -> {a[i]:6.2f} m (truth {truth[keys[i]]:.2f})")

    grew = [i for i in range(len(keys)) if a[i] - b[i] > GROW_FLAG_M]
    grow_cap = GROW_FAIL_FRAC * args.merge_width
    cover = COVER_FRAC * args.merge_width
    bad = [i for i in grew
           if a[i] - b[i] > grow_cap and a[i] > truth[keys[i]] + cover]
    if grew:
        print(f"\n  FLAG: {len(grew)} pair(s) grew > {GROW_FLAG_M} m "
              f"(growth is legitimate where the track genuinely bends — "
              f"the refit restores true curvature the skeleton smoothed):")
        for i in sorted(grew, key=lambda i: a[i] - b[i], reverse=True):
            nid, ea, eb = keys[i]
            lon, lat = node_at[nid]
            print(f"    node {nid:>5} ({lon:.5f},{lat:.5f}) edges {ea}+{eb}: "
                  f"{b[i]:6.2f} -> {a[i]:6.2f} m (+{a[i] - b[i]:.2f}, "
                  f"truth {truth[keys[i]]:.2f})")
    else:
        print("\n  no pair grew beyond the flag threshold")

    ok = True
    if a.mean() >= b.mean():
        print(f"\nFAIL: refit did not improve the mean "
              f"({b.mean():.2f} -> {a.mean():.2f} m)")
        ok = False
    if straight and a[straight].mean() >= b[straight].mean():
        print(f"\nFAIL: refit did not improve the straight-through subset "
              f"({b[straight].mean():.2f} -> {a[straight].mean():.2f} m)")
        ok = False
    if bad:
        print(f"\nFAIL: {len(bad)} pair(s) grew beyond {grow_cap:.1f} m "
              f"without truth + merge-width cover:")
        for i in bad:
            nid, ea, eb = keys[i]
            print(f"    node {nid} edges {ea}+{eb}: {b[i]:.2f} -> {a[i]:.2f} "
                  f"(truth {truth[keys[i]]:.2f})")
        ok = False
    print(f"\n{'PASS' if ok else 'FAIL'}: junction through-ribbon exam "
          f"(feed {args.feed}, {args.mode})")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
