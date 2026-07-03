"""Synthetic exams for the shape-evidence geometry refit (linegraph.refit).

Same authoring convention as test_phase_b.py: geometry in local meters
around a UTM-16N anchor, unprojected to lon/lat, through the REAL build
path (raster -> skeleton -> vectorize), then refit against the same
shapes the raster was stamped from.

Contracts (PAR-12 v3 through-track fix):
  a. two straight tracks crossing at 90 and ~30 degrees: after refit
     every through-edge's max deviation from its straight chord is
     < 1 m; the pre-refit deviation is recorded and, at 30 degrees,
     proves the skeleton artifact existed (arms bow several meters —
     the 14 St / Nevins St failure in miniature);
  b. T junction: the through-bar stays straight;
  c. Y split: the trunk is the positional average of both branch shapes
     up to the split (directional pair -> corridor centerline);
  d. endpoint continuity: incident edges meet EXACTLY at their nodes
     and the snap blend introduces no terminal kink beyond a few
     degrees on straight track;
  robustness: an edge with no attributed shape keeps its skeleton
     geometry; a refit beyond the merge width (mis-attribution) is
     capped back to skeleton geometry; loop shapes contribute one
     sub-polyline per PASS (never a spliced whole-loop projection);
     refit is deterministic.

Run:
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_refit.py -v
"""

import math

import numpy as np
from pyproj import Transformer

from linegraph.build import build_linegraph
from linegraph.refit import refit_geometry

EPSG = 32616
X0, Y0 = 445_000.0, 4_636_000.0
_TO_WGS = Transformer.from_crs(EPSG, 4326, always_xy=True)

MERGE = 18.0
RES = 1.0


def lonlat(pts_local):
    lons, lats = _TO_WGS.transform(
        [p[0] + X0 for p in pts_local], [p[1] + Y0 for p in pts_local]
    )
    return list(zip(lons, lats))


def build(shapes_local):
    return build_linegraph(
        [lonlat(s) for s in shapes_local], MERGE, RES,
        build_key="test", feed_id="test", mode="rail", verbose=False,
    )


def local_xy(edge):
    return [(x - X0, y - Y0) for x, y in edge.coords_xy]


def chord_dev(coords_xy) -> float:
    """Max deviation of a polyline from its endpoint-to-endpoint chord."""
    a, b = coords_xy[0], coords_xy[-1]
    ab = (b[0] - a[0], b[1] - a[1])
    length = math.hypot(*ab) or 1.0
    return max(
        abs((p[0] - a[0]) * ab[1] - (p[1] - a[1]) * ab[0]) / length
        for p in coords_xy
    )


def max_turn_deg(coords_xy, window_m: float = 40.0) -> float:
    """Max vertex turn angle within window_m of either terminal."""
    pts = np.asarray(coords_xy, dtype=float)
    if len(pts) < 3:
        return 0.0
    seg = np.hypot(*(pts[1:] - pts[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    total = cum[-1]
    worst = 0.0
    for i in range(1, len(pts) - 1):
        if cum[i] > window_m and total - cum[i] > window_m:
            continue
        v1, v2 = pts[i] - pts[i - 1], pts[i + 1] - pts[i]
        n1, n2 = np.linalg.norm(v1), np.linalg.norm(v2)
        if n1 < 1e-9 or n2 < 1e-9:
            continue
        cos = np.clip(np.dot(v1, v2) / (n1 * n2), -1.0, 1.0)
        worst = max(worst, math.degrees(math.acos(cos)))
    return worst


def crossing_shapes(angle_deg: float):
    a = math.radians(angle_deg)
    h = [(-400.0, 0.0), (400.0, 0.0)]
    d = [(-400.0 * math.cos(a), -400.0 * math.sin(a)),
         (400.0 * math.cos(a), 400.0 * math.sin(a))]
    return h, d


# ── a. X crossings: through-edges straight after refit ───────────────────────


def _refit_crossing(angle_deg: float):
    h, d = crossing_shapes(angle_deg)
    lg = build([h, d])
    pre = {e.edge_id: chord_dev(e.coords_xy) for e in lg.edges}
    stats = refit_geometry(lg, [lonlat(h), lonlat(d)])
    post = {e.edge_id: chord_dev(e.coords_xy) for e in lg.edges}
    return lg, pre, post, stats


def test_x90_through_edges_straight():
    lg, pre, post, stats = _refit_crossing(90.0)
    print(f"[x90] chord dev per edge: "
          f"{[(k, round(pre[k], 2), round(post[k], 2)) for k in sorted(pre)]}")
    assert stats.n_refit == len(lg.edges)
    for eid, dev in post.items():
        assert dev < 1.0, (eid, pre[eid], dev)


def test_x30_skeleton_bow_removed():
    lg, pre, post, stats = _refit_crossing(30.0)
    print(f"[x30] chord dev per edge: "
          f"{[(k, round(pre[k], 2), round(post[k], 2)) for k in sorted(pre)]}")
    # the artifact existed: shallow crossings bow the through-arms by
    # meters (the H-pattern skeleton), same failure the NYC user review
    # flagged at 14 St / Nevins St
    arms = [e.edge_id for e in lg.edges if e.length_m > 100.0]
    assert max(pre[eid] for eid in arms) > 3.0, pre
    for eid, dev in post.items():
        assert dev < 1.0, (eid, pre[eid], dev)
    # adjacent junction nodes never collapse onto each other
    by_id = {n.node_id: n for n in lg.nodes}
    for e in lg.edges:
        if e.from_node != e.to_node:
            a, b = by_id[e.from_node], by_id[e.to_node]
            assert math.hypot(a.x - b.x, a.y - b.y) > 1.0


# ── b. T junction: the through-bar stays straight ────────────────────────────


def test_t_junction_bar_stays_straight():
    bar = [(-400.0, 0.0), (400.0, 0.0)]
    stem = [(0.0, 0.0), (0.0, -400.0)]
    lg = build([bar, stem])
    refit_geometry(lg, [lonlat(bar), lonlat(stem)])
    junctions = [n for n in lg.nodes if n.degree >= 3]
    assert len(junctions) == 1
    for e in lg.edges:
        dev = chord_dev(e.coords_xy)
        assert dev < 1.0, (e.edge_id, dev)
        # bar edges stay ON the bar axis, stem on the stem axis
        for x, y in local_xy(e):
            assert min(abs(x), abs(y)) <= 1.0, (e.edge_id, x, y)


# ── c. Y split: trunk = positional average of both branch shapes ─────────────


def test_y_split_trunk_is_branch_average():
    # directional pair: the two branch shapes run 8 m apart on the trunk
    branch_a = [(0.0, 4.0), (300.0, 4.0), (600.0, 124.0)]
    branch_b = [(0.0, -4.0), (300.0, -4.0), (600.0, -124.0)]
    lg = build([branch_a, branch_b])
    stats = refit_geometry(lg, [lonlat(branch_a), lonlat(branch_b)])
    trunk = [e for e in lg.edges
             if all(x < 330.0 for x, _ in local_xy(e))]
    assert len(trunk) == 1
    # both branch shapes contributed somewhere
    assert stats.n_contributions >= 4
    # interior of the trunk sits on the corridor midline y = 0 — the
    # average of the +-4 m directional tracks, not either track
    from shapely.geometry import LineString

    line = LineString(local_xy(trunk[0]))
    for t in np.linspace(0.1, 0.8, 8):
        p = line.interpolate(float(t), normalized=True)
        assert abs(p.y) <= 1.0, (p.x, p.y)


def test_y_fork_through_bar_straight():
    # 30-degree Y fork: a straight through-track plus a branch that
    # shares it then diverges. The dominant through-pair (traversal
    # evidence + straightest arms) must pin the fork node to the
    # through line: the through-bar keeps < 0.5 m of bow, instead of
    # being tugged toward the diverging arm by the all-arms LSQ
    # (Lafayette Av receipt).
    a = math.radians(30.0)
    through = [(-500.0, 0.0), (500.0, 0.0)]
    branch = [(-500.0, 0.0), (0.0, 0.0),
              (500.0 * math.cos(a), 500.0 * math.sin(a))]
    lg = build([through, branch])
    stats = refit_geometry(lg, [lonlat(through), lonlat(branch)])
    assert stats.n_y_through >= 1, stats
    # the through-bar: walk the two edges that hug y = 0
    bar_edges = [e for e in lg.edges
                 if all(abs(y) < 20.0 for _, y in local_xy(e))]
    assert len(bar_edges) >= 2, [local_xy(e)[:2] for e in lg.edges]
    for e in bar_edges:
        assert chord_dev(e.coords_xy) < 0.5, (e.edge_id, chord_dev(e.coords_xy))
        for _, y in local_xy(e):
            assert abs(y) < 0.5, (e.edge_id, y)
    # the combined through-path across the fork node stays straight too
    by_node: dict = {}
    for e in bar_edges:
        by_node.setdefault(e.from_node, []).append(e)
        by_node.setdefault(e.to_node, []).append(e)
    shared = [nid for nid, es in by_node.items() if len(es) == 2]
    assert shared, by_node
    ea, eb = by_node[shared[0]]
    path = list(ea.coords_xy) + list(eb.coords_xy)
    assert chord_dev(path) < 0.5, chord_dev(path)


# ── d. endpoint continuity: exact node coincidence, no terminal kink ─────────


def test_endpoints_meet_exactly_no_terminal_kink():
    h, d = crossing_shapes(30.0)
    lg = build([h, d])
    refit_geometry(lg, [lonlat(h), lonlat(d)])
    pos = {n.node_id: (n.x, n.y) for n in lg.nodes}
    for e in lg.edges:
        assert tuple(e.coords_xy[0]) == pos[e.from_node], e.edge_id
        assert tuple(e.coords_xy[-1]) == pos[e.to_node], e.edge_id
        # straight synthetic track: the snap blend must not fold a kink
        # into the terminal zone (raw geometry has zero-degree turns)
        turn = max_turn_deg(e.coords_xy)
        assert turn < 5.0, (e.edge_id, turn)


# ── robustness ───────────────────────────────────────────────────────────────


def test_no_evidence_edge_keeps_skeleton_geometry():
    h, d = crossing_shapes(90.0)
    lg = build([h, d])
    before = {e.edge_id: list(e.coords_xy) for e in lg.edges}
    stats = refit_geometry(lg, [lonlat(h)])  # only the horizontal shape
    assert stats.n_no_evidence == 2, stats
    assert stats.n_refit == 2
    for e in lg.edges:
        ys = {round(y - Y0, 1) for _, y in e.coords_xy}
        vertical = len({round(x - X0, 1) for x, _ in e.coords_xy}) <= 2 \
            and max(abs(v) for v in ys) > 100.0
        if vertical:
            # kept skeleton geometry: unchanged except the terminal
            # snap toward the (H-evidence) node, bounded by its move
            old = np.asarray(before[e.edge_id])
            new_pts = np.asarray(e.coords_xy)
            assert abs(len(old) - len(new_pts)) <= 2
            assert np.hypot(*(new_pts[-1] - old[-1])) < 1.5 or \
                np.hypot(*(new_pts[0] - old[0])) < 1.5


def test_misattributed_refit_is_capped_to_skeleton():
    track = [(0.0, 0.0), (600.0, 0.0)]
    lg = build([track])
    before = [list(e.coords_xy) for e in lg.edges]
    # a shape 25 m off the track: within the snap radius (36 m) and the
    # deviation gate (50 m), so the coarse pass attributes it — but a
    # refit landing 25 m off the skeleton exceeds the merge width (18 m)
    # and must be rejected as mis-attribution
    ghost = [(0.0, 25.0), (600.0, 25.0)]
    stats = refit_geometry(lg, [lonlat(ghost)])
    assert stats.n_capped == 1, stats
    assert stats.n_refit == 0
    assert [list(e.coords_xy) for e in lg.edges] == before


def test_loop_shape_contributes_one_subpolyline_per_pass():
    # lollipop: out the trunk, around the balloon, back the trunk — the
    # shape passes every trunk point TWICE; each pass must contribute
    # its own sub-polyline (projecting endpoints onto the whole shape
    # would span the balloon and be rejected, leaving no evidence)
    trunk_out = [(0.0, 0.0), (400.0, 0.0)]
    balloon = [
        (400.0 + 150.0 + 150.0 * math.cos(math.radians(a)),
         150.0 * math.sin(math.radians(a)))
        for a in range(180, -180, -10)
    ]
    shape = trunk_out + balloon + [(400.0, 0.0), (0.0, 0.0)]
    lg = build([shape])
    stats = refit_geometry(lg, [lonlat(shape)])
    trunk_edges = [
        e for e in lg.edges
        if e.length_m > 100.0 and all(abs(y) < 30.0 for _, y in local_xy(e))
    ]
    assert trunk_edges, [local_xy(e)[:2] for e in lg.edges]
    assert stats.n_refit >= 1
    # two passes averaged: the trunk stays dead straight on its axis
    for e in trunk_edges:
        assert chord_dev(e.coords_xy) < 1.0
        for _, y in local_xy(e):
            assert abs(y) <= 1.5


def test_refit_is_deterministic():
    h, d = crossing_shapes(30.0)

    def run():
        lg = build([h, d])
        refit_geometry(lg, [lonlat(h), lonlat(d)])
        return (
            [(n.node_id, n.lon, n.lat, n.x, n.y) for n in lg.nodes],
            [(e.edge_id, e.from_node, e.to_node, e.length_m,
              tuple(map(tuple, e.coords_xy))) for e in lg.edges],
        )

    assert run() == run()
