"""Merge-boundary regression tests (PAR-12 second live review).

Synthetic corridors driven straight through the way-graph merge
machinery (no OSM, no GTFS):

  C. pair -> single handoff: where a directional-pair midline hands off
     to the single continuing verbatim track, the centerline must leave
     TANGENTIALLY — max lateral step < 0.5 m at the seam, no bend beyond
     a few degrees (it used to step sideways by half the pair gap and
     the downstream fillet rounded the step into a visible wobble).

  D. breathing corridor: real 4-track spacing breathes across the gap
     threshold; a dip that is short (< window_dip_coalesce_m) or that
     never releases (gap <= release_gap_mult x gap sustained
     release_sustain_m) must NOT fragment the window — exactly ONE
     window forms (the Sea Beach pair used to tile into five).

Run:
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_corridor_merges.py -v
"""

import math

import numpy as np

from linegraph.corridors import (Corr, _apply_merge, _State, _try_merge,
                                 _windows)
from linegraph.waygraph import WaygraphConfig

EPSG = 32618  # merge records unproject through this; values are inert


def _mk_state():
    return _State(WaygraphConfig(), verbose=False)


def _line(y0, x_from, x_to, step=10.0, y=None):
    xs = np.arange(x_from, x_to + step / 2, step, dtype=float)
    ys = np.full_like(xs, y0) if y is None else np.array([y(x) for x in xs])
    return np.column_stack([xs, ys])


def _add(st, routes, fams, pts, tracks=1):
    u = st.new_node(pts[0])
    v = st.new_node(pts[-1])
    cid = st.add_corr(routes, fams, u, v, pts, tracks)
    return st.corrs[cid]


# ── C: pair -> single verbatim track handoff ─────────────────────────────────


def test_pair_to_single_handoff_is_tangent():
    st = _mk_state()
    # directional twin ends at x=800 (its switch); the other track is the
    # through track and continues verbatim to x=2000
    c1 = _add(st, {"A"}, {"red"}, _line(8.0, 0.0, 800.0))
    c2 = _add(st, {"A"}, {"red"}, _line(0.0, 0.0, 2000.0))
    cand = _try_merge(st, "pair", c1, c2, EPSG)
    assert cand is not None, "pair window must qualify"
    _apply_merge(st, cand, EPSG)
    st.rechain()

    # midline + continuing tail rechain into ONE through path
    assert len(st.corrs) == 1, [sorted(c.routes) for c in st.corrs.values()]
    c = next(iter(st.corrs.values()))
    path = c.pts if c.pts[0, 0] < c.pts[-1, 0] else c.pts[::-1]

    # zero step at the handoff: past the seam the path IS the continuing
    # verbatim track (y = 0), and the maximum sideways jump between
    # consecutive vertices stays under 0.5 m
    beyond = path[path[:, 0] > 900]
    assert len(beyond) and np.all(np.abs(beyond[:, 1]) < 0.5), \
        f"path leaves the through track by {np.abs(beyond[:, 1]).max():.2f} m"
    lateral = np.abs(np.diff(path[:, 1]))
    assert lateral.max() < 0.5, \
        f"lateral step {lateral.max():.2f} m at the seam (must be < 0.5)"

    # tangent continuity through the easing zone (< 3 deg per vertex; the
    # old 25 m linear ramp put a visible kink at the seam)
    seg = path[1:] - path[:-1]
    keep = np.hypot(seg[:, 0], seg[:, 1]) > 1.0
    seg = seg[keep]
    ang = np.degrees(np.arctan2(seg[:, 1], seg[:, 0]))
    bends = np.abs(np.diff(ang))
    assert bends.max() < 3.0, f"max bend {bends.max():.2f} deg along handoff"

    # interior of the window still the equal-weight midline (y = 4)
    interior = path[(path[:, 0] > 300) & (path[:, 0] < 500)]
    assert np.all(np.abs(interior[:, 1] - 4.0) < 0.3), \
        "window interior must stay on the midline"


def test_cross_bundle_onset_tails_ease_onto_seam():
    # both constituents CONTINUE past the window start (proximity-bundle
    # onset): the cut tails must reach the seam with no kink > 4 deg
    st = _mk_state()

    def y2(x):
        # parallel at 8 m until x=1200, then c2 curves away smoothly
        # (real tracks curve; per-vertex bends of the INPUT stay ~0.2 deg)
        return 8.0 + 1.46e-4 * max(0.0, x - 1200.0) ** 2

    c1 = _add(st, {"A"}, {"red"}, _line(0.0, 0.0, 2400.0))
    c2 = _add(st, {"B"}, {"blue"}, _line(None, 0.0, 2400.0, y=y2))
    cand = _try_merge(st, "cross", c1, c2, EPSG)
    assert cand is not None, "sustained cross bundle must qualify"
    _apply_merge(st, cand, EPSG)
    for c in st.corrs.values():
        pts = c.pts
        seg = pts[1:] - pts[:-1]
        keep = np.hypot(seg[:, 0], seg[:, 1]) > 1.0
        seg = seg[keep]
        if len(seg) < 2:
            continue
        ang = np.degrees(np.arctan2(seg[:, 1], seg[:, 0]))
        bends = np.abs((np.diff(ang) + 180.0) % 360.0 - 180.0)
        assert bends.max() < 4.0, \
            (f"kink {bends.max():.1f} deg on {sorted(c.routes)} "
             f"(boundary residue)")


# ── D: breathing corridor -> exactly one window ──────────────────────────────


def _breathing_pair(cfg):
    gap = cfg.cross_family_gap_m  # 10 m threshold

    def y2(x):
        # 8 m gap, breathing to 12 m (above threshold, below the 15 m
        # release gap) for 150 m twice, and one short 120 m dip to 18 m
        # (beyond release gap but shorter than window_dip_coalesce_m)
        if 400 <= x < 550:
            return 12.0
        if 900 <= x < 1020:
            return 18.0
        if 1400 <= x < 1550:
            return 12.0
        return 8.0

    c1 = Corr(0, frozenset({"A"}), frozenset({"red"}), 0, 1,
              _line(0.0, 0.0, 2000.0))
    c2 = Corr(1, frozenset({"B"}), frozenset({"blue"}), 2, 3,
              _line(None, 0.0, 2000.0, y=y2))
    return c1, c2, gap


def test_breathing_corridor_forms_exactly_one_window():
    cfg = WaygraphConfig()
    c1, c2, gap = _breathing_pair(cfg)
    wins, _cum = _windows(c1, c2, gap, cfg.midline_step_m, cfg)
    assert len(wins) == 1, f"window fragmented: {wins}"
    a, b = wins[0]
    assert a < 50 and b > 1950, f"window must span the co-run: {wins[0]}"


def test_sustained_release_still_splits_the_window():
    cfg = WaygraphConfig()

    def y2(x):
        # a genuine release: 400 m beyond the release gap mid-corridor
        if 800 <= x < 1200:
            return 25.0
        return 8.0

    c1 = Corr(0, frozenset({"A"}), frozenset({"red"}), 0, 1,
              _line(0.0, 0.0, 2000.0))
    c2 = Corr(1, frozenset({"B"}), frozenset({"blue"}), 2, 3,
              _line(None, 0.0, 2000.0, y=y2))
    wins, _cum = _windows(c1, c2, cfg.cross_family_gap_m,
                          cfg.midline_step_m, cfg)
    assert len(wins) == 2, f"genuine divergence must split: {wins}"
