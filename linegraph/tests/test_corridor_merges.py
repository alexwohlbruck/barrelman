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
    gap = cfg.cross_family_gap_m  # 22 m threshold (round 19)
    release = cfg.release_gap_mult * gap  # 33 m

    def y2(x):
        # 8 m gap, breathing just ABOVE the threshold but BELOW the
        # release gap (a bundle that widens gently, never lets go) for
        # 150 m twice, and one short 120 m dip beyond the release gap
        # (shorter than window_dip_coalesce_m, so it still coalesces)
        if 400 <= x < 550:
            return gap + 4.0            # 26 m: above 22, below 33
        if 900 <= x < 1020:
            return release + 5.0        # 38 m: beyond release, short dip
        if 1400 <= x < 1550:
            return gap + 4.0
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
    release = cfg.release_gap_mult * cfg.cross_family_gap_m  # 33 m

    def y2(x):
        # a genuine release: 400 m clearly beyond the release gap
        # (round 19: > 1.5 x 22 m) mid-corridor
        if 800 <= x < 1200:
            return release + 15.0       # 48 m
        return 8.0

    c1 = Corr(0, frozenset({"A"}), frozenset({"red"}), 0, 1,
              _line(0.0, 0.0, 2000.0))
    c2 = Corr(1, frozenset({"B"}), frozenset({"blue"}), 2, 3,
              _line(None, 0.0, 2000.0, y=y2))
    wins, _cum = _windows(c1, c2, cfg.cross_family_gap_m,
                          cfg.midline_step_m, cfg)
    assert len(wins) == 2, f"genuine divergence must split: {wins}"


# ── E: round-19 bundle tolerance + anti-kiss gates ───────────────────────────


def test_wide_parallel_cross_family_bundles_at_raised_gap():
    """A genuine cross-family parallel a bit farther apart than the old
    10 m gap (DeKalb's orange B/D beside yellow N/Q down the Manhattan
    Bridge approach: a stable ~16 m gap, dead parallel, no crossing) must
    BUNDLE at the raised 22 m gap — the under-bundling this round fixes."""
    cfg = WaygraphConfig()
    assert cfg.cross_family_gap_m >= 16.0, "gap must clear a 16 m parallel"
    st = _State(cfg, verbose=False)
    # two straight tracks 16 m apart for 1.5 km (comfortably past
    # cross_family_min_len_m), different families
    c1 = _add(st, {"B", "D"}, {"orange"}, _line(0.0, 0.0, 1500.0))
    c2 = _add(st, {"N", "Q"}, {"yellow"}, _line(16.0, 0.0, 1500.0))
    cand = _try_merge(st, "cross", c1, c2, EPSG)
    assert cand is not None, "a stable 16 m parallel must bundle at gap 22"
    assert cand[5] > cfg.cross_family_min_len_m, "the bundle spans the co-run"


def test_synthetic_kiss_stays_unbundled_at_raised_gap():
    """A KISS — a transient V-shaped convergence where two different
    families cross (Rector 1 x R/W, Whitehall crossing tubes) — must NOT
    bundle even at the raised 22 m gap: it dips under the gap only briefly
    and its geometries CROSS. Distinguished by PROFILE, not gap minimum."""
    cfg = WaygraphConfig()
    st = _State(cfg, verbose=False)

    # c1 straight; c2 dives from far, touches near mid-span, diverges again
    # — a symmetric V that also CROSSES c1 at the closest approach
    def y2(x):
        return (x - 750.0) * 0.20        # crosses y=0 at x=750, slope ~11 deg

    c1 = _add(st, {"1"}, {"red"}, _line(0.0, 0.0, 1500.0))
    c2 = _add(st, {"R", "W"}, {"yellow"}, _line(None, 0.0, 1500.0, y=y2))
    cand = _try_merge(st, "cross", c1, c2, EPSG)
    assert cand is None, "a crossing V-kiss must never bundle"

    # and a NON-crossing but transient near-approach (dips under only
    # briefly within a wide neighbourhood) also stays unbundled
    st2 = _State(cfg, verbose=False)

    def y3(x):
        # 60 m apart, dipping to 12 m only over a 120 m valley near mid
        return 12.0 if 690 <= x <= 810 else 60.0

    d1 = _add(st2, {"1"}, {"red"}, _line(0.0, 0.0, 1500.0))
    d2 = _add(st2, {"R", "W"}, {"yellow"}, _line(None, 0.0, 1500.0, y=y3))
    assert _try_merge(st2, "cross", d1, d2, EPSG) is None, \
        "a brief sub-threshold valley in a wide neighbourhood is a kiss"
