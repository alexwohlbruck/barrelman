"""Synthetic corridor graphs — v3 segmentation contract unit checks.

Coordinates are metres near (0, 0) lon/lat (graph_from_spec meters=True).
Offsets: offset_px = (ribbon_slot - (ribbon_count-1)/2) * 4.4.
"""

import math

import pytest

from segments.corridors import graph_from_spec, walk_corridors
from segments.segment import SegmentConfig, build_segments, transition_sites
from segments.tests.helpers import (MX, MY, endpoints, find, max_spacing_m,
                                    min_radius_near, offset_at_shared_endpoint,
                                    seg_len_m, to_m)

CFG = SegmentConfig()
R, B, G = ("R", "ff0000"), ("B", "0000ff"), ("G", "00ff00")


def build(nodes, edges, cfg=CFG):
    g, ids = graph_from_spec(nodes, edges)
    segs, info = build_segments(g, cfg)
    return g, ids, segs, info


# --------------------------------------------------- Y junction, 2 -> 3

@pytest.fixture(scope="module")
def y_junction():
    return build(
        {"W": (-500, 0), "N": (0, 0), "E": (500, 0), "S": (0, -500)},
        [("W", "N", [R, B]), ("N", "E", [R, B, G]), ("S", "N", [G])])


def test_y_sites_and_counts(y_junction):
    g, ids, segs, info = y_junction
    assert set(info["sites"]) == {ids["N"]}
    assert info["sites"][ids["N"]] == "junction"
    assert len(find(segs, kind="steady")) == 6      # 2 + 3 + 1 ribbons
    assert len(find(segs, kind="transition")) == 3  # R, B, G all move
    assert info["stubs"] == 0 and info["skipped"] == 0


def test_y_continuing_ribbon_offsets(y_junction):
    g, ids, segs, info = y_junction
    (tr,) = find(segs, kind="transition", color_key="ff0000")
    # R: slot 0 of 2 (-2.2 px) -> slot 0 of 3 (-4.4 px), sign per travel frame
    assert {abs(tr.off_from_px), abs(tr.off_to_px)} == {2.2, 4.4}
    (tb,) = find(segs, kind="transition", color_key="0000ff")
    assert {abs(tb.off_from_px), abs(tb.off_to_px)} == {2.2, 0.0}
    (tg,) = find(segs, kind="transition", color_key="00ff00")
    assert {abs(tg.off_from_px), abs(tg.off_to_px)} == {0.0, 4.4}


def test_y_fixed_ground_length(y_junction):
    g, ids, segs, info = y_junction
    (tr,) = find(segs, kind="transition", color_key="ff0000")
    # straight through: two 30 m halves, no fillet shortening
    assert seg_len_m(tr) == pytest.approx(CFG.transition_len_m, rel=0.02)


def test_y_endpoints_meet_steady_and_offsets_match(y_junction):
    g, ids, segs, info = y_junction
    steadies = find(segs, kind="steady")
    for tr in find(segs, kind="transition"):
        matches = [offset_at_shared_endpoint(tr, s) for s in steadies
                   if s.color_key == tr.color_key]
        matches = [m for m in matches if m is not None]
        assert len(matches) == 2, "both transition ends touch a steady"
        for got, expected in matches:
            assert got == pytest.approx(expected, abs=1e-9)


def test_y_densify_spacing(y_junction):
    g, ids, segs, info = y_junction
    for tr in find(segs, kind="transition"):
        assert max_spacing_m(tr) <= CFG.densify_step_m * 1.01


# ------------------------------------------- unchanged-offset skip case

def test_unchanged_offset_skip():
    # R stays at offset 0: alone (slot 0 of 1) then centre of 3 (slot 1),
    # dead straight through the junction -> steady connector, no transition
    A, C = ("A", "00aa00"), ("C", "000088")
    g, ids, segs, info = build(
        {"W": (-500, 0), "N": (0, 0), "E": (500, 0), "S": (0, -500)},
        [("W", "N", [R]), ("N", "E", [A, R, C]), ("S", "N", [A, C])])
    assert info["skipped"] == 1
    assert find(segs, kind="transition", color_key="ff0000") == []
    conn = [s for s in find(segs, kind="steady", color_key="ff0000")
            if s.sites]
    assert len(conn) == 1 and conn[0].offset_px == pytest.approx(0.0)
    assert seg_len_m(conn[0]) == pytest.approx(60.0, rel=0.02)
    # A and C do move (0 +/- 2.2 -> +/- 4.4) and turn: real transitions
    assert len(find(segs, kind="transition")) == 2


# ------------------------------------------- short corridor: merge case

def test_short_corridor_merges_transitions():
    X = ("x", "00aa00")
    g, ids, segs, info = build(
        {"P": (-500, 0), "N1": (0, 0), "N2": (40, 0), "Q": (540, 0),
         "S1": (0, -300), "S2": (40, 300)},
        [("P", "N1", [R]), ("N1", "N2", [R, X]), ("N2", "Q", [R]),
         ("S1", "N1", [X]), ("N2", "S2", [X])])
    assert info["merged"] == 2  # a seam offset mismatch would have raised
    # the 40 m corridor is fully consumed: no CORRIDOR steady piece
    # survives on it (the merged connectors legitimately span it)
    for s in find(segs, kind="steady"):
        if s.corridor_id is None or s.sites:
            continue
        mid = to_m(s.coords)[len(s.coords) // 2]
        assert not (0 < mid[0] < 40 and abs(mid[1]) < 1), \
            "no corridor-steady feature inside the consumed corridor"
    # r: straight through both junctions at equal offsets -> merged piece
    # became a steady connector spanning both sites
    r_conn = [s for s in find(segs, kind="steady", color_key="ff0000")
              if len(s.sites) == 2]
    assert len(r_conn) == 1
    assert seg_len_m(r_conn[0]) == pytest.approx(100.0, rel=0.05)
    # x turns twice: stays one merged transition across both sites
    x_tr = find(segs, kind="transition", color_key="00aa00")
    assert len(x_tr) == 1 and len(x_tr[0].sites) == 2
    assert x_tr[0].off_from_px == pytest.approx(0.0)
    assert x_tr[0].off_to_px == pytest.approx(0.0)


# ------------------------------------- fillet radius + terminating stub

def test_fillet_min_radius_and_stub():
    T = ("T", "888888")
    g, ids, segs, info = build(
        {"W": (-500, 0), "N": (0, 0), "S": (0, -500), "E": (500, 0)},
        [("W", "N", [R, T]), ("N", "S", [R]), ("N", "E", [T])])
    # R turns 90 deg; bundle max 2 ribbons -> target radius 2*4.4*2.5 = 22 m
    (tr,) = find(segs, kind="transition", color_key="ff0000")
    assert not tr.fillet_clamped
    assert tr.fillet_radius_m == pytest.approx(22.0, rel=0.05)
    n_m = to_m([(g.nodes[ids["N"]].lon, g.nodes[ids["N"]].lat)])[0]
    measured = min_radius_near(tr, n_m, within_m=25.0)
    assert 22.0 * 0.9 <= measured <= 22.0 * 1.2
    assert max_spacing_m(tr) <= CFG.densify_step_m * 1.01


def test_fillet_curved_legs_no_seam_kinks():
    """Curved lead-in (raw radius 100 m) into a 90 deg junction turn: the
    accumulated lead-in turn used to be concentrated into single-vertex
    kinks at the fillet seams (PAR-12 v3 review). Seams must be tangent-
    continuous and the whole transition window must meet the min radius."""
    T = ("T", "888888")
    r_lead = 100.0
    nodes = {}
    for k in range(10, 0, -1):  # approach along the arc, heading east at N
        th = -0.075 * k        # 7.5 m steps on r=100
        nodes[f"W{k}"] = (r_lead * math.sin(th), r_lead * (1 - math.cos(th)))
    nodes.update({"N": (0, 0), "S": (0, -500), "E": (500, 0)})
    chain = [f"W{k}" for k in range(10, 0, -1)] + ["N"]
    edges = [(a, b, [R, T]) for a, b in zip(chain, chain[1:])]
    edges += [("N", "S", [R]), ("N", "E", [T])]
    g, ids, segs, info = build(nodes, edges)
    (tr,) = find(segs, kind="transition", color_key="ff0000")
    assert not tr.fillet_clamped
    assert tr.fillet_radius_m >= 22.0 * 0.99
    n_m = to_m([(g.nodes[ids["N"]].lon, g.nodes[ids["N"]].lat)])[0]
    measured = min_radius_near(tr, n_m, within_m=35.0)
    assert measured >= 22.0 * 0.9
    assert max_spacing_m(tr) <= CFG.densify_step_m * 1.01


def test_terminating_ribbon_keeps_offset_to_node():
    # T exists only on W-N and E-N: it terminates at the junction on the
    # W side pairing E<->? no — T is on W-N and N-E, so instead terminate
    # a ribbon that appears on ONE end only.
    T = ("T", "888888")
    Q = ("Q", "004400")
    g, ids, segs, info = build(
        {"W": (-500, 0), "N": (0, 0), "S": (0, -500), "E": (500, 0)},
        [("W", "N", [R, T]), ("N", "S", [R]), ("N", "E", [Q])])
    # T (slot 1 of 2 on W-N) has a single corridor end at N -> stub at
    # constant offset all the way to the node; NO collapse to centreline
    stubs = [s for s in find(segs, kind="steady", color_key="888888")
             if s.sites]
    assert len(stubs) == 1
    stub = stubs[0]
    assert abs(stub.offset_px) == pytest.approx(2.2)
    assert seg_len_m(stub) == pytest.approx(CFG.transition_len_m / 2,
                                            rel=0.05)
    n_m = to_m([(g.nodes[ids["N"]].lon, g.nodes[ids["N"]].lat)])[0]
    ends = endpoints(stub)
    assert min(math.dist(ends[0], n_m), math.dist(ends[1], n_m)) < 0.01
    assert info["stubs"] >= 1


# ------------------------------------ slot lives in the emitted frame

def test_emitted_frame_slot_consistency():
    """Frame-reversed stubs and skipped connectors emit offset_px in the
    feature's travel frame; slot must live in the SAME frame, so
    offset_px == (slot - (line_count-1)/2) * gap_px holds on every steady
    row (PAR-12 v3 review: 5/92 chicago rows had slot on the wrong side).
    Both corridors here are stored AGAINST the travel direction at N."""
    T, T2 = ("T", "888888"), ("T2", "444444")
    g, ids, segs, info = build(
        {"W": (-500, 0), "N": (0, 0), "E": (500, 0)},
        [("N", "W", [R, T]), ("E", "N", [R, T2])])
    # R runs straight through at +2.2 px in both frames -> skip connector
    assert info["skipped"] == 1
    conn = [s for s in find(segs, kind="steady", color_key="ff0000")
            if s.sites]
    assert len(conn) == 1 and conn[0].offset_px == pytest.approx(2.2)
    assert conn[0].slot == 1  # mirrored from storage slot 0
    # T's corridor is stored N->W: its stub at N is frame-reversed
    (t_stub,) = [s for s in find(segs, kind="steady", color_key="888888")
                 if s.sites]
    assert t_stub.offset_px == pytest.approx(-2.2)
    assert t_stub.slot == 0  # mirrored from storage slot 1
    for s in find(segs, kind="steady"):
        expect = (s.slot - (s.line_count - 1) / 2.0) * CFG.gap_px
        assert s.offset_px == pytest.approx(expect, abs=1e-9)


# --------------------------------------- two-end pairing needs evidence

def test_two_end_pairing_requires_shape_evidence():
    """Same colour, different routes, terminating opposite each other at
    one node: matched_shapes contradict a through-transition, so the pair
    demotes to two steady stubs (with a diagnostic). A through pass
    restores the pairing; no shapes at all keeps the old behaviour."""
    def ll(x, y):
        return (x / MX, y / MY)
    A, B2 = ("A", "ff0000"), ("B", "ff0000")
    g, ids = graph_from_spec(
        {"W": (-500, 0), "N": (0, 0), "E": (500, 100)},
        [("W", "N", [A]), ("N", "E", [B2])])
    term_shapes = {("t", "A"): [[ll(-500, 0), ll(0, 0)]],
                   ("t", "B"): [[ll(0, 0), ll(500, 100)]]}
    segs, info = build_segments(g, CFG, shapes=term_shapes)
    assert info.get("two_end_unsupported_sites") == [ids["N"]]
    assert find(segs, kind="transition") == []
    assert info["stubs"] == 2

    thru_shapes = {("t", "A"): [[ll(-500, 0), ll(0, 0), ll(500, 100)]]}
    segs, info = build_segments(g, CFG, shapes=thru_shapes)
    assert not info.get("two_end_unsupported_sites")
    assert len(find(segs, kind="transition")) == 1

    segs, info = build_segments(g, CFG)  # no shapes: evidence not required
    assert not info.get("two_end_unsupported_sites")
    assert len(find(segs, kind="transition")) == 1


def test_two_end_shared_route_pairs_despite_shape_gap():
    """The SAME route on both corridor ends stays paired even when its
    shape stops at the node (GTFS shapes end at the terminal platform
    while the graph's track continues — CTA Red tail at Howard), with a
    shape-gap diagnostic instead of a demotion."""
    def ll(x, y):
        return (x / MX, y / MY)
    A, C = ("A", "ff0000"), ("C", "0000ff")
    g, ids = graph_from_spec(
        {"W": (-500, 0), "N": (0, 0), "E": (500, 100), "S": (0, -500)},
        [("W", "N", [A]), ("N", "E", [A, C]), ("S", "N", [C])])
    shapes = {("t", "A"): [[ll(-500, 0), ll(0, 0)]]}  # stops at the node
    segs, info = build_segments(g, CFG, shapes=shapes)
    assert info.get("two_end_shape_gap_sites") == [ids["N"]]
    assert not info.get("two_end_unsupported_sites")
    assert len(find(segs, kind="transition", color_key="ff0000")) == 1


# ------------------------------- collapsed crossing rung: cusp excision

def test_collapsed_rung_reversal_cusp_excised():
    """A refit-collapsed plan-view X leaves a ~4 m rung between two
    junction nodes; when the node placement overshoots along the through
    direction, the rung points AGAINST travel and the merged through-
    transition retraces it — a self-intersecting micro-hairpin
    (nyc:subway-v3 Borough Hall, segs 521-523). The cusp window must be
    excised and biarc-bridged: the emitted feature is simple, keeps its
    endpoints, and carries no near-reversal turns."""
    from shapely.geometry import LineString

    Q, T = ("Q", "888888"), ("T", "444444")
    # west arm ends at N1(4, 0); the rung runs BACK west to N2(0, 0.4);
    # east arm leaves N2 heading east: traversing W->N1->N2->E reverses
    # direction twice within 4 m. Q/T arms make both nodes junctions.
    g, ids = graph_from_spec(
        {"W": (-496, 0), "N1": (4, 0), "N2": (0, 0.4), "E": (500, 0.4),
         "SA": (44, -300), "SB": (-40, 300)},
        [("W", "N1", [R]), ("N1", "N2", [R]), ("N2", "E", [R]),
         ("N1", "SA", [Q]), ("N2", "SB", [T])])
    segs, info = build_segments(g, CFG)
    assert info["merged"] == 1, "the consumed rung merges the R pair"
    assert info.get("cusp_excised", 0) >= 1
    (tr,) = [s for s in find(segs, kind="transition", color_key="ff0000")
             if len(s.sites) == 2]
    xy = to_m(tr.coords)
    assert LineString(xy).is_simple
    # no near-reversal vertex survives
    for a, b, c in zip(xy, xy[1:], xy[2:]):
        u = (b[0] - a[0], b[1] - a[1])
        v = (c[0] - b[0], c[1] - b[1])
        nu, nv = math.hypot(*u), math.hypot(*v)
        if nu < 1e-9 or nv < 1e-9:
            continue
        dot = max(-1.0, min(1.0, (u[0] * v[0] + u[1] * v[1]) / (nu * nv)))
        assert math.degrees(math.acos(dot)) <= CFG.cusp_turn_deg
    # endpoints preserved at the trim cuts (offset handoff to steadies)
    assert max_spacing_m(tr) <= CFG.densify_step_m * 1.01
    steadies = find(segs, kind="steady", color_key="ff0000")
    matches = [m for s in steadies
               if (m := offset_at_shared_endpoint(tr, s)) is not None]
    assert len(matches) == 2
    for got, expected in matches:
        assert got == pytest.approx(expected, abs=1e-9)


# ------------------------------------------------- corridor walk basics

def test_corridor_walk_joins_deg2_same_set():
    g, ids = graph_from_spec(
        {"A": (0, 0), "B": (100, 0), "C": (200, 0), "D": (300, 0)},
        [("A", "B", [R, B]), ("B", "C", [R, B]), ("C", "D", [R, B])])
    cors = walk_corridors(g)
    assert len(cors) == 1
    assert len(cors[0].steps) == 3
    assert len(cors[0].ribbons) == 2
    assert transition_sites(g) == {}


def test_corridor_composition_change_breaks():
    g, ids = graph_from_spec(
        {"A": (0, 0), "B": (100, 0), "C": (200, 0)},
        [("A", "B", [R, B]), ("B", "C", [R])])
    cors = walk_corridors(g)
    assert len(cors) == 2
    assert transition_sites(g) == {ids["B"]: "composition"}
