"""Synthetic corridor graphs — v3 segmentation contract unit checks.

Coordinates are metres near (0, 0) lon/lat (graph_from_spec meters=True).
Offsets: offset_px = (ribbon_slot - (ribbon_count-1)/2) * 4.4.
"""

import math

import pytest

from segments.corridors import graph_from_spec, walk_corridors
from segments.segment import SegmentConfig, build_segments, transition_sites
from segments.tests.helpers import (endpoints, find, max_spacing_m,
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
    assert info["merged"] == 2
    assert "merge_offset_mismatch" not in info
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
