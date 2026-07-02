"""Per-rule synthetic instances: each rule fires on a tiny hand-built
graph and the reconstruction round-trips with exact score accounting and
a globally optimal result (verified against exhaustive search on the
ORIGINAL graph)."""

import math

from lineorder.model import build_graph
from lineorder.reduce import reduce_graph
from lineorder.score import Weights, brute_force, score, search_space

from .helpers import assert_optimal, roundtrip


def test_p2_partner_collapse():
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 0), "c": (20, 0), "d": (20, 10)},
        [("a", "b", ["A", "B"]),
         ("b", "c", ["A", "B", "C"]),
         ("c", "d", ["C"]),
         ("b", "d", ["C"])],  # C's edge set differs from A/B's
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["P2"] >= 1


def test_p1_contraction_chain():
    # exclude P2 so the corridor keeps 2 real lines through deg-2 nodes
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 1), "c": (20, 0), "d": (30, 1)},
        [("a", "b", ["A", "B"]),
         ("b", "c", ["A", "B"]),
         ("c", "d", ["A", "B"])],
    )
    rules = tuple(r for r in ("P1", "P3", "C1", "C2") if True)
    red, _, _ = assert_optimal(inst, rules=rules)
    assert red.stats["P1"] >= 2
    assert red.stats["P3"] >= 1  # the merged corridor is double-terminus


def test_p3_double_terminus():
    inst, _, _ = build_graph(
        {"u": (0, 0), "v": (10, 0), "a": (-10, 5), "b": (20, 5)},
        [("u", "v", ["A", "B"]),
         ("a", "u", ["C"]),
         ("v", "b", ["D"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["P3"] >= 1


def test_c1_single_line_cut_components():
    # two 2-line bundles joined by a single-line bridge -> 2 components
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 0), "c": (20, 0), "d": (30, 0),
         "a2": (0, 10), "d2": (30, 10)},
        [("a", "b", ["A", "B"]),
         ("a2", "b", ["A", "B"]),
         ("b", "c", ["A"]),
         ("c", "d", ["A", "C"]),
         ("c", "d2", ["A", "C"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["C1"] >= 1
    multi = [c for c in red.components()
             if any(len(red.graph.edges[e].lines) > 1 for e in c.edges)]
    assert len(multi) >= 2


def test_c2_terminus_detachment():
    # A,B both END at junction v (their orders there are free); A gets a
    # private tail so {A,B} cannot partner-collapse into a single line
    inst, _, _ = build_graph(
        {"a2": (-10, 5), "a": (0, 0), "v": (10, 0),
         "b": (20, 5), "c": (20, -5)},
        [("a2", "a", ["A"]),
         ("a", "v", ["A", "B"]),
         ("v", "b", ["C"]),
         ("v", "c", ["C", "D"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["C2"] >= 1


def test_u1_full_x():
    inst, _, _ = build_graph(
        {"v": (0, 0), "nw": (-10, 10), "se": (10, -10),
         "ne": (10, 10), "sw": (-10, -10)},
        [("nw", "v", ["A", "B"]),
         ("v", "se", ["A", "B"]),
         ("ne", "v", ["C", "D"]),
         ("v", "sw", ["C", "D"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U1"] >= 1
    # X untangles into two independent threads which then fully collapse
    assert search_space(red.graph) == 1


def test_u2_full_y():
    inst, _, _ = build_graph(
        {"t": (0, 0), "b": (10, 0), "ne": (20, 10), "se": (20, -10)},
        [("t", "b", ["A", "B", "C"]),
         ("b", "ne", ["A", "B"]),
         ("b", "se", ["C"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U2"] >= 1
    # paper: Y over a terminus fully reduces
    assert search_space(red.graph) == 1


def test_u3_partial_y():
    # D rides both minor legs through b but never enters the major leg
    inst, _, _ = build_graph(
        {"t": (0, 0), "b": (10, 0), "ne": (20, 10), "se": (20, -10)},
        [("t", "b", ["A", "B", "C"]),
         ("b", "ne", ["A", "B", "D"]),
         ("b", "se", ["C", "D"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U3"] >= 1


def _double_y(cross: bool):
    # threads {A,B} and {C} join at u, share e, split at v.
    # cross=False: {A,B} stays on the north side at both ends (Fig 19);
    # cross=True: {A,B} switches sides over e (Fig 18, one unavoidable
    # split crossing).
    y = -10 if cross else 10
    return build_graph(
        {"u": (0, 0), "v": (10, 0),
         "hnw": (-10, 10), "isw": (-10, -10),
         "fne": (20, y), "gse": (20, -y)},
        [("hnw", "u", ["A", "B"]),
         ("isw", "u", ["C"]),
         ("u", "v", ["A", "B", "C"]),
         ("v", "fne", ["A", "B"]),
         ("v", "gse", ["C"])],
    )


def test_u4_double_y_parallel():
    inst, _, _ = _double_y(cross=False)
    red, _, _ = assert_optimal(inst)
    assert red.stats["U4"] >= 1
    assert red.fixed_cost == 0


def test_u4_double_y_crossing():
    inst, _, _ = _double_y(cross=True)
    red, _, sc = assert_optimal(inst)
    assert red.stats["U4"] >= 1
    assert red.fixed_cost > 0
    assert sc.crossings_diff >= 1  # the unavoidable crossing survives


def test_u4_weight_aware_endpoint_choice():
    # same structure, but make u a big station: the unavoidable crossing
    # must land at the cheap junction v
    for station, expect_at in (("u", "v"), ("v", "u")):
        inst, _, nodes = _double_y(cross=True)
        # rebuild with a station flag
        node_xy = {"u": (0, 0), "v": (10, 0), "hnw": (-10, 10),
                   "isw": (-10, -10), "fne": (20, -10), "gse": (20, 10)}
        edges = [("hnw", "u", ["A", "B"]), ("isw", "u", ["C"]),
                 ("u", "v", ["A", "B", "C"]), ("v", "fne", ["A", "B"]),
                 ("v", "gse", ["C"])]
        inst, _, nids = build_graph(node_xy, edges, stations=(station,))
        w = Weights.for_graph(inst.graph)
        red, full, _ = assert_optimal(inst, weights=w)
        g = inst.graph
        cheap = nids[expect_at]
        sc_here = score(g, red.registry, full, w, nodes=[cheap])
        assert sc_here.crossings_diff >= 1, (
            f"crossing should sit at {expect_at}")


def test_u5_partial_double_y():
    # like the double Y, but v also passes an unrelated line D between
    # two of its legs, so only u may be split
    inst, _, _ = build_graph(
        {"u": (0, 0), "v": (10, 0),
         "hnw": (-10, 10), "isw": (-10, -10),
         "fne": (20, 10), "gse": (20, -10), "s": (10, -14)},
        [("hnw", "u", ["A", "B"]),
         ("isw", "u", ["C"]),
         ("u", "v", ["A", "B", "C"]),
         ("v", "fne", ["A", "B", "D"]),
         ("v", "gse", ["C"]),
         ("v", "s", ["D"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U5"] >= 1


def test_u6_stump():
    # C attaches to the trunk at u and dies at v; {A,B} continue.
    inst, _, _ = build_graph(
        {"u": (0, 0), "v": (10, 0),
         "hnw": (-10, 10), "gsw": (-10, -10), "fne": (20, 10)},
        [("hnw", "u", ["A", "B"]),
         ("gsw", "u", ["C"]),
         ("u", "v", ["A", "B", "C"]),
         ("v", "fne", ["A", "B"])],
    )
    red, full, _ = assert_optimal(inst)
    assert red.stats["U6"] >= 1
    assert red.stats["U4"] >= 1  # dummy mirror turned it into a full DY


def test_separation_matters():
    # A,B are partners on two edges around a junction where C interleaves
    # only if separations were free — sanity: optimum has 0 separations
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 0), "c": (20, 0), "n": (10, 10)},
        [("a", "b", ["A", "B", "C"]),
         ("b", "c", ["A", "B", "C"]),
         ("b", "n", ["C"])],
    )
    _, _, sc = assert_optimal(inst)
    assert sc.separations == 0


def test_score_brute_force_agreement_random():
    # cross-check the scorer's frame conventions on an asymmetric graph:
    # reductions off vs on must land on the same optimum
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 2), "c": (20, -1), "d": (30, 3),
         "n1": (10, 12), "n2": (20, -11)},
        [("a", "b", ["A", "B"]),
         ("b", "c", ["A", "B", "C"]),
         ("c", "d", ["A", "C"]),
         ("n1", "b", ["C"]),
         ("c", "n2", ["B"])],
    )
    assert_optimal(inst)
