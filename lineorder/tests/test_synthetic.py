"""Per-rule synthetic instances: each rule fires on a tiny hand-built
graph and the reconstruction round-trips with exact score accounting and
a globally optimal result (verified against exhaustive search on the
ORIGINAL graph). One deliberate exception is pinned by
test_p1_station_flanked_corridor_stability: the reductions optimize over
the corridor-stable subspace, not the unconstrained optimum (see
lineorder/reduce.py, "Optimality semantics")."""

import itertools
import math

from lineorder.model import build_graph
from lineorder.reconstruct import reconstruct
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


def test_p1_station_flanked_corridor_stability():
    """Pinned trade-off: the cascade optimizes over the CORRIDOR-STABLE
    subspace, not the unconstrained optimum (reduce.py "Optimality
    semantics"). Lines A,B share corridor t1-m-t2 (m deg-2 non-station,
    w_same = 4*2 = 8) and diverge at BOTH ends, deg-3 stations
    (w_diff = 3*3 = 9); geometry forces opposite orders at the two ends,
    so exactly one crossing is unavoidable. The unconstrained optimum
    crosses mid-corridor at m for 8.0, but that changes a slot inside a
    corridor — P1 contracts m unconditionally and the reductions take
    the corridor-stable 9.0 junction crossing instead, by design."""
    inst, _, node_ids = build_graph(
        {"t1": (0, 0), "m": (10, 0), "t2": (20, 0),
         "p1": (-10, 5), "q1": (-10, -5),   # A exits NW, B exits SW at t1
         "p2": (30, -5), "q2": (30, 5)},    # A exits SE, B exits NE at t2
        [("t1", "m", ["A", "B"]),
         ("m", "t2", ["A", "B"]),
         ("t1", "p1", ["A"]), ("t1", "q1", ["B"]),
         ("t2", "p2", ["A"]), ("t2", "q2", ["B"])],
        stations=("t1", "t2"),
    )
    g, reg = inst.graph, inst.registry
    w = Weights.for_graph(g)
    assert w.w_same(g, node_ids["m"]) == 8.0
    assert w.w_diff(g, node_ids["t1"]) == 9.0

    # unconstrained optimum: one same-seg crossing at the deg-2 node
    _, best = brute_force(g, reg, w)
    assert best.weighted == 8.0
    assert (best.crossings_same, best.crossings_diff) == (1, 0)

    # cascade: corridor-stable, one junction crossing, cost 9.0
    red, _, orig_score, _ = roundtrip(inst)
    assert red.stats["P1"] >= 1
    assert abs(orig_score.weighted - 9.0) < 1e-9, (
        "corridor-stable semantic changed: expected the deliberate 9.0 "
        f"junction crossing over the unstable 8.0, got "
        f"{orig_score.weighted}")
    assert (orig_score.crossings_same, orig_score.crossings_diff) == (0, 1)


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
    # b carries an unrelated leg E (disjoint from L(e)): U2 cannot fire
    # (union of ALL minors != L(e)), U3 can — the used minors carry
    # exactly their threads
    inst, _, _ = build_graph(
        {"t": (0, 0), "b": (10, 0), "ne": (20, 10), "se": (20, -10),
         "x": (10, 10)},
        [("t", "b", ["A", "B", "C"]),
         ("b", "ne", ["A", "B"]),
         ("b", "se", ["C"]),
         ("b", "x", ["E"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U3"] >= 1
    assert red.stats["U2"] == 0


def test_u3_minor_extra_lines_blocked():
    # D rides both minor legs through b but never enters the major leg:
    # U3 must NOT fire (extra lines on the used minors), yet the cascade
    # still lands on the optimum via the remaining rules + brute force
    inst, _, _ = build_graph(
        {"t": (0, 0), "b": (10, 0), "ne": (20, 10), "se": (20, -10)},
        [("t", "b", ["A", "B", "C"]),
         ("b", "ne", ["A", "B", "D"]),
         ("b", "se", ["C", "D"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U3"] == 0


def test_u3_extra_line_interleave_regression():
    # review finding: with extra line D on minor (b,q), the optimum
    # sandwiches L4 INSIDE the {L1,L2} thread on (b,t) — paying a diff
    # crossing at b to dodge the separation that D (pinned between L1
    # and L2 at q by its far-end leg) would otherwise force. Block
    # concatenation cannot represent that order, so U3 must not split.
    inst, _, _ = build_graph(
        {"t": (10, 0), "b": (0, 0), "q": (-10, -5), "s2": (-10, 5),
         "a1": (-20, 5), "ad": (-20, -5), "a2": (-20, -15)},
        [("b", "t", ["L1", "L2", "L4"]),
         ("b", "q", ["L1", "L2", "D"]),
         ("b", "s2", ["L4"]),
         ("q", "a1", ["L1"]),
         ("q", "ad", ["D"]),
         ("q", "a2", ["L2"])],
    )
    red, _, _ = assert_optimal(inst)
    assert red.stats["U3"] == 0


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


def _az_xy(cx, cy, az_deg, r=10.0):
    a = math.radians(az_deg)
    return (cx + r * math.sin(a), cy + r * math.cos(a))


def _dy4_instance(perm_u, perm_v, extra_d=False, stations=()):
    """Degree-4 double Y: trunk u-v carries three single-line threads;
    the leg azimuths put the trunk INTERIOR in both nodes' absolute
    azimuth-sorted orders, so the residual split node's cyclic order
    differs from the absolute list (the U4/U5 rotation trap).
    perm_u/perm_v assign the threads to the fixed leg azimuths;
    extra_d adds a private-line leg at v, making v a partial side."""
    node_xy = {"u": (0.0, 0.0), "v": (10.0, 0.0)}
    edges = [("u", "v", ["A", "B", "C"])]
    for ln, az in zip(perm_u, (30, 150, 210)):
        node_xy[f"u{az}"] = _az_xy(0.0, 0.0, az)
        edges.append((f"u{az}", "u", [ln]))
    for ln, az in zip(perm_v, (30, 150, 330)):
        node_xy[f"v{az}"] = _az_xy(10.0, 0.0, az)
        edges.append(("v", f"v{az}", [ln]))
    if extra_d:
        node_xy["vd"] = _az_xy(10.0, 0.0, 250)
        edges.append(("v", "vd", ["D"]))
    return build_graph(node_xy, edges, stations=stations)


def test_u4_degree4_azimuth_sweep():
    # all thread-to-azimuth assignments at both ends: every configuration
    # must reconstruct to the exhaustive optimum with exact accounting;
    # station variants exercise the weight-aware side choice with 3
    # threads (unequal w_diff at the two ends)
    perms = list(itertools.permutations(["A", "B", "C"]))
    for stations in ((), ("u",), ("v",)):
        fired = 0
        for pu in perms:
            for pv in perms:
                inst, _, _ = _dy4_instance(pu, pv, stations=stations)
                red, _, _ = assert_optimal(inst)
                fired += red.stats["U4"]
        assert fired >= len(perms) ** 2  # fires again on the residual DY


def test_u5_degree4_azimuth_sweep():
    # partial double Y (extra private leg at v): U5 must realize the
    # unavoidable thread crossings on the CHEAPER side — plain gives
    # w_diff(u)=4 < w_diff(v)=5 (crossings belong at u), station u
    # inverts the preference (crossings belong at v)
    perms = list(itertools.permutations(["A", "B", "C"]))
    for stations in ((), ("u",), ("v",)):
        fired = 0
        for pu in perms:
            for pv in perms:
                inst, _, _ = _dy4_instance(pu, pv, extra_d=True,
                                           stations=stations)
                red, _, _ = assert_optimal(inst)
                fired += red.stats["U5"]
        assert fired >= len(perms) ** 2


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


def test_p2_any_reduced_solution_accounting():
    # review finding: a line fully crossing a collapsed block at a node
    # incurs TWO expanded separations (one per boundary member). The
    # accounting identity must hold for ANY reduced solution — including
    # the block-crossing ones a phase-B solver will evaluate — not just
    # for brute-forced optima.
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 0), "d": (20, 0), "x": (10, -10)},
        [("a", "b", ["m1", "m2", "c"]),
         ("b", "d", ["m1", "m2", "c"]),
         ("b", "x", ["c"])],  # c-only leg at b keeps c out of the block
    )
    w = Weights.for_graph(inst.graph)
    red = reduce_graph(inst, w)
    assert red.stats["P2"] >= 1
    g = red.graph
    free = [eid for eid in g.edges if len(g.edges[eid].lines) > 1]
    for combo in itertools.product(
            *(itertools.permutations(g.edges[eid].lines) for eid in free)):
        sol = {eid: g.edges[eid].lines for eid in g.edges}
        sol.update(zip(free, combo))
        full = reconstruct(red, sol)
        orig = score(inst.graph, red.registry, full, w)
        comp = score(g, red.registry, sol, w)
        assert abs(orig.weighted
                   - (comp.weighted + red.fixed_cost)) < 1e-9, (
            f"accounting broken for reduced sol {sol}: original "
            f"{orig.weighted} != {comp.weighted} + {red.fixed_cost}")


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


def test_merge_then_split_coordinates_zero_crossings():
    """A merge-then-split of two rigid 2-line blocks with CONSISTENT exit
    geometry must order the shared bundle so the pair joins without crossing
    AND splits without crossing (the split's required exit order propagates
    back through the corridor to the merge). This is the Eastern Parkway
    2/3 x 4/5 shape idealized: when red and green ARE rigid blocks end to
    end, ZERO crossings is achievable and the solver must find it. (Real
    Eastern Parkway is NOT this — the Nostrand split pairs 2/5 and 3/4, so
    the blocks are not rigid and the crossing is a genuine objective
    tradeoff; see tools/sandbox/crossing_audit.py.)"""
    # A={a1,a2} from the west, B={b1,b2} from the southwest, merge at M, run
    # bundled east to S, split into A (northeast) and B (east) — exit sides
    # consistent with entry sides, so no crossing is forced anywhere.
    inst, lids, _ = build_graph(
        {"Wa": (-2, 1), "Wb": (-2, -1), "M": (0, 0), "S": (3, 0),
         "Ea": (5, 1), "Eb": (5, -1)},
        [("Wa", "M", ["a1", "a2"]),
         ("Wb", "M", ["b1", "b2"]),
         ("M", "S", ["a1", "a2", "b1", "b2"]),
         ("S", "Ea", ["a1", "a2"]),
         ("S", "Eb", ["b1", "b2"])],
    )
    from lineorder.solve import SolveConfig, solve_instance
    out = solve_instance(inst, SolveConfig(seed=0, jobs=1))
    assert out.after.crossings_same == 0
    assert out.after.crossings_diff == 0
    assert out.after.weighted == 0.0

    # sanity: the crossed bundle order (interleaved a1 b1 a2 b2) is strictly
    # worse, so 0 is not vacuous — the solver actively coordinated the ends.
    g, reg, w = inst.graph, out.reduction.registry, out.reduction.weights
    mid = next(eid for eid, e in g.edges.items() if len(e.lines) == 4)
    crossed = dict(out.full_solution)
    crossed[mid] = (lids["a1"], lids["b1"], lids["a2"], lids["b2"])
    assert score(g, reg, crossed, w).weighted > 0.0
