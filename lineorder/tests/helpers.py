"""Shared test machinery: reduce -> brute-force components -> reconstruct
-> validate the two contract properties:

  1. exact score accounting (for the solutions we produced):
         score(original, reconstructed)
             == sum(component scores) + reduction.fixed_cost
  2. optimality preservation:
         score(original, reconstructed) == brute-force optimum(original)

Property 2 (assert_optimal) only holds where the corridor-stable optimum
equals the unconstrained one — true for these instances by construction;
the deliberate divergence is pinned separately by
test_synthetic.test_p1_station_flanked_corridor_stability (see
lineorder/reduce.py, "Optimality semantics").
"""

from __future__ import annotations

from lineorder.reduce import ALL_RULES, reduce_graph
from lineorder.reconstruct import reconstruct
from lineorder.score import Score, Weights, brute_force, score


def roundtrip(inst, rules=ALL_RULES, weights=None, comp_max_space=200_000):
    """Reduce, solve every component exhaustively, reconstruct.
    Returns (reduction, full_solution, original_score, component_total)."""
    w = weights or Weights.for_graph(inst.graph)
    red = reduce_graph(inst, w, rules)
    reduced_sol = {}
    comp_total = Score()
    for comp in red.components():
        sol, sc = brute_force(red.graph, red.registry, w,
                              edges=comp.edges, nodes=comp.nodes,
                              max_space=comp_max_space)
        reduced_sol.update(sol)
        comp_total = comp_total + sc
    full = reconstruct(red, reduced_sol)
    orig_score = score(inst.graph, red.registry, full, w)
    assert abs(orig_score.weighted
               - (comp_total.weighted + red.fixed_cost)) < 1e-9, (
        f"accounting broken: original {orig_score.weighted} != "
        f"components {comp_total.weighted} + fixed {red.fixed_cost}")
    return red, full, orig_score, comp_total


def assert_optimal(inst, rules=ALL_RULES, weights=None, max_space=2_000_000):
    """roundtrip() + compare against the exhaustive optimum of the
    ORIGINAL (unreduced) instance."""
    w = weights or Weights.for_graph(inst.graph)
    red, full, orig_score, _ = roundtrip(inst, rules, w)
    _, best = brute_force(inst.graph, red.registry, w, max_space=max_space)
    assert abs(orig_score.weighted - best.weighted) < 1e-9, (
        f"reconstructed {orig_score.weighted} != optimum {best.weighted} "
        f"(stats {dict(red.stats)})")
    return red, full, orig_score
