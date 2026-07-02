"""The TSAS 2019 section-4.5 'full solve through untangling' classes:
both must reduce to search-space 1 (no solver needed at all), and the
untangling-derived ordering must equal the exhaustive optimum."""

from lineorder.model import build_graph

from .helpers import assert_optimal


def test_tree_with_disjoint_branches_fig24():
    # tree line graph: children edges pairwise disjoint, union == parent
    inst, _, _ = build_graph(
        {"a": (0, 0), "b": (10, 0), "c": (20, 5), "d": (20, -5),
         "e": (30, 8), "f": (30, 2)},
        [("a", "b", ["A", "B", "C"]),
         ("b", "c", ["A", "B"]),
         ("b", "d", ["C"]),
         ("c", "e", ["A"]),
         ("c", "f", ["B"])],
    )
    red, _, _ = assert_optimal(inst)
    for comp in red.components():
        assert comp.search_space(red.graph) == 1, dict(red.stats)


def test_nested_join_leave_trunk_fig25():
    # lines join the trunk and leave it again, nested (LIFO) on the same
    # side: repeated P1 + double-Y untangling fully solves the instance
    inst, _, _ = build_graph(
        {"t0": (0, 0), "t1": (10, 0), "t2": (20, 0), "t3": (30, 0),
         "t4": (40, 0), "t5": (50, 0),
         "jB": (5, -10), "jC": (15, -10), "lC": (35, -10), "lB": (45, -10)},
        [("t0", "t1", ["A"]),
         ("t1", "t2", ["A", "B"]),
         ("t2", "t3", ["A", "B", "C"]),
         ("t3", "t4", ["A", "B"]),
         ("t4", "t5", ["A"]),
         ("jB", "t1", ["B"]),
         ("jC", "t2", ["C"]),
         ("t3", "lC", ["C"]),
         ("t4", "lB", ["B"])],
    )
    red, _, sc = assert_optimal(inst)
    for comp in red.components():
        assert comp.search_space(red.graph) == 1, dict(red.stats)
    assert sc.crossings_same == 0 and sc.crossings_diff == 0
    assert sc.separations == 0
