"""lineorder.score — the MLNCM-S objective.

Three cost classes per node v (TSAS 2019 sections 3 + 3.4, weights from
the section-6 evaluation setup):

  same-segment crossings  — lines A,B share edges e and f at v and their
        relative order flips between the two cross-sections;
  split (diff-segment) crossings — A,B share e at v but continue into
        different edges f_A != f_B: they cross iff their order in e
        disagrees with the clockwise enumeration pi^v_e of f_A, f_B
        (Lemma 4.4);
  separations             — partners A,B share e and f but are adjacent
        in exactly one of the two.

Weights are evaluated on the ORIGINAL node v* (section 4.1) with the
paper's section-6 scheme:
  non-station:      same 4*deg(v*), diff 1*deg(v*), sep 3*deg(v*)
  station deg == 2: wSx = 4*maxdeg (any crossing), wS|| = 3*maxdeg
  station deg  > 2: same 12*deg(v*), diff 3*deg(v*), sep 9*deg(v*)

Pseudo-lines (P2) count crossings with their multiplicity product;
separations count 1 per partner pair using expanded block adjacency
(only the block-boundary member changes adjacency, see reduce.py notes).

U6 dummy edges are transparent: any crossing/separation term that would
involve a dummy edge is skipped — in the original graph the stump lines
TERMINATE at that node, so no such term exists there either.

`score` works on any OptGraph — the untouched original (for end-to-end
validation) or a reduced component — given a full solution for it.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass

from .model import LineRegistry, OptGraph, ord_arrive, ord_leave


@dataclass(frozen=True)
class Weights:
    """Section-6 weight scheme. max_deg is the ORIGINAL graph's maximum
    node degree (wSx/wS|| must dominate every non-station node)."""
    max_deg: int
    same_seg: float = 4.0
    diff_seg: float = 1.0
    separation: float = 3.0
    station_same: float = 12.0
    station_diff: float = 3.0
    station_sep: float = 9.0

    @classmethod
    def for_graph(cls, g: OptGraph, **kw) -> "Weights":
        return cls(max_deg=max((o.degree for o in g.orig_nodes.values()), default=0), **kw)

    def _o(self, g: OptGraph, nid: int):
        return g.orig_nodes[g.nodes[nid].orig]

    def w_same(self, g: OptGraph, nid: int) -> float:
        o = self._o(g, nid)
        if o.degree == 0:
            return 0.0
        if o.station:
            return self.same_seg * self.max_deg if o.degree == 2 \
                else self.station_same * o.degree
        return self.same_seg * o.degree

    def w_diff(self, g: OptGraph, nid: int) -> float:
        o = self._o(g, nid)
        if o.degree == 0:
            return 0.0
        if o.station:
            return self.same_seg * self.max_deg if o.degree == 2 \
                else self.station_diff * o.degree
        return self.diff_seg * o.degree

    def w_sep(self, g: OptGraph, nid: int) -> float:
        o = self._o(g, nid)
        if o.degree == 0:
            return 0.0
        if o.station:
            return self.separation * self.max_deg if o.degree == 2 \
                else self.station_sep * o.degree
        return self.separation * o.degree

    def dominates(self, g: OptGraph, a: int, b: int) -> bool:
        """All three weights of node a >= those of node b (P1 caveat)."""
        return (self.w_same(g, a) >= self.w_same(g, b)
                and self.w_diff(g, a) >= self.w_diff(g, b)
                and self.w_sep(g, a) >= self.w_sep(g, b))


@dataclass
class Score:
    crossings_same: int = 0
    crossings_diff: int = 0
    separations: int = 0
    weighted: float = 0.0

    def __add__(self, o: "Score") -> "Score":
        return Score(self.crossings_same + o.crossings_same,
                     self.crossings_diff + o.crossings_diff,
                     self.separations + o.separations,
                     self.weighted + o.weighted)


def _positions(reg: LineRegistry, perm) -> dict:
    """Expanded block spans: uid -> (start, end) with pseudo-lines
    occupying mult consecutive slots."""
    pos, at = {}, 0
    for uid in perm:
        m = reg.mult(uid)
        pos[uid] = (at, at + m - 1)
        at += m
    return pos


def _before(pos: dict, a: int, b: int) -> bool:
    return pos[a][0] < pos[b][0]


def _adjacent(pos: dict, a: int, b: int) -> bool:
    (a0, a1), (b0, b1) = pos[a], pos[b]
    return a1 + 1 == b0 or b1 + 1 == a0


def score_node(g: OptGraph, reg: LineRegistry, sol: dict, nid: int,
               w: Weights) -> Score:
    s = Score()
    inc = g.order.get(nid, [])
    if len(inc) < 2:
        return s
    arrive = {eid: _positions(reg, ord_arrive(g, sol, eid, nid))
              for eid in inc}
    leave = {eid: _positions(reg, ord_leave(g, sol, eid, nid))
             for eid in inc}
    cw_rank = {eid: {f: k for k, f in enumerate(g.clockwise_from(nid, eid))}
               for eid in inc}

    ws, wd, wp = w.w_same(g, nid), w.w_diff(g, nid), w.w_sep(g, nid)

    for e, f in itertools.combinations(inc, 2):
        ee, ef = g.edges[e], g.edges[f]
        if ee.dummy or ef.dummy:
            continue
        shared = sorted(set(ee.lines) & set(ef.lines))
        for a, b in itertools.combinations(shared, 2):
            # same-segment crossing: order flips between cross-sections
            if _before(arrive[e], a, b) != _before(leave[f], a, b):
                n = reg.mult(a) * reg.mult(b)
                s.crossings_same += n
                s.weighted += ws * n
            # separation: adjacency differs between the two edges
            if _adjacent(arrive[e], a, b) != _adjacent(arrive[f], a, b):
                s.separations += 1
                s.weighted += wp

    for e in inc:
        ee = g.edges[e]
        if ee.dummy:
            continue
        cont = {uid: [f for f in inc
                      if f != e and not g.edges[f].dummy
                      and uid in g.edges[f].lines]
                for uid in ee.lines}
        for a, b in itertools.combinations(sorted(ee.lines), 2):
            for fa in cont[a]:
                for fb in cont[b]:
                    if fa == fb:
                        continue  # same-segment case, handled above
                    if (_before(arrive[e], a, b)
                            != (cw_rank[e][fa] < cw_rank[e][fb])):
                        n = reg.mult(a) * reg.mult(b)
                        s.crossings_diff += n
                        s.weighted += wd * n
    return s


def score(g: OptGraph, reg: LineRegistry, sol: dict, w: Weights,
          nodes=None) -> Score:
    total = Score()
    for nid in (nodes if nodes is not None else g.nodes):
        total = total + score_node(g, reg, sol, nid, w)
    return total


# ------------------------------------------------------------ brute force

def canonical_solution(g: OptGraph, edges=None) -> dict:
    return {eid: g.edges[eid].lines
            for eid in (edges if edges is not None else g.edges)}


def search_space(g: OptGraph, edges=None) -> int:
    n = 1
    for eid in (edges if edges is not None else g.edges):
        n *= math.factorial(len(g.edges[eid].lines))
    return n


def brute_force(g: OptGraph, reg: LineRegistry, w: Weights, edges=None,
                nodes=None, max_space: int = 5_000_000):
    """Exhaustive optimum over the given edge subset (default: all).
    Returns (solution, Score). Test/validation helper — the production
    solver cascade is phase B."""
    edges = list(edges if edges is not None else g.edges)
    space = search_space(g, edges)
    if space > max_space:
        raise ValueError(f"search space {space} exceeds max_space {max_space}")
    free = [eid for eid in edges if len(g.edges[eid].lines) > 1]
    sol = canonical_solution(g, edges)
    if nodes is None:
        touched = set()
        for eid in edges:
            touched.update((g.edges[eid].u, g.edges[eid].v))
        nodes = touched
    best_sol, best = dict(sol), score(g, reg, sol, w, nodes)
    for combo in itertools.product(
            *(itertools.permutations(g.edges[eid].lines) for eid in free)):
        for eid, perm in zip(free, combo):
            sol[eid] = perm
        sc = score(g, reg, sol, w, nodes)
        if sc.weighted < best.weighted:
            best_sol, best = dict(sol), sc
    return best_sol, best
