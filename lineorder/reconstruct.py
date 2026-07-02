"""lineorder.reconstruct — expand a reduced solution to the original graph.

Every reduction rule records a Transform when it rewrites edges; applying
the transforms in REVERSE order maps a solution of the reduced graph
(one permutation per surviving edge, storage frame u -> v) back to a
solution of the original line graph.

Frame conventions (see model.py): a permutation is the left-to-right
line order for a traveler moving along the edge's stored direction
u -> v. TSplit's `ref_at_v` records whether the split's reference node
(the node whose clockwise leg order fixed the block order) is the stored
v-end of the original edge: blocks concatenate (first, second) in the
arrive-frame at the reference node, which in the storage frame is
first+second when ref == v and second+first when ref == u.

Property (asserted in tests): for ANY reduced solution,
    score(original, reconstructed) ==
        sum(score(component)) + reduction.fixed_cost
where fixed_cost accumulates the unavoidable double-Y crossings the U4/U5
rules resolved by clockwise inversion (constant, independent of the
solution). P2 pseudo-line expansion keeps each partner block contiguous
in a consistent traveler frame along the block's path, so it adds no
crossings or separations (Lemma 4.1).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .model import OptGraph


@dataclass
class TCollapse:
    """P2: pseudo-line -> member block. fwd[eid] says whether the members
    read in canonical order along the edge's storage direction."""
    pseudo: int
    members: tuple[int, ...]
    fwd: dict[int, bool]

    def expand(self, sol: dict):
        for eid, forward in self.fwd.items():
            if eid not in sol:
                continue
            out = []
            for uid in sol[eid]:
                if uid == self.pseudo:
                    out.extend(self.members if forward
                               else reversed(self.members))
                else:
                    out.append(uid)
            sol[eid] = tuple(out)


@dataclass
class TContract:
    """P1: edges e1 + e2 merged into `merged` (same line set). *_fwd:
    the child edge's storage direction agrees with the merged edge's."""
    merged: int
    e1: int
    e1_fwd: bool
    e2: int
    e2_fwd: bool

    def expand(self, sol: dict):
        p = sol.pop(self.merged)
        sol[self.e1] = p if self.e1_fwd else tuple(reversed(p))
        sol[self.e2] = p if self.e2_fwd else tuple(reversed(p))


@dataclass
class TPrune:
    """P3: double-terminus edge removed; any order is optimal."""
    eid: int
    lines: tuple[int, ...]

    def expand(self, sol: dict):
        sol[self.eid] = self.lines


@dataclass
class TCut:
    """C1: single-line edge replaced by two stubs."""
    eid: int
    stub1: int
    stub2: int
    line: int

    def expand(self, sol: dict):
        sol.pop(self.stub1, None)
        sol.pop(self.stub2, None)
        sol[self.eid] = (self.line,)


@dataclass
class TSplit:
    """U2/U3/U4/U5: edge split into `first` (lines of the clockwise-first
    minor leg at the reference node) and `second` (the rest)."""
    eid: int
    first: int
    second: int
    ref_at_v: bool

    def expand(self, sol: dict):
        p1 = tuple(sol.pop(self.first))
        p2 = tuple(sol.pop(self.second))
        sol[self.eid] = p1 + p2 if self.ref_at_v else p2 + p1


@dataclass
class TDummy:
    """U6: dummy stump-mirror edges — dropped from the solution."""
    eids: tuple[int, ...] = field(default_factory=tuple)

    def expand(self, sol: dict):
        for eid in self.eids:
            sol.pop(eid, None)


def expand_solution(transforms: list, reduced_sol: dict) -> dict:
    """Apply the recorded transforms in reverse to a reduced solution."""
    sol = {k: tuple(v) for k, v in reduced_sol.items()}
    for t in reversed(transforms):
        t.expand(sol)
    return sol


def reconstruct(reduction, reduced_sol: dict) -> dict:
    """Expand a (possibly partial) reduced solution to per-edge
    permutations of the ORIGINAL graph. Missing reduced edges get their
    canonical order (only legitimate for edges whose order is free)."""
    g: OptGraph = reduction.graph
    sol = {eid: tuple(reduced_sol.get(eid, g.edges[eid].lines))
           for eid in g.edges}
    full = expand_solution(reduction.transforms, sol)

    orig = reduction.original
    out = {}
    for eid, edge in orig.edges.items():
        if eid not in full:
            raise AssertionError(f"edge {eid} missing after expansion")
        perm = full[eid]
        if sorted(perm) != sorted(edge.lines):
            raise AssertionError(
                f"edge {eid}: expanded lines {sorted(perm)} != "
                f"original {sorted(edge.lines)}")
        out[eid] = perm
    return out
