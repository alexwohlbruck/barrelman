"""lineorder.reduce — optimality-preserving line-graph reductions.

Implements TSAS 2019 section 4 on the OptGraph model:

  P1 node contraction        (deg-2, equal line sets; v*-weight caveat)
  P2 line partner collapsing (identical edge sets -> pseudo-line)
  P3 double-termini pruning
  C1 single-line cut         (stubs keep continuation info at the ends)
  C2 terminus detachment
  U1 full X, U2 full Y, U3 partial Y, U4 full double Y (weight-aware
  endpoint choice), U5 partial double Y, U6 stump (dummy mirror edges,
  then U4 fires on a later pass)

Loop order per the approved stage-5 design: P2 once, then rounds of
untangle (U2 U3 U4 U5 U6 U1) -> inner fixpoint of contract/prune (P1 P3)
and cut/detach (C1 C2), bounded by maxCardinality+1 untangle rounds and
maxDegree+1 inner rounds (paper Algorithm 1).

U4/U5 unavoidable crossings are constants (they only depend on the
clockwise leg orders, never on the remaining free permutations); they
accumulate in `Reduction.fixed_cost` so that for ANY reduced solution
    score(original, reconstruct(sol)) ==
        sum(component scores) + fixed_cost.

CLI:
  uv run --with-requirements lineorder/requirements.txt \
      python -m lineorder.reduce --build-key chicago:l-v3 --stats
"""

from __future__ import annotations

import itertools
from collections import Counter
from dataclasses import dataclass, field

from .model import DEFAULT_DSN, Instance, LineRegistry, OptGraph
from .reconstruct import (TCollapse, TContract, TCut, TDummy, TPrune,
                          TSplit)
from .score import Weights, search_space

ALL_RULES = ("P1", "P2", "P3", "C1", "C2", "U1", "U2", "U3", "U4", "U5", "U6")


@dataclass
class Component:
    nodes: set
    edges: set

    def max_cardinality(self, g: OptGraph) -> int:
        return max((len(g.edges[e].lines) for e in self.edges), default=0)

    def search_space(self, g: OptGraph) -> int:
        return search_space(g, self.edges)


@dataclass
class Reduction:
    original: OptGraph
    graph: OptGraph
    registry: LineRegistry
    weights: Weights
    transforms: list = field(default_factory=list)
    stats: Counter = field(default_factory=Counter)
    fixed_cost: float = 0.0

    def components(self) -> list[Component]:
        g = self.graph
        parent = {nid: nid for nid in g.nodes}

        def find(a):
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a

        for e in g.edges.values():
            ra, rb = find(e.u), find(e.v)
            if ra != rb:
                parent[ra] = rb
        comps: dict[int, Component] = {}
        for e in g.edges.values():
            r = find(e.u)
            c = comps.setdefault(r, Component(set(), set()))
            c.nodes.update((e.u, e.v))
            c.edges.add(e.eid)
        return sorted(comps.values(),
                      key=lambda c: -c.search_space(self.graph))


# ------------------------------------------------------------------ P2

def _rule_p2(g: OptGraph, reg: LineRegistry, out: Reduction) -> int:
    line_edges: dict[int, frozenset] = {}
    for e in g.edges.values():
        for uid in e.lines:
            line_edges[uid] = line_edges.get(uid, frozenset()) | {e.eid}
    groups: dict[frozenset, list[int]] = {}
    for uid, eids in line_edges.items():
        groups.setdefault(eids, []).append(uid)

    n = 0
    for eids, uids in groups.items():
        if len(uids) < 2:
            continue
        # the shared path must be a simple path/cycle: every node touches
        # at most two of the group's edges (else frame propagation is
        # ill-defined; conservatively skip)
        touch: dict[int, list] = {}
        for eid in eids:
            e = g.edges[eid]
            for nid in (e.u, e.v):
                touch.setdefault(nid, []).append(eid)
        if any(len(v) > 2 for v in touch.values()):
            continue

        # orientation flags: True = members read canonically along the
        # edge's storage direction. Adjacent edges must agree in the
        # traveler frame: b_e XOR (e.v != v) == b_f XOR (f.u != v).
        members = tuple(sorted(uids))
        fwd: dict[int, bool] = {}
        start = next(iter(eids))
        stack = [(start, True)]
        ok = True
        while stack:
            eid, flag = stack.pop()
            if eid in fwd:
                if fwd[eid] != flag:
                    ok = False
                    break
                continue
            fwd[eid] = flag
            e = g.edges[eid]
            for nid in (e.u, e.v):
                for f in touch[nid]:
                    if f == eid:
                        continue
                    fe = g.edges[f]
                    nf = flag ^ (e.v != nid) ^ (fe.u != nid)
                    stack.append((f, nf))
        if not ok or len(fwd) != len(eids):
            continue

        pseudo = reg.add_pseudo(members)
        for eid in eids:
            e = g.edges[eid]
            e.lines = tuple(sorted(
                [u for u in e.lines if u not in uids] + [pseudo.uid]))
        out.transforms.append(TCollapse(pseudo.uid, members, fwd))
        out.stats["P2"] += 1
        n += 1
    return n


# ------------------------------------------------------------------ P1

def _rule_p1(g: OptGraph, w: Weights, out: Reduction) -> int:
    n = 0
    for nid in list(g.nodes):
        if nid not in g.nodes or g.degree(nid) != 2:
            continue
        ea, eb = g.order[nid]
        if ea == eb:
            continue
        e, f = g.edges[ea], g.edges[eb]
        if set(e.lines) != set(f.lines):
            continue
        u, v = g.other(ea, nid), g.other(eb, nid)
        if u == nid or v == nid or u == v:
            continue  # never create a self-loop
        o = g.orig_nodes[g.nodes[nid].orig]
        if o.degree != 2 and len(e.lines) > 1:
            # v*-weight caveat: crossings/separations that could have
            # happened here must be movable to a neighbor at <= cost
            if not (w.dominates(g, nid, u) or w.dominates(g, nid, v)):
                continue
        merged = g.add_edge(u, v, e.lines,
                            dummy=e.dummy and f.dummy,
                            stub=e.stub and f.stub)
        g.replace_slot(u, ea, [merged])
        g.replace_slot(v, eb, [merged])
        del g.edges[ea]
        del g.edges[eb]
        del g.order[nid]
        del g.nodes[nid]
        out.transforms.append(TContract(merged, ea, e.u == u, eb, f.u == nid))
        out.stats["P1"] += 1
        n += 1
    return n


# ------------------------------------------------------------------ P3

def _rule_p3(g: OptGraph, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        if not all(g.terminates(l, eid, e.u) and g.terminates(l, eid, e.v)
                   for l in e.lines):
            continue
        out.transforms.append(TPrune(eid, e.lines))
        g.remove_edge(eid)
        out.stats["P3"] += 1
        n += 1
    return n


# ------------------------------------------------------------------ C1

def _rule_c1(g: OptGraph, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        if len(e.lines) != 1 or e.stub:
            continue
        if g.degree(e.u) <= 1 and g.degree(e.v) <= 1:
            continue  # already its own trivial component
        syn_a = g.add_orig(degree=0, station=False, synthetic=True)
        syn_b = g.add_orig(degree=0, station=False, synthetic=True)
        na, nb = g.add_node(syn_a), g.add_node(syn_b)
        s1 = g.add_edge(e.u, na, e.lines, dummy=e.dummy, stub=True)
        s2 = g.add_edge(nb, e.v, e.lines, dummy=e.dummy, stub=True)
        g.replace_slot(e.u, eid, [s1])
        g.replace_slot(e.v, eid, [s2])
        g.order[na] = [s1]
        g.order[nb] = [s2]
        del g.edges[eid]
        out.transforms.append(TCut(eid, s1, s2, e.lines[0]))
        out.stats["C1"] += 1
        n += 1
    return n


# ------------------------------------------------------------------ C2

def _rule_c2(g: OptGraph, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        for nid in (e.u, e.v):
            if g.degree(nid) <= 1:
                continue
            if not all(g.terminates(l, eid, nid) for l in e.lines):
                continue
            syn = g.add_orig(degree=0, station=False, synthetic=True)
            nn = g.add_node(syn)
            g.detach_edge(nid, eid)
            if e.u == nid:
                e.u = nn
            else:
                e.v = nn
            g.order[nn] = [eid]
            out.stats["C2"] += 1
            n += 1
            break
    return n


# ------------------------------------------------------------------ U1

def _rule_u1(g: OptGraph, out: Reduction) -> int:
    n = 0
    for nid in list(g.nodes):
        if nid not in g.nodes:
            continue
        while nid in g.nodes and g.degree(nid) > 2:
            hit = None
            inc = g.order[nid]
            for ea, eb in itertools.combinations(inc, 2):
                la = set(g.edges[ea].lines)
                if la != set(g.edges[eb].lines):
                    continue
                if any(la & set(g.edges[ei].lines)
                       for ei in inc if ei not in (ea, eb)):
                    continue
                hit = (ea, eb)
                break
            if hit is None:
                break
            ea, eb = hit
            nn = g.add_node(g.nodes[nid].orig)  # v* preserved
            g.order[nn] = [e for e in g.order[nid] if e in (ea, eb)]
            g.order[nid] = [e for e in g.order[nid] if e not in (ea, eb)]
            for x in (ea, eb):
                e = g.edges[x]
                if e.u == nid:
                    e.u = nn
                else:
                    e.v = nn
            out.stats["U1"] += 1
            n += 1
    return n


# ----------------------------------------------------------- U2/U3 (Y)

def _split_edge(g: OptGraph, eid: int, block_a: set, block_b: set):
    """Replace edge eid with two edges of the same orientation carrying
    block_a / block_b. Endpoints are NOT attached — callers wire nodes
    and angular orders."""
    e = g.edges[eid]
    e1 = g.add_edge(e.u, e.v, block_a, dummy=e.dummy)
    e2 = g.add_edge(e.u, e.v, block_b, dummy=e.dummy)
    del g.edges[eid]
    return e1, e2


def _rule_u2(g: OptGraph, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        for b, t in ((e.u, e.v), (e.v, e.u)):
            if g.degree(t) != 1 or g.degree(b) < 3:
                continue
            minors = g.clockwise_from(b, eid)
            lsets = [set(g.edges[m].lines) for m in minors]
            if any(la & lb for la, lb in itertools.combinations(lsets, 2)):
                continue
            if set().union(*lsets) != set(e.lines):
                continue
            first = lsets[0]
            rest = set(e.lines) - first
            e1, e2 = _split_edge(g, eid, first, rest)
            # split b: b keeps [e1, minors[0]]; new node takes the rest
            nb = g.add_node(g.nodes[b].orig)
            g.order[nb] = [e2] + minors[1:]
            g.order[b] = [e1, minors[0]]
            for m in minors[1:]:
                me = g.edges[m]
                if me.u == b:
                    me.u = nb
                else:
                    me.v = nb
            # split t: t keeps e1; new node takes e2
            nt = g.add_node(g.nodes[t].orig)
            g.order[t] = [e1]
            g.order[nt] = [e2]
            for new_e, bn, tn in ((e1, b, t), (e2, nb, nt)):
                ed = g.edges[new_e]
                if ed.u == b:
                    ed.u, ed.v = bn, tn
                else:
                    ed.u, ed.v = tn, bn
            out.transforms.append(
                TSplit(eid, e1, e2, ref_at_v=(e.v == b)))
            out.stats["U2"] += 1
            n += 1
            break
    return n


def _rule_u3(g: OptGraph, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        for b, t in ((e.u, e.v), (e.v, e.u)):
            if g.degree(t) != 1 or g.degree(b) < 3:
                continue
            lines = set(e.lines)
            # each major-leg line continues into exactly one minor leg
            cont = {l: [m for m in g.order[b]
                        if m != eid and l in g.edges[m].lines]
                    for l in lines}
            if any(len(v) != 1 for v in cont.values()):
                continue
            minors = [m for m in g.clockwise_from(b, eid)
                      if set(g.edges[m].lines) & lines]
            if len(minors) < 2:
                continue
            first = {l for l in lines if cont[l][0] == minors[0]}
            rest = lines - first
            e1, e2 = _split_edge(g, eid, first, rest)
            # only t splits; both new edges stay attached at b in the old
            # slot (their relative sub-slot order is score-irrelevant:
            # no term can compare pi(e1) with pi(e2), see reconstruct.py)
            g.replace_slot(b, eid, [e2, e1])
            nt = g.add_node(g.nodes[t].orig)
            g.order[t] = [e1]
            g.order[nt] = [e2]
            for new_e, tn in ((e1, t), (e2, nt)):
                ed = g.edges[new_e]
                if ed.u == t:
                    ed.u = tn
                elif ed.v == t:
                    ed.v = tn
                # b endpoint already correct (copied from e)
            # fix the e1 endpoint that pointed at t: handled above; e2's
            # t-side now nt
            out.transforms.append(
                TSplit(eid, e1, e2, ref_at_v=(e.v == b)))
            out.stats["U3"] += 1
            n += 1
            break
    return n


# ------------------------------------------------------ U4/U5 (double Y)

def _thread_partition(g: OptGraph, eid: int, nid: int):
    """Partition L(e) by the single other edge each line continues into
    at nid. Returns (legs clockwise from e, {leg: line set}) or None if
    any line terminates/branches or a leg carries none of L(e)."""
    e = g.edges[eid]
    lines = set(e.lines)
    cont = {}
    for l in lines:
        others = [m for m in g.order[nid]
                  if m != eid and l in g.edges[m].lines]
        if len(others) != 1:
            return None
        cont[l] = others[0]
    legs = [m for m in g.clockwise_from(nid, eid) if m in set(cont.values())]
    blocks = {m: {l for l in lines if cont[l] == m} for m in legs}
    return legs, blocks


def _full_side(g: OptGraph, eid: int, nid: int):
    """U4-grade side: every incident edge at nid other than e is a minor
    leg whose FULL line set is one thread of L(e) (pairwise disjoint,
    union == L(e), no extra lines). Returns (legs, blocks) or None."""
    p = _thread_partition(g, eid, nid)
    if p is None:
        return None
    legs, blocks = p
    if len(legs) != g.degree(nid) - 1 or len(legs) < 2:
        return None
    if any(set(g.edges[m].lines) != blocks[m] for m in legs):
        return None
    return p


def _mult(reg: LineRegistry, uids) -> int:
    return sum(reg.mult(u) for u in uids)


def _dy_fixed_cost(g: OptGraph, reg, w: Weights, s: int, s_rank: dict,
                   blocks0, target0, others) -> float:
    """Unavoidable crossings at the non-reference node s between the
    extracted first thread and each remaining thread j: they cross iff
    the s-side clockwise ranks run in the SAME direction as at the
    reference node (crossing iff rank_s(t_0) < rank_s(t_j))."""
    cost = 0.0
    for tj, blockj in others:
        if s_rank[target0] < s_rank[tj]:
            cost += w.w_diff(g, s) * _mult(reg, blocks0) * _mult(reg, blockj)
    return cost


def _rule_u4(g: OptGraph, reg, w: Weights, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        u, v = e.u, e.v
        if u == v or g.degree(u) < 3 or g.degree(v) < 3:
            continue
        if g.degree(u) != g.degree(v):
            continue
        pu = _full_side(g, eid, u)
        pv = _full_side(g, eid, v)
        if pu is None or pv is None:
            continue
        legs_u, blk_u = pu
        legs_v, blk_v = pv
        # bijection: each left thread equals exactly one right thread
        match = {}
        for lu in legs_u:
            hits = [lv for lv in legs_v if blk_v[lv] == blk_u[lu]]
            if len(hits) != 1:
                match = None
                break
            match[lu] = hits[0]
        if not match:
            continue

        rank = {nid: {f: k for k, f in
                      enumerate(g.clockwise_from(nid, eid))}
                for nid in (u, v)}
        # candidate ref u (crossings land at v) vs ref v (land at u)
        cost_u = _dy_fixed_cost(
            g, reg, w, v, rank[v], blk_u[legs_u[0]], match[legs_u[0]],
            [(match[l], blk_u[l]) for l in legs_u[1:]])
        inv = {vv: kk for kk, vv in match.items()}
        cost_v = _dy_fixed_cost(
            g, reg, w, u, rank[u], blk_v[legs_v[0]], inv[legs_v[0]],
            [(inv[l], blk_v[l]) for l in legs_v[1:]])
        if cost_u <= cost_v:
            ref, refl, refblk, cost = u, legs_u, blk_u, cost_u
        else:
            ref, refl, refblk, cost = v, legs_v, blk_v, cost_v
        other_node = v if ref == u else u

        first = refblk[refl[0]]
        rest = set(e.lines) - first
        e1, e2 = _split_edge(g, eid, first, rest)
        for nid, legs in ((u, legs_u), (v, legs_v)):
            leg0 = next(l for l in legs
                        if set(g.edges[l].lines) == first)
            nn = g.add_node(g.nodes[nid].orig)
            keep = [m for m in g.order[nid] if m not in (eid, leg0)]
            g.order[nid] = [e1, leg0]
            g.order[nn] = [e2] + keep
            for m in keep:
                me = g.edges[m]
                if me.u == nid:
                    me.u = nn
                else:
                    me.v = nn
            for new_e, tgt in ((e1, nid), (e2, nn)):
                ed = g.edges[new_e]
                if ed.u == nid:
                    ed.u = tgt
                elif ed.v == nid:
                    ed.v = tgt
        out.transforms.append(TSplit(eid, e1, e2, ref_at_v=(e.v == ref)))
        out.fixed_cost += cost
        out.stats["U4"] += 1
        n += 1
    return n


def _rule_u5(g: OptGraph, reg, w: Weights, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        if e.u == e.v:
            continue
        for r, s in ((e.u, e.v), (e.v, e.u)):
            if g.degree(r) < 3:
                continue
            pr = _full_side(g, eid, r)
            if pr is None:
                continue
            legs_r, blk_r = pr
            ps = _thread_partition(g, eid, s)
            if ps is None:
                continue
            legs_s, blk_s = ps
            # threads must be preserved at s: bijection thread -> s-leg,
            # but s-legs may carry extra lines / s may have extra edges
            match = {}
            ok = True
            for lr in legs_r:
                hits = [ls for ls in legs_s if blk_s[ls] == blk_r[lr]]
                if len(hits) != 1:
                    ok = False
                    break
                match[lr] = hits[0]
            if not ok or len(set(match.values())) != len(legs_r):
                continue
            if _full_side(g, eid, s) is not None and \
                    g.degree(s) == g.degree(r):
                continue  # full double Y — let U4 take it (weight choice)

            s_rank = {f: k for k, f in enumerate(g.clockwise_from(s, eid))}
            cost = _dy_fixed_cost(
                g, reg, w, s, s_rank, blk_r[legs_r[0]], match[legs_r[0]],
                [(match[l], blk_r[l]) for l in legs_r[1:]])

            first = blk_r[legs_r[0]]
            rest = set(e.lines) - first
            e1, e2 = _split_edge(g, eid, first, rest)
            # split r only
            leg0 = legs_r[0]
            nn = g.add_node(g.nodes[r].orig)
            keep = [m for m in g.order[r] if m not in (eid, leg0)]
            g.order[r] = [e1, leg0]
            g.order[nn] = [e2] + keep
            for m in keep:
                me = g.edges[m]
                if me.u == r:
                    me.u = nn
                else:
                    me.v = nn
            for new_e, tgt in ((e1, r), (e2, nn)):
                ed = g.edges[new_e]
                if ed.u == r:
                    ed.u = tgt
                elif ed.v == r:
                    ed.v = tgt
            # s keeps both pieces in the old angular slot
            g.replace_slot(s, eid, [e1, e2])
            out.transforms.append(TSplit(eid, e1, e2, ref_at_v=(e.v == r)))
            out.fixed_cost += cost
            out.stats["U5"] += 1
            n += 1
            break
    return n


# ------------------------------------------------------------------ U6

def _rule_u6(g: OptGraph, reg, out: Reduction) -> int:
    n = 0
    for eid in list(g.edges):
        if eid not in g.edges:
            continue
        e = g.edges[eid]
        if e.u == e.v or e.dummy:
            continue
        for u, v in ((e.u, e.v), (e.v, e.u)):
            if g.degree(u) < 3 or g.degree(v) < 2:
                continue
            pu = _full_side(g, eid, u)
            if pu is None:
                continue
            legs_u, blk_u = pu
            rights = g.clockwise_from(v, eid)
            if len(rights) >= len(legs_u):
                continue
            # injection: every right leg's FULL line set equals some left
            # leg's thread (paper: L(ev_j) = L(eu_a'(j)))
            match = {}
            ok = True
            for m in rights:
                mset = set(g.edges[m].lines)
                hits = [lu for lu in legs_u if blk_u[lu] == mset]
                if len(hits) != 1:
                    ok = False
                    break
                match[m] = hits[0]
            if not ok:
                continue
            # stump lines must terminate at v
            stumps = [lu for lu in legs_u if lu not in match.values()]
            stump_lines = set().union(*(blk_u[s] for s in stumps))
            if not all(g.terminates(l, eid, v) for l in stump_lines):
                continue
            # existing right legs must already be inverse-ordered
            ru = {lu: k for k, lu in enumerate(legs_u)}
            seq = sorted(rights, key=lambda m: -ru[match[m]])
            if seq != rights:
                continue
            # build the mirrored full right side: dummies at inverse slots
            wnode = g.add_node(g.add_orig(0, False, synthetic=True))
            counterpart = {match[m]: m for m in rights}
            required = []
            dummies = []
            for lu in reversed(legs_u):
                if lu in counterpart:
                    required.append(counterpart[lu])
                else:
                    d = g.add_edge(v, wnode, blk_u[lu], dummy=True)
                    dummies.append(d)
                    required.append(d)
            g.order[v] = [eid] + required
            g.order[wnode] = list(reversed(dummies))
            out.transforms.append(TDummy(tuple(dummies)))
            out.stats["U6"] += 1
            n += 1
            break
    return n


# ------------------------------------------------------------ the loop

def reduce_graph(inst: Instance, weights: Weights | None = None,
                 rules=ALL_RULES) -> Reduction:
    g = inst.graph.copy()
    reg = inst.registry
    w = weights or Weights.for_graph(g)
    out = Reduction(original=inst.graph, graph=g, registry=reg, weights=w)

    if "P2" in rules:
        _rule_p2(g, reg, out)

    max_rounds = g.max_cardinality() + 1
    for _ in range(max_rounds):
        changed = 0
        if "U2" in rules:
            changed += _rule_u2(g, out)
        if "U3" in rules:
            changed += _rule_u3(g, out)
        if "U4" in rules:
            changed += _rule_u4(g, reg, w, out)
        if "U5" in rules:
            changed += _rule_u5(g, reg, w, out)
        if "U6" in rules:
            changed += _rule_u6(g, reg, out)
        if "U1" in rules:
            changed += _rule_u1(g, out)
        inner_rounds = g.max_degree() + 1
        for _ in range(inner_rounds):
            c = 0
            if "P1" in rules:
                c += _rule_p1(g, w, out)
            if "P3" in rules:
                c += _rule_p3(g, out)
            if "C1" in rules:
                c += _rule_c1(g, out)
            if "C2" in rules:
                c += _rule_c2(g, out)
            changed += c
            if not c:
                break
        if not changed:
            break
    return out


# ------------------------------------------------------------------ CLI

def _fmt_space(n: int) -> str:
    return str(n) if n < 10_000_000 else f"{float(n):.3g}"

def main(argv=None):
    import argparse

    from .model import load_build

    ap = argparse.ArgumentParser(
        prog="python -m lineorder.reduce",
        description="Reduce a transit_graph build to ordering components.")
    ap.add_argument("--build-key", required=True)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--stats", action="store_true",
                    help="print per-rule counts and component inventory")
    args = ap.parse_args(argv)

    inst = load_build(args.build_key, args.dsn)
    g = inst.graph
    m = g.max_cardinality()
    n_lines = sum(len(e.lines) for e in g.edges.values())
    print(f"[lineorder] {args.build_key}: {len(g.nodes)} nodes, "
          f"{len(g.edges)} edges, {n_lines} edge-lines, max |L(e)| = {m}, "
          f"search space = {_fmt_space(search_space(g))}")

    red = reduce_graph(inst)
    if args.stats:
        print("\nper-rule reductions:")
        for r in ALL_RULES:
            print(f"  {r:3} {red.stats.get(r, 0)}")
        comps = red.components()
        rg = red.graph
        nontrivial = [c for c in comps if c.search_space(rg) > 1]
        print(f"\nreduced graph: {len(rg.nodes)} nodes, {len(rg.edges)} "
              f"edges, max |L(e)| = {rg.max_cardinality()}, "
              f"fixed cost = {red.fixed_cost}")
        print(f"components: {len(comps)} total, "
              f"{len(nontrivial)} with search space > 1")
        print(f"{'comp':>4} {'edges':>5} {'maxL':>4} {'space':>12}")
        for i, c in enumerate(comps):
            sp = c.search_space(rg)
            if sp == 1 and i >= 20:
                remaining = len(comps) - i
                print(f"  ... {remaining} more search-space-1 components")
                break
            print(f"{i:>4} {len(c.edges):>5} {c.max_cardinality(rg):>4} "
                  f"{_fmt_space(sp):>12}")
        total = 1
        for c in comps:
            total *= c.search_space(rg)
        print(f"total remaining search space = {_fmt_space(total)} "
              f"(original {_fmt_space(search_space(g))})")


if __name__ == "__main__":
    main()
