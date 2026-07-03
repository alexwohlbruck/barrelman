#!/usr/bin/env python3
"""lineorder stability exam (stage 5, phase C acceptance).

Validates the CURRENT transit_graph_edge_lines.slot state of the
chicago:l-v3 build (i.e. run AFTER `python -m lineorder.apply`) against
the invariants that killed the v2 attempt, and contrasts the LOOM
baseline:

  1  corridor stability — walking every maximal corridor (chain of
     edges through degree-2 nodes with identical line sets), each
     line's raw slot is CONSTANT along it; equivalently, zero
     (color_key, slot, line_count) groups — the display SQL's grouping
     (import/create-transit-lines-runtime.sql edge_colors/ranked CTEs)
     — split a corridor into more than one ST_LineMerge run
  2  composition-change-only transitions — a display run may end only
     at a node of degree >= 3 or where the line composition changes;
     every transition site is listed with its routes (these become the
     stage-6 transition segments); in the Loop window the sites must be
     exactly the 6 junction nodes of the stage-4 exam (termini aside)
  3  crossing audit — total crossings and their node locations under
     the stored slots; no crossing at a degree-2 non-station node
  4  LOOM contrast — the same corridor-stability walk over the LOOM
     baseline (chicago:l, read-only) must show the known instability
     (Pink 4-slots pathology) that v3 eliminates: violations > 0
  5  determinism — two fresh solve runs produce identical slots, and
     the stored slots equal that deterministic optimum (so rerunning
     solve+apply is a no-op)

Read-only. Exits non-zero if any check fails. Run:

  uv run --with-requirements lineorder/requirements.txt \
      python lineorder/exam/stability_exam.py

`--build-key` (default chicago:l-v3) points the generic checks (1, 2, 3,
5) at another build; the Chicago-specific assertions (check 2's Loop
window inventory, check 4's LOOM baseline contrast) only run for the
default build.
"""

from __future__ import annotations

import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lineorder.model import DEFAULT_DSN, load_build  # noqa: E402
from lineorder.score import Weights, score_node      # noqa: E402
from lineorder.solve import SolveConfig, solve_instance  # noqa: E402

BUILD = "chicago:l-v3"
BASELINE = "chicago:l"

# same Loop window as linegraph/exam/loop_exam.py (stage-4 acceptance)
LOOP_WINDOW = (-87.6355, 41.8755, -87.6245, 41.8875)
# Stage-4 way-graph era calibration (PAR-12 v3 rebuild): the raster
# skeleton showed 6 blob junctions in the Loop window; the exact build
# shows the REAL interlockings — Tower 18's switch cluster plus the
# corner junctions where the leg bundles change composition. The pin
# holds the inventory so a smeared junction or a phantom mid-corridor
# slot change still fails.
EXPECTED_LOOP_SITES = 5         # transition sites in the Loop window
EXPECTED_LOOP_COMPOSITION = 0   # ...of which deg-2 composition changes

# LOOM edge_lines carry only route_color — CTA hex -> route name
COLOR_TO_ROUTE = {
    "00a1de": "Blue", "c60c30": "Red", "62361b": "Brn", "009b3a": "G",
    "f9461c": "Org", "522398": "P", "e27ea6": "Pink", "f9e300": "Y",
}

FAILURES: list[str] = []


def report(check: str, ok: bool, detail: str = "") -> None:
    print(f"  -> {'PASS' if ok else 'FAIL'}{': ' + detail if detail else ''}")
    if not ok:
        FAILURES.append(f"{check}: {detail}")


def in_window(x: float, y: float, box=LOOP_WINDOW) -> bool:
    w, s, e, n = box
    return w <= x <= e and s <= y <= n


# ------------------------------------------------------ corridor walk

def corridors(edges: dict, incident: dict) -> list[list]:
    """Maximal corridors: union-find over edge ids, joined at every
    degree-2 node whose two incident edges carry identical line sets.
    edges: {eid: {line_key: slot}}; incident: {nid: [eids]}.
    Returns multi-edge corridors only (singletons are trivially stable)."""
    parent = {e: e for e in edges}

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    for _nid, inc in incident.items():
        if len(inc) != 2:
            continue
        e, f = inc
        if set(edges[e]) == set(edges[f]):
            ra, rb = find(e), find(f)
            if ra != rb:
                parent[ra] = rb
    comp: dict = {}
    for e in edges:
        comp.setdefault(find(e), []).append(e)
    return [sorted(v) for v in comp.values() if len(v) > 1]


def slot_violations(edges: dict, cors: list[list]) -> list[tuple]:
    """(corridor index, line_key, sorted distinct slots) for every line
    whose raw slot is not constant along its corridor."""
    out = []
    for i, cor in enumerate(cors):
        for key in sorted(edges[cor[0]], key=str):
            slots = sorted({edges[e][key] for e in cor})
            if len(slots) > 1:
                out.append((i, key, slots))
    return out


def display_groups(rows: list[tuple]) -> dict:
    """Mirror of the display SQL's edge_colors/ranked CTEs for ONE edge.
    rows: [(slot, route_color, route_id)] -> {color_key: (slot, line_count)}
    with color_key = COALESCE(NULLIF(route_color,''), 'rid:'||route_id),
    slot = dense_rank() OVER (ORDER BY min(slot), color_key) - 1 and
    line_count = number of colour groups on the edge."""
    first: dict = {}
    for slot, color, route_id in rows:
        ck = color if color else f"rid:{route_id}"
        if ck not in first or slot < first[ck]:
            first[ck] = slot
    ranked = sorted(first, key=lambda ck: (first[ck], ck))
    return {ck: (rank, len(ranked)) for rank, ck in enumerate(ranked)}


# ----------------------------------------------------------- v3 state

def v3_state(inst):
    """Per-edge line->slot maps + incident lists from the stored slots."""
    g, reg = inst.graph, inst.registry
    edges = {}
    color_rows = {}
    for eid in g.edges:
        perm = inst.provisional[eid]
        edges[eid] = {(reg.get(u).feed_id, reg.get(u).route_id): i
                      for i, u in enumerate(perm)}
        color_rows[eid] = [(i, reg.get(u).color, reg.get(u).route_id)
                           for i, u in enumerate(perm)]
    return edges, color_rows


def routes_at(g, reg, nid) -> str:
    names = set()
    for eid in g.order[nid]:
        for u in g.edges[eid].lines:
            names.add(reg.get(u).short_name)
    return ",".join(sorted(names))


def check1_corridor_stability(inst):
    print("\nCHECK 1 — corridor stability (v3, stored slots)")
    g = inst.graph
    edges, color_rows = v3_state(inst)
    cors = corridors(edges, g.order)
    n_edges = sum(len(c) for c in cors)
    longest = max((len(c) for c in cors), default=0)
    print(f"  {len(cors)} maximal multi-edge corridors covering "
          f"{n_edges}/{len(g.edges)} edges (longest {longest} edges)")

    viol = slot_violations(edges, cors)
    for i, key, slots in viol:
        print(f"  VIOLATION corridor {i}: line {key} spans slots {slots}")
    report("check1.slots", not viol,
           f"{len(viol)} (corridor, line) raw-slot violations")

    grouped = []
    for i, cor in enumerate(cors):
        per_edge = [display_groups(color_rows[e]) for e in cor]
        for ck in per_edge[0]:
            variants = {ge.get(ck) for ge in per_edge}
            if len(variants) > 1:
                grouped.append((i, ck, sorted(variants, key=str)))
    for i, ck, variants in grouped:
        print(f"  VIOLATION corridor {i}: colour {ck} splits into "
              f"(slot, line_count) groups {variants}")
    report("check1.display-groups", not grouped,
           f"{len(grouped)} (color_key, slot, line_count) groups split "
           f"a corridor")
    return cors


def check2_transitions(inst, chicago: bool = True):
    print("\nCHECK 2 — transitions only at junctions / composition changes")
    g, reg = inst.graph, inst.registry
    edges, _ = v3_state(inst)
    sites = []       # (nid, degree, kind, detail)
    bad = []
    for nid in sorted(g.nodes):
        inc = g.order[nid]
        if len(inc) < 2:
            continue  # a terminus ends its runs by definition
        if len(inc) >= 3:
            sites.append((nid, len(inc), "junction", ""))
            continue
        e, f = inc
        se, sf = set(edges[e]), set(edges[f])
        if se != sf:
            gone = ",".join(sorted(k[1] for k in se - sf)) or "-"
            new = ",".join(sorted(k[1] for k in sf - se)) or "-"
            sites.append((nid, 2, "composition", f"{gone} <-> {new}"))
        elif any(edges[e][k] != edges[f][k] for k in se):
            sites.append((nid, 2, "SLOT-CHANGE", ""))
            bad.append(nid)
    for nid, deg, kind, detail in sites:
        n = g.nodes[nid]
        o = g.orig_nodes[n.orig]
        print(f"  node {nid} deg={deg} ({n.x:.6f}, {n.y:.6f})"
              f"{' [' + o.label + ']' if o.label else ''} {kind}"
              f"{' ' + detail if detail else ''}"
              f" routes {{{routes_at(g, reg, nid)}}}")
    print(f"  {len(sites)} transition sites "
          f"({sum(1 for s in sites if s[2] == 'junction')} junctions, "
          f"{sum(1 for s in sites if s[2] == 'composition')} deg-2 "
          f"composition changes)")
    report("check2.slot-change-only-at-composition", not bad,
           f"{len(bad)} deg-2 slot changes without a composition change")

    if chicago:
        loop = [s for s in sites
                if in_window(g.nodes[s[0]].x, g.nodes[s[0]].y)]
        loop_comp = [s for s in loop if s[2] == "composition"]
        loop_bad = [s for s in loop if s[2] == "SLOT-CHANGE"]
        report("check2.loop-junctions-only",
               not loop_bad and len(loop) == EXPECTED_LOOP_SITES
               and len(loop_comp) == EXPECTED_LOOP_COMPOSITION,
               f"Loop-window transition sites: {len(loop)} "
               f"(expected {EXPECTED_LOOP_SITES}), "
               f"{len(loop_comp)} composition changes "
               f"(expected {EXPECTED_LOOP_COMPOSITION}), "
               f"{len(loop_bad)} bare slot changes")
    return sites


def check3_crossings(inst):
    print("\nCHECK 3 — crossing audit (stored slots)")
    g, reg = inst.graph, inst.registry
    w = Weights.for_graph(g)
    sol = {eid: inst.provisional[eid] for eid in g.edges}
    total_same = total_diff = total_sep = 0
    bad = []
    for nid in sorted(g.nodes):
        s = score_node(g, reg, sol, nid, w)
        total_same += s.crossings_same
        total_diff += s.crossings_diff
        total_sep += s.separations
        if s.crossings_same + s.crossings_diff == 0:
            continue
        n = g.nodes[nid]
        o = g.orig_nodes[n.orig]
        deg = len(g.order[nid])
        print(f"  node {nid} deg={deg}{' station' if o.station else ''} "
              f"({n.x:.6f}, {n.y:.6f})"
              f"{' [' + o.label + ']' if o.label else ''}: "
              f"same={s.crossings_same} diff={s.crossings_diff} "
              f"sep={s.separations}")
        if deg == 2 and not o.station:
            bad.append(nid)
    print(f"  totals: same-seg {total_same}, diff-seg {total_diff}, "
          f"separations {total_sep}")
    report("check3.no-deg2-nonstation-crossings", not bad,
           f"{len(bad)} crossings at degree-2 non-station nodes")


def check4_loom_contrast(dsn):
    print("\nCHECK 4 — LOOM baseline contrast (chicago:l, read-only)")
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """WITH ends AS (
                 SELECT e.id AS edge_id, ST_StartPoint(e.geom) AS p
                 FROM transit_graph_edges e WHERE e.build_key = %(b)s
                 UNION ALL
                 SELECT e.id, ST_EndPoint(e.geom)
                 FROM transit_graph_edges e WHERE e.build_key = %(b)s)
               SELECT en.edge_id, n.id
               FROM ends en
               JOIN transit_graph_nodes n
                 ON n.build_key = %(b)s AND ST_DWithin(n.geom, en.p, 1e-5)
               ORDER BY en.edge_id""",
            {"b": BASELINE})
        incident: dict = {}
        matched: dict = {}
        for edge_id, node_id in cur.fetchall():
            incident.setdefault(node_id, []).append(edge_id)
            matched[edge_id] = matched.get(edge_id, 0) + 1
        cur.execute(
            """SELECT l.edge_id, l.slot,
                      lower(COALESCE(NULLIF(l.route_color, ''), l.route_id))
               FROM transit_graph_edge_lines l
               JOIN transit_graph_edges e ON e.id = l.edge_id
               WHERE e.build_key = %s ORDER BY l.edge_id, l.slot""",
            (BASELINE,))
        edges: dict = {}
        for edge_id, slot, key in cur.fetchall():
            edges.setdefault(edge_id, {})[key] = slot

    unmatched = [e for e in edges if matched.get(e, 0) != 2]
    if unmatched:
        print(f"  note: {len(unmatched)} edges without exactly 2 matched "
              f"endpoints (excluded): {unmatched[:10]}")
        for e in unmatched:
            edges.pop(e)
        incident = {n: [e for e in inc if e in edges]
                    for n, inc in incident.items()}

    cors = corridors(edges, incident)
    viol = slot_violations(edges, cors)
    per_route: dict = {}
    for i, key, slots in viol:
        name = COLOR_TO_ROUTE.get(key, key)
        cur_c, cur_s = per_route.get(name, (0, 0))
        per_route[name] = (cur_c + 1, max(cur_s, len(slots)))
    print(f"  {len(cors)} maximal multi-edge corridors, "
          f"{len(viol)} (corridor, line) raw-slot violations")
    for name, (n_cor, max_slots) in sorted(per_route.items()):
        print(f"    {name}: unstable in {n_cor} corridors, "
              f"up to {max_slots} distinct slots in one corridor")
    report("check4.loom-instability-receipt", len(viol) > 0,
           f"LOOM violations {len(viol)} (expected > 0) vs v3's 0")


def check5_determinism(inst, dsn, build: str = BUILD):
    print("\nCHECK 5 — determinism (solve twice + stored slots match)")
    cfg = SolveConfig()  # apply's defaults: seed 0, deterministic CP-SAT

    def perm_by_db_edge(instance, sol):
        reg = instance.registry
        return {instance.edge_db_id[e]:
                tuple((reg.get(u).feed_id, reg.get(u).route_id)
                      for u in sol[e])
                for e in instance.graph.edges}

    out1 = solve_instance(inst, cfg)
    inst2 = load_build(build, dsn)
    out2 = solve_instance(inst2, cfg)
    s1 = perm_by_db_edge(inst, out1.full_solution)
    s2 = perm_by_db_edge(inst2, out2.full_solution)
    diff = [k for k in s1 if s1[k] != s2.get(k)]
    report("check5.solve-deterministic", s1 == s2,
           f"two fresh solves: {len(diff)} differing edges "
           f"(score {out1.after.weighted:.1f} / {out2.after.weighted:.1f})")

    stored = perm_by_db_edge(inst, {e: inst.provisional[e]
                                    for e in inst.graph.edges})
    dbdiff = [k for k in s1 if s1[k] != stored.get(k)]
    report("check5.stored-equals-optimum", not dbdiff,
           f"{len(dbdiff)} edges where stored slots differ from the "
           f"deterministic optimum (0 == rerunning apply is a no-op)")


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--build-key", default=BUILD)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    args = ap.parse_args()
    build, dsn = args.build_key, args.dsn
    chicago = build == BUILD  # Loop window + LOOM baseline checks

    print(f"lineorder stability exam — build {build}"
          + (f" vs baseline {BASELINE}" if chicago else "")
          + f"\ndsn {dsn}")
    inst = load_build(build, dsn)
    print(f"loaded {len(inst.graph.nodes)} nodes, "
          f"{len(inst.graph.edges)} edges, "
          f"{sum(len(p) for p in inst.provisional.values())} edge-lines")

    check1_corridor_stability(inst)
    check2_transitions(inst, chicago)
    check3_crossings(inst)
    if chicago:
        check4_loom_contrast(dsn)
    else:
        print(f"\nCHECK 4 — skipped (LOOM baseline contrast is "
              f"chicago-only; build {build})")
    check5_determinism(inst, dsn, build)

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"EXAM FAILED — {len(FAILURES)} failing check(s):")
        for f in FAILURES:
            print(f"  * {f}")
        return 1
    print("EXAM PASSED — all checks green")
    return 0


if __name__ == "__main__":
    sys.exit(main())
