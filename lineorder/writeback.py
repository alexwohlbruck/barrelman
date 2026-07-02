"""lineorder.writeback — phase-C slot writeback + run ledger.

The ONLY writer of transit_graph_edge_lines.slot. One transaction per
apply, touching exactly one build_key:

  1. ensure the `lineorder_runs` ledger exists (idempotent DDL);
  2. shift the build's slots out of range, then set the final values —
     the primary key is (edge_id, slot), so permuting in place would
     collide row-by-row;
  3. insert a run row: build_key, score before/after, crossing/
     separation counts, per-method component histogram, jsonb detail
     (config, reduction stats, per-component results, residual-cost
     nodes), started/finished timestamps.

Slot semantics: left-to-right position along the edge's stored geometry
direction (u -> v). The display SQL (import/create-transit-lines-
runtime.sql) groups by (color_key, slot, line_count) and ST_LineMerges
each group, so corridor stability == raw slot constancy along every
maximal degree-2 chain with a constant line set; linegraph emits chain
edges head-to-tail, and after P1 contraction a steady corridor is a
single reduced edge, so the reconstructed optimum satisfies this
structurally (asserted by lineorder/exam/stability_exam.py).

Refuses to write when the optimized score is worse than the slots
already stored (heuristic/timeout path): rerun with a higher
--time-limit instead of regressing.

CLI:
  uv run --with-requirements lineorder/requirements.txt \
      python -m lineorder.apply --build-key chicago:l-v3 [--dry-run]
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone

from .model import DEFAULT_DSN, Instance, load_build
from .score import search_space
from .solve import (SolveConfig, SolveOutcome, _fmt_space, crossing_report,
                    print_outcome, solve_instance)

RUNS_DDL = """
CREATE TABLE IF NOT EXISTS lineorder_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    build_key     text NOT NULL,
    score_before  double precision NOT NULL,
    score_after   double precision NOT NULL,
    crossings_same integer NOT NULL,
    crossings_diff integer NOT NULL,
    separations   integer NOT NULL,
    methods       jsonb NOT NULL,
    detail        jsonb NOT NULL,
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz NOT NULL DEFAULT now()
)"""


def write_slots(cur, inst: Instance, full: dict) -> int:
    """Write a full solution into transit_graph_edge_lines.slot for
    inst.build_key on an open cursor (caller owns the transaction).
    Two-phase: shift the build's slots by +1000 (out of the valid
    range), then set the final positions keyed by (edge, feed, route)."""
    rows = []
    for eid, perm in full.items():
        db_eid = inst.edge_db_id.get(eid)
        if db_eid is None:
            continue
        for i, uid in enumerate(perm):
            ln = inst.registry.get(uid)
            rows.append((i, db_eid, ln.feed_id, ln.route_id))
    cur.execute(
        """UPDATE transit_graph_edge_lines l SET slot = l.slot + 1000
           FROM transit_graph_edges e
           WHERE e.id = l.edge_id AND e.build_key = %s""",
        (inst.build_key,))
    shifted = cur.rowcount
    if shifted != len(rows):
        raise RuntimeError(
            f"slot writeback mismatch: {shifted} rows in build "
            f"{inst.build_key!r}, solution covers {len(rows)}")
    cur.executemany(
        """UPDATE transit_graph_edge_lines
           SET slot = %s
           WHERE edge_id = %s AND feed_id = %s AND route_id = %s""",
        rows)
    return len(rows)


def run_detail(out: SolveOutcome, cfg: SolveConfig) -> dict:
    """jsonb detail payload for the lineorder_runs ledger."""
    red = out.reduction
    b, a = out.before, out.after
    residual = [
        {"label": label, "lon": x, "lat": y,
         "same": s.crossings_same, "diff": s.crossings_diff,
         "sep": s.separations, "weighted": s.weighted}
        for _nid, label, x, y, s in crossing_report(
            out.instance.graph, red.registry, out.full_solution,
            red.weights)]
    return {
        "config": asdict(cfg),
        # "optimal" statuses below are relative to the corridor-stable
        # subspace (see lineorder.reduce, "Optimality semantics")
        "objective": "MLNCM-S over the corridor-stable subspace",
        "graph": {"nodes": len(out.instance.graph.nodes),
                  "edges": len(out.instance.graph.edges),
                  "search_space": _fmt_space(
                      search_space(out.instance.graph))},
        "reduction": {"rules": dict(red.stats),
                      "fixed_cost": red.fixed_cost,
                      "edges": len(red.graph.edges)},
        "components": [
            {"index": r.index, "method": r.method, "status": r.status,
             "edges": r.n_edges, "space": _fmt_space(r.space),
             "canonical": r.canonical, "after": r.after,
             "wall_s": round(r.wall, 3)}
            for r in out.results],
        "before": {"weighted": b.weighted, "same": b.crossings_same,
                   "diff": b.crossings_diff, "sep": b.separations},
        "after": {"weighted": a.weighted, "same": a.crossings_same,
                  "diff": a.crossings_diff, "sep": a.separations},
        "residual": residual,
    }


def record_run(cur, out: SolveOutcome, cfg: SolveConfig,
               started_at: datetime) -> int:
    """Insert the run row (same transaction as the slots)."""
    from psycopg.types.json import Jsonb

    cur.execute(RUNS_DDL)
    methods = Counter(r.method for r in out.results)
    cur.execute(
        """INSERT INTO lineorder_runs
             (build_key, score_before, score_after, crossings_same,
              crossings_diff, separations, methods, detail, started_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (out.instance.build_key, out.before.weighted, out.after.weighted,
         out.after.crossings_same, out.after.crossings_diff,
         out.after.separations, Jsonb(dict(methods)),
         Jsonb(run_detail(out, cfg)), started_at))
    return cur.fetchone()[0]


def apply_build(build_key: str, dsn: str = DEFAULT_DSN,
                cfg: SolveConfig | None = None,
                dry_run: bool = False) -> tuple[SolveOutcome, int | None]:
    """Solve a build and write the slots + run record in one
    transaction. Returns (outcome, run id or None when not written).
    Raises RuntimeError when the optimized score would regress."""
    import psycopg

    cfg = cfg or SolveConfig()
    started_at = datetime.now(timezone.utc)
    inst = load_build(build_key, dsn)
    out = solve_instance(inst, cfg)
    if dry_run:
        return out, None
    if out.after.weighted > out.before.weighted + 1e-9:
        raise RuntimeError(
            f"optimized score {out.after.weighted:.1f} is worse than the "
            f"stored slots ({out.before.weighted:.1f}) — refusing to "
            f"regress; rerun with a higher --time-limit")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        write_slots(cur, inst, out.full_solution)
        run_id = record_run(cur, out, cfg, started_at)
        conn.commit()
    return out, run_id


def main(argv=None):
    import argparse
    import sys

    ap = argparse.ArgumentParser(
        prog="python -m lineorder.apply",
        description="Solve a transit_graph build (MLNCM-S) and write the "
                    "optimized line order to transit_graph_edge_lines.slot, "
                    "recording the run in lineorder_runs.")
    ap.add_argument("--build-key", required=True)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--dry-run", action="store_true",
                    help="solve and report, do not write")
    ap.add_argument("--time-limit", type=float, default=30.0,
                    help="CP-SAT seconds per component (default 30)")
    ap.add_argument("--anneal-iters", type=int, default=30_000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--jobs", type=int, default=0,
                    help="parallel component workers (0 = cpu count)")
    args = ap.parse_args(argv)

    cfg = SolveConfig(cpsat_time_limit=args.time_limit,
                      anneal_iters=args.anneal_iters,
                      seed=args.seed, jobs=args.jobs)
    t0 = time.perf_counter()
    try:
        out, run_id = apply_build(args.build_key, args.dsn, cfg,
                                  dry_run=args.dry_run)
    except RuntimeError as exc:
        print(f"[apply] {exc}", file=sys.stderr)
        sys.exit(2)
    print_outcome(out, tag="apply")
    n = sum(len(p) for p in out.full_solution.values())
    if args.dry_run:
        print(f"\n[apply] dry run — slots NOT written "
              f"({time.perf_counter() - t0:.2f}s total)")
    else:
        print(f"\n[apply] wrote {n} slots for {args.build_key} "
              f"(lineorder_runs id {run_id}, "
              f"{time.perf_counter() - t0:.2f}s total)")


if __name__ == "__main__":
    main()
