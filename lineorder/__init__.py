"""lineorder — MLNCM-S line ordering (transit v3 stage 5).

Replaces LOOM's `loom` ordering stage. Implements the optimization-graph
model and the optimality-preserving reductions of Bast/Brosi/Storandt,
"Efficient Generation of Geographically Accurate Transit Maps" (ACM TSAS
2019, extended version): pruning rules P1-P3 (section 4.1), cutting
rules C1-C2 (section 4.2), untangling rules U1-U6 (section 4.3), scored
with the section-6 weights (station vs non-station, degree-scaled, all
evaluated on the ORIGINAL node v*).

Modules:
  model       — line/graph dataclasses + PostGIS loader (transit_graph_*)
  score       — MLNCM-S objective (crossings, split crossings, separations)
  reduce      — reduction rules to fixpoint + connected components + CLI
  reconstruct — expand a reduced-component solution back to the original graph
  solve       — per-component solver cascade (exhaustive / CP-SAT /
                greedy-with-lookahead + annealing) + slot writeback CLI

CLI:
  uv run --with-requirements lineorder/requirements.txt \
      python -m lineorder.reduce --build-key chicago:l-v3 --stats
  uv run --with-requirements lineorder/requirements.txt \
      python -m lineorder.solve --build-key chicago:l-v3 --dry-run
"""
