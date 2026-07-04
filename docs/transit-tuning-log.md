# Transit map v3 — tuning log & dial reference

A chronological record of every geometry/tuning change and the knobs each round
introduced, plus where the dials live today. The authoritative, always-current
list of dial *values* is emitted by `python -m tools.pipeline_dials` (never edit
values here by hand — they drift). This file is the *narrative*: what changed,
why, and which problem each dial exists to solve.

Stages: **shapesnap** (GTFS→OSM matching) · **linegraph/waygraph** (corridors) ·
**lineorder** (slot ordering) · **segments** (steady/transition features + bands).

## Iteration log

| # | Round | What changed | Key dials introduced |
|---|---|---|---|
| 1 | Staged import | MOTIS ingests only processed zips; scripted ingestion | — |
| 2 | Service derivation | Representative-day counting, frequencies, tiered gate | daytime window 06:00–22:00; gate ≥2 weekday-daytime |
| 3 | shapesnap | GTFS→OSM HMM matcher (replaces pfaedle) | dense radius rail 50 / bus 35 m; sparse 200/100; σ 15; β 30; Fréchet gate 100 m; length ratio 0.95–1.15 |
| 4 | Centerline (raster) | Raster skeleton corridors *(later replaced)* | MERGE_WIDTH 18 m; res 2.0 m/px |
| 5 | Ordering | CP-SAT MLNCM-S + reductions | station/junction crossing weights |
| 6 | Segments + tiles | Steady/transition features, `transit_lines_rt2` | TRANSITION_LEN 60 m; gap 4.4 px; fillet radius ≥ bundle width |
| 7–8 | Client + interactions | Render model, mode filter, popover, proprietary clicks | zoom gap squeeze z11→z14 (½→full) |
| 9 | NYC regression | Color-collapse trunks; identity-ref matching | family color_key merge |
| 10 | East River wobble | Gap-retry before bridging | `gap_retry_radius_mult` 2.0 |
| 11 | Junction refit *(raster era)* | Refit edge geometry from shapes; Tikhonov nodes | refit snap radius 2×MERGE_WIDTH |
| 12 | Low-zoom density | Zoom-banded transition lengths | bands 60/120/240/480 m at z15/14/13/≤12 |
| 13 | All-routes-on-OSM | Sparse rescue → graph bridge → agency; on-OSM % | rescue length ratio 0.90–1.60; bridge cutoff 4× |
| 14 | Four sites | Unfuse crossings; cluster-weighted refit; through-pair Y | unfuse both-sides escape; carve gate |
| 15 | **Track-exact rebuild** | **Way-graph corridors replace raster** (geometry = OSM track verbatim) | **pair_gap 15; family_gap 25; family_sustained 450; cross_family_gap 10; cross_family_min_len 450; bearing 20/35** |
| 16 | Mott Haven wye | Corridor loop excision; free-end band clamp; fillet budget | `loop_window_m` 100; fillet budget 1.1× |
| 17 | Merge-boundary easing | C1 seam easing; window coalescing + hysteresis | `ease_len_m` 100; `window_dip_coalesce_m` 200; `release_gap_mult` 1.5; `release_sustain_m` 150; `cov_cut_margin_m` 30 |
| 18 | Corridor-shortcut + guard | Reconcile off-track steadies onto real track; track-fidelity exam | `track_snap_tol_m` 18; exam tol 22 m + bundle margin |
| 19 | Bundle tolerance + non-service | Kissing redesign (profile not gap min); regular-service display filter; measurement tooling | `cross_family_gap_m` 10→18; `cross_family_min_frac_below` 0.60; `cross_family_max_gap_ratio` 6.0; `cross_family_cross_slack_m` 40; `NON_REGULAR_SERVICE_VALUES`/`NON_REGULAR_USAGE_VALUES` |

### Round 19 detail

**Part 1 — bundle tolerance + kiss prevention.** The track-exact rebuild's
`cross_family_gap_m=10` under-bundled genuine parallels a bit farther apart —
DeKalb's orange B/D ran beside yellow N/Q/R/W down the Manhattan Bridge
approach (a stable ~13/17 m mean/max gap, dead parallel, no crossing) yet
rendered as two messy ropes, system-wide. Fix: distinguish a KISS (transient
V-shaped convergence — lines cross or briefly touch then diverge) from a
BUNDLE (sustained parallel co-run) by **profile, not gap minimum**. Raised
`cross_family_gap_m` 10→18 (clears DeKalb's 17.4 m max with margin; 22 was
tried but additionally fused stacked express/local track at the Queens Blvd
interlocking — E,F over E,R — whose midline bowed the through-ribbon, so 18
is the sweet spot), and added three anti-kiss gates so raising the gap does
not re-admit kisses: **non-crossing** (`cross_family_cross_slack_m` — the two
geometries must not intersect in the window interior, endpoint convergence
excluded), **gap stability** (`cross_family_min_frac_below` over a context
window — a kiss dips under only briefly; a bundle stays under throughout;
`cross_family_max_gap_ratio` a loose safety valve). The KISSING count (merge
rejects) is surfaced in the scorecard so the effect is measurable. The
`_kiss_gates` reject reasons: `cross_crosses`, `cross_unstable_gap`,
`cross_too_short`, `cross_bearing`.

**Part 2 — ignore non-service tracks for display geometry.** Yard/siding/
spur/crossover (`service=*`) and industrial/military/tourism (`usage=*`)
tracks are PENALIZED-BUT-AVAILABLE in matching (trains reverse over
crossovers at terminals — do not break terminal approaches) but must be
IGNORED for display geometry (a yard's fan of parallel tracks pulls a
pair/platform midline and a reconciliation snap toward track no train rides).
Canonical predicate `shapesnap.graph.is_regular_service_track`; consumers:
the reconciliation snap target (`segments.build.load_ways`) and the
track-fidelity exam ground truth now filter to regular-service ways. The
exam gained an ON-SERVICE-TRACK advisory (a steady row near a service track
but far from a mainline is riding track the train uses — the 5's E 180 St /
Dyre Av connector, the SIR mainline mis-tagged `service=yard` — not a stray
across open ground).

**Part 3 — measurement tooling** (`tools/`, committed, rerunnable):
`python -m tools.pipeline_dials` (the always-current dial manifest,
source-introspected; refreshes `docs/pipeline-dials.md`) and
`python -m tools.scorecard --build-key <city>` (a one-page quantitative
scorecard — junction deviation, track stray, bundle/kissing counts, corridor
+ feature counts, on-OSM %). Bundle/kissing counts come from a corridor-build
notes sidecar (`linegraph.build._save_notes_sidecar`).

## The dials that actually move the map

Grouped by the problem they solve. Values here may lag — run the manifest tool.

**Bundling (which lines share one ribbon)** — `linegraph/waygraph.py::WaygraphConfig`
- `pair_gap_m` — directional pair (same routes, opposite tracks) → one midline.
- `family_gap_m` / `family_sustained_min_m` — same color family, different sets (N/Q + R/W → one yellow ribbon).
- `cross_family_gap_m` / `cross_family_min_len_m` / `cross_family_max_bearing_deg` — different families running tightly parallel (Chicago Lake St; DeKalb B/D↔N/Q). Raised 10→18 in round 19 (10 m caused the DeKalb-class under-bundling); the anti-kiss profile gates below keep kisses out at the wider gap.
- Anti-kiss guards: `cross_family_min_frac_below` (sustained co-run over the neighbourhood), `cross_family_max_gap_ratio` (flat, loose valve), `cross_family_cross_slack_m` (non-crossing), plus `merge_min_len_m`, `merge_end_slack_m`, `merge_max_bearing_deg`, and the flap guard (`window_dip_coalesce_m`, `release_gap_mult`, `release_sustain_m`).

**Seam & junction geometry** — `WaygraphConfig` + `segments/segment.py::SegmentConfig`
- `ease_len_m`, `blend_m` — how a bundle hands off to a single track (C1, no step).
- `loop_window_m`, fillet radius/budget, `tail_collapse_m`, `ladder_contract_m`.

**Track fidelity** — `track_snap_tol_m` (reconcile off-track steadies), the track-fidelity exam threshold.

**Matching** — `shapesnap/match.py::MatchConfig`, `gates.py::GateConfig`, `graph.py` tag filters/penalties (incl. service-track penalties).

**Zoom/display** — transition bands (`segments`), client gap-squeeze (`parchment` transit.ts).

## Measuring dial effects
- `python -m tools.pipeline_dials` — every config field, value, and docstring across all stages (auto-synced from the dataclasses).
- `python -m tools.scorecard --build-key <city>` — runs the quantitative exams and prints a one-page scorecard (junction deviation, track stray, bundle count, kissing count, corridor count, on-OSM %). Change a dial → rebuild → re-run → compare.
