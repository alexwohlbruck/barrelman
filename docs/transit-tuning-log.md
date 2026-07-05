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
| 20 | Visual verification sandbox | `tools/sandbox/` — renders emitted geometry WITH the client's exact zoom-scaled line-offset applied, measures the ON-SCREEN px gap per site, per-site PASS/FAIL verdict; fast per-site rebuild for dial iteration | (no dial change — tooling; confirmed the DeKalb bundle is 4.4 px tight in data+render) |
| 21 | **Transitive cross-family bundling** | **Genuine parallel corridors of different families collapse onto ONE multi-slot bundle** (Queens Blvd E/F/M/R; the 4/5 beside 2/3; Jay St A/C+F; Chicago North Side P/Red onto the Brown). A network sweep (`tools/sandbox/bundle_audit.py`) found 16 pairs of close-parallel-but-UNMERGED corridors (~9 km); after the fix the missed-bundle count → ~0 (NYC 12→1, CHI 6→0, both residuals real terminal divergences). | `cross_family_min_len_m` 450→200; new `cross_family_sustained_min_m` 450 (both-continue parallels merge); weave-crossing exemption (a 4-track trunk that interleaves at interlockings is not a kiss); near-coincident gap-ratio exemption; ramp gate scoped off cross |
| 22 | **GTFS↔OSM stop conflation + terminal re-anchor** | Each GTFS stop is matched to a same-mode OSM stop within a mode radius by lenient fuzzy name; a confident match OVERRIDES position + name (O'Hare moves onto the OSM platform; MTA bus "ORIENTAL BLVD/MACKENZIE ST" → "Oriental Boulevard & Mackenzie Street"). A moved terminal then RE-ANCHORS its agency shape end so the match terminates on the OSM track — the O'Hare end-of-line kink (104 m agency bridge → 0). Retires `backfill-osm-stop-names.sql`. | conflate `radius_m` rail 200 / bus 60 m, `name_threshold` 0.6; `MatchConfig.reanchor_max_m` 300 m |
| 23 | **Anti-hop track-switch penalty** | A matched route no longer hops between adjacent parallel OSM tracks via crossovers (user rule 11). Leaving the current way for a near-PARALLEL different way at a node where the current way also continues costs a penalty; genuine forks (sharp divergence, way dead-ends) stay free. Network track-switch count (`tools/sandbox/track_switch_audit.py`): NYC 464→178, CHI 30→0; on-OSM ≥99.5% held. | `MatchConfig.track_switch_penalty_m` 180 m; `switch_parallel_cos` 0.819 |

### Round 21 detail — transitive cross-family bundling

The round-19 cross-family merge was **pairwise and length-gated**, so genuine
parallel corridors that should read as one clean bundle were left as separate
crowded lines. A network sweep of `nyc:subway-v3` (`tools/sandbox/bundle_audit.py`)
quantified it: **16 pairs** of different-family steady corridors running < 12 m
apart and parallel for > 150 m but NOT sharing a centerline (~9 km total) —
Queens Blvd F/FX↔E, the 4/5↔2/3 toward Nevins, Jay St A/C↔F, Chicago's North
Side P/Red↔Brown, etc.

Two root causes, both confirmed with data and fixed:

1. **Length threshold too strict.** `cross_family_min_len_m=450` dropped genuine
   ~200–450 m parallels (Jay St A/C+F, J/Z+M). **Lowered 450 → 200**; the
   anti-kiss PROFILE gates (non-crossing, gap-stability frac/ratio, bearing) keep
   the kisses out at the shorter length — a real kiss crosses or is a transient
   valley regardless of the floor (Rector / Brooklyn Bridge / Whitehall stay
   rejected by `crosses` / `frac_below`, verified).

2. **No multi-way (transitive) bundling.** Merging both-continue parallels needed
   a cross-family analogue of `family_sustained_min_m`: new
   **`cross_family_sustained_min_m=450`** merges a sustained cross-family co-run
   even when BOTH corridors continue past the window (the 4/5 beside the 2/3 for
   1.25 km, F beside N/Q/R for 567 m), so a corridor parallel to an existing
   BUNDLE joins it and a whole physical corridor collapses into one multi-slot
   bundle (Queens Blvd E/F/M/R). Three supporting fixes let the real trunks
   through without re-admitting kisses:
   - **weave-crossing exemption** — a 4-track express/local trunk physically
     crosses at every bypass/interlocking but stays near-coincident there; the
     non-crossing gate now ignores a crossing where the pair is hugged around it
     (it diverges away from a true kiss X), so Queens Blvd E↔F/FX (2.5 km, frac
     1.0) and DeKalb N/Q↔B/D (2.6 km) bundle;
   - **near-coincident ratio exemption** — two corridors within a couple of
     meters are the same physical track; their gap dips to ~0 so `gap_max/gap_mean`
     explodes for a purely numerical reason — exempt a ~0 m mean gap from the
     ratio valve;
   - **ramp gate scoped off cross** — the endpoint ramp test (one end converges,
     the other diverges) also fires on a genuine sustained parallel that shares one
     junction; the anti-kiss profile gates already discriminate a ramp for cross
     merges, so the endpoint test is kept only for pair/family.

**Result** (`tools/sandbox/bundle_audit.py`, before → after): NYC missed bundles
**12 → 1** (3057 m → 321 m; the residual is the N/D West End↔Sea Beach terminal
divergence at Coney Island, a real split), Chicago **6 → 0** (2678 m → 0 m). No
labeled kiss flipped into a bundle (all 17 sandbox sites PASS, including the four
separate/kiss sites). The re-pinned Chicago exam counts (Loop transition sites
7→6, segment sites 13→18, opt edges 157→167, edge_lines 229→252, CP-SAT optimum
98→110) all reflect the P/Red-onto-Brown bundle adding genuine composition-change
junctions where a line joins/leaves the shared ribbon — the invariants (0 deg-2
composition changes but Howard, exact CP-SAT optimum, residual only at real
interlockings) are unchanged.

### Round 20 detail — visual verification sandbox

`tools/sandbox/` is a committed, rerunnable harness that closes the gap
between "the DB rows are bundled" and "what the map actually paints". It
renders the emitted `transit_line_segments` WITH the client's exact offset
expression (`server/src/constants/default-layers/transit.ts`:
`zoomScaledOffset(offset_px)` = `offset_px * gapScale(zoom)`,
`gapScale` = interpolate linear zoom 11→0.5, 14→1.0 clamped; transitions
eased cubic-bezier(.4,0,.6,1) along line-progress) at each site's zoom, then
measures the on-screen px gap between family ribbons.

- `tools/sandbox/sites.py` — the site registry (build_key, bbox, zoom,
  families, EXPECTED behaviour: bundle / separate / centered / straight).
- `tools/sandbox/render.py` — the client render reproduced (m/px =
  78271.51696/2^z·cos lat; per-vertex miter offset from
  `segments.exam.loop_visual`).
- `tools/sandbox/verify.py` — reads the CURRENT DB, renders one panel per
  site, measures per-site (bundle: on-screen px gap + PRE-offset centerline
  coincidence; separate: longest contiguous in-window parallel co-run;
  centered: ribbon→platform; straight: through-route stray from real OSM
  track), writes per-site PNGs + a contact-sheet + `verdicts.json`.
- `tools/sandbox/rebuild.py` — fast per-site rebuild: only the patterns
  intersecting the buffered bbox → waygraph → lineorder → segments to a
  scratch `sandbox:<site>` build_key, with `--set dial=value` overrides, then
  re-verify + before/after render. Truncation-limited (generous buffer; the
  full build stays authoritative).

**The DeKalb reconciliation.** Measured verdict: the B/D (orange `EB6800`)
and N/Q/R/W (yellow `F6BC26`) features over the Manhattan-Bridge approach
share ONE centerline (PRE-offset coincidence **0.00 m** — byte-identical
geometry) and render at exactly **4.4 px** apart at the site zoom (offsets
±2.2 px, `line_count=2`). The data IS a tight Apple-style bundle and the
render confirms it. Any perceived "gap" is the correct 4.4 px slot spacing —
not two separated ropes. To make bundles read TIGHTER the dial to move is the
CLIENT `GAP_PX`/`zoomScaledOffset` (segments' `gap_px` bakes 4.4 into
`offset_px`), not the corridor merge tolerance — the merge already fired.

  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.verify
  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.rebuild --site dekalb --set cross_family_gap_m=22

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
