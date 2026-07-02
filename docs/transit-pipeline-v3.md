# Transit pipeline v3 — approved design (PAR-12)

Status: approved by Alex 2026-07-01 (nine review directives folded in). This supersedes
`transit-pipeline-v2-prompt.md` where they conflict; v2's §7 hard lessons still apply.
Client-side constraints live in `parchment/docs/par-12-transit-v3-handoff.md`.

## Binding decisions (from review)

1. **MOTIS ingests only fully preprocessed GTFS** — overrides, transfers, fares, and the
   OSM shape rewrite all land in the feed zip *before* anything parses it; MOTIS
   ingestion is scripted, never manual.
2. **Service-level derivation must be globally correct** — frequencies.txt,
   calendar_dates-only, weekend-only, night-only, once-daily services all attribute
   sanely (representative-day counting + tiered per-complex gate; spec below).
3. **LOOM is replaced by a custom implementation** (reductions + ordering per the
   ad-freiburg papers; we own every knob).
4. **Every route is path-matched to OSM** (rail→railways, bus→roads, ferry→ferry
   routes). The matched geometry *replaces* GTFS shapes and feeds both MOTIS and
   display. Replaces pfaedle (`shapesnap`, spec below).
5. **Ordering scales globally**: single-route edges are cut, connected components solved
   independently (= LOOM Cutting rule C1, proven). Long-distance operators (Amtrak,
   Eurostar, Greyhound) collapse to one visual line per operator, Apple-style.
6. **No baked-tile fallback for lines.** Engines without the variable line-offset fork
   render constant offsets with a hard "step" at junctions. (A baked source may persist
   solely as the route-bullet symbol carrier — bullets cannot be perpendicular-offset.)
7. **No Transitland click-throughs** — interactive transit features open Parchment's own
   backend data.
8. **Buses are in the layer, single color, trunked** — shared corridors collapse to one
   line whose label lists all routes; no multi-slot bundling for buses.
9. **Line-click disambiguation uses `responsivePopover`** (popover desktop, bottom sheet
   mobile).

## Pipeline (GTFS → tiles)

```
regions.json → download raw zips → sanitize
  → per-zip transforms:  shapesnap (OSM shape rewrite)     [stage 3]
                         display overrides → routes/stops.txt
                         computed transfers.txt
                         fares v2 injection
  → data/gtfs-processed/<feedId>.zip        ← the ONLY artifact consumers read
  → DB import (gtfs_*)   + service derivation (rep-day counts)   [stages 1–2]
  → rebuild-motis.sh (volume sync + `motis import` + restart)    [stage 1]
  → centerline pipeline per (mode, region): raster skeleton      [stage 4]
  → ordering + slot stabilization                                [stage 5]
  → steady/transition segmentation → PostGIS                     [stage 6]
  → Martin tile functions → client                               [stage 6–7]
```

### Stage 1 — staged import + scripted MOTIS ingestion (task #1)

Audit verdicts to fix: MOTIS ingestion is manual/out-of-band (stale volume copies);
display overrides are DB-only (itinerary colors from MOTIS disagree with map today);
zip transforms run *after* the DB parse.

- Restructure `import/import-gtfs.ts`: download+sanitize all → per-zip transform stage
  (shapesnap slot, overrides→`routes.txt`/`stops.txt`, transfers, fares) → emit
  `data/gtfs-processed/<feedId>.zip` → `importFeedFile` parses the *processed* zip →
  `generateMotisConfig` points at processed zips.
- All consumers switch to `data/gtfs-processed/`: `importFeedFile`, MOTIS config,
  `vehicles.service.ts`, shape importers, graph build. Raw downloads become inputs no
  consumer reads.
- `scripts/rebuild-motis.sh`: sync processed zips + `motis/config.yml` into the
  `barrelman-gtfs-data` volume, `docker exec barrelman-motis /motis import` (hash-gated,
  cheap when unchanged), restart server. Called/printed as the import's final step.
- Keep `applyDisplayOverrides` (DB) as the fast idempotent path; zip becomes source of
  truth. `run-pfaedle.sh` + `import-shapes-886.ts` retired (shapesnap landed — see
  `docs/shapesnap.md`, `scripts/run-shapesnap.sh`). Fix `download-gtfs.sh` path bug
  (`src/import/…` → `import/…`).

### Stage 2 — service-level derivation, globally correct (task #2)

Replace the single `weekday_trips` count with representative-day counting:

- Resolve each `service_id` to its active-date set over a horizon (feed validity ∩
  [today−7d, today+60d]; feed's own window if stale) — calendar.txt weekday bits AND
  `start_date`/`end_date`, plus calendar_dates additions/removals. Handles
  calendar-only, calendar_dates-only, and mixed feeds identically; kills expired and
  seasonal ghosts.
- Pick three representative dates (max-service weekday, Saturday, Sunday) — dodges
  holidays and deflates overlapping service-variant duplicates.
- Expand `frequencies.txt` (headway multiples per window; offsets from the trip's first
  stop time). Clock test on `sec % 86400` (fixes >24:00 encodings).
- New columns on `gtfs_stop_routes` (keep `weekday_trips = trips_weekday_day` for
  compat): `trips_weekday_day`, `trips_weekday_any`, `trips_weekend_day`, `trips_any`.
- Display gate moves to per-station-complex sums (cluster first, then sum across
  platforms) with tiers: weekday-daytime ≥2 → weekend-daytime ≥2 → any ≥1. A station
  with real service can never render label-less. Anchors preserved: Christopher St
  hides the late-night-only 2; Kingston Av hides the lone AM-rush 5.
- `transit_stops` view uses the same tier policy (window-function-free LATERAL).
- Fix the naive `line.split(',')` in `derive-rail-stop-routes.ts` (quote-aware parse).

### Stage 3 — `shapesnap`: GTFS→OSM map matching (task #3)

Unit of work: route × direction × stop-pattern (from `gtfs_trip_patterns`), never
per-trip. Two regimes, one Viterbi core:

- **Graph**: per mode-class from `data/region.osm.pbf` (pyosmium), edges connect ONLY at
  shared OSM node IDs — elevated (`bridge`, `layer≥1`) and subway (`tunnel`, `layer<0`)
  ways never share nodes, so Loop-style vertical separation is structural, not
  heuristic. Never geometric noding, never `geo_places` linestrings. Tag filters per
  mode (rail/subway/tram/bus/ferry; service/usage penalties; psv access re-opens;
  turn-restriction relations with psv exceptions). Cache per region+mode.
- **Regime A (feed has shapes)**: dense HMM à la Newson-Krumm/Meili — resample shape
  ~30 m, Gaussian emission σ≈15 m, transition `exp(−|along−network|/β)` via bounded A*,
  per-route OSM `route=*` relation bonus (×0.5 on matching-relation edges), station-name
  TED + platform-ref bonuses at stop observations. Relation matching is TIERED
  (`candidates.RouteMatcher`): identity (relation `ref` == `route_short_name`, or the
  relation name contains the long/short name) outranks colour-only equality — on
  colour-collapsed networks `route_color` is a FAMILY key (NYC N/Q/R/W share #F6BC26),
  so whenever any candidate identity-matches the pattern, colour-only relations count
  as non-matching; colour stays the fallback signal for networks whose relations carry
  no usable ref/name. On identity-tier patterns, foreign-identity edges (decorated only
  with OTHER routes' ref/name relations) pay a heavy emission prior, and a contiguous
  observation run whose layers hold only foreign-identity candidates is EXCISED
  (widened to the enclosing stop anchors) so the network path spans the degenerate
  stretch on the route's own track (MTA R drawn through the Lexington corridor at
  Canal St). Break on infeasible gaps and bridge with the original shape segment
  (flagged), never force — but a dense break's gap is first RETRIED once at
  `gap_retry_radius_mult`× the candidate radius (emission σ widened by the same
  factor, endpoints pinned to the already-decoded candidates), so a systematic
  agency-vs-OSM offset just past the radius (4/5 Joralemon St Tunnel: 47–64 m for
  ~420 m) reconnects on the route's own track; a gap that also fails the retry
  (track genuinely absent) bridges as before, and a splice that would fail any
  quality gate is reverted to the bridge.
- **Regime B (no/degenerate shapes)**: pfaedle-style sparse stop-to-stop matching on the
  same core.
- **Output**: `matched_shapes` table (method, confidence, stats jsonb) + rewritten
  `shapes.txt` in the processed zip + `gtfs_shapes` upsert — MOTIS and display consume
  identical geometry. Simplify (~1 m topology-preserving), dedup shapes across patterns.
- **Quality gates per pattern**: coverage % within tolerance, discrete Fréchet ≤100 m
  (evaluated piecewise across excised foreign runs — the fabricated straight jump over
  an excision is not agency geometry), length ratio 0.95–1.15, every stop snaps. Any
  fail → keep original shape (`fallback`) — good feeds are never degraded. Per-feed
  summary logged.

### Stage 4 — centerline pipeline (custom, raster skeleton; task #4)

Per (mode-class, region cell), from *matched* shapes:

- Mode classes: rail (subway+elevated+tram merge together), bus, ferry — separate
  systems. Bus color_key is uniform (decision 8) so bus corridors trunk into single
  lines. Long-distance operators collapse to one line per operator (decision 5) via
  color_key/operator grouping before rasterization.
- Rasterize at ~1–2 m/px, stroke = MERGE_WIDTH (start 15–20 m rail); skeletonize
  (scikit-image); vectorize (nodes at degree≠2 pixels); attribute routes back to edges;
  snap/group stations. Junctions fall out of skeleton topology — crossing-not-parallel
  tracks never merge (Tower 18 exam).
- Enclosed background holes THINNER than the stroke fill before skeletonizing
  (`raster.fill_sliver_holes`): centerlines MERGE_WIDTH..2× MERGE_WIDTH apart leave a
  sliver every point of which lies within MERGE_WIDTH/2 of ink — below the merge
  criterion — and skeletonizing around it yields parallel duplicate centerlines plus
  line-less ladder rungs (NYC receipts: the two F alignments 22 m apart east of
  Broadway-Lafayette drew F twice 5 m apart with two line-less ~200 m rungs; the
  Bowling Green turnback zigzags skeletonized into 3 parallel edges between one node
  pair + a deg-6 node). Holes with genuine clearance — the Chicago Loop interior,
  flying-junction eyes — are untouched; per-hole decision, never a partial fill.
- Attribution carries a CONVERSE deviation gate (`attribute.DEVIATION_GATE_M`, 50 m):
  snapping guarantees every sample is near SOME edge, not that every claimed edge is
  near the pattern — adjacency bridge-fill can paint phantom ribbons onto edges the
  route never rides (NYC receipts: R on the 7's Queens Blvd elevated 700 m from any R
  shape, B on the White Plains Rd 2/5 corridor, R/W on the West End line). A chained
  edge whose densified geometry strays beyond the gate from the pattern's own shape
  anywhere is excised from that pattern; a genuinely ridden edge stays within snap
  radius plus junction displacement everywhere, so the gate never fires on it.
- Station complexes with the SAME name conflicting over one snap target merge into
  the winning node instead of one failing (MTA lists Queensboro Plaza per division).
- Edges NO pattern rides after gating are pruned before emit, and the deg-2 nodes
  they leave behind re-join into single head-to-tail edges (crossing rungs a shade
  over the contraction bound — the 21 m 4/5×N/R/W rung at Fulton St): stage 5's
  raw-slot corridor stability relies on corridors arriving as single aligned edges,
  and line-less rows only inflate `transit_graph_edges` for consumers that skip them.
- Length acceptance: total skeleton km within 0.75–1.25× of the **merge-width-fused
  network** — `area(union(shapes buffered at MERGE_WIDTH/2))) / MERGE_WIDTH`, computed
  independently of the raster path. The raw union of used ways is NOT the contract
  reference: OSM-matched directional track pairs run ~4–10 m apart, so the raw union
  double-counts every double-tracked corridor and fusing those pairs is this stage's
  whole job (CTA: union 288 km vs fused 172 km vs published ~165 km route length —
  skeleton/union sits near 0.55 structurally). The raw union keeps a 0.40–1.00 sanity
  band only.
- Emits the existing `transit_graph_nodes/edges/edge_lines` contract + builds ledger.

#### Loop exam (stage-4 acceptance)

`linegraph/exam/loop_exam.py` compares `chicago:l-v3` against the LOOM baseline
(`chicago:l`) and the `mm_edges` OSM QA table, entirely in PostGIS: every sample
point of every elevated-family (Brn/P/Org/G/Pink) edge lies within 25 m of a
surface/elevated OSM rail way, no escape hatch — the same probe fails 9
LOOM edges on the Dearborn/State subways (the Tower 18 over-merge receipt, 0 in
v3); Blue/Red Loop-interior corridors stay single-route and hug tunnel ways;
per-leg route bundles (Lake/Wabash/Van Buren/Wells) match ground truth derived
from `matched_shapes`; every Loop-window station labels a node within 100 m;
Tower 18 is a junction and mid-block Dearborn is not. Also writes a side-by-side
PNG + per-build GeoJSON of the Loop window. Exits non-zero on any failure. Run:

```
uv run --with-requirements linegraph/requirements.txt \
    python linegraph/exam/loop_exam.py --out data/exam
```

Spec amendment — Lake-leg Blue bundling (needs spec-author sign-off): the
Milwaukee–Dearborn Blue subway genuinely runs beneath the Lake St elevated, so
Lake-leg edges bundle Blue with the elevated routes. This plan-view fusion is
by design and matches the official CTA map treatment; it is also what lets
stage 5 offset Blue beside G/Pink — a separate coincident Blue edge would leave
two bundles stacked at offset 0 with no cross-edge ordering. The exam holds the
allowance to physical proof per edge, not a lat threshold: co-attribution only
on the Lake leg, a tunnel-tagged rail way within 25 m of every sample point,
and Blue's own matched shape within one merge width (18 m) of the centerline.
Red is never co-attributed anywhere in the window.

The 25 m coverage check has no fallback reference: an earlier `mm_edges` hole
at the Purple line's Linden terminal turned out to be a graph-crop bug (the
chicago bbox north edge sat at 42.07; Linden is at 42.0734) — fixed by widening
the bbox to 42.09 in `config/regions.json` / `config/shapesnap.json` and
re-dumping `mm_edges` (`shapesnap.graph --postgis`), not by relaxing the exam.

### Stage 5 — ordering + slot stabilization (task #5)

MLNCM-S per the LOOM TSAS 2019 paper, implemented in `lineorder/` (replaces the
`loom` binary); reductions first, solver second:

- Reductions in value order: P2 partner collapse → P1 deg-2 contraction → C1
  single-route edge cut → C2 terminus detach → U2/U3 (Y) → U4/U5 (double-Y) → U6
  (stump) → U1 (X). Cut everything, then solve *connected components* independently —
  a line leaving and rejoining a bundle stays one component by construction
  (chicago:l-v3: 167 edges → 23 in 2 components).
- Per-component cascade: |search space|=1 → done; <500 → exhaustive; else CP-SAT
  (OR-Tools; permutation vars + reified crossing/separation booleans, interleaved so
  parallel search is deterministic); fallback greedy-with-lookahead + simulated
  annealing (T0=1000, Ti=T0/i), everything seeded. Weights per the paper's
  section-6 scheme, always evaluated on the ORIGINAL node v*: non-station
  (same, diff, sep) = (4, 1, 3)×deg(v*); station deg>2 = (12, 3, 9)×deg(v*);
  station deg 2 = (4, 4, 3)×maxdeg (any crossing dominates).
- Slot writeback: `python -m lineorder.apply --build-key chicago:l-v3` solves and
  writes `transit_graph_edge_lines.slot` (left-to-right position in the edge's
  storage direction) in ONE transaction scoped to that build_key, refuses to
  regress the stored score, and records the run in `lineorder_runs`
  (scores before/after, crossing counts, per-method component histogram, jsonb
  detail, timestamps; table created idempotently). `python -m lineorder.solve`
  is the report-only diagnostic — it never writes.
- Slot stabilization is structural, not a fixup pass: after P1 contraction a
  steady corridor is a single reduced edge and linegraph emits corridor edges
  head-to-tail, so the reconstructed optimum keeps each line's slot constant
  along every maximal degree-2 chain with a constant line set. Machine
  invariant: zero (color_key, slot, line_count) groups — the display SQL's
  grouping — split a corridor; slots change only at junctions (deg≥3) or
  composition-change nodes, which are exactly the stage-6 transition sites.
- Optimality semantic: "optimal" (solve/apply output, `lineorder_runs` statuses)
  means optimal over the *corridor-stable* subspace, not the unconstrained
  MLNCM-S optimum. P1 contracts every original-degree-2 node unconditionally, so
  at a non-station deg-2 node flanked by deg-3 station junctions the true
  optimum can be cheaper (w_same = 4·2 = 8 mid-corridor vs w_diff = 3·3 = 9 at
  the junction) — deliberately excluded because a mid-corridor crossing violates
  the corridor-stability invariant above (pinned by
  `lineorder/tests/test_synthetic.py::test_p1_station_flanked_corridor_stability`;
  chicago:l-v3 is unaffected — a direct unreduced CP-SAT solve matches the
  cascade at 116.0 with zero deg-2 cost).

#### Stability exam (stage-5 acceptance)

`lineorder/exam/stability_exam.py` validates the STORED slots of `chicago:l-v3`
(run after apply; read-only) and exits non-zero on any failure: (1) corridor
stability — raw slot constancy and the equivalent display grouping, zero
violations; (2) transitions only at junctions/composition changes, each site
listed with routes — Loop window holds exactly the stage-4 exam's 6 junction
nodes; (3) crossing audit — totals + locations, no crossing at a degree-2
non-station node; (4) LOOM contrast — the same corridor walk over `chicago:l`
shows the baseline instability v2 inherited (20 corridor/line slot violations
across Brn/G/Org/P/Pink; P and Org up to 3 distinct slots in one corridor) vs
v3's zero; (5) determinism — two fresh
solves and the stored slots are identical, so rerunning solve+apply is a no-op.

```
uv run --with-requirements lineorder/requirements.txt \
    python lineorder/exam/stability_exam.py
```

### Stage 6 — segmentation + tiles (task #6)

Implemented in `segments/` (corridor walk + colour-collapsed ribbons →
steady/transition features → `transit_line_segments` → Martin function
`transit_lines_rt2`):

- Steady segments (junction→junction, constant composition+slot): per-ribbon
  features with constant `offset_px` (colour-collapse mirrors the display SQL:
  `color_key = COALESCE(NULLIF(route_color,''),'rid:'||route_id)`;
  `offset_px = (ribbon_slot - (ribbon_count-1)/2) * 4.4`).
- Transition segments (fixed 60 m ground length — tunable `--transition-len` —
  centred on junction/composition nodes): per-ribbon features with
  `off_from_px`/`off_to_px` in the feature's own travel frame, densified ≤7.5 m,
  centerline biarc-filleted (G1 at the seams) with min radius ≥
  `line_count * gap_px * fillet_radius_factor`; short corridors shrink their
  halves, fully consumed corridors merge their two transitions; ribbons on ≥3
  corridor ends pair by matched_shapes traversal evidence; terminating ribbons
  keep a constant-offset steady stub into the node (terminus polish = client).
- `transit_lines_rt2(z,x,y)` (`import/create-transit-lines-rt2.sql`) emits the
  segment features + local clip fractions (`ST_LineLocatePoint` against the full
  feature, direction-normalized, continuous across tile seams — the proven
  machinery of `create-transit-lines-runtime.sql` repointed at semantic
  segments), plus legacy slot/line_count for stock-Mapbox degradation;
  transitions gated to z ≥ 11. Registered in `martin-config.yaml`.
- No baked line fallback (decision 6). Baked source retained only if needed as the
  bullet carrier. `transit_lines_rt`/`transit_lines_centerline` (v2) untouched.

Build + emit (delete-and-replace per build_key), then apply the tile function:

```
uv run --with-requirements segments/requirements.txt \
    python -m segments.build --build-key chicago:l-v3 --emit
docker exec -i barrelman-db psql -U barrelman -d barrelman \
    < import/create-transit-lines-rt2.sql
```

#### Segments exam (stage-6 acceptance)

`segments/exam/segments_exam.py` validates the EMITTED rows (read-only, exits
non-zero on failure): (0) DB rows equal a fresh deterministic rebuild; (1) C1 —
the transition-site inventory is exactly stage 5's list (18 sites: 17 junctions
+ Howard, cross-checked coordinate-for-coordinate against lineorder's loader),
a full ribbon walk finds ZERO offset discontinuities at feature boundaries —
offsets change only inside transition features anchored to listed sites — and
every feature end is accounted for: shared with another same-ribbon end or
sitting on one of the ribbon's genuine termini (degree-1 nodes of its corridor
subgraph, each occupied), so a dropped or mislocated transition orphans an end
and fails; (2) C3 — every transition length within [0.4, 1.1]× the configured
60 m, vertex spacing ≤ 7.5 m; (3) fillet — every transition meets its
min-radius floor, measured on the emitted DB geometry (clamps and inherited
track curvature recorded), no self-intersections; (4) coverage — per-ribbon
feature length covers the corridors within 1%, no overlaps > 1 m beyond the
tail two same-ribbon transitions legitimately share at a branch-divergence
site (currently the Green Line 63rd St split); (5) per-Loop-leg receipt (Lake /
Wabash / Van Buren / Wells / interior subways with slots + offsets, all Loop
transitions with their ramps).

`segments/exam/loop_visual.py` renders the before/after receipt: the Loop
window at simulated z15 (m/px = 78271.51696/2¹⁵·cos lat) drawn with the same
per-vertex machinery the MapLibre fork uses — miter-joined perpendicular
offsets, bevelled past the client's miter-limit of 2, transitions eased
cubic-bezier(.4,0,.6,1) along line-progress — side by side with the rejected
v2 model (`transit_lines_centerline` merged runs × 0/0.15/0.85/1 linear
taper).

```
uv run --with-requirements segments/requirements.txt \
    python segments/exam/segments_exam.py
uv run --with-requirements segments/requirements.txt \
    python segments/exam/loop_visual.py --out data/exam/loop-v3-vs-v2.png
```

### Stage 7 — client (task #7–8, parchment repo)

Steady = constant `line-offset` (both engines). Transitions = fork-powered
`['interpolate',['cubic-bezier',.4,0,.6,1],['line-progress'],0,['get','off_from_px'],1,['get','off_to_px']]`
on MapLibre; Mapbox shows the step. Delete `applyTransitOffsetTaper`. Port keeper client
work from `par-12-offset-baked`. Interactions: line click → route detail with
`responsivePopover` disambiguation; station/stop clicks → Parchment backend (no
transitland provider); rail/bus/ferry mode filter; wire hover.

## Acceptance exams

- **Chicago Loop (feed 29)**: clean rectangle, 5–6-line bundles per leg; Blue/Red
  subways through the interior, never merged with the elevated at Tower 18; transitions
  only where lines enter/leave the Loop; bullets/labels legible.
- **NYC regression**: Broadway trunk one yellow ribbon set; DeKalb junction sane;
  Christopher St / Kingston Av attribution anchors hold.

### NYC (feed 5, `nyc:subway-v3`) — milestone 5

What NYC exercises that Chicago cannot: colour collapse (N/Q/R/W share
route_color `F6BC26` in feed 5 — the family must render as ONE ribbon on the
Broadway trunk; same for 1/2/3, A/C/E, 4/5/6, B/D/F/M), 4-track express/local
corridors whose parallel OSM ways must fuse to one centerline, and ~10x
Chicago's graph (623 nodes / 708 edges / 1802 edge-lines; 144 corridors,
164 transition sites; solves in seconds after the reductions shatter it).

`segments/exam/nyc_exam.py` (read-only, exits non-zero on failure): (1)
Broadway yellow trunk — perpendicular cross-sections between Times Sq and
Canal St each hit exactly one yellow feature with a co-running subset of
{N,Q,R,W}, and a window sweep asserts no two distinct yellow steady features
run side-by-side (parallel within 20 m for >100 m — the duplicate-centerline
failure the raster's MERGE_WIDTH fusion prevents); (2) trunk family table —
Broadway N/Q/R/W, 7th Av 1/2/3, 8th Av A/C/E, Lexington 4/5/6/6X each one
ribbon, Queens Blvd exactly the E + F/FX/M + R three-ribbon bundle; (3)
DeKalb Av / Flatbush Av — the full transition inventory (48 features), every
one anchored to graph junction / composition-change sites, fillet floors
measured on the emitted rows, no self-intersections.

The generic exams run against the build directly and must also pass:
`lineorder/exam/stability_exam.py --build-key nyc:subway-v3` (zero corridor
violations, deterministic re-solve) and `segments/exam/segments_exam.py
--build-key nyc:subway-v3` (checks 0–4; consumed-corridor merges make
multi-site transitions, hence the per-site length bound).

`segments/exam/nyc_visual.py --window broadway|dekalb` renders the two
receipt windows at simulated z15 with the loop_visual machinery.

```
uv run --with-requirements segments/requirements.txt \
    python segments/exam/nyc_exam.py
uv run --with-requirements segments/requirements.txt \
    python segments/exam/nyc_visual.py --window broadway --out data/exam/nyc-broadway.png
```
- **Global soundness**: a frequencies-based feed and a calendar_dates-only feed
  attribute correctly; a weekend-only station shows its weekend routes.
