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
  TED + platform-ref bonuses at stop observations. Break on infeasible gaps and bridge
  with the original shape segment (flagged), never force.
- **Regime B (no/degenerate shapes)**: pfaedle-style sparse stop-to-stop matching on the
  same core.
- **Output**: `matched_shapes` table (method, confidence, stats jsonb) + rewritten
  `shapes.txt` in the processed zip + `gtfs_shapes` upsert — MOTIS and display consume
  identical geometry. Simplify (~1 m topology-preserving), dedup shapes across patterns.
- **Quality gates per pattern**: coverage % within tolerance, discrete Fréchet ≤100 m,
  length ratio 0.95–1.15, every stop snaps. Any fail → keep original shape
  (`fallback`) — good feeds are never degraded. Per-feed summary logged.

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
- Length acceptance: total skeleton km within 0.75–1.25× of the **merge-width-fused
  network** — `area(union(shapes buffered at MERGE_WIDTH/2))) / MERGE_WIDTH`, computed
  independently of the raster path. The raw union of used ways is NOT the contract
  reference: OSM-matched directional track pairs run ~4–10 m apart, so the raw union
  double-counts every double-tracked corridor and fusing those pairs is this stage's
  whole job (CTA: union 288 km vs fused 172 km vs published ~165 km route length —
  skeleton/union sits near 0.55 structurally). The raw union keeps a 0.40–1.00 sanity
  band only.
- Emits the existing `transit_graph_nodes/edges/edge_lines` contract + builds ledger.

### Stage 5 — ordering + slot stabilization (task #5)

MLNCM-S per the LOOM TSAS 2019 paper; reductions first, solver second:

- Reductions in value order: P2 partner collapse → P1 deg-2 contraction → C1
  single-route edge cut → C2 terminus detach → U2/U3 (Y) → U4/U5 (double-Y) → U6
  (stump) → U1 (X). Cut everything, then solve *connected components* independently —
  a line leaving and rejoining a bundle stays one component by construction.
- Per-component cascade: |search space|=1 → done; <500 → exhaustive; else CP-SAT
  (OR-Tools; permutation vars + reified crossing/separation booleans); fallback
  greedy-lookahead + simulated annealing. Node-local weights per the paper (stations
  penalized ~3–4× over junction nodes).
- Then slot stabilization: propagate slots along corridors; changes only at
  composition-change nodes. Machine invariant: zero (color_key, line_count) groups with
  >1 slot on a steady corridor.

### Stage 6 — segmentation + tiles (task #6)

- Steady segments (junction→junction, constant composition+slot): per-line features
  with constant `offset_px`.
- Transition segments (fixed ground length ~60 m straddling composition-change nodes):
  per-line features with `off_from_px`/`off_to_px`, densified ~5–10 m, centerline
  arc-filleted with min radius ≥ total bundle width.
- `transit_lines_rt(z,x,y)` emits per-line features + local clip fractions
  (`ST_LineLocatePoint` against the feature, direction-normalized) — reuse the proven
  machinery in `create-transit-lines-runtime.sql`, repointed at semantic segments.
- No baked line fallback (decision 6). Baked source retained only if needed as the
  bullet carrier. Retire the v1 fixed-18 m matview.

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
- **Global soundness**: a frequencies-based feed and a calendar_dates-only feed
  attribute correctly; a weekend-only station shows its weekend routes.
