# shapesnap — GTFS→OSM shape rewrite (pipeline v3, stage 3)

shapesnap path-matches every GTFS route onto OSM ways (rail→railways, bus→roads,
ferry→ferry routes) and **replaces the feed's `shapes.txt`** inside the processed zip
(`data/gtfs-processed/<feedId>.zip`), so MOTIS routing and the display pipeline consume
identical OSM-aligned geometry. It replaces pfaedle, whose CTA run put Brown on the
subway ways under the Loop and blew shapes.txt up to 1.3 M points (post-mortem +
approved design: `docs/transit-pipeline-v3.md`, stage 3).

## How it works

- **Unit of work**: route × direction × stop-pattern — never per-trip.
- **Graph** (`shapesnap.graph`): per mode-class from an OSM pbf; edges connect *only* at
  shared OSM node ids, so elevated (`bridge`/`layer≥1`) and subway (`tunnel`/`layer<0`)
  ways never touch except at real portals — Loop-style vertical separation is
  structural. Cached per region+mode under `data/shapesnap/*.graph.pkl.gz`, rebuilt
  automatically when missing/stale.
- **Matcher** (`shapesnap.match`): one Viterbi core, two regimes — dense HMM over the
  resampled feed shape (feeds with shapes), pfaedle-style sparse stop-to-stop (feeds
  without). OSM `route=*` relation bonuses disambiguate vertically stacked tracks.
  Infeasible gaps break (never force); a dense break's gap is retried once with the
  candidate radius and emission σ widened ×`gap_retry_radius_mult` (default 2 — rail
  50→100 m) and its endpoints pinned to the already-decoded candidates, so a
  systematic agency-vs-OSM offset just past the radius (the 4/5 Joralemon St Tunnel:
  47–64 m off for ~420 m; CTA Blue at O'Hare) reconnects seamlessly on the route's
  own track.
- **Gates** (`shapesnap.gates`): coverage ≥95 % within tolerance, discrete Fréchet
  ≤100 m, length ratio 0.95–1.15, every stop within the candidate radius. Any failure
  walks the on-OSM fallback chain below; a match below the bar is never emitted.

## The on-OSM policy

**Every output path lies on OSM ways except where OSM literally lacks the track.**
Two mechanisms enforce it, and a per-pattern metric proves it:

- **Graph-routed gap bridges.** A break whose gap survives the retry is ROUTED through
  the OSM graph between the pinned anchor candidates — same weighted cost model as
  decoding (class penalties, route-relation ×`route_bonus_mult`, turn restrictions,
  reversal penalties), generous weighted cutoff `bridge_route_cutoff_factor` (default
  4×) times the gap's along-reference length, floored at `bridge_route_min_cutoff_m`
  (300 m, one urban block for junction-adjacent micro-gaps). Feasible → the graph path
  is spliced (`stats.gaps[].bridge_kind="graph"`, still on OSM); infeasible — OSM
  genuinely disconnected within the budget — → the original geometry bridges the gap
  exactly as before (`bridge_kind="agency"`). Pattern gates still apply: on gate
  failure a revert ladder walks (retries, graph) → (no retries, graph) → (retries,
  agency) → (no retries, agency) and keeps the first passing assembly, so neither a
  retry splice nor a graph bridge can degrade a pattern below its agency-bridged
  baseline.
- **Sparse rescue (the fallback chain).** When a dense pattern still fails its gates,
  it is re-matched once in the SPARSE regime (stop-sequence matching on the same
  graph — the regime shapeless feeds already use). The rescue is deliberately **not**
  gated on Fréchet-vs-agency: the agency shape is exactly what failed and may itself
  be the wrong thing (96 St 2nd Av: the MTA shape ends 103 m short of the stop while
  the OSM track passes ~1.5 m from it — under this policy that pattern rescues onto
  the track instead of keeping the mis-drawn shape). The rescue's gates are:
  - **stop-snap** — every stop within the sparse candidate radius of the output
    (rail 200 m / bus 100 m / ferry 500 m: the rescue's own matching tolerance);
  - **length plausibility** — output length / stop-chain chord in
    [`rescue_length_ratio_min`, `rescue_length_ratio_max`] = [0.90, 1.60]. The chord
    is a lower bound on any real path, but the output can measure slightly *under*
    it when off-track stop coordinates project inward onto the track at terminals
    (96 St again) or zigzag stop placement inflates the chord — hence 0.90 rather
    than the dense regime's 0.95; 1.60 is the battle-tested sparse upper bound
    (genuine detours fit, >1.6× chord signals wrong-corridor wandering);
  - **no empty stop layers** — a stop with no candidate in radius would be
    chord-bridged straight *through* its own coordinate, faking the stop-snap gate;
    an empty layer is direct evidence OSM lacks the track, so the rescue refuses;
  - **non-degenerate output** (the shared `empty_output` gate).

  A passing rescue is emitted as `hmm_sparse_rescue` (its trips are remapped like any
  match; the dense attempt's failure is kept under `stats.dense_attempt`). Only when
  the rescue also fails does the pattern keep its agency geometry —
  `passthrough_agency`, logged loudly on stderr. `sparse_rescue=False` restores the
  old gates-fail behavior.
- **The metric.** Every result carries `stats.on_osm_m` / `stats.agency_m` — meters of
  output geometry riding OSM edges vs spliced agency geometry (bridges + connector
  seams), measured on the assembled line so the two sum exactly. `shapesnap.run`
  aggregates them into the run summary (`on_osm_pct`, `patterns_off_osm` with per-
  pattern meters) and prints every pattern below 100 %. Acceptance: NYC (feed 5) and
  Chicago (feed 29) each ≥99.5 % on-OSM by length, every exception explainable as
  missing OSM track.

## What a run rewrites

`shapesnap.run` rewrites the zip atomically (tmp + rename):

- `shapes.txt` — matched geometries deduped per feed by geometry hash (ids
  `snap_<hash12>`), plus every original shape still referenced by untouched trips;
  orphaned originals are dropped.
- `trips.txt` — trips of *matched* patterns (`hmm_dense` / `hmm_sparse` /
  `hmm_sparse_rescue`) remapped to the snap ids; `passthrough_agency` /
  `passthrough` trips keep their original `shape_id`.
- `shape_dist_traveled` — recomputed along the new geometry **in the unit the feed
  used** (inferred from the original shapes.txt: m/ft/km/mi; CTA is feet), and the
  `stop_times.txt` rows of remapped trips get matching stop distances (monotonic
  projection), keeping the dist reference consistent zip-wide.

Metadata lands in PostGIS (created idempotently): `matched_shapes` (per pattern:
method, confidence, stats jsonb incl. gates, elevated/subway/surface meters and the
on-OSM split `on_osm_m`/`agency_m`, geom) and `shapesnap_runs` (per-run summary jsonb
incl. `on_osm_pct` + `patterns_off_osm`). The last stdout line is machine-readable:
`[shapesnap] SUMMARY {json}`.

## Running it

```sh
scripts/run-shapesnap.sh 29                 # rewrite data/gtfs-processed/29.zip
scripts/run-shapesnap.sh 29 --dry-run       # match + report only, no writes
scripts/run-shapesnap.sh 29 --routes Brn,Blue --limit 4   # debugging
# equivalent raw invocation (repo convention: uv, never system python):
uv run --with-requirements shapesnap/requirements.txt \
    python -m shapesnap.run --feed 29 --zip data/gtfs-processed/29.zip
```

**Reference reseed (reruns).** The rewrite is in place, so on a rerun the
dense-regime reference would be the *previous run's snapped output*, not the
feed's own shapes — and the reference drifts (verified: the same pattern got a
different snap id, Fréchet 29.5 m vs 49.7 m). `run-shapesnap.sh` therefore
restores `shapes.txt` / `trips.txt` / `stop_times.txt` into the processed zip
from the pristine raw zip (`data/gtfs/<feedId>.zip`) before matching — exactly
what the import pipeline hands shapesnap, since the shape rewrite is the first
transform step; members baked in afterwards (display overrides, transfers.txt,
fares) are preserved. `SHAPESNAP_RESEED=0` skips it; `--dry-run` never touches
the zip. The raw CLI invocation above does **not** reseed — prefer the script
for reruns, or reseed manually first.

Tests: `uv run --with-requirements shapesnap/requirements.txt python -m pytest shapesnap/tests -v`
(the real-data exams auto-skip without `data/il.osm.pbf` + `data/gtfs/29.zip`).

## Config — `config/shapesnap.json`

Per feed: `{enabled, modes, pbf, bbox, graphStem}`. `enabled` gates only the import
pipeline's hook; the CLI runs for any feed you point it at. Default disabled; feed 29
(CTA) rail is the pilot. `pbf` defaults to `data/region.osm.pbf`; `bbox`+`graphStem`
select a cropped graph cache (`data/shapesnap/<graphStem>.<mode>.graph.pkl.gz`).

## Pipeline integration (how MOTIS and display stay in sync)

`import/import-gtfs.ts` transform stage order (raw → processed): **shapesnap first**,
then display overrides, then computed transfers.txt, then fares v2 — all *before* the
DB parse, MOTIS config generation, and `scripts/rebuild-motis.sh`. Everything
downstream (PostGIS `gtfs_shapes`, the centerline/ordering pipeline, MOTIS) reads the
one processed artifact, so routing polylines and map lines can never disagree.

The hook (`applyShapeRewrite`) is fail-open: non-zero exit, missing summary, or a
gate-summary anomaly (0 matched patterns) logs loudly and restores the unrewritten
zip — a feed import never hard-fails on shapesnap.
