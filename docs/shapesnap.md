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
  own track — a genuinely absent track still fails the retry and is bridged with the
  original geometry, and a splice that would push any quality gate below threshold is
  reverted (a retry never degrades a pattern below its bridged baseline).
- **Gates** (`shapesnap.gates`): coverage ≥95 % within tolerance, discrete Fréchet
  ≤100 m, length ratio 0.95–1.15, every stop within the candidate radius. **Any failure
  → the pattern keeps its original shape** (`fallback`); good feeds are never degraded.

## What a run rewrites

`shapesnap.run` rewrites the zip atomically (tmp + rename):

- `shapes.txt` — matched geometries deduped per feed by geometry hash (ids
  `snap_<hash12>`), plus every original shape still referenced by untouched trips;
  orphaned originals are dropped.
- `trips.txt` — trips of *matched* patterns remapped to the snap ids;
  fallback/passthrough trips keep their original `shape_id`.
- `shape_dist_traveled` — recomputed along the new geometry **in the unit the feed
  used** (inferred from the original shapes.txt: m/ft/km/mi; CTA is feet), and the
  `stop_times.txt` rows of remapped trips get matching stop distances (monotonic
  projection), keeping the dist reference consistent zip-wide.

Metadata lands in PostGIS (created idempotently): `matched_shapes` (per pattern:
method, confidence, stats jsonb incl. gates + elevated/subway/surface meters, geom) and
`shapesnap_runs` (per-run summary jsonb). The last stdout line is machine-readable:
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
