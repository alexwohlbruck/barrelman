# Prompt: Rebuild the Parchment transit-map geometry pipeline from scratch (replace LOOM + pfaedle)

You are building the geometry-generation half of Parchment's transit map. The
current implementation uses two external C++ tools — **LOOM**
(`gtfs2graph | topo | loom`) and **pfaedle** (OSM shape map-matching) — and they
produce topology errors we cannot bend: LOOM over-merges non-parallel tracks at
complex junctions, and pfaedle degrades already-good agency shapes. **Trash both
and write a custom, fast, readable geospatial pipeline** that turns GTFS (+
optional OSM) into Apple-/Transit-app-grade interlined geometry, baked into
PostGIS for the existing Martin/MapLibre serving stack.

The display/serving half already works well — **do not rebuild it.** You are only
replacing the graph-generation + shape-cleaning core.

---

## 0. Read these first (all of them, before writing code)

- **Linear PAR-12** "Transit layer" — the requirements + case studies. It shows
  Apple vs Google vs Transit at the **Chicago Loop** and explicitly names
  *"the Chicago blue line joining the loop"* as THE hard junction problem.
- **Transit App blog** (the method to copy):
  - "Transit Maps: Apple vs. Google vs. Us" — https://blog.transitapp.com/transit-maps-apple-vs-google-vs-us-cb3d7cd2c362/
  - "How we built the world's prettiest auto-generated transit maps" —
    https://blog.transitapp.com/how-we-built-the-worlds-prettiest-auto-generated-transit-maps-12d0c6fa502f/
    (THE key one: pixel-skeletonization to find shared segments, ILP line-ordering
    to minimize crossings, OSM matching that "considers what type of vehicle runs
    on the line", cut-holes-and-reconnect at junctions.)
- **Master plan**: `.claude/plans/our-transit-layer-currently-shiny-shell.md`
  (north-star bets, staging, global-scale, cross-platform floor).
- ad-freiburg **LOOM** and **pfaedle** repos — understand what they do so you
  reimplement the *good* parts (topology merge, station grouping, crossing-min
  ordering) without their failure modes.

---

## 1. What already works — KEEP, do not touch

Only the geometry generator is being replaced. Everything below stays:

- **Client rendering** (`parchment/web/src/components/map/layers/`):
  `TransitStationMarker.vue` (custom station dots — route-coloured / white
  interchange — + name/bullet labels), `transit-stations-layer.ts` (screen-space
  declutter, dots for all, labels on the winners), `transit-station-glyphs.ts`
  (entrance/elevator SDF icons). Layer templates + styling in
  `parchment/server/src/constants/default-layers/transit.ts` (top slot, salmon
  station areas, along-line bullets, theme-aware casing).
- **Zoom-baked constant-pixel offsets** — `barrelman/import/create-transit-lines-offset-zoom.sql`:
  per-integer-zoom `ST_OffsetCurve` at `(slot-(line_count-1)/2) * 4.4px *
  (78271.51696 / 2^z)` in EPSG:3857, Chaikin-smoothed, served by the Martin
  **function source** `transit_lines_zoom(z,x,y)` (registered in
  `martin-config.yaml` `postgres.functions:`). This gives a constant on-screen
  gap at every zoom AND clean baked joins, cross-platform (plain MVT). KEEP.
- **Station route attribution** — `create-transit-stations.sql` derives a
  station's routes from **regular weekday-daytime service** (via
  `gtfs_stop_routes.weekday_trips`, populated by the streaming
  `import/derive-rail-stop-routes.ts`), with a `route_id` fallback when
  `route_short_name` is blank (CTA L). KEEP.
- **Serving**: Martin (`cache_size_mb: 0` in dev) + parchment proxy
  `/proxy/barrelman/:source/...` (dev returns `no-store` so rebuilds show on
  reload). KEEP.
- **GTFS import** (`import/import-gtfs.ts`): stops/routes/shapes/stop_routes/
  trip_patterns + mode partitioning. KEEP (it feeds your pipeline).

---

## 2. What to replace — and exactly why it failed

Delete the use of `loom:latest` and `ghcr.io/ad-freiburg/pfaedle` (and
`scripts/run-pfaedle.sh`, the LOOM half of `scripts/build-transit-graph.sh`).

- **LOOM** builds `transit_graph_nodes/edges/edge_lines`. Its `topo
  --max-aggr-dist` (default 50m; we tried 20m) merges edges by geometric
  proximity, which **over-merges tracks that are near but NOT parallel**. Concrete
  failure: at Chicago **Tower 18** it bundled the elevated Loop onto the
  Dearborn/State **subways** (they only cross there), fabricating Brown/Purple
  geometry on Dearborn where the raw shapes never go. Tightening `-d` reduced but
  never eliminated it, and risks fragmenting real bundles.
- **pfaedle** re-matches GTFS shapes onto OSM. With `-D` it produced 1.3M
  over-dense points for CTA and pulled the Loop onto wrong ways — it **degrades
  feeds whose shapes are already good** (its own header admits subway "gains
  little"). Also: its Docker ENTRYPOINT is already `pfaedle` (passing `pfaedle`
  again = "Multiple feeds" error) — moot once removed.

---

## 3. The approach: pixel skeletonization (Transit App's method)

This is the core replacement for LOOM's `topo`. It gives you **one intuitive
tunable — line thickness = merge width** — which is exactly the knob LOOM got
wrong. Per `(mode, region-cell)`:

1. Load every route shape (per route × direction) for the mode. Project to a
   metric CRS (local UTM, or 3857 with the cos(lat) correction baked into the
   px↔metre scale).
2. **Rasterize** all shapes onto a grid (~1–2 m/px), each stroked at width
   `MERGE_WIDTH` (ground metres; start ~15–20m for rail). Overlapping strokes
   fuse; tracks farther apart than `MERGE_WIDTH` (elevated vs subway) stay
   distinct. *This is the merge control.*
3. **Skeletonize** to 1-px centrelines (`skimage.morphology.skeletonize`),
   preserving topology.
4. **Vectorize** the skeleton → a graph: nodes at junctions/degree≠2 pixels,
   edges = the pixel runs between them. Simplify + smooth edges.
5. **Attribute routes to edges**: for each original shape, walk it and assign it
   to the nearest skeleton edge(s) it follows → which routes share each segment
   (`line_count`, per-route membership).
6. **Snap stations** to nearest graph node; group stops into station complexes
   (proximity + shared name / parent_station).
7. **Order lines per edge** to minimize crossings at junctions — propagate a
   consistent slot order across the graph; use a small ILP or LOOM's ordering
   heuristic (the blog reduced this to ~0.2s with ILP).
8. **Emit** to PostGIS in the exact schema below.

Why raster/skeleton over LOOM's geometric aggregation: junctions fall out of the
skeleton topology naturally, and non-parallel crossings don't merge unless their
strokes actually overlap within `MERGE_WIDTH`. Expose `MERGE_WIDTH` per
mode/feed.

Optional refinement: also paint OSM rail ways (mode-filtered) onto the raster as
faint "ink" to bias the skeleton toward real alignment — replacing pfaedle's
*goal* without its over-matching (the agency shape still dominates).

---

## 4. OSM alignment (pfaedle's other job) — optional, conservative, per-feed

Most agency shapes are fine; only align feeds whose shapes are visibly off the
basemap. Make it **opt-in per feed and validated**:
- Custom HMM map-matcher onto OSM ways filtered by mode, OR nearest-rail snap
  with a small tolerance, OR the raster-ink bias in §3.
- Measure Hausdorff distance before/after and reject if it moves geometry beyond
  a threshold or increases jaggedness. **Never blind-rematch good feeds** — that
  was pfaedle's mistake.

---

## 5. Recommended stack: **Python**

Rationale: the core algorithm is a first-class `scikit-image` function
(`skeletonize`, `medial_axis`); Python has the best geospatial ecosystem; it's
the easiest to read and iterate; the heavy libs are C-backed so it's fast enough
for offline batch. Port hot loops to Rust/numba later only if global scale needs
it.
- `shapely` + `geopandas` + `pyproj` (geometry, CRS)
- `numpy` + `scikit-image` (rasterize, skeletonize) + `rasterio`/`affine` (grid↔world)
- `networkx` (line graph + ordering)
- `psycopg2` (+ `shapely.wkb`) or `SQLAlchemy`+`GeoAlchemy2` for PostGIS I/O
- Run as a one-shot Docker job (`profiles: [import]`), per feed × mode,
  staging→swap, mirroring the current build orchestration.

Alternatives if the team wants a compiled language: **Rust** (`geo`, `image`,
`imageproc`, `petgraph`, `rstar`) — faster, more work; **Go** — simple but weaker
geospatial. **Start with Python.**

---

## 6. Output contract — emit EXACTLY this (so downstream is untouched)

Match `barrelman/src/db.ts ensureTransitGraphSchema()` and how
`import/load-transit-graph.ts` populates it. Keep `build_key = "<region>:<mode>"`
(e.g. `chicago:l`, `nyc:subway`).

- `transit_graph_nodes(id, build_key, loom_id, station_id, station_label, geom Point4326)`
- `transit_graph_edges(id, build_key, loom_id, line_count, geom LineString4326)`
- `transit_graph_edge_lines(edge_id, slot, feed_id, route_id, route_short_name, route_type, route_color, route_text_color)`

Then, unchanged: `create-transit-lines-offset-zoom.sql` bakes the offsets,
`create-transit-stations.sql` builds station labels, Martin serves, the client
renders. You can reuse `load-transit-graph.ts` as the loader (feed it your
GeoJSON), or write straight to PostGIS.

---

## 7. Hard lessons from the last attempt — do NOT repeat

- Merge width must equal the visual line thickness and must NOT merge tracks that
  merely cross/approach (Tower 18). This is the whole point of the raster method.
- Never blind-rematch feeds with good shapes (pfaedle degraded CTA).
- Offsets use the **512-px** tile resolution `78271.51696 / 2^z` (256-px
  `156543/2^z` is 2× too wide → "off by one zoom").
- Route attribution = regular weekday-daytime service, not any-trip presence,
  not track proximity.
- `ST_ChaikinSmoothing(geom, 2, true)` is good junction rounding (endpoints
  preserved → stations stay on the line).
- Station route bullets: fall back to `route_id` when `route_short_name` is blank.

---

## 8. Acceptance criteria — the Chicago Loop is the exam

Build CTA (feed 29) + NYC subway (feed 5); verify live in the preview:
- **Chicago Loop / Tower 18**: the elevated Loop is a clean **rectangle** with
  the 5–6 lines bundled on each leg; the **Blue (Dearborn)** and **Red (State)**
  subways run separately N–S through the interior and are **NOT** merged with the
  elevated lines; **no line fabricated through the interior**. (This is exactly
  where LOOM failed — it is the pass/fail test.)
- **NYC subway**: clean bundled trunks (e.g. Broadway N/Q/R/W as one yellow
  ribbon set), stations grouped, interchanges correct.
- Constant-pixel offset across zoom; routes/stops clickable → detail panels; mode
  filter; correct dark mode; cross-platform (MapLibre + Mapbox + native SDKs, via
  baked MVT).
- Offline batch build time acceptable; incremental per-cell rebuilds (hash
  ledger).

---

## 9. Milestones

1. Scaffold the Python job + PostGIS I/O emitting the §6 schema; wire it into the
   build orchestration (replace the LOOM half of `build-transit-graph.sh`).
2. Raster→skeleton→graph for ONE feed (CTA). **Pass the Loop exam** (rectangle +
   subway separation) before moving on.
3. Route-to-edge attribution + crossing-minimizing ordering.
4. Station grouping.
5. NYC subway + one bus feed; tune `MERGE_WIDTH` per mode.
6. Optional conservative OSM alignment.
7. Global cells + incremental rebuild ledger. Delete `run-pfaedle.sh`, the LOOM
   Docker usage, and the `loom`/`pfaedle` images once the exam passes.

Work on a feature branch, never merge to main. Verify each milestone live with
the preview (the user runs the dev servers; barrelman + Martin in Docker). Use
`bun` for any JS glue. Keep commits short and logical.
