# Barrelman

Geospatial search API powered by OSM + PostGIS. Named after the sailor stationed in the crow's nest who watches the horizon.

Barrelman is the self-hosted OSM search engine that powers [Parchment](https://github.com/alexwohlbruck/parchment). It provides place search, spatial queries, vector tiles (via Martin), and routing (via GraphHopper) — all from the same OSM PBF extract, with no dependency on commercial map data APIs.

## Architecture

```
         REGIONS  ──►  region registry  ──►  what every importer fetches
                       (config/regions.json + import_regions table)
                              │
        ┌─────────────────────┼──────────────┬──────────────┐
        ▼                     ▼              ▼              ▼
  OSM PBF extract        GTFS feeds      GBFS systems   OpenAddresses
   (Geofabrik)          (Transitland)                    + WOF + TIGER
        │                     │              │              │
        ▼                     ▼              ▼              ▼
  osm2pgsql (flex)         MOTIS          geo_* tables     Pelias
  ←─ osm2pgsql-flex.lua   (transit)                      (addresses,
        │                                                 opt-in profile)
        ▼
  geo_places table   ←── import/post-import.sql (indexes, addr extraction)
  (PostGIS)          ←── import/embed-places.ts (Ollama embeddings, optional)
        │
        ▼
  Barrelman API      ←── src/routes/    ── /console (admin UI)
  (Elysia / Bun)
        │
  ┌─────┼───────┬────────────┐
  │     │       │            │
Martin  │  GraphHopper     MOTIS
(tiles) │   (streets)     (transit)
        │
  Parchment API
```

| Service | Image | Port | Description |
|---------|-------|------|-------------|
| `barrelman` | `alexwohlbruck/barrelman` | 5001 | REST API (Elysia/Bun), and the console at `/console` |
| `barrelman-db` | `alexwohlbruck/barrelman-db` | 5434 | PostgreSQL + PostGIS + pgvector, and osm2pgsql |
| `barrelman-ops` | `alexwohlbruck/barrelman-ops` | — | Privileged worker that runs import jobs |
| `martin` | `ghcr.io/maplibre/martin` | 5002 | Vector tile server |
| `graphhopper` | `israelhikingmap/graphhopper` | 5003 | Street routing engine (walk / bike / car) |
| `motis` | `ghcr.io/motis-project/motis` | 5004 | Transit routing engine (schedules, one-to-all) |
| `pelias_api` | `pelias/api` | 4000 | Address geocoder — opt-in, see below |

Ports are host-side. Services reach each other over the Compose network on
their own container ports, so remapping a host port here changes nothing
internal.

### Optional services (profiles)

Everything above comes up with `docker compose up -d` except the address
geocoder, which is included from [`pelias/`](pelias/README.md) and gated behind
profiles — Elasticsearch alone wants a 1 GB heap, and address search isn't
needed to run Barrelman:

```bash
docker compose up -d                                       # core stack
docker compose --profile pelias up -d                      # + geocoder (api + elasticsearch)
docker compose --profile pelias --profile pelias-full up -d  # + libpostal / pip / interpolation
```

It's one Compose project, so everything shares a network and shows as a single
group in Docker UIs. Provisioning the geocoder's index is a separate, much
longer job — see [`pelias/README.md`](pelias/README.md).

---

## Documentation

| | |
|---|---|
| [**Self-hosting**](https://docs.barrelman.dev/self-hosting) | **Start here** — server to running instance, end to end |
| [Regions](docs/REGIONS.md) | Choosing what data to import |
| [Development](docs/development.md) | Running the stack from a clone, with hot reload |
| [Accounts & API keys](docs/accounts.md) | Sign-in, sessions, keys, scopes |
| [Pricing & credits](docs/pricing.md) | Endpoint costs and plans |
| [Abuse controls](docs/abuse-controls.md) | Throttling, suspension, terms |
| [Configuration](docs/configuration.md) | Every environment variable |

---

## Quick Start (Production)

No clone or build required — all services pull pre-built images from Docker Hub / GHCR.

This is the short version. **[docs.barrelman.dev/self-hosting](https://docs.barrelman.dev/self-hosting)** is
the complete walkthrough, including account setup, the optional transit and
address pipelines, TLS, and how to verify each layer.

### 1. Create a config directory

The directory name becomes the Compose project name, which some scripts resolve
sibling volumes and networks by. Name it `barrelman`.

```bash
mkdir -p /opt/barrelman && cd /opt/barrelman
```

### 2. Download the compose file

```bash
BASE=https://raw.githubusercontent.com/alexwohlbruck/barrelman/main
curl -fsSL -O $BASE/docker-compose.yml
curl -fsSL -O $BASE/martin-config.yaml
curl -fsSL -O $BASE/graphhopper-config.yml
mkdir -p custom_models data/gtfs
for m in barrelman_car barrelman_bike barrelman_foot; do
  curl -fsSL -o custom_models/$m.json $BASE/custom_models/$m.json
done
```

The Compose file bind-mounts `martin-config.yaml`, `graphhopper-config.yml` and
`custom_models/` from this directory. Docker silently creates a **directory** in
place of any missing bind source, so fetching only `docker-compose.yml` leaves
Martin and GraphHopper trying to parse a directory as their config.

### 3. Create `.env`

```dotenv
BARRELMAN_DB_PASSWORD=changeme
BARRELMAN_API_KEY=brm_changeme_use_a_strong_key
BARRELMAN_ADMIN_KEY=brm_admin_a_different_strong_key
REGIONS=north-carolina
OLLAMA_HOST=http://ollama:11434   # optional — skip if not using semantic search
```

`BARRELMAN_ADMIN_KEY` falls back to `BARRELMAN_API_KEY` when unset, so leaving
it blank hands console power — full re-imports, `DROP`/`TRUNCATE` — to anyone
holding a data key.

### 4. Start

```bash
docker compose up -d
curl http://localhost:5001/health
# {"status":"degraded","database":"connected","motis":"unavailable"}
```

`degraded` is correct on a fresh install — `status` is `ok` only once MOTIS has
a timetable, which it gets from the optional transit import. Check
`"database":"connected"`.

### 5. Claim the administrator account

The **first account created on a fresh instance becomes an administrator**. Open
`http://localhost:5001/console`, request a sign-in code, and read it out of the
log (with no SMTP configured, codes are printed rather than emailed):

```bash
docker logs barrelman --tail 50 | grep -i "sign-in code"
```

### 6. Choose a region and import

Barrelman imports named **regions**, not "everything". Fetch the boundary
catalog once, then define a region by name:

```bash
# One-time: cache the index of every importable region (no API key needed)
docker compose exec barrelman-ops bun run scripts/fetch-boundaries.ts --search colorado
```

Add `REGIONS=colorado` to your `.env` (or create the region in the admin
console under **Regions → Add by name**, which fills in the download URLs,
bounding box, transit search area and address sources for you), then run the
import — from the console's **Scripts** page, or directly:

```bash
docker compose exec -d barrelman-ops bash scripts/run-import.sh

# Check progress
docker exec barrelman-db psql -U barrelman -d barrelman \
  -c "SELECT count(*) FROM geo_places;"
```

Import commands run in **`barrelman-ops`**, not `barrelman` — the API container
is deliberately lean and has neither the docker CLI nor osmium.

A US state (~400 MB PBF) takes about 15 minutes; add ~5 for the GraphHopper
graph that follows it.

**[→ Full region guide](docs/REGIONS.md)** — what a region controls, the
ordering of the transit/address/bikeshare steps, and how to build one by hand.

---

## Local Development

### Prerequisites

- [Docker](https://docker.com) + Docker Compose v2
- [Bun](https://bun.sh) ≥ 1.1 — only for running tests on the host

### 1. Clone and configure

```bash
git clone https://github.com/alexwohlbruck/barrelman.git
cd barrelman
cp .env.example .env
```

The defaults work as-is. Nothing needs setting to get a running instance.

Optionally clone the marketing site alongside it, the same way
`parchment`/`parchment-landing` sit together — `start.sh` picks it up
automatically if present:

```bash
git clone https://github.com/alexwohlbruck/barrelman-landing.git ../barrelman-landing
```

### 2. Start everything

```bash
./start.sh dev
```

That brings up the API, the database, the tile server, both routing engines, the
console dev server and — if you cloned it — the landing site:

| | |
|---|---|
| API | http://localhost:5001 |
| API docs | http://localhost:5001/docs |
| Console | http://localhost:5199/console/ — trailing slash required in dev |
| Landing site | http://localhost:5200 |

Source is bind-mounted with hot reload, so edits to `src/`, `web/` and the
landing site apply without a rebuild.

```bash
./start.sh dev --build   # rebuild images
./start.sh dev --down    # stop
```

### 3. Pick a region and import

A fresh database has no OSM data, so search returns nothing until you import
some.

`.env` ships with `REGIONS=north-carolina,nyc-metro`, so you can skip straight
to the import if those suit you. Otherwise pick one by name from the catalog of
importable boundaries — the **Regions** page in the console does this, or:

```bash
# One-time: cache the catalog of importable regions
docker compose exec barrelman-ops bun run scripts/fetch-boundaries.ts

# See what matches, then put the key in .env as REGIONS=colorado
docker compose exec barrelman-ops bun run scripts/fetch-boundaries.ts --skip-fetch --search colorado
```

Then import, from the **Scripts** page in the console or directly:

```bash
docker compose exec -d barrelman-ops bash scripts/run-import.sh
```

North Carolina takes 20–40 minutes. See [Data Import](#data-import) for the rest
of the pipeline, and the **[region guide](docs/REGIONS.md)** for transit, address
and bikeshare data, which are separate steps.

### 4. Sign in

With no SMTP configured, sign-in codes go to the log:

```bash
docker logs barrelman --tail 20 | grep "sign-in code"
```

The first account created becomes an administrator.

More detail — hot-reload caveats, tests, database access — in
[docs/development.md](docs/development.md).

---

## Admin Console

An internal operator UI for running every data task, watching live job logs, and
monitoring service/data health. It lives in `web/` (Vue 3 + Reka UI + Tailwind)
and is served by the API at `/console`.

### What it does

- **Scripts** — run any of the ~30 catalogued tasks (OSM/GTFS/GBFS imports,
  search enrichment, migrations, routing-graph rebuilds, config generation) from
  a form UI, with parameter inputs, a live command preview, and a confirmation
  gate for destructive operations.
- **Jobs** — every run is a tracked job with streamed stdout/stderr (SSE), status,
  exit code, duration, and cancellation for process jobs.
- **Dashboard / Data** — downstream service health (Postgres, MOTIS, GraphHopper,
  Martin) and data metrics (table sizes, coverage, freshness).
- **API Tester** — send requests to the running API with server-side key injection.
- **API keys / Usage / Billing** — every signed-in developer manages their own
  keys, watches credit consumption per endpoint and per key, and handles
  subscriptions. Operator views (Scripts, Jobs, Data, Regions) are admin-only.

### Auth

The console and all `/admin/*` script/job routes are gated by `BARRELMAN_ADMIN_KEY`
(falls back to `BARRELMAN_API_KEY` when unset; open in dev when neither is set).
Set a strong, separate secret in production — these routes can trigger full
re-imports and `DROP`/`TRUNCATE`.

```dotenv
BARRELMAN_ADMIN_KEY=brm_admin_use_a_strong_key
```

### Execution model

Jobs are rows in Postgres, and there are two kinds.

- **`internal`** — SQL and migration tasks. These run in-process in the **API**,
  which already has the DB client, and stream their logs to the job store.
- **`process`** — everything that shells out (`run-import.sh`, `download-gtfs.sh`,
  graph rebuilds). The API only *enqueues* these. The privileged
  **`barrelman-ops`** worker claims them one at a time, holding a Postgres
  advisory lock so two runs of the same script can never overlap.

`barrelman-ops` exists because the API container is deliberately lean: no docker
CLI, no osmium, no python. The worker mounts the docker socket and carries that
tooling, so it can `docker exec` into `barrelman-db` to drive osm2pgsql and
restart sibling engines.

Job state — status, exit code, logs — lives entirely in the DB, so the console
renders one unified job list regardless of which process ran a job.

### Development

The console dev server (Vite + hot-reload) starts automatically with the dev
stack — no separate command:

```bash
./start.sh dev        # brings up the API + the console at http://localhost:5199/console
```

It runs as the `barrelman-console` service in `docker-compose.dev.yml` and proxies
`/admin` to the API over the compose network. It's dev-only.

To run it standalone on the host instead (e.g. without Docker):

```bash
cd web && bun install && bun run dev     # http://localhost:5199/console (proxies /admin → :5001)
```

For a production-style check, build it and let the API serve it directly:

```bash
cd web && bun run build     # emits web/dist
# then open http://localhost:5001/console
```

The production Docker image builds the console automatically (multi-stage) and
serves it at `/console` — no dev server in prod.

---

## Data Import

### Regions decide what gets imported

Coverage is driven by one environment variable, `REGIONS`, resolved against a
region registry (the `import_regions` table, seeded from
[`config/regions.json`](config/regions.json)):

```dotenv
REGIONS=colorado                    # one region
REGIONS=north-carolina,nyc-metro    # several, merged into one extract
REGIONS=global                      # planet OSM + every transit feed
```

Every importer — OSM, GTFS, GBFS and the Pelias geocoder — resolves what to
fetch from it. Define regions by name with `scripts/fetch-boundaries.ts` plus
**Regions → Add by name** in the console, or by hand.

**[→ Full region guide](docs/REGIONS.md)**

### Pipeline order

Each step depends on the previous one. Run them from the console's **Scripts**
page, or in `barrelman-ops`:

```bash
bash scripts/run-import.sh                  # OSM → PostGIS + GraphHopper graph
bash scripts/prepare-motis-osm.sh           # transit-specific OSM repair
bash scripts/download-gtfs.sh               # transit feeds (needs GraphHopper)
bash scripts/rebuild-motis.sh               # rebuild the MOTIS timetable — REQUIRED
bun run import/import-gbfs-systems.ts       # bikeshare
bun run scripts/generate-pelias-config.ts   # addresses (then pelias/provision.sh)
```

Only the first is required; the rest add transit, bikeshare and address search.

**If you do the transit steps, `rebuild-motis.sh` is not optional.** `motis
server` only serves the pre-built dataset at `/data/data` and never re-imports
when the feeds change, so a plain restart keeps serving stale schedules
indefinitely.

### What the OSM import does

| Step | Description |
|------|-------------|
| download + merge | Fetches each region's PBF and `osmium merge`s them into one extract |
| osm2pgsql | Imports all OSM objects via flex Lua style into `geo_places` |
| post-import.sql | Extracts structured address/contact fields, builds GiST + GIN indexes, computes `area_m2` |
| codes + abbreviations | Pre-computes `codes` (IATA/ICAO/ref) and `name_abbrev` for autocomplete |
| intersections | Generates road-intersection records |
| parent context | Spatial join resolving each place's containing city/county/state |
| tsvector rebuild | Rebuilds full-text search vectors to include abbreviations |

> **Note:** Do not use `--flat-nodes` for regional imports. It creates a ~31 GB sparse file that is only beneficial for full planet imports.

> **Note:** The import is not clipped to a region's bounding box — the boundary
> of your data is whatever the OSM extracts cover. The bbox narrows bikeshare
> and transit feed *discovery* only.

### Embeddings (optional)

Semantic search uses Ollama vector embeddings. All other search layers work without it.

```bash
# Pull the model (one-time, ~270 MB)
ollama pull nomic-embed-text

# Generate embeddings
bun run import:embed
```

---

## Accounts, keys and credits

Barrelman has a full account system: developers sign in to the console, mint
their own scoped keys, and their usage is metered in credits.

| | |
|---|---|
| [Accounts & API keys](docs/accounts.md) | Sign-in, sessions, keys, scopes |
| [Pricing & credits](docs/pricing.md) | What each endpoint costs on the hosted API |
| [Abuse controls](docs/abuse-controls.md) | Throttling, suspension, terms enforcement |

The short version:

- **Keys** look like `brm_live_…`, are shown once, carry scopes limiting which
  endpoint groups they may call, and revoke immediately.
- **Credits** price endpoints by what they actually cost — a tile is 1, a
  geocode 5, an isochrone 40 — because charging both as "one request" would
  either give tiles away or price routing as if it were free.
- **`BARRELMAN_API_KEY` remains a shared, unmetered service credential** — how
  Parchment calls barrelman, and how existing deployments keep working.

On a self-hosted instance, metering exists to show you your own usage. There is
no plan to buy and no bill: accounts sit on the free tier, and the console hides
its billing pages.

### Selling access is not permitted

Barrelman is source-available under Apache 2.0 with the **Commons Clause**. You
may run it for yourself or your business, including as internal infrastructure,
but you may not charge third parties for a service whose value is substantially
Barrelman — see [LICENSING.md](LICENSING.md).

Subscription billing is accordingly gated on a signed license that only the
official deployment holds (`src/lib/license.ts`). Setting `POLAR_ACCESS_TOKEN`
without one logs a warning and leaves billing off. Commercial licensing is
available if you want to offer Barrelman as a paid product or service — reach
out to discuss.

---

## API Reference

Data endpoints require a key:

```
Authorization: Bearer brm_live_...
```

Interactive docs: `http://localhost:5001/docs` — that is the authoritative
surface. The table below is the shape of it.

| Method | Path | Group | Description |
|--------|------|-------|-------------|
| `GET` | `/health` | — | Liveness + database. No auth; safe for LB probes |
| `GET` | `/health/auth` | — | Same, but validates a credential. Spends no credits |
| `POST` | `/search` | `search` | Hybrid text + semantic search |
| `GET` | `/geocode` | `geocode` | Reverse geocode a coordinate to its administrative areas |
| `GET` | `/geocode/reverse` | `geocode` | Reverse geocode to the places at a point |
| `GET` | `/geocode/place` | `geocode` | Hydrate a geocoder result |
| `GET` | `/contains` | `spatial` | Find parent areas containing a point |
| `GET` | `/children` | `spatial` | Find POIs inside an area |
| `GET` | `/place/:osmType/:osmId` | `places` | Get a single place by OSM ID |
| `GET` | `/brands`, `/brands/:key` | `places` | Brand lookup |
| `GET` `POST` | `/isochrone` | `isochrone` | Reachability polygons for any travel mode |
| `GET` | `/isochrone/modes` | `isochrone` | Supported isochrone modes and their limits |
| `POST` | `/route` | `routing` | Point-to-point street routing. Takes a **GraphHopper-native** body — `profile`, not `mode` (see below) |
| `GET` | `/graphhopper/*` | `routing` | Proxied GraphHopper |
| `POST` `GET` | `/transit/*` | `transit` | Stops, routes, departures, vehicles, intermodal routing |
| `GET` | `/gbfs/*` | `transit` | Bikeshare systems and stations |
| `GET` | `/tiles/:source/:z/:x/:y` | `tiles` | Vector tiles, proxied from Martin |

The **group** is what a key's scopes name, and what pricing is defined against —
see [accounts.md](docs/accounts.md#scopes) and [pricing.md](docs/pricing.md).

### POST `/search`

Hybrid four-layer search: full-text → abbreviation → trigram fuzzy → semantic vector.

```json
{
  "query": "coffee",
  "lat": 35.2271,
  "lng": -80.8431,
  "radius": 5000,
  "limit": 20,
  "semantic": false,
  "autocomplete": false
}
```

Set `autocomplete: true` for typeahead (skips the slow semantic layer). Set `semantic: true` to force vector search for concept queries like _"somewhere quiet to study"_.

One endpoint, three modes:

- **Text search** — pass `query`. The four-layer pipeline above.
- **Browse** — omit `query` and pass `lat`/`lng`/`radius` with `categories`
  (OSM preset IDs like `cafe`, `fuel`) and/or `tags`. Sorted by proximity,
  paginated with `offset`.
- **Route corridor** — pass a GeoJSON `route` LineString instead of a point.
  Results are constrained to a `buffer`-wide corridor around it, ranked by
  exponential decay from the line. This is what powers "what's on the way".

### GET `/geocode?lat=&lng=`

Reverse geocodes a coordinate — returns the administrative areas containing the
point (city, county, state), smallest first.

Which levels come back depends on what the imported extract actually contains,
not on the endpoint. Two things routinely reduce it:

- **A single-state extract usually has no state polygon.** Geofabrik clips the
  state boundary relation at the extract edge, so those features import as
  lines rather than closed areas and cannot contain anything. A Colorado-only
  import returns county but no state; importing a wider extract restores it.
- **Consolidated city-counties return one area, not two.** Denver, San
  Francisco and similar are a single administrative entity in OSM.

### GET `/geocode/reverse?lat=&lng=&limit=&radius=`

Reverse geocodes a coordinate to the places at it — venues, addresses, and streets — in the same result shape as `/search`.

Hits come from the geocoder (OSM + OpenAddresses) and are hydrated into their full `geo_places` rows where one exists, so a result carries real geometry, tags, and categories rather than a bare point. A hit with no address of its own borrows the street address the geocoder found at the same spot.

When nothing addressable sits within `radius` (default 100 m), falls back to the smallest administrative area containing the point, so a click on open water still resolves to something.

### GET `/contains?lat=&lng=`

Returns all named areas (smallest first) containing the given point.

### GET `/children?id=&categories=`

Returns places whose centroids fall inside the given area's polygon.

### GET `/place/:osmType/:osmId`

Fetch full details for a single OSM element. `osmType` is `node`, `way`, or `relation`.

### POST `/route`

Point-to-point routing, enriched with per-edge surface / road class / bike
network / smoothness / slope details and elevation statistics.

The body is passed through to GraphHopper, so it takes GraphHopper's own
parameters — **`profile`, not `mode`** (unlike `/isochrone`, which takes `mode`).
Omitting it returns `profile parameter required`.

```bash
curl -X POST http://localhost:5001/route \
  -H "Authorization: Bearer $BARRELMAN_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"points":[[-104.9903,39.7392],[-105.2705,40.0150]],"profile":"car"}'
```

`points` are `[lng, lat]` pairs. Profiles are `car`, `bike`, `foot` and the
custom models in [`custom_models/`](custom_models).

### GET / POST `/isochrone`

Where can you get in _N_ minutes? Returns one polygon per requested duration as a GeoJSON `FeatureCollection`.

```bash
curl -H "Authorization: Bearer $BARRELMAN_API_KEY" \
  "http://localhost:5001/isochrone?lat=35.7796&lng=-78.6382&mode=walk&durations=300,600,900"
```

The same request as JSON:

```json
{
  "lat": 35.7796,
  "lng": -78.6382,
  "mode": "transit",
  "durations": [900, 1800, 2700],
  "time": "2026-07-30T13:00:00Z",
  "simplify": 20
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lat`, `lng` | — | Origin (required) |
| `mode` | `walk` | `walk`, `bike`, `car`, `transit` (aliases: `foot`, `bicycle`, `drive`, `pt`, …) |
| `durations` | `900` | Contour budgets in **seconds** — any values, up to 8 per request. Query-string form is comma-separated |
| `arriveBy` | `false` | Reverse isochrone: the area that can *reach* the point |
| `time` | now | ISO 8601 departure (or arrival) time. Transit only |
| `simplify` | `0` | Douglas–Peucker tolerance in meters. `30` typically cuts payload ~5× |
| `maxTransfers` | — | Transit only |
| `transitModes` | all | Transit only — `BUS`, `RAIL`, `SUBWAY`, `TRAM`, `FERRY`, … |
| `maxEgressTime` | `600` | Transit only — cap (s) on the walk away from each reachable stop |
| `wheelchair` | `false` | Transit only — accessible stops and vehicles |
| `maxStopIsochrones` | `250` | Transit only — per-contour walk-isochrone budget (see below) |

Features come back smallest contour first (`bucket: 0`), so renderers should draw them back to front. Contours nest — each one is folded into the next, so a 30-minute polygon is always fully inside the 45-minute one. Geometry is a valid `Polygon` or `MultiPolygon`, never a `GeometryCollection`. Ceilings are 3 h for street modes, 2 h for transit.

**How each mode is computed.** `walk`/`bike`/`car` are GraphHopper `/isochrone` searches (evenly spaced durations like `300,600,900` are served by a single bucketed search). `transit` is composed here: MOTIS `one-to-all` reports every stop reachable within the budget, each stop gets a GraphHopper walking isochrone sized to its leftover time, and those are unioned in PostGIS with the direct walk from the origin.

**Cost.** Street isochrones are sub-second to a few seconds depending on area. A transit isochrone fans out to hundreds of walk isochrones — a 45-minute, 3-contour Manhattan query takes ~14 s cold and under a second once the polygon cache is warm. When more stops are reachable than `maxStopIsochrones`, the stop set is thinned by coarsening its dedupe grid (evenly, so the contour's outer edge survives) rather than by dropping the farthest stops; `meta.stopGridMeters` reports the resolution actually used.

---

## Configuration

Full reference: **[docs/configuration.md](docs/configuration.md)**. `.env.example`
carries the same list with inline commentary.

Nothing is required to run locally — the defaults give a working engine with an
open API and no accounts, which is the right shape for development and for a
private self-hosted instance.

The ones worth knowing:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://barrelman:barrelman@localhost:5434/barrelman` | PostGIS connection string |
| `REGIONS` | `north-carolina,nyc-metro` | Which geographies the importers pull. `global` for everything |
| `BARRELMAN_API_KEY` | — | Shared **service** credential, unmetered. **Unset means the data API is open** |
| `BARRELMAN_ADMIN_KEY` | falls back to the API key | Gates `/admin/*`. An admin-role session works too |
| `BARRELMAN_ADMIN_EMAILS` | — | Granted admin on sign-up. The first account is always an admin |
| `SMTP_HOST` | — | Without it, sign-in codes print to the log |
| `BARRELMAN_LICENSE` | — | Signed token unlocking billing. Official deployment only — see [LICENSING.md](LICENSING.md) |
| `BARRELMAN_TOS_URL` | — | Setting it requires accepting terms before creating a key |
| `OLLAMA_HOST` | `http://localhost:11434` | Embeddings for semantic search (optional) |

### Database sizing

The compose defaults are deliberately modest so a dev machine can run Postgres
alongside MOTIS, Elasticsearch and GraphHopper. **Production should size these
up** — search latency is dominated by whether the working set stays resident.
On a 32GB database host:

```dotenv
BARRELMAN_DB_SHARED_BUFFERS=8GB
BARRELMAN_DB_CACHE_SIZE=24GB
BARRELMAN_DB_WORK_MEM=128MB
BARRELMAN_DB_MAINTENANCE_WORK_MEM=2GB
BARRELMAN_DB_MEM_LIMIT=28g
```

---

## Production Deployment

Full walkthrough: **[docs.barrelman.dev/self-hosting](https://docs.barrelman.dev/self-hosting)**.

### Reverse proxy (Caddy example)

The API listens on **5001**.

```
api.example.com {
  reverse_proxy barrelman:5001
}
```

Caddy auto-provisions TLS. Connect the `barrelman` container to Caddy's network:

```bash
docker network connect caddy_network barrelman
```

Then set `BARRELMAN_SERVER_ORIGIN` and `PUBLIC_BASE_URL` to that hostname, and
`BARRELMAN_TRUSTED_PROXY_HOPS` to the number of proxies in front — every
per-address rate limit depends on it, and both mistakes are silent. See
[configuration.md](docs/configuration.md#barrelman_trusted_proxy_hops).

The compose file publishes 5002, 5003, 5004 and 5434 to the host as well. Only
the API needs to be reachable; bind the rest to loopback in an override file.

This repo's own [`Caddyfile`](Caddyfile) is a working example of the two-host
setup (`api.` + `console.`).

### Updating a deployment

New Barrelman releases are published to Docker Hub on every push to `main` via
GitHub Actions. To roll them out:

```bash
cd /opt/barrelman
docker compose pull
docker compose up -d
```

> **`docker compose pull` refreshes *every* service's image, not just
> Barrelman's.** That's fine for anything stateless, which is why most images
> here track a floating tag and pick up upstream fixes for free.
>
> Watch out for **engines that bake data into a version-specific binary
> format** — MOTIS and GraphHopper. Their on-disk timetable and routing graph
> are written by one engine version and rejected by another, so a pull that
> brings in a new major version leaves the engine unable to start until its data
> is rebuilt:
>
> ```
> tt: binary version mismatch [existing=34 vs expected=37], please re-run import
> ```
>
> That's a job, not a restart — `scripts/rebuild-motis.sh` for MOTIS,
> `scripts/rebuild-graphhopper.sh` for GraphHopper, both available from the
> console under **Scripts → Routing**. Everything else in the stack is stateless
> enough to upgrade in place. If you'd rather not be surprised, pull during a
> window where you can run the rebuild.

Watchtower can automate the pull, with two caveats worth knowing before you rely
on it:

- **It only manages containers on its own Docker host.** Watchtower watches the
  Docker socket it is given and nothing else, so an instance running on a
  different machine to the stack will never update it. Every host you want
  auto-updated needs its own Watchtower.
- **A blanket Watchtower will eventually hit the binary-format trap above.** It
  pulls the same floating tags `docker compose pull` would, so if MOTIS or
  GraphHopper are on `:latest` it will one day restart them onto an incompatible
  engine version with no one watching. Either pin those two services to explicit
  version tags, or run Watchtower with `--label-enable` and label only the
  stateless services.

Note that the original [containrrr/watchtower](https://github.com/containrrr/watchtower)
has been unmaintained since November 2023, and its Docker client negotiates API
version 1.25 — which Docker 29 refuses (`client version 1.25 is too old`),
leaving the container dead on arrival. Use the maintained community fork,
[nicholas-fedor/watchtower](https://github.com/nicholas-fedor/watchtower)
(`nickfedor/watchtower`), which takes the same flags and the same
`com.centurylinklabs.watchtower.*` labels.

### Resource recommendations

Whole-host figures, assuming you run the API, database and both routing engines
together. GraphHopper loads its graph into JVM heap rather than memory-mapping
it, so it wants several GB to itself; add ~1 GB and tens of GB of disk again if
you enable the Pelias geocoder.

| Scale | DB size | RAM | Disk | |
|-------|---------|-----|------|---|
| Single US state (Colorado) | 11 GB | 8 GB min, 16 GB comfortable | 25 GB | **measured** |
| Full United States | ~60 GB | 32 GB | 250 GB | estimated |
| Europe | ~100 GB | 64 GB | 400 GB | estimated |

The first row is measured end to end on a Hetzner CPX41 (8 shared vCPU, 16 GB):
a 360 MB Colorado extract → 6.04M places in 15 minutes, full stack with transit
in ~35 minutes, peaking at 6.4 GB resident. The rest are scaled estimates.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Runtime | [Bun](https://bun.sh) |
| HTTP framework | [Elysia](https://elysiajs.com) |
| ORM | [Drizzle ORM](https://orm.drizzle.team) |
| Database | [PostgreSQL](https://postgresql.org) + [PostGIS](https://postgis.net) + [pgvector](https://github.com/pgvector/pgvector) |
| OSM import | [osm2pgsql](https://osm2pgsql.org) (flex output) |
| Embeddings | [Ollama](https://ollama.com) (`nomic-embed-text`) |
| Tile server | [Martin](https://martin.maplibre.org) |
| Routing | [GraphHopper](https://graphhopper.com) |

---

## License

Barrelman is source-available under the **Apache License 2.0 with the Commons Clause** — see [LICENSE](LICENSE).

In short: **free to self-host and use, including inside your business — but you may not sell it.** You can run it for yourself or your company's internal use (for example, an on-prem GIS API for your own business), fork it, and contribute back. You may not resell it, or host it as a paid service, where the value comes substantially from Barrelman. See [LICENSING.md](LICENSING.md) for a plain-language explanation with examples.

Commercial and enterprise licensing — including a managed, enterprise-grade GIS REST API, dedicated support, and SLAs — is available. Reach out to Alex Wohlbruck to discuss.
