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
        │                                                separate stack)
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
| `barrelman` | `alexwohlbruck/barrelman` | 5001 | REST API (Elysia/Bun) |
| `barrelman-db` | `alexwohlbruck/barrelman-db` | 5434 | PostgreSQL + PostGIS + pgvector |
| `martin` | `ghcr.io/maplibre/martin` | 5002 | Vector tile server |
| `graphhopper` | `israelhikingmap/graphhopper` | 5003 | Street routing engine (walk / bike / car) |
| `motis` | `ghcr.io/motis-project/motis` | 5004 | Transit routing engine (schedules, one-to-all) |
| `pelias_api` | [separate stack](pelias/README.md) | 4000 | Address geocoder (OpenAddresses + OSM + WOF) |

Ports are host-side. Services reach each other over the Compose network on
their own container ports, so remapping a host port here changes nothing
internal.

Pelias is **not** part of the root `docker-compose.yml` — it joins barrelman's
network as its own stack. Everything else comes up together.

---

## Quick Start (Production)

No clone or build required — all services pull pre-built images from Docker Hub / GHCR.

### 1. Create a config directory

```bash
mkdir -p /opt/barrelman && cd /opt/barrelman
```

### 2. Download the compose file

```bash
curl -o docker-compose.yml \
  https://raw.githubusercontent.com/alexwohlbruck/barrelman/main/docker-compose.yml
```

### 3. Create `.env`

```dotenv
BARRELMAN_API_KEY=brm_changeme_use_a_strong_key
BARRELMAN_DB_PASSWORD=changeme
OLLAMA_HOST=http://ollama:11434   # optional — skip if not using semantic search
```

### 4. Start

```bash
docker compose up -d
curl http://localhost:5001/health
# {"status":"ok","database":"connected"}
```

### 5. Choose a region and import

Barrelman imports named **regions**, not "everything". Fetch the boundary
catalog once, then define a region by name:

```bash
# One-time: cache the index of every importable region (no API key needed)
docker exec barrelman bun run scripts/fetch-boundaries.ts --search colorado
```

Add `REGIONS=colorado` to your `.env` (or create the region in the admin
console under **Regions → Add by name**, which fills in the download URLs,
bounding box, transit search area and address sources for you), then run the
import:

```bash
docker exec -d barrelman bash scripts/run-import.sh

# Check progress
docker exec barrelman-db psql -U barrelman -d barrelman \
  -c "SELECT count(*) FROM geo_places;"
```

A US state (~400 MB PBF) takes roughly 20–40 minutes.

**[→ Full region guide](docs/REGIONS.md)** — what a region controls, the
ordering of the transit/address/bikeshare steps, and how to build one by hand.

---

## Local Development

### Prerequisites

- [Bun](https://bun.sh) ≥ 1.1
- [Docker](https://docker.com) + Docker Compose v2

### 1. Clone and configure

```bash
git clone https://github.com/alexwohlbruck/barrelman.git
cd barrelman
cp .env.example .env
```

Edit `.env` as needed (defaults work for local development):

```dotenv
DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/barrelman
REGIONS=north-carolina,nyc-metro
BARRELMAN_API_KEY=brm_dev_changeme
BARRELMAN_ADMIN_KEY=brm_admin_dev_changeme
OLLAMA_HOST=http://localhost:11434
TRANSITLAND_API_KEY=            # only needed for transit data
```

### 2. Start the database

```bash
docker compose up -d barrelman-db
```

Wait ~15 seconds for PostGIS to initialise.

### 3. Install dependencies

```bash
bun install
```

### 4. Pick a region and import

```bash
# One-time: cache the catalog of importable regions
bun run scripts/fetch-boundaries.ts

# See what matches, then put the key in .env as REGIONS=colorado
bun run scripts/fetch-boundaries.ts --skip-fetch --search colorado

# Import (~20-40 min for a US state)
./scripts/run-import.sh

# Optional: generate semantic search embeddings (~30-90 min on CPU)
bun run import:embed
```

`.env` ships with `REGIONS=north-carolina,nyc-metro`, so you can skip straight
to the import if those suit you. See the **[region guide](docs/REGIONS.md)** for
transit, address and bikeshare data, which are separate steps.

### 5. Run the server

```bash
bun run dev
```

Server: `http://localhost:5001`
Swagger UI: `http://localhost:5001/swagger`

---

## Admin Console

An internal operator UI for running every data task, watching live job logs, and
monitoring service/data health. It lives in `web/` (Vue 3 + Reka UI + Tailwind)
and is served by the API at `/console`.

### What it does

- **Scripts** — run any of the ~28 catalogued tasks (OSM/GTFS/GBFS imports,
  search enrichment, migrations, routing-graph rebuilds, config generation) from
  a form UI, with parameter inputs, a live command preview, and a confirmation
  gate for destructive operations.
- **Jobs** — every run is a tracked job with streamed stdout/stderr (SSE), status,
  exit code, duration, and cancellation for process jobs.
- **Dashboard / Data** — downstream service health (Postgres, MOTIS, GraphHopper,
  Martin) and data metrics (table sizes, coverage, freshness).
- **API Tester** — send requests to the running API with server-side key injection.

### Auth

The console and all `/admin/*` script/job routes are gated by `BARRELMAN_ADMIN_KEY`
(falls back to `BARRELMAN_API_KEY` when unset; open in dev when neither is set).
Set a strong, separate secret in production — these routes can trigger full
re-imports and `DROP`/`TRUNCATE`.

```dotenv
BARRELMAN_ADMIN_KEY=brm_admin_use_a_strong_key
```

### Execution model

Jobs run as child processes of the **API process** (or in-process for SQL/migration
tasks). Host-oriented scripts (`run-import.sh`, `update-osm.sh`, graph rebuilds that
`docker exec` into sibling containers) therefore expect the API to run on the host
(`bun run dev`) where the repo layout and `docker` CLI are available. DB/migration
tasks work anywhere the API can reach Postgres.

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

Each step depends on the previous one:

```bash
./scripts/run-import.sh                     # OSM → PostGIS + GraphHopper graph
./scripts/prepare-motis-osm.sh              # transit-specific OSM repair
./scripts/download-gtfs.sh                  # transit feeds (needs GraphHopper)
bun run import/import-gbfs-systems.ts       # bikeshare
bun run scripts/generate-pelias-config.ts   # addresses (then pelias/provision.sh)
```

Only the first is required; the rest add transit, bikeshare and address search.

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

## API Reference

All endpoints require a `Bearer` token:

```
Authorization: Bearer <BARRELMAN_API_KEY>
```

Interactive docs: `http://localhost:5001/swagger`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `POST` | `/search` | Hybrid text + semantic search |
| `POST` | `/nearby` | Find places within a radius |
| `GET` | `/geocode` | Reverse geocode a coordinate to city/county/state |
| `GET` | `/contains` | Find parent areas containing a point |
| `GET` | `/children` | Find POIs inside an area |
| `GET` | `/place/:osmType/:osmId` | Get a single place by OSM ID |
| `GET` `POST` | `/isochrone` | Reachability polygons for any travel mode |
| `GET` | `/isochrone/modes` | Supported isochrone modes and their limits |

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

### POST `/nearby`

Find places within a radius, sorted by distance.

```json
{
  "lat": 35.2271,
  "lng": -80.8431,
  "radius": 1000,
  "categories": ["amenity/cafe"],
  "limit": 20
}
```

### GET `/geocode?lat=&lng=`

Reverse geocodes a coordinate — returns the city, county, and state containing the point.

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

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `REGIONS` | `north-carolina,nyc-metro` | **Which regions to import.** Comma-separated keys, or `global`. See the [region guide](docs/REGIONS.md) |
| `DATABASE_URL` | `postgresql://barrelman:barrelman@localhost:5434/barrelman` | PostGIS connection string |
| `BARRELMAN_DB_PASSWORD` | `barrelman` | Used by `docker-compose.yml` for the DB container |
| `PORT` | `5001` | HTTP port the API listens on |
| `BARRELMAN_API_KEY` | `brm_dev_changeme` | Shared Bearer token for API auth. **Change before deploying.** |
| `BARRELMAN_ADMIN_KEY` | falls back to `BARRELMAN_API_KEY` | Gates `/console` and every `/admin/*` route. Set a strong, separate secret in production |
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama endpoint for generating search embeddings |
| `ISOCHRONE_CONCURRENCY` | `8` | Parallel GraphHopper isochrone requests. Raise it only when GraphHopper isn't also serving interactive routing |
| `BARRELMAN_STATEMENT_TIMEOUT_MS` | `10000` | Statement timeout on the API query pool. Schema DDL and the enrichment backfill are exempt. `0` disables |

### Import & updates

| Variable | Default | Description |
|----------|---------|-------------|
| `UPDATE_MODE` | `replication` | `replication` (incremental diffs) or `full` (re-download + re-import) for `update-osm.sh` |
| `GEOFABRIK_URL` | — | Legacy single-extract override. Bypasses `REGIONS`; normally leave unset |
| `GEOFABRIK_REPLICATION_URL` | — | Replication server for `update-osm.sh` / `init-replication.sh` |
| `GITHUB_TOKEN` | — | Optional. Raises the GitHub API rate limit when resolving OpenAddresses coverage for a new region |

### Downstream services

| Variable | Default | Description |
|----------|---------|-------------|
| `TRANSITLAND_API_KEY` | — | Required for GTFS import. Free at [transit.land](https://www.transit.land/users/sign_up) |
| `GTFS_REGION` | — | Optional override of the GTFS search area, bypassing `REGIONS` |
| `MOTIS_URL` | `http://localhost:8080` | Transit routing engine. Set automatically in Compose |
| `MOTIS_RT_UPDATE_INTERVAL` | `60` | Seconds between GTFS-RT polls of every feed. Raise on a dev box to cut polling |
| `GRAPHHOPPER_URL` | `http://localhost:8989` | Street routing engine. Set automatically in Compose |
| `GRAPHHOPPER_JAVA_OPTS` | `-Xmx6g -Xms1g` | **Must exceed the on-disk `graph-cache` size** — the graph is loaded into heap, not mapped. Raise whenever `REGIONS` grows |
| `PELIAS_URL` | `http://pelias_api:4000` | Address geocoder. A [separate stack](pelias/README.md) |
| `BARRELMAN_DB_SHARED_BUFFERS` | `2GB` | Postgres `shared_buffers`. Dev-sized — see below |
| `BARRELMAN_DB_CACHE_SIZE` | `4GB` | Postgres `effective_cache_size` |
| `BARRELMAN_DB_WORK_MEM` | `64MB` | Postgres `work_mem`. Below ~64MB, bitmap scans over `geo_places` go lossy |
| `BARRELMAN_DB_MAINTENANCE_WORK_MEM` | `1GB` | Postgres `maintenance_work_mem` (index builds, `VACUUM`) |
| `BARRELMAN_DB_RANDOM_PAGE_COST` | `1.1` | SSD/NVMe value. Raise toward `4` on spinning disks |
| `BARRELMAN_DB_MEM_LIMIT` | `4g` | Container memory cap for `barrelman-db` |

### Database sizing

The compose defaults are deliberately modest so a dev machine can run Postgres
alongside MOTIS, Elasticsearch and GraphHopper. **Production should size these
up** — search latency is dominated by whether the working set stays resident.
On a 32GB database host:

```
BARRELMAN_DB_SHARED_BUFFERS=8GB
BARRELMAN_DB_CACHE_SIZE=24GB
BARRELMAN_DB_WORK_MEM=128MB
BARRELMAN_DB_MAINTENANCE_WORK_MEM=2GB
BARRELMAN_DB_MEM_LIMIT=28g
```

---

## Production Deployment

### Reverse proxy (Caddy example)

```
barrelman.example.com {
  reverse_proxy barrelman:3001
}
```

Caddy auto-provisions TLS. Connect the `barrelman` container to Caddy's network:

```bash
docker network connect caddy_network barrelman
```

### Automatic updates with Watchtower

[Watchtower](https://containrrr.dev/watchtower/) automatically pulls and restarts containers when new images are published:

```bash
docker run -d \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower --interval 3600
```

New Barrelman releases are published to Docker Hub on every push to `main` via GitHub Actions.

### Resource recommendations

| Scale | DB size | RAM | Disk |
|-------|---------|-----|------|
| Single US state (e.g. NC) | ~10 GB | 2 GB | 20 GB |
| Full United States | ~60 GB | 8 GB | 120 GB |
| Europe | ~100 GB | 16 GB | 200 GB |

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
