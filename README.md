# Barrelman

Geospatial search API powered by OSM + PostGIS. Named after the sailor stationed in the crow's nest who watches the horizon.

Barrelman is the self-hosted OSM search engine that powers [Parchment](https://github.com/alexwohlbruck/parchment). It provides place search, spatial queries, vector tiles (via Martin), and routing (via GraphHopper) — all from the same OSM PBF extract, with no dependency on commercial map data APIs.

## Architecture

```
OSM PBF extract (Geofabrik)
        │
        ▼
  osm2pgsql (flex)   ←── import/osm2pgsql-flex.lua
        │
        ▼
  geo_places table   ←── import/post-import.sql (indexes, addr extraction)
  (PostGIS)          ←── import/generate-abbreviations.ts
        │             ←── import/embed-places.ts (Ollama embeddings, optional)
        ▼
  Barrelman API      ←── src/routes/
  (Elysia / Bun)
        │
  ┌─────┼──────┐
  │     │      │
Martin  │  GraphHopper
(tiles) │  (routing)
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

Ports are host-side. Services reach each other over the Compose network on
their own container ports, so remapping a host port here changes nothing
internal.

---

## Documentation

| | |
|---|---|
| [Development](docs/development.md) | Running the stack with Docker Compose |
| [Accounts & API keys](docs/accounts.md) | Sign-in, sessions, keys, scopes |
| [Pricing & credits](docs/pricing.md) | Endpoint costs and plans |
| [Abuse controls](docs/abuse-controls.md) | Throttling, suspension, terms |
| [Polar setup](docs/polar-setup.md) | Wiring up billing |
| [Configuration](docs/configuration.md) | Every environment variable |

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

### 5. Import OSM data

Download a PBF and run the import inside the DB container (all tools are baked in):

```bash
# Download region (example: North Carolina)
docker exec barrelman-db bash -c '
  wget -O /data/region.osm.pbf \
    https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf
'

# Run the import detached (survives SSH disconnects)
docker exec -d barrelman-db bash -c '
  osm2pgsql --create --slim --output=flex \
    --style=/app/import/osm2pgsql-flex.lua \
    -d "$DATABASE_URL" /data/region.osm.pbf \
  && psql "$DATABASE_URL" -f /app/import/post-import.sql \
  && echo IMPORT_COMPLETE || echo IMPORT_FAILED
'

# Check progress
docker exec barrelman-db psql -U barrelman -d barrelman \
  -c "SELECT count(*) FROM geo_places;"
```

A US state (~400 MB PBF) takes roughly 20–40 minutes. See [Data Import](#data-import) for more detail.

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
| Console | http://localhost:5199/console |
| Landing site | http://localhost:5200 |

Source is bind-mounted with hot reload, so edits to `src/`, `web/` and the
landing site apply without a rebuild.

```bash
./start.sh dev --build   # rebuild images
./start.sh dev --down    # stop
```

### 3. Import data

A fresh database has no OSM data, so search returns nothing until you import
some. Either use the **Scripts** page in the console, or:

```bash
docker compose exec barrelman-ops bash scripts/import-osm.sh
```

North Carolina takes 20–40 minutes. See [Data Import](#data-import) for other
regions and the rest of the pipeline.

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

- **Scripts** — run any of the ~28 catalogued tasks (OSM/GTFS/GBFS imports,
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

The import pipeline transforms an OSM PBF extract into a fully indexed PostGIS database.

### Download a PBF extract

```bash
# North Carolina (~400 MB)
wget -O data/region.osm.pbf \
  https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf

# Germany
wget -O data/region.osm.pbf \
  https://download.geofabrik.de/europe/germany-latest.osm.pbf

# Full United States
wget -O data/region.osm.pbf \
  https://download.geofabrik.de/north-america/us-latest.osm.pbf
```

Find all regions at [download.geofabrik.de](https://download.geofabrik.de).

### Import pipeline

| Step | Description |
|------|-------------|
| osm2pgsql | Imports all OSM objects via flex Lua style into `geo_places` |
| post-import.sql | Extracts structured address/contact fields, builds GiST + GIN indexes, computes `area_m2` |
| generate-abbreviations.ts | Pre-computes `name_abbrev` for autocomplete |
| tsvector rebuild | Rebuilds full-text search vectors to include abbreviations |

> **Note:** Do not use `--flat-nodes` for regional imports. It creates a ~31 GB sparse file that is only beneficial for full planet imports.

### Embeddings (optional)

Semantic search uses Ollama vector embeddings. All other search layers work without it.

```bash
# Pull the model (one-time, ~270 MB)
ollama pull nomic-embed-text

# Generate embeddings
bun run import:embed
```

---

## Public API — accounts, keys and credits

Barrelman can be run as a public, metered API as well as a private engine.
Developers sign in to the console, mint their own keys, and are billed in
credits against a monthly allowance.

| | |
|---|---|
| [Accounts & API keys](docs/accounts.md) | Sign-in, sessions, keys, scopes |
| [Pricing & credits](docs/pricing.md) | What each endpoint costs, what each plan includes |
| [Abuse controls](docs/abuse-controls.md) | Throttling, suspension, terms enforcement |
| [Polar setup](docs/polar-setup.md) | Wiring up billing |

The short version:

- **Keys** look like `brm_live_…`, are shown once, carry scopes limiting which
  endpoint groups they may call, and revoke immediately.
- **Credits** price endpoints by what they actually cost — a tile is 1, a
  geocode 5, an isochrone 40 — because charging both as "one request" would
  either give tiles away or price routing as if it were free.
- **The free tier stops at its allowance** with a `402` rather than accruing
  overage. Nobody can run up a bill on a plan they did not pay for.
- **`BARRELMAN_API_KEY` remains a shared, unmetered service credential** — how
  Parchment calls barrelman, and how existing deployments keep working.

| Plan | Price | Credits / month | Past the allowance |
|---|---|---|---|
| Free | $0 | 100,000 | **Stops with `402`** |
| Developer | $19 | 1,000,000 | $0.030 / 1k |
| Business | $99 | 10,000,000 | $0.018 / 1k |
| Scale | $299 | 40,000,000 | $0.012 / 1k |
| Enterprise | Custom | Negotiated | $0.008 / 1k |

All of it is optional. With no `POLAR_ACCESS_TOKEN` the billing surface is
inert, every account sits on free, and metering exists only to show a
self-hosted operator their own usage.

---

## API Reference

Data endpoints require a key:

```
Authorization: Bearer brm_live_...
```

Interactive docs: `http://localhost:5001/docs`

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
| `POLAR_ACCESS_TOKEN` | — | Enables billing. Without it every account stays on free |
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
