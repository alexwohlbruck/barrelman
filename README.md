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
DATABASE_URL=postgresql://barrelman:barrelman@localhost:5433/barrelman
BARRELMAN_API_KEY=brm_dev_changeme
OLLAMA_HOST=http://localhost:11434
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

### 4. Import data

```bash
# Download NC OSM extract
wget -O data/region.osm.pbf \
  https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf

# Import (~20-40 min)
bun run import:osm

# Optional: generate semantic search embeddings (~30-90 min on CPU)
bun run import:embed
```

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

Barrelman can be run as a public API. Developers sign in to the console, mint
their own API keys, and are metered in credits against a monthly allowance.

### Signing in

The console at `/console` supports one-time email codes, passkeys, and
Google/GitHub/GitLab. Passkeys and OAuth are additive — email codes always work,
so removing the last passkey or unlinking every provider cannot lock an account
out.

The **first account created on a fresh instance becomes an administrator**, so a
new deployment is never locked out of its own console. After that, promote by
listing addresses in `BARRELMAN_ADMIN_EMAILS`.

### API keys

Keys look like `brm_live_…` (or `brm_test_…`) and are shown exactly once, at
creation — only a SHA-256 digest is stored, so a lost key is rolled, not
recovered. Revocation takes effect immediately.

```bash
curl -H "Authorization: Bearer brm_live_..." \
  "http://localhost:5001/contains?lat=35.77&lng=-78.63"
```

Tile URLs also accept `?api_key=` (or the older `?token=`), because a map
library fetches tiles itself and cannot set a header.

Each key carries **scopes** naming the endpoint groups it may call. A key
embedded in a web map can be limited to `tiles` and `search`, and is then
worthless for running up a routing bill. **Test keys** run the entire request
path — auth, scopes, rate limits — while spending no credits, so integration
suites cost nothing.

### Credits

Endpoints are metered in credits rather than requests, because they are not
equally expensive — a vector tile is one indexed read, an isochrone fans out to
hundreds of routing calls.

| Group | Credits | Endpoints |
|---|---|---|
| `tiles` | 1 | `/tiles/*` |
| `places` | 2 | `/place/*`, `/brands` |
| `spatial` | 2 | `/contains`, `/children` |
| `geocode` | 2 | `/geocode/*` |
| `search` | 3 | `/search`, `/autocomplete` |
| `routing` | 10 | `/route`, `/graphhopper/*` |
| `transit` | 25 | `/transit/*`, `/gbfs/*` |
| `isochrone` | 25 | `/isochrone` |

Every response carries `X-Barrelman-Credits-Charged`. A request that fails with
a 5xx is refunded — customers should not pay for our outages.

| Plan | Credits / month | Requests / minute | Past the allowance |
|---|---|---|---|
| Free | 50,000 | 60 | Stops with `402` |
| Developer | 1,000,000 | 600 | Billed as overage |
| Scale | 10,000,000 | 3,000 | Billed as overage |

The free plan **stops** rather than accruing charges, so nobody can run up a
bill on a plan they did not pay for. Live figures come from
`GET /account/plans`.

Billing is optional: with no `POLAR_ACCESS_TOKEN` configured the subscription
surface is inert, every account sits on the free plan, and metering exists only
to show a self-hosted operator their own usage.

### Abuse controls

Throttling is layered, cheapest check first, so an abusive caller is refused
before we spend anything on them:

| Layer | Bounds | Why the layer below is not enough |
|---|---|---|
| Penalty box | Callers collecting a stream of 401/402/429 | Answering an error forever is free for a key-guesser, not for us |
| Per-IP | One source address | Anonymous traffic never reaches an account |
| Per-key | 80% of the account budget | One leaked key must not starve the account's other keys |
| Per-account | The plan's published limit | — |
| Concurrency | Simultaneous isochrone / transit / routing | A caller inside their per-minute limit can still pin every engine |

All of it is in memory, so with N API replicas the effective limits are N times
these. That protects the process and the upstreams; the credit ledger in
Postgres remains the accurate record for anything with money attached.

A periodic sweep raises **abuse signals** for an administrator to review —
burn-rate spikes, sustained error hammering, many accounts from one sign-up
address. A signal is an observation, not a judgement, and nothing here bans
anyone automatically, with one exception: a paid account burning 25× its
monthly allowance in a single day takes a **self-lifting six-hour hold**,
because that is real money accruing with no ceiling.

### Suspension

An administrator can suspend an account from **Accounts** in the console. It
takes effect immediately — every session ends and every API key stops working —
and the reason is shown to the user verbatim, both at sign-in and on every API
response. Suspensions can be time-limited, in which case they lift themselves.
Every action lands in an append-only audit log.

### Terms of service

Set `BARRELMAN_TOS_URL` and users must accept the terms before creating an API
key. Bumping `BARRELMAN_TOS_VERSION` asks everyone again. **Existing keys keep
working across a bump**, so a terms change never breaks a running integration.

### Service key

`BARRELMAN_API_KEY` remains a shared, **unmetered** service credential — this is
how Parchment's own server calls barrelman, and how existing deployments keep
working unchanged. It is not a customer key and is never billed.

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

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://barrelman:barrelman@localhost:5433/barrelman` | PostGIS connection string |
| `BARRELMAN_DB_PASSWORD` | `barrelman` | Used by `docker-compose.yml` for the DB container |
| `PORT` | `3001` | HTTP port the API listens on |
| `BARRELMAN_API_KEY` | `brm_dev_changeme` | Shared, unmetered **service** credential (Parchment → barrelman). Not a customer key. **Change before deploying.** |
| `BARRELMAN_ADMIN_KEY` | falls back to `BARRELMAN_API_KEY` | Shared key for `/admin/*`. An admin-role account session works too |
| `BARRELMAN_SERVER_ORIGIN` | `http://localhost:$PORT` | Public origin. Used in emails and as the OAuth redirect base |
| `BARRELMAN_CONSOLE_ORIGIN` | server origin | Only if the console is hosted separately. Also sets the WebAuthn relying-party ID |
| `BARRELMAN_ADMIN_EMAILS` | — | Addresses granted admin on sign-up. The first account is always an admin |
| `BARRELMAN_REGISTRATION_MODE` | `open` | `invite` limits sign-in to accounts an admin created |
| `BARRELMAN_ACCOUNTS_ENABLED` | `true` | `false` disables accounts entirely, falling back to shared-key auth |
| `BARRELMAN_SIGNUPS_PER_IP_PER_DAY` | `5` | New accounts one address may create per day. `0` disables |
| `SMTP_HOST` / `SMTP_USER` / `SMTP_PASS` | — | Sign-in code delivery. Without it, codes are printed to the server log |
| `POLAR_ACCESS_TOKEN` | — | Enables billing. Without it every account stays on the free plan |
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama endpoint for generating search embeddings |
| `ISOCHRONE_CONCURRENCY` | `8` | Parallel GraphHopper isochrone requests. Raise it only when GraphHopper isn't also serving interactive routing |
| `BARRELMAN_STATEMENT_TIMEOUT_MS` | `10000` | Statement timeout on the API query pool. Schema DDL and the enrichment backfill are exempt. `0` disables |
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
