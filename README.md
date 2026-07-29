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
| `barrelman` | `alexwohlbruck/barrelman` | 3001 | REST API (Elysia/Bun) |
| `barrelman-db` | `alexwohlbruck/barrelman-db` | 5433 | PostgreSQL + PostGIS + pgvector |
| `martin` | `ghcr.io/maplibre/martin` | 3002 | Vector tile server |
| `graphhopper` | `israelhikingmap/graphhopper` | 8990 | Routing engine |

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
| `BARRELMAN_API_KEY` | `brm_dev_changeme` | Shared Bearer token for API auth. **Change before deploying.** |
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama endpoint for generating search embeddings |
| `ISOCHRONE_CONCURRENCY` | `8` | Parallel GraphHopper isochrone requests. Raise it only when GraphHopper isn't also serving interactive routing |

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
