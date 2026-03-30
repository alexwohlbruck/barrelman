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
        │             ←── import/embed-places.ts (Ollama embeddings)
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

| Service | Description | Port |
|---------|-------------|------|
| `barrelman` | Main API (Elysia/Bun) | 3001 |
| `barrelman-db` | PostgreSQL + PostGIS | 5434 |
| `ollama` | Embedding model server | 11434 |
| `martin` | Vector tile server | 3002 |
| `graphhopper` | Routing engine | 8989 |

## Quick Start (Local Dev)

### Prerequisites

- [Bun](https://bun.sh) ≥ 1.1
- [Docker](https://docker.com) + Docker Compose
- [osm2pgsql](https://osm2pgsql.org) ≥ 1.10 (`brew install osm2pgsql` on macOS)
- [DuckDB CLI](https://duckdb.org) (`brew install duckdb`) — used for parquet import steps

### 1. Clone and configure

```bash
git clone https://github.com/alexwohlbruck/barrelman.git
cd barrelman
cp .env.example .env
```

Edit `.env` as needed (defaults work for local development):

```dotenv
DATABASE_URL=postgresql://barrelman:barrelman@localhost:5434/barrelman
BARRELMAN_API_KEY=brm_dev_changeme   # Change before exposing publicly
OLLAMA_HOST=http://localhost:11434
IMPORT_BBOX=-84.4,33.7,-75.4,36.6   # NC bounding box — change for other regions
```

### 2. Start infrastructure

```bash
docker compose up -d barrelman-db ollama
```

Wait ~15 seconds for PostGIS to initialise.

### 3. Install dependencies

```bash
bun install
```

### 4. Import data

See [Data Import](#data-import) for full details. For a quick North Carolina import:

```bash
# Download NC OSM extract and import into PostGIS (~10-15 min)
bun run import:osm

# Generate semantic search embeddings (~30-90 min on CPU, faster on GPU)
bun run import:embed
```

### 5. Run the server

```bash
bun run dev
```

Server: `http://localhost:3001`
Swagger UI: `http://localhost:3001/swagger`

---

## Data Import

The import pipeline transforms an OSM PBF extract into a fully indexed PostGIS database. There are two approaches depending on coverage needed.

### Regional import (recommended for development)

Best for a single country, state, or metro area. Fast and low disk/RAM requirement.

**1. Download a PBF extract from Geofabrik:**

```bash
# North Carolina (~200 MB)
wget -O data/region.osm.pbf https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf

# Other examples:
# Germany:       https://download.geofabrik.de/europe/germany-latest.osm.pbf
# France:        https://download.geofabrik.de/europe/france-latest.osm.pbf
# United States: https://download.geofabrik.de/north-america/us-latest.osm.pbf
```

**2. Set the matching bounding box in `.env`:**

```dotenv
# Format: west,south,east,north (decimal degrees)
IMPORT_BBOX=-84.4,33.7,-75.4,36.6    # North Carolina
# IMPORT_BBOX=-180,-90,180,90        # Global (no bbox filtering)
```

**3. Run the full import pipeline:**

```bash
bun run import:osm
```

This runs:
1. `osm2pgsql` with the flex Lua style → writes raw `geo_places` rows
2. `post-import.sql` → extracts structured address fields, builds GiST/GIN indexes, computes centroids
3. `generate-abbreviations.ts` → pre-computes name abbreviations for faster autocomplete
4. tsvector update → rebuilds full-text search vectors with abbreviations included

Then generate embeddings for semantic search:

```bash
# Pull the embedding model first (one-time, ~270 MB)
docker exec barrelman-ollama ollama pull nomic-embed-text

bun run import:embed
```

Embedding generation processes ~500 places/min on CPU. For 200k POIs (typical US state): ~7 hours. Skip this step if you don't need semantic search — all other search layers still work.

### Global import

A full planet import requires more resources but covers all of OSM.

**Requirements:**
- ~100 GB disk for the PBF + import scratch space
- ~16 GB RAM recommended for osm2pgsql slim mode
- SSD strongly recommended for PostGIS

**Steps:**

```bash
# Download planet (updated weekly, ~80 GB)
wget -O data/region.osm.pbf https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf

# Or use a regional mirror for faster download:
# https://download.geofabrik.de/  (regional extracts, e.g. full Europe/Asia/etc.)

# Set global bbox in .env (no filtering)
# IMPORT_BBOX=-180,-90,180,90

# Import (several hours for planet)
bun run import:osm

# Embeddings for full planet: run in batches or skip
bun run import:embed
```

> **Tip:** For production global deployments, prefer continent-level Geofabrik extracts merged with `osmium merge` rather than the full planet file. This allows parallel imports and easier regional updates.

### Updating data

OSM data is static after import. To refresh:

```bash
# Download fresh PBF, then re-run the pipeline
rm data/region.osm.pbf
bun run import:osm
bun run import:embed
```

For automated nightly/weekly updates, set up a cron job or use `osmium extract` + `osmupdate` for incremental diffs.

---

## API Reference

All endpoints require a `Bearer` token in the `Authorization` header:

```
Authorization: Bearer brm_dev_changeme
```

Interactive docs available at `http://localhost:3001/swagger` when the server is running.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/nearby` | Find places within a radius |
| `POST` | `/search` | Hybrid text + semantic search |
| `GET` | `/contains` | Find parent areas containing a point |
| `GET` | `/children` | Find POIs inside an area |
| `GET` | `/place/:osmType/:osmId` | Get a single place by OSM ID |
| `GET` | `/health` | Health check |

### POST `/nearby`

Find places within a radius, sorted by distance.

```json
{
  "lat": 35.2271,
  "lng": -80.8431,
  "radius": 1000,
  "categories": ["bicycle_parking"],
  "limit": 20,
  "offset": 0
}
```

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

Set `autocomplete: true` for typeahead (skips slow semantic layer). Set `semantic: true` to force vector search for concept queries like _"somewhere quiet to study"_.

### GET `/contains?lat=&lng=`

Returns named areas (smallest first) containing the given point — useful for reverse geocoding a coordinate to its containing city, county, state, etc.

### GET `/children?id=&categories=`

Returns places whose centroids fall inside the given area's polygon. Useful for listing shops in a mall or POIs on a university campus.

### GET `/place/:osmType/:osmId`

Fetch full details for a single OSM element. `osmType` is `node`, `way`, or `relation`.

```
GET /place/node/5718230659
GET /place/way/123456
GET /place/relation/9876
```

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env`.

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://barrelman:barrelman@localhost:5434/barrelman` | PostGIS connection string |
| `BARRELMAN_DB_PASSWORD` | `barrelman` | Used by `docker-compose.yml` for the DB container |
| `PORT` | `3001` | HTTP port the API listens on |
| `BARRELMAN_API_KEY` | `brm_dev_changeme` | Shared Bearer token for API auth. **Change before deploying.** |
| `OLLAMA_HOST` | `http://localhost:11434` | Ollama endpoint for generating search embeddings |
| `IMPORT_BBOX` | `-84.4,33.7,-75.4,36.6` | Bounding box filter for import (`west,south,east,north`). Set to `-180,-90,180,90` for global. |
| `GEOFABRIK_URL` | NC extract URL | PBF download URL used by `import:osm` when no local file exists |

---

## Production Deployment

### Docker Compose (recommended)

The included `docker-compose.yml` runs the full stack. For production, you'll want to:

1. **Change secrets:** Set a strong `BARRELMAN_API_KEY` and `BARRELMAN_DB_PASSWORD`.
2. **Mount a data volume** for the PostGIS database so data persists across container restarts.
3. **Increase shared memory** for PostGIS: add `shm_size: '256mb'` to the db service.
4. **Attach to your reverse proxy network** (e.g. Traefik, nginx-proxy) to expose the API via HTTPS.

```bash
# First-time setup
cp .env.example .env
# Edit .env with production values

docker compose up -d
```

The Parchment API container connects to Barrelman over the `parchment-network` Docker network. See the [Parchment docs](https://docs.parchment.app/docs/development/barrelman) for how to wire the two services together.

### Resource recommendations

| Scale | DB size | RAM | Disk |
|-------|---------|-----|------|
| Single US state (e.g. NC) | ~1 GB | 2 GB | 10 GB |
| Full United States | ~15 GB | 8 GB | 80 GB |
| Europe | ~25 GB | 16 GB | 150 GB |
| Planet | ~80 GB | 32 GB | 500 GB |

Embedding vectors add ~3 GB per 1M places (768-dim float32).

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
