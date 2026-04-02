#!/bin/bash
set -euo pipefail

# Full OSM import pipeline — orchestrates both containers.
# Run this from the host after starting the stack with docker compose up -d.
#
# Usage:
#   ./scripts/run-import.sh
#
# Environment variables (or set in .env):
#   GEOFABRIK_URL         - PBF download URL
#                           Default: NC extract (north-carolina-latest.osm.pbf)
#   IMPORT_PBF            - Path to a local PBF file inside the container (/data/...)
#                           Skips download if set and file exists.
#   BARRELMAN_DB_PASSWORD - DB password (default: barrelman)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

# Load .env if present
if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

echo "==> [1/3] Running osm2pgsql import + post-import SQL (barrelman-db container)..."
docker exec \
  -e DATABASE_URL="$DB_URL" \
  -e GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}" \
  ${IMPORT_PBF:+-e IMPORT_PBF="$IMPORT_PBF"} \
  barrelman-db bash /app/scripts/import-osm.sh

echo "==> [2/4] Generating abbreviation index (barrelman container)..."
docker exec \
  -e DATABASE_URL="postgresql://barrelman:${DB_PASS}@barrelman-db:5432/barrelman" \
  barrelman bun run import/generate-abbreviations.ts

echo "==> [3/4] Populating codes from OSM tags (barrelman container)..."
docker exec \
  -e DATABASE_URL="postgresql://barrelman:${DB_PASS}@barrelman-db:5432/barrelman" \
  barrelman bun run import/generate-codes.ts

echo "==> [4/4] Rebuilding tsvector with abbreviations + categories (barrelman-db container)..."
docker exec barrelman-db psql "$DB_URL" -c "
UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    coalesce(name, '') || ' ' || coalesce(name_abbrev, '') || ' ' ||
    coalesce(array_to_string(
        ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
    ' '), '')
))
WHERE name IS NOT NULL;"

echo "==> Import complete!"
echo "    Optional: generate semantic embeddings with:"
echo "    docker exec barrelman bun run import/embed-places.ts"
