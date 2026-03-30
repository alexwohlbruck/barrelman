#!/bin/bash
set -euo pipefail

# Import OSM PBF data into Barrelman PostGIS database
# Usage: ./scripts/import-osm.sh [path-to-pbf]
#
# Environment variables:
#   DATABASE_URL    - PostgreSQL connection string (required)
#   GEOFABRIK_URL   - URL to download PBF from (default: NC extract)
#   IMPORT_PBF      - Path to local PBF file (overrides download)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_DIR/data"

# Default to NC extract
GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}"
PBF_FILE="${IMPORT_PBF:-${1:-$DATA_DIR/region.osm.pbf}}"

DATABASE_URL="${DATABASE_URL:?DATABASE_URL is required}"

mkdir -p "$DATA_DIR"

# Step 1: Download PBF if not present
if [ ! -f "$PBF_FILE" ]; then
    echo "Downloading PBF from $GEOFABRIK_URL..."
    wget -O "$PBF_FILE" "$GEOFABRIK_URL"
    echo "Download complete: $(du -h "$PBF_FILE" | cut -f1)"
else
    echo "Using existing PBF: $PBF_FILE ($(du -h "$PBF_FILE" | cut -f1))"
fi

# Step 2: Import with osm2pgsql flex output
echo "Running osm2pgsql import..."
osm2pgsql \
    --create \
    --slim \
    --flat-nodes="$DATA_DIR/nodes.cache" \
    --output=flex \
    --style="$PROJECT_DIR/import/osm2pgsql-flex.lua" \
    -d "$DATABASE_URL" \
    "$PBF_FILE"

echo "osm2pgsql import complete."

# Step 3: Post-import SQL (extract structured fields, build indexes)
echo "Running post-import SQL..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/post-import.sql"

# Step 4: Generate abbreviations
echo "Generating abbreviations..."
cd "$PROJECT_DIR"
bun run import/generate-abbreviations.ts

# Step 5: Rebuild tsvector with abbreviations
echo "Rebuilding tsvector with abbreviations..."
psql "$DATABASE_URL" -c "
UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    coalesce(name, '') || ' ' || coalesce(name_abbrev, '')
))
WHERE name IS NOT NULL AND name_abbrev IS NOT NULL AND name_abbrev != '';
"

echo "Import pipeline complete!"
echo "Next steps:"
echo "  1. Run 'bun run import/embed-places.ts' to generate embeddings"
echo "  2. Start the server with 'bun run dev'"
