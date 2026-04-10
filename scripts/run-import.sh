#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Full Import (Host Wrapper)
# =============================================================================
#
# Downloads OSM data and runs the complete import pipeline. Everything runs
# inside the barrelman-db container (osm2pgsql + all SQL post-processing).
#
# Usage:
#   ./scripts/run-import.sh
#
# Environment variables (or set in .env):
#   GEOFABRIK_URL         - PBF download URL (default: NC extract)
#   IMPORT_PBF            - Path to a local PBF inside the container (/data/...)
#   BARRELMAN_DB_PASSWORD - DB password (default: barrelman)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

# Load .env if present
if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

echo "Starting full OSM import pipeline..."
echo ""

docker exec \
  -e DATABASE_URL="$DB_URL" \
  -e GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}" \
  ${IMPORT_PBF:+-e IMPORT_PBF="$IMPORT_PBF"} \
  barrelman-db bash /app/scripts/import-osm.sh

echo ""
echo "Full import pipeline complete!"
