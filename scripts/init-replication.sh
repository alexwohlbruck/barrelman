#!/bin/bash
set -euo pipefail

# Initialize osm2pgsql-replication state after the first full import.
# Run this once before enabling the OSM Update schedule.
#
# This records the current OSM replication sequence in the database so that
# update-osm.sh knows which diffs to apply going forward.
#
# Usage:
#   ./scripts/init-replication.sh
#
# Environment variables (or set in .env):
#   GEOFABRIK_REPLICATION_URL - Geofabrik update server for your region.
#                               Defaults to the first feed REGIONS resolves to.
#                               Find yours at: https://download.geofabrik.de
#                               e.g. https://download.geofabrik.de/europe/germany-updates/
#   BARRELMAN_DB_PASSWORD     - DB password (default: barrelman)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

# Match update-osm.sh: derive the feed from REGIONS so init and update can never
# seed and follow different servers.
OSM_REPLICATION="$(cd "$PROJECT_DIR" && bun run src/config/regions.ts osm-replication 2>/dev/null || true)"
REPLICATION_COUNT="$(printf '%s\n' "$OSM_REPLICATION" | grep -c . || true)"
REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-$(printf '%s\n' "$OSM_REPLICATION" | head -n1)}"
REPLICATION_URL="${REPLICATION_URL:-https://download.geofabrik.de/north-america/us/north-carolina-updates/}"

# The URL contains BARRELMAN_DB_PASSWORD, and you can run this script from the
# console, where the job runner saves its output to the database and shows it in
# the job log. Print the connection without the password.
echo "Initializing replication state..."
echo "  DB:     postgresql://barrelman:***@localhost:5432/barrelman"
echo "  Server: $REPLICATION_URL"

if [ "${REPLICATION_COUNT:-0}" -gt 1 ]; then
  echo ""
  echo "  WARNING: REGIONS resolves to $REPLICATION_COUNT replication feeds, but a database"
  echo "  can follow only one. The remaining regions will only move on UPDATE_MODE=full."
fi

docker exec barrelman-db \
  osm2pgsql-replication init \
    -d "$DB_URL" \
    --server "$REPLICATION_URL"

echo "Replication initialized. Enable the OSM Update schedule in the console"
echo "(Schedules → OSM Update) to apply diffs on a cadence."
