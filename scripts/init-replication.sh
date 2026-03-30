#!/bin/bash
set -euo pipefail

# Initialize osm2pgsql-replication state after the first full import.
# Run this once before enabling the daily update cron job.
#
# This records the current OSM replication sequence in the database so that
# update-osm.sh knows which diffs to apply going forward.
#
# Usage:
#   ./scripts/init-replication.sh
#
# Environment variables (or set in .env):
#   GEOFABRIK_REPLICATION_URL - Geofabrik update server for your region
#                               Default: North Carolina updates
#                               Find yours at: https://download.geofabrik.de
#                               e.g. https://download.geofabrik.de/europe/germany-updates/
#   BARRELMAN_DB_PASSWORD     - DB password (default: barrelman)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"
REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-https://download.geofabrik.de/north-america/us/north-carolina-updates/}"

echo "Initializing replication state..."
echo "  DB:     $DB_URL"
echo "  Server: $REPLICATION_URL"

docker exec barrelman-db \
  osm2pgsql-replication init \
    -d "$DB_URL" \
    --server "$REPLICATION_URL"

echo "Replication initialized. You can now enable the daily update cron job."
echo "  Run: crontab -e"
echo "  Add: 0 3 * * * /opt/barrelman/scripts/update-osm.sh >> /var/log/barrelman-update.log 2>&1"
