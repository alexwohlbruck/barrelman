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
#   GEOFABRIK_REPLICATION_URL - Geofabrik update server. Defaults to the feed of
#                               whatever REGIONS resolves to, so it normally
#                               needs no setting at all. Override to point at a
#                               different server:
#                               e.g. https://download.geofabrik.de/europe/germany-updates/
#   BARRELMAN_DB_PASSWORD     - DB password (default: barrelman)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

# Only present when run from a host checkout — barrelman-ops has no /app/.env.
if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

# Follows REGIONS by default; the old hard-coded North Carolina default meant
# replication was initialized against the wrong feed for every other region.
REGION_REPLICATION="$(cd "$PROJECT_DIR" && bun run src/config/regions.ts osm-replication 2>/dev/null | head -1 || true)"
REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-${REGION_REPLICATION:-https://download.geofabrik.de/north-america/us/north-carolina-updates/}}"

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
