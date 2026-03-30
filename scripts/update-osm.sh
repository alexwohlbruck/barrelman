#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Daily OSM Update
# =============================================================================
#
# Applies OSM changes to the Barrelman database. Intended to run as a daily
# cron job on the host machine.
#
# MODES:
#   replication (default)
#     Applies incremental diffs from Geofabrik's replication server. Fast —
#     only processes changes since the last run. Requires init-replication.sh
#     to have been run once after the initial import.
#
#   full
#     Downloads the latest full extract and re-imports from scratch. Slower
#     (~5 min for a single state) but always consistent. No initialization
#     needed; safe to use without running init-replication.sh first.
#
# CONFIGURATION (set in .env or export before running):
#
#   UPDATE_MODE                 replication | full  (default: replication)
#   GEOFABRIK_URL               Full extract download URL
#                               Default: NC latest OSM extract
#   GEOFABRIK_REPLICATION_URL   Diff update server URL (replication mode only)
#                               Default: NC updates server
#                               Find your region at https://download.geofabrik.de
#   BARRELMAN_DB_PASSWORD       DB password (default: barrelman)
#
# SCHEDULING (crontab):
#   Run daily at 3am:
#     0 3 * * * /opt/barrelman/scripts/update-osm.sh >> /var/log/barrelman-update.log 2>&1
#
#   View logs:
#     tail -f /var/log/barrelman-update.log
#
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL_LOCAL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"
DB_URL_DOCKER="postgresql://barrelman:${DB_PASS}@barrelman-db:5432/barrelman"

UPDATE_MODE="${UPDATE_MODE:-replication}"
GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}"
GEOFABRIK_REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-https://download.geofabrik.de/north-america/us/north-carolina-updates/}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting OSM update (mode: $UPDATE_MODE)"

if [ "$UPDATE_MODE" = "full" ]; then
  # ── Full re-import ──────────────────────────────────────────────────────────
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Downloading latest extract..."
  docker exec barrelman-db \
    wget -q --show-progress -O /data/region.osm.pbf "$GEOFABRIK_URL"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running full import..."
  docker exec \
    -e DATABASE_URL="$DB_URL_LOCAL" \
    barrelman-db bash /app/scripts/import-osm.sh

else
  # ── Incremental replication ─────────────────────────────────────────────────
  # If replication state is missing, initialize it first automatically.
  INIT_CHECK=$(docker exec barrelman-db \
    psql "$DB_URL_LOCAL" -tAc \
    "SELECT count(*) FROM osm2pgsql_properties WHERE property='replication_base_url';" 2>/dev/null || echo "0")

  if [ "$INIT_CHECK" = "0" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Replication not initialized — running init first..."
    docker exec barrelman-db \
      osm2pgsql-replication init \
        -d "$DB_URL_LOCAL" \
        --server "$GEOFABRIK_REPLICATION_URL"
  fi

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Applying OSM diffs..."
  docker exec barrelman-db \
    osm2pgsql-replication update \
      -d "$DB_URL_LOCAL" \
      -- \
      --style /app/import/osm2pgsql-flex.lua \
      --slim
fi

# ── Post-update steps (both modes) ───────────────────────────────────────────

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Rebuilding abbreviation index..."
docker exec \
  -e DATABASE_URL="$DB_URL_DOCKER" \
  barrelman bun run import/generate-abbreviations.ts

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Rebuilding tsvectors..."
docker exec barrelman-db psql "$DB_URL_LOCAL" -c "
UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    coalesce(name, '') || ' ' || coalesce(name_abbrev, '')
))
WHERE name IS NOT NULL AND name_abbrev IS NOT NULL AND name_abbrev != '';"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] OSM update complete."
