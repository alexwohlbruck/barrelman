#!/bin/bash
set -euo pipefail

# =============================================================================
# Refresh one of the map's materialized views: buildings_3d or street_furniture
# =============================================================================
#
# Usage: refresh-view.sh <view>
#
# These views store their rows instead of computing them per tile request, so
# an OSM edit reaches their tiles only after a refresh. update-osm.sh runs this
# after each update; import-osm.sh refreshes them directly after an import.
#
# A populated view is refreshed CONCURRENTLY. Martin keeps reading the old rows
# until the new ones are ready, so the layer stays on the map throughout. That
# needs a unique index and room for a second copy of the view while it runs. A
# view that has never been populated, or has no unique index, gets a plain
# refresh, during which it cannot be read.
#
# Runs from barrelman-ops, like update-osm.sh.
# =============================================================================

VIEW="${1:?usage: refresh-view.sh <view>}"
case "$VIEW" in
  buildings_3d|street_furniture) ;;
  *) echo "Unknown view: $VIEW" >&2; exit 2 ;;
esac

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman@localhost:5432/barrelman"
psql_db() { docker exec -e PGPASSWORD="$DB_PASS" barrelman-db psql "$DB_URL" -v ON_ERROR_STOP=1 "$@"; }
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

POPULATED=$(psql_db -tAc "SELECT relispopulated FROM pg_class WHERE relname = '$VIEW' AND relkind = 'm';")

if [ -z "$POPULATED" ]; then
  log "$VIEW does not exist yet — skipping (create-detail-views.sql makes it)."
  exit 0
fi

# Without its highway index each nearest-way lookup walks the whole geometry
# index (~12 ms an object on the US import), which turns a daily refresh into hours.
if [ "$VIEW" = "street_furniture" ] && [ "$POPULATED" = "t" ] && [ "$(psql_db -tAc \
  "SELECT to_regclass('geo_places_highway_lines_geom_idx') IS NOT NULL;")" != "t" ]; then
  log "street_furniture: geo_places_highway_lines_geom_idx is missing — skipping. Run the Map Detail Indexes task first."
  exit 0
fi

HAS_UNIQUE=$(psql_db -tAc "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
  WHERE c.relname = '$VIEW' AND i.indisunique AND i.indisvalid);")

if [ "$POPULATED" = "t" ] && [ "$HAS_UNIQUE" = "t" ]; then
  log "Refreshing $VIEW (concurrently, tiles keep serving)..."
  psql_db -c "REFRESH MATERIALIZED VIEW CONCURRENTLY $VIEW;"
elif [ "$POPULATED" = "t" ]; then
  log "Refreshing $VIEW (no unique index, so tiles pause until it finishes)..."
  psql_db -c "REFRESH MATERIALIZED VIEW $VIEW;"
else
  log "Populating $VIEW for the first time..."
  psql_db -c "REFRESH MATERIALIZED VIEW $VIEW;"
fi
log "$VIEW refreshed."
