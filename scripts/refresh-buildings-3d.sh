#!/bin/bash
set -euo pipefail

# =============================================================================
# Refresh the buildings_3d materialized view
# =============================================================================
#
# buildings_3d stores its rows instead of computing them per tile request, so
# an edit to a building reaches the 3D tiles only after a refresh. update-osm.sh
# runs this after each update unless REFRESH_BUILDINGS_3D=0. On a large import
# the refresh takes hours, so turn it off there and schedule this script
# weekly instead.
#
# A populated view is refreshed CONCURRENTLY. Martin keeps reading the old rows
# until the new ones are ready, so 3D buildings stay on the map throughout. It
# needs the unique index on fid and room for a second copy of the view while it
# runs. A view that has never been populated, or has no unique index, cannot
# be refreshed that way, so it gets a plain refresh, during which the view
# cannot be read.
#
# Runs from barrelman-ops, like update-osm.sh.
# =============================================================================

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman@localhost:5432/barrelman"
psql_db() { docker exec -e PGPASSWORD="$DB_PASS" barrelman-db psql "$DB_URL" -v ON_ERROR_STOP=1 "$@"; }

POPULATED=$(psql_db -tAc "SELECT relispopulated FROM pg_class WHERE relname = 'buildings_3d' AND relkind = 'm';")

if [ -z "$POPULATED" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] buildings_3d does not exist yet — skipping (create-detail-views.sql makes it)."
  exit 0
fi

# CONCURRENTLY needs a unique index. create-detail-views.sql makes one
# (buildings_3d_fid_idx), but a view created before that index existed has none.
HAS_UNIQUE=$(psql_db -tAc "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
  WHERE c.relname = 'buildings_3d' AND i.indisunique AND i.indisvalid);")

if [ "$POPULATED" = "t" ] && [ "$HAS_UNIQUE" = "t" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Refreshing buildings_3d (concurrently, tiles keep serving)..."
  psql_db -c "REFRESH MATERIALIZED VIEW CONCURRENTLY buildings_3d;"
elif [ "$POPULATED" = "t" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Refreshing buildings_3d (no unique index, so tiles pause until it finishes)..."
  psql_db -c "REFRESH MATERIALIZED VIEW buildings_3d;"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Populating buildings_3d for the first time..."
  psql_db -c "REFRESH MATERIALIZED VIEW buildings_3d;"
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] buildings_3d refreshed."
