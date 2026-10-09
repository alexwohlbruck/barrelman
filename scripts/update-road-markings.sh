#!/bin/bash
set -euo pipefail
# =============================================================================
# Rebuild road markings where the map changed
# =============================================================================
#
# Works off road_markings_dirty, which replicate-extract.sh fills with the old
# and new outlines of every road a diff touched (import/queue-road-markings.sql).
# The queue is cut into grid cells, and each cell that already has road
# markings, or borders one, is rebuilt in place with generate-road-markings.sql
# scoped to it. Cells nobody built are dropped from the queue, so an install
# that never built road markings does no work here.
#
# Called by update-osm.sh after each replication run, and runnable on its own.
# A run stops after ROAD_MARKINGS_MAX_CELLS cells; the rest stays queued for
# the next one. A database updated through osm2pgsql's middle tables does not
# fill the queue, so there this is a no-op.
#
# Environment:
#   BARRELMAN_DB_PASSWORD     DB password (default: barrelman)
#   ROAD_MARKINGS_CELL        cell size in degrees (default 0.02, about 2 km)
#   ROAD_MARKINGS_MAX_CELLS   cells per run (default 500)
# =============================================================================

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman@localhost:5432/barrelman"
CELL="${ROAD_MARKINGS_CELL:-0.02}"
MAX_CELLS="${ROAD_MARKINGS_MAX_CELLS:-500}"

psql_db() { docker exec -i -e PGPASSWORD="$DB_PASS" barrelman-db psql "$DB_URL" -v ON_ERROR_STOP=1 -qAt "$@"; }
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

ready=$(psql_db -c "SELECT to_regclass('road_markings_dirty') IS NOT NULL AND to_regclass('road_surfaces') IS NOT NULL")
if [ "$ready" != "t" ]; then
  log "Road markings: nothing queued."
  exit 0
fi

started=$(psql_db -c "SELECT now()")
cells=$(psql_db -F " " -v cell="$CELL" -v max_cells="$MAX_CELLS" -f /app/import/road-markings-dirty-cells.sql)
total=$(echo -n "$cells" | grep -c . || true)
log "Road markings: $total cell(s) to rebuild."

done_cells=()
i=0
while read -r cx cy; do
  [ -z "$cx" ] && continue
  i=$((i + 1))
  box="ST_MakeEnvelope($cx * $CELL, $cy * $CELL, ($cx + 1) * $CELL, ($cy + 1) * $CELL, 4326)"
  psql_db <<SQL > /dev/null
SET client_min_messages = warning;
CREATE TEMP TABLE _rm_scope AS SELECT $box::geometry(Polygon, 4326) AS box;
\i /app/import/generate-road-markings.sql
SQL
  done_cells+=("($cx, $cy)")
  log "  [$i/$total] cell $cx,$cy"
done <<< "$cells"

# Clear what this run covered, and what lies where no road markings were built.
values="(NULL::int, NULL::int)"
[ ${#done_cells[@]} -gt 0 ] && values=$(IFS=,; echo "${done_cells[*]}")
cleared=$(psql_db <<SQL
WITH done(cx, cy) AS (VALUES $values),
covered AS (
  SELECT ST_Union(ST_MakeEnvelope(cx * $CELL, cy * $CELL, (cx + 1) * $CELL, (cy + 1) * $CELL, 4326)) AS g FROM done WHERE cx IS NOT NULL
),
gone AS (
  DELETE FROM road_markings_dirty d
  WHERE d.queued_at <= '$started'
    AND (ST_CoveredBy(d.box, COALESCE((SELECT g FROM covered), 'POLYGON EMPTY'::geometry))
         OR NOT EXISTS (SELECT 1 FROM road_surfaces s WHERE s.geom && ST_Expand(d.box, $CELL)))
  RETURNING 1
)
SELECT count(*) FROM gone;
SQL
)
left=$(psql_db -c "SELECT count(*) FROM road_markings_dirty")
log "Road markings: rebuilt $i cell(s); cleared $cleared queue entries, $left left."
