#!/bin/bash
set -euo pipefail
# =============================================================================
# Rebuild road markings where the map changed
# =============================================================================
#
# Works off its rows of detail_dirty, which replicate-extract.sh fills with the old
# and new outlines of the roads, crossings and signals each diff touched
# (import/queue-road-markings.sql), when ROAD_MARKINGS_INCREMENTAL=1. The queue
# is cut into grid cells, and each cell that already has road markings, or
# borders one, is rebuilt in place with generate-road-markings.sql scoped to it
# (import/road-markings-dirty-cells.sql plans the run). Entries in areas nobody
# built are dropped, and a database that never built road markings has its
# queue emptied, so neither does any work here.
#
# Called by update-osm.sh after each replication run when
# ROAD_MARKINGS_INCREMENTAL=1, and runnable on its own. A run rebuilds at most
# ROAD_MARKINGS_MAX_CELLS cells; the rest stays queued for the next one. A
# database updated through osm2pgsql's middle tables does not fill the queue,
# so there this is a no-op.
#
# A cell that fails to build is logged and skipped, and the entries over it go
# to the back of the queue; after MAX_ATTEMPTS failed runs they are dropped.
# A full "Build Road Markings" holds the build lock for as long as it runs;
# this takes it without waiting for each cell, and stops when it cannot,
# leaving the rest of the queue as it is, rather than waiting days behind it.
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
MAX_ATTEMPTS=3
# Taken by every generate-road-markings.sql build; see "One build at a time".
LOCK="5393739, 0"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# Both end up inside SQL text.
if ! [[ "$CELL" =~ ^[0-9]*\.?[0-9]+$ ]] || ! [[ "$MAX_CELLS" =~ ^[0-9]+$ ]] || [ "$MAX_CELLS" -lt 1 ]; then
  log "ERROR: ROAD_MARKINGS_CELL ($CELL) must be a positive number of degrees, ROAD_MARKINGS_MAX_CELLS ($MAX_CELLS) a positive whole number." >&2
  exit 1
fi

# SQL on stdin. docker exec -i reads stdin whether psql wants it or not, so
# every call is given its own.
psql_db() { docker exec -i -e PGPASSWORD="$DB_PASS" barrelman-db psql "$DB_URL" -v ON_ERROR_STOP=1 -qAt "$@"; }
q() { psql_db -c "$1" < /dev/null; }

lock_free() {
  [ "$(q "SELECT CASE WHEN pg_try_advisory_lock($LOCK) THEN pg_advisory_unlock($LOCK) ELSE false END")" = "t" ]
}

if [ "$(q "SELECT to_regclass('detail_dirty') IS NOT NULL OR to_regclass('road_markings_dirty') IS NOT NULL")" != "t" ]; then
  log "Road markings: nothing queued."
  exit 0
fi
# Moves a queue from before detail_dirty over.
psql_db -c "SET client_min_messages = warning" -f /app/import/detail-queue-table.sql < /dev/null > /dev/null

if [ "$(q "SELECT to_regclass('road_surfaces') IS NOT NULL")" != "t" ]; then
  dropped=$(q "WITH gone AS (DELETE FROM detail_dirty WHERE layer = 'road_markings' RETURNING 1) SELECT count(*) FROM gone")
  log "Road markings were never built here; emptied the queue ($dropped entries)."
  exit 0
fi

if ! lock_free; then
  log "Road markings: a build is running; leaving the queue for the next run."
  exit 0
fi

plan=$(psql_db -F " " -v cell="$CELL" -v max_cells="$MAX_CELLS" -v max_attempts="$MAX_ATTEMPTS" \
  -c "SET client_min_messages = warning" -f /app/import/road-markings-dirty-cells.sql < /dev/null)
entries=""
cells=()
while read -r kind a b; do
  case "$kind" in
    dropped)
      if [ "${a:-0}" != "0" ]; then log "Road markings: dropped $a entries where none are built."; fi
      if [ "${b:-0}" != "0" ]; then
        log "WARNING: road markings: gave up on $b entries after $MAX_ATTEMPTS failed runs." >&2
      fi
      ;;
    entries) entries="$a" ;;
    cell) cells+=("$a $b") ;;
  esac
done <<< "$plan"

total=${#cells[@]}
log "Road markings: $total cell(s) to rebuild."

# Cells not rebuilt this run, as SQL rows (cx, cy, failed).
pending=()
built=0
stopped=0
for cell in "${cells[@]}"; do
  read -r cx cy <<< "$cell"
  if [ "$stopped" = 1 ]; then
    pending+=("($cx, $cy, false)")
    continue
  fi
  # The build lock is taken without waiting, before the build would wait for
  # it: a full build that started since the last cell runs for days.
  if out=$(psql_db 2>&1 <<SQL
SET client_min_messages = warning;
SELECT pg_try_advisory_lock($LOCK) AS got_lock \gset
\if :got_lock
CREATE TEMP TABLE _rm_scope AS
SELECT ST_MakeEnvelope($cx * $CELL, $cy * $CELL, ($cx + 1) * $CELL, ($cy + 1) * $CELL, 4326)::geometry(Polygon, 4326) AS box;
\i /app/import/generate-road-markings.sql
\else
\echo ROAD_MARKINGS_LOCK_BUSY
\endif
SQL
  ); then
    if [[ "$out" == *ROAD_MARKINGS_LOCK_BUSY* ]]; then
      log "Road markings: a build started; stopping here, the rest stays queued."
      stopped=1
      pending+=("($cx, $cy, false)")
      continue
    fi
    built=$((built + 1))
    log "  [$((built + ${#pending[@]}))/$total] cell $cx,$cy"
  else
    pending+=("($cx, $cy, true)")
    log "  [$((built + ${#pending[@]}))/$total] cell $cx,$cy FAILED, skipped:" >&2
    printf '%s\n' "$out" | tail -n 5 | sed 's/^/      /' >&2
  fi
done

# Clear the picked entries all of whose cells were rebuilt. One over a failed
# cell counts an attempt; one over a cell skipped for the lock waits as it is.
failed=0
if [ -n "$entries" ]; then
  values="(NULL::int, NULL::int, NULL::boolean)"
  if [ ${#pending[@]} -gt 0 ]; then values=$(IFS=,; echo "${pending[*]}"); fi
  result=$(psql_db -F " " <<SQL
CREATE TEMP TABLE _picked AS SELECT unnest('{$entries}'::bigint[]) AS id;
WITH pending(cx, cy, failed) AS (VALUES $values),
hit AS (
  SELECT d.id, bool_or(p.failed) AS failed
  FROM detail_dirty d
  JOIN _picked USING (id)
  JOIN pending p
    ON p.cx BETWEEN floor(ST_XMin(d.box) / $CELL) AND floor(ST_XMax(d.box) / $CELL)
   AND p.cy BETWEEN floor(ST_YMin(d.box) / $CELL) AND floor(ST_YMax(d.box) / $CELL)
  GROUP BY d.id
),
retried AS (
  UPDATE detail_dirty d SET attempts = d.attempts + 1
  FROM hit WHERE hit.id = d.id AND hit.failed
  RETURNING 1
),
cleared AS (
  DELETE FROM detail_dirty d
  USING _picked p
  WHERE d.id = p.id AND NOT EXISTS (SELECT 1 FROM hit WHERE hit.id = d.id)
  RETURNING 1
)
SELECT (SELECT count(*) FROM cleared), (SELECT count(*) FROM retried);
SQL
  )
  read -r cleared retried <<< "$result"
  failed=$(printf '%s\n' "${pending[@]}" | grep -c 'true' || true)
else
  cleared=0
  retried=0
fi
left=$(q "SELECT count(*) FROM detail_dirty WHERE layer = 'road_markings'")
log "Road markings: rebuilt $built cell(s); cleared $cleared queue entries, $left left."
if [ "$failed" -gt 0 ]; then
  log "WARNING: road markings: $failed cell(s) failed; $retried entries over them go to the back of the queue." >&2
  exit 1
fi
