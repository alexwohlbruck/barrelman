#!/bin/bash
set -euo pipefail

# =============================================================================
# Replication without osm2pgsql's middle tables
# =============================================================================
#
# Applies replication diffs to a database that has no middle tables.
#
# osm2pgsql-replication needs the middle tables (planet_osm_ways and friends)
# to work out which rows a diff touches. They are large: about 13 times the
# size of the extract, which for the United States is around 155 GB, more than
# the rest of the database. A big import usually drops them to save disk. This
# script works from the extract on disk instead:
#
#   1. Download the diffs since the last run and apply them to region.osm.pbf.
#   2. List the objects the diffs touch: the changed objects, the ways that
#      contain a changed node, the way members a changed bicycle route has or
#      had, and every relation that contains one of those ways. A relation's
#      geometry comes from its ways, and bicycle-route membership is how the
#      flex style marks route streets, so both directions are needed.
#   3. Cut those objects, and everything they reference, out of the patched
#      extract. Import that small file with the normal flex style into a
#      throwaway staging database.
#   4. In one transaction: delete the live rows of the touched objects, copy
#      the staged rows in, re-derive the post-import columns for those rows
#      only (import/replay-derive.sql), and move the cursor.
#
# The patched extract replaces region.osm.pbf before the transaction commits.
# If a run dies between the two, the next run downloads the same diffs and
# applies them again. That is harmless: osmium keeps the newest version of
# each object, and the database side rebuilds its rows from the extract.
#
# The staging database is separate from the live one on purpose. osm2pgsql
# --create drops the tables it is about to create, so pointing it at a schema
# inside the live database would drop the live geo_places if the schema setting
# were ever lost.
#
# Runs inside barrelman-db. Called by update-osm.sh when the database has no
# middle tables. The cursor is the osm_replication_state table. The first run
# sets it from osm2pgsql's own replication state if the database has one, and
# otherwise from the replication header of region.osm.pbf, the extract the
# database was imported from.
#
# Environment:
#   DATABASE_URL                 live database, without the password
#   PGPASSWORD                   its password
#   OSM_PBF_FILE                 extract to patch (default /data/region.osm.pbf)
#   REPLICATION_WORK_DIR         scratch space (default /data/replication)
#   REPLICATION_MAX_DIFFS        diffs merged per cycle (default 7)
#   GEOFABRIK_REPLICATION_URL    overrides the feed recorded in the cursor
#   REPLICATION_ALLOW_SHRINK     1 to skip the shrink guard (see apply_cycle)
#   REPLICATION_MIN_MEMORY_GB    smallest container memory limit to run under (4)
#   ROAD_MARKINGS_INCREMENTAL    1 to queue the roads each cycle touches for
#                                update-road-markings.sh (default 0)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

DATABASE_URL="${DATABASE_URL:?DATABASE_URL is required}"
PBF_FILE="${OSM_PBF_FILE:-/data/region.osm.pbf}"
WORK_DIR="${REPLICATION_WORK_DIR:-/data/replication}"
MAX_DIFFS="${REPLICATION_MAX_DIFFS:-7}"
STAGE_DB="barrelman_replication_stage"
ADMIN_URL="${DATABASE_URL%/*}/postgres"
STAGE_URL="${DATABASE_URL%/*}/$STAGE_DB"

# Postgres NOTICEs ("does not exist, skipping") are noise in the job log.
export PGOPTIONS="${PGOPTIONS:-} -c client_min_messages=warning"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
fail() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" >&2; exit 1; }
q() { psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -tAc "$1"; }

# A replication header value from region.osm.pbf, or empty.
pbf_header() {
  osmium fileinfo "$PBF_FILE" 2>/dev/null | sed -n "s/^ *$1=//p" | head -n1
}

# Geofabrik's sequence path: 4921 -> 000/004/921
seq_path() {
  local p; p="$(printf '%09d' "$1")"
  echo "${p:0:3}/${p:3:3}/${p:6:3}"
}

# The timestamp a state file reports, with the colons unescaped.
state_timestamp() {
  sed -n 's/^timestamp=//p' "$1" | tr -d '\\'
}

# `osmium getid` exits 1 when some of the requested IDs are absent, and deleted
# objects always are. It also exits 1 when it cannot open a file, but then it
# says so on stderr, while missing IDs alone print nothing (osmium 1.15). So an
# exit of 1 with nothing on stderr is fine, and anything else is a failure.
getid_allow_missing() {
  local err rc=0; err="$(mktemp)"
  osmium getid "$@" 2>"$err" || rc=$?
  if [ "$rc" -eq 0 ] || { [ "$rc" -eq 1 ] && [ ! -s "$err" ]; }; then
    rm -f "$err"; return 0
  fi
  cat "$err" >&2; rm -f "$err"; return 1
}

# IDs (w123) of the way members of those relations listed in $2 that are
# bicycle routes in extract $1. Membership of a bicycle route is the one thing
# about a relation that changes its member ways' rows (the flex style marks
# them in stage 2), so other relations' members are left alone. Rewriting every
# member of every edited bus route or boundary would multiply the work.
# Each grep is allowed to match nothing: no members is a normal answer.
route_way_members() {
  [ -s "$2" ] || return 0
  getid_allow_missing -f opl -o - "$1" -i "$2" \
    | { grep -E '( T|,)route=bicycle(,| |$)' || true; } \
    | { grep -oE '(^| )M[^ ]*' || true; } \
    | sed 's/^ *M//' | tr ',' '\n' \
    | sed -n 's/^\(w[0-9]*\)@.*/\1/p'
}

# IDs of the ways and relations in extract $1 that contain an object in $2.
parents_of() {
  [ -s "$2" ] || return 0
  osmium getparents -f opl -o - "$1" -i "$2" | cut -d' ' -f1
}

# Columns two tables share, in the staging table's order, ready for a COPY
# list. Generated columns are left out: Postgres computes them itself.
common_columns() {
  local table="$1" sql
  sql="SELECT column_name FROM information_schema.columns
       WHERE table_schema = 'public' AND table_name = '$table' AND is_generated = 'NEVER'
       ORDER BY ordinal_position"
  local live; live="$(psql "$DATABASE_URL" -tAc "$sql")"
  local out=() col
  while IFS= read -r col; do
    [ -n "$col" ] || continue
    if grep -qxF "$col" <<<"$live"; then
      out+=("$col")
    else
      # A column the flex style now writes but this database predates. The
      # row still goes in; that one value is lost until a re-import.
      echo "  note: $table.$col is not in the live table — not copied" >&2
    fi
  done < <(psql "$STAGE_URL" -tAc "$sql")
  local IFS=,; echo "${out[*]}"
}

# ── Memory ──────────────────────────────────────────────────────────────────
# osmium's ID sets allocate by region of the ID space, so a diff's few thousand
# scattered node IDs can cost hundreds of MB, and a country's daily diff
# several GB across getparents and getid. That memory comes out of this
# container's limit, which Postgres shares. Running out here lets the kernel
# kill a Postgres backend and put the database into crash recovery, so a limit
# too small to be safe stops the run before osmium starts.
MIN_MEM_BYTES=$(( ${REPLICATION_MIN_MEMORY_GB:-4} * 1024 * 1024 * 1024 ))
MEM_LIMIT="$(cat /sys/fs/cgroup/memory.max 2>/dev/null \
  || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null || echo max)"
if [ "$MEM_LIMIT" != "max" ] && [ "$MEM_LIMIT" -lt "$MIN_MEM_BYTES" ]; then
  fail "barrelman-db is limited to $(( MEM_LIMIT / 1024 / 1024 )) MB of memory, and replication
  without middle tables needs at least ${REPLICATION_MIN_MEMORY_GB:-4} GB alongside Postgres.
  Raise BARRELMAN_DB_MEM_LIMIT in .env and recreate barrelman-db
  (docker compose up -d barrelman-db)."
fi

# ── Cursor ──────────────────────────────────────────────────────────────────

q "CREATE TABLE IF NOT EXISTS osm_replication_state (
     id             boolean PRIMARY KEY DEFAULT true CHECK (id),
     base_url       text NOT NULL,
     sequence       integer NOT NULL,
     data_timestamp timestamptz,
     updated_at     timestamptz NOT NULL DEFAULT now()
   );" >/dev/null

if [ "$(q "SELECT count(*) FROM osm_replication_state")" = "0" ]; then
  # Two places can say where the database is: the replication header Geofabrik
  # writes into the extract, and osm2pgsql-replication's own state, if the
  # database replicated with its middle tables before they were dropped. Each
  # can lag. apply-osm-diff.sh patches the extract without touching its header,
  # and a full re-import leaves an old replication row behind. Whichever is
  # further along is the one that matches the data, so take the larger.
  INIT_URL="" INIT_SEQ="" INIT_TS="" INIT_FROM=""
  if [ -f "$PBF_FILE" ]; then
    INIT_URL="$(pbf_header osmosis_replication_base_url)"
    INIT_SEQ="$(pbf_header osmosis_replication_sequence_number)"
    INIT_TS="$(pbf_header osmosis_replication_timestamp)"
    if [ -n "$INIT_SEQ" ]; then INIT_FROM="the replication header of $(basename "$PBF_FILE")"; fi
  fi
  STATUS_ROW="$(q "SELECT url || '|' || sequence || '|' || coalesce(importdate::text, '')
                   FROM planet_osm_replication_status LIMIT 1" 2>/dev/null || true)"
  if [ -n "$STATUS_ROW" ]; then
    IFS='|' read -r S_URL S_SEQ S_TS <<<"$STATUS_ROW"
    if [ -z "$INIT_SEQ" ] || [ "$S_SEQ" -gt "$INIT_SEQ" ]; then
      INIT_URL="$S_URL" INIT_SEQ="$S_SEQ" INIT_TS="$S_TS"
      INIT_FROM="osm2pgsql's replication state"
    fi
  fi
  INIT_URL="${GEOFABRIK_REPLICATION_URL:-$INIT_URL}"
  if [ -z "$INIT_URL" ] || [ -z "$INIT_SEQ" ]; then
    fail "the starting point is unknown: $(basename "$PBF_FILE") carries no replication header
  (osmosis_replication_*) and the database has no osm2pgsql replication state.
  Re-import from a Geofabrik extract, which has one."
  fi
  log "Starting replication from $INIT_FROM: sequence $INIT_SEQ ($INIT_TS)"
  q "INSERT INTO osm_replication_state (base_url, sequence, data_timestamp)
     VALUES ('${INIT_URL%/}', $INIT_SEQ, NULLIF('$INIT_TS', '')::timestamptz);" >/dev/null
fi

BASE_URL="$(q "SELECT base_url FROM osm_replication_state")"
BASE_URL="${GEOFABRIK_REPLICATION_URL:-$BASE_URL}"
BASE_URL="${BASE_URL%/}"

mkdir -p "$WORK_DIR"
wget -q --tries=5 --waitretry=10 -O "$WORK_DIR/state.txt" "$BASE_URL/state.txt" \
  || fail "could not read $BASE_URL/state.txt"
LATEST="$(sed -n 's/^sequenceNumber=//p' "$WORK_DIR/state.txt")"
[ -n "$LATEST" ] || fail "$BASE_URL/state.txt has no sequenceNumber"

CURRENT="$(q "SELECT sequence FROM osm_replication_state")"
log "Replication cursor at $CURRENT, feed at $LATEST ($BASE_URL)"
if [ "$CURRENT" -ge "$LATEST" ]; then
  log "Already up to date."
  echo "REPLICATION_APPLIED=0"
  exit 0
fi

[ -f "$PBF_FILE" ] || fail "$PBF_FILE is missing. The extract is what this kind of replication reads from."

# ── One cycle: diffs CURRENT+1 .. END ───────────────────────────────────────

apply_cycle() {
  local from="$1" end="$2"
  local dir="$WORK_DIR/cycle"
  rm -rf "$dir"; mkdir -p "$dir/diffs"

  log "[1/5] Downloading diffs $((from + 1))..$end"
  local s files=()
  for s in $(seq $((from + 1)) "$end"); do
    local f="$dir/diffs/$s.osc.gz"
    wget -q --tries=5 --waitretry=10 -O "$f" "$BASE_URL/$(seq_path "$s").osc.gz" \
      || fail "could not download diff $s"
    gzip -t "$f" || fail "diff $s is corrupt"
    files+=("$f")
  done
  wget -q --tries=5 --waitretry=10 -O "$dir/end.state.txt" "$BASE_URL/$(seq_path "$end").state.txt" \
    || fail "could not download the state file for $end"
  local end_ts; end_ts="$(state_timestamp "$dir/end.state.txt")"

  # -s keeps only the last version of each object across the merged diffs.
  osmium merge-changes -s -O -o "$dir/merged.osc.gz" "${files[@]}"

  log "[2/5] Patching $(basename "$PBF_FILE") and listing what the diffs touch"
  osmium cat -f opl "$dir/merged.osc.gz" | cut -d' ' -f1 | sort -u > "$dir/changed.ids"
  grep '^n' "$dir/changed.ids" > "$dir/changed-nodes.ids" || true
  grep '^w' "$dir/changed.ids" > "$dir/changed-ways.ids" || true
  grep '^r' "$dir/changed.ids" > "$dir/changed-rels.ids" || true
  echo "  changed: $(wc -l < "$dir/changed-nodes.ids") nodes, $(wc -l < "$dir/changed-ways.ids") ways, $(wc -l < "$dir/changed-rels.ids") relations"

  # The way members a changed bicycle route had before the diff. A way that
  # left the route must lose its route marking, and nothing else would list it.
  route_way_members "$PBF_FILE" "$dir/changed-rels.ids" > "$dir/old-members.ids"

  # Ends in .osm.pbf because osmium picks the input format from the name.
  local next="${PBF_FILE%.osm.pbf}.next.osm.pbf"
  osmium apply-changes -O -f pbf -o "$next" \
    --output-header="osmosis_replication_base_url=$BASE_URL" \
    --output-header="osmosis_replication_sequence_number=$end" \
    --output-header="osmosis_replication_timestamp=$end_ts" \
    --output-header="timestamp=$end_ts" \
    "$PBF_FILE" "$dir/merged.osc.gz"

  route_way_members "$next" "$dir/changed-rels.ids" > "$dir/new-members.ids"
  parents_of "$next" "$dir/changed-nodes.ids" > "$dir/node-parents.ids"

  # Every way whose row this cycle rewrites, then every relation containing
  # one of them, so a rewritten way always sees all of its routes.
  sort -u "$dir/changed-ways.ids" "$dir/old-members.ids" "$dir/new-members.ids" \
    <(grep '^w' "$dir/node-parents.ids" || true) > "$dir/ways.ids"
  sort -u "$dir/ways.ids" "$dir/changed-nodes.ids" > "$dir/ways-and-nodes.ids"
  parents_of "$next" "$dir/ways-and-nodes.ids" > "$dir/way-parents.ids"

  sort -u "$dir/changed.ids" "$dir/ways.ids" "$dir/node-parents.ids" "$dir/way-parents.ids" \
    > "$dir/affected.ids"
  echo "  rewriting $(wc -l < "$dir/affected.ids") objects"
  if [ ! -s "$dir/affected.ids" ]; then
    # An empty diff still advances the cursor and the extract's header.
    mv -f "$next" "$PBF_FILE"
    q "UPDATE osm_replication_state SET sequence = $end,
         data_timestamp = NULLIF('$end_ts', '')::timestamptz, updated_at = now();" >/dev/null
    rm -rf "$dir"
    log "Nothing to apply through sequence $end"
    return 0
  fi

  getid_allow_missing -r -O -f pbf -o "$dir/affected.osm.pbf" "$next" -i "$dir/affected.ids"

  # The extract moves first. See the note at the top. A retry after a failure
  # from here on reads the old relation members from the already patched
  # extract, so a way that left a bicycle route in this cycle keeps its route
  # marking until it is next edited. That needs a crash and a route edit in the
  # same cycle, and costs one stale bicycle_ways row.
  mv -f "$next" "$PBF_FILE"

  log "[3/5] Importing the touched objects into $STAGE_DB"
  psql "$ADMIN_URL" -v ON_ERROR_STOP=1 -q \
    -c "DROP DATABASE IF EXISTS $STAGE_DB WITH (FORCE)" \
    -c "CREATE DATABASE $STAGE_DB"
  psql "$STAGE_URL" -v ON_ERROR_STOP=1 -q -c "CREATE EXTENSION IF NOT EXISTS postgis"
  if ! osm2pgsql --create --slim --drop --output=flex \
      --style "$PROJECT_DIR/import/osm2pgsql-flex.lua" \
      -d "$STAGE_URL" "$dir/affected.osm.pbf" > "$dir/osm2pgsql.log" 2>&1; then
    tail -n 30 "$dir/osm2pgsql.log" >&2
    fail "the staging import failed (log above). Nothing was swapped."
  fi
  tail -n 2 "$dir/osm2pgsql.log"

  # osm2pgsql writes osm_type as N/W/R; the ID file uses n/w/r.
  sed -E 's/^n/N\t/; s/^w/W\t/; s/^r/R\t/' "$dir/affected.ids" > "$dir/affected.tsv"

  local places_cols ways_cols routes_cols
  places_cols="$(common_columns geo_places)"
  ways_cols="$(common_columns bicycle_ways)"
  routes_cols="$(common_columns bicycle_routes)"
  local s_places; s_places="s.${places_cols//,/, s.}"
  local s_ways; s_ways="s.${ways_cols//,/, s.}"
  local s_routes; s_routes="s.${routes_cols//,/, s.}"

  psql "$STAGE_URL" -v ON_ERROR_STOP=1 -q <<SQL
CREATE TABLE affected (osm_type char(1) NOT NULL, osm_id bigint NOT NULL);
\copy affected FROM '$dir/affected.tsv'
CREATE INDEX ON affected (osm_type, osm_id);
ANALYZE affected;
\copy (SELECT $s_places FROM geo_places s JOIN affected a ON a.osm_type = s.osm_type AND a.osm_id = s.osm_id) TO '$dir/geo_places.copy'
\copy (SELECT $s_ways FROM bicycle_ways s JOIN affected a ON a.osm_type = 'W' AND a.osm_id = s.osm_id) TO '$dir/bicycle_ways.copy'
\copy (SELECT $s_routes FROM bicycle_routes s JOIN affected a ON a.osm_type = 'R' AND a.osm_id = s.osm_id) TO '$dir/bicycle_routes.copy'
SQL

  log "[4/5] Swapping rows and re-deriving their columns (one transaction)"
  # Shrink guard: a diff mostly modifies objects, so the rows going in roughly
  # match the rows coming out. Far fewer going in means the extract or the
  # staging import lost data, and committing would delete live places. Real
  # mass deletions are rare enough to approve by hand.
  local min_ratio="0.5"
  [ "${REPLICATION_ALLOW_SHRINK:-0}" = "1" ] && min_ratio="0"
  # Normalised here because psql's \if refuses anything but a boolean, and an
  # error inside the transaction would roll the whole cycle back.
  local queue_road_markings="off"
  [ "${ROAD_MARKINGS_INCREMENTAL:-0}" = "1" ] && queue_road_markings="on"

  # BEGIN and COMMIT are spelled out: psql's -1 only applies to -c and -f, and
  # stdin silently runs each statement in its own transaction. With
  # ON_ERROR_STOP an error makes psql exit inside the open transaction, and the
  # server rolls all of it back.
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -q \
    -v min_ratio="$min_ratio" -v sequence="$end" -v data_ts="$end_ts" \
    -v queue_road_markings="$queue_road_markings" <<SQL
BEGIN;
DROP SCHEMA IF EXISTS osm_replay CASCADE;
CREATE SCHEMA osm_replay;

CREATE TABLE osm_replay.affected (osm_type char(1) NOT NULL, osm_id bigint NOT NULL);
\copy osm_replay.affected FROM '$dir/affected.tsv'
CREATE INDEX ON osm_replay.affected (osm_type, osm_id);
ANALYZE osm_replay.affected;

-- The rows being replaced, as they were. replay-derive.sql compares them with
-- their replacements to find intersections and parent context to redo.
CREATE TABLE osm_replay.old_places (
  id text, osm_type char(1), osm_id bigint, name text, categories text[],
  geom geometry, geom_type text, admin_level int
);
WITH gone AS (
  DELETE FROM geo_places g
  USING osm_replay.affected a
  WHERE g.osm_type = a.osm_type AND g.osm_id = a.osm_id
  RETURNING g.id, g.osm_type, g.osm_id, g.name, g.categories, g.geom, g.geom_type, g.admin_level
)
INSERT INTO osm_replay.old_places SELECT * FROM gone;

\copy geo_places ($places_cols) FROM '$dir/geo_places.copy'

DELETE FROM bicycle_ways b USING osm_replay.affected a
WHERE a.osm_type = 'W' AND b.osm_id = a.osm_id;
\copy bicycle_ways ($ways_cols) FROM '$dir/bicycle_ways.copy'

DELETE FROM bicycle_routes b USING osm_replay.affected a
WHERE a.osm_type = 'R' AND b.osm_id = a.osm_id;
\copy bicycle_routes ($routes_cols) FROM '$dir/bicycle_routes.copy'

CREATE TABLE osm_replay.changed AS
SELECT g.id FROM geo_places g
JOIN osm_replay.affected a ON g.osm_type = a.osm_type AND g.osm_id = a.osm_id;
CREATE INDEX ON osm_replay.changed (id);
ANALYZE osm_replay.changed;

SELECT (SELECT count(*) FROM osm_replay.old_places) AS removed,
       (SELECT count(*) FROM osm_replay.changed) AS added \gset
\echo '  rows:' :removed 'out,' :added 'in'
SELECT :removed >= 1000 AND :added < :removed * :min_ratio AS shrunk \gset
\if :shrunk
  \echo 'ERROR: the swap would remove far more rows than it adds. Rolled back.'
  \echo 'If the diffs really delete that much, re-run with REPLICATION_ALLOW_SHRINK=1.'
  SELECT 1/0;
\endif

\i $PROJECT_DIR/import/replay-derive.sql
\if :queue_road_markings
\i $PROJECT_DIR/import/road-markings-queue-table.sql
\i $PROJECT_DIR/import/queue-road-markings.sql
\endif

UPDATE osm_replication_state
SET sequence = :sequence, data_timestamp = NULLIF(:'data_ts', '')::timestamptz, updated_at = now();

-- old_places holds a geometry for every replaced row. Nothing reads it after
-- this point.
DROP SCHEMA osm_replay CASCADE;
COMMIT;
SQL

  log "[5/5] Cleaning up"
  psql "$ADMIN_URL" -q -c "DROP DATABASE IF EXISTS $STAGE_DB WITH (FORCE)" || true
  rm -rf "$dir"
  log "Applied through sequence $end ($end_ts)"
  # update-osm.sh reads this to decide on the follow-up steps, so it is printed
  # per committed cycle: a later cycle failing must not hide an earlier one.
  echo "REPLICATION_APPLIED=1"
}

while [ "$CURRENT" -lt "$LATEST" ]; do
  END=$(( CURRENT + MAX_DIFFS ))
  [ "$END" -gt "$LATEST" ] && END="$LATEST"
  apply_cycle "$CURRENT" "$END"
  CURRENT="$END"
done
