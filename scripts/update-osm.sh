#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Daily OSM Update (Patch Import)
# =============================================================================
#
# Applies OSM changes and re-runs post-processing. Everything runs inside the
# barrelman-db container.
#
# Run it from the console (Scripts → "OSM Update") or, for the daily refresh,
# from Schedules — a scheduled run is an ordinary tracked job with logs and a
# progress bar. That replaces the host crontab this script used to document:
# cron drove it with `docker exec` and left no trace in the job list.
#
# MODES:
#   replication (default)
#     Applies incremental diffs from Geofabrik's replication server. Fast —
#     only processes changes since the last run. Auto-initializes replication
#     state if not already set up.
#
#     The cursor lives in the database (osm2pgsql_properties), not in a
#     timestamp, so this can be run at any interval and still applies every
#     diff since the last successful run. The one hard deadline is upstream
#     retention: Geofabrik keeps roughly four months of diffs, and a database
#     that falls further behind than that can only be recovered with a full
#     re-import followed by init-replication.
#
#     Each chunk is also applied to /data/region.osm.pbf (see
#     apply-osm-diff.sh), because osm2pgsql updates Postgres alone and
#     GraphHopper builds its graph from that file.
#
#   full
#     Downloads the latest extracts and re-imports from scratch. Slower
#     but always consistent. No initialization needed.
#
# CONFIGURATION (set in .env or export before running):
#   UPDATE_MODE                 replication | full  (default: replication)
#   GEOFABRIK_URL               Full extract download URL
#   GEOFABRIK_REPLICATION_URL   Diff update server URL (replication mode only).
#                               Defaults to the REGIONS-resolved feed.
#   BARRELMAN_DB_PASSWORD       DB password (default: barrelman)
#
# SCHEDULING:
#   Console → Schedules. The seeded "OSM Update" entry runs daily at 03:00 with
#   UPDATE_MODE=replication; enable it to turn it on.
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

UPDATE_MODE="${UPDATE_MODE:-replication}"
GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}"

# The extract that GraphHopper and MOTIS read, and that replication now patches.
PBF_FILE="/data/region.osm.pbf"
# Retained between chunks so apply-osm-diff.sh can find the diff — the hook is
# handed a sequence number, not a path.
DIFF_FILE="/data/replication-diff.osc.gz"

# Resolve the extracts and replication feeds from the unified REGIONS config,
# the same way run-import.sh does, so an install that imported four states does
# not silently update one. Falls back to the legacy single-URL vars.
OSM_EXTRACTS="$(cd "$PROJECT_DIR" && bun run src/config/regions.ts osm-extracts 2>/dev/null | tr '\n' ' ' || true)"
OSM_REPLICATION="$(cd "$PROJECT_DIR" && bun run src/config/regions.ts osm-replication 2>/dev/null || true)"

REPLICATION_COUNT="$(printf '%s\n' "$OSM_REPLICATION" | grep -c . || true)"
GEOFABRIK_REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-$(printf '%s\n' "$OSM_REPLICATION" | head -n1)}"
GEOFABRIK_REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-https://download.geofabrik.de/north-america/us/north-carolina-updates/}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting OSM update (mode: $UPDATE_MODE)"

# ── Step 1: Apply OSM changes ────────────────────────────────────────────────

if [ "$UPDATE_MODE" = "full" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/8] Running full re-import..."
  echo "  Regions: ${REGIONS:-north-carolina,nyc-metro}"
  echo "  Extracts: ${OSM_EXTRACTS:-$GEOFABRIK_URL}"

  # Let import-osm.sh do the downloading. It already merges multiple extracts
  # into one region.osm.pbf; the wget that used to live here fetched only
  # GEOFABRIK_URL and overwrote that merge, quietly shrinking a four-state
  # install to one state on the next "full" update. FORCE_DOWNLOAD is what
  # makes this a refresh rather than a re-import of the extract on disk, so it
  # defaults on here; an operator replaying a known-good download can still
  # pass 0.
  docker exec \
    -e DATABASE_URL="$DB_URL" \
    -e GEOFABRIK_URL="$GEOFABRIK_URL" \
    -e FORCE_DOWNLOAD="${FORCE_DOWNLOAD:-1}" \
    ${OSM_EXTRACTS:+-e OSM_EXTRACTS="$OSM_EXTRACTS"} \
    barrelman-db bash /app/scripts/import-osm.sh

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Triggering GraphHopper graph rebuild..."
  "$SCRIPT_DIR/rebuild-graphhopper.sh"

  # Full import runs the complete pipeline — we're done
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Full re-import complete."
  exit 0
fi

# ── Replication mode ─────────────────────────────────────────────────────────

# osm2pgsql-replication tracks a single upstream feed per database, so a
# multi-region install cannot follow all of its regions' diffs. Say so rather
# than reporting success on an update that covered one state out of four.
if [ "${REPLICATION_COUNT:-0}" -gt 1 ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: REGIONS resolves to $REPLICATION_COUNT replication feeds, but"
  echo "  osm2pgsql-replication follows only one per database. Diffs will be applied from:"
  echo "    $GEOFABRIK_REPLICATION_URL"
  echo "  The other regions stay at their last full import. Use UPDATE_MODE=full to refresh them all."
fi

# Auto-initialize replication state if missing
INIT_CHECK=$(docker exec barrelman-db \
  psql "$DB_URL" -tAc \
  "SELECT count(*) FROM osm2pgsql_properties WHERE property='replication_base_url';" 2>/dev/null || echo "0")

if [ "$INIT_CHECK" = "0" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Replication not initialized — running init..."
  docker exec barrelman-db \
    osm2pgsql-replication init \
      -d "$DB_URL" \
      --server "$GEOFABRIK_REPLICATION_URL"
fi

PBF_MTIME_BEFORE=$(docker exec barrelman-db stat -c %Y "$PBF_FILE" 2>/dev/null || echo 0)

# --post-processing patches region.osm.pbf with each chunk as it lands, so the
# routing graph tracks the database instead of the last full import. The hook
# runs before the cursor advances, so a failed patch is retried rather than
# skipped. See apply-osm-diff.sh.
#
# --output=flex is not redundant. osm2pgsql-replication builds its own append
# command and passes only what it is given after `--`; without an explicit
# output the binary falls back to the pgsql output and dies on the Lua style
# with "Weird style line ...:1". osm2pgsql 1.9+ can recover the output from
# osm2pgsql_properties, but the version in barrelman-db predates that table.
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/8] Applying OSM diffs (database + extract)..."
docker exec \
  -e OSM_DIFF_FILE="$DIFF_FILE" \
  -e OSM_PBF_FILE="$PBF_FILE" \
  barrelman-db \
  osm2pgsql-replication update \
    -d "$DB_URL" \
    --diff-file "$DIFF_FILE" \
    --post-processing /app/scripts/apply-osm-diff.sh \
    -- \
    --output=flex \
    --style /app/import/osm2pgsql-flex.lua \
    --slim

docker exec barrelman-db rm -f "$DIFF_FILE" || true

PBF_MTIME_AFTER=$(docker exec barrelman-db stat -c %Y "$PBF_FILE" 2>/dev/null || echo 0)

# ── Step 2: Post-import SQL (idempotent — ensures columns + extracts new data)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [2/8] Running post-import SQL..."
docker exec barrelman-db psql "$DB_URL" -f /app/import/post-import.sql

# ── Step 3: Generate codes (incremental — only new/changed rows) ─────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [3/8] Extracting codes..."
docker exec barrelman-db psql "$DB_URL" -c "
UPDATE geo_places
SET codes = sub.codes
FROM (
  SELECT id,
    array_agg(DISTINCT lower(trim(code))) FILTER (WHERE trim(code) <> '') AS codes
  FROM geo_places,
  LATERAL unnest(
    string_to_array(coalesce(tags->>'iata', ''), ';') ||
    string_to_array(coalesce(tags->>'icao', ''), ';') ||
    string_to_array(coalesce(tags->>'ref', ''), ';') ||
    string_to_array(coalesce(tags->>'short_name', ''), ';') ||
    string_to_array(coalesce(tags->>'abbreviation', ''), ';') ||
    string_to_array(coalesce(tags->>'alt_name', ''), ';')
  ) AS code
  WHERE tags IS NOT NULL
    AND (
      tags->>'iata' IS NOT NULL OR
      tags->>'icao' IS NOT NULL OR
      tags->>'ref' IS NOT NULL OR
      tags->>'short_name' IS NOT NULL OR
      tags->>'abbreviation' IS NOT NULL OR
      tags->>'alt_name' IS NOT NULL
    )
  GROUP BY id
) sub
WHERE geo_places.id = sub.id
  AND (geo_places.codes IS NULL OR geo_places.codes <> sub.codes);
"

# ── Step 4: Generate abbreviations (incremental — only missing rows) ─────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [4/8] Generating abbreviations..."
docker exec barrelman-db psql "$DB_URL" -c "
UPDATE geo_places
SET name_abbrev = sub.abbrev
FROM (
  SELECT id,
    lower(string_agg(left(word, 1), '' ORDER BY ord)) AS abbrev
  FROM (
    SELECT id, word, ord
    FROM geo_places,
    LATERAL unnest(regexp_split_to_array(name, '\s+')) WITH ORDINALITY AS t(word, ord)
    WHERE name IS NOT NULL
      AND name_abbrev IS NULL
      AND name ~ '^[\w\s\d\-''\.&]+\$'
  ) words
  WHERE lower(word) NOT IN (
    'of','the','and','at','in','for','a','an',
    'de','la','le','les','du','des','et','au',
    'der','die','das','von','und','im','am',
    'del','los','las','el','dos','e',
    'di','della','dei','degli'
  )
  AND length(word) > 0
  GROUP BY id
  HAVING count(*) >= 2
) sub
WHERE geo_places.id = sub.id;
"

# ── Step 5: Generate road intersections ──────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [5/8] Generating road intersections..."
docker exec barrelman-db psql "$DB_URL" -f /app/import/generate-intersections.sql

# ── Step 6: Resolve parent context (incremental — only NULL rows + cascade) ──
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [6/8] Resolving parent context (incremental)..."
docker exec barrelman-db psql "$DB_URL" -f /app/import/resolve-parent-context-incremental.sql

# ── Step 7: Rebuild tsvectors (intersections + new/changed rows only) ────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [7/8] Rebuilding tsvectors (intersections + new rows)..."
docker exec barrelman-db psql "$DB_URL" -v scope='intersections' -f /app/import/rebuild-tsvectors.sql

# ── Step 8: ANALYZE ──────────────────────────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [8/8] Running ANALYZE..."
docker exec barrelman-db psql "$DB_URL" -c "ANALYZE geo_places; ANALYZE bicycle_ways; ANALYZE bicycle_routes;"

# A graph rebuild wipes graph-cache and takes street routing down for the length
# of the import, so it is only worth doing when the extract it reads actually
# moved. Before the extract was patched above, this ran unconditionally and
# rebuilt an identical graph on every replication run.
if [ "$PBF_MTIME_AFTER" != "$PBF_MTIME_BEFORE" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extract changed — triggering GraphHopper graph rebuild..."
  "$SCRIPT_DIR/rebuild-graphhopper.sh"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Extract unchanged — skipping GraphHopper rebuild."
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] OSM update complete."
