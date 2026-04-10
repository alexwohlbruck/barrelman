#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Daily OSM Update (Patch Import)
# =============================================================================
#
# Applies OSM changes and re-runs post-processing. Intended for daily/weekly
# cron jobs. Everything runs inside the barrelman-db container.
#
# MODES:
#   replication (default)
#     Applies incremental diffs from Geofabrik's replication server. Fast —
#     only processes changes since the last run. Auto-initializes replication
#     state if not already set up.
#
#   full
#     Downloads the latest full extract and re-imports from scratch. Slower
#     but always consistent. No initialization needed.
#
# CONFIGURATION (set in .env or export before running):
#   UPDATE_MODE                 replication | full  (default: replication)
#   GEOFABRIK_URL               Full extract download URL
#   GEOFABRIK_REPLICATION_URL   Diff update server URL (replication mode only)
#   BARRELMAN_DB_PASSWORD       DB password (default: barrelman)
#
# SCHEDULING (crontab):
#   Daily at 3am:
#     0 3 * * * /opt/barrelman/scripts/update-osm.sh >> /var/log/barrelman-update.log 2>&1
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$(dirname "$SCRIPT_DIR")/.env"

if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

UPDATE_MODE="${UPDATE_MODE:-replication}"
GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}"
GEOFABRIK_REPLICATION_URL="${GEOFABRIK_REPLICATION_URL:-https://download.geofabrik.de/north-america/us/north-carolina-updates/}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting OSM update (mode: $UPDATE_MODE)"

# ── Step 1: Apply OSM changes ────────────────────────────────────────────────

if [ "$UPDATE_MODE" = "full" ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/7] Downloading latest extract..."
  docker exec barrelman-db \
    wget -q --show-progress -O /data/region.osm.pbf "$GEOFABRIK_URL"

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/7] Running full re-import..."
  docker exec \
    -e DATABASE_URL="$DB_URL" \
    barrelman-db bash /app/scripts/import-osm.sh

  # Full import runs the complete pipeline — we're done
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Full re-import complete."
  exit 0
fi

# ── Replication mode ─────────────────────────────────────────────────────────

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

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [1/7] Applying OSM diffs..."
docker exec barrelman-db \
  osm2pgsql-replication update \
    -d "$DB_URL" \
    -- \
    --style /app/import/osm2pgsql-flex.lua \
    --slim

# ── Step 2: Post-import SQL (idempotent — ensures columns + extracts new data)
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [2/7] Running post-import SQL..."
docker exec barrelman-db psql "$DB_URL" -f /app/import/post-import.sql

# ── Step 3: Generate codes (incremental — only new/changed rows) ─────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [3/7] Extracting codes..."
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
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [4/7] Generating abbreviations..."
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

# ── Step 5: Resolve parent context (incremental — only NULL rows + cascade) ──
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [5/7] Resolving parent context (incremental)..."
docker exec barrelman-db psql "$DB_URL" -f /app/import/resolve-parent-context-incremental.sql

# ── Step 6: Rebuild tsvectors ────────────────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [6/7] Rebuilding full-text search index..."
docker exec barrelman-db psql "$DB_URL" -c "
UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    coalesce(name, '') || ' ' || coalesce(name_abbrev, '') || ' ' ||
    coalesce(array_to_string(
        ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
    ' '), '') || ' ' ||
    coalesce(parent_context, '')
))
WHERE name IS NOT NULL;
"

# ── Step 7: ANALYZE ──────────────────────────────────────────────────────────
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [7/7] Running ANALYZE..."
docker exec barrelman-db psql "$DB_URL" -c "ANALYZE geo_places; ANALYZE bicycle_ways; ANALYZE bicycle_routes;"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] OSM update complete."
