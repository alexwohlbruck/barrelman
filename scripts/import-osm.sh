#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Full OSM Import
# =============================================================================
#
# Complete import pipeline: downloads PBF, imports with osm2pgsql, and runs ALL
# post-processing steps. After this script finishes, the database is fully
# ready for search (tsvectors, codes, abbreviations, parent context, indexes).
#
# Runs inside the barrelman-db container (needs osm2pgsql + psql).
# Called from the host via scripts/run-import.sh, or directly:
#
#   docker exec -e DATABASE_URL=... barrelman-db bash /app/scripts/import-osm.sh
#
# Environment variables:
#   DATABASE_URL          - PostgreSQL connection string (required)
#   GEOFABRIK_URL         - PBF download URL (default: NC extract)
#   IMPORT_PBF            - Path to local PBF file, overrides download
#   BARRELMAN_DATA_DIR    - Data directory path (default: /data)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${BARRELMAN_DATA_DIR:-$PROJECT_DIR/data}"

GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}"
PBF_FILE="${IMPORT_PBF:-${1:-$DATA_DIR/region.osm.pbf}}"
DATABASE_URL="${DATABASE_URL:?DATABASE_URL is required}"

mkdir -p "$DATA_DIR"

# ── Step 1: Download PBF ─────────────────────────────────────────────────────
if [ ! -f "$PBF_FILE" ]; then
    echo "[$(date '+%H:%M:%S')] [1/7] Downloading PBF from $GEOFABRIK_URL..."
    wget -q --show-progress -O "$PBF_FILE" "$GEOFABRIK_URL"
    echo "  Downloaded: $(du -h "$PBF_FILE" | cut -f1)"
else
    echo "[$(date '+%H:%M:%S')] [1/7] Using existing PBF: $PBF_FILE ($(du -h "$PBF_FILE" | cut -f1))"
fi

# ── Step 2: osm2pgsql import ─────────────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] [2/7] Running osm2pgsql import..."
osm2pgsql \
    --create \
    --slim \
    --output=flex \
    --style="$PROJECT_DIR/import/osm2pgsql-flex.lua" \
    -d "$DATABASE_URL" \
    "$PBF_FILE"
echo "  osm2pgsql complete."

# ── Step 3: Post-import SQL (columns, structured fields, indexes) ────────────
echo "[$(date '+%H:%M:%S')] [3/7] Running post-import SQL..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/post-import.sql"

# ── Step 4: Generate codes from OSM tags ─────────────────────────────────────
echo "[$(date '+%H:%M:%S')] [4/7] Extracting codes (IATA, ICAO, ref, short_name, alt_name)..."
psql "$DATABASE_URL" -c "
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

# ── Step 5: Generate abbreviations ───────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] [5/7] Generating abbreviations for multi-word names..."
psql "$DATABASE_URL" -c "
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
      AND name ~ '^[\w\s\d\-''\.&]+$'
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

# ── Step 6: Resolve parent context (spatial join) ────────────────────────────
echo "[$(date '+%H:%M:%S')] [6/7] Resolving parent boundary context (spatial join)..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/resolve-parent-context.sql"

# ── Step 7: Rebuild tsvectors (final, with all enriched data) ────────────────
echo "[$(date '+%H:%M:%S')] [7/7] Building full-text search index..."
psql "$DATABASE_URL" -c "
UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    coalesce(name, '') || ' ' || coalesce(name_abbrev, '') || ' ' ||
    coalesce(array_to_string(
        ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
    ' '), '') || ' ' ||
    coalesce(parent_context, '')
))
WHERE name IS NOT NULL;
"

# ── Final: ANALYZE ───────────────────────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] Running ANALYZE..."
psql "$DATABASE_URL" -c "ANALYZE geo_places; ANALYZE bicycle_ways; ANALYZE bicycle_routes;"

echo ""
echo "[$(date '+%H:%M:%S')] ✓ Import complete!"
echo ""
echo "  Optional: generate semantic embeddings with:"
echo "  docker exec -e DATABASE_URL=\$DB_URL barrelman bun run import/embed-places.ts"
