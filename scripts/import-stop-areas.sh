#!/bin/bash
set -euo pipefail

# =============================================================================
# Import public_transport=stop_area relations into the Barrelman DB
# =============================================================================
#
# Extracts stop_area relations from the OSM pbf (import/import-stop-areas.py)
# and loads them into the stop_area_members table. These power Tier 0 of the
# nearest-entrance search: an entrance that shares a stop_area relation with
# the platform being boarded wins over purely geometric candidates.
#
# Run after the OSM pbf is in place / refreshed. Idempotent (table is
# rebuilt atomically).
#
# Environment variables:
#   OSM_PBF      - OSM extract path (default: ./data/region.osm.pbf)
#   DB_CONTAINER - Postgres container name (default: barrelman-db)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OSM_PBF="${OSM_PBF:-$PROJECT_DIR/data/region.osm.pbf}"
DB_CONTAINER="${DB_CONTAINER:-barrelman-db}"

if [ ! -f "$OSM_PBF" ]; then
  echo "error: $OSM_PBF not found" >&2
  exit 1
fi

PSQL=(docker exec -i "$DB_CONTAINER" sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1')

echo "[$(date '+%H:%M:%S')] Extracting stop_area relations from $OSM_PBF"
TSV="$(mktemp)"
trap 'rm -f "$TSV"' EXIT
if command -v uv >/dev/null 2>&1 && ! python3 -c 'import osmium' 2>/dev/null; then
  uv run --with osmium python3 "$PROJECT_DIR/import/import-stop-areas.py" "$OSM_PBF" > "$TSV"
else
  python3 "$PROJECT_DIR/import/import-stop-areas.py" "$OSM_PBF" > "$TSV"
fi
echo "  $(wc -l < "$TSV" | tr -d ' ') member rows"

echo "[$(date '+%H:%M:%S')] Loading stop_area_members"
"${PSQL[@]}" <<'SQL'
CREATE TABLE IF NOT EXISTS stop_area_members (
  relation_id   bigint NOT NULL,
  relation_name text,
  member_type   char(1) NOT NULL,
  member_ref    bigint NOT NULL,
  member_role   text
);
TRUNCATE stop_area_members;
SQL
docker exec -i "$DB_CONTAINER" sh -c 'PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "\copy stop_area_members FROM STDIN"' < "$TSV"
"${PSQL[@]}" <<'SQL'
CREATE INDEX IF NOT EXISTS stop_area_members_ref_idx
  ON stop_area_members (member_type, member_ref);
CREATE INDEX IF NOT EXISTS stop_area_members_rel_idx
  ON stop_area_members (relation_id);
ANALYZE stop_area_members;
SQL

echo "[$(date '+%H:%M:%S')] ✓ stop_area_members loaded"
