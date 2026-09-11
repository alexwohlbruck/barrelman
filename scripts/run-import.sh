#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Full Import (Host Wrapper)
# =============================================================================
#
# Downloads OSM data and runs the complete import pipeline, overlapping the
# work that does not need the database:
#
#   download PBF ──┬── GraphHopper graph build   (own container, reads the PBF)
#                  ├── basemap render            (planetiler, reads the PBF)
#                  └── osm2pgsql → post-processing SQL (barrelman-db)
#
# GraphHopper and planetiler consume region.osm.pbf directly, so waiting for
# hours of osm2pgsql + SQL before starting them (the old sequential flow) added
# their entire duration to the wall clock for no reason. The basemap render is
# deliberately started AFTER osm2pgsql finishes rather than with GraphHopper:
# planetiler's page-cache appetite next to osm2pgsql's sort phase risks memory
# pressure on smaller hosts, while overlapping it with the (index/UPDATE-bound)
# SQL phase is safe.
#
# Usage:
#   ./scripts/run-import.sh
#
# Environment variables (or set in .env):
#   GEOFABRIK_URL         - PBF download URL (default: NC extract)
#   IMPORT_PBF            - Path to a local PBF inside the container (/data/...)
#   BARRELMAN_DB_PASSWORD - DB password (default: barrelman)
#   REBUILD_BASEMAP       - 0 to skip the basemap render (default: 1)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

# Load .env if present
if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

DB_PASS="${BARRELMAN_DB_PASSWORD:-barrelman}"
DB_URL="postgresql://barrelman:${DB_PASS}@localhost:5432/barrelman"

# Resolve which OSM extracts to import from the unified REGIONS config.
# REGIONS selects the regions (default dev: north-carolina,nyc-metro; prod: global).
# Falls back to the single GEOFABRIK_URL if the resolver is unavailable.
OSM_EXTRACTS="$(cd "$PROJECT_DIR" && bun run src/config/regions.ts osm-extracts 2>/dev/null | tr '\n' ' ' || true)"

echo "Starting full OSM import pipeline..."
echo "  Regions: ${REGIONS:-north-carolina,nyc-metro}"
echo "  OSM extracts: ${OSM_EXTRACTS:-$GEOFABRIK_URL}"
echo ""

# All three DB-container phases share this invocation; only IMPORT_PHASE varies.
db_import_phase() {
  docker exec \
    -e DATABASE_URL="$DB_URL" \
    -e IMPORT_PHASE="$1" \
    -e GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}" \
    ${OSM_EXTRACTS:+-e OSM_EXTRACTS="$OSM_EXTRACTS"} \
    ${IMPORT_PBF:+-e IMPORT_PBF="$IMPORT_PBF"} \
    ${FORCE_DOWNLOAD:+-e FORCE_DOWNLOAD="$FORCE_DOWNLOAD"} \
    ${OSM2PGSQL_FLAT_NODES:+-e OSM2PGSQL_FLAT_NODES="$OSM2PGSQL_FLAT_NODES"} \
    ${OSM2PGSQL_CACHE_MB:+-e OSM2PGSQL_CACHE_MB="$OSM2PGSQL_CACHE_MB"} \
    ${OSM2PGSQL_PROCESSES:+-e OSM2PGSQL_PROCESSES="$OSM2PGSQL_PROCESSES"} \
    barrelman-db bash /app/scripts/import-osm.sh
}

echo "[1/4] OSM download"
db_import_phase download

# The extract is on disk — everything that needs only the PBF can start now.
# rebuild-graphhopper.sh is fire-and-forget by design (it wipes the cache and
# restarts the container; the build runs inside GraphHopper), so this line
# costs seconds and the graph builds concurrently with everything below.
echo ""
echo "[2/4] GraphHopper rebuild (starts now, builds in its own container)"
"$SCRIPT_DIR/rebuild-graphhopper.sh"

echo ""
echo "[3/4] OSM import (osm2pgsql + post-processing)"
db_import_phase osm2pgsql

# martin serves the DB-backed sources live, but the `basemap` source is a
# static PMTiles archive — a full import moves everything else and leaves it
# frozen unless it is re-rendered. Render concurrently with the SQL phase;
# planetiler reads the PBF and writes an archive, touching nothing the SQL
# needs. Skips itself when the install has no basemap.
BASEMAP_PID=""
BASEMAP_LOG=""
if [ "${REBUILD_BASEMAP:-1}" = "1" ]; then
  BASEMAP_LOG="$(mktemp /tmp/basemap-rebuild.XXXXXX.log)"
  echo ""
  echo "  Basemap render started in parallel (log: $BASEMAP_LOG)"
  "$SCRIPT_DIR/rebuild-basemap.sh" > "$BASEMAP_LOG" 2>&1 &
  BASEMAP_PID=$!
else
  echo "  Basemap rebuild disabled (REBUILD_BASEMAP=0) — skipping."
fi

db_import_phase post

echo ""
echo "[4/4] Basemap"
if [ -n "$BASEMAP_PID" ]; then
  # set -e must not kill the whole (finished) import over a basemap failure —
  # rebuild-basemap.sh already leaves the previous archive in place on error.
  if wait "$BASEMAP_PID"; then
    echo "  Basemap render finished."
  else
    echo "  WARNING: basemap render failed — the previous basemap remains in place."
    echo "  ── basemap log tail ──"
    tail -20 "$BASEMAP_LOG" || true
  fi
  rm -f "$BASEMAP_LOG"
fi

echo ""
echo "Full import pipeline complete!"
echo "NOTE: the GraphHopper graph may still be building — it started at step 2"
echo "and finishes on its own. Check: docker logs barrelman-graphhopper"
