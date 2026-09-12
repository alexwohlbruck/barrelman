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

# ── Engine overlap vs. memory safety ─────────────────────────────────────────
# GraphHopper builds its graph (JVM heap = -Xmx, several GB, and landmark
# preparation holds it for hours) and planetiler renders the basemap (its own
# heap) — both from the PBF, needing nothing the database work produces. On a
# roomy box, starting them alongside the import removes hours from the wall
# clock. On a small one it is how you lose the import: a US run on a 30 GB box
# OOM-killed GraphHopper (10 GB heap) when its landmark prep ran next to
# Postgres and a memory-heavy step, and the kernel reaped the JVM mid-build.
#
# IMPORT_ENGINE_OVERLAP: auto (default) | 1 (force overlap) | 0 (sequential).
# "auto" overlaps only when the box clears a RAM threshold; below it the engines
# build one after another, after the database import, trading wall-clock for not
# being OOM-killed.
TOTAL_RAM_GB=$(awk "/MemTotal/ {printf \"%d\", \$2/1024/1024}" /proc/meminfo 2>/dev/null || echo 0)
OVERLAP="${IMPORT_ENGINE_OVERLAP:-auto}"
if [ "$OVERLAP" = "auto" ]; then
  # 48 GB comfortably holds GraphHopper's heap + planetiler + Postgres +
  # osm2pgsql + headroom at once; 30 GB does not (measured).
  if [ "${TOTAL_RAM_GB:-0}" -ge 48 ]; then OVERLAP=1; else OVERLAP=0; fi
  echo "Engine overlap: auto -> $([ "$OVERLAP" = 1 ] && echo ON || echo OFF) (host has ${TOTAL_RAM_GB} GB RAM)"
fi

echo "[1/4] OSM download"
db_import_phase download

# The extract is on disk — the engines need only the PBF. When overlapping, the
# GraphHopper rebuild starts now (it is fire-and-forget: wipes the cache and
# restarts the container, the build runs inside GraphHopper) and the graph
# builds concurrently with everything below.
echo ""
if [ "$OVERLAP" = 1 ]; then
  echo "[2/4] GraphHopper rebuild (starts now, builds in its own container)"
  "$SCRIPT_DIR/rebuild-graphhopper.sh"
else
  echo "[2/4] GraphHopper rebuild deferred (engine overlap off — runs after the import)"
fi

echo ""
echo "[3/4] OSM import (osm2pgsql + post-processing)"
db_import_phase osm2pgsql

# martin serves the DB-backed sources live, but the `basemap` source is a
# static PMTiles archive — a full import moves everything else and leaves it
# frozen unless it is re-rendered. When overlapping, render it concurrently with
# the SQL phase; planetiler reads the PBF and writes an archive, touching
# nothing the SQL needs. Skips itself when the install has no basemap.
BASEMAP_PID=""
BASEMAP_LOG=""
if [ "${REBUILD_BASEMAP:-1}" != "1" ]; then
  echo "  Basemap rebuild disabled (REBUILD_BASEMAP=0) — skipping."
elif [ "$OVERLAP" = 1 ]; then
  BASEMAP_LOG="$(mktemp /tmp/basemap-rebuild.XXXXXX.log)"
  echo ""
  echo "  Basemap render started in parallel (log: $BASEMAP_LOG)"
  "$SCRIPT_DIR/rebuild-basemap.sh" > "$BASEMAP_LOG" 2>&1 &
  BASEMAP_PID=$!
fi

db_import_phase post

# Sequential mode: the database is done and has released its working memory, so
# build the engines now — but one at a time. rebuild-graphhopper.sh returns as
# soon as it restarts the container (the graph builds asynchronously inside it),
# and a still-building GraphHopper holds its full heap; starting planetiler on
# top of that is the very OOM this mode exists to avoid. So wait for GraphHopper
# to finish (its main HTTP port only opens once the graph is loaded — landmark
# prep can take hours at continent scale) before the basemap render.
if [ "$OVERLAP" != 1 ]; then
  echo ""
  echo "[3b/4] GraphHopper rebuild (sequential)"
  "$SCRIPT_DIR/rebuild-graphhopper.sh"
  if [ "${REBUILD_BASEMAP:-1}" = "1" ]; then
    echo "  Waiting for GraphHopper to finish before the basemap render..."
    # Container-network probe (no docker-proxy in the way, so a successful
    # connect is honest): 8989 only listens once the graph is built and served.
    for _ in $(seq 1 480); do  # up to ~8h; landmark prep on a planet-scale graph is long
      if timeout 5 bash -c "exec 3<>/dev/tcp/barrelman-graphhopper/8989" 2>/dev/null; then
        echo "  GraphHopper is serving; starting basemap."
        break
      fi
      sleep 60
    done
    echo "  Basemap render (sequential)"
    BASEMAP_LOG="$(mktemp /tmp/basemap-rebuild.XXXXXX.log)"
    "$SCRIPT_DIR/rebuild-basemap.sh" > "$BASEMAP_LOG" 2>&1 &
    BASEMAP_PID=$!
  fi
fi

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
