#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman MOTIS Dataset Rebuild
# =============================================================================
#
# Rebuild the MOTIS transit dataset (timetable + street graph) after a GTFS
# refresh. This is REQUIRED: `motis server` only SERVES the pre-built dataset
# at /data/data — it never re-imports when the config or feeds change. A plain
# `docker restart` therefore keeps serving the stale schedules, so the new feeds
# never reach riders until this runs.
#
# Steps:
#   [1/3] Regenerate config.yml from the gtfs_feeds table. Region-aware by
#         construction — it covers exactly the feeds imported for the configured
#         regions (regions.json), and points at the merged region.osm.pbf.
#   [2/3] Clean-rebuild the dataset with `motis import` from config + the merged
#         region.osm.pbf. The existing dataset is moved aside first so ALL
#         derived artifacts — timetable, street graph, stop<->street match index,
#         footpaths — rebuild together and stay consistent. (An in-place
#         incremental import can rebuild the timetable while leaving the match
#         index stale, which silently breaks routing to every stop.)
#   [3/3] Recreate the MOTIS server to serve the fresh dataset.
#
# Run after a GTFS download/import (feeds changed → timetable must rebuild):
#   ./scripts/rebuild-motis.sh
#
# The previous dataset is kept at /data/data.prev until the next run, and is
# restored automatically if the import fails.
#
# Skips silently if barrelman-motis does not exist (e.g. minimal dev setup).
# =============================================================================

CONTAINER="barrelman-motis"
SERVICE="motis"
MOTIS_IMAGE="ghcr.io/motis-project/motis:latest"
NETWORK="barrelman_default"
GTFS_VOL="barrelman_barrelman-gtfs-data"
OSM_VOL="barrelman_barrelman-osm-data"

log() { echo "[$(date '+%H:%M:%S')] [motis] $*"; }

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  log "container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

BARRELMAN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BARRELMAN_DIR"

echo "[$(date '+%H:%M:%S')] [1/3] [motis] Regenerating config from gtfs_feeds..."
# --street-routing is REQUIRED: without it the config omits the OSM input and
# MOTIS builds only the timetable (no street graph, no stop<->street matches),
# so the router cannot reach any stop and returns no transit. --osm-path is the
# merged region.osm.pbf on the shared osm-data volume (mounted at /osm-data in
# the MOTIS container).
docker exec barrelman sh -lc 'cd /app && bun run import/generate-motis-config.ts --street-routing --osm-path /osm-data/region.osm.pbf --output /gtfs-data/config.yml'

echo "[$(date '+%H:%M:%S')] [2/3] [motis] Clean-rebuilding dataset (motis import)..."
# Move the current dataset aside (while the server is still up — it keeps serving
# via the memory-mapped inodes) so the import builds a fresh, internally
# consistent /data/data. Then stop the server so nothing holds files open.
docker exec "$CONTAINER" sh -c 'rm -rf /data/data.prev; [ -d /data/data ] && mv /data/data /data/data.prev || true'
docker stop "$CONTAINER" >/dev/null 2>&1 || true

if docker run --rm --network "$NETWORK" \
     -v "${GTFS_VOL}:/data" \
     -v "${OSM_VOL}:/osm-data:ro" \
     -w /data "$MOTIS_IMAGE" /motis import; then
  echo "[$(date '+%H:%M:%S')] [3/3] [motis] Recreating server to serve fresh dataset..."
  docker compose up -d --force-recreate "$SERVICE" >/dev/null
else
  log "ERROR: motis import failed — restoring previous dataset"
  # Server is stopped; use a throwaway container to swap the dataset back.
  docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c \
    'rm -rf /data/data; [ -d /data/data.prev ] && mv /data/data.prev /data/data || true'
  docker compose up -d --force-recreate "$SERVICE" >/dev/null
  exit 1
fi

sleep 6
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://localhost:8080/ 2>/dev/null || echo "000")
log "server responding: HTTP ${code} (404 at / is expected — the API lives under /api/v1)"
log "Rebuild complete."
