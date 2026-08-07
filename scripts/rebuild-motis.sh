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

# The server is stopped before the import and started again after, rather than
# recreated via `docker compose up --force-recreate`. Nothing about the
# container's definition changes — only the dataset on the shared volume — and a
# stopped container re-opens (re-mmaps) the fresh files on start. Using plain
# `docker` also keeps this runnable from barrelman-ops, which has the docker CLI
# but no compose plugin, and whose working directory would resolve to a
# different compose project name than the operator's.
CONTAINER="barrelman-motis"
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
# Move the current dataset aside so the import builds a fresh, internally
# consistent /data/data.
#
# On a healthy instance this happens while the server is still up — it keeps
# serving via the memory-mapped inodes, so there is no gap. But a MOTIS with no
# valid dataset crash-loops, and `docker exec` into a restarting container
# fails ("Container is restarting, wait until the container is running") —
# which is exactly the state a first-time install is in. So fall back to moving
# the dataset from outside once the container is stopped. Same failure mode
# rebuild-graphhopper.sh had.
MOVE='rm -rf /data/data.prev; [ -d /data/data ] && mv /data/data /data/data.prev || true'

if [ "$(docker inspect "$CONTAINER" --format '{{.State.Running}}' 2>/dev/null)" = "true" ] \
   && docker exec "$CONTAINER" sh -c "$MOVE" 2>/dev/null; then
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
else
  log "container not usable for an in-place move — stopping and moving from outside"
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  # barrelman-ops mounts the same volume at /gtfs-data; otherwise use a
  # throwaway container holding it.
  if [ -d /gtfs-data ] && [ -w /gtfs-data ]; then
    rm -rf /gtfs-data/data.prev
    [ -d /gtfs-data/data ] && mv /gtfs-data/data /gtfs-data/data.prev || true
  else
    docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c "$MOVE"
  fi
fi

# The feed ZIPs are a host bind mount, not part of the gtfs-data volume, so the
# import container needs them mounted exactly where the config expects them
# (/data/gtfs) — same bind the motis service carries in docker-compose.yml.
GTFS_ZIPS="$(docker inspect "$CONTAINER" \
  --format '{{range .Mounts}}{{if eq .Destination "/data/gtfs"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)"

if docker run --rm --network "$NETWORK" \
     -v "${GTFS_VOL}:/data" \
     -v "${OSM_VOL}:/osm-data:ro" \
     ${GTFS_ZIPS:+-v "${GTFS_ZIPS}:/data/gtfs:ro"} \
     -w /data "$MOTIS_IMAGE" /motis import; then
  echo "[$(date '+%H:%M:%S')] [3/3] [motis] Restarting server to serve fresh dataset..."
  docker start "$CONTAINER" >/dev/null
else
  log "ERROR: motis import failed — restoring previous dataset"
  # Server is stopped; use a throwaway container to swap the dataset back.
  docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c \
    'rm -rf /data/data; [ -d /data/data.prev ] && mv /data/data.prev /data/data || true'
  docker start "$CONTAINER" >/dev/null
  exit 1
fi

sleep 6
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 http://localhost:8080/ 2>/dev/null || echo "000")
log "server responding: HTTP ${code} (404 at / is expected — the API lives under /api/v1)"
log "Rebuild complete."
