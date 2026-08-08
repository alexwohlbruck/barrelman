#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman GraphHopper Graph Rebuild
# =============================================================================
#
# Refresh GraphHopper routing graph after an OSM update. Wipes the graph cache
# inside the barrelman-graphhopper container and restarts it; on startup
# GraphHopper will re-import the OSM PBF and rebuild the graph.
#
# Called automatically from:
#   - scripts/run-import.sh   (after a full import)
#   - scripts/update-osm.sh   (after replication or full update)
#
# Or run manually:
#   ./scripts/rebuild-graphhopper.sh
#
# Skips silently if barrelman-graphhopper does not exist (e.g. minimal dev
# setup without the routing engine).
# =============================================================================

CONTAINER="barrelman-graphhopper"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  echo "[graphhopper] container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

echo "[$(date '+%H:%M:%S')] [1/2] [graphhopper] Wiping graph cache..."

# Stop first, then wipe from OUTSIDE the container.
#
# This used to `docker exec` the rm into barrelman-graphhopper itself, which
# fails with "Container is restarting, wait until the container is running"
# precisely when the graph is corrupt — because a corrupt graph is what makes
# GraphHopper crash-loop. The rebuild then silently skipped the wipe, restarted
# onto the same bad cache, and looped forever on:
#
#   Not a GraphHopper file /data/graph-cache/properties! Expected 'GH' as file marker
#
# A first boot with no PBF leaves exactly that skeleton behind, so a fresh
# install hit it every time. set -e did not catch it either: the exec's failure
# was the last command in a pipeline-free line that still returned 0 often
# enough to slip past.
docker stop "$CONTAINER" >/dev/null 2>&1 || true

# The israelhikingmap/graphhopper entrypoint defaults to /data/default-gh unless
# overridden with -o. Wipe both possible locations to be safe.
#
# barrelman-ops (which runs this script) mounts the same barrelman-osm-data
# volume at /data, so the wipe happens directly. On a host without that mount,
# fall back to a throwaway container holding the volume.
if [ -d /data ] && [ -w /data ]; then
  rm -rf /data/graph-cache /data/default-gh
else
  VOLUME="$(docker inspect "$CONTAINER" \
    --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}')"
  if [ -z "$VOLUME" ]; then
    echo "[graphhopper] could not resolve the /data volume for ${CONTAINER}" >&2
    exit 1
  fi
  docker run --rm -v "${VOLUME}:/data" alpine sh -c 'rm -rf /data/graph-cache /data/default-gh'
fi

if [ -e /data/graph-cache ]; then
  echo "[graphhopper] graph-cache still present after wipe — refusing to restart onto a stale graph" >&2
  exit 1
fi

echo "[$(date '+%H:%M:%S')] [2/2] [graphhopper] Restarting to rebuild graph..."
docker start "$CONTAINER" >/dev/null

echo "[$(date '+%H:%M:%S')] [graphhopper] Rebuild started in background. Tail logs with:"
echo "    docker logs -f ${CONTAINER}"
