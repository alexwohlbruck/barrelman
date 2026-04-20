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

echo "[$(date '+%H:%M:%S')] [graphhopper] Wiping graph cache..."
# The israelhikingmap/graphhopper entrypoint defaults to /data/default-gh unless
# overridden with -o.  Wipe both possible locations to be safe.
docker exec "$CONTAINER" bash -c 'rm -rf /data/graph-cache /data/default-gh'

echo "[$(date '+%H:%M:%S')] [graphhopper] Restarting to rebuild graph..."
docker restart "$CONTAINER" >/dev/null

echo "[$(date '+%H:%M:%S')] [graphhopper] Rebuild started in background. Tail logs with:"
echo "    docker logs -f ${CONTAINER}"
