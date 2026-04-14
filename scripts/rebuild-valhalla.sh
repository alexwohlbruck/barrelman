#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Valhalla Tile Rebuild
# =============================================================================
#
# Refresh Valhalla tiles after an OSM update. Wipes built artifacts inside the
# barrelman-valhalla container and restarts it; the gis-ops entrypoint then
# rebuilds the graph from /custom_files/region.osm.pbf on startup.
#
# Custom graph.lua
# ────────────────
# The custom graph.lua is mounted at /custom_files/graph.lua. Bicycle
# access on pedestrian-only edges (footway, pedestrian, path) is handled
# at routing time by the custom use_pedestrian_paths costing parameter
# in our forked Valhalla build, NOT via graph.lua access flags.
#
# Country access overrides are disabled (apply_country_overrides=false)
# to prevent US defaults from overriding custom access flags.
#
# Called automatically from:
#   - scripts/run-import.sh   (after a full import)
#   - scripts/update-osm.sh   (after replication or full update)
#
# Or run manually:
#   ./scripts/rebuild-valhalla.sh
#
# Skips silently if barrelman-valhalla does not exist (e.g. minimal dev setup
# without the routing engine).
# =============================================================================

CONTAINER="barrelman-valhalla"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  echo "[valhalla] container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

echo "[$(date '+%H:%M:%S')] [valhalla] Wiping old tile artifacts..."
# Run as root because the gis-ops image's runtime user (uid 59999) cannot
# delete files written by the build (owned by root). The PBF and any host-
# placed files are intentionally preserved so the rebuild has input data.
# graph.lua is a read-only bind mount and is not deleted.
docker exec -u root "$CONTAINER" bash -c '
  cd /custom_files && \
  rm -rf valhalla_tiles valhalla_tiles.tar valhalla.json \
         admin_data timezone_data elevation_data \
         file_hashes.txt duplicateways.txt
'

# ── Pre-generate and patch valhalla.json BEFORE restarting ──────────
#
# The gis-ops entrypoint generates valhalla.json then immediately starts
# building tiles. If we wait until after restart to patch, tiles get built
# with the default config (no custom lua, country overrides enabled).
#
# Instead, we generate the config now, patch it, and place it so the
# entrypoint finds an existing config and uses it directly for the build.
echo "[$(date '+%H:%M:%S')] [valhalla] Generating and patching valhalla.json before build..."
docker exec -u root "$CONTAINER" bash -c '
  valhalla_build_config \
    --mjolnir-tile-dir /custom_files/valhalla_tiles \
    --mjolnir-tile-extract /custom_files/valhalla_tiles.tar \
    --mjolnir-admin /custom_files/admin_data/admins.sqlite \
    --mjolnir-timezone /custom_files/timezone_data/timezones.sqlite \
    --additional-data-elevation /custom_files/elevation_data \
    --mjolnir-concurrency '"${VALHALLA_THREADS:-2}"' \
    > /custom_files/valhalla.json

  # Patch: custom lua + disable country overrides
  CONFIG=/custom_files/valhalla.json
  jq ".mjolnir.graph_lua_name = \"/custom_files/graph.lua\"" "$CONFIG" | sponge "$CONFIG"
  jq ".mjolnir.data_processing.apply_country_overrides = false" "$CONFIG" | sponge "$CONFIG"
'

echo "[$(date '+%H:%M:%S')] [valhalla] Config ready. Restarting to build tiles..."
docker restart "$CONTAINER" >/dev/null

echo "[$(date '+%H:%M:%S')] [valhalla] Rebuild started in background. Tail logs with:"
echo "    docker logs -f ${CONTAINER}"
