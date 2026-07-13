#!/usr/bin/env bash
#
# Download GTFS feeds from Transitland and import into Barrelman.
#
# Usage:
#   TRANSITLAND_API_KEY=tlk_xxx ./scripts/download-gtfs.sh
#   REGIONS=global TRANSITLAND_API_KEY=tlk_xxx ./scripts/download-gtfs.sh
#
# Environment:
#   REGIONS              - Region keys driving the whole pipeline (default dev:
#                          north-carolina,nyc-metro; prod: global). The GTFS
#                          region tokens are resolved from config/regions.json.
#   GTFS_REGION          - Optional override of a single GTFS region token,
#                          bypassing the REGIONS config.
#   TRANSITLAND_API_KEY  - Required. Get one at https://transit.land/users/sign_up
#   GTFS_DATA_DIR        - Output directory for GTFS ZIPs. Default: ./data/gtfs
#   MOTIS_URL            - MOTIS base URL for reload. Default: http://localhost:8080
#
# The script (per resolved region):
#   1. Fetches the feed list from Transitland API
#   2. Downloads each GTFS ZIP
#   3. Runs the Bun import script to parse and load stops/routes into PostGIS
#   4. Computes walking transfers via GraphHopper
#   5. Injects transfers.txt into each feed
#
set -euo pipefail

API_KEY="${TRANSITLAND_API_KEY:-}"
DATA_DIR="${GTFS_DATA_DIR:-./data/gtfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -z "$API_KEY" ]]; then
  echo "Error: TRANSITLAND_API_KEY is required"
  echo "Get one at https://transit.land/users/sign_up"
  exit 1
fi

cd "$PROJECT_DIR"

# Resolve GTFS region tokens from the unified REGIONS config, unless a single
# GTFS_REGION override is given.
if [[ -n "${GTFS_REGION:-}" ]]; then
  REGIONS_LIST=("$GTFS_REGION")
else
  read -ra REGIONS_LIST <<< "$(bun run src/config/regions.ts gtfs-regions | tr '\n' ' ')"
fi

echo "=== GTFS Download Pipeline ==="
echo "Regions: ${REGIONS_LIST[*]}"
echo "Output: $DATA_DIR"
echo ""

mkdir -p "$DATA_DIR"

# Run the Bun import script per region (fetch feed list, download ZIPs, import
# stops/routes into PostGIS, compute walking transfers, generate transfers.txt).
for region in "${REGIONS_LIST[@]}"; do
  echo "── GTFS region: $region ──────────────────────────────"
  bun run import/import-gtfs.ts \
    --region "$region" \
    --api-key "$API_KEY" \
    --output-dir "$DATA_DIR"
done
