#!/usr/bin/env bash
#
# Download GTFS feeds from Transitland and import into Barrelman.
#
# Usage:
#   GTFS_REGION=nc TRANSITLAND_API_KEY=tlk_xxx ./scripts/download-gtfs.sh
#
# Environment:
#   GTFS_REGION         - "nc" (dev) or "global" (prod). Default: nc
#   TRANSITLAND_API_KEY  - Required. Get one at https://transit.land/users/sign_up
#   GTFS_DATA_DIR        - Output directory for GTFS ZIPs. Default: /data/gtfs (Docker) or ./data/gtfs (local)
#   MOTIS_URL            - MOTIS base URL for reload. Default: http://localhost:8080
#
# The script:
#   1. Fetches the feed list from Transitland API
#   2. Downloads each GTFS ZIP
#   3. Runs the Bun import script to parse and load stops/routes into PostGIS
#   4. Computes walking transfers via GraphHopper
#   5. Injects transfers.txt into each feed
#
set -euo pipefail

REGION="${GTFS_REGION:-nc}"
API_KEY="${TRANSITLAND_API_KEY:-}"
DATA_DIR="${GTFS_DATA_DIR:-./data/gtfs}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -z "$API_KEY" ]]; then
  echo "Error: TRANSITLAND_API_KEY is required"
  echo "Get one at https://transit.land/users/sign_up"
  exit 1
fi

echo "=== GTFS Download Pipeline ==="
echo "Region: $REGION"
echo "Output: $DATA_DIR"
echo ""

mkdir -p "$DATA_DIR"

# Run the Bun import script which handles:
# - Fetching feed list from Transitland
# - Downloading GTFS ZIPs
# - Parsing and importing stops/routes into PostGIS
# - Computing walking transfers
# - Generating transfers.txt
cd "$PROJECT_DIR"
exec bun run src/import/import-gtfs.ts \
  --region "$REGION" \
  --api-key "$API_KEY" \
  --output-dir "$DATA_DIR"
