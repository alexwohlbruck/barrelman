#!/bin/bash
set -euo pipefail

# =============================================================================
# Prepare the MOTIS-specific OSM extract
# =============================================================================
#
# Produces region-transit.osm.pbf from region.osm.pbf by stripping underground
# transit-platform ways (see import/strip-underground-platforms.py for why).
# MOTIS consumes this extract; region.osm.pbf is left intact for GraphHopper,
# osm2pgsql, and the vector tile layers.
#
# Run after the OSM pbf is in place and whenever it is refreshed, before the
# MOTIS import. Idempotent — safe to re-run.
#
# Requires: python3 with pyosmium (pip install osmium).
#
# Environment variables:
#   OSM_DATA_DIR  - directory holding region.osm.pbf (default: ./data)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OSM_DATA_DIR="${OSM_DATA_DIR:-$PROJECT_DIR/data}"

IN="$OSM_DATA_DIR/region.osm.pbf"
OUT="$OSM_DATA_DIR/region-transit.osm.pbf"

if [ ! -f "$IN" ]; then
  echo "error: $IN not found — download the OSM extract first" >&2
  exit 1
fi

echo "[$(date '+%H:%M:%S')] Stripping underground platforms: $IN -> $OUT"
python3 "$PROJECT_DIR/import/strip-underground-platforms.py" "$IN" "$OUT"
echo "[$(date '+%H:%M:%S')] ✓ MOTIS OSM extract ready: $(du -h "$OUT" | cut -f1)"
