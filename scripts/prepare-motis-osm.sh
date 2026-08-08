#!/bin/bash
set -euo pipefail

# =============================================================================
# Prepare the MOTIS-specific OSM extract
# =============================================================================
#
# Produces region-transit.osm.pbf from region.osm.pbf by repairing the
# topology of underground transit platforms: isolated platforms get explicit
# synthetic connector ways (steps/elevators) to the nearest station entrance
# or elevator node; only anchorless platforms are stripped (see
# import/synthesize-platform-connectors.py for why). MOTIS consumes this
# extract; region.osm.pbf is left intact for GraphHopper, osm2pgsql, and the
# vector tile layers.
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

echo "[$(date '+%H:%M:%S')] Synthesizing platform connectors: $IN -> $OUT"
# pyosmium is not installed system-wide in barrelman-ops (Dockerfile.ops ships
# python3 + uv, not the module), so fetch it on demand the same way
# import-stop-areas.sh does. Falls back to the system interpreter where the
# module is already present.
if command -v uv >/dev/null 2>&1 && ! python3 -c 'import osmium' 2>/dev/null; then
  uv run --with osmium python3 "$PROJECT_DIR/import/synthesize-platform-connectors.py" "$IN" "$OUT"
else
  python3 "$PROJECT_DIR/import/synthesize-platform-connectors.py" "$IN" "$OUT"
fi
echo "[$(date '+%H:%M:%S')] ✓ MOTIS OSM extract ready: $(du -h "$OUT" | cut -f1)"
