#!/bin/bash
set -euo pipefail

# =============================================================================
# Prepare the GraphHopper OSM extract
# =============================================================================
#
# Produces region-walk.osm.pbf: region.osm.pbf plus straight paths across
# pedestrian areas (import/synthesize-area-paths.py). GraphHopper otherwise
# walks a plaza around its outline. Also writes region-walk.spaces.ndjson (+
# .index.json), the open space of each area, which rebuild-graphhopper.sh puts
# beside the graph for the API to round a walk's bends within. region.osm.pbf is left intact for
# osm2pgsql, MOTIS and the tile layers.
#
# Called by rebuild-graphhopper.sh before every graph build. If the pass fails,
# the extract is linked through unchanged so routing still builds.
#
# Environment variables:
#   OSM_DATA_DIR  - directory holding region.osm.pbf (default: /data in ops, else ./data)
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
if [ -z "${OSM_DATA_DIR:-}" ]; then
  if [ -f /data/region.osm.pbf ]; then OSM_DATA_DIR=/data; else OSM_DATA_DIR="$PROJECT_DIR/data"; fi
fi

IN="$OSM_DATA_DIR/region.osm.pbf"
OUT="$OSM_DATA_DIR/region-walk.osm.pbf"
SPACES="$OSM_DATA_DIR/region-walk.spaces"
WORK="$OSM_DATA_DIR/.area-paths"

if [ ! -f "$IN" ]; then
  echo "error: $IN not found — download the OSM extract first" >&2
  exit 1
fi

log() { echo "[$(date '+%H:%M:%S')] [area-paths] $*"; }

python() {
  if command -v uv >/dev/null 2>&1 && ! python3 -c 'import osmium, shapely' 2>/dev/null; then
    uv run --no-project --with osmium --with shapely python3 "$@"
  else
    python3 "$@"
  fi
}

build() {
  rm -rf "$WORK" && mkdir -p "$WORK"
  log "Finding pedestrian areas..."
  osmium tags-filter "$IN" w/area=yes r/type=multipolygon -o "$WORK/tagged.osm.pbf" --overwrite
  osmium tags-filter "$WORK/tagged.osm.pbf" wr/highway=pedestrian,footway,path -o "$WORK/areas.osm.pbf" --overwrite
  python "$PROJECT_DIR/import/synthesize-area-paths.py" plan "$WORK/areas.osm.pbf" "$WORK/regions.geojson"

  log "Cutting out their surroundings..."
  osmium extract -p "$WORK/regions.geojson" -s smart "$IN" -o "$WORK/nearby.osm.pbf" --overwrite

  log "Joining their ways in..."
  python "$PROJECT_DIR/import/synthesize-area-paths.py" paths "$WORK/nearby.osm.pbf" "$WORK/paths.osm.pbf" "$WORK/spaces.ndjson"

  osmium merge "$IN" "$WORK/paths.osm.pbf" -o "$OUT.tmp" -f pbf --overwrite
  mv -f "$WORK/spaces.ndjson" "$SPACES.ndjson"
  mv -f "$WORK/spaces.index.json" "$SPACES.index.json"
  mv -f "$OUT.tmp" "$OUT"
}

# A subshell, not `if build`: bash ignores set -e inside a function run as a condition.
set +e
(set -e; build)
STATUS=$?
set -e
if [ "$STATUS" = 0 ]; then
  rm -rf "$WORK"
  log "✓ GraphHopper extract ready: $(du -h "$OUT" | cut -f1)"
else
  log "pass failed — linking region.osm.pbf through unchanged" >&2
  rm -rf "$WORK" "$OUT.tmp" "$SPACES.ndjson" "$SPACES.index.json"
  ln -f "$IN" "$OUT"
fi
