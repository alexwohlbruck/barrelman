#!/usr/bin/env bash
# Build a LOOM display line-graph for a GTFS feed and load it into transit_graph_*.
#
# Runs LOOM's geographic pipeline (gtfs2graph | topo | loom) on a feed's rail
# routes to produce a bundled line-graph GeoJSON, then loads it via
# import/load-transit-graph.ts under a build_key. Both NYC subway (nyc:subway)
# and Chicago's L (chicago:l) were built this way. After loading, refresh the
# display matviews.
#
# Requires the local loom:latest docker image and the feed already imported
# (data/gtfs-processed/<feedId>.zip present — the fully preprocessed copy the
# import pipeline builds, so LOOM sees the same shapes/overrides MOTIS does;
# falls back to the raw data/gtfs zip for feeds predating the transform stage).
#
# Usage:
#   scripts/build-transit-graph.sh <feedId> <buildKey> [mode] [routeType]
#   e.g. scripts/build-transit-graph.sh 5  nyc:subway subway 1
#        scripts/build-transit-graph.sh 29 chicago:l  subway 1
set -euo pipefail
FEED="${1:?feedId required}"
KEY="${2:?buildKey required (e.g. chicago:l)}"
MODE="${3:-subway}"
RT="${4:-1}"
# topo max-aggregation-distance (metres). This is effectively the "line drawing
# thickness" from the Transit App blog: tracks closer than this bundle onto one
# centreline. LOOM's default (50m) over-merges genuinely-separate parallel
# infrastructure that only runs near each other — e.g. the CTA elevated Loop vs
# the Dearborn/State subways at Tower 18. 20m keeps true shared track bundled
# (the Loop's 5-6 line legs) while separating tracks that merely pass close.
AGGR="${5:-20}"
DATA="$(cd "$(dirname "$0")/.." && pwd)/data"
OUT="${KEY//:/-}-loom.json"

# Prefer the fully preprocessed zip; fall back to raw with a warning.
ZIP="gtfs-processed/$FEED.zip"
if [ ! -f "$DATA/$ZIP" ]; then
  echo "⚠ No processed zip at data/$ZIP — falling back to raw data/gtfs/$FEED.zip"
  ZIP="gtfs/$FEED.zip"
fi
[ -f "$DATA/$ZIP" ] || { echo "Missing $DATA/$ZIP — import the feed first"; exit 1; }

echo "Building LOOM graph for feed $FEED (mode: $MODE, aggr-dist ${AGGR}m) → data/$OUT"
docker run --rm -v "$DATA:/data" loom:latest \
  sh -c "gtfs2graph -m $MODE /data/$ZIP | topo -d $AGGR | loom > /data/$OUT"

echo "Loading into transit_graph_* (build_key $KEY)…"
docker exec -w /app barrelman bun run import/load-transit-graph.ts \
  --geojson "/data/$OUT" --build-key "$KEY" --feed "$FEED" --mode "$MODE" --route-type "$RT"

echo "Done. Refresh display matviews:"
echo "  docker exec -i barrelman-db psql -U barrelman -d barrelman < import/create-transit-lines-offset-zoom.sql"
echo "  docker exec -i barrelman-db psql -U barrelman -d barrelman < import/create-transit-stations.sql"
