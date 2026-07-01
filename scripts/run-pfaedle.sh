#!/usr/bin/env bash
# Opt-in GTFS shape cleaning via pfaedle (OSM map-matching).
#
# Map-matches a feed's trips onto OSM ways to produce an accurate shapes.txt.
# Most valuable for feeds that publish NO or poor shapes (many bus agencies);
# feeds with good shapes (e.g. NYC subway) gain little, and the subway is
# already topology-cleaned by LOOM, so this is deliberately opt-in per feed.
#
# pfaedle only computes shapes for trips that LACK them by default. The corrected
# feed is written to data/pfaedle-out/<feedId>/; to use it, inject its shapes.txt
# into the canonical data/gtfs/<feedId>.zip BEFORE the import + build-transit-graph
# steps (so both MOTIS routing and the LOOM display graph derive from one source).
#
# Usage:
#   scripts/run-pfaedle.sh <feedId> [mode]   mode = bus|subway|rail|tram|ferry|all
#
# Requires data/region.osm.pbf covering the feed's area.
set -euo pipefail

FEED="${1:?feedId required (e.g. 886 for CATS)}"
MODE="${2:-all}"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"

[ -f "$DATA_DIR/region.osm.pbf" ] || { echo "Missing $DATA_DIR/region.osm.pbf"; exit 1; }
[ -f "$DATA_DIR/gtfs/$FEED.zip" ] || { echo "Missing $DATA_DIR/gtfs/$FEED.zip"; exit 1; }

mkdir -p "$DATA_DIR/pfaedle-out/$FEED"
echo "pfaedle map-matching feed $FEED (mode: $MODE)…"
# The image ENTRYPOINT is already `pfaedle`, so pass ONLY its args (a second
# `pfaedle` here is parsed as a positional GTFS feed → "Multiple feeds" error).
# -D drops the feed's existing shapes and recomputes them (map-match all trips);
# without it, feeds that already have shapes (e.g. subway) are a no-op.
docker run --rm -v "$DATA_DIR:/data" ghcr.io/ad-freiburg/pfaedle:latest \
  -D -x /data/region.osm.pbf -m "$MODE" \
  -o "/data/pfaedle-out/$FEED" "/data/gtfs/$FEED.zip"

echo "Done → data/pfaedle-out/$FEED/"
echo "Next: inject its shapes.txt into data/gtfs/$FEED.zip, then re-run"
echo "  bun run import/import-gtfs.ts --skip-download  (re-import shapes)"
echo "  scripts/build-transit-graph.sh $FEED           (rebuild LOOM graph)"
