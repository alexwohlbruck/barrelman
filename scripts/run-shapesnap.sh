#!/usr/bin/env bash
#
# Run shapesnap (the GTFS→OSM shape rewrite) standalone on one processed feed.
#
# The import pipeline runs this automatically for feeds enabled in
# config/shapesnap.json (applyShapeRewrite in import/import-gtfs.ts); this
# script is the manual/debug entry point for a single feed. It matches every
# pattern of the feed's configured modes onto the cached OSM mode graph and
# rewrites data/gtfs-processed/<feedId>.zip IN PLACE (shapes.txt, trips.txt
# shape_id remap, shape_dist_traveled), writing matched_shapes/shapesnap_runs
# metadata to PostGIS. See docs/shapesnap.md.
#
# After a rewrite, hand the zip to MOTIS with scripts/rebuild-motis.sh.
#
# Usage:
#   scripts/run-shapesnap.sh <feedId> [extra shapesnap.run args]
#   scripts/run-shapesnap.sh 29
#   scripts/run-shapesnap.sh 29 --dry-run
#   scripts/run-shapesnap.sh 29 --modes rail --routes Brn,Blue
#
# Environment:
#   SHAPESNAP_ZIP - zip to rewrite (default: ./data/gtfs-processed/<feedId>.zip)
#   DATABASE_URL  - PostGIS DSN for metadata (default: the dev DB)
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FEED="${1:?feedId required (e.g. 29)}"
shift || true
ZIP="${SHAPESNAP_ZIP:-$PROJECT_DIR/data/gtfs-processed/$FEED.zip}"

# ── Preflight ────────────────────────────────────────────────────────
command -v uv >/dev/null 2>&1 || {
  echo "✗ uv not found — install it first (brew install uv)"
  exit 1
}
[ -f "$PROJECT_DIR/config/shapesnap.json" ] || {
  echo "✗ Missing config/shapesnap.json"
  exit 1
}
[ -f "$ZIP" ] || {
  echo "✗ Missing $ZIP — produce it with the import pipeline first:"
  echo "    bun run import/import-gtfs.ts --skip-download …"
  echo "  or seed it from the sanitized raw zip:"
  echo "    cp data/gtfs/$FEED.zip data/gtfs-processed/$FEED.zip"
  exit 1
}

cd "$PROJECT_DIR"
exec uv run --with-requirements shapesnap/requirements.txt \
  python -m shapesnap.run --feed "$FEED" --zip "$ZIP" "$@"
