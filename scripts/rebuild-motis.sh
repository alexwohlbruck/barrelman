#!/usr/bin/env bash
#
# Rebuild MOTIS from the fully preprocessed GTFS zips.
#
# MOTIS only re-ingests data when `/motis import` runs — restarting the
# container does NOT. This script is the single supported hand-off to MOTIS:
#
#   1. docker cp data/gtfs-processed/*.zip → barrelman-motis:/data/gtfs/
#   2. docker cp motis/config.yml         → barrelman-motis:/data/config.yml
#   3. docker exec barrelman-motis /motis import -c /data/config.yml -d /data
#   4. docker restart barrelman-motis (serve the freshly imported dataset)
#
# docker cp is used deliberately: the compose bridge mount on the barrelman
# container (./data/gtfs:/gtfs-zips) exposes only the RAW zips, so copying
# straight into the motis container's barrelman-gtfs-data volume needs no
# compose changes and can never pick up unprocessed feeds.
#
# The config's dataset paths ("gtfs/<feed_id>.zip") resolve against /data
# inside the container, matching the destination above.
#
# Usage:
#   scripts/rebuild-motis.sh
#
# Environment:
#   MOTIS_CONTAINER     - motis container name (default: barrelman-motis)
#   GTFS_PROCESSED_DIR  - processed zip dir (default: ./data/gtfs-processed)
#   MOTIS_CONFIG        - config path (default: ./motis/config.yml)
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONTAINER="${MOTIS_CONTAINER:-barrelman-motis}"
PROCESSED_DIR="${GTFS_PROCESSED_DIR:-$PROJECT_DIR/data/gtfs-processed}"
CONFIG="${MOTIS_CONFIG:-$PROJECT_DIR/motis/config.yml}"

# ── Preflight ────────────────────────────────────────────────────────
[ -d "$PROCESSED_DIR" ] || {
  echo "✗ Missing $PROCESSED_DIR — run the import pipeline first:"
  echo "    bun run import/import-gtfs.ts …"
  exit 1
}
[ -f "$CONFIG" ] || {
  echo "✗ Missing $CONFIG — the import pipeline generates it"
  exit 1
}

shopt -s nullglob
ZIPS=("$PROCESSED_DIR"/*.zip)
shopt -u nullglob
[ "${#ZIPS[@]}" -gt 0 ] || {
  echo "✗ No processed zips in $PROCESSED_DIR — run the import pipeline first"
  exit 1
}

docker inspect "$CONTAINER" >/dev/null 2>&1 || {
  echo "✗ Container '$CONTAINER' not found — is docker compose up?"
  exit 1
}

# ── 1+2. Sync processed zips + config into the gtfs-data volume ─────
echo "==> Syncing ${#ZIPS[@]} processed zip(s) → $CONTAINER:/data/gtfs/"
docker exec "$CONTAINER" mkdir -p /data/gtfs 2>/dev/null || true
docker cp "$PROCESSED_DIR/." "$CONTAINER:/data/gtfs/"

echo "==> Syncing $CONFIG → $CONTAINER:/data/config.yml"
docker cp "$CONFIG" "$CONTAINER:/data/config.yml"

# ── 3. Ingest ────────────────────────────────────────────────────────
echo "==> Running MOTIS import (ingests all timetables — may take a while)"
docker exec "$CONTAINER" /motis import -c /data/config.yml -d /data

# ── 4. Serve the new dataset ─────────────────────────────────────────
echo "==> Restarting $CONTAINER"
docker restart "$CONTAINER"

echo "✓ MOTIS rebuilt from $PROCESSED_DIR (${#ZIPS[@]} feeds)"
