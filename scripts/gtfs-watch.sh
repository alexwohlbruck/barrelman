#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman nightly GTFS update watcher (host wrapper)
# =============================================================================
#
# GTFS static feeds are republished by agencies on no fixed cadence, and nothing
# refreshes them automatically — download-gtfs.sh is only ever run by hand. This
# job closes that gap: it detects which imported feeds have a newer version
# upstream (via Transitland's per-feed sha1), re-imports only the regions that
# changed, then rebuilds the MOTIS dataset so the new schedules go live.
#
# Flow:
#   1. Detect drift (scripts/gtfs-watch.ts, run inside the barrelman container).
#   2. First run  -> just record the current shas as a baseline; no re-import.
#      No changes  -> exit (the common case; no expensive work).
#      Changes     -> re-import each changed region (download-gtfs.sh), record
#                     the new shas, then rebuild MOTIS (scripts/rebuild-motis.sh).
#
# Called nightly from scripts/barrelman-daily.sh, or run manually:
#   ./scripts/gtfs-watch.sh
#
# Skips silently if the barrelman container is not present.
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BARRELMAN_DIR="$(dirname "$SCRIPT_DIR")"
cd "$BARRELMAN_DIR"
[ -f "$BARRELMAN_DIR/.env" ] && { set -a; source "$BARRELMAN_DIR/.env"; set +a; }

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [gtfs-watch] $*"; }

if ! docker ps --format '{{.Names}}' | grep -q '^barrelman$'; then
  log "barrelman container not found — skipping"
  exit 0
fi
if [ -z "${TRANSITLAND_API_KEY:-}" ]; then
  log "TRANSITLAND_API_KEY not set — skipping"
  exit 0
fi

# Detection runs the repo script inside the container with the baked image, so
# no redeploy is needed to update watcher logic — just re-copy it each run.
run_watch() { # args passed through to gtfs-watch.ts (e.g. --record)
  docker cp "$SCRIPT_DIR/gtfs-watch.ts" barrelman:/tmp/gtfs-watch.ts >/dev/null
  docker exec -w /app -e TRANSITLAND_API_KEY="$TRANSITLAND_API_KEY" \
    barrelman bun /tmp/gtfs-watch.ts "$@"
}

log "Checking upstream feed versions..."
OUT="$(run_watch --check)"
echo "$OUT" | grep -v '^BASELINE=\|^CHANGED_REGIONS=' || true

BASELINE="$(echo "$OUT" | sed -n 's/^BASELINE=//p' | tail -1)"
CHANGED="$(echo "$OUT" | sed -n 's/^CHANGED_REGIONS=//p' | tail -1)"

if [ "${BASELINE:-0}" = "1" ]; then
  log "First run — recording current feed versions as baseline (no re-import)."
  run_watch --record
  log "Baseline recorded. Future runs will detect changes."
  exit 0
fi

if [ -z "${CHANGED:-}" ]; then
  log "No feed updates. Nothing to do."
  exit 0
fi

log "Updated regions: $CHANGED — re-importing."
IFS=',' read -ra REGION_LIST <<< "$CHANGED"
for region in "${REGION_LIST[@]}"; do
  [ -z "$region" ] && continue
  log "Re-importing GTFS for region: $region"
  docker exec -e GTFS_REGION="$region" -e TRANSITLAND_API_KEY="$TRANSITLAND_API_KEY" \
    barrelman bash /app/scripts/download-gtfs.sh
done

log "Recording new feed versions."
run_watch --record

log "Rebuilding MOTIS to publish the updated schedules."
"$SCRIPT_DIR/rebuild-motis.sh"

log "GTFS update complete."
