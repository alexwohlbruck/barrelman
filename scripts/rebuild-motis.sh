#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman MOTIS Dataset Rebuild
# =============================================================================
#
# Rebuild the MOTIS transit dataset (timetable + street graph) after a GTFS
# refresh. This is REQUIRED: `motis server` only SERVES the pre-built dataset
# at /data/data — it never re-imports when the config or feeds change. A plain
# `docker restart` therefore keeps serving the stale schedules, so the new feeds
# never reach riders until this runs.
#
# Steps:
#   [1/3] Regenerate config.yml from the gtfs_feeds table. Region-aware by
#         construction — it covers exactly the feeds imported for the configured
#         regions (regions.json), and points at the merged region.osm.pbf.
#   [2/3] Clean-rebuild the dataset with `motis import` from config + the merged
#         region.osm.pbf. The existing dataset is moved aside first so ALL
#         derived artifacts — timetable, street graph, stop<->street match index,
#         footpaths — rebuild together and stay consistent. (An in-place
#         incremental import can rebuild the timetable while leaving the match
#         index stale, which silently breaks routing to every stop.)
#   [3/3] Recreate the MOTIS server to serve the fresh dataset.
#
# Run after a GTFS download/import (feeds changed → timetable must rebuild):
#   ./scripts/rebuild-motis.sh
#
# The previous dataset is kept at /data/data.prev until the next run, and is
# restored automatically if the import fails.
#
# Skips silently if barrelman-motis does not exist (e.g. minimal dev setup).
# =============================================================================

# The server is stopped before the import and started again after, rather than
# recreated via `docker compose up --force-recreate`. Nothing about the
# container's definition changes — only the dataset on the shared volume — and a
# stopped container re-opens (re-mmaps) the fresh files on start. Using plain
# `docker` also keeps this runnable from barrelman-ops, which has the docker CLI
# but no compose plugin, and whose working directory would resolve to a
# different compose project name than the operator's.
CONTAINER="barrelman-motis"
MOTIS_IMAGE="ghcr.io/motis-project/motis:latest"
NETWORK="barrelman_default"
GTFS_VOL="barrelman_barrelman-gtfs-data"
OSM_VOL="barrelman_barrelman-osm-data"

log() { echo "[$(date '+%H:%M:%S')] [motis] $*"; }

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  log "container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

BARRELMAN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BARRELMAN_DIR"

# A MOTIS dataset cannot be built without a timetable: with no feeds the import
# fails either on an empty `datasets:` map or, once `osm:` is set, on
# "feature OSR_FOOTPATH requires features STREET_ROUTING and TIMETABLE".
# Checked up front because the failure otherwise happens *after* the current
# dataset has been moved aside — costing a restore and exiting non-zero — and it
# is easy to reach: importing GBFS (step 3) or rebuilding before the transit
# import (step 2) both land here.
FEED_COUNT="$(docker exec "${DB_CONTAINER:-barrelman-db}" \
  psql -U barrelman -d barrelman -tAc 'SELECT count(*) FROM gtfs_feeds' 2>/dev/null | tr -d '[:space:]')"
if [ "${FEED_COUNT:-0}" = "0" ]; then
  log "no GTFS feeds in the database — nothing to build, leaving the current dataset alone"
  log "run scripts/download-gtfs.sh first (transit step 2b)"
  exit 0
fi

# The feed ZIPs are a host bind mount, not part of the gtfs-data volume, so the
# import container needs them mounted exactly where the config expects them
# (/data/gtfs) — same bind the motis service carries in docker-compose.yml.
GTFS_ZIPS="$(docker inspect "$CONTAINER" \
  --format '{{range .Mounts}}{{if eq .Destination "/data/gtfs"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)"

# Whichever way the ZIPs arrive, confirm the import will actually SEE them, and
# do it here rather than at the import — same reason as the feed count above,
# the dataset has been moved aside by then. An instance whose motis service is
# missing the bind still resolves /data/gtfs, to a directory inside the volume,
# so the import succeeds against whatever stale copy lives there and reports a
# fresh rebuild. That is worse than failing: the operator is told the new
# schedules are live.
ZIP_COUNT="$(docker run --rm \
  -v "${GTFS_VOL}:/data" \
  ${GTFS_ZIPS:+-v "${GTFS_ZIPS}:/data/gtfs:ro"} \
  alpine sh -c 'ls /data/gtfs/*.zip 2>/dev/null | wc -l' | tr -cd '0-9')"

if [ -z "$GTFS_ZIPS" ]; then
  log "WARNING: ${CONTAINER} has no /data/gtfs bind mount — falling back to the"
  log "         copy inside the ${GTFS_VOL} volume (${ZIP_COUNT:-0} ZIPs)."
  log "         Downloads land in ./data/gtfs on the host, so this import will"
  log "         NOT see them. Add './data/gtfs:/data/gtfs:ro' to the motis"
  log "         service in docker-compose.yml and recreate it."
fi

if [ "${ZIP_COUNT:-0}" -eq 0 ]; then
  log "ERROR: no feed ZIPs visible at /data/gtfs — leaving the current dataset alone"
  log "run scripts/download-gtfs.sh first (transit step 2b)"
  exit 1
fi

# Regenerate the config and (re)build the dataset, tolerating two real
# continent-scale failure modes rather than aborting all transit:
#
#  1. Malformed feeds. One feed with a missing/invalid timezone or bad table
#     makes MOTIS abort the WHOLE import ("failed to load gtfs/<id>.zip: ...").
#     On the US corpus a handful of ~1,200 feeds are broken. We retry, dropping
#     the named feed from the config each time, until it imports — and log what
#     was dropped. (generate-motis-config also gives every dataset a
#     default_timezone, which rescues the "no timezone" class outright.)
#
#  2. MOTIS's street router (osr) caps a node at 16 ways; the US street network
#     has junctions exceeding that ("node ... has N ways, maximum is 16"), and a
#     damaged extract trips "invalid location". Street routing improves
#     access/egress on a city-sized import, but at continent scale it cannot
#     load at all. So we attempt it, and if the failure is an osr/OSM one (not a
#     feed), fall back to a timetable-only build that routes stop-to-stop over
#     the walking transfers already baked into the feeds. MOTIS_STREET_ROUTING=0
#     skips straight to timetable-only.
CFG_PATH=/var/lib/postgresql # placeholder, replaced below
GTFS_DATA_HOST="$(docker inspect "$CONTAINER" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)"
CFG="${GTFS_DATA_HOST:+$GTFS_DATA_HOST/config.yml}"

gen_config() { # $1 = street|timetable
  if [ "$1" = street ]; then
    docker exec barrelman sh -lc 'cd /app && bun run import/generate-motis-config.ts --street-routing --osm-path /osm-data/region.osm.pbf --output /gtfs-data/config.yml'
  else
    docker exec barrelman sh -lc 'cd /app && bun run import/generate-motis-config.ts --output /gtfs-data/config.yml'
  fi
}

# Remove one dataset (by feed id) from the generated config, in place.
drop_feed() {
  docker run --rm -v "${GTFS_VOL}:/d" alpine sh -c '
    awk -v bad="'"$1"'" '"'"'
      /^    "[^"]+":[[:space:]]*$/ { k=$0; sub(/^    "/,"",k); sub(/":[[:space:]]*$/,"",k); skip=(k==bad) }
      { if (!skip) print }
    '"'"' /d/config.yml > /d/config.yml.tmp && mv /d/config.yml.tmp /d/config.yml'
}

run_import() { # returns 0 on success; writes /tmp/motis-import.log
  docker run --rm --network "$NETWORK"     -v "${GTFS_VOL}:/data" -v "${OSM_VOL}:/osm-data:ro"     ${GTFS_ZIPS:+-v "${GTFS_ZIPS}:/data/gtfs:ro"}     -w /data "$MOTIS_IMAGE" /motis import > /tmp/motis-import.log 2>&1
}

echo "[$(date '+%H:%M:%S')] [1/3] [motis] Regenerating config from gtfs_feeds..."
MODE=street
[ "${MOTIS_STREET_ROUTING:-1}" = "0" ] && MODE=timetable
gen_config "$MODE"

echo "[$(date '+%H:%M:%S')] [2/3] [motis] Clean-rebuilding dataset (motis import)..."
# Move the current dataset aside so the import builds a fresh, internally
# consistent /data/data.
#
# On a healthy instance this happens while the server is still up — it keeps
# serving via the memory-mapped inodes, so there is no gap. But a MOTIS with no
# valid dataset crash-loops, and `docker exec` into a restarting container
# fails ("Container is restarting, wait until the container is running") —
# which is exactly the state a first-time install is in. So fall back to moving
# the dataset from outside once the container is stopped. Same failure mode
# rebuild-graphhopper.sh had.
MOVE='rm -rf /data/data.prev; [ -d /data/data ] && mv /data/data /data/data.prev || true'

if [ "$(docker inspect "$CONTAINER" --format '{{.State.Running}}' 2>/dev/null)" = "true" ] \
   && docker exec "$CONTAINER" sh -c "$MOVE" 2>/dev/null; then
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
else
  log "container not usable for an in-place move — stopping and moving from outside"
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  # barrelman-ops mounts the same volume at /gtfs-data; otherwise use a
  # throwaway container holding it.
  if [ -d /gtfs-data ] && [ -w /gtfs-data ]; then
    rm -rf /gtfs-data/data.prev
    [ -d /gtfs-data/data ] && mv /gtfs-data/data /gtfs-data/data.prev || true
  else
    docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c "$MOVE"
  fi
fi

MAX_FEED_DROPS="${MOTIS_MAX_FEED_DROPS:-50}"
dropped=0
import_ok=0
while : ; do
  if run_import; then import_ok=1; break; fi

  # A named feed failed to load → drop it and retry (bounded).
  bad="$(tr '\r' '\n' < /tmp/motis-import.log \
        | grep -aoiE '(failed to load|unable to import[^\n]*) gtfs/[A-Za-z0-9_.:-]+\.zip' \
        | grep -aoE 'gtfs/[A-Za-z0-9_.:-]+\.zip' | head -1 | sed 's|gtfs/||;s|\.zip||')"
  if [ -n "$bad" ]; then
    if [ "$dropped" -ge "$MAX_FEED_DROPS" ]; then
      log "ERROR: dropped $dropped malformed feeds and still failing — giving up"; break
    fi
    log "  malformed feed '$bad' — excluding it and retrying"
    drop_feed "$bad"; dropped=$((dropped + 1)); continue
  fi

  # Not a feed. If we were attempting street routing and MOTIS choked on the
  # street network (osr 16-ways cap) or a damaged extract, fall back to a
  # timetable-only build that still routes over the feeds' walking transfers.
  if [ "$MODE" = street ] && grep -qiE 'maximum is 16|invalid location|osr' /tmp/motis-import.log; then
    log "  street routing cannot load this extract (osr limit / invalid location)"
    log "  → falling back to timetable-only (stop-to-stop over precomputed transfers)"
    MODE=timetable; gen_config timetable; dropped=0; continue
  fi

  log "ERROR: motis import failed for a non-feed reason:"
  tr '\r' '\n' < /tmp/motis-import.log | grep -aiE 'unable to import|error|VERIFY FAIL' | tail -3
  break
done

if [ "$import_ok" = 1 ]; then
  [ "$dropped" -gt 0 ] && log "imported with $dropped malformed feed(s) excluded (mode: $MODE)"
  echo "[$(date '+%H:%M:%S')] [3/3] [motis] Restarting server to serve fresh dataset..."
  docker start "$CONTAINER" >/dev/null
else
  log "restoring previous dataset"
  docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c \
    'rm -rf /data/data; [ -d /data/data.prev ] && mv /data/data.prev /data/data || true'
  docker start "$CONTAINER" >/dev/null
  exit 1
fi

sleep 6
# Probe the container by name, not localhost: this script runs inside
# barrelman-ops, where :8080 is ops itself and the check could only ever fail.
# `-w` already prints 000 on a connection error, so a `|| echo` fallback would
# concatenate onto it and report "000000".
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "http://${CONTAINER}:8080/" 2>/dev/null) || true
log "server responding: HTTP ${code:-000} (404 at / is expected — the API lives under /api/v1)"
if [ "${code:-000}" = "000" ]; then
  log "WARNING: could not reach ${CONTAINER}:8080 — the dataset was rebuilt but the server is not answering"
fi
log "Rebuild complete."
