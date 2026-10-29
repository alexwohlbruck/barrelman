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
#  2. Street routing. MOTIS's street router (osr) aborts on a way with a
#     missing node ("invalid location") or a node on more than 16 ways. Both
#     are repaired up front (prepare_street_extract below). If street routing
#     still won't build, fall back to a timetable-only build that routes
#     stop-to-stop over the walking transfers baked into the feeds, so transit
#     stays up, and exit 2 so the job shows as failed: that build answers no
#     bikeshare, bike or park-and-ride trip. MOTIS_STREET_ROUTING=0 skips
#     straight to timetable-only, deliberately.
CFG_PATH=/var/lib/postgresql # placeholder, replaced below
GTFS_DATA_HOST="$(docker inspect "$CONTAINER" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)"
CFG="${GTFS_DATA_HOST:+$GTFS_DATA_HOST/config.yml}"

# The extract MOTIS builds its street graph from, as the import container sees
# it. osr aborts the whole import on a way with a missing node ("invalid
# location", which replication leaves in a merged extract) or a node on more
# than 16 ways (the US has a few), and the rebuild then falls back to
# timetable-only. scripts/repair-motis-extract.sh writes a copy without those
# ways into the gtfs-data volume, which the import mounts at /data;
# region.osm.pbf itself is left alone for GraphHopper, osm2pgsql and tiles.
# Runs from barrelman-ops, which mounts both volumes and carries osmium.
STREET_OSM=/osm-data/region.osm.pbf
REPAIRED=/gtfs-data/region-motis-repaired.osm.pbf
# Ways dropped at over-full nodes, kept between runs: finding them is a full
# count of the extract, and the junctions don't move.
EXCESS_IDS=/gtfs-data/motis-osr-excess-ways.ids

prepare_street_extract() { # $1 = --recount to redo the over-full node count
  local src="${OSM_DATA_DIR:-/data}/region.osm.pbf" dropped
  if ! command -v osmium >/dev/null || [ ! -d /gtfs-data ] || [ ! -f "$src" ]; then
    log "not running in barrelman-ops — using region.osm.pbf without repair"
    return
  fi
  log "checking region.osm.pbf for ways osr can't load${1:+ (recounting over-full nodes — slow)}..."
  dropped="$(bash "$BARRELMAN_DIR/scripts/repair-motis-extract.sh" "$src" "$REPAIRED" "$EXCESS_IDS" ${1:-})"
  if [ "${dropped:-0}" -gt 0 ]; then
    log "  building MOTIS from region-motis-repaired.osm.pbf ($dropped way(s) left out)"
    STREET_OSM=/data/region-motis-repaired.osm.pbf
  else
    rm -f "$REPAIRED"
    STREET_OSM=/osm-data/region.osm.pbf
  fi
}

gen_config() { # $1 = street|timetable
  if [ "$1" = street ]; then
    docker exec barrelman sh -lc "cd /app && bun run import/generate-motis-config.ts --street-routing --osm-path $STREET_OSM --output /gtfs-data/config.yml"
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
WANTED_MODE="$MODE"
RECOUNTED=0
FALLBACK_REASON=""
[ "$MODE" = street ] && prepare_street_extract
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
#
# Only a complete dataset (one with a timetable, tt.bin) replaces data.prev. A
# failed import leaves a partial /data/data behind, and rotating that over the
# last good dataset would leave nothing to restore — the next failed run would
# then take transit down for good.
rotate() { # $1 = the volume's mount point
  echo "if [ -f $1/data/tt.bin ]; then rm -rf $1/data.prev && mv $1/data $1/data.prev; else rm -rf $1/data; fi"
}

if [ "$(docker inspect "$CONTAINER" --format '{{.State.Running}}' 2>/dev/null)" = "true" ] \
   && docker exec "$CONTAINER" sh -c "$(rotate /data)" 2>/dev/null; then
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
else
  log "container not usable for an in-place move — stopping and moving from outside"
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  # barrelman-ops mounts the same volume at /gtfs-data; otherwise use a
  # throwaway container holding it.
  if [ -d /gtfs-data ] && [ -w /gtfs-data ]; then
    sh -c "$(rotate /gtfs-data)"
  else
    docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c "$(rotate /data)"
  fi
fi

# From here the server is stopped, so every way out of the script — including
# an unexpected `set -e` abort — has to put a dataset back and start it again.
restore_previous() {
  log "restoring previous dataset"
  docker run --rm -v "${GTFS_VOL}:/data" alpine sh -c \
    'rm -rf /data/data; [ -d /data/data.prev ] && mv /data/data.prev /data/data || true' || true
  docker start "$CONTAINER" >/dev/null || true
}
trap restore_previous EXIT

MAX_FEED_DROPS="${MOTIS_MAX_FEED_DROPS:-50}"
dropped=0
import_ok=0
while : ; do
  rc=0; run_import || rc=$?
  [ "$rc" = 0 ] && { import_ok=1; break; }

  # A named feed failed to load → drop it and retry (bounded). `|| true`
  # because under pipefail a grep that matches nothing fails the assignment,
  # and errexit would then abort before the street-routing fallback below.
  bad="$(tr '\r' '\n' < /tmp/motis-import.log \
        | grep -aoiE '(failed to load|unable to import[^\n]*) gtfs/[A-Za-z0-9_.:-]+\.zip' \
        | grep -aoE 'gtfs/[A-Za-z0-9_.:-]+\.zip' | head -1 | sed 's|gtfs/||;s|\.zip||' || true)"
  if [ -n "$bad" ]; then
    if [ "$dropped" -ge "$MAX_FEED_DROPS" ]; then
      log "ERROR: dropped $dropped malformed feeds and still failing — giving up"; break
    fi
    log "  malformed feed '$bad' — excluding it and retrying"
    drop_feed "$bad"; dropped=$((dropped + 1)); continue
  fi

  # Not a feed. If we were attempting street routing, keep transit up with a
  # timetable-only build that routes stop to stop over the feeds' walking
  # transfers — but name the cause, and fail the job at the end (below), since
  # that build silently drops every bikeshare, bike and park-and-ride trip.
  # This used to match any log line mentioning "osr", which is most of a street
  # import's progress output, so an out-of-memory kill or a damaged extract
  # read as "the 16-ways cap" and nobody looked.
  if [ "$MODE" = street ]; then
    if [ "$rc" = 137 ]; then
      FALLBACK_REASON="the street import was killed (exit 137 — out of memory?)"
    elif grep -qaE 'maximum is 16' /tmp/motis-import.log && [ "$RECOUNTED" = 0 ]; then
      # New over-full junctions since the saved list (or no list yet). Count
      # again and retry once with those ways left out too.
      log "  $(tr '\r' '\n' < /tmp/motis-import.log | grep -aoE 'node [0-9]+ \(osm=[0-9]+\) has [0-9]+ ways, maximum is 16' | head -1)"
      RECOUNTED=1; prepare_street_extract --recount; gen_config street; continue
    elif grep -qaE 'maximum is 16' /tmp/motis-import.log; then
      FALLBACK_REASON="a junction still exceeds osr's 16-ways-per-node cap after recounting ($(tr '\r' '\n' < /tmp/motis-import.log | grep -aoE 'node [0-9]+ \(osm=[0-9]+\) has [0-9]+ ways' | head -1))"
    else
      FALLBACK_REASON="$(tr '\r' '\n' < /tmp/motis-import.log \
        | grep -aiE 'unable to import|VERIFY FAIL|what\(\)|error' | tail -1 || true)"
      FALLBACK_REASON="${FALLBACK_REASON:-the street import failed with exit $rc}"
    fi
    log "  street routing failed: $FALLBACK_REASON"
    log "  → falling back to timetable-only (stop-to-stop over precomputed transfers)"
    rm -f "$REPAIRED"
    MODE=timetable; gen_config timetable; dropped=0; continue
  fi

  log "ERROR: motis import failed for a non-feed reason:"
  tr '\r' '\n' < /tmp/motis-import.log | grep -aiE 'unable to import|error|VERIFY FAIL' | tail -3
  break
done

if [ "$import_ok" != 1 ]; then
  exit 1 # the EXIT trap restores the previous dataset
fi

trap - EXIT
[ "$dropped" -gt 0 ] && log "imported with $dropped malformed feed(s) excluded (mode: $MODE)"
echo "[$(date '+%H:%M:%S')] [3/3] [motis] Restarting server to serve fresh dataset..."
docker start "$CONTAINER" >/dev/null

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

if [ "$WANTED_MODE" = street ] && [ "$MODE" = timetable ]; then
  log "WARNING: MOTIS is serving a timetable-only dataset because $FALLBACK_REASON."
  log "  Walking transit works; bikeshare, scooter, bike-to-station, bike-on-board and"
  log "  park-and-ride trips return nothing until street routing builds again."
  log "  Exiting non-zero so this shows as a failed job. Set MOTIS_STREET_ROUTING=0"
  log "  to choose timetable-only deliberately."
  exit 2
fi
log "Rebuild complete."
