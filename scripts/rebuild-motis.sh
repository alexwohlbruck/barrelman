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
#   [2/3] Clean-build a new dataset into /data/data.next with `motis import`.
#         By default the server is stopped for the import, as it always was;
#         MOTIS_BUILD_ALONGSIDE keeps it serving. Building from empty keeps
#         ALL derived artifacts — timetable, street graph, stop<->street match
#         index, footpaths — consistent. (An in-place incremental import can
#         rebuild the timetable while leaving the match index stale, which
#         silently breaks routing to every stop.)
#   [3/3] Verify data.next (the import exited 0, tt.bin is there, the import's
#         timetable_metrics.json counts trips, and optionally a trial server
#         loads it), then swap: data -> data.prev, data.next -> data, and
#         restart the server. Built alongside, transit is down only for that
#         restart.
#
# Run after a GTFS download/import (feeds changed → timetable must rebuild):
#   ./scripts/rebuild-motis.sh
#
# Nothing touches data/ or data.prev until the new dataset has passed those
# checks. A failed import only leaves data.next behind, and it is deleted. An
# earlier version moved data/ aside before importing, so each failed night
# rotated a partial dataset over data.prev until no good dataset was left.
#
# Flags:
#   --rollback          swap data and data.prev and restart; no build
#   --verify-server     load the new dataset in a trial server before the swap
#   --no-verify-server  never start the trial server
#   --stop-server       stop the server during the import, whatever
#                       MOTIS_BUILD_ALONGSIDE says
#
# Environment:
#   MOTIS_BUILD_ALONGSIDE false (default) | true | auto. Whether the live
#                         server keeps serving during the import. An import
#                         peaks at 5-8 GB beside a live server of about 5 GB
#                         on a US dataset, which has OOM-killed MOTIS, so auto
#                         builds alongside only with 8 GB plus a 4 GB margin
#                         available
#   MOTIS_VERIFY_SERVER   auto (default) | 1 | 0. auto starts the trial server
#                         when available memory exceeds the dataset's size
#                         plus 1 GB
#
# Disk: the new dataset sits beside the current one until the swap, so the
# volume needs about 1.2x the current dataset free (data.prev is released if
# that is enough and the current dataset is serving).
#
# Skips silently if barrelman-motis does not exist (e.g. minimal dev setup).
# =============================================================================

# The server is stopped and started again for the swap, rather than recreated
# via `docker compose up --force-recreate`. Nothing about the container's
# definition changes — only the dataset on the shared volume — and a stopped
# container re-opens (re-mmaps) the fresh files on start. Using plain `docker`
# also keeps this runnable from barrelman-ops, which has the docker CLI but no
# compose plugin, and whose working directory would resolve to a different
# compose project name than the operator's.
CONTAINER="barrelman-motis"
TRIAL="barrelman-motis-verify"
NETWORK="barrelman_default"
GTFS_VOL="barrelman_barrelman-gtfs-data"
OSM_VOL="barrelman_barrelman-osm-data"

log() { echo "[$(date '+%H:%M:%S')] [motis] $*"; }

# shellcheck source=staged-rebuild-lib.sh
. "$(dirname "$0")/staged-rebuild-lib.sh"

ROLLBACK=0
FORCE_STOP=0
ALONGSIDE="${MOTIS_BUILD_ALONGSIDE:-false}"
VERIFY="${MOTIS_VERIFY_SERVER:-auto}"
IMPORT="barrelman-motis-import"
for arg in "$@"; do
  case "$arg" in
    --rollback) ROLLBACK=1 ;;
    --verify-server) VERIFY=1 ;;
    --no-verify-server) VERIFY=0 ;;
    --stop-server) FORCE_STOP=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  log "container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

# Files on the gtfs-data volume, with paths relative to its root. barrelman-ops
# mounts it at /gtfs-data; elsewhere a throwaway container holds it.
if [ -d /gtfs-data ] && [ -w /gtfs-data ]; then
  vol_sh() { (cd /gtfs-data && sh -c "$1"); }
else
  vol_sh() { docker run --rm -w /data -v "${GTFS_VOL}:/data" alpine sh -c "$1"; }
fi
size_kb() { vol_sh "du -sk $1 2>/dev/null | cut -f1" | tr -cd '0-9'; }
has_tt() { vol_sh "[ -s $1/tt.bin ]"; }

# Whether a MOTIS container answers HTTP. Asked from inside the container
# (busybox wget), so it works the same from ops and from a host shell, where
# docker-proxy would accept a connection to the published port with nothing
# behind it. Any status counts: / is a 404, the API lives under /api/v1.
# Captured rather than piped to grep: wget exits 1 on that 404, which under
# pipefail would fail the pipeline even when grep matched.
answers() {
  local out
  out="$(docker exec "$1" wget -q -S -O /dev/null http://127.0.0.1:8080/ 2>&1 || true)"
  case "$out" in *HTTP/*) return 0 ;; *) return 1 ;; esac
}

START_TIMEOUT=600 # seconds for a restarted server to answer
wait_answers() { # $1 container, $2 seconds
  local deadline=$(( $(date +%s) + $2 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    answers "$1" && return 0
    sleep 5
  done
  return 1
}
restart_serving() {
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  docker start "$CONTAINER" >/dev/null
  wait_answers "$CONTAINER" "$START_TIMEOUT"
}

if [ "$ROLLBACK" = 1 ]; then
  if ! has_tt data.prev; then
    log "ERROR: no complete dataset at /data/data.prev to roll back to" >&2
    exit 1
  fi
  log "Rolling back: swapping data and data.prev..."
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  # A data.swap left by a rollback killed halfway is the live dataset if data
  # is missing, and stale otherwise; either way it must not stay, or `mv data
  # data.swap` would nest the live dataset inside it.
  vol_sh 'set -e
    if [ -d data.swap ]; then if [ -d data ]; then rm -rf data.swap; else mv data.swap data; fi; fi
    if [ -d data ]; then mv data data.swap; fi
    mv data.prev data
    if [ -d data.swap ]; then mv data.swap data.prev; fi'
  if restart_serving; then
    log "Serving the previous dataset. Run --rollback again to return to the one it replaced."
    exit 0
  fi
  log "ERROR: MOTIS did not answer on the rolled-back dataset within ${START_TIMEOUT}s. Check: docker logs ${CONTAINER}" >&2
  exit 1
fi

# Import with the image the server runs. The dataset is written in the engine's
# own binary format, so an import by any other version leaves a timetable the
# server refuses to load. The image ID is used, not the name it was created
# from: a name like motis:latest may point at a newer pull than the one running.
MOTIS_IMAGE="$(docker inspect -f '{{.Image}}' "$CONTAINER")"

BARRELMAN_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BARRELMAN_DIR"

# A MOTIS dataset cannot be built without a timetable: with no feeds the import
# fails either on an empty `datasets:` map or, once `osm:` is set, on
# "feature OSR_FOOTPATH requires features STREET_ROUTING and TIMETABLE".
# Checked up front so the job exits cleanly instead of failing an import, and
# it is easy to reach: importing GBFS (step 3) or rebuilding before the transit
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
# do it here rather than at the import. An instance whose motis service is
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
#     straight to timetable-only, deliberately. MOTIS_STREET_ROUTING=gbfs
#     builds the street graph only around the bikeshare systems MOTIS polls,
#     for an extract too large to build whole beside the rest of the stack.
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
# MOTIS_STREET_ROUTING=gbfs: the extract cut down to the bikeshare areas first.
# MOTIS only routes over its own street graph for rentals (barrelman composes
# every other walk, bike and drive to a station over GraphHopper), so outside
# those areas the graph is dead weight, and a continent's worth of it needs
# more memory than a box running the rest of the stack has spare.
CLIPPED=/gtfs-data/region-motis-clipped.osm.pbf
STREET_AREA=/gtfs-data/motis-street-area.geojson
# Ways dropped at over-full nodes, kept between runs: finding them is a full
# count of the extract, and the junctions don't move.
EXCESS_IDS=/gtfs-data/motis-osr-excess-ways.ids

prepare_street_extract() { # $1 = --recount to redo the over-full node count
  local src="${OSM_DATA_DIR:-/data}/region.osm.pbf" dropped
  if ! command -v osmium >/dev/null || [ ! -d /gtfs-data ] || [ ! -f "$src" ]; then
    [ "$CLIP" = 1 ] && { log "ERROR: MOTIS_STREET_ROUTING=gbfs needs osmium — run this from barrelman-ops"; exit 1; }
    log "not running in barrelman-ops — using region.osm.pbf without repair"
    return
  fi
  if [ "$CLIP" = 1 ] && [ -n "${1:-}" ] && [ -f "$CLIPPED" ]; then
    src="$CLIPPED" # recounting: the cut from this run is still current
  elif [ "$CLIP" = 1 ]; then
    local rc=0
    docker exec barrelman sh -lc "cd /app && bun run import/generate-motis-config.ts --street-area $STREET_AREA" || rc=$?
    if [ "$rc" = 3 ]; then
      # No bikeshare to serve, so no street graph to build.
      log "  no bikeshare system in this region — building timetable-only"
      MODE=timetable; WANTED_MODE=timetable; return
    fi
    [ "$rc" = 0 ] || { log "ERROR: could not compute the bikeshare street area"; exit 1; }
    log "cutting region.osm.pbf down to the bikeshare areas..."
    # complete_ways keeps every node of a way that crosses the edge, so the cut
    # itself leaves no way pointing at a missing node.
    osmium extract --overwrite -s complete_ways -p "$STREET_AREA" -o "$CLIPPED.partial" -f pbf "$src" \
      || { log "ERROR: osmium extract failed"; exit 1; }
    mv -f "$CLIPPED.partial" "$CLIPPED"
    log "  $(du -h "$CLIPPED" | cut -f1) of $(du -h "$src" | cut -f1)"
    src="$CLIPPED"
  fi
  log "checking $(basename "$src") for ways osr can't load${1:+ (recounting over-full nodes — slow)}..."
  dropped="$(bash "$BARRELMAN_DIR/scripts/repair-motis-extract.sh" "$src" "$REPAIRED" "$EXCESS_IDS" ${1:-})"
  if [ "${dropped:-0}" -gt 0 ]; then
    log "  building MOTIS from region-motis-repaired.osm.pbf ($dropped way(s) left out)"
    STREET_OSM=/data/region-motis-repaired.osm.pbf
  else
    rm -f "$REPAIRED"
    STREET_OSM=/osm-data/region.osm.pbf
    if [ "$CLIP" = 1 ]; then STREET_OSM=/data/region-motis-clipped.osm.pbf; fi
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
  docker run --rm --name "$IMPORT" --network "$NETWORK" \
    -v "${GTFS_VOL}:/data" -v "${OSM_VOL}:/osm-data:ro" \
    ${GTFS_ZIPS:+-v "${GTFS_ZIPS}:/data/gtfs:ro"} \
    -w /data "$MOTIS_IMAGE" /motis import -c config.yml -d data.next > /tmp/motis-import.log 2>&1
}

echo "[$(date '+%H:%M:%S')] [1/3] [motis] Regenerating config from gtfs_feeds..."
MODE=street
CLIP=0
case "${MOTIS_STREET_ROUTING:-1}" in
  0) MODE=timetable ;;
  gbfs) CLIP=1 ;;
esac
WANTED_MODE="$MODE"
RECOUNTED=0
FALLBACK_REASON=""
if [ "$MODE" = street ]; then prepare_street_extract; fi
gen_config "$MODE"

echo "[$(date '+%H:%M:%S')] [2/3] [motis] Clean-building a new dataset into data.next (motis import)..."

# Room for a second dataset beside the live one. "Serving" decides whether
# data.prev may be released to make room: if the live server is down, data.prev
# may be the only good dataset left, and it is kept.
SERVING=0
answers "$CONTAINER" && SERVING=1
CURRENT_KB="$(size_kb data)"; CURRENT_KB="${CURRENT_KB:-0}"
PREV_KB="$(size_kb data.prev)"; PREV_KB="${PREV_KB:-0}"
# data.next (an interrupted run) and data.failed (a swap that would not start)
# are deleted below, so they count as free.
STALE_KB="$(size_kb data.next)"; FAILED_KB="$(size_kb data.failed)"
FREE_KB="$(vol_sh 'df -Pk . | awk "NR == 2 { print \$4 }"' | tr -cd '0-9')"
FREE_KB=$(( ${FREE_KB:-0} + ${STALE_KB:-0} + ${FAILED_KB:-0} ))
# A first build has no size to go on; 1 GB covers a metro area's timetable.
NEED_KB="$(staged_need_kb "$CURRENT_KB" 1048576)"
log "  volume: $(staged_human_kb "$FREE_KB") free; current dataset $(staged_human_kb "$CURRENT_KB"), previous $(staged_human_kb "$PREV_KB"); a new one needs about $(staged_human_kb "$NEED_KB")"
case "$(staged_disk_decision "$FREE_KB" "$NEED_KB" "$PREV_KB" "$SERVING")" in
  refuse)
    log "ERROR: not enough free space on the gtfs-data volume to build a new dataset beside the current one." >&2
    log "  Needed: about $(staged_human_kb "$NEED_KB") (1.2x the current dataset). Free: $(staged_human_kb "$FREE_KB")." >&2
    if [ "$PREV_KB" -gt 0 ] && [ "$SERVING" != 1 ]; then
      log "  data.prev was kept because MOTIS is not serving; it may be the only good dataset. Try --rollback." >&2
    fi
    exit 1 ;;
  drop-prev)
    log "  releasing data.prev ($(staged_human_kb "$PREV_KB")) to make room; the live dataset is serving"
    vol_sh 'rm -rf data.prev' ;;
esac
HEAVY="$(heavy_sql_running)"
if [ -n "$HEAVY" ]; then
  log "  WARNING: long-running SQL in barrelman-db. The import and the live server share memory with it:"
  printf '%s\n' "$HEAVY" | sed 's/^/      /'
fi

# An import container still running under the fixed name was orphaned by a
# killed run (SIGKILL skips the cleanup below) and is still writing into
# data.next, and would double the memory if a second one started beside it.
if [ "$(docker inspect -f '{{.State.Running}}' "$IMPORT" 2>/dev/null)" = "true" ]; then
  log "ERROR: an import container ($IMPORT) is already running. Another rebuild is in progress," >&2
  log "  or one was killed and left it behind: docker rm -f $IMPORT once you are sure." >&2
  exit 1
fi
docker rm -f "$IMPORT" "$TRIAL" >/dev/null 2>&1 || true
vol_sh 'rm -rf data.next data.failed'

# Every way out before the swap (a failed import, a failed check, an
# unexpected `set -e` abort, a cancelled job) leaves the live dataset alone,
# deletes the unverified one and makes sure the server is running.
SWAPPED=0
cleanup() {
  docker rm -f "$IMPORT" "$TRIAL" >/dev/null 2>&1 || true
  if [ "$SWAPPED" != 1 ]; then
    vol_sh 'rm -rf data.next' || true
    if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != true ]; then
      log "starting the server again on the dataset it had"
      docker start "$CONTAINER" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM INT

# Alongside or not. 8 GB is the top of what a US import was measured to peak
# at; a metro dataset needs far less, so `auto` errs toward stopping.
if [ "$FORCE_STOP" = 1 ]; then
  MODE_DECISION="stop: --stop-server"
else
  MODE_DECISION="$(staged_alongside_decision "$ALONGSIDE" "$(mem_available_kb)" $((8 * 1048576)))"
fi
if [ "$MODE_DECISION" = alongside ]; then
  log "  the server keeps serving the current dataset during the import (MOTIS_BUILD_ALONGSIDE=$ALONGSIDE)"
else
  log "  importing with the server stopped (${MODE_DECISION#stop: }); transit is down until the swap"
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
fi

MAX_FEED_DROPS="${MOTIS_MAX_FEED_DROPS:-50}"
dropped=0
import_ok=0
while : ; do
  rc=0; run_import || rc=$?
  [ "$rc" = 0 ] && { import_ok=1; break; }

  # A named feed failed to load → drop it and retry (bounded). `|| true`
  # because under pipefail a grep that matches nothing fails the assignment,
  # and errexit would then abort before the street-routing fallback below.
  # `.*`, not `[^\n]*`: inside a bracket that excludes the letter n, so
  # "unable to import: no loader for gtfs/67.zip found" (a zip with no GTFS
  # tables) never matched and read as a street-routing failure.
  bad="$(tr '\r' '\n' < /tmp/motis-import.log \
        | grep -aoiE '(failed to load|unable to import.*) gtfs/[A-Za-z0-9_.:-]+\.zip' \
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
    rm -f "$REPAIRED" "$CLIPPED"
    MODE=timetable; gen_config timetable; dropped=0; continue
  fi

  log "ERROR: motis import failed for a non-feed reason:"
  tr '\r' '\n' < /tmp/motis-import.log | grep -aiE 'unable to import|error|VERIFY FAIL' | tail -3
  break
done

if [ "$import_ok" != 1 ]; then
  log "the live dataset is untouched; the partial data.next is deleted"
  exit 1
fi
[ "$dropped" -gt 0 ] && log "imported with $dropped malformed feed(s) excluded (mode: $MODE)"

echo "[$(date '+%H:%M:%S')] [3/3] [motis] Verifying data.next and swapping it in..."
# The import's exit status is not enough on its own: an import that ran out of
# feeds to load, or crashed after writing the street graph, can leave a
# dataset with no timetable, and serving that is what took transit down for
# three days in October 2026.
if ! has_tt data.next; then
  log "ERROR: the import exited 0 but data.next has no tt.bin; the live dataset is untouched." >&2
  exit 1
fi
TRIPS="$(vol_sh 'cat data.next/timetable_metrics.json 2>/dev/null' > /tmp/motis-metrics.json; motis_metrics_trips /tmp/motis-metrics.json)"
if [ "${TRIPS:-0}" -le 0 ]; then
  log "ERROR: the import's timetable_metrics.json reports no trips; the live dataset is untouched." >&2
  exit 1
fi
NEW_KB="$(size_kb data.next)"; NEW_KB="${NEW_KB:-0}"
log "  tt.bin present, $TRIPS trips across all feeds, $(staged_human_kb "$NEW_KB")"

# Optionally load the dataset in a second MOTIS. This catches a dataset the
# server rejects at load time (a version mismatch, a damaged file) while the
# live one still serves. The trial loads the whole dataset into memory beside
# the live server, so auto only runs it when that fits.
TRIAL_DECISION="$(staged_trial_decision "$VERIFY" "$(mem_total_kb)" 0 "$(mem_available_kb)" $(( NEW_KB + 1048576 )))"
if [ "$TRIAL_DECISION" = run ]; then
  log "  starting a trial server on data.next..."
  docker run -d --name "$TRIAL" --network "$NETWORK" \
    -v "${GTFS_VOL}:/data" -v "${OSM_VOL}:/osm-data:ro" \
    ${GTFS_ZIPS:+-v "${GTFS_ZIPS}:/data/gtfs:ro"} \
    -w /data "$MOTIS_IMAGE" /motis server -d data.next --log-level info >/dev/null
  if ! wait_answers "$TRIAL" 600; then
    log "ERROR: the trial server did not load data.next; the live dataset is untouched." >&2
    docker logs --tail 15 "$TRIAL" >&2 2>&1 || true
    exit 1
  fi
  # Any stop at all, anywhere: the stop index is the first thing a request uses.
  STOPS="$(docker exec "$TRIAL" wget -q -O - 'http://127.0.0.1:8080/api/v1/map/stops?min=-90,-180&max=90,180' 2>/dev/null | head -c 2 || true)"
  if [ "$STOPS" != '[{' ]; then
    log "ERROR: the trial server loaded data.next but returns no stops; the live dataset is untouched." >&2
    exit 1
  fi
  log "  trial server loaded data.next and returns stops"
  docker rm -f "$TRIAL" >/dev/null 2>&1 || true
else
  log "  trial server ${TRIAL_DECISION} (--verify-server forces it)"
fi

# The dataset being replaced becomes data.prev only if it has a timetable; a
# partial one is deleted, so it never overwrites a good data.prev.
docker stop "$CONTAINER" >/dev/null 2>&1 || true
vol_sh 'set -e
  if [ -s data/tt.bin ]; then rm -rf data.prev; mv data data.prev; else rm -rf data; fi
  mv data.next data'
SWAPPED=1
if restart_serving; then
  log "server answering on the new dataset; the previous one is kept at /data/data.prev (--rollback to return to it)"
else
  log "ERROR: MOTIS did not answer on the new dataset within ${START_TIMEOUT}s." >&2
  docker logs --tail 15 "$CONTAINER" >&2 2>&1 || true
  if has_tt data.prev; then
    log "  putting the previous dataset back; the new one is kept at /data/data.failed" >&2
    docker stop "$CONTAINER" >/dev/null 2>&1 || true
    vol_sh 'set -e; rm -rf data.failed; mv data data.failed; mv data.prev data'
    restart_serving || log "  the previous dataset did not start either. Check: docker logs ${CONTAINER}" >&2
  fi
  exit 1
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
