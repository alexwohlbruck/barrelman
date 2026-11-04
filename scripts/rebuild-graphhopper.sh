#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman GraphHopper Graph Rebuild
# =============================================================================
#
# Refresh the GraphHopper routing graph after an OSM update. The new graph is
# built beside the old one and checked before it replaces anything.
#
#   [1/4] Check there is room for a second graph, and decide whether the live
#         server can keep serving during the build.
#   [2/4] Build the new graph into /data/graph-cache.next with a one-off
#         container (`--import`), from the image the server runs and with its
#         mounts, so the config and custom models are the same.
#   [3/4] Verify it: the import exited 0, the graph files are there, it is not
#         much smaller than the graph it replaces, and (optionally) a trial
#         server on the new graph answers a test route.
#   [4/4] Swap: graph-cache -> graph-cache.prev, graph-cache.next ->
#         graph-cache, and restart the server. If the server does not come
#         back on the new graph, the previous one is put back.
#
# Built alongside (GRAPHHOPPER_BUILD_ALONGSIDE), routing is down only for the
# restart. By default the server is stopped for the build; see Memory below.
#
# One previous graph is kept for rollback:
#   ./scripts/rebuild-graphhopper.sh --rollback
# swaps graph-cache and graph-cache.prev and restarts (run it again to undo).
#
# Flags:
#   --rollback          swap back to graph-cache.prev and restart; no build
#   --force             rebuild even if the graph matches the extract (also
#                       FORCE_REBUILD=1), e.g. after a graphhopper-config.yml change
#   --verify-route      start a trial server on the new graph and route through
#                       it before swapping, whatever the host's RAM
#   --no-verify-route   never start the trial server
#   --stop-server       stop the serving container for the build, whatever
#                       GRAPHHOPPER_BUILD_ALONGSIDE says
#   --allow-smaller     accept a new graph under half the size of the current one
#
# Environment:
#   GRAPHHOPPER_BUILD_ALONGSIDE    false (default) | true | auto. Whether the
#                                  live server keeps serving during the build.
#                                  auto builds alongside only when MemAvailable
#                                  covers the build heap plus 4 GB
#   GRAPHHOPPER_BUILD_JAVA_OPTS    JAVA_OPTS for the build container. Unset, the
#                                  serving JAVA_OPTS with -Xmx cut to what is
#                                  available less 1 GB
#   GRAPHHOPPER_VERIFY_ROUTE       auto (default) | 1 | 0, as the flags above
#   GRAPHHOPPER_VERIFY_MIN_RAM_GB  auto runs the trial server only on a host
#                                  with at least this much RAM (default 48)
#
# Memory: by default the serving container is stopped for the build, so only
# one GraphHopper JVM is in memory and routing is down for the build, as it
# always was; the new graph is still built aside, checked and swapped, and the
# old one kept for rollback. Building alongside holds both JVMs (the serving
# heap plus the graph's pages, and the build's full -Xmx for hours of landmark
# preparation), which an unattended nightly run on a small box cannot afford.
# Overlapping a build with heavy SQL has OOM-killed it before, so this warns
# when it starts beside a long-running statement.
#
# Disk: the new graph is built beside the current one, so the volume needs
# free space of about 1.2x the current graph (graph-cache.prev is released to
# make room if that is enough and the current graph is serving). With no graph
# yet, 2.5x the extract is assumed. The script refuses to start otherwise.
#
# Called automatically from:
#   - scripts/run-import.sh   (after a full import)
#   - scripts/update-osm.sh   (after replication or full update)
#
# The script waits for the build, so a run lasts as long as the graph import
# (hours for a country). Before staged builds it returned at once and the
# import ran inside the serving container.
#
# Skips silently if barrelman-graphhopper does not exist (e.g. minimal dev
# setup without the routing engine).
# =============================================================================

CONTAINER="barrelman-graphhopper"
BUILD="barrelman-graphhopper-build"
TRIAL="barrelman-graphhopper-verify"
GRAPH=graph-cache

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=staged-rebuild-lib.sh
. "$SCRIPT_DIR/staged-rebuild-lib.sh"

log() { echo "[$(date '+%H:%M:%S')] [graphhopper] $*"; }

ROLLBACK=0
FORCE_STOP=0
ALLOW_SMALLER=0
ALONGSIDE="${GRAPHHOPPER_BUILD_ALONGSIDE:-false}"
VERIFY="${GRAPHHOPPER_VERIFY_ROUTE:-auto}"
FORCE="${FORCE_REBUILD:-0}"
for arg in "$@"; do
  case "$arg" in
    --rollback) ROLLBACK=1 ;;
    --force) FORCE=1 ;;
    --verify-route) VERIFY=1 ;;
    --no-verify-route) VERIFY=0 ;;
    --stop-server) FORCE_STOP=1 ;;
    --allow-smaller) ALLOW_SMALLER=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done
VERIFY_MIN_RAM_GB="${GRAPHHOPPER_VERIFY_MIN_RAM_GB:-48}"

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  log "container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

# ── Volume access ────────────────────────────────────────────────────────────
# barrelman-ops (which runs this script) mounts the same barrelman-osm-data
# volume at /data, so files are handled directly. On a host without that mount,
# a throwaway container holding the volume does it. Never `docker exec` into
# the serving container for this: a corrupt graph makes it crash-loop, and exec
# into a restarting container fails, which once left a rebuild looping forever
# on "Not a GraphHopper file /data/graph-cache/properties!".
VOLUME="$(docker inspect "$CONTAINER" \
  --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Name}}{{end}}{{end}}')"
if [ -d /data ] && [ -w /data ]; then
  IN_OPS=1
  vol_sh() { sh -c "$1"; }
else
  IN_OPS=0
  if [ -z "$VOLUME" ]; then
    log "could not resolve the /data volume for ${CONTAINER}" >&2
    exit 1
  fi
  vol_sh() { docker run --rm -v "${VOLUME}:/data" alpine sh -c "$1"; }
fi
size_kb() { vol_sh "du -sk /data/$1 2>/dev/null | cut -f1" | tr -cd '0-9'; }
# A graph directory is complete when GraphHopper's core files are there and
# properties carries its file marker; a first boot without a PBF leaves a
# properties file without one.
complete_sh='complete() { [ -s "$1/properties" ] && [ -s "$1/nodes" ] && [ -s "$1/edges" ] && [ -s "$1/geometry" ] && [ -s "$1/location_index" ] && head -c 16 "$1/properties" | tr -cd A-Z | grep -q GH; }'

# Whether the live server answers. GraphHopper only binds its port once a
# graph is loaded. ops reaches the container by name on the compose network; a
# host shell goes through the published port, where docker-proxy accepts a
# connection whether or not anything listens behind it, so this asks for
# /health over HTTP rather than trusting a TCP connect.
gh_serving() {
  curl -sf -o /dev/null --max-time 5 "http://${CONTAINER}:8989/health" 2>/dev/null && return 0
  curl -sf -o /dev/null --max-time 5 "http://127.0.0.1:5003/health" 2>/dev/null && return 0
  return 1
}

wait_serving() { # $1 = seconds
  local deadline=$(( $(date +%s) + $1 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    gh_serving && return 0
    sleep 5
  done
  return 1
}

# Restart onto whatever is now in graph-cache, and confirm it loads.
START_TIMEOUT=900 # seconds for a restarted server to answer; loading a memory-mapped graph is quick
restart_serving() {
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  docker start "$CONTAINER" >/dev/null
  wait_serving "$START_TIMEOUT"
}

# Swap graph-cache and graph-cache.prev. Both moves happen with the server
# stopped; each is a rename on the same volume, so nothing is copied.
# A .swap left by a rollback killed halfway is handled first, or `mv` would
# move the live graph inside it: with the live graph missing it is the live
# graph and goes back, otherwise it is stale and is deleted.
swap_prev() {
  vol_sh "set -e; cd /data
    if [ -d $GRAPH.swap ]; then
      if [ -d $GRAPH ]; then rm -rf $GRAPH.swap; else mv $GRAPH.swap $GRAPH; fi
    fi
    if [ -d $GRAPH ]; then mv $GRAPH $GRAPH.swap; fi
    mv $GRAPH.prev $GRAPH
    if [ -d $GRAPH.swap ]; then mv $GRAPH.swap $GRAPH.prev; fi"
}

# ── --rollback ───────────────────────────────────────────────────────────────
if [ "$ROLLBACK" = 1 ]; then
  if ! vol_sh "$complete_sh; complete /data/$GRAPH.prev"; then
    log "ERROR: no complete graph at /data/$GRAPH.prev to roll back to" >&2
    exit 1
  fi
  log "Rolling back: swapping $GRAPH and $GRAPH.prev..."
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  swap_prev
  if restart_serving; then
    log "Serving the previous graph. Run --rollback again to return to the one it replaced."
    exit 0
  fi
  log "ERROR: GraphHopper did not start on the rolled-back graph within ${START_TIMEOUT}s." >&2
  log "  Check: docker logs ${CONTAINER}" >&2
  exit 1
fi

# ── Skip when the serving graph is already built from the current extract ────
# run-import.sh starts this rebuild the moment the PBF is downloaded, so by the
# time an operator (or a re-run after a failed later stage) invokes it again,
# the graph may already be exactly what a rebuild would produce. Rebuilding
# then throws away hours on a large region.
#
# The health probe is what makes this safe against a HALF-built graph:
# GraphHopper only answers HTTP after a graph is loaded. The mtime
# comparison then ties that graph to the extract on disk — apply-osm-diff.sh
# patches region.osm.pbf in place, so a stale graph always has properties older
# than the PBF and rebuilds as before. --force overrides (e.g. after changing
# graphhopper-config.yml, which the mtimes cannot see).
SERVING=0
gh_serving && SERVING=1
if [ "$FORCE" != "1" ] && [ "$SERVING" = 1 ]; then
  PROPS_M=$(vol_sh "stat -c %Y /data/$GRAPH/properties 2>/dev/null || echo 0" | tr -cd '0-9')
  PBF_M=$(vol_sh 'stat -c %Y /data/region.osm.pbf 2>/dev/null || echo 0' | tr -cd '0-9')
  if [ "${PBF_M:-0}" -gt 0 ] && [ "${PROPS_M:-0}" -gt "$PBF_M" ]; then
    log "Graph already built from the current extract — skipping."
    log "  (--force or FORCE_REBUILD=1 rebuilds anyway, e.g. after a graphhopper-config.yml change.)"
    exit 0
  fi
fi

# The build runs the image the server runs. The graph is written in a
# version-specific format, so a build by any other image could leave a graph
# the server refuses to load. The image ID, not the name: a name may point at a
# newer pull than the one running (same reason as rebuild-motis.sh).
GH_IMAGE="$(docker inspect -f '{{.Image}}' "$CONTAINER")"
NETWORK="$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' "$CONTAINER" | head -1)"
# The server's own mounts: the data volume, graphhopper-config.yml and the
# custom models. Bind sources are host paths, which is what the daemon wants.
MOUNTS=()
while IFS= read -r m; do
  [ -n "$m" ] && MOUNTS+=(-v "$m")
done < <(docker inspect "$CONTAINER" --format \
  '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{else}}{{.Source}}{{end}}:{{.Destination}}{{if not .RW}}:ro{{end}}{{"\n"}}{{end}}')
# The extract the server was started with (-i), so the build reads the same one.
PBF="$(docker inspect -f '{{range .Config.Cmd}}{{.}}{{"\n"}}{{end}}' "$CONTAINER" | awk 'p { print; exit } $0 == "-i" || $0 == "--input" { p = 1 }')"
PBF="${PBF:-/data/region.osm.pbf}"
SERVING_JAVA_OPTS="$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$CONTAINER" | sed -n 's/^JAVA_OPTS=//p' | head -1)"
SERVING_JAVA_OPTS="${SERVING_JAVA_OPTS:--Xmx6g -Xms1g}"
SERVING_XMX_KB="$(java_xmx_kb "$SERVING_JAVA_OPTS")"

# ── [1/4] Disk and memory ────────────────────────────────────────────────────
log "[1/4] Checking disk and memory..."
CURRENT_KB="$(size_kb "$GRAPH")"; CURRENT_KB="${CURRENT_KB:-0}"
PREV_KB="$(size_kb "$GRAPH.prev")"; PREV_KB="${PREV_KB:-0}"
PBF_KB="$(vol_sh "du -sk $PBF 2>/dev/null | cut -f1" | tr -cd '0-9')"; PBF_KB="${PBF_KB:-0}"
FREE_KB="$(vol_sh 'df -Pk /data | awk "NR == 2 { print \$4 }"' | tr -cd '0-9')"; FREE_KB="${FREE_KB:-0}"
# A stale .next from an interrupted run, and a .failed graph from an earlier
# swap that would not start, are deleted below, so they count as free.
STALE_KB="$(size_kb "$GRAPH.next")"; FAILED_KB="$(size_kb "$GRAPH.failed")"
FREE_KB=$(( FREE_KB + ${STALE_KB:-0} + ${FAILED_KB:-0} ))
NEED_KB="$(staged_need_kb "$CURRENT_KB" $(( PBF_KB * 5 / 2 )))"

if [ "$PBF_KB" = 0 ]; then
  log "ERROR: $PBF is missing — nothing to build from. Run the OSM import first." >&2
  exit 1
fi

DISK="$(staged_disk_decision "$FREE_KB" "$NEED_KB" "$PREV_KB" "$SERVING")"
log "  volume: $(staged_human_kb "$FREE_KB") free; current graph $(staged_human_kb "$CURRENT_KB"), previous $(staged_human_kb "$PREV_KB"); a new graph needs about $(staged_human_kb "$NEED_KB")"
case "$DISK" in
  refuse)
    log "ERROR: not enough free space on the graph volume to build a new graph beside the current one." >&2
    log "  Needed: about $(staged_human_kb "$NEED_KB") (1.2x the current graph). Free: $(staged_human_kb "$FREE_KB")." >&2
    if [ "$PREV_KB" -gt 0 ] && [ "$SERVING" != 1 ]; then
      log "  $GRAPH.prev ($(staged_human_kb "$PREV_KB")) was kept because the live graph is not serving;" >&2
      log "  it may be the only good graph. Try --rollback, or delete it by hand if it is not needed." >&2
    fi
    log "  Free space on the volume (old extracts, basemap tmp files) and run this again." >&2
    exit 1 ;;
  drop-prev)
    log "  releasing $GRAPH.prev ($(staged_human_kb "$PREV_KB")) to make room; the live graph is serving"
    vol_sh "rm -rf /data/$GRAPH.prev" ;;
esac

# Alongside or not. The build is assumed to want the serving heap, or the
# configured one.
WANT_XMX_KB="$SERVING_XMX_KB"
[ -n "${GRAPHHOPPER_BUILD_JAVA_OPTS:-}" ] && WANT_XMX_KB="$(java_xmx_kb "$GRAPHHOPPER_BUILD_JAVA_OPTS")"
if [ "$FORCE_STOP" = 1 ]; then
  MODE_DECISION="stop: --stop-server"
else
  MODE_DECISION="$(staged_alongside_decision "$ALONGSIDE" "$(mem_available_kb)" "$WANT_XMX_KB")"
fi
STOP_SERVER=1
[ "$MODE_DECISION" = alongside ] && STOP_SERVER=0
if [ "$STOP_SERVER" = 1 ]; then
  log "  building with the server stopped (${MODE_DECISION#stop: }); routing is down until the swap"
else
  log "  building alongside the live server (GRAPHHOPPER_BUILD_ALONGSIDE=$ALONGSIDE)"
fi
HEAVY="$(heavy_sql_running)"
if [ -n "$HEAVY" ]; then
  log "  WARNING: long-running SQL in barrelman-db. A GraphHopper build beside heavy SQL has"
  log "  been OOM-killed before; consider waiting for it to finish:"
  printf '%s\n' "$HEAVY" | sed 's/^/      /'
fi

# ── [2/4] Build ──────────────────────────────────────────────────────────────
# Anything left from an interrupted run: a build or trial container, and a
# partial graph-cache.next.
if [ "$(docker inspect -f '{{.State.Running}}' "$BUILD" 2>/dev/null)" = "true" ]; then
  log "ERROR: a build container ($BUILD) is already running. Another rebuild is in progress." >&2
  exit 1
fi
docker rm -f "$BUILD" "$TRIAL" >/dev/null 2>&1 || true
vol_sh "rm -rf /data/$GRAPH.next /data/$GRAPH.failed"

# Every way out before the swap (a failed build or check, an unexpected
# `set -e` abort, a cancelled job) deletes the unverified graph and makes sure
# the server is running on the graph it had, whoever stopped it.
SWAPPED=0
cleanup() {
  docker rm -f "$BUILD" "$TRIAL" >/dev/null 2>&1 || true
  if [ "$SWAPPED" != 1 ]; then
    vol_sh "rm -rf /data/$GRAPH.next" || true
    if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != true ]; then
      log "starting the server again on the graph it had"
      docker start "$CONTAINER" >/dev/null 2>&1 || true
    fi
  fi
}
trap cleanup EXIT
trap 'exit 143' TERM INT

if [ "$STOP_SERVER" = 1 ]; then
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
fi

# The build heap. Measured after the stop, so a stopped server's memory counts
# as available. Unset, it is the serving heap cut to what is available less
# 1 GB, so a build never starts with a heap the box cannot back.
AVAIL_KB="$(mem_available_kb)"
if [ -n "${GRAPHHOPPER_BUILD_JAVA_OPTS:-}" ]; then
  BUILD_JAVA_OPTS="$GRAPHHOPPER_BUILD_JAVA_OPTS"
else
  HEAP_KB="$(staged_build_heap_kb "$SERVING_XMX_KB" "$AVAIL_KB")"
  if [ "$HEAP_KB" = 0 ]; then
    log "ERROR: only $(staged_human_kb "$AVAIL_KB") of memory available, too little for a graph build." >&2
    exit 1
  fi
  BUILD_JAVA_OPTS="$(staged_with_xmx "$SERVING_JAVA_OPTS" $(( HEAP_KB / 1024 )))"
fi
BUILD_XMX_KB="$(java_xmx_kb "$BUILD_JAVA_OPTS")"
log "  memory: $(staged_human_kb "$AVAIL_KB") available; the build JVM takes up to $(staged_human_kb "$BUILD_XMX_KB") ($BUILD_JAVA_OPTS)"
if [ "$BUILD_XMX_KB" -gt $(( AVAIL_KB - 1048576 )) ]; then
  log "  WARNING: GRAPHHOPPER_BUILD_JAVA_OPTS asks for more heap than is available."
  log "  The kernel may OOM-kill the build or something else on the box."
fi

log "[2/4] Building the new graph into /data/$GRAPH.next$([ "$STOP_SERVER" = 1 ] || echo ' (the server keeps serving)')..."
docker run -d --name "$BUILD" ${NETWORK:+--network "$NETWORK"} "${MOUNTS[@]}" \
  -e JAVA_OPTS="$BUILD_JAVA_OPTS" "$GH_IMAGE" \
  -i "$PBF" -o "/data/$GRAPH.next" --import >/dev/null
log "  build container: $BUILD. Follow it with: docker logs -f $BUILD"

# Wait, with a line of progress every ten minutes so a long job's log shows life.
ticks=0
while [ "$(docker inspect -f '{{.State.Running}}' "$BUILD" 2>/dev/null)" = "true" ]; do
  sleep 30
  ticks=$((ticks + 1))
  if [ $((ticks % 20)) = 0 ]; then
    log "  still building: $(docker logs --tail 1 "$BUILD" 2>&1 | cut -c1-160)"
  fi
done
BUILD_RC="$(docker inspect -f '{{.State.ExitCode}}' "$BUILD")"
BUILD_OOM="$(docker inspect -f '{{.State.OOMKilled}}' "$BUILD")"
docker logs "$BUILD" > /tmp/graphhopper-build.log 2>&1 || true
docker rm "$BUILD" >/dev/null 2>&1 || true

# ── [3/4] Verify ─────────────────────────────────────────────────────────────
log "[3/4] Verifying the new graph..."
if [ "$BUILD_RC" != 0 ]; then
  log "ERROR: the graph import exited $BUILD_RC$([ "$BUILD_OOM" = true ] && echo ' (out of memory)'); the current graph is untouched." >&2
  [ "$BUILD_RC" = 137 ] && log "  Exit 137 is a kill, usually the OOM killer. Lower GRAPHHOPPER_BUILD_JAVA_OPTS, or build with the server stopped (the default)." >&2
  log "  ── build log tail ──" >&2
  tail -15 /tmp/graphhopper-build.log >&2 || true
  exit 1
fi
if ! vol_sh "$complete_sh; complete /data/$GRAPH.next"; then
  log "ERROR: the import exited 0 but /data/$GRAPH.next is missing graph files; the current graph is untouched." >&2
  vol_sh "ls -la /data/$GRAPH.next" >&2 || true
  exit 1
fi
# properties.txt lists every profile that got landmarks; each needs its file,
# or that profile falls back to slow routing (or fails) once served.
MISSING_LM="$(vol_sh "cd /data/$GRAPH.next && for p in \$(sed -n 's/^graph[.]profiles[.]lm[.]\\([^.]*\\)[.]version=.*/\\1/p' properties.txt 2>/dev/null); do [ -s landmarks_\$p ] || echo \$p; done")"
if [ -n "$MISSING_LM" ]; then
  log "ERROR: landmark files missing for: $(echo $MISSING_LM); the current graph is untouched." >&2
  exit 1
fi
NEW_KB="$(size_kb "$GRAPH.next")"; NEW_KB="${NEW_KB:-0}"
log "  graph files present ($(staged_human_kb "$NEW_KB"))"
if [ "$(staged_size_check "$NEW_KB" "$CURRENT_KB" 50)" = small ] && [ "$ALLOW_SMALLER" != 1 ]; then
  log "ERROR: the new graph is $(staged_human_kb "$NEW_KB"), under half the current $(staged_human_kb "$CURRENT_KB")." >&2
  log "  That is usually a truncated extract. If the region was meant to shrink, re-run with --allow-smaller." >&2
  exit 1
fi

# The trial server: a second GraphHopper on the new graph, on its own container
# network address, so it never touches the live port. It runs with the live
# server's JAVA_OPTS, so it behaves as the swapped-in server will, and costs
# that heap plus page cache for the graph it maps (a quarter of the graph is
# assumed for a handful of routes), on top of the live server. Hence the RAM
# threshold.
TRIAL_JAVA_OPTS="${SERVING_JAVA_OPTS:--Xmx6g -Xms1g}"
TRIAL_NEED_KB=$(( $(java_xmx_kb "$TRIAL_JAVA_OPTS") + NEW_KB / 4 ))
TRIAL_DECISION="$(staged_trial_decision "$VERIFY" "$(mem_total_kb)" "$VERIFY_MIN_RAM_GB" "$(mem_available_kb)" "$TRIAL_NEED_KB")"

route_ok() { # $1 = base URL, $2 = profile, $3 = "lat,lon lat,lon"
  local a="${3% *}" b="${3#* }" code
  code="$(curl -s -o /tmp/graphhopper-verify-route.json -w '%{http_code}' --max-time 60 \
    "$1/route?point=$a&point=$b&profile=$2&calc_points=false" 2>/dev/null)" || true
  [ "$code" = 200 ] && grep -q '"paths"' /tmp/graphhopper-verify-route.json
}

if [ "$TRIAL_DECISION" = run ]; then
  log "  starting a trial server on the new graph ($TRIAL_JAVA_OPTS)..."
  PUBLISH=()
  if [ "$IN_OPS" = 1 ]; then
    TRIAL_URL="http://$TRIAL:8989"
  else
    TRIAL_URL="http://127.0.0.1:5013"
    PUBLISH=(-p "127.0.0.1:5013:8989")
  fi
  docker run -d --name "$TRIAL" ${NETWORK:+--network "$NETWORK"} "${PUBLISH[@]}" "${MOUNTS[@]}" \
    -e JAVA_OPTS="$TRIAL_JAVA_OPTS" "$GH_IMAGE" -i "$PBF" -o "/data/$GRAPH.next" >/dev/null
  deadline=$(( $(date +%s) + 900 ))
  until curl -sf -o /dev/null --max-time 5 "$TRIAL_URL/health" 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ] || [ "$(docker inspect -f '{{.State.Running}}' "$TRIAL" 2>/dev/null)" != true ]; then
      log "ERROR: the trial server did not come up on the new graph; the current graph is untouched." >&2
      docker logs --tail 15 "$TRIAL" >&2 2>&1 || true
      exit 1
    fi
    sleep 5
  done
  INFO="$(curl -s --max-time 10 "$TRIAL_URL/info" || true)"
  PROFILE="$(printf '%s' "$INFO" | grep -oE '"profiles":\[\{"name":"[^"]+"' | sed 's/.*"name":"//;s/"$//' || true)"
  PROFILE="${PROFILE:-car}"
  BBOX="$(printf '%s' "$INFO" | grep -oE '"bbox":\[[^]]+\]' | sed 's/"bbox":\[//;s/\]//;s/,/ /g' || true)"
  routed=""
  CANDIDATES=""
  if [ -n "$BBOX" ]; then
    # shellcheck disable=SC2086
    CANDIDATES="$(gh_route_candidates $BBOX)"
    while IFS= read -r pair; do
      if route_ok "$TRIAL_URL" "$PROFILE" "$pair"; then routed="$pair"; break; fi
    done <<< "$CANDIDATES"
  fi
  if [ -n "$routed" ]; then
    log "  trial route ($PROFILE, $routed) answered on the new graph"
  else
    # None of the sample points routed. That is a failed graph if the live
    # server can route one of them, and inconclusive otherwise (a region
    # whose bounding box is mostly water).
    live_routed=""
    if [ "$SERVING" = 1 ] && [ -n "$CANDIDATES" ]; then
      LIVE_URL="http://$CONTAINER:8989"; [ "$IN_OPS" = 1 ] || LIVE_URL="http://127.0.0.1:5003"
      while IFS= read -r pair; do
        if route_ok "$LIVE_URL" "$PROFILE" "$pair"; then live_routed="$pair"; break; fi
      done <<< "$CANDIDATES"
    fi
    if [ -n "$live_routed" ]; then
      log "ERROR: the live graph routes $live_routed ($PROFILE) and the new graph does not; the current graph is untouched." >&2
      exit 1
    fi
    log "  WARNING: no sample route answered on either graph (profile '${PROFILE:-?}'); the trial server is healthy, so going ahead."
  fi
  docker rm -f "$TRIAL" >/dev/null 2>&1 || true
else
  log "  trial server ${TRIAL_DECISION} (--verify-route forces it; it needs about $(staged_human_kb "$TRIAL_NEED_KB") more RAM)"
fi

# ── [4/4] Swap ───────────────────────────────────────────────────────────────
log "[4/4] Swapping the new graph in and restarting ${CONTAINER}..."
docker stop "$CONTAINER" >/dev/null 2>&1 || true
# The graph being replaced becomes .prev only if it is complete. A half-written
# one (a crash-looping server, a first boot without a PBF) is deleted instead,
# so it never overwrites a good .prev. /data/default-gh is where the image
# writes when -o is missing; nothing reads it.
vol_sh "set -e; $complete_sh; cd /data
  if [ -d $GRAPH ] && complete $GRAPH; then rm -rf $GRAPH.prev; mv $GRAPH $GRAPH.prev
  else rm -rf $GRAPH; fi
  mv $GRAPH.next $GRAPH
  rm -rf default-gh"
SWAPPED=1

if restart_serving; then
  log "Rebuild complete. The previous graph is kept at /data/$GRAPH.prev (--rollback to return to it)."
  exit 0
fi

log "ERROR: GraphHopper did not start on the new graph within ${START_TIMEOUT}s." >&2
docker logs --tail 15 "$CONTAINER" >&2 2>&1 || true
if vol_sh "$complete_sh; complete /data/$GRAPH.prev"; then
  log "  putting the previous graph back" >&2
  docker stop "$CONTAINER" >/dev/null 2>&1 || true
  # The new graph goes to .failed rather than .prev, so a later --rollback
  # cannot land on it.
  vol_sh "set -e; cd /data; rm -rf $GRAPH.failed; mv $GRAPH $GRAPH.failed; mv $GRAPH.prev $GRAPH"
  if restart_serving; then
    log "  serving the previous graph again. The new one is kept at /data/$GRAPH.failed for inspection." >&2
  else
    log "  the previous graph did not start either. Check: docker logs ${CONTAINER}" >&2
  fi
fi
exit 1
