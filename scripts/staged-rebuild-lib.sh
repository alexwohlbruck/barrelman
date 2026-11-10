# Shared by scripts/rebuild-graphhopper.sh and scripts/rebuild-motis.sh.
# Sourced, not run.
#
# Both engines serve a dataset they cannot rebuild in place without going down
# for the length of the build. The two scripts build the replacement beside the
# live one (<dir>.next), check it, and only then rename it into place, keeping
# the dataset it replaced as <dir>.prev for --rollback. The decisions involved
# (is there room, can the box afford a trial server, does the new dataset look
# complete) are plain functions of numbers, kept here so they can be tested
# without Docker: see scripts/staged-rebuild-lib.test.ts.
#
# Sizes are in KiB throughout, which is what `du -sk` and `df -Pk` report.

# Free space needed to build a new dataset beside the current one: 1.2x the
# current size, since a rebuild of the same region comes out about the same
# size and both copies sit on the volume until the swap. With no current
# dataset (a first build) the caller's estimate is used instead.
#   $1 current dataset KiB (0 = none)   $2 fallback estimate KiB
staged_need_kb() {
  local current="${1:-0}" fallback="${2:-0}"
  if [ "$current" -gt 0 ]; then
    echo $(( (current * 12 + 9) / 10 ))
  else
    echo "$fallback"
  fi
}

# Whether the build can start, and whether the old rollback copy has to go to
# make room. Prints one word:
#   ok         - enough free space; keep <dir>.prev
#   drop-prev  - enough only once <dir>.prev is deleted, and the live dataset
#                is serving, so the old rollback copy is the one to give up
#   refuse     - not enough even then, or the live dataset is not serving and
#                <dir>.prev may be the only good copy left
#   $1 free KiB   $2 needed KiB   $3 <dir>.prev KiB (0 = none)   $4 serving 1/0
staged_disk_decision() {
  local free="$1" need="$2" prev="${3:-0}" serving="${4:-0}"
  if [ "$free" -ge "$need" ]; then
    echo ok
  elif [ "$serving" = 1 ] && [ "$prev" -gt 0 ] && [ $((free + prev)) -ge "$need" ]; then
    echo drop-prev
  else
    echo refuse
  fi
}

# Human-readable size for log lines.
staged_human_kb() {
  local kb="${1:-0}"
  if [ "$kb" -ge 1048576 ]; then
    awk -v k="$kb" 'BEGIN { printf "%.1f GB", k / 1048576 }'
  else
    awk -v k="$kb" 'BEGIN { printf "%.0f MB", k / 1024 }'
  fi
}

# The -Xmx of a JAVA_OPTS string, in KiB. Prints 0 when there is none.
java_xmx_kb() {
  local opts="$1" v
  v="$(printf '%s\n' "$opts" | grep -oE -- '-Xmx[0-9]+[kKmMgG]?' | tail -1 | sed 's/-Xmx//' || true)"
  [ -z "$v" ] && { echo 0; return; }
  case "$v" in
    *[gG]) echo $(( ${v%?} * 1048576 )) ;;
    *[mM]) echo $(( ${v%?} * 1024 )) ;;
    *[kK]) echo "${v%?}" ;;
    *) echo $(( v / 1024 )) ;;
  esac
}

# Whether to start a trial server on the new dataset before swapping it in.
# A trial server is a second copy of the engine in memory beside the live one,
# so `auto` runs it only on a box with at least $3 GiB of RAM in total and
# enough available right now for what the trial needs. Prints `run`, or
# `skip: <reason>`.
#   $1 mode auto|1|0   $2 MemTotal KiB   $3 minimum total GiB
#   $4 MemAvailable KiB   $5 KiB the trial needs
staged_trial_decision() {
  local mode="${1:-auto}" total="${2:-0}" min_gb="${3:-0}" avail="${4:-0}" need="${5:-0}"
  case "$mode" in
    1|true|yes|on) echo run; return ;;
    0|false|no|off) echo "skip: turned off"; return ;;
  esac
  if [ "$total" -lt $((min_gb * 1048576)) ]; then
    echo "skip: host has $(staged_human_kb "$total") of RAM, under the ${min_gb} GB threshold"
  elif [ "$avail" -lt "$need" ]; then
    echo "skip: $(staged_human_kb "$avail") available, the trial needs about $(staged_human_kb "$need")"
  else
    echo run
  fi
}

# Whether to build with the live server still running. Two copies of an engine
# in memory at once has OOM-killed production engines, so the default is to
# stop the server for the build; the new dataset is still built aside, checked
# and swapped. `true` always builds alongside; `auto` does so only when
# MemAvailable covers the build plus a margin. Prints `alongside`, or
# `stop: <reason>`.
#   $1 mode false|true|auto   $2 MemAvailable KiB   $3 build needs KiB
#   $4 margin KiB (default 4 GiB)
staged_alongside_decision() {
  local mode="${1:-false}" avail="${2:-0}" need="${3:-0}" margin="${4:-4194304}"
  case "$mode" in
    1|true|yes|on) echo alongside ;;
    auto)
      if [ "$avail" -ge $((need + margin)) ]; then
        echo alongside
      else
        echo "stop: $(staged_human_kb "$avail") available, building alongside needs $(staged_human_kb "$need") plus a $(staged_human_kb "$margin") margin"
      fi ;;
    *) echo "stop: building alongside is off by default" ;;
  esac
}

# The build heap when none is configured: the serving heap, cut down to what
# is available less 1 GiB so the build never asks the kernel for memory that
# is not there. Prints KiB; 0 means less than 1 GiB would be left, too little
# to build anything.
#   $1 serving -Xmx KiB (0 = unknown)   $2 MemAvailable KiB
staged_build_heap_kb() {
  local serving="${1:-0}" avail="${2:-0}" cap
  cap=$((avail - 1048576))
  [ "$serving" -gt 0 ] && [ "$serving" -lt "$cap" ] && cap="$serving"
  if [ "$cap" -lt 1048576 ]; then echo 0; else echo $(( cap / 1024 * 1024 )); fi
}

# JAVA_OPTS with the heap set to $2 MiB: every -Xmx replaced by one, and an
# -Xms above it dropped, since the JVM refuses to start with Xms > Xmx. Other
# options (GC and the like) are kept.
#   $1 JAVA_OPTS   $2 MiB
staged_with_xmx() {
  local opts="$1" mb="$2" out="" tok xms
  for tok in $opts; do
    case "$tok" in
      -Xmx*) ;;
      -Xms*)
        xms="$(java_xmx_kb "-Xmx${tok#-Xms}")"
        [ "$xms" -le $((mb * 1024)) ] && out="$out $tok" ;;
      *) out="$out $tok" ;;
    esac
  done
  echo "-Xmx${mb}m${out}"
}

# A rebuild of the same extract that comes out much smaller than the graph it
# replaces is usually a truncated download or a crashed build that still
# exited 0. Prints `ok` or `small`.
#   $1 new KiB   $2 current KiB (0 = none)   $3 minimum ratio in percent
staged_size_check() {
  local new="$1" current="${2:-0}" pct="${3:-50}"
  if [ "$current" -gt 0 ] && [ $((new * 100)) -lt $((current * pct)) ]; then
    echo small
  else
    echo ok
  fi
}

# Total trips in a MOTIS timetable_metrics.json (the import's own report, one
# object per dataset). Prints 0 when the file is empty or missing.
motis_metrics_trips() {
  local f="$1"
  [ -s "$f" ] || { echo 0; return; }
  # `|| true`: the scripts run under pipefail, and a report with no noTrips
  # field must read as 0 trips, not abort the rebuild.
  { grep -oE '"noTrips": *[0-9]+' "$f" || true; } | grep -oE '[0-9]+$' | awk '{ s += $1 } END { print s + 0 }' || true
}

# Test routes for a GraphHopper graph: short hops from points spread over its
# bounding box. Prints "lat1,lon1 lat2,lon2" per line. A point can land in
# water or forest, so several are tried; any one routing is enough.
#   $1..$4 minLon minLat maxLon maxLat (the order of GraphHopper's /info bbox)
gh_route_candidates() {
  awk -v a="$1" -v b="$2" -v c="$3" -v d="$4" 'BEGIN {
    n = split("0.5 0.3 0.7", f, " ")
    for (i = 1; i <= n; i++) for (j = 1; j <= n; j++) {
      lon = a + (c - a) * f[i]; lat = b + (d - b) * f[j]
      printf "%.5f,%.5f %.5f,%.5f\n", lat, lon, lat + 0.01, lon + 0.01
    }
  }'
}

# MemTotal and MemAvailable in KiB. /proc/meminfo is not namespaced, so inside
# barrelman-ops this is the host's memory, which is what an OOM is about.
mem_total_kb() { awk '/^MemTotal:/ { print $2 }' /proc/meminfo 2>/dev/null || echo 0; }
mem_available_kb() { awk '/^MemAvailable:/ { print $2 }' /proc/meminfo 2>/dev/null || echo 0; }

# Long-running statements in barrelman-db. A GraphHopper build running beside
# heavy SQL has been OOM-killed (241 restarts on a 30 GB box), so the rebuilds
# warn when they start next to one. Prints "<pid> <minutes>m <query prefix>" per statement.
heavy_sql_running() {
  docker exec "${DB_CONTAINER:-barrelman-db}" psql -U barrelman -d barrelman -tAc "
    SELECT pid || ' ' || floor(extract(epoch FROM now() - query_start) / 60) || 'm ' || left(regexp_replace(query, '\s+', ' ', 'g'), 80)
    FROM pg_stat_activity
    WHERE state = 'active' AND pid <> pg_backend_pid()
      AND now() - query_start > interval '2 minutes'
      AND backend_type = 'client backend'" 2>/dev/null || true
}
