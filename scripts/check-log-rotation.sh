#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Container Log Rotation Check
# =============================================================================
#
# Reports containers whose Docker log file is UNCAPPED, and how big each one has
# already grown.
#
# Why this exists: docker-compose.yml caps every service through the x-logging
# anchor, but log options are baked in at CONTAINER CREATION — a running
# container keeps the config it was born with, forever. The release pipeline only
# recreates `barrelman` and `barrelman-ops` (DEPLOY_SERVICES in release.yml), so
# motis, the database, graphhopper and martin can outlive many deploys still
# uncapped. That is not hypothetical: barrelman-motis reached 27.9 GB on a dev
# box months after the cap was merged, filling the disk that also holds the OSM
# database and taking unrelated services down with ENOSPC.
#
# Run manually:
#   ./scripts/check-log-rotation.sh
#
# Read-only — it never restarts or recreates anything, it only tells you which
# containers need recreating. Exits 1 when any container is uncapped so it can
# gate a cron job or CI step.
#
# Scoped to this compose project by label, not by name: a substring match on
# "barrelman" also catches unrelated stacks that happen to embed the word (a
# parchment test database named direct-barrelman-tiles-parchment-db-test-1, for
# one) and would then print a recreate command naming services this repo does
# not define. CONTAINER_FILTER=<substring> falls back to name matching for
# containers started outside compose.
#
# Pelias is `include`d into this project, so its containers are covered too.
# pelias/docker-compose.yml has no cap of its own, so expect them to report
# uncapped on a stack running the pelias profiles.
# =============================================================================

# Container discovery, most authoritative first. `docker compose ps` resolves the
# project itself — honouring COMPOSE_PROJECT_NAME, COMPOSE_FILE and the include
# of pelias — which guessing a project name from the directory does not.
#
# The mode is chosen before the names are read: a process substitution runs in a
# subshell, so anything it assigns (the scope description) would be lost.
COMPOSE_IDS=""
if [ -n "${CONTAINER_FILTER:-}" ]; then
  SCOPE_DESC="name matching '${CONTAINER_FILTER}'"
  SCOPE_MODE="name"
elif COMPOSE_IDS=$(docker compose ps -q 2>/dev/null) && [ -n "$COMPOSE_IDS" ]; then
  SCOPE_DESC="this compose project"
  SCOPE_MODE="compose"
else
  # No compose file in reach (a bare `docker` host, or run from outside the
  # repo). Fall back to the project label, which compose stamps on creation.
  PROJECT="${COMPOSE_PROJECT_NAME:-barrelman}"
  SCOPE_DESC="compose project '${PROJECT}'"
  SCOPE_MODE="label"
fi

case "$SCOPE_MODE" in
  name)
    mapfile -t CONTAINERS < <(docker ps --filter "name=${CONTAINER_FILTER}" --format '{{.Names}}' | sort) ;;
  compose)
    # shellcheck disable=SC2086
    mapfile -t CONTAINERS < <(docker inspect --format '{{.Name}}' $COMPOSE_IDS 2>/dev/null | sed 's|^/||' | sort) ;;
  label)
    mapfile -t CONTAINERS < <(docker ps --filter "label=com.docker.compose.project=${PROJECT}" --format '{{.Names}}' | sort) ;;
esac

if [ ${#CONTAINERS[@]} -eq 0 ]; then
  echo "[log-rotation] no running containers in ${SCOPE_DESC} — nothing to check"
  exit 0
fi

echo "[log-rotation] checking ${#CONTAINERS[@]} container(s) in ${SCOPE_DESC}"
echo

# Docker reports the log path but the file is root-owned; a non-root caller gets
# the path and no size. Missing size is not a failure, the cap is what matters.
log_size() {
  local path="$1"
  [ -n "$path" ] && [ -r "$path" ] || { echo "-"; return; }
  du -h "$path" 2>/dev/null | cut -f1 || echo "-"
}

printf '%-26s %-10s %-10s %s\n' CONTAINER DRIVER MAX-SIZE CURRENT
uncapped=()
any_size=0
for c in "${CONTAINERS[@]}"; do
  driver=$(docker inspect --format '{{.HostConfig.LogConfig.Type}}' "$c" 2>/dev/null || echo '?')
  max=$(docker inspect --format '{{index .HostConfig.LogConfig.Config "max-size"}}' "$c" 2>/dev/null || echo '')
  path=$(docker inspect --format '{{.LogPath}}' "$c" 2>/dev/null || echo '')
  size=$(log_size "$path")
  [ "$size" = "-" ] || any_size=1

  # Drivers that are not json-file (journald, local, a shipper) do their own
  # retention — absence of max-size there is not drift.
  if [ "$driver" != "json-file" ]; then
    printf '%-26s %-10s %-10s %s\n' "$c" "$driver" "n/a" "$size"
    continue
  fi

  if [ -z "$max" ] || [ "$max" = "<no value>" ]; then
    printf '%-26s %-10s %-10s %s\n' "$c" "$driver" "UNCAPPED" "$size"
    uncapped+=("$c")
  else
    printf '%-26s %-10s %-10s %s\n' "$c" "$driver" "$max" "$size"
  fi
done

size_note() {
  [ "$any_size" = "0" ] || return 0
  echo "[log-rotation] CURRENT is blank: log files are root-owned. Re-run with"
  echo "[log-rotation] sudo to see how large each one has grown."
}

if [ ${#uncapped[@]} -eq 0 ]; then
  echo
  echo "[log-rotation] all ${#CONTAINERS[@]} container(s) capped"
  size_note
  exit 0
fi

echo
echo "[log-rotation] ${#uncapped[@]} container(s) uncapped: ${uncapped[*]}"
echo "[log-rotation] recreate them to adopt the compose cap:"
echo
# Compose service names, read from the container's own label. Deriving them by
# stripping the `barrelman-` prefix is wrong: the keys are `barrelman-db` and
# `barrelman-ops`, which would come out as `db` and `ops` and match nothing.
services=""
for c in "${uncapped[@]}"; do
  svc=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.service"}}' "$c" 2>/dev/null || echo '')
  if [ -z "$svc" ] || [ "$svc" = "<no value>" ]; then
    svc="$c"
  fi
  services+="$svc "
done
echo "    docker compose up -d --force-recreate --no-deps ${services% }"
echo
echo "[log-rotation] --no-deps matters: without it compose recreates"
echo "[log-rotation] dependencies too, bouncing the database. Recreating motis"
echo "[log-rotation] after a :latest pull needs a timetable re-import first —"
echo "[log-rotation] see scripts/rebuild-motis.sh."
size_note
exit 1
