#!/usr/bin/env bash
# =============================================================================
# Barrelman deploy / rollback: the half that runs ON the host
# =============================================================================
#
# Not run directly. scripts/deploy.sh and scripts/rollback.sh copy this file to
# the host over ssh and run it there, so the host needs nothing beyond bash,
# docker compose and the deployment directory.
#
#   deploy-remote.sh deploy   <version> <components>
#   deploy-remote.sh rollback
#
# <components> is a comma-separated subset of api,ops,db. Settings come from
# the environment the wrapper sets:
#
#   DEPLOY_PATH            deployment directory (default /opt/barrelman)
#   DEPLOY_HOLDER          who is deploying, for the lock and the log
#   DEPLOY_FORCE           1 = take the lock even if someone holds it
#   DEPLOY_TIMEOUT         seconds to wait for /health (default 300)
#   DEPLOY_ALLOW_DEGRADED  1 = accept /health status "degraded" as healthy
#
# There is no console manifest entry for this. It drives `docker compose` on
# the host from outside, and restarts the containers the console runs in.
# =============================================================================
set -euo pipefail

DEPLOY_PATH="${DEPLOY_PATH:-/opt/barrelman}"
DEPLOY_HOLDER="${DEPLOY_HOLDER:-$(whoami)@$(hostname)}"
DEPLOY_FORCE="${DEPLOY_FORCE:-0}"
DEPLOY_TIMEOUT="${DEPLOY_TIMEOUT:-300}"
DEPLOY_ALLOW_DEGRADED="${DEPLOY_ALLOW_DEGRADED:-0}"

LOCK_FILE="$DEPLOY_PATH/.deploy.lock"
LOG_FILE="$DEPLOY_PATH/deploy.log"
# The image refs that were running before the last successful deploy, which
# rollback restores. One line per component: component|service|file|ref.
STATE_FILE="$DEPLOY_PATH/.deploy-previous"

say() { printf '[deploy] %s\n' "$*" >&2; }
die() {
  printf '[deploy] error: %s\n' "$*" >&2
  exit 1
}

component_service() {
  case "$1" in
    api) echo barrelman ;;
    ops) echo barrelman-ops ;;
    db) echo barrelman-db ;;
    *) return 1 ;;
  esac
}

component_image() {
  case "$1" in
    api) echo alexwohlbruck/barrelman ;;
    ops) echo alexwohlbruck/barrelman-ops ;;
    db) echo alexwohlbruck/barrelman-db ;;
    *) return 1 ;;
  esac
}

# ── Compose files ────────────────────────────────────────────────────────────

# A variable from ./.env, unquoted. Empty when absent.
env_file_value() {
  local line
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?$1=" .env 2>/dev/null | tail -n 1 || true)"
  line="${line#*=}"
  line="${line%$'\r'}"
  line="${line%\"}"
  line="${line#\"}"
  line="${line%\'}"
  line="${line#\'}"
  printf '%s' "$line"
}

# The compose files `docker compose` uses in this directory, in order, one per
# line. COMPOSE_FILE from the shell wins over .env, as it does for Compose.
#
# The deploy never passes -f itself. A host whose .env sets COMPOSE_FILE to
# layer an override file (a production pin file, a proxy network) would lose
# that layer the moment -f is given, and `up -d` would quietly recreate the
# services without it.
compose_files() {
  local list sep
  list="${COMPOSE_FILE:-$(env_file_value COMPOSE_FILE)}"
  sep="${COMPOSE_PATH_SEPARATOR:-$(env_file_value COMPOSE_PATH_SEPARATOR)}"
  sep="${sep:-:}"
  if [ -n "$list" ]; then
    local IFS="$sep"
    local f
    for f in $list; do [ -n "$f" ] && printf '%s\n' "$f"; done
    return
  fi
  local base
  for base in compose.yaml compose.yml docker-compose.yaml docker-compose.yml; do
    if [ -f "$base" ]; then
      printf '%s\n' "$base"
      local stem="${base%.*}" ext="${base##*.}"
      [ -f "$stem.override.$ext" ] && printf '%s\n' "$stem.override.$ext"
      return
    fi
  done
}

# The image Compose will actually run for a service, after every file in
# COMPOSE_FILE and every variable is applied. Read from the canonical YAML
# `docker compose config` prints (services at two spaces, their keys at four),
# so it needs no YAML or JSON parser on the host. `config --images <service>`
# would be simpler, but it also prints the service's dependencies.
effective_image() {
  docker compose config </dev/null 2>/dev/null | awk -v s="$1" '
    /^[^ ]/ { insvc = ($0 ~ /^services:/); blk = 0; next }
    insvc && $0 == "  " s ":" { blk = 1; next }
    insvc && /^  [^ ]/ { blk = 0 }
    blk && /^    image: / { v = $0; sub(/^    image: /, "", v); gsub(/["\047]/, "", v); print v; exit }'
}

# The `image:` line a file sets for a service, as "<line number><TAB><value>",
# or nothing. Scoped to that service's block under `services:`, so an image
# named on another service (or a volume of the same name) never matches.
service_image_line() {
  awk -v s="$2" '
    /^[[:space:]]*(#|$)/ { next }
    {
      match($0, /^ */); ind = RLENGTH
      if (ind == 0) { insvc = ($0 ~ /^services:/); svcind = 0; blk = 0; next }
      if (!insvc) next
      if (svcind == 0) svcind = ind
      if (ind == svcind) {
        key = $0; sub(/^ +/, "", key); sub(/:.*/, "", key); gsub(/["\047]/, "", key)
        blk = (key == s); childind = 0; next
      }
      if (!blk) next
      if (childind == 0) childind = ind
      if (ind == childind && $0 ~ /^ +image:/) {
        v = $0; sub(/^ +image:[ ]*/, "", v); sub(/[ ]+#.*$/, "", v); sub(/[ ]+$/, "", v); gsub(/["\047]/, "", v)
        print NR "\t" v
      }
    }' "$1" | tail -n 1
}

# The file that decides a service's image: the last file in COMPOSE_FILE that
# sets one for it, since a later file overrides an earlier one.
pin_file_for() {
  local service="$1" found="" f
  while IFS= read -r f; do
    [ -f "$f" ] || continue
    [ -n "$(service_image_line "$f" "$service")" ] && found="$f"
  done < <(compose_files)
  [ -n "$found" ] || return 1
  printf '%s\n' "$found"
}

# The ref that file writes for the service, e.g. alexwohlbruck/barrelman:0.10.38.
pinned_ref() {
  service_image_line "$1" "$2" | cut -f 2-
}

# Rewrite the service's image line in the file to `ref`, keeping indentation,
# quotes and any trailing comment.
set_pin() {
  local file="$1" service="$2" ref="$3" line
  line="$(service_image_line "$file" "$service" | cut -f 1)"
  [ -n "$line" ] || return 1
  sed -E -i "${line}s#^([[:space:]]*image:[[:space:]]*[\"']?)[^\"'[:space:]#]+#\1$ref#" "$file"
}

# Whether a ref belongs to this repository (and not one it prefixes:
# alexwohlbruck/barrelman is not alexwohlbruck/barrelman-ops).
ref_in_repo() {
  case "$1" in
    "$2" | "$2:"* | "$2@"*) return 0 ;;
    *) return 1 ;;
  esac
}

# The ref to restore for a service: what its file pinned, unless that was a
# floating tag (none, or :latest), which may have moved since. Then the exact
# digest of the image the container was running, when Docker knows it.
restorable_ref() {
  local service="$1" image="$2" pinned="$3"
  case "$pinned" in
    "$image" | "$image:latest") ;;
    *)
      printf '%s\n' "$pinned"
      return
      ;;
  esac
  local cid image_id digest
  cid="$(docker compose ps -q "$service" </dev/null 2>/dev/null | head -n 1 || true)"
  if [ -n "$cid" ]; then
    image_id="$(docker inspect -f '{{.Image}}' "$cid" 2>/dev/null || true)"
    digest="$(docker image inspect -f '{{range .RepoDigests}}{{println .}}{{end}}' "$image_id" 2>/dev/null |
      grep -E "^$image@" | head -n 1 || true)"
    if [ -n "$digest" ]; then
      printf '%s\n' "$digest"
      return
    fi
  fi
  printf '%s\n' "$pinned"
}

# ── Lock, job gate, health ───────────────────────────────────────────────────

LOCK_TOKEN=""

take_lock() {
  local what="$1"
  LOCK_TOKEN="holder=$DEPLOY_HOLDER time=$(date -u +%Y-%m-%dT%H:%M:%SZ) pid=$$ action=$what"
  if (set -o noclobber; printf '%s\n' "$LOCK_TOKEN" >"$LOCK_FILE") 2>/dev/null; then
    trap release_lock EXIT
    return
  fi
  if [ "$DEPLOY_FORCE" = "1" ]; then
    say "taking the lock by force from: $(cat "$LOCK_FILE" 2>/dev/null || echo unknown)"
    printf '%s\n' "$LOCK_TOKEN" >"$LOCK_FILE"
    trap release_lock EXIT
    return
  fi
  die "another deploy holds $LOCK_FILE ($(cat "$LOCK_FILE" 2>/dev/null || echo unreadable)). Pass --force if it is stale."
}

# Only removes the lock if it is still ours, so a forced takeover by someone
# else is not undone when this run exits.
release_lock() {
  if [ -n "$LOCK_TOKEN" ] && [ "$(cat "$LOCK_FILE" 2>/dev/null || true)" = "$LOCK_TOKEN" ]; then
    rm -f "$LOCK_FILE"
  fi
}

# The same question the Watchtower pre-update hooks ask: is an ops job running?
# Asked through scripts/deploy-gate.ts in the API container when it is up,
# else straight from ops_jobs. Fails closed: no answer is not a "no".
assert_no_jobs_running() {
  if docker compose exec -T barrelman bun run scripts/deploy-gate.ts </dev/null >/dev/null 2>&1; then
    return
  fi
  local running
  running="$(docker compose exec -T barrelman-db psql -U barrelman -d barrelman -tAc \
    "SELECT count(*) FROM ops_jobs WHERE status = 'running'" </dev/null 2>/dev/null | tr -d '[:space:]' || true)"
  case "$running" in
    0) return ;;
    '') die "could not read the ops job queue (API and database both unreachable). Not deploying blind." ;;
    *) die "$running ops job(s) running. An import cannot be resumed after a restart; try again when it finishes." ;;
  esac
}

health_ok() {
  local accept="r.ok && j.status === 'ok'"
  [ "$DEPLOY_ALLOW_DEGRADED" = "1" ] && accept="r.ok && (j.status === 'ok' || j.status === 'degraded')"
  # The API image has bun and no curl, the same reason its healthcheck is bun.
  docker compose exec -T barrelman bun -e \
    "fetch('http://127.0.0.1:5001/health').then(async r => { const j = await r.json(); process.exit($accept ? 0 : 1) }, () => process.exit(1))" \
    </dev/null >/dev/null 2>&1
}

wait_healthy() {
  local deadline=$((SECONDS + DEPLOY_TIMEOUT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if health_ok; then return 0; fi
    sleep 5
  done
  return 1
}

log_line() {
  printf '%s\t%s\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$DEPLOY_HOLDER" "$*" >>"$LOG_FILE"
}

# ── Applying and verifying refs ──────────────────────────────────────────────

# Seconds a recreated container must stay up, without restarting, to count as
# stable. A crash-looping worker is "running" between restarts, and the API's
# /health says nothing about the worker or the database.
DEPLOY_STABLE_SECONDS="${DEPLOY_STABLE_SECONDS:-20}"

service_container() {
  docker compose ps -q "$1" </dev/null 2>/dev/null | head -n 1
}

service_running() {
  [ -n "$(docker compose ps --status running -q "$1" </dev/null 2>/dev/null)" ]
}

container_state() {
  docker inspect -f '{{.RestartCount}} {{.State.StartedAt}}' "$1" 2>/dev/null
}

# Read component|service|file|ref lines on stdin, write each pin, check
# Compose now resolves each service to its ref, then pull (when asked) and
# recreate those services.
apply_refs() {
  local pull="$1" services=() component service file ref effective
  local lines=()
  while IFS= read -r line; do
    [ -n "$line" ] && lines+=("$line")
  done
  [ "${#lines[@]}" -gt 0 ] || die "nothing to apply"

  for line in "${lines[@]}"; do
    IFS='|' read -r component service file ref <<<"$line"
    set_pin "$file" "$service" "$ref" || { say "$file no longer sets an image for $service"; return 1; }
    services+=("$service")
  done
  # The edit has to be what Compose runs. If a later file still overrides it,
  # stop here rather than restart onto the old image and call it a success.
  for line in "${lines[@]}"; do
    IFS='|' read -r component service file ref <<<"$line"
    effective="$(effective_image "$service")"
    if [ "$effective" != "$ref" ]; then
      say "$service still resolves to ${effective:-nothing} after pinning $ref in $file"
      return 1
    fi
  done

  if [ "$pull" = "1" ]; then
    docker compose pull "${services[@]}" </dev/null || return 1
  fi
  # --no-deps: recreate exactly these. Without it Compose reconciles their
  # dependencies too, and a deploy meant for the API bounces the database.
  docker compose up -d --no-deps "${services[@]}" </dev/null || return 1
}

# After apply_refs: /health passes, every named service runs the ref it was
# given, and none of them restarts during DEPLOY_STABLE_SECONDS.
verify_refs() {
  local lines=() line component service file ref cid running
  while IFS= read -r line; do
    [ -n "$line" ] && lines+=("$line")
  done

  if ! wait_healthy; then
    say "/health did not pass within ${DEPLOY_TIMEOUT}s"
    return 1
  fi

  local -A before=() cids=()
  for line in "${lines[@]}"; do
    IFS='|' read -r component service file ref <<<"$line"
    cid="$(service_container "$service")"
    if [ -z "$cid" ]; then
      say "$service has no container"
      return 1
    fi
    running="$(docker inspect -f '{{.Config.Image}}' "$cid" 2>/dev/null || true)"
    if [ "$running" != "$ref" ]; then
      say "$service is running ${running:-nothing}, not $ref"
      return 1
    fi
    cids[$service]="$cid"
    before[$service]="$(container_state "$cid")"
  done

  sleep "$DEPLOY_STABLE_SECONDS"
  for line in "${lines[@]}"; do
    IFS='|' read -r component service file ref <<<"$line"
    if ! service_running "$service"; then
      say "$service is not running"
      return 1
    fi
    cid="$(service_container "$service")"
    if [ "$cid" != "${cids[$service]}" ] || [ "$(container_state "$cid")" != "${before[$service]}" ]; then
      say "$service restarted within ${DEPLOY_STABLE_SECONDS}s"
      return 1
    fi
  done
}

# ── Commands ─────────────────────────────────────────────────────────────────

cmd_deploy() {
  local version="$1" components="$2"
  cd "$DEPLOY_PATH"
  take_lock "deploy $version"
  assert_no_jobs_running

  local previous to_apply from_summary="" component service image file pinned effective ref
  previous="$(mktemp)"
  to_apply="$(mktemp)"
  local backed_up=" "
  IFS=',' read -ra list <<<"$components"
  for component in "${list[@]}"; do
    service="$(component_service "$component")" || die "unknown component: $component"
    image="$(component_image "$component")"

    # What Compose would run now, with every override applied.
    effective="$(effective_image "$service")"
    [ -n "$effective" ] || die "docker compose config has no image for $service"
    file="$(pin_file_for "$service")" ||
      die "no compose file in use sets an image for $service (looked in: $(compose_files | tr '\n' ' '))"
    pinned="$(pinned_ref "$file" "$service")"
    if ! ref_in_repo "$effective" "$image"; then
      die "$service runs $effective (set in $file), not $image. That is usually a hotfix someone pinned by hand, so this deploy leaves it alone. Remove that pin, or deploy $service by hand."
    fi
    if [ "$pinned" != "$effective" ]; then
      die "$file sets $service to \"$pinned\", which Compose resolves to $effective (a variable?). Edit it by hand."
    fi

    ref="$(restorable_ref "$service" "$image" "$pinned")"
    printf '%s|%s|%s|%s\n' "$component" "$service" "$file" "$ref" >>"$previous"
    printf '%s|%s|%s|%s\n' "$component" "$service" "$file" "$image:$version" >>"$to_apply"
    from_summary+="$component=${pinned##*/} "
    case "$backed_up" in
      *" $file "*) ;;
      *)
        # Never overwrite: a second run for the same version would otherwise
        # replace the original with an already-edited copy.
        [ -e "$file.bak-pre-$version" ] || cp -p "$file" "$file.bak-pre-$version"
        backed_up+="$file "
        ;;
    esac
    say "$service: $pinned -> $image:$version ($file)"
  done

  if apply_refs 1 <"$to_apply" && verify_refs <"$to_apply"; then
    mv "$previous" "$STATE_FILE"
    rm -f "$to_apply"
    log_line "deploy ${components} from ${from_summary% } to $version ok"
    say "healthy on $version"
    return 0
  fi

  say "deploy failed; rolling back to the recorded refs"
  if apply_refs 0 <"$previous" && verify_refs <"$previous"; then
    log_line "deploy ${components} from ${from_summary% } to $version FAILED, rolled back"
    say "rolled back and healthy"
  else
    log_line "deploy ${components} from ${from_summary% } to $version FAILED, rollback unhealthy"
    say "rollback did not come back healthy either. Previous refs are in $previous"
  fi
  docker compose logs --tail 40 barrelman </dev/null >&2 2>&1 || true
  rm -f "$to_apply"
  return 1
}

cmd_rollback() {
  cd "$DEPLOY_PATH"
  [ -s "$STATE_FILE" ] || die "no recorded refs in $STATE_FILE; nothing to roll back to"
  take_lock "rollback"
  assert_no_jobs_running

  local summary="" component service file ref
  while IFS='|' read -r component service file ref; do
    [ -n "$component" ] || continue
    say "$service: -> $ref ($file)"
    summary+="$component=${ref##*/} "
  done <"$STATE_FILE"

  # The recorded refs were running here before, so they are local already:
  # no pull. If one has been pruned since, `up` fetches it.
  if apply_refs 0 <"$STATE_FILE" && verify_refs <"$STATE_FILE"; then
    log_line "rollback to ${summary% } ok"
    say "rolled back and healthy"
    return 0
  fi
  log_line "rollback to ${summary% } unhealthy"
  docker compose logs --tail 40 barrelman </dev/null >&2 2>&1 || true
  return 1
}

main() {
  case "${1:-}" in
    deploy)
      [ $# -eq 3 ] || die "usage: deploy-remote.sh deploy <version> <components>"
      cmd_deploy "$2" "$3"
      ;;
    rollback) cmd_rollback ;;
    *) die "usage: deploy-remote.sh deploy <version> <components> | rollback" ;;
  esac
}

# Sourcing defines the functions without running anything (the tests do that).
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
