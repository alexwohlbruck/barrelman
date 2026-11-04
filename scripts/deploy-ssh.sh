#!/usr/bin/env bash
# Shared by scripts/deploy.sh and scripts/rollback.sh: copy
# scripts/deploy-remote.sh to the host and run it there.
#
# Copied rather than piped into `bash -s`, because the remote script runs
# docker commands, and anything that read stdin would swallow the rest of the
# script.

run_remote() {
  local host="$1" path="$2" force="$3" timeout="$4" degraded="$5"
  shift 5
  local here holder env_args remote_args
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  holder="${USER:-$(whoami)}@$(hostname)"

  env_args="$(printf 'DEPLOY_PATH=%q DEPLOY_HOLDER=%q DEPLOY_FORCE=%q DEPLOY_TIMEOUT=%q DEPLOY_ALLOW_DEGRADED=%q' \
    "$path" "$holder" "$force" "$timeout" "$degraded")"
  remote_args="$(printf '%q ' "$@")"

  # shellcheck disable=SC2029  # expanded locally on purpose: these are the args
  ssh -o ConnectTimeout=20 "$host" \
    "f=\$(mktemp) && cat > \"\$f\" && $env_args bash \"\$f\" $remote_args; rc=\$?; rm -f \"\$f\"; exit \$rc" \
    <"$here/deploy-remote.sh"
}
