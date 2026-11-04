#!/usr/bin/env bash
# =============================================================================
# Roll a host back to the image refs recorded by its last successful deploy
# =============================================================================
#
# Run from an operator machine:
#
#   scripts/rollback.sh <host> [--force] [--path /opt/barrelman] [--timeout 300]
#                       [--allow-degraded]
#
# Restores the refs scripts/deploy.sh recorded in /opt/barrelman/.deploy-previous
# before its last successful run: writes them back into the compose file that
# pins each image, recreates those services with --no-deps, waits for /health
# and appends a line to /opt/barrelman/deploy.log. Takes the same lock and
# refuses while an ops job is running, like a deploy. Running it twice is
# harmless: the recorded refs don't change until the next successful deploy.
#
# No console manifest entry: it runs from outside the box (see deploy.sh).
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/deploy-ssh.sh
source "$HERE/deploy-ssh.sh"

usage() {
  sed -n '7,9p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

[ $# -ge 1 ] || usage
HOST="$1"
shift

FORCE=0
DEPLOY_PATH=/opt/barrelman
TIMEOUT=300
ALLOW_DEGRADED=0
while [ $# -gt 0 ]; do
  case "$1" in
    --force) FORCE=1; shift ;;
    --path) DEPLOY_PATH="${2:?--path needs a value}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?--timeout needs a value}"; shift 2 ;;
    --allow-degraded) ALLOW_DEGRADED=1; shift ;;
    -h | --help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[[ "$TIMEOUT" =~ ^[0-9]+$ ]] || { echo "--timeout takes whole seconds" >&2; exit 2; }

run_remote "$HOST" "$DEPLOY_PATH" "$FORCE" "$TIMEOUT" "$ALLOW_DEGRADED" rollback
