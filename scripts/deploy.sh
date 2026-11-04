#!/usr/bin/env bash
# =============================================================================
# Deploy a released barrelman version to a host, over ssh
# =============================================================================
#
# Run from an operator machine, not from the host and not from the console:
#
#   scripts/deploy.sh <host> <version> [--services api,ops,db] [--force]
#                     [--path /opt/barrelman] [--timeout 300] [--allow-degraded]
#
#   <host>       anything ssh accepts: an alias from ~/.ssh/config, user@host
#   <version>    a published release, e.g. 0.10.39 (a leading v is dropped)
#   --services   which images to move. Default api,ops. db restarts Postgres,
#                so it is never included unless named
#   --force      take the deploy lock even if another deploy holds it
#   --path       the deployment directory on the host (default /opt/barrelman)
#   --timeout    seconds to wait for /health after the restart (default 300)
#   --allow-degraded  accept /health "degraded" (an optional engine down)
#
# On the host, in order: takes /opt/barrelman/.deploy.lock, refuses if an ops
# job is running, finds the compose file that pins each image (from the
# COMPOSE_FILE the host's .env sets), backs it up as <file>.bak-pre-<version>,
# records the refs running now, pins <image>:<version>, pulls, recreates only
# the named services, and waits for /health. Success and failure both append a
# line to /opt/barrelman/deploy.log. On failure it puts the recorded refs back.
# scripts/rollback.sh restores the refs recorded by the last successful deploy.
#
# The work happens in scripts/deploy-remote.sh, which this copies to the host.
#
# No console manifest entry, by design: it runs from outside the box and
# restarts the containers the console itself runs in.
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/deploy-ssh.sh
source "$HERE/deploy-ssh.sh"

usage() {
  sed -n '7,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

[ $# -ge 2 ] || usage
HOST="$1"
VERSION="${2#v}"
shift 2

SERVICES="api,ops"
FORCE=0
DEPLOY_PATH=/opt/barrelman
TIMEOUT=300
ALLOW_DEGRADED=0
while [ $# -gt 0 ]; do
  case "$1" in
    --services) SERVICES="${2:?--services needs a value}"; shift 2 ;;
    --services=*) SERVICES="${1#*=}"; shift ;;
    --force) FORCE=1; shift ;;
    --path) DEPLOY_PATH="${2:?--path needs a value}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?--timeout needs a value}"; shift 2 ;;
    --allow-degraded) ALLOW_DEGRADED=1; shift ;;
    -h | --help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-.][A-Za-z0-9.]+)?$ ]] ||
  { echo "not a release version: $VERSION" >&2; exit 2; }
[[ "$SERVICES" =~ ^(api|ops|db)(,(api|ops|db))*$ ]] ||
  { echo "--services takes a comma-separated list of api, ops, db" >&2; exit 2; }
[[ "$TIMEOUT" =~ ^[0-9]+$ ]] || { echo "--timeout takes whole seconds" >&2; exit 2; }

run_remote "$HOST" "$DEPLOY_PATH" "$FORCE" "$TIMEOUT" "$ALLOW_DEGRADED" deploy "$VERSION" "$SERVICES"
