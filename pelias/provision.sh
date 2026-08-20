#!/usr/bin/env bash
#
# Provision a fresh Pelias geocoder for barrelman. Idempotent — safe to re-run.
# Run from barrelman/pelias. See README.md for the annotated manual equivalent.
#
# Prereq: the barrelman stack must be up first (this joins its docker network,
# `barrelman_default`, so barrelman can reach pelias_api:4000).
#
set -euo pipefail
cd "$(dirname "$0")"

PELIAS_DOCKER_DIR=${PELIAS_DOCKER_DIR:-/opt/pelias-docker}

log() { echo "[provision] $*"; }

# 1+2. Host prereqs, both of which need root — and this script must NOT be run
# as root, because the pelias CLI refuses to run as 0:0. Rather than calling
# sudo from an account that generally will not have it, check and report: the
# operator runs these two as root once, then runs this script as a normal user.
MISSING=()
if [ "$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)" -lt 262144 ]; then
  MISSING+=("sysctl -w vm.max_map_count=262144 && echo 'vm.max_map_count=262144' > /etc/sysctl.d/99-pelias-es.conf")
fi
if ! command -v pelias >/dev/null 2>&1; then
  MISSING+=("git clone https://github.com/pelias/docker.git $PELIAS_DOCKER_DIR && ln -sf $PELIAS_DOCKER_DIR/pelias /usr/local/bin/pelias")
fi
if [ ${#MISSING[@]} -gt 0 ]; then
  log "missing host prerequisites. Run these as root, then re-run this script:"
  for cmd in "${MISSING[@]}"; do echo "    $cmd"; done
  exit 1
fi

if [ "$(id -u)" = "0" ]; then
  log "refusing to run as root — the pelias CLI does too, and 'sudo -u' does not"
  log "help (it reads SUDO_USER). See README.md; use: su - <user> -c '...'"
  exit 1
fi

# 3. Env: created from template if missing; DATA_DIR must be an absolute path.
if [ ! -f .env ]; then
  log "no .env — creating from .env.example; set DATA_DIR to an absolute path and re-run"
  cp .env.example .env
  exit 1
fi

# 4. Elasticsearch + schema. `elastic create` is a no-op if the index exists.
log "starting Elasticsearch"
pelias compose pull
pelias elastic start
pelias elastic wait
pelias elastic create || log "elastic index already exists — continuing"

# 5. Download the configured sources (WOF, OpenAddresses, OSM PBFs, TIGER).
#
# Named individually rather than `pelias download all`: the CLI's `all` target
# also drives a `transit` service, which this stack does not define, so it
# prints `no such service: transit`. Harmless during download (the CLI
# backgrounds each source), but see the import step below, where it is not.
# Downloaded in parallel, as `all` does — this is the long pole.
log "downloading sources (this is large; resumable)"
dl_pids=()
for src in wof oa osm tiger; do
  pelias download "$src" &
  dl_pids+=($!)
done
for pid in "${dl_pids[@]}"; do wait "$pid"; done

# 6. Prepare polylines — generates /data/polylines/extract.0sv via Valhalla from
#    the OSM PBFs. WITHOUT this the `street` layer stays empty and street-name
#    search returns nothing. This is the step `import all` alone does not cover.
log "preparing polylines (Valhalla; CPU/RAM heavy)"
pelias prepare polylines

# 7. Import each source into ES (WOF + OpenAddresses + OSM addresses + streets).
#
# NOT `pelias import all`. That target calls its importers in sequence and
# includes `transit`, which this stack does not define; the CLI aborts there
# with `no such service: transit` and returns non-zero. Under `set -e` that
# killed this script *after* a multi-hour import had succeeded but *before*
# step 8, so the run ended in an error with the API never started.
log "importing sources into Elasticsearch"
for src in wof oa osm polylines; do
  log "  importing $src"
  pelias import "$src"
done

# 8. Starting the API is left to the operator, deliberately.
#
# It has to happen from the root compose file with the profile named — NOT
# `pelias compose up`, which issues a bare `docker compose up -d` that starts
# nothing now every Pelias service sits behind a profile. But that command also
# has to read ../.env, which is chmod 600 and root-owned, and a bare `up -d`
# with no service named touches every service in an active profile — so running
# it from this unprivileged account would reconcile the core stack with an
# unreadable .env, i.e. an empty BARRELMAN_DB_PASSWORD.
log "index built. Start the API as root:"
echo "    cd $(cd .. && pwd) && docker compose --profile pelias up -d api"
echo
log "then verify: curl 'localhost:4000/v1/autocomplete?text=350+5th+ave'"
