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

# 1+2. Two things need root, and this script must NOT be run as root, because
# the pelias CLI will not run as uid 0. So check for them and report what is
# missing, instead of calling sudo from an account that usually cannot. Run these
# as root once, then run this script as a normal user.
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
# Listed one by one instead of using `pelias download all`. The CLI's `all`
# target also runs a `transit` service, which this stack does not define, so it
# prints `no such service: transit`. That is harmless here, because the CLI runs
# each source in the background, but see the import step below where it is not.
# Downloaded in parallel, as `all` does, since this is the slowest step.
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
# Not `pelias import all`. That target runs the importers one after another and
# includes `transit`, which this stack does not define, so the CLI stops there
# with `no such service: transit` and exits with an error. With `set -e` that
# killed this script after the long import had finished but before step 8, so
# the run failed and the API was never started.
log "importing sources into Elasticsearch"
for src in wof oa osm polylines; do
  log "  importing $src"
  pelias import "$src"
done

# 8. Starting the API is left to you.
#
# It has to be done from the root compose file, naming both the profile and the
# service. Do not use `pelias compose up`: it runs a plain `docker compose up -d`,
# which now starts nothing, because every Pelias service sits behind a profile.
#
# Do not run it from this account either. The command reads ../.env, which is
# mode 600 and owned by root, so this user cannot read it. And `up -d` with no
# service named restarts every service in an active profile. Together that would
# recreate the main stack with an empty BARRELMAN_DB_PASSWORD.
log "index built. Start the API as root:"
echo "    cd $(cd .. && pwd) && docker compose --profile pelias up -d api"
echo
log "then verify: curl 'localhost:4000/v1/autocomplete?text=350+5th+ave'"
