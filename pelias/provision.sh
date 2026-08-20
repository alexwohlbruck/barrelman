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

# 1. Host prereq: Elasticsearch refuses to boot without a high mmap count.
if [ "$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)" -lt 262144 ]; then
  log "raising vm.max_map_count to 262144 (needs sudo)"
  sudo sysctl -w vm.max_map_count=262144
  echo 'vm.max_map_count=262144' | sudo tee /etc/sysctl.d/99-pelias-es.conf >/dev/null
fi

# 2. The pelias CLI (orchestrates the stack pinned by this dir's compose+config).
if ! command -v pelias >/dev/null 2>&1; then
  log "installing pelias CLI -> $PELIAS_DOCKER_DIR"
  [ -d "$PELIAS_DOCKER_DIR" ] || sudo git clone https://github.com/pelias/docker.git "$PELIAS_DOCKER_DIR"
  sudo ln -sf "$PELIAS_DOCKER_DIR/pelias" /usr/local/bin/pelias
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

# 8. Bring up the API — via the root compose file, with the profile.
# Not `pelias compose up`: that issues a bare `docker compose up -d`, and every
# Pelias service is behind a profile now, so it would start nothing. The CLI's
# service-specific commands above are unaffected — naming a service explicitly
# activates its profile.
log "starting Pelias API"
(cd .. && docker compose --profile pelias up -d)

log "done — verify: curl 'localhost:4000/v1/autocomplete?text=350+5th+ave' | jq '.features[].properties.layer'"
