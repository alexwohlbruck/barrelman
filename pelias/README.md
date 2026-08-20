# Pelias (address geocoder)

Barrelman's own PostGIS layers cover POIs/categories, but street **addresses**
have no `name` and aren't searchable there. Pelias fills that gap: barrelman's
`forwardGeocode` (see `src/services/geocode.service.ts`) proxies `pelias_api:4000`
and folds address + street results into `/search`.

Pelias is **part of the barrelman stack** — barrelman's root
`docker-compose.yml` pulls this file in with `include:`, so both run as one
Compose project on one network, and the API reaches `pelias_api:4000`
(barrelman's `PELIAS_URL` default) with no external-network wiring.

Its services sit behind profiles, so they don't start with a plain
`docker compose up -d`. From the **repo root**:

```sh
docker compose --profile pelias up -d                        # api + elasticsearch
docker compose --profile pelias --profile pelias-full up -d  # + libpostal / pip / interpolation
```

| Profile | Services |
|---|---|
| `pelias` | `api`, `elasticsearch` — all barrelman's `/v1/autocomplete` needs |
| `pelias-full` | `libpostal`, `placeholder`, `interpolation`, `pip` — for `/v1/search` & `/v1/reverse` (~4GB) |
| `pelias-tools` | one-shot importers; started by `docker compose run`, never by `up` |

Starting the containers is the easy part. The index still has to be built, which
is the job below. On Colorado that took about **25 minutes** for the download,
polylines and import together. It produced 7.31M documents and a 1.9 GB index
from 9.2 GB of downloaded sources. A whole country takes hours.

## Provisioning a fresh server

Everything below is codified in [`provision.sh`](./provision.sh). It is
idempotent; safe to re-run.

**The `pelias` CLI will not run as root.** It works out which user to run as
from the account that invoked it, and stops with "You are running as root" if
that is uid 0. The rest of the self-hosting guide runs as root, so this part,
and only this part, needs an ordinary user.

`sudo -u` does not help. The CLI reads `id -u ${SUDO_USER-${USER}}`, and when you
use `sudo` from root that is still `root`, so you get the same error. Use `su -`.

Two things have to be set up as root before you start. `provision.sh` checks for
both and tells you what to run if either is missing. It does not call `sudo`
itself, because the account you run it from will usually not have it.

```sh
# ── As root, once ────────────────────────────────────────────────────────────

# Elasticsearch needs a high mmap count or it will not boot. Check first —
# Ubuntu 24.04 already ships 1048576 — and only raise it if yours is lower.
sysctl vm.max_map_count
sysctl -w vm.max_map_count=262144
echo 'vm.max_map_count=262144' > /etc/sysctl.d/99-pelias-es.conf

# The pelias CLI (this repo pins the stack; the CLI orchestrates it).
git clone https://github.com/pelias/docker.git /opt/pelias-docker
ln -sf /opt/pelias-docker/pelias /usr/local/bin/pelias

# The unprivileged account that will own the index. uid 1000 is deliberate:
# Elasticsearch runs as uid 1000 and owns pelias/data, so matching them keeps a
# single owner.
useradd -m -u 1000 -s /bin/bash barrelman
usermod -aG docker barrelman
chown -R 1000:1000 /opt/barrelman/pelias
```

```sh
# ── As that user: su - barrelman ─────────────────────────────────────────────

# 1. Env — copy the template, set DATA_DIR to an absolute path under here.
#    Keep COMPOSE_PROJECT_NAME=barrelman so the CLI drives the same stack the
#    root compose file does, rather than a second one with clashing names.
#    Do NOT set DOCKER_USER — it is deprecated and the CLI overrides it.
cp .env.example .env && $EDITOR .env      # DATA_DIR=<abs>/pelias/data

# 2. Elasticsearch up + schema.
pelias compose pull
pelias elastic start && pelias elastic wait
pelias elastic create

# 3. Download the sources (WOF, OpenAddresses, OSM PBFs, TIGER for interpolation).
#    Listed one by one. See the `transit` note below for why.
pelias download wof & pelias download oa & pelias download osm & pelias download tiger & wait

# 4. Prepare polylines — THE STEP THAT IS EASY TO MISS.
#    `import polylines` imports the `street` layer from /data/polylines/extract.0sv,
#    but nothing GENERATES that file. This runs Valhalla over the OSM PBFs to
#    build it. Skip this and street-name search silently returns nothing
#    (only the `address` layer gets populated). See the layers note below.
pelias prepare polylines

# 5. Import each source (WOF + OpenAddresses + OSM addresses + polyline streets).
#    Not `pelias import all`. See the `transit` note below for why.
for src in wof oa osm polylines; do pelias import "$src"; done
```

```sh
# ── Back as root: start the API ──────────────────────────────────────────────
#
# Name both the profile and the service. `pelias compose up` would start
# nothing, because every service here sits behind a profile. And `up -d` without
# a service name restarts everything in an active profile, which from the
# unprivileged account would recreate the main stack with an empty
# BARRELMAN_DB_PASSWORD. That account cannot read ../.env, which is mode 600 and
# owned by root.
cd /opt/barrelman && docker compose --profile pelias up -d api
```

### `no such service: transit`

The CLI's `all` targets (`pelias download all`, `pelias import all`) drive a
`transit` service that this stack does not define, so both print:

```
no such service: transit
```

During **download** this is harmless. The CLI runs each source in the
background, so the others still finish. During **import** it is not. `import all`
runs the importers one after another and reaches `transit` last, so the CLI exits
with an error. With `set -e`, that stopped `provision.sh` after the long import
had finished but before it started the API. The run did all the work and still
ended in failure.

So both steps above list the sources this stack defines, and avoid `all`.

### Re-importing into an already-running API

The steps above assume a fresh box (the API starts *after* the import). If you
import a new layer into an **already-running** `pelias_api` (e.g. adding
polylines later), the API won't see it until it's recreated — and
`pelias compose up api` is a no-op when the container is already running. Force
it: `docker restart pelias_api`. Verify with
`curl 'localhost:4000/v1/autocomplete?text=providence&layers=street'`.

## Regions

Coverage lives in the `imports` block of [`pelias.json`](./pelias.json):
`imports.openstreetmap.download/import` (PBFs), `imports.openaddresses.files`,
`imports.whosonfirst.importPlace`, `imports.interpolation.download.tiger.states`.

**Don't hand-edit that block** — it is generated from barrelman's region
registry so the geocoder's coverage always matches the rest of the pipeline:

```sh
bun run scripts/generate-pelias-config.ts   # rewrites imports from REGIONS
```

Everything outside `imports` (logger, esclient, api) is preserved. To add a
region, define it in barrelman (see [the region guide](https://docs.barrelman.dev/self-hosting/regions) —
adding one by name fills in its OpenAddresses files and TIGER state codes
automatically), regenerate, then re-run steps 4-6 above.

## The `layers=address,street` gotcha

Pelias returns **nothing** when a requested `layers` value has zero docs in the
index — e.g. `layers=address,street` returns 0 if the `street` layer was never
imported, even though millions of `address` docs exist. Two independent guards:

1. **Import polylines** (step 5) so the `street` layer actually has data — the
   correct fix, done here.
2. barrelman's `forwardGeocode` falls back to an **unfiltered** autocomplete when
   a layer-filtered call comes back empty, so addresses still surface even if a
   layer is missing (defensive; PR #11).

## Profiles

Default (lean) run brings up only `elasticsearch` + `api`. The heavier
`libpostal` / `placeholder` / `pip` / `interpolation` services are opt-in — they
improve parsing and house-number interpolation but cost RAM. Enable per your
box's resources.
