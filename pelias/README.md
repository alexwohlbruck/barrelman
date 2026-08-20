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

Running the containers is the easy part — the index still has to be built, which
is the job below. Measured on Colorado: about **25 minutes** for download,
polylines and import together, producing 7.31M documents and a 1.9 GB index from
9.2 GB of sources. A country-sized region is hours.

## Provisioning a fresh server

Everything below is codified in [`provision.sh`](./provision.sh). It is
idempotent; safe to re-run.

**The `pelias` CLI refuses to run as root** — it derives the container user from
the invoking account and hard-fails on `0:0` with "You are running as root". The
rest of the self-hosting guide runs as root, so this part, and only this part,
needs an ordinary user. `sudo -u` does not satisfy it either: the CLI reads
`id -u ${SUDO_USER-${USER}}`, and under `sudo` from root that is still `root`,
so it refuses with the identical message. Use `su -`.

Two host prerequisites need root, so they are done first and separately.
`provision.sh` checks for both and tells you what to run if either is missing,
rather than reaching for sudo from an account that will not have it.

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
#    Named individually on purpose — see the `transit` note below.
pelias download wof & pelias download oa & pelias download osm & pelias download tiger & wait

# 4. Prepare polylines — THE STEP THAT IS EASY TO MISS.
#    `import polylines` imports the `street` layer from /data/polylines/extract.0sv,
#    but nothing GENERATES that file. This runs Valhalla over the OSM PBFs to
#    build it. Skip this and street-name search silently returns nothing
#    (only the `address` layer gets populated). See the layers note below.
pelias prepare polylines

# 5. Import each source (WOF + OpenAddresses + OSM addresses + polyline streets).
#    NOT `pelias import all` — see the `transit` note below.
for src in wof oa osm polylines; do pelias import "$src"; done
```

```sh
# ── Back as root: start the API ──────────────────────────────────────────────
#
# Naming the service matters twice over. `pelias compose up` runs a bare
# `docker compose up -d` and would start nothing, since every service here is
# behind a profile. But a bare `up -d` is also wrong from the *unprivileged*
# account: it reconciles every service in an active profile, and ../.env is
# chmod 600 and root-owned, so the core stack would be recreated with an empty
# BARRELMAN_DB_PASSWORD.
cd /opt/barrelman && docker compose --profile pelias up -d api
```

### `no such service: transit`

The CLI's `all` targets (`pelias download all`, `pelias import all`) drive a
`transit` service that this stack does not define, so both print:

```
no such service: transit
```

During **download** that is only noise — the CLI backgrounds each source, so the
rest still run. During **import** it is not: `import all` runs its importers in
sequence and hits `transit` last, so the CLI exits non-zero. Under `set -e` that
aborted `provision.sh` *after* a multi-hour import had succeeded but *before* it
started the API — a run that did all the work and still ended in an error.

Both steps above therefore name the sources this stack actually has rather than
using `all`.

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
