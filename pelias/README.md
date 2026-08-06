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
is the multi-hour job below.

## Provisioning a fresh server

Everything below is codified in [`provision.sh`](./provision.sh) — run it from
this directory. It is idempotent; safe to re-run. The manual equivalent:

```sh
# 0. Host prereq — Elasticsearch needs a high mmap count, or ES won't boot.
sudo sysctl -w vm.max_map_count=262144
echo 'vm.max_map_count=262144' | sudo tee /etc/sysctl.d/99-pelias-es.conf

# 1. The `pelias` CLI (this repo pins the stack; the CLI orchestrates it).
git clone https://github.com/pelias/docker.git /opt/pelias-docker
sudo ln -sf /opt/pelias-docker/pelias /usr/local/bin/pelias

# 2. Env — copy the template, set DATA_DIR to an absolute path under here.
#    Keep COMPOSE_PROJECT_NAME=barrelman so the CLI drives the same stack the
#    root compose file does, rather than a second one with clashing names.
cp .env.example .env && $EDITOR .env      # DATA_DIR=<abs>/pelias/data, DOCKER_USER=1000:1000

# 3. Elasticsearch up + schema.
pelias compose pull
pelias elastic start && pelias elastic wait
pelias elastic create

# 4. Download all sources (WOF, OpenAddresses, OSM PBFs, TIGER for interpolation).
pelias download all

# 5. Prepare polylines — THE STEP THAT IS EASY TO MISS.
#    `import all` imports the `street` layer from /data/polylines/extract.0sv,
#    but nothing GENERATES that file. This runs Valhalla over the OSM PBFs to
#    build it. Skip this and street-name search silently returns nothing
#    (only the `address` layer gets populated). See the layers note below.
pelias prepare polylines

# 6. Import everything (WOF + OpenAddresses + OSM addresses + polyline streets).
pelias import all

# 7. Start the API — from the repo root, with the profile.
#    NOT `pelias compose up`: that runs a bare `docker compose up -d`, and every
#    service here is behind a profile, so it would start nothing. The CLI's
#    service-specific commands (`pelias elastic start`) are unaffected — naming a
#    service explicitly activates its profile.
cd .. && docker compose --profile pelias up -d
```

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
region, define it in barrelman (see [`docs/REGIONS.md`](../docs/REGIONS.md) —
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
