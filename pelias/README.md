# Pelias (address geocoder)

Barrelman's own PostGIS layers cover POIs/categories, but street **addresses**
have no `name` and aren't searchable there. Pelias fills that gap: barrelman's
`forwardGeocode` (see `src/services/geocode.service.ts`) proxies `pelias_api:4000`
and folds address + street results into `/search`.

Pelias is a **separate stack** — it is NOT part of barrelman's root
`docker-compose.yml`. It joins barrelman's docker network (`barrelman_default`,
declared `external` here) so the API can reach `pelias_api:4000` (barrelman's
`PELIAS_URL` default). Bring barrelman up first, then provision Pelias.

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

# 7. Start the API.
pelias compose up
```

### Re-importing into an already-running API

The steps above assume a fresh box (the API starts *after* the import). If you
import a new layer into an **already-running** `pelias_api` (e.g. adding
polylines later), the API won't see it until it's recreated — and
`pelias compose up api` is a no-op when the container is already running. Force
it: `docker restart pelias_api`. Verify with
`curl 'localhost:4000/v1/autocomplete?text=providence&layers=street'`.

## Regions

Coverage is defined entirely in [`pelias.json`](./pelias.json):
`imports.openstreetmap.download/import` (PBFs), `imports.openaddresses.files`,
`imports.whosonfirst.importPlace`, `imports.interpolation.download.tiger.states`.
Currently NC + NY/NJ/CT. To add a region, add its PBF + OA files + WOF place id +
TIGER state code, then re-run steps 4-6.

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
