# Regions — choosing what data to import

Barrelman doesn't import "the world" by default. A **region** is a named bundle
of data sources — an OSM extract, a transit search area, address files — and the
`REGIONS` environment variable picks which ones the pipeline pulls in.

```dotenv
REGIONS=colorado             # one region
REGIONS=north-carolina,nyc-metro   # several, merged
REGIONS=global               # planet OSM + every transit feed
```

Every importer — OSM, GTFS, GBFS and the Pelias geocoder — resolves what to
fetch from this one variable. There is no second place to configure coverage.

---

## Quick start: add a region by name

Type a place name, get a complete region definition. Two steps:

### 1. Fetch the boundary catalog (once)

```bash
docker compose exec barrelman-ops bun run scripts/fetch-boundaries.ts
```

This downloads [Geofabrik's extract index][gf-index] — 555 published extracts
(countries, their subregions, and all 53 US state-level extracts) with each
one's real political boundary, download URLs and ISO codes — into the
`boundary_catalog` table. No API key, no configuration.

It's also available in the admin console as **Scripts → Config Generation →
Fetch Boundary Catalog**.

### 2. Create the region

In the console: **Regions → Add by name**, type `colorado`, pick the match.
Review the auto-filled definition and save.

From the CLI, to see what's available:

```bash
docker compose exec barrelman-ops bun run scripts/fetch-boundaries.ts --skip-fetch --search colorado
```

Then set it live and import:

```dotenv
REGIONS=colorado
```

`REGIONS` is read by the importers in `barrelman-ops`, so recreate that service
after changing it — `docker compose up -d barrelman-ops`.

### What gets filled in automatically

| Field | Value for `colorado` | Source |
|---|---|---|
| `osmExtracts` | `.../us/colorado-latest.osm.pbf` | Geofabrik index |
| `osmReplication` | `.../us/colorado-updates/` | Geofabrik index |
| `bbox` | `-109.06, 36.99, -102.04, 41.00` | Computed from the real boundary polygon |
| `gtfsRegion` | the same bbox | Used as the Transitland feed search area |
| `pelias.tigerStates` | `[8]` | ISO `US-CO` → Census FIPS |
| `pelias.openaddresses` | 55 county CSV paths | OpenAddresses source listing |

Two things are deliberately **not** guessed:

- **`pelias.wofIds`** is left empty. There's no zero-config lookup from an ISO
  code to a Who's-on-First place id, and a wrong id silently imports the wrong
  place. Empty just means Pelias imports the country's full admin hierarchy —
  slower, but correct. Narrow it later from [spelunker.whosonfirst.org][wof].
- **OpenAddresses coverage is uneven and is read, not assumed.** New York has a
  single `statewide` file; Colorado has 55 county files and *no* statewide file.
  Guessing `us/co/statewide.csv` would silently import zero addresses, so the
  list comes from the actual repository listing.

> The OpenAddresses lookup uses the GitHub API (60 requests/hour
> unauthenticated — one per region). If it's rate-limited you'll get a warning
> and an empty list, not a failure; set `GITHUB_TOKEN` and re-resolve.

[gf-index]: https://download.geofabrik.de/index-v1.json
[wof]: https://spelunker.whosonfirst.org

---

## Running the import

Order matters, and each step depends on the one before it.

Run these from the console's **Scripts** page — which gives you streamed logs,
job history, and survival across a dropped SSH session — or inside
`barrelman-ops`, which is the container that carries the docker CLI, osmium and
python that these scripts need. The API container has the scripts but not the
tools.

```bash
docker compose up -d                        # 1. bring the stack up

# then, inside barrelman-ops:
bash scripts/run-import.sh                  # 2. OSM → PostGIS, then rebuild
                                            #    the GraphHopper routing graph

bash scripts/prepare-motis-osm.sh           # 3. repair underground transit
                                            #    platforms → region-transit.osm.pbf

bash scripts/download-gtfs.sh               # 4. transit feeds. Needs GraphHopper
                                            #    from step 2 — walking transfers
                                            #    between stops are routed through it

bash scripts/rebuild-motis.sh               # 5. rebuild the MOTIS timetable +
                                            #    street graph from those feeds

bun run import/import-gbfs-systems.ts       # 6. bikeshare, filtered by the region bbox

bun run scripts/generate-pelias-config.ts   # 7. addresses: regenerate pelias.json…
cd pelias && ./provision.sh                 #    …then provision the geocoder stack
```

For example:

```bash
docker compose exec -d barrelman-ops bash scripts/run-import.sh
docker compose logs -f barrelman-ops
```

Steps 3–7 are optional — skip transit, bikeshare or address search if you don't
need them. Step 2 is not optional.

**If you do steps 3 and 4, step 5 is not optional either.** `motis server` only
serves the pre-built dataset at `/data/data`; it never re-imports when the
config or feeds change, so a plain restart keeps serving the old schedules and
the new feeds never reach riders. Re-run it after every GTFS refresh, and after
any MOTIS version bump.

Rough timings for a single US state: OSM import 20–40 min, GraphHopper graph
~10–20 min, GTFS depends on feed count, MOTIS rebuild a few minutes, Pelias
several hours (it's a separate stack — see
[`pelias/README.md`](../pelias/README.md)).

### Sizing

`GRAPHHOPPER_JAVA_OPTS` must give the JVM more heap than the graph cache on
disk — GraphHopper loads the graph into heap rather than memory-mapping it, and
an undersized heap dies before it logs its version, so the failure looks like a
crash rather than an out-of-memory. Check with `du -sh` on the `graph-cache`
directory and raise `-Xmx` whenever `REGIONS` grows.

| Scale | DB size | RAM | Disk |
|---|---|---|---|
| One US state | ~10 GB | 2 GB | 20 GB |
| Full United States | ~60 GB | 8 GB | 120 GB |
| Europe | ~100 GB | 16 GB | 200 GB |

---

## Where region definitions live

Two layers, DB first:

1. **`import_regions` table** — the live source of truth, edited from the admin
   console. Seeded from the JSON file on first boot.
2. **[`config/regions.json`](../config/regions.json)** — the shipped defaults
   and the fallback used when the table is empty or the database is unreachable.

`resolveRegions()` ([`src/config/regions.ts`](../src/config/regions.ts)) reads
the table and falls back to the file, so a fresh checkout works with no database
and an operator's console edits win once there is one.

Bash scripts read resolved values through a small CLI rather than re-parsing the
config. It's also the quickest way to check what an instance will actually
import:

```bash
docker compose exec barrelman-ops bun run src/config/regions.ts summary        # what's currently selected
docker compose exec barrelman-ops bun run src/config/regions.ts osm-extracts   # one URL per line
docker compose exec barrelman-ops bun run src/config/regions.ts bbox           # w,s,e,n
```

### Field reference

| Field | Purpose |
|---|---|
| `label` | Display name |
| `osmExtracts` | PBF URLs. Several are downloaded and `osmium merge`d into one extract |
| `osmReplication` | Geofabrik `-updates/` URLs, for incremental OSM updates |
| `bbox` | `[west, south, east, north]`. Filters GBFS stations; also the default GTFS search area |
| `gtfsRegion` | Transitland feed search area: a `w,s,e,n` bbox, `global`, or a legacy named token |
| `pelias.openaddresses` | OpenAddresses CSV paths |
| `pelias.wofIds` | Who's-on-First place ids (optional narrowing) |
| `pelias.tigerStates` | Census FIPS codes for US address interpolation |
| `enabled` | Console toggle. A disabled region named in `REGIONS` is an error, not a silent skip |

### Adding a region by hand

Everything above is editable — the boundary catalog is a convenience, not a
requirement. Add an entry to `config/regions.json` (or create it in the console)
and it behaves identically. This is how you build a region that doesn't
correspond to one published extract, e.g. a metro area spanning three states:

```json
"nyc-metro": {
  "label": "NYC Metro (NY/NJ/CT)",
  "osmExtracts": [
    "https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf",
    "https://download.geofabrik.de/north-america/us/new-jersey-latest.osm.pbf",
    "https://download.geofabrik.de/north-america/us/connecticut-latest.osm.pbf"
  ],
  "bbox": [-74.60, 40.30, -73.30, 41.40],
  "gtfsRegion": "-74.3,40.45,-73.7,40.95",
  "pelias": { "openaddresses": ["us/ny/statewide.csv"], "wofIds": [], "tigerStates": [36, 34, 9] }
}
```

Note that the extracts cover three whole states while `bbox` and `gtfsRegion`
are tighter: the OSM import is not clipped, so `geo_places` gets all three
states, while bikeshare and transit feed discovery stay focused on the metro.
**There is no clipping step** — the boundary of your imported data is whatever
the extracts cover.

---

## Troubleshooting

**`Unknown GTFS region "co"`**
`gtfsRegion` accepts a `w,s,e,n` bounding box, `global`, or one of the legacy
tokens (`nc`, `nyc`, `southeast`, `us`). Anything else is rejected rather than
falling through to an unfiltered query that would download the entire ~2,800
feed global catalog. Use the region's own bbox.

**`Region "colorado" is disabled`**
The console toggle is off. Enable it in **Regions**, or drop it from `REGIONS`.

**`Unknown boundary "us/colorado"` when resolving**
The catalog is empty or stale — run `scripts/fetch-boundaries.ts`.

**Transit routing returns nothing, but `/transit/stops` finds stops**
The MOTIS dataset was never rebuilt after the GTFS import. Run
`scripts/rebuild-motis.sh`. Stops live in PostGIS (populated by step 4) while
routing comes from MOTIS's own baked timetable (step 5), so the two can
disagree.

**Address search returns nothing**
Either `pelias.openaddresses` is empty, or the Pelias `street` layer was never
populated. See the layers note in [`pelias/README.md`](../pelias/README.md).

**GraphHopper won't start after adding a region**
The graph outgrew the heap. Raise `-Xmx` in `GRAPHHOPPER_JAVA_OPTS` above the
on-disk `graph-cache` size and re-run `./scripts/rebuild-graphhopper.sh`.

**Incremental OSM updates unavailable**
Not every extract publishes a replication feed. Set `UPDATE_MODE=full` to
re-download and re-import instead.
