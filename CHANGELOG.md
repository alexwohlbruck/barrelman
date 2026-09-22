# Changelog

All notable changes to Barrelman are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/).

Entries accumulate under `[Unreleased]` as work lands. Cutting a version stamps
that section with the version and date — `scripts/changelog.sh release X.Y.Z`
does it — and the release pipeline turns it into the GitHub Release notes.

## [Unreleased]

### Added

* **Tile bundles: several map sources in one request.** A map view is thirty to
  sixty tiles *per source*, and a client drawing the detail overlays was asking
  for each of them separately — five requests for every tile of ground. Asking
  for `/tiles/detail/{z}/{x}/{y}` now returns `buildings_3d`, `parking_areas`,
  `bicycle_ways`, `street_trees` and `tree_rows` as one tile, each still its own
  layer under its own name, so a style only needs its `source` changed. Martin's
  comma syntax (`/tiles/buildings_3d,parking_areas/…`) works too, for a set that
  has no name. Measured against the public deployment, a six-tile viewport over
  Manhattan went from 18 requests to 6, and from 2.04 s to 1.14 s warm.

  `street_furniture` is deliberately not in the bundle: it is minzoom 17, so it
  could contribute nothing to a bundle a client reads at z≤16, and including it
  would have taken street furniture off the map. Drop its minzoom to 16 in the
  deployment's `martin-config.yaml` and it joins.

### Changed

* **Tiles are served with `stale-while-revalidate`.** They already carried a
  day's `max-age`; the week of `stale-while-revalidate` behind it means the
  first request after expiry is answered from the edge while the refresh
  happens behind it, instead of making one unlucky user wait out a full origin
  round trip for a byte-identical tile.

### Fixed

* **Building the map detail indexes no longer holds the API off its port.** They
  shipped inside `create-detail-views.sql`, which the API runs synchronously on
  every startup — and each one scans the whole of `geo_places`, so on a
  continental import the first start after the upgrade sat there for minutes
  with nothing answering. They now live in their own
  `import/create-detail-indexes.sql`, built `CONCURRENTLY` out of band by the
  import or by the new **Build Map Detail Tile Indexes** console task. Missing
  them costs speed, never correctness.


### Fixed

* Low-zoom tiles no longer stall the database. Five tile sources read
  `geo_places` with no filter at all, so a tile serialised everything inside it
  rather than the layer's namesake — `parchment_boundaries` at z8 returned
  **83.6 MB in 10.1 seconds**, and z4 and z6 died with a db error after a ten
  second timeout; roads and water at z10 returned 48-69 MB. Because the filter
  was absent they also returned near-identical bytes to one another, so a client
  drawing three of them downloaded the same data three times.

  Those sources are detail overlays, and z14 is where an unfiltered table is
  affordable, so below z14 they now answer `404` instead. Low zoom wants
  generalisation, which a live table query cannot do and which the basemap
  already does. `parchment_boundaries` is the exception: administrative areas
  are few and already indexed for exactly this predicate, so it is served from a
  filtered `admin_boundaries` view and still starts at z4 — z8 went from 83.6 MB
  in 10.1 s to **47 KB in 0.07 s**

* **Rate limiting no longer escalates a busy client into an outage.** Exceeding
  a rate limit returns 429 with a `Retry-After`, which is an instruction: wait,
  then continue. Those 429s were also being counted as abuse strikes, so a
  client that crossed its limit collected 25 of them within seconds and was then
  refused outright for up to half an hour — and because that refusal is *also* a
  429, a client that simply retried kept itself locked out. Rate limiting and
  abuse detection are now separate: only refusals a correct client cannot avoid
  (a bad key, a scope it does not hold, exhausted credits) count towards the
  penalty box.

* **A penalty no longer blacks out an entire account.** Strikes were recorded
  against the account, so one wedged client hammering one endpoint with a
  credential that could not reach it refused every other caller on that account,
  on every endpoint. For a deployment whose busiest consumer is a server-side
  tile proxy, that meant the map went dark because something unrelated asked for
  an admin route. Strikes are now held against the account *at the address that
  earned them* — rotating keys still sheds nothing, which is what the account
  keying was for.

* **`BARRELMAN_IP_RPM` no longer overrides the plan an account is on.** It is a
  backstop against a single abusive host, but at a fixed 3,000 requests a minute
  it sat below what the larger plans sell, and every server-side integration
  presents one address — so it quietly became the real limit. It is now the
  greater of the configured value and the account's own per-minute allowance.
  Anonymous callers are unaffected.

* **Refusals from the penalty box are now recorded.** They return before the
  metering path, so an account being refused every request still showed zero
  rejections in the console — the outage that most needed to be visible was the
  one that left no trace.

* **Tiles are compressed on the way out.** Martin serves them gzipped and
  `fetch()` transparently decompresses, taking the `Content-Encoding` header with
  it, so every tile left the API at roughly twice the size Martin produced — a
  242 KB buildings tile that Martin had already squeezed to 125 KB. A CDN in
  front would re-compress it for the browser and hide the cost on the one hop
  that actually crosses a network: the request filling the edge cache.

* **Parking, tree and street-furniture tiles are no longer a full scan of the
  tile envelope.** These views had no index matching their own predicate, so a
  tile request read every feature in the envelope and discarded what did not
  match: a Tucson z14 parking tile read 18,866 rows to return 436. Partial
  spatial indexes over each view's predicate bring that down to the rows the
  tile actually contains, which matters most on a cold cache, where those
  discarded rows were hundreds of milliseconds of random reads.

### Added

* Portolan can now draw **bus** routes, not just rail. Portolan only draws
  buses where a feed names a `streets` extract — the highway layer it snaps bus
  patterns onto — and without one a feed builds rail-only and its buses simply
  never appear. Upstream, that layer comes from Overpass, which takes minutes
  per city and rate-limits bulk callers. The new **Portolan Street Extracts**
  script cuts it straight out of `geo_places` instead: the same OpenStreetMap
  data Barrelman already imported, with OSM way identity preserved one-to-one,
  which is what Portolan's matcher needs. Denver's 134,794 street ways take
  5.8 seconds. Run it for a feed, then a Portolan patch import, and that
  agency's bus network appears on the map. A feed whose window falls outside
  the imported region is reported and skipped rather than drawn onto an empty
  street layer

### Fixed

* Clicking a station on the transit map no longer 404s. A station's identity
  across systems is its transitland stop key — `<feed-onestop-id>:<stop_id>` —
  and that is what the map holds when it opens a station, but
  `/transit/station/:feedId/:stopId` only accepted a `feed_id`, so the map had
  to resolve the station through transit.land's own API instead. That API needs
  a key, and on an instance that gets its transit data from Barrelman there is
  no reason to hold one — so the lookup failed and every station click 404'd.
  The feed segment now accepts either form. A direct `feed_id` match is still
  tried first, so the common path costs no extra query

## [0.3.6] - 2026-09-15

### Changed

* A fresh install no longer arrives with sample regions already configured.
  `north-carolina`, `nyc-metro` and `global` were seeded into the regions store
  on first boot — repo fixtures, not anything an operator chose. On a real
  instance they sit in the console beside the region you actually import, and
  each is a runnable target on the Scripts page, where picking the wrong one
  replaces the entire dataset with one state. Onboarding is now an empty
  Regions page and **Add by name**, which searches the Geofabrik index for the
  region you want. `BARRELMAN_SEED_SAMPLE_REGIONS=1` restores the fixtures for
  development

* A blank `REGIONS` no longer silently means `north-carolina,nyc-metro`. With
  exactly one region configured it resolves to that one; otherwise it fails and
  names the choices, rather than importing two US states nobody asked for

### Fixed

* The global region can now be deleted. The console hid its delete button, and
  for good reason — removing the global row made the store report itself
  unusable, which sent every caller to the baked `config/regions.json` and
  silently resurrected the sample regions. The store is authoritative whenever
  it has rows and `global` is simply optional, so an instance that will never
  import the planet can drop it. `REGIONS=global` without one is now a clear
  error instead of a planet download
## [0.3.5] - 2026-09-14

### Fixed

* A basemap render can no longer take the rest of the stack down with it.
  `PLANETILER_MEMORY` bounds the JVM heap, but the render also holds direct
  buffers and mmaps its sorted features, so the container's real footprint runs
  well above the heap — uncapped, that is charged to the host. A US render on a
  16 GB box drove the machine out of memory and the kernel killed the largest
  resident process, which was MOTIS: transit died for a basemap rebuild, and
  the render died too, two and a half hours in. The new
  `PLANETILER_CONTAINER_MEMORY` sets a hard cgroup cap so an over-large render
  kills only itself. Leave it unset on a dedicated build host

## [0.3.4] - 2026-09-14

### Fixed
* Searching a code or reference no longer returns unnamed features that cannot
  be displayed. The codes layer matches ref-style tags, which unnamed things
  carry freely, and it is the highest-priority source in the result merge — so
  searching "m15" returned three unnamed camp pitches and a parking deck tagged
  `ref=M15`, evicting both the named "M15" and the M15 bus route and leaving
  nothing clickable in the response. Unnamed rows are now excluded from that
  layer, which is the only one that could surface them

## [0.3.3] - 2026-09-14

### Fixed
* Searching for a place no longer returns unrelated bus routes instead of the
  place. GTFS route matching includes a loose trigram branch — the thing that
  lets "harlem line" reach Metro-North's "Harlem" — and across a national feed
  corpus it paired almost any phrase with some route: "Mount Rainier" matched
  "MOUNT ROYAL", "Mountain", "Mountaineer Route" and two more, all scoring
  0.33-0.40. Routes rank above places in the result merge, so those five
  accidents filled an entire five-result response and the mountain never
  appeared. Matches below `BARRELMAN_TRANSIT_ROUTE_MIN_RANK` (default 0.5) are
  now excluded; measured against the live feed corpus, genuine route queries
  score 0.60-1.00 and are unaffected

## [0.3.2] - 2026-09-14

### Fixed

* A misspelled search no longer costs the full statement timeout. Deferring the
  fuzzy trigram layer (0.3.1) fixed well-spelled queries, but a query that
  actually needed it still waited the whole 10 s — the layer's KNN scan reads
  ~215 MB of index, so on an instance whose table dwarfs RAM it is cancelled by
  `statement_timeout` every time, meaning the caller waited 10 s to receive
  nothing extra and got the same hits the precise layers already had. The wait
  is now bounded by `BARRELMAN_SEARCH_TRIGRAM_BUDGET_MS` (default 2500), the
  same treatment the Pelias address wait already had: a warm index still
  supplies typo tolerance, a cold one costs the budget instead of the timeout

## [0.3.1] - 2026-09-14

### Fixed

* Text search no longer stalls for the full statement timeout on a cold query.
  The fuzzy trigram layer — typo tolerance, and the lowest-priority source in
  the result merge — was being issued on *every* search alongside the precise
  layers. Its KNN scan reads roughly 215 MB of trigram index per call, which is
  fast when that index is cached and takes many seconds when it is not; on an
  instance whose table dwarfs RAM that is the usual case, so a cold search spent
  10 s waiting and then silently discarded the trigram result when the timeout
  cancelled it — a slower answer with *fewer* results than not running it at
  all. It now runs only when the precise layers come back short, so a
  well-spelled query never pays for it and a misspelled one still gets it

* A US-scale import built a second GiST trigram index on `geo_places.name` at
  the default signature length, duplicating the `siglen=128` index that
  supersedes it. The planner never once chose it, so it was 5 GB competing for
  page cache against the index that actually serves fuzzy search. It is no
  longer created, and is dropped on existing installs the next time
  `finalize-indexes.sql` runs

## [0.3.0] - 2026-09-14

### Changed

* Street routing no longer caps waypoint spacing at 1,000 km — any two points
  routable over the imported graph now answer (LA to NYC was rejected with
  "Point 1 is too far from Point 0"). Landmark preparation is what makes
  continental queries affordable, and it was already being built

* A full import only overlaps the GraphHopper and basemap builds with the
  database work when the host has the RAM for it (`IMPORT_ENGINE_OVERLAP`,
  default `auto`, threshold 48 GB). On a smaller box the engines build after
  the import instead — GraphHopper's heap running next to Postgres during the
  import otherwise OOM-killed the graph build mid-run

* Computing GTFS walking transfers no longer runs on the API connection pool,
  whose statement timeout cancelled the all-pairs proximity join partway
  through a large import (`canceling statement due to statement timeout` after
  every feed had already imported). It now uses the untimed maintenance
  connection, like the other minutes-long batch queries

* GTFS feed discovery tiles a large region's bounding box before querying
  Transitland, which rejects an oversized bbox with `500 bbox too large` — the
  continental US tripped it, so a country- or continent-scale import fetched no
  feeds at all. State- and metro-sized regions are unaffected (a bbox under the
  area threshold makes one call, as before); feeds straddling a tile edge are
  deduplicated

* A full import overlaps its independent stages instead of running everything
  in sequence. The GraphHopper graph build starts the moment the extract is
  downloaded, and the basemap render runs alongside the post-processing SQL —
  both consume only the PBF, so on a large region the old ordering added their
  entire duration to the wall clock for nothing
* The import's enrichment passes write far fewer row versions: address, hours,
  phone and website extraction collapsed from four table rewrites into one,
  codes and abbreviations from two into one, and parent context and the
  full-text document are computed in the same pass. The query-serving indexes
  are now built once at the end of the import over settled data instead of
  being maintained row-by-row through every enrichment rewrite
* Parent-boundary resolution subdivides admin polygons before the containment
  join, so a place is tested against a small fragment instead of its state's
  full outline. One invalid boundary geometry no longer aborts the pass
* `area_m2` is a generated column computed by Postgres during osm2pgsql's own
  COPY, replacing a post-import statement that at US scale spent ~30 minutes
  computing areas and eleven hours rewriting half the table through live
  indexes. Databases imported before the change are backfilled once, in place
* The import drops the spatial indexes for the enrichment passes and rebuilds
  them with the rest at the end — after the 3D-buildings view, nothing in the
  pipeline reads them, and their per-row maintenance was most of the cost of
  every enrichment rewrite
* The GraphHopper rebuild skips itself when the serving graph was built from
  the extract currently on disk (a port that answers proves the build
  finished), so re-running an import cannot throw away a finished graph.
  `FORCE_REBUILD=1` — also a console toggle — forces the wipe, e.g. after a
  config change. `prepare.lm.threads` guidance documented: landmark
  preparation, not the import, dominates the graph build at continent scale
* The full-text document is built by one SQL function (`build_ts`) instead of
  two hand-synced copies of the same expression, and rebuilds skip rows whose
  document did not change
* The MOTIS extract preparation asks the database whether the extract contains
  any underground platforms before streaming the whole PBF through the repair
  pass — an extract with none gets a verbatim copy in seconds instead of a
  rewrite that grows with extract size
* The OSM import strips provenance and import-bookkeeping tags (`tiger:*`,
  `gnis:*`, `source`, `created_by` and similar — about 16% of all tag bytes on
  the US extract) before storing places. Objects whose only tags were such
  bookkeeping are no longer imported at all
* New database pacing knobs with better defaults: `max_wal_size` rises from
  Postgres's 1GB stock (which forced a checkpoint every couple of minutes for
  the whole of a large import) to 4GB, with `BARRELMAN_DB_MAX_WAL_SIZE`,
  `BARRELMAN_DB_CHECKPOINT_TIMEOUT`, `BARRELMAN_DB_WAL_COMPRESSION`,
  `BARRELMAN_DB_MAINT_WORKERS`, `BARRELMAN_DB_WAL_LEVEL` and
  `BARRELMAN_DB_MAX_WAL_SENDERS` for import-heavy boxes
* The self-hosting docs now describe GraphHopper's memory correctly: heap is
  consumed by the graph build; serving memory-maps the finished graph, so it
  needs free page cache, not `-Xmx`

### Added

* `OSM2PGSQL_FLAT_NODES` puts osm2pgsql's node coordinates in a flat file
  instead of the `planet_osm_nodes` table. Past roughly a country that table no
  longer fits in memory and every way the import assembles costs a disk seek,
  which is what made continent- and planet-sized imports take days. The daily
  diff apply reads the same variable, so replication keeps working. Leave it
  unset for anything smaller — the file is sized by the highest node ID in the
  extract rather than the nodes kept, so a city still produces ~100 GB of it.
  `OSM2PGSQL_CACHE_MB` and `OSM2PGSQL_PROCESSES` are exposed alongside it

### Fixed

* The admin console's tsvector rebuild no longer degrades search. Both console
  paths ("Rebuild tsvectors" and the full migration) carried their own copy of
  the full-text document expression, and it had drifted from the import's: no
  intersection-name expansion, no alias array, no apostrophe stripping. Running
  either after an import silently replaced good tsvectors with worse ones — an
  intersection indexed as "Main Street & Oak Avenue" stopped matching "Main St
  & Oak Ave". Both now call `build_ts()`, the one definition the import uses
* `BARRELMAN_SEARCH_ADDRESS_BUDGET_MS` now actually applies. It was documented
  in `.env.example` but never forwarded to the API container, so setting it did
  nothing
* Transfer prohibitions now cover every platform under a forbidden station.
  A `transfer_type=3` row is declared between the STATIONS a rider recognises
  — the MTA forbids Borough Hall to Jay St-MetroTech as `423` to `A41` — while
  computed walking transfers are between PLATFORMS (`423N` to `A41S`). Matching
  ids exactly let every platform pairing under a forbidden station back in,
  which is precisely the phantom the prohibition exists to stop. Both sides now
  resolve to their parent station, so one row covers all of them. Checked
  against production's own feed: of the 3928 computed rows carried forward,
  every out-of-system Borough Hall/Jay St pairing is dropped and all 613 agency
  transfers survive
* MOTIS runs at `--log-level info` instead of its default `debug`. At debug it
  logged every GTFS-RT entity it could not resolve — `could not resolve
  trip_id`, `unsupported: no "trip_update" field` — once per feed per poll,
  measured at 28 GB/day on a production host. Unresolvable realtime ids are
  normal where a feed's realtime and static halves disagree, and nothing acts
  on the messages
* Container logs are capped at 50 MB per service (three rotations). Docker's
  `json-file` driver is unbounded by default and the engines log forever — a
  timer pair per realtime poll from MOTIS, a line per request from GraphHopper
  — which had grown to 21.5 GB and 1.5 GB on a production host, on the disk
  that also holds the OSM database. Only MOTIS was capped before; every
  long-running service is now. Docker applies log options at container
  creation, so an existing deployment must recreate its containers
  (`docker compose up -d --force-recreate`) for the cap to take effect, and
  should truncate what has already accumulated. `self-hosting/troubleshooting`
  covers both

## [0.2.26] - 2026-09-14

### Added

* Intermodal routing takes `requireBikeTransport`, which keeps only trips that
  allow bike carriage — for BIKE on both ends. The response metadata confirms
  the enforcement, so a caller can tell "no bike-friendly trip exists" apart
  from "the filter was never applied"
* Unstated GTFS `bikes_allowed` now defaults to "allowed" at import time
  (`import/inject-bikes-allowed.ts`). GTFS has three states — no information,
  allowed, not allowed — but MOTIS collapses them to a boolean and reads "no
  information" as no. Most agencies omit the column, so carriage looked
  prohibited everywhere and `requireBikeTransport` returned nothing at all: of
  the feeds we import, only two declared a single bike-carrying trip between
  them. An explicit "not allowed" is left alone, so a feed that really does
  forbid carriage stays the authority on its own services. Buses are left
  unstated — most carry only a folding bike or a two-slot front rack, neither
  of which GTFS can express, and guessing "allowed" routes riders onto a bus
  that will turn them away
* Itineraries now report `farePayments` — how many separate fares a trip
  charges, which is not the same as the number of legs. MOTIS groups legs into
  fare transfers using the feeds' GTFS `fare_transfer_rules`, so a subway change
  inside the gates is one payment across two legs while stepping out to another
  operator is two. Reported only where the feeds actually publish fare data;
  with none, MOTIS still emits one empty group per leg and reading that as "a
  fare per leg" would be inventing a price out of a coverage gap
* Itineraries are ordered preferring fewer fare payments where the time cost is
  small (within ten minutes), rather than on duration alone. The router is
  Pareto-optimal over time and transfers, and fare is neither — MOTIS prices an
  itinerary *after* routing it — so a trip charging two fares could outrank one
  charging one and arriving at the same minute. The ordering only chooses among
  itineraries the router already returned, so it can never surface a slower trip
  than before

### Fixed

* Computed walking transfers are now **merged** into a feed's `transfers.txt`
  instead of replacing it. The file is the only authoritative record of which
  station pairs sit inside fare control: the MTA declares 613 transfers and
  connects Borough Hall to Jay St-MetroTech in none of them, because walking
  between them means leaving the paid area and paying again. Overwriting that
  with "every stop pair within 500m, timed by GraphHopper" asserted the very
  transfer the agency spent the file denying. The agency's rows now win, a pair
  the feed forbids (`transfer_type=3`) is never re-added, and computed rows only
  fill pairs nobody has spoken about. **Re-run the GTFS import** on instances
  that imported before this to restore the agency transfers it discarded
* Walking transfers are no longer computed for pairs a feed forbids. A
  `transfer_type=3` row is a fare gate, and routing the walk anyway burned a
  GraphHopper call per pair to produce a transfer the merge then discarded.
  Prohibitions are declared between parent stations while the pairs are
  platforms, so both sides resolve to their parent — one prohibition covers
  every platform pairing under it

## [0.2.25] - 2026-09-07

### Fixed

* `/transit/trip-stops` searches every TripUpdate feed a GTFS feed publishes.
  MTA New York City Transit lists nine subway line groups plus the bus feed on
  one row, and only the first was read — so a subway trip's stop times came
  back empty

## [0.2.24] - 2026-09-07

### Fixed

* A route's live vehicles are its own trains only. "Related routes" meant
  "same colour", so asking which trains were on the 4 answered with the 5s
  and 6s as well — a whole trunk line's fleet on one line's map. Now only a
  line's express working counts as the same line (the `6X` for the `6`)
* `/transit/trip-stops` names each stop's parent station, so a caller listing
  stations can line a run's times up against a feed that predicts against
  platforms

## [0.2.23] - 2026-09-07

### Fixed

* Departure boards drop the runs a skip alert disowns. A station closed for
  a parade until 9pm listed 2s "in 5 minutes" — the schedule plus realtime
  never removes a planned skip, only the agency's alert says it. Each run is
  judged at its own time, so the trains after the window stay: their times
  are what tell a rider when the station reopens. Applies to every consumer
  of `/transit/departures` — widgets, transfer boards, anything

## [0.2.22] - 2026-09-07

### Fixed

* `/transit/alerts` asked about a platform now matches alerts naming its
  station, and the other way around. Agencies inform stations ("238") while
  boards and pages hold platforms ("238N"); exact-id matching made a "trains
  skip this stop" alert invisible on the very stop page it was about
* `/transit/departures` names each board's GTFS parent station, so a caller
  holding a board can join it to the alerts about its stop

## [0.2.21] - 2026-09-07

### Fixed

* `/transit/route-vehicles` and the map's vehicle layer report the subway
  fleet that is actually running. The MTA prunes passed stops from a
  TripUpdate, so most trips carry nothing to interpolate a position between
  and were dropped outright — 2 of 34 4-trains survived one afternoon. The
  VehiclePosition entities the same feed publishes now anchor the rest,
  placed along the run in from the station before using the route's own
  stop order
* Realtime positions read the time from the FEED's header rather than the
  host clock. A server running ten minutes slow saw every arrival as further
  off and every departure as still to come, which alone cut those 34 trains
  to 8
* `/transit/alerts` no longer answers with another railway's notices.
  Metro-North numbers a route `4`, the LIRR numbers one `4`, the subway's 4
  is the Lexington Av express, and the MTA files all three through one feed
  — so "LIRR trains run on a weekend schedule" landed on the subway's page.
  The agency that owns a feed's stops is now the only one whose ids count

## [0.2.20] - 2026-09-07

### Added

* Each stop on `/transit/route-detail` names its GTFS parent station. GTFS-RT
  alerts inform stations, not platforms — the MTA's "4 runs local" names stop
  `237` while route detail carries `237N` — and the parent is the join between
  the two


## [0.2.19] - 2026-09-07

### Fixed

* `/transit/route-detail` no longer lists stops a route only reaches on a
  reroute. A feed's `stop_times` is the union of everything the route has ever
  been scheduled to do, so the MTA's R arrived carrying seven stations it
  reaches on one trip out of 735 — three on Second Av, three on the West End
  line. `gtfs_trip_patterns` now records how many trips run each pattern, and a
  stop kept by fewer than three of them, and by under 1% of the route's
  busiest, is dropped. Feeds imported before this ship with no counts and are
  left untouched until re-imported or backfilled with
  `import/backfill-trip-patterns.ts`

* `/transit/route-detail` no longer drops a stop whose name is shared with
  another station on the same route. It collapsed its stop list by name, so the
  R lost 36 St and 86 St in Brooklyn to the Queens Blvd 36 St and the Second Av
  86 St, and the 5 lost Gun Hill Rd and Pelham Pkwy on the Dyre Av branch to
  the White Plains Rd stations of the same names. It now collapses by station

### Changed

* A release now deploys itself. The pipeline's last job reaches the host over
  the tailnet, waits for the console's job queue to go idle, recreates
  `barrelman` and `barrelman-ops`, and fails if the health check does not come
  back — so a release lands in minutes instead of whenever the hourly poll
  happens to notice, and never in the middle of an import. It only recreates the
  services it names, so a deploy can no longer bounce the database as a
  dependency. Unconfigured (no deploy secrets) it does nothing and Watchtower
  goes on doing what it did; see `docs/development.md`

* The `barrelman` API carries the same Watchtower pre-update gate the database
  and the worker already had. It was the one container an update could restart
  mid-import: `kind:'internal'` jobs run inside it, and the MOTIS rebuild
  generates its config with `docker exec barrelman` while the job is running

## [0.2.18] - 2026-09-06

### Added

* `GET /transit/resolve-route` turns a bare GTFS `route_id` plus a coordinate
  into the `(feedId, routeId)` pair every other transit endpoint is keyed by.
  A route id is only unique within its feed — "2" is the New York subway's
  Seventh Avenue express here and the Long Island Rail Road's Ronkonkoma
  branch twenty miles east — so callers holding an id off a map tile pick the
  feed whose stops lie nearest the point, with an optional mode class
  (`metro`, `bus`, `ferry`, …) to break the remaining ties

## [0.2.17] - 2026-09-04

### Fixed

* Lines named sideways now match: "M60" finds M60-SBS (a decorated short name),
  "harlem line" finds Metro-North's "Harlem", "east river ferry" the NYC Ferry
  "East River", and "flixbus" a carrier whose route names are all destinations —
  short names match up to a separator, the mode-word-stripped phrase is tried
  against long names, and agencies join the haystack

* A line's own track no longer buries it. Every mapped segment of a railway
  carries the line's name ("Hempstead Branch" ×40), and at full rank they
  crowded out the one result a rider wants; rail infrastructure is now demoted
  like roads, and segments or relations named after a returned line are
  dropped as duplicates. Stations keep full rank

* The same line filed in several feeds appears once — the MTA carries all 307
  bus routes in each borough's feed, so "M60" answered twice

## [0.2.16] - 2026-09-04

### Fixed

* Typing a single character finds a transit line. Autocomplete used to return
  nothing below two characters — a guard against 1-character prefixes scanning
  the whole place table — but one character is exactly how riders name lines.
  A micro-query now runs only an indexed exact short-name lookup on GTFS
  routes, so "7" returns the 7 and "q" the Q in a couple of milliseconds,
  ranked nearest-system first

* Abbreviations match in both directions. Search demanded every query word
  appear verbatim in a name, so "franklin ave medgar" missed Franklin
  Avenue-Medgar Evers College and "82 st jackson heights" missed 82 St-Jackson
  Hts. Each query word now expands to every spelling of itself — ave/av/avenue,
  heights/hts, 42/42nd — using the same table that reconciles station names, so
  the two folds cannot drift apart. Expansion only ever adds matches

## [0.2.15] - 2026-09-04

### Added

* Search now returns transit lines and stops. A text query matches GTFS routes
  by short name ("7 train", "route 40") and long name ("Flushing Local");
  the hits carry `kind: 'transit_route'` and a `transit` object with the
  `feedId`/`routeId` pair every `/transit` endpoint is keyed by, plus mode,
  colours and agency. GTFS stops surface only where OSM doesn't already cover
  them: portolan's stop→OSM index (`stops.json`) is loaded into the new
  `portolan_stop_links` table at startup and after every portolan sync, linked
  stops defer to their OSM place, and a same-name stop within 250 m of an OSM
  station in the same results is dropped too. OSM `type=route` relations that
  duplicate a returned GTFS line are removed the same way. No setup: the
  schema, indexes and route centroids build themselves at startup

## [0.2.14] - 2026-09-03

### Added

* `/tiles/portolan/{feed}/stops.json` — the join from a feed's GTFS stop ids to
  the OSM object each one actually is, keyed `<feed-onestop-id>:<stop_id>`.
  Portolan has written this file next to every pyramid since 0.4.4, but nothing
  served it, so the only way for a client to turn a stop id into a place was to
  search by name and coordinates — which cannot tell New York's three Chambers
  St stations apart, and picks the wrong one. A feed built before the index
  existed has no file and answers 404, which callers already treat as "fall
  back to what you did before"

### Fixed

* The MOTIS rebuild no longer fails on a region the operator has switched off.
  `scripts/rebuild-motis.sh` generates the MOTIS config inside the API
  container, but `REGIONS` was named only for `barrelman-ops`, so the API fell
  back to the built-in dev pair `north-carolina,nyc-metro`. That went unnoticed
  while both were enabled and became a hard failure the moment either was
  disabled — and it struck at the very end, after the portolan sync it follows
  had already spent its hour. The API is given `REGIONS` too

## [0.2.13] - 2026-09-03

### Added

* Each board on `/transit/departures` reports its feed's transit.land onestop
  id as `stop.feedOnestopId`. Barrelman's feed ids are local to its own
  database, and everything the wider world publishes about a feed is keyed the
  way the world keys it — portolan's station index among them, whose keys are
  `<onestop>:<stop_id>`. A caller holding `("5", "M21")` had no way to join to
  any of it. Omitted for a feed with no onestop id, rather than invented

### Fixed

* A station added to a board by `transfers.txt` carries its own name and
  position instead of being left blank for MOTIS to fill in. A connecting
  station with no departures in the window came back nameless and at 0,0 — so
  it could not be labelled in a list, let alone linked to

## [0.2.12] - 2026-09-03

### Fixed

* A search no longer waits half a minute for an embedding service that is not
  there. The query embedding used the same 30-second timeout as the offline
  batch importer, which on the request path is not a timeout but an outage: an
  instance without Ollama running answered every semantic search in exactly 30s
  — the search itself finished in milliseconds and then sat waiting for a vector
  that was never coming, long enough that callers gave up first and the request
  failed outright. A query now waits two seconds, and a refusal is remembered
  for a minute so a burst of searches does not each pay for it. Batch embedding
  keeps the long timeout, which is correct for work that runs offline

## [0.2.11] - 2026-09-03

### Added

* `/transit/departures` takes `transfers=true` and returns a board for every
  station `transfers.txt` joins to this one under a different name, each marked
  `stop.via: 'transfer'`. A connection is more useful with its times than
  without: a rider changing at Chambers St wants to know when the 4 actually
  leaves Brooklyn Bridge–City Hall, not merely that it can be reached. Boards
  for the station itself are unmarked, so the two can never be confused

## [0.2.10] - 2026-09-03

### Fixed

* A station only groups with others of the same name. `complex=true` folded in
  everything `transfers.txt` reached, which is a free walk rather than one
  station: Borough Hall took in Court St and reported the N and R as departing
  from Borough Hall. Same name is what one drawn symbol over one station means
  — the six "Canal St" stations still merge, Court St is a transfer again.
  Names are compared within a feed on case and punctuation only, not the looser
  fold used to reconcile a feed against OSM, so anything not plainly the same
  station stays a transfer

## [0.2.9] - 2026-09-02

### Fixed

* A station only groups with others of the same name. `complex=true` folded in
  everything `transfers.txt` reached, which is a free walk rather than one
  station: Borough Hall took in Court St and reported the N and R as departing
  from Borough Hall. Same name is what one drawn symbol over one station means
  — the six "Canal St" stations still merge, Court St is a transfer again.
  Names are compared within a feed on case and punctuation only, not the looser
  fold used to reconcile a feed against OSM, so anything not plainly the same
  station stays a transfer

### Fixed

* `/transit/routes` caps nearby lines at six, nearest train first, and takes a
  `nearbyLimit` to change that. The radius alone never bounded the list: two
  hundred metres of Lower Manhattan holds twenty-five distinct lines, so Rector
  St answered with the 1 fifty metres away and then every Staten Island express
  bus and commuter coach passing the corner — SIM1, SIM1C, SIM2, SIM4C, SIM15,
  SIM32 and the rest. The cap runs after the sort, which is route type before
  distance, so what survives is the nearest train rather than the nearest bus
  stop
* Removing data no longer triggers a full re-derivation of every search column.
  Startup compares the row count against the one recorded when enrichment last
  completed and re-enriches when they differ by more than 10%, which could not
  tell a prune from a re-import: dropping one region from a two-region instance
  moved 27M rows to 19.4M, and the 28% fall queued a rewrite of `codes`,
  `name_abbrev` and `ts` that nothing needed, since the surviving rows keep the
  values they already had. Worse, it never finished — each backfill is a single
  statement, so a container restart rolled the whole thing back and the next
  boot began again, holding transactions long enough to block index maintenance
  for as long as it ran. Only growth counts as a re-import now
## [0.2.8] - 2026-09-02

### Added

* `/transit/route-detail` reports the other lines available at each stop on the
  route, so a stop row can draw its connections the way the Transit app does —
  Union Square on the N showing Q and R and W beside it, and the 4, 5, 6 and L
  a passageway away. Lines that call at the stop are marked `station` and ones
  transfers.txt reaches from it `transfer`, so a caller can tell a train on this
  platform from a walk across the interchange. The route being viewed is left
  out. One query for the whole route, not one per stop

### Fixed

* `RouteDetailResponse` declares `bikesAllowed`, which it has been returning
  since the field was added without ever saying so in its type

## [0.2.7] - 2026-09-02

### Added

* OSM Update, Check for GTFS Updates and Import GBFS Systems take a Regions
  override, the same one Full OSM Import and Download GTFS Feeds already had.
  Every script that resolves regions can now be pointed at a different set for
  one run, from the console, without editing the server's `.env` and restarting
  ops. That matters most when the two disagree: naming a region in `REGIONS`
  that has been switched off in the console makes the resolver refuse — by
  design, since silently importing a disabled region would be worse — and
  without an override the only way out was a shell on the host
* `/transit/routes` takes `complex=true`, which treats the whole interchange as
  one station: every line in it comes back as `via: 'station'` instead of the
  connecting ones being filed under `transfer`. It is the counterpart to the
  same flag on `/transit/departures` — a tap on the single symbol a map draws
  over an interchange is a question about the interchange, so Brooklyn
  Bridge–City Hall answers 4 5 6 J Z rather than 4 5 6 with the J and Z listed
  as connections. Tapping one station of the group keeps the split
* The three portolan sync scripts take a **Parallel feed builds** setting,
  passed to `portolan sync --jobs`. Portolan otherwise sizes its own
  parallelism from CPU count, with no view of what else the host is running,
  and charts are memory-heavy: on a machine that also serves the API, four
  concurrent charts can take all the RAM. What that looks like is worth
  knowing, because it does not look like a crash — the kernel keeps running,
  so the host still answers pings and still completes TCP handshakes on every
  open port, while nothing in userspace gets scheduled and even SSH hangs at
  the banner. Blank still means portolan's default; set 1 or 2 where memory is
  thin. Also note that stopping the ops worker gracefully *requeues* the
  running job by design, so a job has to be cancelled before the worker comes
  back or it starts over

### Fixed

* Feeds whose realtime trip ids do not match their schedule trip ids now
  resolve. MOTIS matches realtime trips by exact trip_id, so where an agency
  publishes the two in different id spaces nothing resolves and the feed serves
  schedules with no realtime — MTA's subway drops a schedule-version prefix
  (`ASP26GEN-1038-Sunday-00_000600_1..S03R` in the schedule against
  `000600_1..S03R` on the wire), and every subway departure came back
  `realTime: false`. The import now samples each feed's own realtime data,
  tries a set of candidate id transforms against it, and rewrites the schedule
  only where one measurably resolves more trips. There is no list of affected
  agencies to maintain: a feed that already resolves is measured as such and
  left untouched, and a transform that stops working is caught on the next
  import. Against the seven live MTA subway feeds it finds the prefix strip on
  its own and takes 495 of 705 sampled realtime trips from unmatched to
  matched. The rewrite is skipped, with the reason printed, when the sample is
  too small to judge or when it would leave two trips running the same day
  sharing one id
* Re-running the GTFS import no longer empties `gtfs_feeds.rt_urls`, which took
  realtime down for every feed rather than just one. Importing a feed clears its
  row before writing the new one, so anything the importer did not itself carry
  was dropped — and RT URLs come from `import/backfill-rt-urls.ts`, not from the
  import. The next `scripts/rebuild-motis.sh` then baked a config with no `rt:`
  section at all, and nothing along the way reported an error. The importer now
  carries the stored values forward, `--skip-download` included, where the same
  bug also overwrote each feed's onestop id, name and URL
* `generate-motis-config.ts` warns when it writes a config that has feeds but no
  realtime URLs, instead of reporting success. Recovering from one takes a full
  `motis import`; restarting MOTIS keeps serving the config baked into the
  dataset
* Feed ZIPs are written compressed. Every step that rewrote one — the GTFS-Flex
  strip, the transfers injection, the Fares v2 conversion — re-serialized it
  uncompressed, taking the subway feed from 5.6 MB to 43 MB on the volume MOTIS
  imports from
* MOTIS is handed only the GBFS systems inside the regions an instance imports,
  rather than the whole catalog. `gbfs_systems` keeps every system the operator
  directory lists — the stations are filtered by bbox, the systems are not — and
  MOTIS polls every feed it is given for the life of the process. A New York
  instance was polling 1345 live feeds to serve 2, and reporting itself
  unhealthy for the whole time, its health endpoint being an AND over all of
  them. A global instance still gets everything, as does one whose regions
  declare no usable bounding box
* `/health` no longer reports transit as down when MOTIS is merely degraded.
  MOTIS answers its health endpoint with a flag per updater and only returns
  200 when every one is true, so an instance whose GBFS feeds failed to load
  replies `400 {"rt":true,"gbfs":false}` while serving stoptimes queries
  normally. That was read as an outage, which took the whole transit endpoint
  group off `/health` and lit the console red over a working timetable. A
  subsystem report now counts as up and names what is degraded; a status with
  no such report is still unavailable

## [0.2.6] - 2026-09-02

### Added

* `/transit/departures` takes `complex=true` and returns a board for every
  station the agency's transfers.txt joins to the resolved one, rather than
  only that station. This is what a merged station label on a map stands for:
  New York draws four separate GTFS stations named "Canal St" — Q01 (N/Q), M20
  (J/Z), 639 (4/6/6X) and R23 (N/R/W) — as a single symbol, and tapping it asks
  about all four rather than whichever was nearest the tap. One hop, not a
  transitive closure, so it wanders around an interchange and not down a line;
  capped at eight stations
* `/transit/routes` takes `lat`/`lng` (and an optional `radius`, default 200 m)
  and additionally reports `via: 'nearby'` lines — stops within walking
  distance that the feed does not join to this station. This is how a subway
  station's bus connections are found at all: `transfers.txt` is scoped to a
  single feed, so no file in either the subway's or the bus operator's feed can
  reference the other, and proximity is the only signal left. Nearby rows carry
  `distanceM`, are folded to one row per line and agency (several overlapping
  feeds cover the same New York buses, so the M22 otherwise arrives three
  times), and say nothing about whether the connection is free

## [0.2.5] - 2026-09-02

### Fixed

* A departure board never carries a neighbouring station's runs. MOTIS answers
  a stoptimes query with every stop that shares the requested stop's name, so a
  board for the Chambers St J/Z platform arrived with the 1, 2, 3, A and C of
  the unrelated Chambers St 200 m away. Those were already dropped once the
  caller identified the station; now every board is filtered to the stop it
  names, including a plain nearby-stops lookup

## [0.2.4] - 2026-09-02

### Added

* `/transit/departures` takes a `name` parameter — the place's own name — and
  uses it to identify which station a set of coordinates belongs to. A stop
  whose name matches claims the board even when another is nearer, and the
  board is then reported for that station alone
* Every route from `/transit/routes` now carries `via`: `station` for a line
  that calls there, `transfer` for one reachable at a connecting station
  without leaving the paid area — the J and Z at Chambers St, from Brooklyn
  Bridge–City Hall. Station lines are listed first. A transfer a fare rule buys
  rather than a walk between platforms is not in `transfers.txt` and is not
  reported

### Fixed

* A departure board opened on a subway station showed the lines of whichever
  stop happened to be nearest, which is not always the station itself. The
  Brooklyn Bridge–City Hall stop_position sits 37.8 m from the Chambers St
  platforms and 52.2 m from its own, so its board filled with the 1, 2, 3, A
  and C from an unrelated complex 200 m away and listed its own 4, 5 and 6
  last, if at all. Naming the place now settles which station it is
* A station's departures are no longer listed twice. The board was built by
  asking MOTIS about each platform, and MOTIS answers every platform with the
  same station-level list — so each train appeared once per platform ("Now,
  Now"). It is now asked once, at the GTFS parent
* Departures are filtered to runs that actually call at the station. MOTIS
  resolves a stoptimes query to every stop sharing the requested stop's name,
  and New York has two unrelated Chambers St complexes 200 m apart

## [0.2.3] - 2026-08-31

### Fixed

* A slow query no longer kills the ops worker and the job it is running. The
  log-flush, heartbeat and cancel-check timers each talked to Postgres on a
  schedule with their rejections unhandled, which Bun treats as fatal — so
  `canceling statement due to statement timeout` on an *log line insert* took the
  whole worker down. A basemap render lost its parent that way mid-run: the
  planetiler container was left orphaned, and the console reported the job failed
  after forty minutes of work that had in fact succeeded. All three now log and
  continue, none of them being worth a job. A genuinely unreachable database
  still stops the job, through the heartbeat timeout that marks it failed
* A basemap rebuild that is killed rather than exited no longer disables every
  later one. The single-flight lock is released by an `EXIT` trap, which cannot
  run on `SIGKILL` — what a container restart delivers — so the lock directory
  outlived its holder and every subsequent run took the "already running" branch
  and exited 0. The console showed a green job that had rendered nothing. A lock
  older than six hours, well beyond the longest render, is now reclaimed with a
  warning instead of obeyed
* A replication update can leave the OSM extract with ways referencing nodes
  that are no longer in it. MOTIS then fails outright with `unable to import:
  invalid location`, an hour after the update reported success — and GraphHopper
  does something worse, building a graph that silently omits the affected ways,
  so street routing loses roads with nothing in the logs to say why. Geofabrik
  clips its diffs to one region's polygon, so on a
  merged multi-region extract a node deleted in the followed region is dropped
  while a neighbouring region's ways still reference it; patching cannot repair
  that. `update-osm.sh` now verifies the patched extract before the rebuilds
  that consume it, and refuses to run them on a damaged one, naming
  `UPDATE_MODE=full` as the repair. `rebuild-motis.sh` points at the same
  diagnosis when an import dies this way

### Added

* The console sidebar shows the version of the instance it is talking to, below
  the sign-out button. `/admin/config` now reports the version from
  `package.json` — what the release pipeline tags from — rather than a hardcoded
  literal, which had drifted to a 0.4.0 that was never released

## [0.2.2] - 2026-08-31

### Fixed

* Rebuild Basemap now renders successfully. Planetiler reads the archive format
  from the output file's last extension, and the script staged its render as
  `basemap.pmtiles.next` — so every run died during argument parsing with
  "Unsupported format next", before reading a single OSM block. The staging file
  is now `basemap.next.pmtiles`. The existing basemap was never at risk: the
  failure path leaves it in place, so instances have been serving an archive
  that simply stopped following the data
* `rebuild-motis.sh` now checks that the import will actually see the feed ZIPs,
  instead of quietly rebuilding from a stale copy. It locates them by inspecting
  the motis service for a `/data/gtfs` bind mount, and simply omitted the mount
  when there wasn't one — but `/data/gtfs` still resolves inside the gtfs-data
  volume, so on an instance missing that bind the import found whatever old
  feeds lived there, succeeded, and reported fresh schedules. The check counts
  the ZIPs the import container will see, warns and names the missing compose
  line when it falls back to the in-volume copy, and refuses when there are none
  at all. Like the existing feed-count guard it runs before the current dataset
  is moved aside, so a misconfigured instance costs nothing

## [0.2.1] - 2026-08-31

### Added

* A `buildings_3d` tile source, carrying building outlines and `building:part`
  polygons with the `hide_3d` flag that separates them. OpenStreetMap maps a
  detailed building twice — an outline covering the whole footprint and parts
  inside it holding the real heights — and a stock OpenMapTiles basemap carries
  no way to tell one from the other, so a 3D client draws both and every
  part-mapped building comes out doubled and z-fighting. The source also carries
  wall and roof colour, the latter having no field in the OpenMapTiles schema at
  all
* The PMTiles basemap is re-rendered whenever an import or an OSM update moves
  the extract, so it stops being the one output that never followed the data.
  Martin queries every `postgres:` source live, so a replication diff reaches
  those tiles at once, but `basemap` is a static archive — the database and the
  routing graph moved while the map went on showing whatever planetiler last
  rendered by hand. It is always a full re-render: PMTiles is write-once, with
  absolute directory offsets and content-deduplicated tiles, and planetiler has
  no incremental mode. A couple of US states take a few minutes, which is
  cheaper than the machinery incremental tiling would need. Gated on
  `REBUILD_BASEMAP`, and skipped outright on an install that has no basemap
* Scripts that chain into other scripts say so in the console. An OSM update
  also rebuilds the routing graph and the basemap, and a GTFS check rebuilds the
  MOTIS dataset — all of which the Scripts page rendered as a single step, so
  the only way to learn what a run would touch was to read the shell. The card
  now badges its follow-ups and the run dialog lists them with the conditions
  under which each is skipped

## [0.2.0] - 2026-08-30

### Added

* Each account's usage is visible from the console's accounts page — the current
  billing cycle broken down by endpoint group and by key, a lifetime total
  beside it, and the account's API keys and moderation history in the same
  place. An account that was abusive last month reads as spotless through a
  cycle window, which is why the lifetime figure is there
* Accounts can be deleted outright, not only suspended. Deleting erases the
  account's keys, usage, credit ledger, abuse signals and its own moderation
  history, so it is the answer to "remove me" rather than to a troublesome
  customer — suspension remains the reversible option, and the one that keeps a
  record. The console asks for the address typed back and the API checks it;
  your own account and the last remaining administrator are both refused

## [0.1.0] - 2026-08-28

First tagged release. Barrelman has been running in production for some time;
this is the point at which its images stop being "whatever was last pushed to
main" and start carrying a version you can pin to and roll back from.

### Added

* **Search** over an OSM extract — full-text, trigram, abbreviation and optional
  semantic vector matching in one pipeline, with point-radius and
  route-corridor modes
* **Geocoding**, forward and reverse, backed by Pelias where it is deployed
* **Brand search**, resolving chains and franchises against a brand catalog
* **Places** by OSM id, with the spatial relationships around them — which areas
  contain a point, and what an area contains
* **Vector tiles** through Martin, from the same extract everything else reads
* **Routing** for car, bike and foot through GraphHopper, with elevation and
  path detail
* **Transit** — trip planning through MOTIS, plus live GTFS-RT vehicles,
  departures, stops and route detail
* **GBFS** — shared bike and scooter systems, and live station availability
* **Accounts, API keys and metering** — per-key scopes and origin restrictions,
  credit-based quotas, plans, and usage the account holder can see
* **Abuse controls** — six layers of throttling, automated detection that raises
  signals rather than banning anyone, and moderation with an audit trail
* **Admin console** — imports, migrations, a serial job queue with live logs,
  scheduled jobs, region management, data metrics and downstream service health
* **Self-hosting documentation** at [docs.barrelman.dev](https://docs.barrelman.dev),
  covering install, import, configuration and the failure modes operators hit

### Notes

Billing is gated on a signed licence and is not part of a self-hosted
deployment. Everything above is not.
