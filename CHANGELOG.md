# Changelog

All notable changes to Barrelman are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/).

Entries accumulate under `[Unreleased]` as work lands. Cutting a version stamps
that section with the version and date — `scripts/changelog.sh release X.Y.Z`
does it — and the release pipeline turns it into the GitHub Release notes.

## [Unreleased]

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
