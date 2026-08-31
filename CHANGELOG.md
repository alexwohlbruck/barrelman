# Changelog

All notable changes to Barrelman are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/).

Entries accumulate under `[Unreleased]` as work lands. Cutting a version stamps
that section with the version and date — `scripts/changelog.sh release X.Y.Z`
does it — and the release pipeline turns it into the GitHub Release notes.

## [Unreleased]

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
