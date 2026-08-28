# Changelog

All notable changes to Barrelman are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[semantic versioning](https://semver.org/).

Entries accumulate under `[Unreleased]` as work lands. Cutting a version stamps
that section with the version and date — `scripts/changelog.sh release X.Y.Z`
does it — and the release pipeline turns it into the GitHub Release notes.

## [Unreleased]

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
