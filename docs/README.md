# Barrelman documentation

| Document | What it covers |
|---|---|
| [**Self-hosting**](https://docs.barrelman.dev/self-hosting) | **Start here.** Server to running instance, end to end |
| [Regions](REGIONS.md) | Choosing what data to import, and the import pipeline |
| [Configuration](configuration.md) | Every environment variable |
| [Development](development.md) | Running the stack from a clone, with hot reload |
| [Accounts & API keys](accounts.md) | Sign-in, sessions, keys, scopes |
| [Pricing & credits](pricing.md) | What each endpoint costs, what each plan includes |
| [Abuse controls](abuse-controls.md) | Throttling, suspension, terms enforcement |
| [Pelias](../pelias/README.md) | Provisioning the optional address geocoder |

The [top-level README](../README.md) covers architecture, the API surface and
deployment. [CLAUDE.md](../CLAUDE.md) is the working guide for agents and the
checklist for keeping the admin console in sync.

## The shape of the thing

Barrelman is two products sharing one codebase:

- **A geospatial engine.** OSM imported into PostGIS, served as search,
  geocoding, tiles, routing and transit. This is what Parchment consumes.
- **A metered public API.** Accounts, keys, credits and billing on top of that
  engine, so other developers can consume it too.

The second is optional, and its billing half is not available to self-hosters:
the Commons Clause forbids selling barrelman, so subscriptions are gated on a
signed license only the official deployment holds. Accounts, keys and metering
work everywhere — on your own instance metering shows you your own usage.
