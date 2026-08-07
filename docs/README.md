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
| [Polar setup](polar-setup.md) | Wiring up billing |
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

The second is optional. With no `BARRELMAN_API_KEY` and no `POLAR_ACCESS_TOKEN`
the whole commercial surface is inert and you have a self-hosted engine — which
is the mode most people running it themselves want.
