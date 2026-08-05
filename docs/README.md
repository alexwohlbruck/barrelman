# Barrelman documentation

| Document | What it covers |
|---|---|
| [Development](development.md) | Running the whole stack with Docker Compose |
| [Accounts & API keys](accounts.md) | Sign-in, sessions, keys, scopes |
| [Pricing & credits](pricing.md) | What each endpoint costs, what each plan includes |
| [Abuse controls](abuse-controls.md) | Throttling, suspension, terms enforcement |
| [Polar setup](polar-setup.md) | Wiring up billing |
| [Configuration](configuration.md) | Every environment variable |

The [top-level README](../README.md) covers architecture, data import and
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
