# Barrelman documentation

Most documentation now lives on the docs site, **[docs.barrelman.dev](https://docs.barrelman.dev)**,
built from `content/docs/` in this directory. Only the pages that are about
working on barrelman, rather than running it, are kept as markdown here.

| Document | What it covers | Where |
|---|---|---|
| [**Self-hosting**](https://docs.barrelman.dev/self-hosting) | **Start here.** Server to running instance, end to end | site |
| [Regions](https://docs.barrelman.dev/self-hosting/regions) | Choosing what data to import, and the import pipeline | site |
| [Configuration](https://docs.barrelman.dev/self-hosting/configuration) | Every environment variable | site |
| [Abuse controls](https://docs.barrelman.dev/self-hosting/abuse-controls) | Throttling, suspension, terms enforcement | site |
| [Accounts & API keys](https://docs.barrelman.dev/usage/accounts) | Sign-in, sessions, keys, scopes | site |
| [Pricing & credits](https://docs.barrelman.dev/usage/pricing) | What each endpoint costs, what each plan includes | site |
| [Development](development.md) | Running the stack from a clone, with hot reload | repo |
| [Polar setup](polar-setup.md) | Billing for the official deployment — not a self-hosting step | repo |
| [Pelias](../pelias/README.md) | Provisioning the optional address geocoder | repo |

`development.md` stays here because it is for people changing this repository,
and `polar-setup.md` because it covers issuing the signed licence and wiring
Polar for the one deployment that is allowed to charge — neither is something a
self-hoster does.

## Editing the site

Pages are MDX under `content/docs/`, and **only** that directory is built
(`source.config.ts` sets `dir: 'content/docs'`). A markdown file dropped beside
this README is not published — it is invisible to the site and reachable only as
a GitHub blob.

Only `bash`, `dotenv`, `json`, `yaml` and `sql` code fences are bundled; an
unknown language 500s every page in the collection, not just its own file.
Internal links take no `/docs` prefix (`baseUrl: '/'`): `/usage`, not
`/docs/usage`.

```bash
cd docs && bun install && bun run dev
```

The API reference under `content/docs/api/` is generated — see the top-level
[CLAUDE.md](../CLAUDE.md) for how to regenerate it.

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
