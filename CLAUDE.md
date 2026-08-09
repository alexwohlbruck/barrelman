# Barrelman

OSM geospatial API (Elysia/Bun + PostGIS) that powers Parchment. Imports
OSM/GTFS/GBFS and serves search, tiles, spatial queries, routing. Also a public
metered API with accounts, keys and billing. See `README.md`.

## Architecture

Docker Compose. Ports are host-side.

| Service | Port | Notes |
|---|---|---|
| `barrelman` | 5001 | API + console at `/console` + OpenAPI at `/docs`. Hot-reload, `src/` mounted |
| `barrelman-db` | 5434 | Postgres + PostGIS + pgvector. Carries osm2pgsql; the OSM import runs here |
| `barrelman-ops` | — | Privileged worker, docker socket. Runs `process` jobs. **Not** source-mounted |
| `barrelman-martin` | 5002 | Vector tiles |
| `barrelman-graphhopper` | 5003 | Street routing |
| `barrelman-motis` | 5004 | Transit routing |
| `barrelman-console` | 5199 | Dev only, Vite HMR. Prod serves the built SPA from the API |
| `barrelman-landing` | 5200 | Dev only, `landing` profile, from `../barrelman-landing` |
| `pelias_api` | 4000 | Optional, `pelias` profile. Index build is a multi-hour job |

The API is lean — no docker CLI, osmium or python. It only **enqueues**
`process` jobs; `barrelman-ops` claims them one at a time under an advisory lock
and `docker exec`s into siblings. `internal` jobs (SQL, migrations) run in the
API. So **imports run in `barrelman-ops`, never `barrelman`**:

```bash
docker compose exec -d barrelman-ops bash scripts/run-import.sh
```

## When to restart what

- `src/`, `import/`, `web/src/` — hot-reloaded, nothing to do.
- `scripts/` or anything ops runs — ops isn't source-mounted. `./start.sh dev --build`.
- New dependency — `package.json` is baked in, not mounted. `./start.sh dev --build`,
  which also renews the anon `node_modules` volumes. Rebuilding alone leaves a
  stale volume masking the new image, and the API crash-loops on a module that
  is present in it.
- Module-level singletons (Lucia, Polar, timers) survive `bun --hot`. If a change
  won't apply, `docker restart barrelman` before hunting a bug.
- GTFS changed → `scripts/rebuild-motis.sh`. `motis server` only serves the
  pre-built dataset and never re-imports; a restart keeps stale schedules.
- OSM changed → `scripts/rebuild-graphhopper.sh`.

---

## ⚠️ Keep the console, docs and landing page in sync

None of these auto-discover changes. Check all three before finishing. If a
change touches none (an internal refactor), just confirm you considered it.

### 1. Admin console

| If you… | Update… |
|---|---|
| Add/remove/rename a runnable script (`scripts/*.sh`, `import/*`, a `package.json` script, a new `admin.service` migration) | the `SCRIPTS` entry in `src/admin/scripts-manifest.ts` |
| Change a script's flags, positional args or env vars | that entry's `params` |
| Add an `exec.kind: 'internal'` task | also its handler in `src/services/admin-internal-handlers.ts` |
| Make a script need a new tool (docker, python, a binary) | `Dockerfile.ops` — ops carries only docker/osmium/python/uv |
| Add/rename a table or coverage-relevant column | queries in `src/services/admin-metrics.service.ts`, plus `DataMetrics` + `web/src/lib/types.ts` + the Dashboard/Data views if shown |
| Add/remove a downstream service or change its health URL | `getServiceStatuses()` in `src/services/admin-metrics.service.ts` |
| Add a notable public endpoint | consider a preset in `web/src/views/ApiTesterView.vue` — session-free endpoints only, the tester replays server-side with no cookie |
| Add a metered endpoint | a group in `groupForPath()` (`src/billing/plans.ts`) + `apiAuth('<group>')`, or it's free and unattributed |
| Add an `/admin/*` route | `.onBeforeHandle(adminAuthHandler)` — never `.use()` (see auth footgun) |
| Add a script that refreshes data on a cadence | consider a `SEEDS` entry in `src/services/schedules.service.ts` — seeded schedules ship **disabled**, so adding one is safe |

A manifest entry the ops image can't run is worse than none — it's a button that
always fails. Check tooling first.

Then: `cd web && bun run typecheck && bun run build`.

### 2. Documentation

Two surfaces. **Public site**: `docs/`, Next.js + Fumadocs → `docs.barrelman.dev`.
MDX in `docs/content/docs/` — `introduction`, `usage/`, `api/` (generated),
`self-hosting/`. **Repo markdown**: `docs/*.md` (`REGIONS`, `configuration`,
`development`, `accounts`, `pricing`, `abuse-controls`, `polar-setup`) for
operators reading the repo.

| If you… | Update… |
|---|---|
| Add/change/remove a public endpoint | regenerate the API reference (below); `usage/index.mdx` if calling or auth changes |
| Change a capability visibly | `introduction.mdx` |
| Change install, import, deploy or ops behaviour | the right page in `self-hosting/` |
| Add a failure mode operators will hit | `self-hosting/troubleshooting.mdx` |
| Change pricing or credit costs | `src/billing/plans.ts` is source of truth → `docs/pricing.md` + the landing pricing table |
| Add an env var | `.env.example` + `docs/configuration.md` + the compose block |

```bash
cd docs && curl -sSf https://api.barrelman.dev/docs/json -o openapi.json && bun run generate:api
```

Notes:

- Only `bash`, `dotenv`, `json`, `yaml` fences are bundled. An unknown one
  (`caddy`) 500s **every page in the collection**, not just its own file.
- Don't run `bun run build` against a live docs dev server — it opens with
  `rm -rf .next .source` and wedges it until stopped, cleaned and restarted.
- Internal links take no `/docs` prefix (`baseUrl: '/'`): `/usage`, not `/docs/usage`.

### 3. Landing page (`../barrelman-landing`, Nuxt → barrelman.dev)

| If you… | Update… |
|---|---|
| Change plan names, prices or credit allowances | `app/components/PricingTable.vue` — hardcodes what `plans.ts` defines |
| Add or materially change an endpoint group | `app/components/CapabilityGrid.vue` |
| Change what the hero demo calls | `server/api/demo/[group].get.ts` — fixed request shape + allowlist, deliberately not a proxy |
| Change auth, key format or base URL | samples in `CodeWindow.vue`, `SiteHero.vue` |

The demo uses `BARRELMAN_DEMO_KEY` (server-side) and `BARRELMAN_TILE_DEMO_KEY`
(browser, scoped `tiles`) on a `demo`-plan account — never `BARRELMAN_API_KEY`,
which is Parchment's service credential. Per-group credit costs were
deliberately removed from `CapabilityGrid.vue`; don't re-add that duplication.

---

## Gotchas

**Auth.** Elysia scopes a plugin's hooks to that plugin, so `.use(authMiddleware)`
leaves sibling routes on the parent **public**. Always attach directly:
`.onBeforeHandle(apiAuth('<group>'))` or `.onBeforeHandle(adminAuthHandler)`.
Check every new route file with an unauthenticated `curl`.

**Env vars.** Three places or it won't work: `.env.example`,
`docs/configuration.md`, and the right service's `environment:` block in
`docker-compose.yml` (Compose doesn't forward the host env). Note *which*
service — `REGIONS` is read by the importers in ops, not the API. Read values
with `envNumber`/`envString` from `src/config/env.ts`, never
`process.env.X ?? default`: Compose passes optional vars as `${VAR:-}`, so `??`
never reaches its fallback and `Number('')` is 0.

**Testing guards.** Never stub `onBeforeHandle`/`onAfterHandle` with `mock()`.
Elysia inspects the hook to compile its chain; a mock defeats that, so the
guard's refusal is returned **and the handler still runs**. Use a plain function
and count calls — see `src/routes/admin-users.test.ts`.

**Licensing.** The Commons Clause forbids selling barrelman, so billing is gated
on a signed Ed25519 license granting the `billing` feature (`src/lib/license.ts`,
consumed in `src/config/billing.config.ts`). Only the official deployment holds
one. Never document billing setup as something a self-hoster does, and never add
a code path that enables paid features without checking `billing.enabled`.
Everything else — search, tiles, routing, transit, accounts, keys, metering —
stays unlicensed. Issue tokens with `scripts/generate-license.ts` (see
`docs/polar-setup.md`); the private key never enters the repo. That script is a
deliberate exception to the console-sync rule above — it takes the private key
as input, so it must never get a `SCRIPTS` entry or a `package.json` script.

**Admin auth.** `/admin/*` takes an admin-role session or an account API key
with the `admin` scope owned by an admin. There is no shared secret — both
`BARRELMAN_ADMIN_KEY` and `BARRELMAN_ADMIN_EMAILS` are retired. The first account
on a fresh instance is an admin; the rest are promoted in the console. Changing a
role goes through `setUserRole()`, which **refuses to remove the last admin** —
zero admins is only recoverable from psql, so keep that guard. Keep `admin` OUT of the `*` wildcard
(`src/billing/plans.ts`): `*` is the default for a scopeless key, so folding
admin in would promote every existing key. Only an admin may grant it, on
create *and* on scope update — both paths need the guard.

**Suspension.** `suspendUser()` tears down sessions, the API-key cache and the
balance cache. A new cache keyed on account state belongs there too.

## Where things live

- Regions: `src/config/regions.ts`, `config/regions.json`, `src/services/region-store.service.ts`
- Boundary catalog: `src/services/boundary-catalog.service.ts`
- Script catalog: `src/admin/scripts-manifest.ts` · job runner: `src/services/job-runner.service.ts` · ops worker: `src/worker/index.ts`
- Schedules: `src/services/schedules.service.ts` (store + seeds) · `src/services/scheduler.service.ts` (tick) · `src/lib/cron.ts` (parser). A schedule fires via `startJob(…, { trigger: 'schedule' })`, so scheduled runs are ordinary tracked jobs — never add a path that bypasses the job store
- Metrics/health: `src/services/admin-metrics.service.ts`
- Admin routes: `src/routes/admin-console.ts`, `src/routes/admin.ts`, `src/routes/admin-users.ts`
- Console SPA: `web/` (views in `web/src/views/`, types in `web/src/lib/types.ts` — mirror backend shapes)
- Accounts/billing: `src/services/{auth,api-keys,usage,credits,billing,throttle,moderation}.service.ts`, `src/billing/plans.ts`

The first account on a fresh instance becomes an admin. `BARRELMAN_API_KEY` is a
shared unmetered **service** credential and is never billed.

## Dev

```bash
./start.sh dev            # everything, hot reload. Console at :5199/console/ (trailing slash)
./start.sh dev --build    # rebuild + renew anon volumes
./start.sh dev --down
bun test                  # no DB needed
bun run test:integration  # hits the real DB, opt-in
```

Sign-in codes print to the log without SMTP:
`docker logs barrelman --tail 20 | grep "sign-in code"`.

## Rules

- Don't start dev servers or Compose stacks — the user runs their own.
- Don't merge to main. Branch (`feat/…`, `docs/…`, `fix/…`) and open a PR. `dev`
  is the working branch.
- Update the Linear ticket status as work progresses, if one is linked.
- `bun`, not `npm`. Commits short (5–20 words), distinct and logical.
- Keep code modular and DRY; move code to the right module. Comment *why* for
  non-obvious choices.
- Offer to refactor malformed code you come across.
- Meaningful tests only. Keep OpenAPI accurate — it generates the public reference.
- UI: clean, minimalist, refined. No uppercase tracking-wider text.
- Verify runtime claims against the running stack, not just the source.
