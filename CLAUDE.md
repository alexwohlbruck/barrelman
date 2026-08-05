# Barrelman — project guide for Claude

Barrelman is an OSM geospatial API (Elysia/Bun + PostGIS) that powers Parchment.
It imports and processes OSM/GTFS/GBFS data and serves search, tiles, spatial
queries, and routing. See `README.md` for architecture and setup.

## ⚠️ Keep the Admin Console in sync

There is an internal **management dashboard / admin console** (`web/`, Vue 3 +
Reka UI, served by the API at `/console`) for running every data task, watching
job logs, and monitoring health. Its script catalog and metrics are **manually
maintained** — they do not auto-discover changes. So whenever you change
barrelman, check whether the console needs a matching update.

**Before finishing a change, run this checklist. Update the console if any apply:**

| If you… | Then update… |
|---|---|
| Add / remove / rename a runnable script (`scripts/*.sh`, `import/*.ts`/`.sql`/`.py`, a `package.json` script, or a new `admin.service` migration) | `src/admin/scripts-manifest.ts` — add/edit/remove the `SCRIPTS` entry (id, name, description, category, danger, longRunning, params, exec) |
| Change a script's CLI flags, positional args, or env vars | that script's `params` in `src/admin/scripts-manifest.ts` |
| Add an in-process SQL/migration task (`exec.kind: 'internal'`) | also add its handler in `src/services/admin-internal-handlers.ts` |
| Add / rename a DB table or a coverage-relevant column (e.g. new `gtfs_*`/`gbfs_*` table, a new enrichment column on `geo_places`) | the queries in `src/services/admin-metrics.service.ts` (and `DataMetrics` type + `web/src/lib/types.ts` + the Dashboard/Data views if it should be shown) |
| Add / remove a downstream service, or change its URL / health endpoint | `getServiceStatuses()` in `src/services/admin-metrics.service.ts` |
| Add a notable public API endpoint | consider adding a preset in `web/src/views/ApiTesterView.vue` — but only for endpoints that need no session, since the tester replays requests server-side and carries no cookie |
| Add a metered public endpoint | give it a group in `groupForPath()` (`src/billing/plans.ts`) and attach `apiAuth('<group>')`, or it is served free and unattributed |
| Add a new `/admin/*` route | gate it with `.onBeforeHandle(authHandler)` / `.onBeforeHandle(adminAuthHandler)` — **not** `.use(authMiddleware)` (see below) |

If a change genuinely doesn't touch scripts, tables, services, or endpoints
(e.g. an internal refactor), no console update is needed — just confirm you
considered it.

### Console architecture (where things live)
- Script catalog (pure data, sent to the browser): `src/admin/scripts-manifest.ts`
- Job runner (spawns processes / runs internal handlers, streams logs via SSE): `src/services/job-runner.service.ts`
- Internal (in-process) task handlers: `src/services/admin-internal-handlers.ts`
- Metrics + service health: `src/services/admin-metrics.service.ts`
- Admin API routes: `src/routes/admin-console.ts` (console) and `src/routes/admin.ts` (migrations)
- SPA served at `/console`: `src/lib/console-ui.ts`
- Frontend: `web/` (views in `web/src/views/`, shared types in `web/src/lib/types.ts` — keep in sync with the backend shapes)

### Public API accounts (`/auth`, `/account`, `/billing`)
Barrelman is a public, metered API as well as an internal service. Developers
sign in to the console (email code / passkey / OAuth), mint their own
`brm_live_*` keys, and are billed in credits.

- Sign-in + sessions (Lucia): `src/services/auth.service.ts`, `src/lib/lucia.ts`
- Passkeys + OAuth: `src/services/passkey.service.ts`, `src/services/oauth.service.ts`
- API keys: `src/services/api-keys.service.ts`
- Pricing, plans, scopes and path→group mapping: `src/billing/plans.ts`
- Metering (buffered, flushed on a timer): `src/services/usage.service.ts`
- Balances and quota decisions: `src/services/credits.service.ts`
- Polar + overage reporting: `src/services/billing.service.ts`, `src/services/overage.service.ts`
- Layered throttling (penalty box / IP / key / account / concurrency): `src/services/throttle.service.ts`
- Suspension, bans, audit log, abuse signals: `src/services/moderation.service.ts`
- Automated detection, run on the sweep: `src/services/abuse-detection.service.ts`
- Admin moderation API: `src/routes/admin-users.ts`

Suspension must take effect immediately, so `suspendUser()` tears down all three
things that hold access open: sessions, the API-key cache and the balance cache.
Adding a new cache keyed on account state means adding it there too.

`BARRELMAN_API_KEY` remains a shared, unmetered **service** credential — it is
how Parchment calls barrelman — and is deliberately never billed.

### Auth footgun (important)
Elysia scopes a plugin's lifecycle hooks to that plugin instance, so
`.use(authMiddleware)` does **not** protect sibling routes on the parent
instance — it silently leaves them public. Always attach auth directly with
`.onBeforeHandle(apiAuth('<group>'))` (metered read API) or
`.onBeforeHandle(adminAuthHandler)` (`/admin/*`).

This is not hypothetical: `/brands`, `/children`, `/contains` and `/geocode`
used `.use(authMiddleware)` and were reachable **with no key at all** while
`BARRELMAN_API_KEY` was set — confirmed against a running server before being
fixed. If you add a route file, check it with an unauthenticated `curl`.

Admin routes accept either an admin-role session or `BARRELMAN_ADMIN_KEY`
(falling back to `BARRELMAN_API_KEY`).

### Dev
`./start.sh dev` brings up everything: the API, database, engines, the console
dev server (`barrelman-console`, Vite + HMR) at `http://localhost:5199/console`,
and — when `../barrelman-landing` is checked out — the marketing site at
`http://localhost:5200` behind the `landing` compose profile. In production the
API serves the pre-built console (multi-stage Docker build).

Prefer compose over running things by hand; the user works this way.

After changing the console: `cd web && bun run typecheck && bun run build`.

Two hot-reload caveats that have cost real debugging time:

- **New dependencies are not picked up** — `package.json` is baked into the
  image, not mounted. Use `./start.sh dev --build`, which also renews the
  anonymous `node_modules` volumes; Compose reuses those across recreation, so
  rebuilding alone leaves a stale `node_modules` masking the new image and the
  API crash-loops on a module that is present in the image. `docker cp` +
  `bun install` patches the running container but is lost on recreation.
- **Module-level singletons survive a hot reload.** `bun --hot` re-evaluates a
  module body but keeps instances constructed at import time (the Lucia client,
  the Polar client, interval timers). If a change appears not to apply,
  `docker restart barrelman` before hunting for a bug.

### Adding an environment variable
Three places, or it will not work: `.env.example`, `docs/configuration.md`, and
the `barrelman` service's `environment:` block in `docker-compose.yml`. Compose
does not forward the host environment, so a variable missing from that block is
absent inside the container while looking configured everywhere else.

**Read it with `envNumber`/`envString` from `src/config/env.ts`, never
`process.env.X ?? default`.** Compose forwards optional settings as
`${VAR:-}`, which defines them as the *empty string* — `??` never reaches its
fallback and `Number('')` is 0. That is not theoretical: blank
`BARRELMAN_OTP_TTL_MINUTES` made every sign-in code expire in the millisecond it
was issued, blank `BARRELMAN_SESSION_TTL_DAYS` made Lucia mint dead sessions,
blank `BARRELMAN_ACCOUNT_SWEEP_MS` ran the sweep in a tight loop, and blank
`BARRELMAN_IP_RPM` throttled every caller to one request per minute.

### Testing Elysia guards
Never stub a lifecycle hook (`onBeforeHandle`, `onAfterHandle`) with `mock()`.
Elysia compiles its handler chain by inspecting the hook function, and a bun
mock defeats that: the guard's refusal is returned as the response **but the
handler still runs**. A test written that way reports a passing guard while the
side effect happened anyway. Use a plain function and count calls by hand — see
`src/routes/admin-users.test.ts`.

### Docs
Prose documentation lives in `docs/` — `development.md`, `accounts.md`,
`pricing.md`, `abuse-controls.md`, `polar-setup.md`, `configuration.md`, indexed
by `docs/README.md`. The top-level README covers architecture, import and
deployment and links out rather than restating.

When you change behaviour, update the doc that owns it: pricing numbers live in
`src/billing/plans.ts` and are described in `docs/pricing.md`; every environment
variable belongs in both `.env.example` and `docs/configuration.md`.
