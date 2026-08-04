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
`./start.sh dev` brings up the API **and** the console dev server
(`barrelman-console` service, Vite + HMR) at `http://localhost:5199/console`.
In production the API serves the pre-built console (multi-stage Docker build).
After changing the console, a quick sanity check: `cd web && bun run typecheck && bun run build`.
