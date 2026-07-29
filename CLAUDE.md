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
| Add a notable public API endpoint | consider adding a preset in `web/src/views/ApiTesterView.vue` |
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

### Auth footgun (important)
Elysia scopes a plugin's lifecycle hooks to that plugin instance, so
`.use(authMiddleware)` does **not** protect sibling routes on the parent
instance — it silently leaves them public. Always attach auth directly with
`.onBeforeHandle(authHandler)` (read API) or `.onBeforeHandle(adminAuthHandler)`
(`/admin/*`). Admin routes are gated by `BARRELMAN_ADMIN_KEY` (falls back to
`BARRELMAN_API_KEY`).

### Dev
`./start.sh dev` brings up the API **and** the console dev server
(`barrelman-console` service, Vite + HMR) at `http://localhost:5199/console`.
In production the API serves the pre-built console (multi-stage Docker build).
After changing the console, a quick sanity check: `cd web && bun run typecheck && bun run build`.
