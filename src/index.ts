import { Elysia } from 'elysia'
import { cors } from '@elysiajs/cors'
import { swagger } from '@elysiajs/swagger'
import swaggerConfig from './config/swagger'
import { healthRoutes } from './routes/health'
import { searchRoutes } from './routes/search'
import { brandsRoutes } from './routes/brands'
import { containsRoutes } from './routes/contains'
import { childrenRoutes } from './routes/children'
import { placeRoutes } from './routes/place'
import { geocodeRoutes } from './routes/geocode'
import { authRoutes } from './routes/auth'
import { passkeyRoutes } from './routes/auth-passkeys'
import { oauthRoutes } from './routes/auth-oauth'
import { accountRoutes } from './routes/account'
import { billingRoutes } from './routes/billing'
import { adminRoutes } from './routes/admin'
import { adminUserRoutes } from './routes/admin-users'
import { adminConsoleRoutes, adminConsoleConfigRoutes } from './routes/admin-console'
import { consoleUiRoutes } from './lib/console-ui'
import { tileRoutes } from './routes/tiles'
import { graphhopperRoutes } from './routes/graphhopper'
import { routeRoutes } from './routes/route'
import { isochroneRoutes } from './routes/isochrone'
import { transitRoutes } from './routes/transit'
import { gbfsRoutes } from './routes/gbfs'
import { ensureSchema, ensureGtfsSchema, ensureGbfsSchema } from './db'
import { ensureAccountsSchema } from './services/accounts.service'
import { ensureOpsJobsSchema } from './services/ops-job-store'
import { ensureRegionsSchema } from './services/region-store.service'
import { initJobHistory } from './services/job-history.service'
import { ensureSearchEnrichment } from './lib/search-enrichment'
import { ensureBrandLogos } from './lib/brand-logos'
import { startTransitWarmup } from './lib/warmup'
import { flushUsage, startUsageFlush } from './services/usage.service'
import { startOverageReporting } from './services/overage.service'
import { startAccountSweep } from './services/account-maintenance.service'
import { startScheduler } from './services/scheduler.service'
import { assertAuthConfigured } from './middleware/api-auth'
import { setPeerAddressResolver } from './lib/rate-limit'

const port = Number(process.env.PORT) || 5001

// Safety net: a stray unhandled rejection (a fire-and-forget task that forgot to
// .catch, a background poll hitting a transient upstream error) must not take the
// whole server down — that would drop search/geocoding/tiles for every client.
// This matters more now that the query pool carries a statement timeout: a query
// that used to be merely slow now rejects, and several of those run detached
// from any request (warm-up passes, GBFS/pricing refreshes, the enrichment
// backfill). Log loudly and keep serving; individual request handlers still
// surface their own errors normally.
process.on('unhandledRejection', (reason) => {
  console.error('[unhandledRejection]', reason)
})
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err)
})

// Ensure post-import columns exist before accepting requests
await ensureSchema()
await ensureGtfsSchema()
await ensureGbfsSchema()
// Console job queue (shared with the barrelman-ops worker).
await ensureOpsJobsSchema()
// Import-region store (seeded from config/regions.json; editable in the console).
await ensureRegionsSchema()
// User accounts, API keys, usage and credits for the public API.
await ensureAccountsSchema()
// Script run history, for job runtime estimates and progress bars.
await initJobHistory()

// Backfill derived search columns (codes/name_abbrev/parent_context/ts) if a
// prior import left them empty. Fire-and-forget so it never blocks startup —
// it self-skips once the data is enriched. Then resolve brand logos from
// Wikidata (needs the geo_brands catalog to exist first).
void ensureSearchEnrichment().then(() => ensureBrandLogos())

/**
 * CORS.
 *
 * The data API is meant to be called from a browser on any origin — a tile key
 * in a customer's map, the landing page demo — so the origin stays open. What
 * is NOT open is credentials: the plugin's default is `credentials: true`
 * alongside a reflected origin, which is the combination that lets any page
 * read a credentialed response.
 *
 * Nothing needs it. The console authenticates with `credentials: 'same-origin'`
 * and is reached same-origin in every layout: the API serves it at /console,
 * and the split deployment proxies /admin, /auth, /account and /billing through
 * the console's own origin (which is also why the Vite dev proxy sets
 * changeOrigin: false). So cookies never legitimately cross an origin here.
 *
 * SameSite=lax on the session cookie already stopped a cross-site fetch from
 * carrying it, and `middleware/session.ts` origin-checks cookie-authenticated
 * mutations. This removes the reliance on those being the only line of defence
 * — flipping the cookie to SameSite=none for some future embed should not
 * silently become an account-takeover bug.
 */
const app = new Elysia()
  .use(cors({ credentials: false }))
  .use(swagger(swaggerConfig))
  .use(healthRoutes)
  .use(searchRoutes)
  .use(brandsRoutes)
  .use(containsRoutes)
  .use(childrenRoutes)
  .use(placeRoutes)
  .use(geocodeRoutes)
  .use(authRoutes)
  .use(passkeyRoutes)
  .use(oauthRoutes)
  .use(accountRoutes)
  .use(billingRoutes)
  .use(adminRoutes)
  .use(adminUserRoutes)
  .use(adminConsoleConfigRoutes)
  .use(adminConsoleRoutes)
  .use(consoleUiRoutes)
  .use(tileRoutes)
  .use(graphhopperRoutes)
  .use(routeRoutes)
  .use(isochroneRoutes)
  .use(transitRoutes)
  .use(gbfsRoutes)
  .listen(port)

// Give the rate limiters a real peer address to fall back on when no proxy has
// written one. Bun exposes it only through the server object, which does not
// exist until `listen()` has run.
setPeerAddressResolver((request) => app.server?.requestIP(request)?.address ?? null)

// Keep MOTIS (and rental pricing) hot so the first trip request after an idle
// gap doesn't eat MOTIS's multi-second cold-start. Engine warming, not result
// caching — every real request still runs a fresh search. Fire-and-forget.
startTransitWarmup()

// Metered usage is buffered in memory and written periodically — see
// services/usage.service.ts for why a write per request is not viable here.
startUsageFlush()

// Metered overage is batched to the billing provider on a timer — see
// services/overage.service.ts. No-op unless Polar is configured.
startOverageReporting()

// Expired sessions and spent one-time codes are swept periodically; Lucia
// validates expiry on read but never deletes, so the rows would otherwise
// accumulate for the life of the instance.
startAccountSweep()

// Cron-driven refreshes (OSM replication, GTFS drift, GBFS). These enqueue
// ordinary jobs the ops worker executes, so a nightly import is visible and
// cancelable in the console instead of being a host crontab nobody can see.
void startScheduler()

assertAuthConfigured()

// Flush whatever is buffered before the process goes away, so a rolling deploy
// doesn't discard the last few seconds of billing counters.
for (const signal of ['SIGTERM', 'SIGINT'] as const) {
  process.on(signal, () => {
    void flushUsage().finally(() => process.exit(0))
  })
}

console.log(`Barrelman running at http://localhost:${port}`)
console.log(`API docs at http://localhost:${port}/docs`)
console.log(`Admin console at http://localhost:${port}/console`)
