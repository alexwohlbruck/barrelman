import { Elysia } from 'elysia'
import { cors } from '@elysiajs/cors'
import { swagger } from '@elysiajs/swagger'
import { healthRoutes } from './routes/health'
import { searchRoutes } from './routes/search'
import { brandsRoutes } from './routes/brands'
import { containsRoutes } from './routes/contains'
import { childrenRoutes } from './routes/children'
import { placeRoutes } from './routes/place'
import { geocodeRoutes } from './routes/geocode'
import { adminRoutes } from './routes/admin'
import { adminConsoleRoutes, adminConsoleConfigRoutes } from './routes/admin-console'
import { consoleUiRoutes } from './lib/console-ui'
import { tileRoutes } from './routes/tiles'
import { graphhopperRoutes } from './routes/graphhopper'
import { routeRoutes } from './routes/route'
import { transitRoutes } from './routes/transit'
import { gbfsRoutes } from './routes/gbfs'
import { ensureSchema, ensureGtfsSchema, ensureGbfsSchema } from './db'
import { initJobHistory } from './services/job-history.service'
import { ensureSearchEnrichment } from './lib/search-enrichment'
import { ensureBrandLogos } from './lib/brand-logos'
import { startTransitWarmup } from './lib/warmup'

const port = Number(process.env.PORT) || 5001

// Safety net: a stray unhandled rejection (a fire-and-forget task that forgot to
// .catch, a background poll hitting a transient upstream error) must not take the
// whole server down — that would drop search/geocoding/tiles for every client.
// Log loudly and keep serving; individual request handlers still surface their
// own errors normally.
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
await initJobHistory()

// Backfill derived search columns (codes/name_abbrev/parent_context/ts) if a
// prior import left them empty. Fire-and-forget so it never blocks startup —
// it self-skips once the data is enriched. Then resolve brand logos from
// Wikidata (needs the geo_brands catalog to exist first).
void ensureSearchEnrichment().then(() => ensureBrandLogos())

const app = new Elysia()
  .use(cors())
  .use(
    swagger({
      documentation: {
        info: {
          title: 'Barrelman',
          version: '0.3.0',
          description: 'OSM geospatial engine — search, tiles, spatial queries',
        },
      },
    }),
  )
  .use(healthRoutes)
  .use(searchRoutes)
  .use(brandsRoutes)
  .use(containsRoutes)
  .use(childrenRoutes)
  .use(placeRoutes)
  .use(geocodeRoutes)
  .use(adminRoutes)
  .use(adminConsoleConfigRoutes)
  .use(adminConsoleRoutes)
  .use(consoleUiRoutes)
  .use(tileRoutes)
  .use(graphhopperRoutes)
  .use(routeRoutes)
  .use(transitRoutes)
  .use(gbfsRoutes)
  .listen(port)

// Keep MOTIS (and rental pricing) hot so the first trip request after an idle
// gap doesn't eat MOTIS's multi-second cold-start. Engine warming, not result
// caching — every real request still runs a fresh search. Fire-and-forget.
startTransitWarmup()

console.log(`Barrelman running at http://localhost:${port}`)
console.log(`Swagger docs at http://localhost:${port}/swagger`)
console.log(`Admin console at http://localhost:${port}/console`)
