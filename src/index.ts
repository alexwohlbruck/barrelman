import { Elysia } from 'elysia'
import { cors } from '@elysiajs/cors'
import { swagger } from '@elysiajs/swagger'
import { healthRoutes } from './routes/health'
import { searchRoutes } from './routes/search'
import { containsRoutes } from './routes/contains'
import { childrenRoutes } from './routes/children'
import { placeRoutes } from './routes/place'
import { geocodeRoutes } from './routes/geocode'
import { adminRoutes } from './routes/admin'
import { tileRoutes } from './routes/tiles'
import { graphhopperRoutes } from './routes/graphhopper'
import { routeRoutes } from './routes/route'
import { transitRoutes } from './routes/transit'
import { gbfsRoutes } from './routes/gbfs'
import { ensureSchema, ensureGtfsSchema, ensureGbfsSchema } from './db'

const port = Number(process.env.PORT) || 5001

// Ensure post-import columns exist before accepting requests
await ensureSchema()
await ensureGtfsSchema()
await ensureGbfsSchema()

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
  .use(containsRoutes)
  .use(childrenRoutes)
  .use(placeRoutes)
  .use(geocodeRoutes)
  .use(adminRoutes)
  .use(tileRoutes)
  .use(graphhopperRoutes)
  .use(routeRoutes)
  .use(transitRoutes)
  .use(gbfsRoutes)
  .listen(port)

console.log(`Barrelman running at http://localhost:${port}`)
console.log(`Swagger docs at http://localhost:${port}/swagger`)
