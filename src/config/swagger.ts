/**
 * OpenAPI / Swagger documentation config for the Barrelman API.
 *
 * Served by @elysiajs/swagger (Scalar UI) at `/docs`. Route-level docs come
 * from each handler's `detail: { tags, summary, description }`; this file
 * supplies the top-level metadata, tag groups, and server list.
 */

// Keep in sync with package.json.
const VERSION = '0.1.0'

const localUrl = `http://localhost:${process.env.PORT || 5001}`

// A public base URL (e.g. https://barrelman.parchment.app) can be advertised as
// the default "try it" server via PUBLIC_BASE_URL; localhost is always offered.
const servers = [
  ...(process.env.PUBLIC_BASE_URL
    ? [{ url: process.env.PUBLIC_BASE_URL.replace(/\/$/, ''), description: 'Production' }]
    : []),
  { url: localUrl, description: 'Local development' },
]

const description = `
**Barrelman** is a self-hosted OpenStreetMap geospatial engine that powers Parchment —
fuzzy place search, geocoding, spatial containment/children queries, vector tiles,
multimodal routing (GraphHopper / Valhalla / MOTIS), and live transit (GTFS + GTFS-RT).

### Data
OSM extracts are imported per region (see \`config/regions.json\`) and enriched into a
searchable \`geo_places\` catalog, alongside GTFS transit and GBFS shared-mobility feeds.

### Authentication
Public read endpoints (search, geocoding, places, brands, tiles) are open. Transit
(\`/transit/*\`) and admin (\`/admin/*\`) endpoints require a bearer token — send
\`Authorization: Bearer <BARRELMAN_API_KEY>\`.
`.trim()

const swaggerConfig = {
  path: '/docs',
  documentation: {
    info: {
      title: 'Barrelman API',
      version: VERSION,
      description,
      contact: { name: 'Parchment', url: 'https://parchment.app' },
    },
    servers,
    components: {
      securitySchemes: {
        bearerAuth: {
          type: 'http',
          scheme: 'bearer',
          description: 'BARRELMAN_API_KEY for /transit/* and /admin/* endpoints.',
        },
      },
    },
    tags: [
      { name: 'Health', description: 'Liveness and dependency (DB, MOTIS) health checks.' },
      { name: 'Search', description: 'Fuzzy place and category search over the OSM geo_places catalog.' },
      { name: 'Geocoding', description: 'Forward and reverse geocoding (Pelias-backed).' },
      { name: 'Places', description: 'Place details, enrichment, and lookups by OSM id.' },
      { name: 'Brands', description: 'Brand catalog — chains, logos, and their locations.' },
      { name: 'Tiles', description: 'Mapbox Vector Tiles served via the Martin tile server.' },
      { name: 'Routing', description: 'Point-to-point route planning across travel profiles.' },
      { name: 'GraphHopper', description: 'GraphHopper routing engine passthrough and status.' },
      { name: 'Transit', description: 'GTFS schedules and GTFS-RT live vehicles, trips, and departures. Auth required.' },
      { name: 'GBFS', description: 'Shared-mobility (bike/scooter) systems and stations via GBFS.' },
      { name: 'Admin', description: 'Operator console API — scripts, jobs, and metrics. Auth required.' },
    ],
  },
}

export default swaggerConfig
