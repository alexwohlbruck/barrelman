/**
 * OpenAPI / Swagger documentation config for the Barrelman API.
 *
 * Served by @elysiajs/swagger (Scalar UI) at `/docs`. Route-level docs come
 * from each handler's `detail: { tags, summary, description }`; this file
 * supplies the top-level metadata, tag groups, and server list.
 */

// Keep in sync with package.json.
const VERSION = '0.4.0'

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
Every data endpoint requires an API key. Create one in the
[console](/console) and send it as \`Authorization: Bearer brm_live_...\`.
Tile URLs also accept \`?api_key=\` (or the older \`?token=\`), since a map
library fetches tiles itself and cannot set a header.

Keys carry **scopes** limiting which endpoint groups they may call, so a key
embedded in a web map can be restricted to tiles and search and is then
worthless for running up a routing bill. Keys created in the \`test\`
environment (\`brm_test_...\`) exercise the whole request path — auth, scopes,
rate limits — without spending credits.

### Billing
Usage is metered in **credits**, because the endpoints are not equally
expensive: a vector tile is one indexed read, an isochrone fans out to hundreds
of routing calls.

| Group | Credits | Endpoints |
|---|---|---|
| \`tiles\` | 1 | \`/tiles/*\` |
| \`places\` | 2 | \`/place/*\`, \`/brands\` |
| \`spatial\` | 2 | \`/contains\`, \`/children\` |
| \`geocode\` | 2 | \`/geocode/*\` |
| \`search\` | 3 | \`/search\`, \`/autocomplete\` |
| \`routing\` | 10 | \`/route\`, \`/graphhopper/*\` |
| \`transit\` | 25 | \`/transit/*\`, \`/gbfs/*\` |
| \`isochrone\` | 25 | \`/isochrone\` |

Each response carries \`X-Barrelman-Credits-Charged\`. The free plan includes
50,000 credits per month and **stops** at that ceiling with a \`402\` rather
than accruing charges; paid plans continue and bill the overage. Rate limits are
per plan and answer \`429\` with \`Retry-After\`.

See \`GET /account/plans\` for the current pricing table.
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
          description: 'An account API key (brm_live_… / brm_test_…), created in the console at /console.',
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
      { name: 'Isochrone', description: 'Reachability polygons — how far you get in N minutes, per travel mode.' },
      { name: 'GraphHopper', description: 'GraphHopper routing engine passthrough and status.' },
      { name: 'Transit', description: 'GTFS schedules and GTFS-RT live vehicles, trips, and departures.' },
      { name: 'GBFS', description: 'Shared-mobility (bike/scooter) systems and stations via GBFS.' },
      { name: 'Auth', description: 'Sign-in: email codes, passkeys and OAuth. Used by the console, not by API clients.' },
      { name: 'Account', description: 'API keys, usage and credit balance for the signed-in account.' },
      { name: 'Billing', description: 'Plans, checkout and the metered-usage webhook.' },
      { name: 'Admin', description: 'Operator console API — scripts, jobs, and metrics. Admin access required.' },
    ],
  },
}

export default swaggerConfig
