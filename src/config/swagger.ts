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

// A public base URL (e.g. https://api.barrelman.dev) can be advertised as
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
worthless for running up a routing bill.

### Billing
Usage is metered in **credits**, because the endpoints are not equally
expensive: a vector tile is one indexed read, an isochrone fans out to hundreds
of routing calls.

| Group | Credits | Endpoints |
|---|---|---|
| \`tiles\` | 1 | \`/tiles/*\` |
| \`places\` | 3 | \`/place/*\`, \`/brands\` |
| \`spatial\` | 3 | \`/contains\`, \`/children\` |
| \`geocode\` | 5 | \`/geocode/*\` |
| \`search\` | 6 | \`/search\`, \`/autocomplete\` |
| \`routing\` | 12 | \`/route\`, \`/graphhopper/*\` |
| \`transit\` | 20 | \`/transit/*\`, \`/gbfs/*\` |
| \`isochrone\` | 40 | \`/isochrone\` |

Each response carries \`X-Barrelman-Credits-Charged\`.

| Plan | Price | Credits / month | Past the allowance |
|---|---|---|---|
| Free | \$0 | 100,000 | **Stops with \`402\`** |
| Developer | \$19 | 1,000,000 | \$0.030 / 1k |
| Business | \$99 | 10,000,000 | \$0.018 / 1k |
| Scale | \$299 | 40,000,000 | \$0.012 / 1k |
| Enterprise | Custom | Negotiated | \$0.008 / 1k |

The free plan **stops** at its ceiling rather than accruing charges, so nobody
can run up a bill on a plan they did not pay for; paid plans continue and meter
the overage. Rate limits are per plan and answer \`429\` with \`Retry-After\`.

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
          description: 'An account API key (brm_live_…), created in the console at /console.',
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
