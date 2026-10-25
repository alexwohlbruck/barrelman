/**
 * OpenAPI / Swagger documentation config for the Barrelman API.
 *
 * Served by @elysiajs/swagger (Scalar UI) at `/docs`. Route-level docs come
 * from each handler's `detail: { tags, summary, description }`; this file
 * supplies the top-level metadata, tag groups, and server list.
 */

// Read from package.json rather than restated here. This used to be a literal
// with a "keep in sync" comment above it, and it drifted — the spec advertised
// 0.4.0 while the released version was 0.1.0. package.json is what the release
// pipeline tags from, so it is the only version that means anything.
import pkg from '../../package.json' with { type: 'json' }

const VERSION = pkg.version

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
| \`routing\` | 12 | \`/route\`, \`/match\`, \`/graphhopper/*\` |
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

// The GraphHopper passthrough is one `.all('/graphhopper/*')` route, and
// @elysiajs/swagger drops every path containing `*`, so it never reached the
// spec or the docs. Declared by hand here; keep in step with routes/graphhopper.ts.
const graphhopperOperation = (method: 'get' | 'post') => ({
  tags: ['GraphHopper'],
  summary: method === 'get' ? 'GraphHopper passthrough (GET)' : 'GraphHopper passthrough (POST)',
  description:
    'Forwards the request to the GraphHopper engine unchanged — method, query string, body and content type ' +
    'are preserved, and the response is GraphHopper\'s own. Any GraphHopper endpoint works under this prefix ' +
    '(`route`, `isochrone`, `matrix`, `match`, `info`, `health`); see the ' +
    '[GraphHopper API docs](https://docs.graphhopper.com/) for their parameters. Prefer `POST /route` and ' +
    '`POST /match` for enriched, stable responses. Each call is capped by `GRAPHHOPPER_TIMEOUT_MS` and answers ' +
    '`504` past it. Billed as `routing`.',
  parameters: [
    {
      name: 'endpoint',
      in: 'path',
      required: true,
      description: 'GraphHopper endpoint, e.g. `route` or `info`.',
      schema: { type: 'string', example: 'route' },
    },
  ],
  ...(method === 'post'
    ? { requestBody: { description: 'Passed through as-is (JSON, or GPX for `match`).', content: { 'application/json': { schema: { type: 'object' } } } } }
    : {}),
  responses: {
    '200': { description: 'GraphHopper\'s response, unmodified.' },
    '502': { description: 'GraphHopper is unreachable.' },
    '504': { description: 'GraphHopper did not answer within `GRAPHHOPPER_TIMEOUT_MS`.' },
  },
})

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
    paths: {
      '/graphhopper/{endpoint}': { get: graphhopperOperation('get'), post: graphhopperOperation('post') },
    },
    components: {
      securitySchemes: {
        bearerAuth: {
          type: 'http',
          scheme: 'bearer',
          description: 'An account API key (brm_live_…), created in the console at /console.',
        },
      },
    },
    // These descriptions open each page of the public API reference
    // (docs/content/docs/api/*.mdx is generated from them), so write them for
    // a reader there. `x-displayName` overrides fumadocs' title-casing of the
    // tag name, which turns GBFS into "G B F S".
    tags: [
      { name: 'Health', description: 'Liveness and dependency checks. `GET /health/` is public and safe for load-balancer probes, and reports each endpoint group\'s status; `GET /health/auth` also validates your bearer token.' },
      { name: 'Search', description: 'Fuzzy place and category search over the OSM catalog, plus spatial relationship queries — which areas contain a point, and the children of an area.' },
      { name: 'Geocoding', description: 'Reverse geocode a coordinate to addresses and places, and fetch a geocoder (Pelias) place by its gid.' },
      { name: 'Places', description: 'Full place detail by OSM identity (type + id), with enrichment.' },
      { name: 'Brands', description: 'Autocomplete over the brand catalog to resolve chains and franchises, and fetch a single brand by key.' },
      { name: 'Tiles', description: 'Mapbox Vector Tiles served by Martin, plus precomputed bridge deck meshes, 3D landmark placements and models, and corrected transit geometry. Tile URLs accept `?api_key=` because map libraries fetch tiles themselves and cannot set a header.' },
      { name: 'Routing', description: 'Turn-by-turn routing enriched with elevation and path details, and map matching: snapping a GPS trace — dense or sparse — to the roads it followed.' },
      { name: 'Isochrone', description: 'Reachability polygons — how far you get in N minutes, per travel mode.' },
      { name: 'GraphHopper', 'x-displayName': 'GraphHopper', description: 'Raw passthrough to the GraphHopper routing engine, for its endpoints that barrelman does not wrap. Responses are GraphHopper\'s own.' },
      { name: 'Transit', description: 'Trip planning via MOTIS plus live GTFS-RT data — vehicle positions, departures, alerts, stops, route detail, and station geometry.' },
      { name: 'GBFS', 'x-displayName': 'GBFS', description: 'Shared-mobility (bikeshare and scooter) systems and real-time station availability from GBFS feeds.' },
      { name: 'Auth', description: 'Sign-in: email codes, passkeys and OAuth. Used by the console, not by API clients, which authenticate with an API key.' },
      { name: 'Account', description: 'The signed-in account: profile, terms, API keys and their scopes, usage and credit balance.' },
      { name: 'Billing', description: 'Plans, checkout, the customer portal and the metered-usage webhook. Only where billing is enabled (the hosted service); elsewhere these answer that billing is not enabled.' },
      { name: 'Admin', description: 'Operator console API — data migrations, runnable scripts and jobs, schedules, regions, accounts, metrics and downstream service health. Admin access required; not part of the public query surface.' },
    ],
  },
}

export default swaggerConfig
