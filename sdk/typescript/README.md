# @barrelman/sdk

A small TypeScript client for the [Barrelman API](https://docs.barrelman.dev):
place search, geocoding, OSM places, routing, isochrones, map matching, transit
and map tiles. It has no runtime dependencies and uses only `fetch`, so it runs
in browsers, Node 18+, Bun and Deno.

Request types are generated from the API's OpenAPI spec.

## Install

The package is coming to npm. Until it is published, build it from this
directory:

```bash
bun install
bun run build    # writes dist/
```

Once published:

```bash
bun add @barrelman/sdk     # or: npm install @barrelman/sdk
```

## Usage

Create a key in the [console](https://api.barrelman.dev/console).

```ts
import { createClient, BarrelmanError } from '@barrelman/sdk'

const barrelman = createClient({ apiKey: process.env.BARRELMAN_API_KEY! })

// Search near a point
const cafes = await barrelman.search({ query: 'coffee', lat: 35.2271, lng: -80.8431, radius: 2000 })

// Typeahead: skips the semantic layer for lower latency
const suggestions = await barrelman.autocomplete({ query: 'sta', lat: 35.2271, lng: -80.8431, limit: 5 })

// Geocoding
const matches = await barrelman.geocode.forward('600 E 4th St, Charlotte')
const nearby = await barrelman.geocode.reverse({ lat: 35.3079, lng: -80.7332 })
const areas = await barrelman.geocode.hierarchy({ lat: 35.3079, lng: -80.7332 })

// One OSM element
const place = await barrelman.place('way', 30521456)

// Routing (GraphHopper request body, points as [lng, lat])
const route = await barrelman.route({ profile: 'bike', points: [[-80.8431, 35.2271], [-80.7332, 35.3079]] })
const reachable = await barrelman.isochrone({ lat: 35.2271, lng: -80.8431, mode: 'walk', durations: [600, 1200] })
const matched = await barrelman.match({
  profile: 'car',
  points: [
    { lat: 35.2271, lon: -80.8431 },
    { lat: 35.2085, lon: -80.812 },
  ],
})

// Transit
const trip = await barrelman.transit.route({ from: { lat: 35.2271, lng: -80.8431 }, to: { lat: 35.3079, lng: -80.7332 } })
const stops = await barrelman.transit.stops({ lat: 35.2271, lng: -80.8431, radius: 500 })
const boards = await barrelman.transit.departures({ lat: 35.2271, lng: -80.8431 })
```

Response bodies are typed `unknown` for now, because the OpenAPI spec does not
describe them yet. Pass the shape you expect as a type argument:

```ts
interface Place { name: string; lat: number; lng: number }
const results = await barrelman.search<Place[]>({ query: 'library' })
```

Any other endpoint is reachable through `request`:

```ts
const brands = await barrelman.request('/brands', { query: { q: 'star' } })
```

Every method takes an optional last argument `{ signal, headers }`, so a
request can be cancelled with an `AbortController`.

### Self-hosted instances

```ts
const barrelman = createClient({ apiKey, baseUrl: 'http://localhost:5001' })
```

## Maps

A map library fetches the style, TileJSON and tiles itself and cannot set a
header, so the URL helpers carry the key as `?api_key=` by default. The API
copies a URL key on to every tile URL it hands back.

```ts
import maplibregl from 'maplibre-gl'

new maplibregl.Map({
  container: 'map',
  style: barrelman.tiles.styleUrl('basemap'), // or 'basemap-dark'
})
```

Use a key scoped to `tiles` in the browser. To keep the key out of URLs, build
them with `{ withKey: false }` and let `transformRequest` add the header to
requests for this API only:

```ts
new maplibregl.Map({
  container: 'map',
  style: barrelman.tiles.styleUrl('basemap', { withKey: false }),
  transformRequest: barrelman.tiles.transformRequest,
})
```

`tiles.tileJsonUrl(source)` and `tiles.tileUrlTemplate(source)` build the URLs
for a single source or bundle, and `tiles.tileJson(source)` fetches the
document.

## Errors

A non-2xx response throws a `BarrelmanError`:

| Field | |
|---|---|
| `status` | HTTP status |
| `code` | Stable refusal code, such as `invalid_key`, `scope_denied` or `rate_limited` |
| `error` | The message from the body. Its wording may change; branch on `code` |
| `requestId` | The response's `X-Request-Id`. Include it when reporting a problem |
| `retryAfter` | Seconds from `Retry-After`, sent with every 429 |
| `body` | The parsed body |

```ts
try {
  await barrelman.search({ query: 'coffee' })
} catch (error) {
  if (error instanceof BarrelmanError && error.code === 'insufficient_credits') {
    // The allowance for this billing period is used up
  }
  throw error
}
```

The codes are listed in [Stability and errors](https://docs.barrelman.dev/usage/stability).

## Retries

Retries are off by default, since a retried request is charged again if it
reaches the endpoint. Turn them on for 429 and 5xx responses:

```ts
createClient({ apiKey, retry: 3 })
createClient({ apiKey, retry: { retries: 3, minDelayMs: 250, maxDelayMs: 5000 } })
```

The client waits for `Retry-After` when the response carries one, and otherwise
backs off exponentially with jitter. A `Retry-After` longer than `maxDelayMs` is
thrown to the caller instead of being waited out. `too_many_failures` is never
retried: it answers requests that could not succeed, such as a wrong key.

## Development

```bash
bun install
bun test
bun run typecheck
bun run generate                                       # regenerate src/schema.ts from ../../docs/openapi.json
bun run generate https://api.barrelman.dev/docs/json   # or from a URL or another file
bun run check:schema                                   # CI: fail if src/schema.ts is stale
bun run build
```
