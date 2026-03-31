import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { searchPlaces as _searchPlaces } from '../services/search.service'

export function createSearchRoutes(deps = { searchPlaces: _searchPlaces }) {
  return new Elysia()
    .use(authMiddleware)
    .post(
      '/search',
      async ({ body }) => {
        return deps.searchPlaces(body)
      },
      {
        body: t.Object({
          query: t.Optional(t.String({
            description: 'Search query text. When omitted, returns places matching categories/tags sorted by proximity (browse mode).',
            examples: ['coffee', 'Starbucks', 'UNCC'],
          })),
          lat: t.Optional(t.Number({
            description: 'Latitude for point-based spatial filtering and proximity ranking (WGS 84).',
            examples: [35.2271],
          })),
          lng: t.Optional(t.Number({
            description: 'Longitude for point-based spatial filtering and proximity ranking (WGS 84).',
            examples: [-80.8431],
          })),
          radius: t.Optional(t.Number({
            description: 'Search radius in meters around the lat/lng point. Max recommended: 50000.',
            examples: [1000, 5000, 25000],
          })),
          route: t.Optional(t.Object({
            type: t.Literal('LineString'),
            coordinates: t.Array(t.Array(t.Number(), { minItems: 2, maxItems: 2 }), {
              description: 'Array of [lng, lat] coordinate pairs defining the route',
            }),
          }, {
            description: 'GeoJSON LineString geometry. When provided, search is constrained to a corridor around this route instead of a point radius.',
          })),
          buffer: t.Optional(t.Number({
            default: 1000,
            description: 'Corridor width in meters when using route mode. Places closer to the route are strongly preferred via exponential decay ranking.',
            examples: [500, 1000, 2000, 5000],
          })),
          categories: t.Optional(t.Array(t.String(), {
            description: 'OSM preset category IDs to filter by (e.g. ["fuel", "cafe"]). Multiple values are OR\'d together.',
            examples: [['fuel'], ['cafe', 'restaurant']],
          })),
          tags: t.Optional(t.Record(t.String(), t.String(), {
            description: 'Additional OSM tag key/value pairs that must all be present (JSONB containment).',
            examples: [{ cuisine: 'pizza' }, { sport: 'table_tennis' }],
          })),
          limit: t.Optional(t.Number({
            default: 20,
            description: 'Maximum number of results to return.',
            examples: [10, 20, 50],
          })),
          offset: t.Optional(t.Number({
            default: 0,
            description: 'Number of results to skip for pagination (browse mode only).',
            examples: [0, 20, 40],
          })),
          semantic: t.Optional(t.Boolean({
            default: false,
            description: 'Force semantic vector search for concept queries (e.g. "somewhere quiet to study"). Requires Ollama.',
          })),
          autocomplete: t.Optional(t.Boolean({
            default: false,
            description: 'Enable autocomplete mode — skips the slow semantic layer for low-latency typeahead.',
          })),
        }),
        detail: {
          summary: 'Search places',
          description: `Unified search endpoint supporting text search, category browsing, and route corridor search.

**Text search** (provide \`query\`): Runs a hybrid four-layer pipeline:
1. Full-text search (FTS) via \`tsvector\` GIN index
2. Trigram fuzzy match via \`pg_trgm\` GIN index
3. Abbreviation match on pre-computed \`name_abbrev\`
4. Semantic vector search via \`pgvector\` (conditional)

Results are deduplicated in priority order (FTS > abbreviation > trigram > semantic) then re-ranked with proximity decay when coordinates are provided.

**Browse mode** (omit \`query\`, provide \`categories\` and/or \`tags\`): Returns matching places sorted by distance. Requires a spatial constraint (lat/lng or route).

**Spatial modes:**
- **Point**: \`lat\` + \`lng\` + optional \`radius\` — search within a circular area
- **Route corridor**: \`route\` (GeoJSON LineString) + optional \`buffer\` — search along a path with exponential proximity decay

Both modes can be combined with text search and/or category/tag filters.`,
          tags: ['Search'],
        },
      },
    )
}

export const searchRoutes = createSearchRoutes()
