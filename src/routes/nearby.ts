import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { findNearby as _findNearby } from '../services/spatial.service'

export function createNearbyRoutes(deps = { findNearby: _findNearby }) {
  return new Elysia()
    .use(authMiddleware)
    .post(
      '/nearby',
      async ({ body }) => {
        const { lat, lng, radius, categories, tags, limit, offset } = body
        return deps.findNearby({ lat, lng, radius, categories, tags, limit, offset })
      },
      {
        body: t.Object({
          lat: t.Number({
            description: 'Latitude of the center point (WGS 84)',
            examples: [35.2271],
          }),
          lng: t.Number({
            description: 'Longitude of the center point (WGS 84)',
            examples: [-80.8431],
          }),
          radius: t.Optional(t.Number({
            default: 1000,
            description: 'Search radius in meters. Results are filtered to places within this distance from the center point. Max recommended: 50000.',
            examples: [500, 1000, 5000],
          })),
          categories: t.Optional(t.Array(t.String(), {
            description: 'OSM preset category IDs to filter by (e.g. ["bicycle_parking", "cafe"]). When omitted all place types are returned. Multiple values are OR\'d together.',
            examples: [['bicycle_parking'], ['cafe', 'restaurant', 'bar']],
          })),
          tags: t.Optional(t.Record(t.String(), t.String(), {
            description: 'Additional OSM tag key/value pairs that must all be present on the place (JSONB containment). Used to filter sub-preset categories, e.g. `{"cuisine":"pizza"}` to narrow `amenity/restaurant` results to pizza restaurants.',
            examples: [{ cuisine: 'pizza' }, { sport: 'table_tennis' }],
          })),
          limit: t.Optional(t.Number({
            default: 20,
            description: 'Maximum number of results to return.',
            examples: [20, 50, 100],
          })),
          offset: t.Optional(t.Number({
            default: 0,
            description: 'Number of results to skip for pagination.',
            examples: [0, 20, 40],
          })),
        }),
        detail: {
          summary: 'Find nearby places',
          description: `Returns places within a given radius of a coordinate point, sorted by distance ascending.

Results are filtered using a two-step approach:
1. Bounding box pre-filter hits the GIST spatial index for fast candidate selection.
2. Geography-accurate \`ST_DWithin\` enforces the true circular radius.

Responses are cached keyed on all input parameters. Cache is invalidated on server restart.`,
          tags: ['Spatial'],
        },
      },
    )
}

export const nearbyRoutes = createNearbyRoutes()
