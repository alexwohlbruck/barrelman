import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { findContainingAreas as _findContainingAreas } from '../services/spatial.service'

export function createContainsRoutes(deps = { findContainingAreas: _findContainingAreas }) {
  return new Elysia()
    .use(authMiddleware)
    .get(
      '/contains',
      async ({ query: q }) => {
        const { lat, lng, exclude } = q
        return deps.findContainingAreas({ lat: Number(lat), lng: Number(lng), exclude })
      },
      {
        query: t.Object({
          lat: t.String({
            description: 'Latitude of the point to test containment for (WGS 84).',
            examples: ['35.2271'],
          }),
          lng: t.String({
            description: 'Longitude of the point to test containment for (WGS 84).',
            examples: ['-80.8431'],
          }),
          exclude: t.Optional(t.String({
            description: 'Barrelman place ID to exclude from results. Useful to omit the place you are currently viewing (e.g. the place whose detail page you are on).',
            examples: ['way/123456'],
          })),
        }),
        detail: {
          summary: 'Find areas containing a point',
          description: `Returns all named area geometries whose polygons contain the given coordinate point. Results are ordered smallest-first (innermost area first), so the most specific containing region appears at index 0.

Typical use: reverse-geocode a coordinate to its administrative hierarchy (building → neighbourhood → city → county → state → country) or find which venue/campus a point is inside.

Only areas with names are returned. \`building:part\` features are excluded. Both centroid and full GeoJSON geometry are included in the response.`,
          tags: ['Search'],
        },
      },
    )
}

export const containsRoutes = createContainsRoutes()
