import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { getPlace as _getPlace } from '../services/place.service'

export function createPlaceRoutes(deps = { getPlace: _getPlace }) {
  return new Elysia()
    .use(authMiddleware)
    .get(
      '/place/:osmType/:osmId',
      async ({ params, set }) => {
        const place = await deps.getPlace(params.osmType, params.osmId)
        if (!place) {
          set.status = 404
          return { error: 'Place not found' }
        }
        return place
      },
      {
        params: t.Object({
          osmType: t.String({
            description: 'OSM element type: `node`, `way`, or `relation`.',
            examples: ['node', 'way', 'relation'],
          }),
          osmId: t.String({
            description: 'Numeric OSM element ID.',
            examples: ['5718230659', '123456'],
          }),
        }),
        detail: {
          summary: 'Get place by OSM ID',
          description: `Fetches a single place by its OSM element type and ID (e.g. \`/place/node/5718230659\`).

Returns the full place record including:
- All raw OSM tags
- Structured address, contact info, opening hours
- All name variants (\`names\` array — multilingual / alt names)
- Abbreviated name for autocomplete (\`name_abbrev\`)
- Centroid geometry (always present)
- Full polygon/linestring geometry for ways and relations (\`full_geometry\`)
- Administrative level and area in m² for area features

Returns **404** if the place is not found in the database (either it doesn't exist in OSM or was filtered during import).`,
          tags: ['Places'],
        },
      },
    )
}

export const placeRoutes = createPlaceRoutes()
