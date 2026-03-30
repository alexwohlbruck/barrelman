import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { reverseGeocode as _reverseGeocode } from '../services/geocode.service'

export function createGeocodeRoutes(deps = { reverseGeocode: _reverseGeocode }) {
  return new Elysia()
    .use(authMiddleware)
    .get(
      '/geocode',
      async ({ query }) => {
        const { lat, lng } = query
        return deps.reverseGeocode(Number(lat), Number(lng))
      },
      {
        query: t.Object({
          lat: t.String({
            description: 'Latitude of the point to reverse geocode (WGS 84).',
            examples: ['35.2271'],
          }),
          lng: t.String({
            description: 'Longitude of the point to reverse geocode (WGS 84).',
            examples: ['-80.8431'],
          }),
        }),
        detail: {
          summary: 'Reverse geocode a coordinate',
          description: `Finds the administrative hierarchy containing the given coordinate and returns a structured address object along with the raw boundary rows.

Address components are mapped from OSM admin levels:
- \`admin_level >= 8\` → city
- \`admin_level >= 6\` → county
- \`admin_level >= 4\` → state
- \`admin_level >= 2\` → country`,
          tags: ['Geocoding'],
        },
      },
    )
}

export const geocodeRoutes = createGeocodeRoutes()
