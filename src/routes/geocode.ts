import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import {
  reverseGeocode as _reverseGeocode,
  fetchPeliasPlaceByGid as _fetchPeliasPlaceByGid,
} from '../services/geocode.service'

export function createGeocodeRoutes(deps = {
  reverseGeocode: _reverseGeocode,
  fetchPeliasPlaceByGid: _fetchPeliasPlaceByGid,
}) {
  return new Elysia()
    .use(authMiddleware)
    .get(
      '/geocode/place',
      async ({ query, request, set }) => {
        const place = await deps.fetchPeliasPlaceByGid(query.id, { signal: request.signal })
        if (!place) {
          set.status = 404
          return { error: 'Place not found' }
        }
        return place
      },
      {
        query: t.Object({
          id: t.String({
            description: 'Pelias global id (gid), e.g. "openaddresses:address:us/ny/city_of_new_york:7e5b…".',
            examples: ['openaddresses:address:us/ny/city_of_new_york:7e5bd55eb1baa131'],
          }),
        }),
        detail: {
          summary: 'Fetch a geocoder (Pelias) place by gid',
          description: 'Resolves a single address/street record from the Pelias geocoder by its global id. These records have no geo_places row, so they cannot be fetched via `/place/:osmType/:osmId`.',
          tags: ['Geocoding'],
        },
      },
    )
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
