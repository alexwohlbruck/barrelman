import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import {
  reverseGeocode as _reverseGeocode,
  reverseGeocodePlaces as _reverseGeocodePlaces,
  fetchPeliasPlaceByGid as _fetchPeliasPlaceByGid,
} from '../services/geocode.service'

export function createGeocodeRoutes(deps = {
  reverseGeocode: _reverseGeocode,
  reverseGeocodePlaces: _reverseGeocodePlaces,
  fetchPeliasPlaceByGid: _fetchPeliasPlaceByGid,
}) {
  return new Elysia()
    .onBeforeHandle(apiAuth('geocode'))
    .onAfterHandle(apiAuthAfter)
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
      '/geocode/reverse',
      async ({ query, request }) => {
        return deps.reverseGeocodePlaces(Number(query.lat), Number(query.lng), {
          limit: query.limit ? Number(query.limit) : undefined,
          radiusM: query.radius ? Number(query.radius) : undefined,
          signal: request.signal,
        })
      },
      {
        query: t.Object({
          lat: t.String({
            description: 'Latitude of the point to reverse geocode (WGS 84).',
            examples: ['35.3079'],
          }),
          lng: t.String({
            description: 'Longitude of the point to reverse geocode (WGS 84).',
            examples: ['-80.7332'],
          }),
          limit: t.Optional(t.String({
            description: 'Maximum number of places to return. Defaults to 5.',
            examples: ['1', '5'],
          })),
          radius: t.Optional(t.String({
            description: 'Search radius in metres around the point. Defaults to 100.',
            examples: ['50', '250'],
          })),
        }),
        detail: {
          summary: 'Reverse geocode a coordinate to places',
          description: `Returns the addressable features at a coordinate — venues, addresses, and streets — in the same result shape as \`/search\`.

Results come from the geocoder (OSM + OpenAddresses) and are hydrated into their full \`geo_places\` rows where one exists, so a hit carries real geometry, tags, and categories rather than a bare point.

When nothing addressable sits within \`radius\`, falls back to the smallest administrative area containing the point, so a click on open water or a field still resolves to something meaningful.

For just the administrative hierarchy (city/county/state/country), use \`GET /geocode\` instead.`,
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
