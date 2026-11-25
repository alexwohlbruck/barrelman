import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter, callerFor, type ApiAuthOptions } from '../middleware/api-auth'
import {
  deleteProviderMatch,
  getProviderMatches,
  OSM_TYPES,
  setProviderMatch,
  type OsmType,
} from '../services/provider-matches.service'

const defaultDeps = { getProviderMatches, setProviderMatch, deleteProviderMatch }
export type PlaceMatchDeps = typeof defaultDeps

const placeParams = {
  osmType: t.Union(OSM_TYPES.map((type) => t.Literal(type)), {
    description: 'OSM element type.',
  }),
  osmId: t.String({ pattern: '^[1-9][0-9]{0,18}$', description: 'Numeric OSM element ID.', examples: ['6493733562'] }),
}

const providerParam = t.String({
  pattern: '^[a-z][a-z0-9_-]{1,31}$',
  description: 'Provider the listing belongs to, e.g. `google` or `foursquare`.',
  examples: ['foursquare'],
})

const matchSchema = t.Object({
  provider: t.String(),
  providerPlaceId: t.Nullable(t.String()),
  confirmed: t.Boolean(),
  label: t.Nullable(t.String({ description: "The listing's display name, when one is stored." })),
  updatedAt: t.String({ format: 'date-time' }),
})

/**
 * Matches are shared map data that every Parchment user sees, so only the
 * service credential writes them; customer keys can read but never write.
 * With no service key configured the instance is open local development.
 */
function mayWrite(request: Request): boolean {
  const caller = callerFor(request)
  if (caller?.kind === 'service') return true
  return caller?.kind === 'anonymous' && !process.env.BARRELMAN_API_KEY
}

const writeRefused = { error: 'Only the service credential can change matches', code: 'forbidden' }

export function createPlaceMatchRoutes(deps: PlaceMatchDeps = defaultDeps, auth: ApiAuthOptions = {}) {
  return new Elysia()
    .onBeforeHandle(apiAuth('places', auth))
    .onAfterHandle(apiAuthAfter)
    .get(
      '/place/:osmType/:osmId/matches',
      async ({ params }) => ({
        matches: await deps.getProviderMatches({ osmType: params.osmType as OsmType, osmId: params.osmId }),
      }),
      {
        params: t.Object(placeParams),
        response: { 200: t.Object({ matches: t.Array(matchSchema) }) },
        detail: {
          summary: 'Get provider matches for a place',
          description: `The external listings (Google, Foursquare, …) this OSM element has been matched to.

A \`providerPlaceId\` of \`null\` records that the place has no listing at that provider. \`confirmed\` marks a match a person chose; automatic matching leaves those alone. \`label\` is the listing's display name, kept for providers whose ids can't be looked up for free. Matches are keyed by the OSM element and survive every OSM re-import.`,
          tags: ['Places'],
        },
      },
    )
    .put(
      '/place/:osmType/:osmId/matches/:provider',
      async ({ params, body, request, set }) => {
        if (!mayWrite(request)) {
          set.status = 403
          return writeRefused
        }
        return deps.setProviderMatch(
          { osmType: params.osmType as OsmType, osmId: params.osmId },
          params.provider,
          { providerPlaceId: body.providerPlaceId, confirmed: body.confirmed ?? false, label: body.label },
        )
      },
      {
        params: t.Object({ ...placeParams, provider: providerParam }),
        body: t.Object({
          providerPlaceId: t.Nullable(t.String({ minLength: 1, maxLength: 255 })),
          confirmed: t.Optional(t.Boolean({ default: false })),
          label: t.Optional(
            t.Nullable(
              t.String({
                maxLength: 300,
                description:
                  "The listing's display name, for providers whose ids can't be looked up for free (e.g. a Google Hotels property token). Omit to keep the stored name while the listing is unchanged; `null` clears it.",
              }),
            ),
          ),
        }),
        detail: {
          summary: 'Set a provider match for a place',
          description:
            'Records which listing this OSM element is at a provider, or `null` for none. Service credential only.',
          tags: ['Places'],
        },
      },
    )
    .delete(
      '/place/:osmType/:osmId/matches/:provider',
      async ({ params, request, set }) => {
        if (!mayWrite(request)) {
          set.status = 403
          return writeRefused
        }
        const deleted = await deps.deleteProviderMatch(
          { osmType: params.osmType as OsmType, osmId: params.osmId },
          params.provider,
        )
        set.status = deleted ? 204 : 404
        return deleted ? undefined : { error: 'No match for that provider' }
      },
      {
        params: t.Object({ ...placeParams, provider: providerParam }),
        detail: {
          summary: 'Forget a provider match for a place',
          description: 'Removes the match, so the next lookup searches the provider again. Service credential only.',
          tags: ['Places'],
        },
      },
    )
}

export const placeMatchRoutes = createPlaceMatchRoutes()
