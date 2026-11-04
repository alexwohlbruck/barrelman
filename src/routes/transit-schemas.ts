/**
 * OpenAPI response documentation for the transit endpoints' `attribution`.
 *
 * The transit handlers declare no `response` schema, because Elysia would then
 * validate (and strip) every itinerary and board against it, and those shapes
 * are large and still moving. These go in `detail.responses` instead, which
 * documents without validating, and describe only the parts a caller needs to
 * find the credits.
 */
import { t } from 'elysia'

export const feedAttributionSchema = t.Object(
  {
    feedId: t.String({ description: 'The feed id, as in `feedId` elsewhere in the response.' }),
    agency: t.Nullable(t.String({ description: 'The feed\'s name as Transitland lists it, usually the agency.' })),
    text: t.Nullable(t.String({ description: 'The attribution text the feed\'s licence asks for, when it publishes one.' })),
    url: t.Nullable(t.String({ description: 'The feed\'s licence URL, when it publishes one.' })),
  },
  { description: 'Credit owed to one GTFS feed. Show the agency (and `text`, where set) beside the results it contributed to.' },
)

const attributionArray = {
  type: 'array',
  description: 'One entry per GTFS feed that contributed, each listed once.',
  items: feedAttributionSchema,
}

const json = (description: string, schema: object) => ({
  '200': { description, content: { 'application/json': { schema } } },
})

/** `POST /transit/route` and `/transit/intermodal-route`. */
export const itineraryResponses = json('Itineraries, with credits for the feeds they ride.', {
  type: 'object',
  properties: {
    itineraries: { type: 'array', items: { type: 'object' } },
    metadata: { type: 'object' },
    attribution: attributionArray,
  },
})

/** `GET /transit/departures`: credits are per board. */
export const departureResponses = json('One board per stop, each crediting the feeds on it.', {
  type: 'array',
  items: {
    type: 'object',
    properties: {
      stop: { type: 'object' },
      departures: { type: 'array', items: { type: 'object' } },
      attribution: attributionArray,
    },
  },
})

/** `GET /transit/route-detail`. */
export const routeDetailResponses = json('The route, crediting its feed.', {
  type: 'object',
  properties: {
    feedId: { type: 'string' },
    routeId: { type: 'string' },
    stops: { type: 'array', items: { type: 'object' } },
    attribution: attributionArray,
  },
})

/** `GET /transit/stops` and `/transit/routes`: bare arrays, credited by feed id. */
export const feedRowResponses = (what: string) =>
  json(
    `${what}. Each row carries \`feedId\`; resolve the credits with \`GET /transit/attribution?feedIds=…\`.`,
    { type: 'array', items: { type: 'object', properties: { feedId: { type: 'string' } } } },
  )
