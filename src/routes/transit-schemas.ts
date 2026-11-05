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

const nullable = (schema: Record<string, unknown>) => ({ ...schema, nullable: true })

const stationEndpoint = {
  type: 'object',
  properties: {
    id: { type: 'string', description: 'OSM id of the platform or entrance.' },
    kind: { type: 'string', enum: ['platform', 'entrance'] },
    name: nullable({ type: 'string' }),
    lat: { type: 'number' },
    lng: { type: 'number' },
    level: nullable({ type: 'number', description: 'OSM `level`; 0 is usually the street.' }),
  },
}

const stationWalk = {
  type: 'object',
  description:
    'A walk inside a station, from the station paths build. `graph` (walked on the mapped station) and `bridged` ' +
    '(a gap of up to 30 m bridged) walks carry `geometry`, `vertexLevels` and `steps`; `estimated`, `agency` and ' +
    '`straight_line` walks are a time between their ends only (`estimate: true`) and should not be drawn or narrated.',
  properties: {
    source: { type: 'string', enum: ['graph', 'bridged', 'agency', 'estimated', 'straight_line'] },
    estimate: { type: 'boolean' },
    seconds: { type: 'integer' },
    uncertaintyS: { type: 'integer', description: 'p90 error of the source, in seconds.' },
    metres: { type: 'integer' },
    levels: { type: 'number', description: 'Levels climbed or descended in all.' },
    from: stationEndpoint,
    to: stationEndpoint,
    geometry: { type: 'object', description: 'GeoJSON LineString.' },
    vertexLevels: { type: 'array', items: nullable({ type: 'number' }), description: 'The level at each vertex of `geometry`.' },
    steps: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          type: { type: 'string', enum: ['walk', 'platform', 'stairs', 'escalator', 'elevator', 'bridge', 'fare_gate', 'enter', 'exit', 'board'] },
          instruction: { type: 'string', description: '"Take the stairs down to the mezzanine", "Follow signs for the 7 · Queens · 40 m".' },
          distance: { type: 'number', description: 'Metres.' },
          duration: { type: 'integer', description: 'Expected seconds.' },
          fromLevel: nullable({ type: 'number' }),
          toLevel: nullable({ type: 'number' }),
          levelChange: nullable({ type: 'number', description: 'Positive up, negative down.' }),
          landmark: nullable({ type: 'string', description: 'What to look for: a line and direction, or an exit\'s name.' }),
          bridged: { type: 'boolean', description: 'Across a gap the map does not connect; no turn-by-turn there.' },
          fromVertex: { type: 'integer' },
          toVertex: { type: 'integer' },
        },
      },
    },
  },
}

const accessChoice = (what: string) => ({
  type: 'object',
  description: `The ${what} that makes the whole walk shortest: the street walk (estimated from the straight line) plus the station path.`,
  properties: {
    id: { type: 'string' },
    name: nullable({ type: 'string' }),
    ref: nullable({ type: 'string' }),
    cornerLabel: nullable({ type: 'string', description: '"Broadway & West 28th Street · NW corner".' }),
    lat: { type: 'number' },
    lng: { type: 'number' },
    streetSeconds: { type: 'integer' },
    stationSeconds: { type: 'integer' },
    totalSeconds: { type: 'integer' },
    walk: stationWalk,
  },
})

const transitLeg = {
  type: 'object',
  description: 'One leg. Transit legs may carry station guidance where the station paths build covers the station.',
  properties: {
    directionLabel: { type: 'string', description: 'The boarding platform\'s signed direction ("Uptown & Queens").' },
    entrance: accessChoice('entrance used to board the first ride'),
    exit: accessChoice('exit used after the last ride'),
    transfer: {
      type: 'object',
      description: 'How the rider gets here from the previous ride.',
      properties: {
        class: {
          type: 'string',
          enum: ['same_platform', 'in_station', 'in_system_passage', 'out_of_system'],
          description:
            '`same_platform`: across the platform. `in_station`: inside fare control, by stairs or corridor. ' +
            '`in_system_passage`: a long paid passage between differently named stations. `out_of_system`: tap out and ' +
            'back in, because the agency does not join the lines inside fare control or the walked path crosses a fare gate.',
        },
        surfaces: nullable({ type: 'boolean', description: 'Goes up to street level; null when only an estimate is known.' }),
        reentryFree: nullable({ type: 'boolean', description: 'For `out_of_system`: whether one fare covers both rides; null without fare data.' }),
        fareGateDelayS: { type: 'integer', description: 'Seconds for tapping out and in, included in `seconds`.' },
        seconds: nullable({ type: 'integer', description: 'Walk plus fare gates. The agency\'s own time for the pair (never its flat default) outranks an estimated walk.' }),
        source: nullable({ type: 'string', enum: ['graph', 'bridged', 'agency', 'estimated', 'straight_line'], description: 'Where `seconds` came from.' }),
        walk: nullable(stationWalk),
      },
    },
    boardingPosition: {
      type: 'object',
      description:
        'Where to stand so the train stops next to the exit or transfer at the alighting station. Only from walked ' +
        'paths with the stairs measured along the platform, and only when the far end costs at least 45 s more.',
      properties: {
        section: { type: 'string', enum: ['front', 'middle', 'back'] },
        fraction: { type: 'number', description: '0 at the back of the train, 1 at the front.' },
        savesSeconds: { type: 'integer' },
        toward: { type: 'string', enum: ['exit', 'transfer'] },
        landmark: nullable({ type: 'string' }),
      },
    },
  },
}

/** `POST /transit/route` and `/transit/intermodal-route`. */
export const itineraryResponses = json('Itineraries, with credits for the feeds they ride.', {
  type: 'object',
  properties: {
    itineraries: {
      type: 'array',
      items: { type: 'object', properties: { legs: { type: 'array', items: transitLeg } } },
    },
    metadata: { type: 'object' },
    attribution: attributionArray,
    warnings: {
      type: 'array',
      items: { type: 'string' },
      description: 'Present only when no itinerary was found: why, in plain sentences. Such a plan is not charged.',
    },
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
