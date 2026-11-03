import Elysia from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import {
  matchTrace as _matchTrace,
  parseGpx,
  parseJsonPoints,
  MatchRequestError,
  GraphHopperTimeoutError,
  DEFAULT_GPS_ACCURACY,
  DEFAULT_MAX_GAP_METERS,
  DEFAULT_MAX_GAP_SECONDS,
  MAX_POINTS,
  type MatchRequest,
} from '../services/match.service'
import { GraphHopperError, type FetchFn } from '../services/route.service'

const isGpx = (contentType: string) => /gpx|xml/i.test(contentType)

function optionalNumber(value: unknown, name: string): number | undefined {
  if (value == null || value === '') return undefined
  const n = Number(value)
  if (!Number.isFinite(n)) throw new MatchRequestError(`${name} must be a number`)
  return n
}

/** snap_prevention as an array or a comma-separated string. */
function stringList(value: unknown): string[] | undefined {
  if (value == null || value === '') return undefined
  const list = Array.isArray(value) ? value : String(value).split(',')
  const cleaned = list.map((s) => String(s).trim()).filter(Boolean)
  return cleaned.length ? cleaned : undefined
}

/**
 * Build the service request from either body form. Options may come from the
 * query string in both forms — GPX has nowhere else to put them — and a JSON
 * body field wins over the same query parameter.
 */
export function parseMatchRequest(
  raw: string,
  contentType: string,
  query: Record<string, string | undefined>,
): MatchRequest {
  let fields: Record<string, any> = {}
  let points
  if (isGpx(contentType)) {
    points = parseGpx(raw)
  } else {
    try {
      fields = JSON.parse(raw || '{}')
    } catch {
      throw new MatchRequestError('Body must be JSON or GPX (send GPX as application/gpx+xml)')
    }
    if (!fields || typeof fields !== 'object' || Array.isArray(fields)) {
      throw new MatchRequestError('JSON body must be an object with a points array')
    }
    points = parseJsonPoints(fields.points)
  }

  const pick = (key: string) => fields[key] ?? query[key]
  return {
    profile: pick('profile'),
    points,
    gpsAccuracy: optionalNumber(pick('gps_accuracy'), 'gps_accuracy'),
    snapPrevention: stringList(pick('snap_prevention')),
    maxGapMeters: optionalNumber(pick('max_gap_meters'), 'max_gap_meters'),
    maxGapSeconds: optionalNumber(pick('max_gap_seconds'), 'max_gap_seconds'),
  }
}

/** GraphHopper's error body, parsed when it is JSON. */
function upstreamBody(body: string) {
  try {
    return JSON.parse(body)
  } catch {
    return { error: body }
  }
}

const DESCRIPTION = `
Snaps a GPS trace to the road network and returns the path it most likely took,
with elevation, distance, per-edge attributes and path details (road_class,
road_environment, street_name, surface and the rest of what \`/route\` returns).

Send JSON — \`{"profile": "bike", "points": [{"lat": 35.2271, "lon": -80.8431, "time": "2026-05-01T10:00:00Z"}]}\` —
or a GPX document as \`application/gpx+xml\` with the options in the query string.
\`time\` is optional: an ISO 8601 string or an epoch number (seconds or milliseconds).

Dense and sparse traces both work. Consecutive points more than \`max_gap_meters\`
(default ${DEFAULT_MAX_GAP_METERS}) or \`max_gap_seconds\` (default ${DEFAULT_MAX_GAP_SECONDS}) apart are not
map-matched — the gap is routed instead, so a handful of photo geotags minutes
apart returns in about a second rather than stalling the matcher. Dense runs are
matched in parallel chunks. A stretch the matcher rejects or can't finish in time
is routed through its own points, and \`segments\` says which parts were matched,
which were routed, and why.

\`gps_accuracy\` (metres, default ${DEFAULT_GPS_ACCURACY}, max 50) is the search radius
around each point. Larger values tolerate noisier GPS but are much slower.
\`snap_prevention\` (e.g. \`bridge,motorway\`) applies to routed stretches: it stops a point
taken under an overpass from snapping onto the road above. At most ${MAX_POINTS} points.

\`snapped_points\` gives each input point's position on the returned path, how
far it moved (\`offset\`) and how far along the path it lies.
`.trim()

/**
 * Map-matching endpoint.
 *
 * POST /match — JSON {profile, points, ...} or a GPX body. See DESCRIPTION.
 *
 * The raw GraphHopper matcher stays reachable at /graphhopper/match; this adds
 * gap handling, chunking, fallbacks and the same enrichment as /route.
 */
export function createMatchRoutes(deps: { matchTrace?: typeof _matchTrace; fetchFn?: FetchFn } = {}) {
  const matchTrace = deps.matchTrace || _matchTrace

  return new Elysia()
    .onBeforeHandle(apiAuth('routing'))
    .onAfterHandle(apiAuthAfter)
    .post('/match', async ({ body, request, query, set }) => {
      let req: MatchRequest
      try {
        req = parseMatchRequest(body as string, request.headers.get('content-type') ?? '', query)
        return await matchTrace(req, deps.fetchFn)
      } catch (err) {
        if (err instanceof MatchRequestError) {
          set.status = 400
          return { error: err.message }
        }
        if (err instanceof GraphHopperTimeoutError) {
          set.status = 504
          return { error: err.message }
        }
        if (err instanceof GraphHopperError) {
          // 4xx is something we passed on that GraphHopper rejected (an
          // unknown profile, a point off the graph); 5xx is the engine failing.
          set.status = err.statusCode >= 500 ? 502 : err.statusCode
          return upstreamBody(err.body)
        }
        set.status = 502
        return {
          error: 'Map matching unavailable',
          detail: err instanceof Error ? err.message : String(err),
        }
      }
    }, {
      // Raw text for both body forms: Elysia has no GPX parser, and the JSON
      // form is decoded in parseMatchRequest so both report errors the same way.
      parse: ({ request }) => request.text(),
      detail: {
        summary: 'Match a GPS trace to the road network',
        description: DESCRIPTION,
        tags: ['Routing'],
        parameters: [
          { name: 'profile', in: 'query', schema: { type: 'string', example: 'bike' }, description: 'Routing profile (bike, car, foot). Required for GPX; may be in the JSON body instead.' },
          { name: 'gps_accuracy', in: 'query', schema: { type: 'number', default: DEFAULT_GPS_ACCURACY }, description: 'Search radius around each point, in metres (1–50).' },
          { name: 'max_gap_meters', in: 'query', schema: { type: 'number', default: DEFAULT_MAX_GAP_METERS }, description: 'Points farther apart than this are routed between, not matched.' },
          { name: 'max_gap_seconds', in: 'query', schema: { type: 'number', default: DEFAULT_MAX_GAP_SECONDS }, description: 'Points further apart in time than this are routed between, not matched.' },
          { name: 'snap_prevention', in: 'query', schema: { type: 'string', example: 'bridge,motorway' }, description: 'Comma-separated road types routed stretches must not snap to.' },
        ],
        requestBody: {
          required: true,
          content: {
            'application/json': {
              schema: {
                type: 'object',
                required: ['points'],
                properties: {
                  profile: { type: 'string', example: 'bike' },
                  points: {
                    type: 'array',
                    items: {
                      type: 'object',
                      required: ['lat', 'lon'],
                      properties: {
                        lat: { type: 'number' },
                        lon: { type: 'number' },
                        time: { oneOf: [{ type: 'string', format: 'date-time' }, { type: 'number' }] },
                      },
                    },
                  },
                  gps_accuracy: { type: 'number', default: DEFAULT_GPS_ACCURACY },
                  max_gap_meters: { type: 'number', default: DEFAULT_MAX_GAP_METERS },
                  max_gap_seconds: { type: 'number', default: DEFAULT_MAX_GAP_SECONDS },
                  snap_prevention: { type: 'array', items: { type: 'string' }, example: ['bridge', 'motorway'] },
                },
              },
              example: {
                profile: 'bike',
                points: [
                  { lat: 35.2271, lon: -80.8431, time: '2026-05-01T10:00:00Z' },
                  { lat: 35.2085, lon: -80.8120, time: '2026-05-01T10:20:00Z' },
                  { lat: 35.2150, lon: -80.7800, time: '2026-05-01T10:40:00Z' },
                ],
              },
            },
            'application/gpx+xml': { schema: { type: 'string' } },
          },
        },
      },
    })
}

export const matchRoutes = createMatchRoutes()
