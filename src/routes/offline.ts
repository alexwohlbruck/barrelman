import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import { serverError } from '../lib/errors'
import { clampInt } from '../lib/params'
import { parseBbox } from '../lib/offline/bbox'
import { acceptsGzip } from './tiles'
import {
  buildPlacesPackage as _buildPlacesPackage,
  buildStreetsPackage as _buildStreetsPackage,
  buildTimetablePackage as _buildTimetablePackage,
  MAX_TIMETABLE_DAYS,
} from '../services/offline-packages.service'

const DEFAULT_TIMETABLE_DAYS = 14

const bboxQuery = t.String({
  description: 'Region as `west,south,east,north` in WGS 84. Each side at most 0.5°.',
  examples: ['-74.000,40.715,-73.965,40.745'],
})

/** A package as JSON, gzipped on the way out when the caller accepts it, without blocking on the compression. */
function packageResponse(body: unknown, acceptEncoding: string | undefined): Response {
  const headers: Record<string, string> = { 'content-type': 'application/json', 'cache-control': 'no-store' }
  const json = new Response(JSON.stringify(body)).body!
  if (!acceptsGzip(acceptEncoding)) return new Response(json, { headers })
  headers['content-encoding'] = 'gzip'
  return new Response(json.pipeThrough(new CompressionStream('gzip')), { headers })
}

export function createOfflineRoutes(deps = {
  buildPlacesPackage: _buildPlacesPackage,
  buildStreetsPackage: _buildStreetsPackage,
  buildTimetablePackage: _buildTimetablePackage,
}) {
  const handle = async (
    request: Request,
    set: { status?: number | string },
    rawBbox: string,
    build: (bounds: [number, number, number, number]) => Promise<unknown>,
    what: string,
  ) => {
    const bounds = parseBbox(rawBbox)
    if (typeof bounds === 'string') {
      set.status = 400
      return { error: bounds }
    }
    try {
      return packageResponse(await build(bounds), request.headers.get('accept-encoding') ?? undefined)
    } catch (err) {
      return serverError(request, set, err, `Failed to build the ${what} package`)
    }
  }

  const search = new Elysia()
    .onBeforeHandle(apiAuth('search'))
    .onAfterHandle(apiAuthAfter)
    .get('/search/region', ({ query, request, set }) => handle(request, set, query.bbox, deps.buildPlacesPackage, 'places'), {
      query: t.Object({ bbox: bboxQuery }),
      detail: {
        tags: ['Search'],
        summary: 'Places package for offline search',
        description:
          'Every named place and transit stop in a region, for an app to search and show with no connection. ' +
          'Places carry their OSM id, primary category, centroid, address, opening hours, phone and website; ' +
          'each named street appears once per ~2 km cell, at its longest segment; stops (`stop/<feedId>/<stopId>`) ' +
          'list the routes that call there. Built on request, so a large region takes a few seconds.',
      },
    })

  const transit = new Elysia()
    .onBeforeHandle(apiAuth('transit'))
    .onAfterHandle(apiAuthAfter)
    .get('/transit/timetable', ({ query, request, set }) => {
      const days = clampInt(query.days, { min: 1, max: MAX_TIMETABLE_DAYS, fallback: DEFAULT_TIMETABLE_DAYS })!
      return handle(request, set, query.bbox, bounds => deps.buildTimetablePackage(bounds, days), 'timetable')
    }, {
      query: t.Object({
        bbox: bboxQuery,
        days: t.Optional(t.Numeric({ description: `Days of service from today (default ${DEFAULT_TIMETABLE_DAYS}, max ${MAX_TIMETABLE_DAYS}).` })),
      }),
      detail: {
        tags: ['Transit'],
        summary: 'Timetable package for offline trip planning',
        description:
          'The scheduled service of every feed with stops in a region: stops, routes, stop patterns and trips ' +
          'with their running days and times, clipped to the calls inside the region, plus walking transfers ' +
          'between stops. It starts the day before today so trips running past midnight are included. ' +
          'Schedule only, no realtime. Read from the feeds on request, one build at a time.',
      },
    })

  const routing = new Elysia()
    .onBeforeHandle(apiAuth('routing'))
    .onAfterHandle(apiAuthAfter)
    .get('/route/graph', ({ query, request, set }) => handle(request, set, query.bbox, deps.buildStreetsPackage, 'street graph'), {
      query: t.Object({ bbox: bboxQuery }),
      detail: {
        tags: ['Routing'],
        summary: 'Street graph package for offline walk and bike routing',
        description:
          'The OSM streets and paths of a region as a graph: junction nodes, edges with length, shape, name, ' +
          'and flags for who may use them (walking, cycling, one-way for bikes, steps, bike lanes, car-free, ' +
          'busy roads). Ways that cross without a shared node, such as a bridge over a street, are not joined.',
      },
    })

  return new Elysia().use(search).use(transit).use(routing)
}

export const offlineRoutes = createOfflineRoutes()
