import { Elysia, t } from 'elysia'
import { identifyCaller } from '../middleware/api-auth'
import { getNormalizedFeed } from '../services/rt-normalize.service'

export function createGtfsRtRoutes(deps: {
  identifyCaller?: typeof identifyCaller
  getNormalizedFeed?: typeof getNormalizedFeed
} = {}) {
  const identify = deps.identifyCaller ?? identifyCaller
  const normalized = deps.getNormalizedFeed ?? getNormalizedFeed
  return new Elysia({ prefix: '/gtfs-rt' })
    // Upstream feeds can carry the operator's API key; only the instance's own MOTIS may read them.
    .onBeforeHandle(async ({ headers, request, set }) => {
      const { caller, error } = await identify(headers, request)
      if (error) { set.status = error.status; return error.body }
      if (caller.kind === 'account') {
        set.status = 403
        return { error: 'GTFS-RT relay requires the instance service credential' }
      }
    })
    .get('/:feedId/:index', async ({ params, set }) => {
      try {
        const feed = await normalized(params.feedId, params.index)
        if (!feed) {
          set.status = 404
          return { error: 'No normalized realtime feed at that index' }
        }
        return new Response(feed.body as Uint8Array<ArrayBuffer>, {
          headers: {
            'Content-Type': 'application/x-protobuf',
            'Cache-Control': 'no-store',
            ...(feed.stats && { 'X-Normalize-Stats': JSON.stringify(feed.stats) }),
          },
        })
      } catch (err) {
        set.status = 502
        return { error: `Upstream realtime unavailable: ${err instanceof Error ? err.message : err}` }
      }
    }, {
      params: t.Object({ feedId: t.String(), index: t.Numeric({ minimum: 0 }) }),
      detail: {
        tags: ['Transit'],
        summary: "A feed's GTFS-RT, matched to its timetable for MOTIS",
        description:
          'Trip updates restated against the static timetable by stop times, unscheduled trains added as NEW trips, and scheduled runs the feed shows are not running cancelled. Internal: requires the instance service key.',
        responses: { 200: { description: 'GTFS-RT FeedMessage', content: { 'application/x-protobuf': { schema: { type: 'string', format: 'binary' } } } } },
      },
    })
}

export const gtfsRtRoutes = createGtfsRtRoutes()
