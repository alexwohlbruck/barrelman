/**
 * GET /transit/attribution: credit lines for GTFS feeds by id.
 *
 * `/transit/stops` and `/transit/routes` answer with bare arrays, which have
 * nowhere to carry an `attribution` field without breaking every caller. Their
 * rows each name a `feedId`; this resolves those ids to credits. Kept in its
 * own file so routes/transit.ts stays a list of planner and timetable calls.
 */
import Elysia, { t } from 'elysia'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import { attributionFor } from '../services/feed-license.service'
import { feedAttributionSchema } from './transit-schemas'

/** Enough for any one response; a request for more is a catalogue scrape. */
const MAX_FEEDS = 50

export function createTransitAttributionRoutes(deps: { attributionFor?: typeof attributionFor } = {}) {
  const lookup = deps.attributionFor ?? attributionFor
  return new Elysia({ prefix: '/transit' })
    .onBeforeHandle(apiAuth('transit'))
    .onAfterHandle(apiAuthAfter)
    .get(
      '/attribution',
      async ({ query }) => {
        const feedIds = query.feedIds
          .split(',')
          .map((f) => f.trim())
          .filter(Boolean)
          .slice(0, MAX_FEEDS)
        return { attribution: await lookup(feedIds) }
      },
      {
        query: t.Object({
          feedIds: t.String({ description: `Comma-separated feed ids, as returned in \`feedId\`. At most ${MAX_FEEDS}.` }),
        }),
        response: t.Object({ attribution: t.Array(feedAttributionSchema) }),
        detail: {
          summary: 'Credit lines for GTFS feeds',
          description:
            'Resolves feed ids to the credit each feed is owed: the agency name, the attribution text its ' +
            'licence asks for, and the licence URL. `text` and `url` are null where the licence on record ' +
            'publishes none. Use it for `/transit/stops` and `/transit/routes`, whose rows carry `feedId` ' +
            'but no `attribution`; the other transit responses include `attribution` themselves.',
          tags: ['Transit'],
        },
      },
    )
}

export const transitAttributionRoutes = createTransitAttributionRoutes()
