import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { findChildren as _findChildren } from '../services/spatial.service'

export function createChildrenRoutes(deps = { findChildren: _findChildren }) {
  return new Elysia()
    .use(authMiddleware)
    .get(
      '/children',
      async ({ query }) => {
        const { id, categories, limit, offset, lat, lng } = query
        return deps.findChildren({ id, categories, limit, offset, lat, lng })
      },
      {
        query: t.Object({
          id: t.String({
            description: 'Barrelman place ID of the parent area (e.g. `way/123456`). Must be an area geometry (`geom_type = \'area\'`).',
            examples: ['way/123456', 'relation/9876'],
          }),
          categories: t.Optional(t.String({
            description: 'Comma-separated list of OSM preset category IDs to filter unnamed children by. Named places inside the area are always included regardless of this filter.',
            examples: ['cafe,restaurant', 'bicycle_parking'],
          })),
          limit: t.Optional(t.String({
            description: 'Maximum number of children to return.',
            examples: ['20', '50'],
          })),
          offset: t.Optional(t.String({
            description: 'Number of children to skip for pagination.',
            examples: ['0', '20'],
          })),
          lat: t.Optional(t.String({
            description: 'Latitude for proximity sorting. When provided, results closer to this point are ranked first. Falls back to parent centroid when omitted.',
            examples: ['35.2271'],
          })),
          lng: t.Optional(t.String({
            description: 'Longitude for proximity sorting. When provided, results closer to this point are ranked first. Falls back to parent centroid when omitted.',
            examples: ['-80.8431'],
          })),
        }),
        detail: {
          summary: 'Find children of an area',
          description: `Returns places whose centroids fall within the geometry of the given parent area (e.g. all shops inside a mall, all POIs inside a university campus).

**Sorting priority:**
1. Named places first (unnamed amenities ranked lower)
2. Places with recognized categories before unclassified features
3. Proximity to \`lat\`/\`lng\` if provided, otherwise proximity to the parent area's centroid
4. Alphabetical by name as a tiebreaker

**Category filtering** applies only to *unnamed* children — named places inside the area are always returned, allowing landmark buildings and venues to appear even when filtering for a specific type.

\`building:part\` features are always excluded to avoid surfacing architectural sub-elements.`,
          tags: ['Search'],
        },
      },
    )
}

export const childrenRoutes = createChildrenRoutes()
