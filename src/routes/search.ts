import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { searchPlaces as _searchPlaces } from '../services/search.service'

export function createSearchRoutes(deps = { searchPlaces: _searchPlaces }) {
  return new Elysia()
    .use(authMiddleware)
    .post(
      '/search',
      async ({ body }) => {
        const { query, lat, lng, radius, limit, semantic, autocomplete } = body
        return deps.searchPlaces({ query, lat, lng, radius, limit, semantic, autocomplete })
      },
      {
        body: t.Object({
          query: t.String({
            minLength: 1,
            description: 'Search query string. Special characters are stripped; accents are normalized.',
            examples: ['coffee', 'Starbucks', 'UNCC'],
          }),
          lat: t.Optional(t.Number({
            description: 'Latitude for proximity boosting and optional radius filtering (WGS 84).',
            examples: [35.2271],
          })),
          lng: t.Optional(t.Number({
            description: 'Longitude for proximity boosting and optional radius filtering (WGS 84).',
            examples: [-80.8431],
          })),
          radius: t.Optional(t.Number({
            description: 'When combined with lat/lng, restricts results to places within this many meters. Omit for city-wide or global search.',
            examples: [5000, 25000],
          })),
          limit: t.Optional(t.Number({
            default: 20,
            description: 'Maximum number of results to return.',
            examples: [10, 20, 50],
          })),
          semantic: t.Optional(t.Boolean({
            default: false,
            description: 'Force semantic (vector) search even when text layers return sufficient results. Useful for concept queries like "somewhere quiet to study". Requires Ollama to be running.',
          })),
          autocomplete: t.Optional(t.Boolean({
            default: false,
            description: 'When true, skips the slow semantic layer entirely for low-latency typeahead. Use false for final search submissions.',
          })),
        }),
        detail: {
          summary: 'Search places by text',
          description: `Hybrid multi-layer search combining four strategies, run in order of speed:

1. **Full-text search (FTS)** — PostgreSQL \`tsvector\` GIN index; handles word stemming and accent normalization.
2. **Trigram fuzzy match** — \`pg_trgm\` GIN index; catches typos and partial matches (e.g. "cofee" → "coffee").
3. **Abbreviation match** — Exact match against pre-computed \`name_abbrev\` column (e.g. "UNCC" → "UNC Charlotte").
4. **Semantic search** — Vector cosine similarity via \`pgvector\`; used as a fallback when text layers return few results, or when \`semantic=true\`.

Results are deduplicated in priority order (FTS > abbreviation > trigram > semantic) then re-ranked with a proximity decay function when coordinates are provided.

The semantic layer requires [Ollama](https://ollama.com) running with the \`nomic-embed-text\` model. If Ollama is unavailable the layer is silently skipped.`,
          tags: ['Search'],
        },
      },
    )
}

export const searchRoutes = createSearchRoutes()
