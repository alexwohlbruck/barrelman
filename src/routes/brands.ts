import Elysia, { t } from 'elysia'
import { authMiddleware } from '../middleware/auth'
import { searchBrands as _searchBrands, getBrand as _getBrand } from '../services/brands.service'

export function createBrandsRoutes(deps = { searchBrands: _searchBrands, getBrand: _getBrand }) {
  return new Elysia()
    .use(authMiddleware)
    .get(
      '/brands',
      async ({ query }) => {
        const brands = await deps.searchBrands({
          q: query.q ?? '',
          limit: query.limit ? Number(query.limit) : undefined,
        })
        return { brands }
      },
      {
        query: t.Object({
          q: t.String({
            description: 'Brand name prefix / fuzzy query, e.g. "McDon".',
            examples: ['McDonald', 'Starbucks', 'Shell'],
          }),
          limit: t.Optional(t.Union([t.String(), t.Number()], {
            description: 'Maximum number of brand suggestions.',
            examples: [8],
          })),
        }),
        detail: {
          summary: 'Brand autocomplete',
          description: 'Search the brand catalog (geo_brands) by name. Prefix matches rank above fuzzy matches, then by number of locations.',
          tags: ['Brands'],
        },
      },
    )
    .get(
      '/brands/:key',
      async ({ params, status }) => {
        const brand = await deps.getBrand(decodeURIComponent(params.key))
        if (!brand) return status(404, { message: 'Brand not found' })
        return brand
      },
      {
        params: t.Object({
          key: t.String({
            description: 'Brand key: a brand:wikidata QID (e.g. "Q38076") or "name:<lower>".',
          }),
        }),
        detail: {
          summary: 'Get a brand by key',
          description: 'Fetch a single brand from the catalog (canonical name, wikidata QID, location count, representative location).',
          tags: ['Brands'],
        },
      },
    )
}

export const brandsRoutes = createBrandsRoutes()
