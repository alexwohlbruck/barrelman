import Elysia, { t } from 'elysia'
import { clampInt } from '../lib/params'
import { apiAuth, apiAuthAfter } from '../middleware/api-auth'
import { searchBrands as _searchBrands, getBrand as _getBrand } from '../services/brands.service'
import { getBrandLogoFile as _getBrandLogoFile } from '../services/brand-logo-files.service'
import { createRateLimiter, perIpLimit, sendBytes } from '../lib/static-response'

export function createBrandsRoutes(deps = { searchBrands: _searchBrands, getBrand: _getBrand }) {
  return new Elysia()
    .onBeforeHandle(apiAuth('places'))
    .onAfterHandle(apiAuthAfter)
    .get(
      '/brands',
      async ({ query }) => {
        const brands = await deps.searchBrands({
          q: query.q ?? '',
          limit: clampInt(query.limit, { min: 1, max: 50 }),
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
          description: 'Search the brand catalog (geo_brands) by name. Prefix matches rank above fuzzy matches, then by number of locations.\n\nEach brand carries `logoUrl` (barrelman\'s copy of its Wikidata logo, see `/brand-logos/{qid}`), `logoLicense` (the Commons licence name and URL, whether credit is required, whether the file is public domain or a trademark, and the file page) and `logoAttribution` (a ready credit line). The licence fields are null until Commons has been asked.',
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
          description: 'Fetch a single brand from the catalog (canonical name, wikidata QID, location count, representative location, and the logo fields described under brand autocomplete).',
          tags: ['Brands'],
        },
      },
    )
}

export const brandsRoutes = createBrandsRoutes()

/**
 * A logo is fetched by the browser from an `<img>` tag, which cannot carry a
 * Bearer header, so this route takes no key. It is also free: the caller paid
 * for the /brands or /search response that handed out the URL. The
 * per-address limit keeps it from being a way to page through the catalog.
 */
const LOGO_REQUESTS_PER_MINUTE = 300

/**
 * SVGs can carry script. Served from our origin, one opened directly would run
 * with the API's origin, so the response forbids every fetch and script and
 * allows only inline styles, which some logos use for their colours.
 */
export const LOGO_CSP = "default-src 'none'; style-src 'unsafe-inline'"

export function createBrandLogoRoutes(deps = { getBrandLogoFile: _getBrandLogoFile }) {
  const limiter = createRateLimiter(LOGO_REQUESTS_PER_MINUTE, 60_000)
  return new Elysia()
    .onBeforeHandle(perIpLimit(limiter))
    .get(
      '/brand-logos/:qid',
      async ({ params, headers, set }) => {
        const logo = await deps.getBrandLogoFile(params.qid).catch(() => null)
        set.headers['x-content-type-options'] = 'nosniff'
        set.headers['content-security-policy'] = LOGO_CSP
        if (!logo) {
          set.status = 404
          set.headers['cache-control'] = 'public, max-age=600'
          return { error: 'No logo for this brand' }
        }
        return sendBytes(set, headers, logo)
      },
      {
        params: t.Object({
          qid: t.String({ description: 'The brand\'s Wikidata QID, e.g. `Q38076`.', examples: ['Q38076'] }),
        }),
        query: t.Object({
          v: t.Optional(t.String({ description: 'Version tag from `logoUrl`. Changes when the brand\'s logo file does; ignored by the server.' })),
        }),
        detail: {
          summary: 'Brand logo image',
          description:
            'The brand\'s logo from Wikidata (P154), as stored by barrelman after one fetch from Wikimedia Commons. SVGs are passed through as SVG; a raster logo, or an SVG over 512 KB, is Commons\' 256 px PNG thumbnail. Use the `logoUrl` a brand response gives rather than building this URL, and show `logoAttribution` where the licence asks for credit (see `logoLicense`).\n\n' +
            'No key needed and no credits charged, since an `<img>` tag cannot send one; limited per address. Cached for a month, with an ETag.',
          tags: ['Brands'],
        },
      },
    )
}

export const brandLogoRoutes = createBrandLogoRoutes()
