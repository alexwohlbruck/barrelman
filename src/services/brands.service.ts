import { db } from '../db'
import { sql } from 'drizzle-orm'
import { brandCache } from '../lib/cache'

/**
 * A brand from the geo_brands catalog — one distinct brand aggregated across all
 * of its OSM locations. Keyed by the brand:wikidata QID when present, otherwise
 * the normalized brand name ("name:<lower>").
 */
export interface Brand {
  brandKey: string
  name: string
  wikidata: string | null
  locationCount: number
  category: string | null
  repLat: number | null
  repLng: number | null
  logoUrl: string | null
  description: string | null
}

function adaptRow(r: any): Brand {
  return {
    brandKey: r.brand_key,
    name: r.name,
    wikidata: r.wikidata ?? null,
    locationCount: Number(r.location_count ?? 0),
    category: r.category ?? null,
    repLat: r.rep_lat != null ? Number(r.rep_lat) : null,
    repLng: r.rep_lng != null ? Number(r.rep_lng) : null,
    logoUrl: r.logo_url ?? null,
    description: r.description ?? null,
  }
}

/**
 * How close a non-prefix match has to be. The trigram `%` operator alone
 * accepts anything above pg_trgm's default 0.3, and one shared word gets there:
 * "power outlet" matched Home Outlet (0.39), Sears Outlet and Grocery Outlet.
 * Real misspellings score higher: "starbuks" 0.58, "whole food" 0.50,
 * "bank of america" 0.48 on similarity, or at least 0.64 on word_similarity,
 * which scores a query that is a close part of a longer name ("home depo" →
 * The Home Depot, 0.90). Measured on the US brand catalog.
 */
const MIN_SIMILARITY = 0.45
const MIN_WORD_SIMILARITY = 0.6

/**
 * Autocomplete over the brand catalog. Prefix matches (ILIKE) rank above fuzzy
 * trigram matches (%), then by popularity (location_count). Returns [] on any
 * error (e.g. the geo_brands matview not yet populated) so search degrades
 * gracefully rather than failing the whole request.
 */
export async function searchBrands(
  { q, limit = 8 }: { q: string; limit?: number },
): Promise<Brand[]> {
  const query = (q ?? '').trim()
  if (query.length < 2) return []

  const cacheKey = `brands:search:${query.toLowerCase()}:${limit}`
  const cached = brandCache.get(cacheKey)
  if (cached) return cached

  try {
    const rows = await db.execute(sql`
      SELECT b.brand_key, b.name, b.wikidata, b.location_count, b.category, b.rep_lat, b.rep_lng,
             l.logo_url, l.description
      FROM geo_brands b
      LEFT JOIN brand_logos l ON l.wikidata = b.wikidata
      WHERE b.name ILIKE (${query} || '%')
         OR (b.name % ${query}
             AND (similarity(b.name, ${query}) >= ${MIN_SIMILARITY}
                  OR word_similarity(${query}, b.name) >= ${MIN_WORD_SIMILARITY}))
      ORDER BY (b.name ILIKE (${query} || '%')) DESC, similarity(b.name, ${query}) DESC, b.location_count DESC
      LIMIT ${limit}
    `)
    const brands = Array.from(rows as any[]).map(adaptRow)
    brandCache.set(cacheKey, brands)
    return brands
  } catch {
    // geo_brands may not exist / be populated yet — don't break search.
    return []
  }
}

/**
 * Fetch a single brand by its brand_key (QID or "name:<lower>"). Used for the
 * brand results header (canonical name + location count).
 */
export async function getBrand(brandKey: string): Promise<Brand | null> {
  const key = (brandKey ?? '').trim()
  if (!key) return null

  const cacheKey = `brands:get:${key}`
  const cached = brandCache.get(cacheKey)
  if (cached !== undefined) return cached

  try {
    const rows = await db.execute(sql`
      SELECT b.brand_key, b.name, b.wikidata, b.location_count, b.category, b.rep_lat, b.rep_lng,
             l.logo_url, l.description
      FROM geo_brands b
      LEFT JOIN brand_logos l ON l.wikidata = b.wikidata
      WHERE b.brand_key = ${key}
      LIMIT 1
    `)
    const list = Array.from(rows as any[])
    const brand = list.length > 0 ? adaptRow(list[0]) : null
    brandCache.set(cacheKey, brand)
    return brand
  } catch {
    return null
  }
}
