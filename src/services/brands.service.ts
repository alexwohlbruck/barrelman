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
 * Trigram scores can't tell a misspelt brand from a different word that shares
 * its letters. "charleston" scored Charles Schwab 0.64 on word_similarity, and
 * "columbus" scored Columbia 0.50/0.67, the same range as the real typos
 * "walgren" → Walgreens (0.50/0.75) and "starbuks" → Starbucks (0.58/0.67).
 * So a fuzzy candidate must also be a near-miss of how the brand name *starts*:
 * at most one edit for a query of up to 10 characters, two beyond that.
 * Measured: every typo above is one edit off; "charleston" is three edits from
 * Charles Schwab and two from Charles Tyrwhitt, "columbus" two from Columbia.
 */
const FUZZY_MAX_EDITS_SHORT = 1
const FUZZY_MAX_EDITS_LONG = 2
const FUZZY_SHORT_QUERY = 10

/** Letters and digits only, lowercased, accents and a leading "The" dropped:
 *  "Chick-fil-A" and "chik fil a" compare as "chickfila" and "chikfila". */
function brandKey(s: string): string {
  return s.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase()
    .replace(/^the\s+/, '').replace(/[^\p{L}\p{N}]/gu, '')
}

/** Levenshtein distance, two rows. Inputs here are a few dozen characters. */
function editDistance(a: string, b: string): number {
  let prev = Array.from({ length: b.length + 1 }, (_, j) => j)
  for (let i = 1; i <= a.length; i++) {
    const cur = [i]
    for (let j = 1; j <= b.length; j++) {
      cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1))
    }
    prev = cur
  }
  return prev[b.length]
}

/** Whether `query` is a misspelling of the start of `name` (see above). The
 *  start is taken at every length the allowed edits could reach, since a typo
 *  can add or drop letters ("wallmart", "walgren"). */
export function isNearMissOfStart(query: string, name: string): boolean {
  const q = brandKey(query)
  const n = brandKey(name)
  if (!q || !n) return false
  const k = q.length <= FUZZY_SHORT_QUERY ? FUZZY_MAX_EDITS_SHORT : FUZZY_MAX_EDITS_LONG
  for (let len = Math.max(1, q.length - k); len <= q.length + k; len++) {
    if (editDistance(q, n.slice(0, len)) <= k) return true
  }
  return false
}

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
             l.logo_url, l.description, b.name ILIKE (${query} || '%') AS is_prefix
      FROM geo_brands b
      LEFT JOIN brand_logos l ON l.wikidata = b.wikidata
      WHERE b.name ILIKE (${query} || '%')
         OR (b.name % ${query}
             AND (similarity(b.name, ${query}) >= ${MIN_SIMILARITY}
                  OR word_similarity(${query}, b.name) >= ${MIN_WORD_SIMILARITY}))
      ORDER BY (b.name ILIKE (${query} || '%')) DESC, similarity(b.name, ${query}) DESC, b.location_count DESC
      -- Over-fetched: fuzzy candidates are filtered below, and a rejected one
      -- sorted ahead of a good one must not leave the response short.
      LIMIT ${limit * 3}
    `)
    const brands = Array.from(rows as any[])
      .filter((r: any) => r.is_prefix || isNearMissOfStart(query, r.name))
      .slice(0, limit)
      .map(adaptRow)
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
