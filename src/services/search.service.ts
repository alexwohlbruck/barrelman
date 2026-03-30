import { db } from '../db'
import { sql } from 'drizzle-orm'
import { searchCache, embeddingCache } from '../lib/cache'
import { generateQueryEmbedding } from '../lib/embeddings'

export interface SearchParams {
  query: string
  lat?: number
  lng?: number
  radius?: number
  limit?: number
  semantic?: boolean
  autocomplete?: boolean
}

export async function searchPlaces({
  query,
  lat,
  lng,
  radius,
  limit = 20,
  semantic = false,
  autocomplete = false,
}: SearchParams): Promise<any[]> {
  const cacheKey = `search:${query}:${lat}:${lng}:${radius}:${limit}:${semantic}:${autocomplete}`
  const cached = searchCache.get(cacheKey)
  if (cached) return cached

  const sanitizedQuery = query.replace(/[^\w\s\-'\.]/g, ' ').trim()

  const hasLocation = lat != null && lng != null
  const locationPoint = hasLocation
    ? sql`ST_SetSRID(ST_MakePoint(${lng!}, ${lat!}), 4326)`
    : null

  const distanceSelect = hasLocation
    ? sql`, ST_Distance(centroid::geography, ${locationPoint}::geography) AS distance_m`
    : sql`, NULL::float AS distance_m`

  // Bbox pre-filter + geography distance — hits GIST index before the expensive geography cast
  const degExpand = radius ? radius / 111320 : 0
  const radiusFilter =
    hasLocation && radius
      ? sql`AND centroid && ST_Expand(${locationPoint}::geometry, ${degExpand}) AND ST_DWithin(centroid::geography, ${locationPoint}::geography, ${radius})`
      : sql``

  // ── Run all text layers in parallel ──────────────────────────────────────
  // Layer 1: Full-text search via tsvector GIN index
  const ftsPromise = db.execute(sql`
    SELECT
      id, osm_type, osm_id, name, name_abbrev, categories, tags,
      address, hours, phones, websites, geom_type,
      ST_AsGeoJSON(centroid)::jsonb AS geometry,
      ts_rank(ts, plainto_tsquery('simple', unaccent(${sanitizedQuery}))) AS text_rank
      ${distanceSelect}
    FROM geo_places
    WHERE ts @@ plainto_tsquery('simple', unaccent(${sanitizedQuery}))
    ${radiusFilter}
    ORDER BY text_rank DESC
    LIMIT ${limit}
  `).catch(() => [] as any[])

  // Layer 2: Trigram fuzzy match via GIN (name gin_trgm_ops) index.
  // Using the % operator is required for index usage — similarity() > threshold does a seqscan.
  // The % operator uses pg_trgm.similarity_threshold (default 0.3).
  const trigramPromise = db.execute(sql`
    SELECT
      id, osm_type, osm_id, name, name_abbrev, categories, tags,
      address, hours, phones, websites, geom_type,
      ST_AsGeoJSON(centroid)::jsonb AS geometry,
      similarity(name, ${sanitizedQuery}) AS text_rank
      ${distanceSelect}
    FROM geo_places
    WHERE name % ${sanitizedQuery}
    ${radiusFilter}
    ORDER BY text_rank DESC
    LIMIT ${limit}
  `).catch(() => [] as any[])

  // Layer 3: Abbreviation match (UNCC → UNC Charlotte, etc.)
  // Also run in parallel; no length restriction needed since name_abbrev is short by design.
  const abbrevPromise = sanitizedQuery.length <= 20
    ? db.execute(sql`
        SELECT
          id, osm_type, osm_id, name, name_abbrev, categories, tags,
          address, hours, phones, websites, geom_type,
          ST_AsGeoJSON(centroid)::jsonb AS geometry,
          0.95 AS text_rank
          ${distanceSelect}
        FROM geo_places
        WHERE name_abbrev = ${sanitizedQuery.toLowerCase()}
        ${radiusFilter}
        ORDER BY name ASC
        LIMIT ${limit}
      `).catch(() => [] as any[])
    : Promise.resolve([] as any[])

  const [ftsRows, trigramRows, abbrevRows] = await Promise.all([ftsPromise, trigramPromise, abbrevPromise])

  // ── Merge, deduplicating in priority order: FTS > abbrev > trigram ───────
  const seen = new Set<string>()
  let results: any[] = []

  for (const row of [...(ftsRows as any[]), ...(abbrevRows as any[]), ...(trigramRows as any[])]) {
    const r = row as any
    if (!seen.has(r.id)) {
      seen.add(r.id)
      results.push(r)
    }
  }

  results = results.slice(0, limit)

  // ── Layer 4: Semantic search ──────────────────────────────────────────────
  // Never run for autocomplete (too slow for typing latency).
  // For full search: run when explicitly requested OR results are sparse.
  if (!autocomplete && (semantic || results.length < Math.min(5, limit))) {
    try {
      let queryEmbedding = embeddingCache.get(sanitizedQuery)
      if (!queryEmbedding) {
        queryEmbedding = await generateQueryEmbedding(sanitizedQuery)
        embeddingCache.set(sanitizedQuery, queryEmbedding)
      }

      const embeddingStr = `[${queryEmbedding.join(',')}]`
      const remaining = limit - results.length
      const existingIds = results.map((r: any) => r.id)

      const excludeClause = existingIds.length > 0
        ? sql`AND id NOT IN (${sql.join(existingIds.map((id) => sql`${id}`), sql`, `)})`
        : sql``

      const semanticResults = await db.execute(sql`
        SELECT
          id, osm_type, osm_id, name, name_abbrev, categories, tags,
          address, hours, phones, websites, geom_type,
          ST_AsGeoJSON(centroid)::jsonb AS geometry,
          1 - (embedding <=> ${embeddingStr}::vector) AS text_rank
          ${distanceSelect}
        FROM geo_places
        WHERE embedding IS NOT NULL
        ${excludeClause}
        ${radiusFilter}
        ORDER BY embedding <=> ${embeddingStr}::vector ASC
        LIMIT ${remaining}
      `)
      results = results.concat(semanticResults as any[])
    } catch {
      // Ollama unavailable — skip semantic layer
    }
  }

  // ── Proximity boost re-rank (if location provided) ───────────────────────
  if (hasLocation && results.length > 1) {
    results.sort((a: any, b: any) => {
      const rankA = (a.text_rank || 0) * (1 / (1 + (a.distance_m || 100000) / 10000))
      const rankB = (b.text_rank || 0) * (1 / (1 + (b.distance_m || 100000) / 10000))
      return rankB - rankA
    })
  }

  searchCache.set(cacheKey, results)
  return results
}
