import { db } from '../db'
import { sql } from 'drizzle-orm'
import { searchCache, embeddingCache } from '../lib/cache'
import { generateQueryEmbedding } from '../lib/embeddings'

export interface SearchParams {
  query?: string
  lat?: number
  lng?: number
  radius?: number
  route?: { type: 'LineString'; coordinates: number[][] }
  buffer?: number
  categories?: string[]
  tags?: Record<string, string>
  limit?: number
  offset?: number
  semantic?: boolean
  autocomplete?: boolean
}

export async function searchPlaces({
  query,
  lat,
  lng,
  radius,
  route,
  buffer = 1000,
  categories,
  tags,
  limit = 20,
  offset = 0,
  semantic = false,
  autocomplete = false,
}: SearchParams): Promise<any[]> {
  const routeGeoJSON = route ? JSON.stringify(route) : ''
  const cacheKey = `search:${query || ''}:${lat}:${lng}:${radius}:${routeGeoJSON}:${buffer}:${categories?.join(',')}:${JSON.stringify(tags || {})}:${limit}:${offset}:${semantic}:${autocomplete}`
  const cached = searchCache.get(cacheKey)
  if (cached) return cached

  const sanitizedQuery = query?.replace(/[^\w\s\-'\.]/g, ' ').trim() || ''
  const hasQuery = sanitizedQuery.length > 0
  const hasPointLocation = lat != null && lng != null
  const hasRoute = route != null

  // ── Build spatial primitives ────────────────────────────────────────────
  const locationPoint = hasPointLocation
    ? sql`ST_SetSRID(ST_MakePoint(${lng!}, ${lat!}), 4326)`
    : null
  const routeLine = hasRoute
    ? sql`ST_SetSRID(ST_GeomFromGeoJSON(${routeGeoJSON}), 4326)`
    : null

  // Distance: to route line or to point
  const distanceSelect = hasRoute
    ? sql`, ST_Distance(centroid::geography, ${routeLine}::geography) AS distance_m`
    : hasPointLocation
      ? sql`, ST_Distance(centroid::geography, ${locationPoint}::geography) AS distance_m`
      : sql`, NULL::float AS distance_m`

  // ── Spatial filter ──────────────────────────────────────────────────────
  let spatialFilter: ReturnType<typeof sql>
  if (hasRoute) {
    const degExpand = buffer / 111320
    spatialFilter = sql`AND centroid && ST_Expand(ST_Envelope(${routeLine}::geometry), ${degExpand}) AND ST_DWithin(centroid::geography, ${routeLine}::geography, ${buffer})`
  } else if (hasPointLocation && radius) {
    const degExpand = radius / 111320
    spatialFilter = sql`AND centroid && ST_Expand(${locationPoint}::geometry, ${degExpand}) AND ST_DWithin(centroid::geography, ${locationPoint}::geography, ${radius})`
  } else {
    spatialFilter = sql``
  }

  // ── Category / tag filters ─────────────────────────────────────────────
  const categoryArray = categories && categories.length > 0
    ? `{${categories.join(',')}}` : null
  const categoryFilter = categoryArray
    ? sql`AND categories && ${categoryArray}::text[]`
    : sql``

  const tagsFilterJson = tags && Object.keys(tags).length > 0
    ? JSON.stringify(tags) : null
  const tagsFilter = tagsFilterJson
    ? sql`AND tags @> ${tagsFilterJson}::jsonb`
    : sql``

  let results: any[]

  if (hasQuery) {
    // ── Text search mode: 4-layer hybrid pipeline ─────────────────────────

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
      ${spatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ORDER BY text_rank DESC
      LIMIT ${limit}
    `).catch(() => [] as any[])

    // Layer 2: Trigram fuzzy match via GIN (name gin_trgm_ops) index
    const trigramPromise = db.execute(sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        similarity(name, ${sanitizedQuery}) AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE name % ${sanitizedQuery}
      ${spatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ORDER BY text_rank DESC
      LIMIT ${limit}
    `).catch(() => [] as any[])

    // Layer 3: Abbreviation match
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
          ${spatialFilter}
          ${categoryFilter}
          ${tagsFilter}
          ORDER BY name ASC
          LIMIT ${limit}
        `).catch(() => [] as any[])
      : Promise.resolve([] as any[])

    const [ftsRows, trigramRows, abbrevRows] = await Promise.all([ftsPromise, trigramPromise, abbrevPromise])

    // Merge, deduplicating in priority order: FTS > abbrev > trigram
    const seen = new Set<string>()
    results = []
    for (const row of [...(ftsRows as any[]), ...(abbrevRows as any[]), ...(trigramRows as any[])]) {
      const r = row as any
      if (!seen.has(r.id)) {
        seen.add(r.id)
        results.push(r)
      }
    }
    results = results.slice(0, limit)

    // Layer 4: Semantic search
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
          ${spatialFilter}
          ${categoryFilter}
          ${tagsFilter}
          ORDER BY embedding <=> ${embeddingStr}::vector ASC
          LIMIT ${remaining}
        `)
        results = results.concat(semanticResults as any[])
      } catch {
        // Ollama unavailable — skip semantic layer
      }
    }
  } else {
    // ── Browse mode: spatial + category/tag filter, no text query ──────────
    results = Array.from(await db.execute(sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        1.0 AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE true
      ${spatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ORDER BY distance_m ASC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `) as any[])
  }

  // ── Proximity re-rank ───────────────────────────────────────────────────
  if (results.length > 1 && (hasRoute || hasPointLocation)) {
    if (hasRoute) {
      // Exponential decay for route corridor — strongly biases toward the route
      const decayConstant = buffer / 3
      results.sort((a: any, b: any) => {
        const scoreA = (a.text_rank || 0) * Math.exp(-(a.distance_m || buffer) / decayConstant)
        const scoreB = (b.text_rank || 0) * Math.exp(-(b.distance_m || buffer) / decayConstant)
        return scoreB - scoreA
      })
    } else if (hasQuery) {
      // Linear decay for point proximity — gentler falloff for area search
      results.sort((a: any, b: any) => {
        const rankA = (a.text_rank || 0) * (1 / (1 + (a.distance_m || 100000) / 10000))
        const rankB = (b.text_rank || 0) * (1 / (1 + (b.distance_m || 100000) / 10000))
        return rankB - rankA
      })
    }
    // Browse mode with point: already sorted by distance_m ASC from the query
  }

  searchCache.set(cacheKey, results)
  return results
}
