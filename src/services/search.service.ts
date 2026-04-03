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
  // Applied for browse/category mode and route mode, NOT for text search queries.
  // Text queries search globally; proximity re-rank at the end biases closer results.
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

  // Text search layers use no spatial restriction — results are globally searched.
  // However, when a location is provided we incorporate distance into the ORDER BY
  // so the DB itself prefers nearby results during the initial fetch (not just
  // post-fetch re-rank). This avoids the problem where common queries like
  // "restaurant" return arbitrary distant results because all matches have similar
  // text_rank. No WHERE filter is applied — any place worldwide can still match.
  const textSearchSpatialFilter = sql``

  // Proximity-aware ORDER BY helper for text search layers.
  // PostgreSQL doesn't allow column aliases in ORDER BY expressions, so each
  // layer must inline its own text_rank expression.  This helper wraps the
  // distance-decay multiplier so each layer can compose its ORDER BY.
  // Uses the cheap `<->` geometry operator (Euclidean in degrees) instead of
  // expensive ST_Distance(::geography) for ranking — precise geodesic distance
  // isn't needed for sort order, just relative proximity.
  // 1 degree ≈ 111 km, so dividing by ~0.45 (≈50km in degrees) gives half-life
  // at ~50 km.  Specific queries surface from anywhere; common queries cluster
  // near the user.
  const proximityDecay = hasPointLocation
    ? (rankExpr: ReturnType<typeof sql>) =>
        sql`ORDER BY (${rankExpr}) / (1.0 + (centroid <-> ${locationPoint}) / 0.45) DESC`
    : (rankExpr: ReturnType<typeof sql>) =>
        sql`ORDER BY (${rankExpr}) DESC`

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

  // ── Category importance factor ──────────────────────────────────────────
  // Demotes low-interest categories (roads, surveillance cameras, etc.) so
  // they only surface when the name is a strong match or very nearby.
  // Applied as a multiplier on text_rank inside each search layer.
  const categoryDemotion = sql`CASE
    WHEN categories[1] LIKE 'highway/%' THEN 0.3
    WHEN categories[1] LIKE 'man_made/surveillance%' THEN 0.2
    ELSE 1.0
  END`

  let results: any[]

  if (hasQuery) {
    // ── Text search mode: 4-layer hybrid pipeline ─────────────────────────

    // Layer 1: Full-text search via tsvector GIN index
    // FTS proves ALL query tokens are present in the tsvector (name + categories +
    // parent_context).  For multi-word queries like "walmart independence", the name
    // is "Walmart Supercenter" and "independence" matches via parent_context (street
    // name).  We rank by how well the name matches the *best* query word, with a
    // 1.5x boost reflecting FTS's higher confidence (all tokens matched) vs trigram
    // (name similarity only).  For generic queries like "restaurant" where name
    // similarity is low across the board, proximity dominates naturally.
    const queryWords = sanitizedQuery.split(/\s+/).filter(Boolean)
    const wordSims = queryWords.map((w) => sql`similarity(name, ${w})`)
    const bestWordSim = wordSims.length > 1
      ? sql`GREATEST(${sql.join(wordSims, sql`, `)})`
      : wordSims[0]
    // For multi-word queries, apply a floor of 0.5 before the 1.5x boost.
    // This ensures FTS results (where ALL tokens matched) rank above trigram
    // results that only match on a location qualifier word in the name
    // (e.g. "Independence Woods" for query "walmart independence").
    const simFloor = queryWords.length > 1 ? sql`0.5` : sql`0`
    const ftsRankExpr = sql`(1.5 * GREATEST(similarity(name, ${sanitizedQuery}), ${bestWordSim}, ${simFloor}) * ${categoryDemotion})`

    // Build tsquery: in autocomplete mode, treat the last word as a prefix
    // so "walmart indep" matches "independence" in the tsvector.
    // In non-autocomplete mode, use exact token matching.
    const tsQueryExpr = autocomplete && queryWords.length > 0
      ? (() => {
          const prefixQuery = queryWords
            .map((w, i) => i === queryWords.length - 1 ? `${w}:*` : w)
            .join(' & ')
          return sql`to_tsquery('simple', unaccent(${prefixQuery}))`
        })()
      : sql`plainto_tsquery('simple', unaccent(${sanitizedQuery}))`

    const ftsPromise = db.execute(sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        ${ftsRankExpr} AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE ts @@ ${tsQueryExpr}
      ${textSearchSpatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ${proximityDecay(ftsRankExpr)}
      LIMIT ${limit}
    `).catch(() => [] as any[])

    // Layer 2: Trigram fuzzy match via GIN (name gin_trgm_ops) index
    // For multi-word queries, match name against individual words so that
    // a misspelled name + location qualifier ("boajngles tryon") still finds
    // results.  Context words (street, neighbourhood) boost ranking via
    // substring match on parent_context.
    let trigramRankExpr: ReturnType<typeof sql>
    let trigramWhereExpr: ReturnType<typeof sql>

    if (queryWords.length > 1) {
      // Match name against any individual query word
      const wordMatchConds = queryWords.map((w) => sql`name % ${w}`)
      trigramWhereExpr = sql`(${sql.join(wordMatchConds, sql` OR `)})`

      // Rank: best per-word name similarity, scaled by word coverage.
      // Coverage = fraction of query words accounted for by name OR context.
      // A result matching 2/2 words (name + context) ranks above one matching 1/2.
      const perWordSims = queryWords.map((w) => sql`similarity(name, ${w})`)
      const bestSim = sql`GREATEST(${sql.join(perWordSims, sql`, `)})`
      const coverageChecks = queryWords.map((w) =>
        sql`CASE WHEN similarity(name, ${w}) > 0.3 OR parent_context ILIKE '%' || ${w} || '%' THEN 1 ELSE 0 END`)
      const coverageSum = sql`(${sql.join(coverageChecks, sql` + `)})`
      const coverageFactor = sql`(0.3 + 0.7 * ${coverageSum}::float / ${queryWords.length}::float)`
      trigramRankExpr = sql`(${bestSim} * ${coverageFactor} * ${categoryDemotion})`
    } else {
      trigramWhereExpr = sql`name % ${sanitizedQuery}`
      trigramRankExpr = sql`(similarity(name, ${sanitizedQuery}) * ${categoryDemotion})`
    }

    const trigramPromise = db.execute(sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        ${trigramRankExpr} AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE ${trigramWhereExpr}
      ${textSearchSpatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ${proximityDecay(trigramRankExpr)}
      LIMIT ${limit}
    `).catch(() => [] as any[])

    // Layer 3: Abbreviation + codes match
    // Split into two separate queries so codes matches (explicit identifiers like
    // IATA/ICAO) always rank above auto-generated abbreviation matches. A codes
    // hit for "AVL" → Asheville Regional Airport is near-certain; an abbreviation
    // hit for "avl" → "Alta Vista Lane" is a heuristic guess. We query both but
    // place codes results first in the merge, guaranteeing they win dedup and
    // appear at the top regardless of distance.
    const lowerQuery = sanitizedQuery.toLowerCase()
    // All abbreviation matches share the same base text_rank, so when a
    // location is provided we simply sort by distance (nearest first).
    // NOTE: PostgreSQL doesn't allow column aliases inside ORDER BY
    // expressions, so we can't use `text_rank / (1 + distance_m/50000)`.
    const abbrevProximityOrder = hasPointLocation
      ? sql`ORDER BY centroid <-> ${locationPoint} ASC`
      : sql`ORDER BY name ASC`

    const codesPromise = sanitizedQuery.length <= 20
      ? db.execute(sql`
          SELECT
            id, osm_type, osm_id, name, name_abbrev, categories, tags,
            address, hours, phones, websites, geom_type,
            ST_AsGeoJSON(centroid)::jsonb AS geometry,
            0.98::float AS text_rank
            ${distanceSelect}
          FROM geo_places
          WHERE codes @> ARRAY[${lowerQuery}]
          ${textSearchSpatialFilter}
          ${categoryFilter}
          ${tagsFilter}
          LIMIT ${limit}
        `).catch(() => [] as any[])
      : Promise.resolve([] as any[])

    const nameAbbrevPromise = sanitizedQuery.length <= 20
      ? db.execute(sql`
          SELECT
            id, osm_type, osm_id, name, name_abbrev, categories, tags,
            address, hours, phones, websites, geom_type,
            ST_AsGeoJSON(centroid)::jsonb AS geometry,
            0.90::float * ${categoryDemotion} AS text_rank
            ${distanceSelect}
          FROM geo_places
          WHERE name_abbrev = ${lowerQuery}
          ${textSearchSpatialFilter}
          ${categoryFilter}
          ${tagsFilter}
          ${abbrevProximityOrder}
          LIMIT ${limit}
        `).catch(() => [] as any[])
      : Promise.resolve([] as any[])

    const [ftsRows, trigramRows, codesRows, nameAbbrevRows] = await Promise.all([ftsPromise, trigramPromise, codesPromise, nameAbbrevPromise])

    // Merge, deduplicating in priority order: codes > abbreviation > FTS > trigram
    // Tag codes results so they're exempt from proximity re-ranking — an exact
    // IATA/ICAO code match is definitive regardless of distance.
    const codesIds = new Set((codesRows as any[]).map((r: any) => r.id))
    const seen = new Set<string>()
    results = []
    for (const row of [...(codesRows as any[]), ...(nameAbbrevRows as any[]), ...(ftsRows as any[]), ...(trigramRows as any[])]) {
      const r = row as any
      if (!seen.has(r.id)) {
        seen.add(r.id)
        if (codesIds.has(r.id)) r._codesMatch = true
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
  // Codes matches (IATA/ICAO) are pinned at the top — they're definitive and
  // should never be displaced by proximity.  Remaining results are re-ranked.
  if (results.length > 1 && (hasRoute || hasPointLocation)) {
    const pinned = results.filter((r: any) => r._codesMatch)
    const rest = results.filter((r: any) => !r._codesMatch)

    if (hasRoute) {
      const decayConstant = buffer / 3
      rest.sort((a: any, b: any) => {
        const scoreA = (a.text_rank || 0) * Math.exp(-(a.distance_m || buffer) / decayConstant)
        const scoreB = (b.text_rank || 0) * Math.exp(-(b.distance_m || buffer) / decayConstant)
        return scoreB - scoreA
      })
    } else if (hasQuery) {
      // 50 km half-life decay — matches the SQL ORDER BY in text search layers.
      rest.sort((a: any, b: any) => {
        const rankA = (a.text_rank || 0) * (1 / (1 + (a.distance_m || 100000) / 50000))
        const rankB = (b.text_rank || 0) * (1 / (1 + (b.distance_m || 100000) / 50000))
        return rankB - rankA
      })
    }

    results = [...pinned, ...rest]
    // Browse mode with point: already sorted by distance_m ASC from the query
  }

  // Clean up internal tags before returning
  for (const r of results) delete (r as any)._codesMatch

  searchCache.set(cacheKey, results)
  return results
}
