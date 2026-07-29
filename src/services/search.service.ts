import { db } from '../db'
import { sql } from 'drizzle-orm'
import { searchCache, embeddingCache } from '../lib/cache'
import { generateQueryEmbedding } from '../lib/embeddings'
import { forwardGeocode } from './geocode.service'

// ── Autocomplete fast path ──────────────────────────────────────────────────
// Typeahead fires one request per keystroke, so its budget is ~50ms — an order
// of magnitude tighter than a submitted search. Three properties of the general
// text-search shape blow that budget:
//
//   1. No spatial restriction. Every layer scans the whole geo_places table
//      (21.9M rows / 16GB heap); a one-letter prefix like "d:*" matches 284K
//      lexemes and ~357K rows, measured at 6.6s for the FTS layer alone.
//   2. An ORDER BY built from similarity()/distance arithmetic. No index can
//      serve it, so Postgres materialises *every* match and evaluates
//      similarity() per row before taking the top N.
//   3. The trigram KNN layer, which costs 250ms-1.2s and — unlike FTS — gets no
//      benefit from a spatial filter (measured: still ~800ms inside a 10km box).
//
// So autocomplete restricts the FTS layer to a box around the viewport, orders
// by the index-assisted `centroid <-> point` KNN, over-fetches a candidate pool,
// and skips trigram — leaving the ranking to the JS proximity re-rank further
// down, which already applies the same text_rank × distance-decay formula.
// Measured on the same data: "divin" 596ms → 7ms, "divine barrel" 2014ms → 6ms.
//
// A submitted (non-autocomplete) search is untouched: still global, still
// trigram-backed, still fully ranked in SQL.

/** Shortest query autocomplete will run. A single-character prefix matches a
 *  sizeable fraction of the table (~20s uncached) and its results are noise. */
const AUTOCOMPLETE_MIN_QUERY = 2

/** Half-width of the autocomplete box around the viewport, in metres. Generous
 *  enough to cover a metro area — it only has to bound the scan, since the
 *  proximity re-rank does the actual distance weighting. Widened to the caller's
 *  `radius` when that is larger. */
const AUTOCOMPLETE_RADIUS_M = 50_000

/** Rows pulled from the autocomplete FTS layer before the JS re-rank. Ordering
 *  by pure distance means a strong-but-slightly-further match would be cut by a
 *  tight LIMIT, so over-fetch and let the re-rank pick the winners. */
const AUTOCOMPLETE_POOL = 200

/** Below this many local hits, autocomplete retries on the global (submitted-
 *  search) path so a place outside the viewport is still findable.
 *
 *  This is 1 — i.e. retry only on a genuine zero-result miss — because the
 *  global retry is dominated by the trigram layer at 240-330ms, and anything
 *  higher makes the *precise* queries pay it. Measured local hit counts:
 *  "divine barrel" 1, "sycamore brewing" 1, "walmart independence" 1, against
 *  "times square" 0, "trade and tryon" 0, "1600 e 7th st" 0. A precise query
 *  matching exactly one place is the success case, not a miss; at a threshold
 *  of 5 every one of them triggered a global scan to pad the list with fuzzy
 *  near-misses. Dropping to 1 took the suite median from 157ms to 20ms and
 *  those three queries from ~250ms to ~6ms, while the genuine misses still
 *  retry and still return full results. */
const AUTOCOMPLETE_FALLBACK_MIN = 1

/** Minimum length before the global retry is allowed. Short prefixes match
 *  enormous row counts globally — exactly the case the fast path exists to
 *  avoid — and a 2-3 character prefix is never a deliberate search for a
 *  faraway place. */
const AUTOCOMPLETE_FALLBACK_MIN_QUERY = 4

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

export async function searchPlaces(
  {
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
  }: SearchParams,
  signal?: AbortSignal,
): Promise<any[]> {
  const routeGeoJSON = route ? JSON.stringify(route) : ''
  const tagsCacheKey = tags ? Object.keys(tags).sort().map(k => `${k}=${tags[k]}`).join('&') : ''
  const cacheKey = `search:${query || ''}:${lat}:${lng}:${radius}:${routeGeoJSON}:${buffer}:${categories?.join(',')}:${tagsCacheKey}:${limit}:${offset}:${semantic}:${autocomplete}`
  const cached = searchCache.get(cacheKey)
  if (cached) return cached

  // Strip apostrophes ("sal's" → "sals") to mirror the tsvector normalization,
  // then replace remaining punctuation with spaces.
  const sanitizedQuery = query?.replace(/['’]/g, '').replace(/[^\w\s\-.]/g, ' ').trim() || ''
  const hasQuery = sanitizedQuery.length > 0
  const hasPointLocation = lat != null && lng != null
  const hasRoute = route != null
  const hasCategory = !!(categories && categories.length > 0)
  // "Widen" mode: a category browse with a point but NO radius. Drive the scan
  // from the category GIN index and take the nearest N (bounded to a wide bbox),
  // instead of the KNN walk — which is fast for a dense category but crawls
  // through millions of rows for a sparse one like fuel. This is what lets a
  // zoomed-in search still surface far matches (e.g. gas stations) in ~1s.
  const isWiden = hasPointLocation && !radius && hasCategory
  const WIDEN_BBOX_DEG = 1.35 // ~150 km — caps how far a dense category scans

  // Single-character typeahead: bail before touching Postgres or Pelias.
  if (autocomplete && hasQuery && sanitizedQuery.length < AUTOCOMPLETE_MIN_QUERY) {
    return []
  }

  // The autocomplete fast path needs a viewport to bound the scan; without
  // coordinates it falls back to the ordinary global text-search shape.
  const localAutocomplete = autocomplete && hasPointLocation

  // Address intent — a leading digit ("350 5th ave"). Used twice: to skip the
  // global POI retry (Pelias already answers these, and it returns in <10ms
  // where the retry costs ~250ms) and to decide result ordering further down.
  const addressLike = /^\s*\d/.test(sanitizedQuery)

  // Address geocoding (Pelias) runs in parallel with the PostGIS layers so
  // street addresses appear alongside POIs without adding latency. Text queries
  // only — not browse/category or route corridor searches. Skipped under 3
  // chars: no address is identifiable from 1-2 chars, and such prefixes make
  // Elasticsearch grind through 10k+ candidates for nothing.
  const wantAddresses = hasQuery && sanitizedQuery.length >= 3 && !hasRoute && !(categories && categories.length)
  const peliasPromise: Promise<any[]> = wantAddresses
    ? forwardGeocode(sanitizedQuery, { lat, lng, limit, signal })
    : Promise.resolve([])

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
  } else if (isWiden) {
    // Bound the widen to a wide bbox so a dense category (e.g. 134k parking rows)
    // doesn't sort the whole planet; the category index does the heavy lifting.
    spatialFilter = sql`AND centroid && ST_Expand(${locationPoint}::geometry, ${WIDEN_BBOX_DEG})`
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

  // Autocomplete's bounded variant of the above (see AUTOCOMPLETE_* at the top).
  // Applied ONLY to the FTS layer: the codes and abbreviation layers are exact
  // indexed lookups that already run in single-digit milliseconds, and bounding
  // them would break deliberate long-range lookups like "jfk" typed from
  // Charlotte — which the merge below explicitly pins to the top.
  const autocompleteBoxFilter = localAutocomplete
    ? sql`AND centroid && ST_Expand(${locationPoint}::geometry, ${Math.max(radius ?? 0, AUTOCOMPLETE_RADIUS_M) / 111320})`
    : sql``

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
    ? isWiden
      // The categories GIN index is partial (WHERE categories <> '{}'); state that
      // predicate explicitly so the planner can actually use it for the widen scan.
      ? sql`AND categories && ${categoryArray}::text[] AND categories <> '{}'::text[]`
      : sql`AND categories && ${categoryArray}::text[]`
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
    WHEN categories[1] = 'highway/intersection' THEN 0.7
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

    // `local` runs the autocomplete fast path: viewport box, index-assisted KNN
    // ordering, over-fetched pool. `local: false` is the original global shape.
    // Note the ORDER BY is the only thing that changes about ranking — text_rank
    // is still projected identically, and the JS re-rank below scores on it.
    const ftsQuery = (local: boolean) => db.execute(sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        ${ftsRankExpr} AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE ts @@ ${tsQueryExpr}
      ${local ? autocompleteBoxFilter : textSearchSpatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ${local ? sql`ORDER BY centroid <-> ${locationPoint}` : proximityDecay(ftsRankExpr)}
      LIMIT ${local ? AUTOCOMPLETE_POOL : limit}
    `).catch(() => [] as any[])

    const ftsPromise = ftsQuery(localAutocomplete)

    // Layer 2: Trigram fuzzy match via GiST KNN (name <-> query)
    // Uses the GiST trigram index (geo_places_name_gist_trgm_idx) for ordered
    // retrieval of the N closest matches — avoids the GIN bitmap scan that
    // chokes on short/common trigrams (225K+ candidates).
    // For multi-word queries, coverage scoring boosts results that match more
    // query words across name + parent_context.
    let trigramRankExpr: ReturnType<typeof sql>

    if (queryWords.length > 1) {
      const perWordSims = queryWords.map((w) => sql`similarity(name, ${w})`)
      const bestSim = sql`GREATEST(${sql.join(perWordSims, sql`, `)})`
      const coverageChecks = queryWords.map((w) =>
        sql`CASE WHEN similarity(name, ${w}) > 0.3 OR ts @@ plainto_tsquery('simple', ${w}) THEN 1 ELSE 0 END`)
      const coverageSum = sql`(${sql.join(coverageChecks, sql` + `)})`
      const coverageFactor = sql`(0.3 + 0.7 * ${coverageSum}::float / ${queryWords.length}::float)`
      trigramRankExpr = sql`(${bestSim} * ${coverageFactor} * ${categoryDemotion})`
    } else {
      trigramRankExpr = sql`(similarity(name, ${sanitizedQuery}) * ${categoryDemotion})`
    }

    // Skip trigram for short queries (≤4 chars) — GiST KNN degrades with few
    // trigrams and these are covered by codes/abbreviation/FTS layers.
    //
    // Filter with the `%` similarity operator (pg_trgm.similarity_threshold,
    // default 0.3 ≡ the old `(name <-> q) < 0.7` distance bound) rather than a
    // raw `<-> < threshold` predicate. `%` gives the planner a tight, accurate
    // selectivity estimate so it commits to the GiST index for BOTH the filter
    // and the KNN `ORDER BY`. The `<-> < threshold` form, by contrast, was
    // mis-costed and degraded to a full parallel seq scan (~45s on 21M rows)
    // for low-/no-match queries — exactly the partial words typed mid-search —
    // which blew past the API timeout and returned no place results.
    //
    // The autocomplete fast path skips this layer entirely: it costs 250ms-1.2s
    // and a spatial filter doesn't help it (the GiST KNN walk still dominates —
    // ~800ms even inside a 10km box). Prefix matching, which is what typeahead
    // actually needs, is already covered by the `word:*` tsquery in the FTS
    // layer; trigram's contribution is typo tolerance, which the global retry
    // below restores whenever the local pass comes up short.
    const trigramQuery = () => db.execute(sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        ${trigramRankExpr} AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE name IS NOT NULL
        AND name % ${sanitizedQuery}
      ${textSearchSpatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ORDER BY name <-> ${sanitizedQuery}
      LIMIT ${limit}
    `).catch(() => [] as any[])

    const runTrigram = !localAutocomplete && sanitizedQuery.length > 4
    const trigramPromise = runTrigram ? trigramQuery() : Promise.resolve([] as any[])

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
          -- Surface the real destination, not the road. A code like "jfk" is
          -- carried by both the airport (one big area) and dozens of road
          -- segments tagged ref=JFK (lines). Demote lines and prefer larger
          -- features so "jfk" returns the airport, not "JFK Expressway" ×40.
          ORDER BY (geom_type = 'line') ASC, area_m2 DESC NULLS LAST
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

    let [ftsRows, trigramRows, codesRows, nameAbbrevRows] = await Promise.all([ftsPromise, trigramPromise, codesPromise, nameAbbrevPromise])

    // Autocomplete retry: the local pass only sees the viewport, so a place the
    // user is deliberately reaching for in another city would come back empty.
    // When it finds too little, re-run FTS (and trigram, restoring typo
    // tolerance) on the global path. Gated on query length — a 2-3 character
    // prefix matches enormous row counts globally, and is never a deliberate
    // search for somewhere far away.
    if (
      localAutocomplete &&
      !addressLike &&
      sanitizedQuery.length >= AUTOCOMPLETE_FALLBACK_MIN_QUERY &&
      (ftsRows as any[]).length + (codesRows as any[]).length + (nameAbbrevRows as any[]).length < AUTOCOMPLETE_FALLBACK_MIN
    ) {
      const [globalFts, globalTrigram] = await Promise.all([
        ftsQuery(false),
        sanitizedQuery.length > 4 ? trigramQuery() : Promise.resolve([] as any[]),
      ])
      // Append rather than replace: the global pass is a superset in principle,
      // but it ranks by text_rank and so can drop a nearby hit the local pass
      // found. The dedup in the merge below collapses the overlap.
      ftsRows = [...(ftsRows as any[]), ...(globalFts as any[])]
      trigramRows = globalTrigram
    }

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
    // The autocomplete pool is deliberately wider than `limit` — it is trimmed
    // after the proximity re-rank below, not before, so the re-rank gets to see
    // the whole candidate set. Every other mode is capped here as before.
    results = results.slice(0, localAutocomplete ? AUTOCOMPLETE_POOL : limit)

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
          ? sql`AND id != ALL(ARRAY[${sql.join(existingIds.map((id) => sql`${id}`), sql`, `)}])`
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
    // Order nearest-first via the GiST KNN operator (centroid <-> point) rather
    // than `ORDER BY distance_m` (ST_Distance::geography). The latter computes an
    // exact geodesic distance for EVERY row inside the radius before sorting —
    // fine for a tight viewport, but catastrophic once the radius is widened over
    // a dense category (e.g. ~18s for cafes within 10km of midtown). The KNN
    // operator is index-driven: it walks the centroid index nearest-first and
    // stops at `limit`, so cost scales with the result size, not the radius.
    // `<->` is planar-degree distance (geography KNN can't use the index); the
    // ordering is indistinguishable from geodesic at POI scale, and matches the
    // proximity ranking the text-search layers already use. `distance_m` is still
    // selected (exact geodesic metres) for display/consumers.
    const browseOrder = isWiden
      // Widen: sort the category-index rows by a cheap planar distance (a scalar,
      // so it does NOT force the centroid KNN index — the category GIN drives).
      ? sql`ORDER BY ST_Distance(centroid, ${locationPoint}) ASC`
      : hasPointLocation
        ? sql`ORDER BY centroid <-> ${locationPoint} ASC`
        : sql`ORDER BY distance_m ASC NULLS LAST`
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
      ${browseOrder}
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

  // Trim the autocomplete candidate pool now that the re-rank has scored it.
  // A no-op for every other mode, which was already capped at `limit`.
  if (results.length > limit) results = results.slice(0, limit)

  // ── Fold in address results from Pelias ─────────────────────────────────
  // POIs come from PostGIS above; Pelias supplies street addresses. For an
  // address-intent query ("350 5th ave" — starts with a number) addresses lead;
  // otherwise they're appended so POIs still win. Dedup against POIs at the same
  // spot so an OSM-addressed POI isn't shown twice.
  const addressResults = await peliasPromise
  if (addressResults.length > 0) {
    // Dedup by id — Pelias OSM records carry the same node/way/relation id as
    // barrelman's rows, so a place already returned from PostGIS isn't repeated.
    const seenIds = new Set(results.map((r: any) => r.id))
    const fresh = addressResults.filter((a) => !seenIds.has(a.id))
    results = addressLike ? [...fresh, ...results] : [...results, ...fresh]
    results = results.slice(0, limit)
  }

  // Clean up internal tags before returning
  for (const r of results) {
    delete (r as any)._codesMatch
    delete (r as any)._peliasLayer
  }

  searchCache.set(cacheKey, results)
  return results
}
