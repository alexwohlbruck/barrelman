import { db } from '../db'
import { sql } from 'drizzle-orm'
import { searchCache, embeddingCache } from '../lib/cache'
import { generateQueryEmbedding } from '../lib/embeddings'
import { forwardGeocode } from './geocode.service'
import { searchTransitRoutes, searchTransitStops } from './transit-search.service'
import { searchLocalities, isPostalShaped } from './locality-search.service'
import { estimateMatches } from './lexeme-stats.service'
import { executeWithin } from '../lib/bounded-query'
import { reconcileTransitHits } from '../lib/transit-search'
import { buildTsQueryText, isStreetQuery } from '../lib/search-query'
import { envNumber } from '../config/env'

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

// ── Local typeahead stages ──────────────────────────────────────────────────
// The autocomplete FTS layer used to be one query: every ts match inside the
// 50 km box, sorted by distance. Whether that is fast depends on how many rows
// the words match *near the viewport*, which the planner cannot know — it
// multiplies word frequencies as if words were independent. Measured on the
// 218M-row US instance: "harris teeter" in Charlotte 7ms, but "new york" in
// Manhattan (every POI's parent_context says New York) 357K rows and 8.5s, and
// "coffee" in Manhattan 2.4s.
//
// Walking the centroid index nearest-first is the mirror image: instant for a
// word that is everywhere nearby, a scan of the whole box for one that is not.
// So the layer measures density before choosing:
//
//   1. Probe the LOCAL_PROBE_ROWS nearest rows and keep those that match. This
//      costs the same 20-70ms whatever the words are, and a dense word fills
//      the pool from it ("new york" 4.8s → 36ms, "charlotte" 478 → 19ms).
//   2. Otherwise the word is locally sparse, which is exactly when the ts
//      index is cheap — unless the word is common nationally ("texas",
//      "street", a one-letter prefix), when no plan is: there the probe's hits
//      stand alone. See lexeme-stats.service.ts.
//   3. The index search runs inside 5 km first and widens to the full box only
//      when that comes up short; "coffee" in Manhattan 2.4s → 73ms.
//
// Each index search runs under its own statement timeout, so a misjudged one
// is cancelled in the server rather than abandoned.

/** Nearest rows the density probe walks. */
const LOCAL_PROBE_ROWS = 2000

/** Statement timeout for the probe. It measures 20-70ms; this only catches a
 *  cold disk or a starved pool. */
const LOCAL_PROBE_TIMEOUT_MS = 300

/** Probe hits that make the word dense enough to skip the index searches. At
 *  1% of nearby rows, a 50 km box holds tens of thousands of matches. */
const LOCAL_PROBE_ENOUGH = 20

/** Rows a word may match nationally before the ts index stops being able to
 *  narrow a local search ("pizza" 366K: 54ms; "texas" 18M: 1.4s even in 5 km). */
const LOCAL_INDEX_MAX_ROWS = 2_000_000

/** Rows a prefix word may expand to. Lower than the above, because the index
 *  materialises a prefix in full and a statement timeout cannot stop it
 *  meanwhile: "coffee:*" (580K) answered in 73ms, "state:*" (2M) ran 2.6s
 *  under a 500ms timeout. See lexeme-stats.service.ts. */
const LOCAL_PREFIX_MAX_ROWS = 1_000_000

/** Whether the ts index can serve a query cheaply. Unknown (no statistics
 *  yet) counts as yes, which is how every search behaved before. */
function indexCanNarrow(tsQueryText: string): boolean {
  const e = estimateMatches(tsQueryText)
  return !e || (e.rows <= LOCAL_INDEX_MAX_ROWS && e.prefixRows <= LOCAL_PREFIX_MAX_ROWS)
}

/** Inner radius of the index search, and the hits that make it enough. */
const LOCAL_NEAR_RADIUS_M = 5_000
const LOCAL_NEAR_ENOUGH = 10

/** Statement timeout for each local index search. */
const LOCAL_INDEX_TIMEOUT_MS = 500

/** Statement timeout for the global FTS retry when typeahead finds nothing
 *  nearby. Rare words answer in ~150ms; past this the word is common and the
 *  locality layer already holds what typeahead can usefully show. */
const AUTOCOMPLETE_RETRY_TIMEOUT_MS = 800

/** Statement timeout for the abbreviation layer in typeahead. An abbreviation
 *  that names few places answers in ~20ms. */
const AUTOCOMPLETE_ABBREV_TIMEOUT_MS = 300

/** Pelias budget for typeahead that doesn't look like an address. Healthy
 *  answers measured 15ms at the median and 67ms at p90. */
const AUTOCOMPLETE_ADDRESS_BUDGET_MS = 400

/** Below this much of the trigram budget left, a submitted search skips the
 *  trigram layer rather than start a scan it cannot finish. */
const TRIGRAM_MIN_REMAINING_MS = 150

/** Transit routes at or above this rank matched on their short name ("7",
 *  "M15") or nearly their whole long name; below it they matched a word in a
 *  long or agency name
 *  — every "Asheville Rides Transit" route for "asheville", every Greyhound
 *  "Raleigh - Asheville". Those rank with the other text matches instead of
 *  above them. */
const STRONG_ROUTE_RANK = 0.9

/** Most locality hits one search returns. Enough for the Springfields; any
 *  more and pinned places crowd out what else the name matches. */
const LOCALITY_LIMIT = 3

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

// How long /search will wait for Pelias address results before returning POIs
// without them. Above Pelias's healthy latency, well below its 10s hang
// backstop. Env-overridable for operators whose Pelias is slower.
const SEARCH_ADDRESS_BUDGET_MS = envNumber('BARRELMAN_SEARCH_ADDRESS_BUDGET_MS', 2500)

// How long /search will wait for the fuzzy trigram layer before answering with
// the precise layers alone.
//
// The trigram KNN scan is fast on a warm index (~360ms measured) and very slow
// on a cold one — it reads ~215 MB of index, so on an instance whose table
// dwarfs RAM it routinely exceeds BARRELMAN_STATEMENT_TIMEOUT_MS and is
// cancelled. Cancelled means the caller waited the entire statement timeout to
// receive *nothing extra*: the layer is a supplement, so its rows are simply
// absent from the merge. Measured on a 229 GB / 16 GB instance, every
// misspelled query cost exactly 10s and returned only FTS hits.
//
// Bounding the wait below the statement timeout turns that into a fast
// degrade: a warm index still contributes typo tolerance, a cold one costs the
// budget instead of the timeout. Set to 0 to wait the full statement timeout.
const SEARCH_TRIGRAM_BUDGET_MS = envNumber('BARRELMAN_SEARCH_TRIGRAM_BUDGET_MS', 2500)

// How long /search waits for the FTS layer once the locality layer has found
// the place. A place name is common words — "New Jersey" matches ~350K rows —
// so FTS ran into the 10s statement timeout on exactly the queries the locality
// layer answers in milliseconds, and the right answer arrived 12s late (18s in
// typeahead, which then retried). Within this budget FTS still adds what else
// the name matches; past it, the place returns without it.
const LOCALITY_FTS_BUDGET_MS = 2500

/** Resolve to [] if `promise` hasn't settled within `ms` (0 waits forever).
 *  The abandoned query is left to its own statement timeout. The timer is
 *  cleared once the race settles, so it never outlives the request. */
function withBudget(promise: Promise<any[]>, ms: number): Promise<any[]> {
  if (ms <= 0) return promise
  let timer: ReturnType<typeof setTimeout> | undefined
  return Promise.race([
    promise,
    new Promise<any[]>((resolve) => {
      timer = setTimeout(() => resolve([]), ms)
    }),
  ]).finally(() => clearTimeout(timer))
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
  const startedAt = performance.now()

  // Strip apostrophes ("sal's" → "sals") to mirror the tsvector normalization,
  // then replace remaining punctuation with spaces. Letters are matched by
  // Unicode class, not \w: \w is ASCII-only, and turned "Neukölln" into
  // "Neuk lln", which no layer could match. Marks are kept too — Devanagari
  // and Thai spell with them — and NFC folds a decomposed "o" + "¨" into "ö".
  const sanitizedQuery = query?.normalize('NFC').replace(/['’]/g, '').replace(/[^\p{L}\p{M}\p{N}\s\-.]/gu, ' ').trim() || ''
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

  // Single-character typeahead: the geo_places layers are off the table (a
  // 1-char prefix matches ~357K rows; measured ~20s) and so is Pelias — but a
  // single character is exactly how riders name a line ("7", "Q", "L"), and
  // an exact short-name match on gtfs_routes costs single-digit milliseconds.
  // Micro-queries return transit lines and nothing else.
  if (autocomplete && hasQuery && sanitizedQuery.length < AUTOCOMPLETE_MIN_QUERY) {
    const lines = !hasCategory && !(tags && Object.keys(tags).length > 0) && !hasRoute
      ? await searchTransitRoutes({
          query: sanitizedQuery,
          lat,
          lng,
          autocomplete: true,
          exactOnly: true,
          limit: Math.min(5, limit),
        })
      : []
    searchCache.set(cacheKey, lines)
    return lines
  }

  // The autocomplete fast path needs a viewport to bound the scan; without
  // coordinates it falls back to the ordinary global text-search shape.
  const localAutocomplete = autocomplete && hasPointLocation

  // Address intent — a leading digit ("350 5th ave"). Used twice: to skip the
  // global POI retry (Pelias already answers these, and it returns in <10ms
  // where the retry costs ~250ms) and to decide result ordering further down.
  const addressLike = /^\s*\d/.test(sanitizedQuery)
  // A street name ("elm street") is answered by Pelias's street layer, so it leads too.
  const streetLike = !addressLike && isStreetQuery(sanitizedQuery.split(/\s+/))

  // Address geocoding (Pelias) runs in parallel with the PostGIS layers so
  // street addresses appear alongside POIs without adding latency. Text queries
  // only — not browse/category or route corridor searches. Skipped under 3
  // chars: no address is identifiable from 1-2 chars, and such prefixes make
  // Elasticsearch grind through 10k+ candidates for nothing.
  const wantAddresses = hasQuery && sanitizedQuery.length >= 3 && !hasRoute && !(categories && categories.length)
  // The raw text: Pelias's parser needs the comma in "3625 Ramos Dr, West Sacramento".
  // A postal-code-shaped query also asks for Pelias's postalcode layer: OSM
  // maps ZIP codes as boundaries almost nowhere in the US, but Who's on First
  // has them all.
  // Aborted when the address budget below runs out, so Elasticsearch stops
  // too instead of finishing a search nobody will read.
  const peliasAbort = new AbortController()
  const peliasSignal = signal ? AbortSignal.any([signal, peliasAbort.signal]) : peliasAbort.signal
  const peliasPromise: Promise<any[]> = wantAddresses
    ? forwardGeocode(query!.trim(), {
        lat, lng, limit, signal: peliasSignal,
        ...(isPostalShaped(sanitizedQuery) ? { layers: 'postalcode,address,street' } : {}),
      })
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
  // The categories GIN index is partial (WHERE categories <> '{}'), and the
  // planner only considers a partial index when the query states its predicate.
  // Without it a radius browse walks every centroid in the circle instead:
  // measured on Berlin, 20 km for power/outlet went from 4.3s to 10ms, and for
  // amenity/cafe from 4.0s to 32ms. Stating it only offers the index; a dense
  // category in a small radius can still take the centroid KNN.
  const categoryFilter = categoryArray
    ? sql`AND categories && ${categoryArray}::text[] AND categories <> '{}'::text[]`
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
  // Rail infrastructure is track, not a destination: a line's every mapped
  // segment carries the line's name ("Hempstead Branch" ×40), and at full
  // weight they bury the one result a rider wants — the GTFS line itself.
  // Stations, halts and entrances are NOT in this list and keep full rank.
  const categoryDemotion = sql`CASE
    WHEN categories[1] = 'highway/intersection' THEN 0.7
    WHEN categories[1] LIKE 'highway/%' THEN 0.3
    WHEN categories[1] IN ('railway/rail', 'railway/subway', 'railway/tram',
      'railway/light_rail', 'railway/monorail', 'railway/narrow_gauge',
      'railway/funicular', 'railway/disused', 'railway/abandoned',
      'railway/construction') THEN 0.3
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

    // Build tsquery: every word expanded to an OR-group of its spellings
    // ("ave" ↔ "avenue", "heights" ↔ "hts", "42" ↔ "42nd" — see
    // lib/search-query.ts), since the 'simple' config can't stem them
    // together and FTS requires every token to match. In autocomplete mode
    // the last word is additionally a prefix, so "walmart indep" matches
    // "independence".
    const tsQueryText = buildTsQueryText(queryWords, autocomplete)
    const tsQueryExpr = tsQueryText
      ? sql`to_tsquery('simple', unaccent(${tsQueryText}))`
      : sql`plainto_tsquery('simple', unaccent(${sanitizedQuery}))`

    // `local` runs the autocomplete fast path: viewport box, index-assisted KNN
    // ordering, over-fetched pool. `local: false` is the original global shape.
    // Note the ORDER BY is the only thing that changes about ranking — text_rank
    // is still projected identically, and the JS re-rank below scores on it.
    const ftsSql = (local: boolean, boxFilter = autocompleteBoxFilter, tsExpr = tsQueryExpr) => sql`
      SELECT
        id, osm_type, osm_id, name, name_abbrev, categories, tags,
        address, hours, phones, websites, geom_type,
        ST_AsGeoJSON(centroid)::jsonb AS geometry,
        ${ftsRankExpr} AS text_rank
        ${distanceSelect}
      FROM geo_places
      WHERE ts @@ ${tsExpr}
      ${local ? boxFilter : textSearchSpatialFilter}
      ${categoryFilter}
      ${tagsFilter}
      ${local ? sql`ORDER BY centroid <-> ${locationPoint}` : proximityDecay(ftsRankExpr)}
      LIMIT ${local ? AUTOCOMPLETE_POOL : limit}
    `
    const ftsQuery = (local: boolean) => db.execute(ftsSql(local)).catch(() => [] as any[])

    // The autocomplete fast path, staged by density (see LOCAL_PROBE_ROWS).
    const localFts = async (): Promise<any[]> => {
      // The probe filters an index-ordered walk, so it must not carry a ts
      // predicate the planner could turn into a bitmap instead.
      const probe = await executeWithin(sql`
        SELECT
          id, osm_type, osm_id, name, name_abbrev, categories, tags,
          address, hours, phones, websites, geom_type,
          ST_AsGeoJSON(centroid)::jsonb AS geometry,
          ${ftsRankExpr} AS text_rank
          ${distanceSelect}
        FROM (
          SELECT * FROM geo_places
          WHERE true ${autocompleteBoxFilter}
          ORDER BY centroid <-> ${locationPoint}
          LIMIT ${LOCAL_PROBE_ROWS}
        ) nearest
        WHERE ts @@ ${tsQueryExpr}
        ${categoryFilter}
        ${tagsFilter}
        LIMIT ${AUTOCOMPLETE_POOL}
      `, LOCAL_PROBE_TIMEOUT_MS)
      if (probe.length >= LOCAL_PROBE_ENOUGH) return probe

      if (!indexCanNarrow(tsQueryText || sanitizedQuery)) return probe

      const near = await executeWithin(
        ftsSql(true, sql`AND centroid && ST_Expand(${locationPoint}::geometry, ${LOCAL_NEAR_RADIUS_M / 111320})`),
        LOCAL_INDEX_TIMEOUT_MS,
      )
      if (near.length >= LOCAL_NEAR_ENOUGH) return [...probe, ...near]
      const wide = await executeWithin(ftsSql(true), LOCAL_INDEX_TIMEOUT_MS)
      // Duplicates across the stages collapse in the merge's id dedupe.
      return [...probe, ...near, ...wide]
    }

    const ftsPromise = localAutocomplete ? localFts() : ftsQuery(false)

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
    const trigramSql = () => sql`
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
    `

    // Trigram is typo tolerance, not a primary source: it ranks below FTS,
    // codes and abbreviations in the merge below, so it only ever contributes
    // rows the precise layers failed to find. It is also by far the most
    // expensive of them — the KNN scan over the GiST trigram index touches
    // ~215 MB of index per call, which is ~360ms when that index is warm and
    // many seconds when it is not. On an instance whose table dwarfs RAM that
    // is the common case, so running it on every search made each cold query
    // burn the full statement timeout and then silently discard the result via
    // the .catch above — a 10s wait for *fewer* results than a 300ms one.
    //
    // So defer it: issue the precise layers first and only reach for trigram
    // when they came back short. A well-spelled query never pays for it.
    // An address-shaped query is answered by Pelias; fuzzy POI names add only the wait.
    const runTrigram = !localAutocomplete && sanitizedQuery.length > 4 && !addressLike

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
            -- A code match on an unnamed feature has nothing to show the user.
            -- The codes column is built from ref-style tags, which unnamed
            -- things carry freely: searching "m15" matched three unnamed camp
            -- pitches and a parking deck tagged ref=M15. Because codes are the
            -- highest priority in the merge below, those four filled the
            -- response and evicted both the named "M15" and the M15 bus route,
            -- so the query returned nothing a user could click. Every other
            -- text layer is already name-bound (trigram requires a name,
            -- abbreviations and FTS derive from one); this was the only way an
            -- unnamed row could reach a result set.
            AND name IS NOT NULL
            -- A chain store's ref is often just its branch name: a Whole
            -- Foods tagged branch=Asheville, ref=asheville pinned itself above
            -- the city of Asheville. A code that is the branch name is a name,
            -- and the FTS layer already finds names.
            AND lower(coalesce(tags->>'branch', '')) <> ${lowerQuery}
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

    // Bounded in typeahead: a two-letter abbreviation is shared by thousands
    // of names ("ha" took 1.2s to order them by distance), and the FTS layer
    // already covers a short prefix.
    const nameAbbrevPromise = sanitizedQuery.length <= 20
      ? executeWithin(sql`
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
        `, autocomplete ? AUTOCOMPLETE_ABBREV_TIMEOUT_MS : 0)
      : Promise.resolve([] as any[])

    // Transit layers: GTFS routes (lines) and GTFS stops OSM doesn't cover.
    // Category/tag filters are geo_places vocabulary and can't be applied to
    // the GTFS tables, so a filtered search stays places-only; a route
    // corridor search is about what's *along* the way, not lines themselves.
    const TRANSIT_LIMIT = Math.min(5, limit)
    const wantTransit = !hasCategory && !tagsFilterJson && !hasRoute
    const transitParams = { query: sanitizedQuery, lat, lng, autocomplete, limit: TRANSIT_LIMIT }
    const transitRoutesPromise = wantTransit
      ? searchTransitRoutes(transitParams)
      : Promise.resolve([] as any[])
    const transitStopsPromise = wantTransit
      ? searchTransitStops({
          ...transitParams,
          localBoxRadiusM: localAutocomplete
            ? Math.max(radius ?? 0, AUTOCOMPLETE_RADIUS_M)
            : undefined,
        })
      : Promise.resolve([] as any[])

    // Locality layer: cities, states, neighbourhoods, postal codes. Global even
    // in autocomplete — its index holds only places, so it is cheap, and a city
    // is exactly what someone reaches for outside the viewport. Strong hits are
    // pinned to the top alongside codes; see locality-search.service.ts.
    const localitiesPromise = !hasCategory && !tagsFilterJson && !hasRoute
      ? searchLocalities({
          query: sanitizedQuery,
          tsQueryText: tsQueryText || sanitizedQuery,
          lat,
          lng,
          autocomplete,
          limit: Math.min(LOCALITY_LIMIT, limit),
        })
      : Promise.resolve([] as any[])

    let [ftsRows, codesRows, nameAbbrevRows, transitRouteRows, transitStopRows, localityRows] =
      await Promise.all([
        // FTS is bounded only once a place has matched; see LOCALITY_FTS_BUDGET_MS.
        localitiesPromise.then((places) =>
          places.length > 0 && !localAutocomplete ? withBudget(ftsPromise, LOCALITY_FTS_BUDGET_MS) : ftsPromise),
        codesPromise, nameAbbrevPromise, transitRoutesPromise, transitStopsPromise, localitiesPromise])
    let trigramRows: any[] = []

    // Autocomplete retry: the local pass only sees the viewport, so a place the
    // user is deliberately reaching for in another city would come back empty.
    // When it finds too little, re-run FTS on the global path. Gated on query
    // length — a 2-3 character prefix matches enormous row counts globally,
    // and is never a deliberate search for somewhere far away.
    //
    // Not trigram. It is the one layer whose cost no filter bounds: a KNN walk
    // over a 5 GB index that ran into the 10s statement timeout on every
    // retry measured ("ocean isle beach" typed anywhere but the NC coast) and
    // then contributed nothing. Typeahead's typo tolerance comes from the
    // locality layer's trigram fallback, over places alone, instead.
    if (
      localAutocomplete &&
      !addressLike &&
      sanitizedQuery.length >= AUTOCOMPLETE_FALLBACK_MIN_QUERY &&
      (ftsRows as any[]).length + (codesRows as any[]).length + (nameAbbrevRows as any[]).length +
        (localityRows as any[]).length < AUTOCOMPLETE_FALLBACK_MIN
    ) {
      // A trailing one- or two-letter word ("ocean isle b") is dropped first:
      // as a prefix it expands to millions of rows nationwide, and the words
      // before it already say where the user is headed. If what remains is
      // still too common for the index, there is no cheap global answer, and
      // the locality layer's places stand alone.
      const retryWords = queryWords.length > 1 && queryWords[queryWords.length - 1].length < 3
        ? queryWords.slice(0, -1)
        : queryWords
      const retryTsText = buildTsQueryText(retryWords, retryWords === queryWords)
      if (retryTsText && indexCanNarrow(retryTsText)) {
        const globalFts = await executeWithin(
          ftsSql(false, undefined, sql`to_tsquery('simple', unaccent(${retryTsText}))`),
          AUTOCOMPLETE_RETRY_TIMEOUT_MS,
        )
        // Append rather than replace: the global pass is a superset in
        // principle, but it ranks by text_rank and so can drop a nearby hit the
        // local pass found. The dedup in the merge below collapses the overlap.
        ftsRows = [...(ftsRows as any[]), ...globalFts]
      }
    }

    // The deferred typo-tolerance pass promised above. The precise layers have
    // answered by now, so we know whether there is anything left to fill: if
    // they already produced `limit` distinct rows, trigram could not add one
    // that survives the cap and the scan would be pure latency.
    if (runTrigram) {
      const precise = new Set<string>()
      for (const row of [
        ...(codesRows as any[]),
        ...(localityRows as any[]),
        ...(transitRouteRows as any[]),
        ...(nameAbbrevRows as any[]),
        ...(ftsRows as any[]),
      ]) {
        precise.add((row as any).id)
      }
      if (precise.size < limit) {
        // Bounded like the Pelias wait below, and for the same reason: a
        // supplementary layer must not be able to hold the whole response
        // hostage. The budget is for the search, not for this layer, so time
        // already spent waiting on FTS counts against it — "new york" used to
        // wait 2.5s for FTS and then 2.5s more here.
        const remaining = SEARCH_TRIGRAM_BUDGET_MS - (performance.now() - startedAt)
        if (SEARCH_TRIGRAM_BUDGET_MS <= 0) {
          trigramRows = await executeWithin(trigramSql(), 0)
        } else if (remaining >= TRIGRAM_MIN_REMAINING_MS) {
          trigramRows = await executeWithin(trigramSql(), remaining)
        }
      }
    }

    // Merge, deduplicating in priority order: exact-name localities > codes >
    // other localities > short-name transit routes > abbreviation > FTS >
    // other transit routes > trigram > transit stops. Transit ids can't
    // collide with OSM ids, so their position only decides who survives the cap.
    // Codes and locality hits are pinned — exempt from proximity re-ranking. An
    // exact IATA/ICAO code is definitive regardless of distance, and a locality
    // hit already carries its own distance decay, scaled to the size of place.
    const localityIds = new Set((localityRows as any[]).map((r: any) => r.id))
    const pinnedIds = new Set([...(codesRows as any[]).map((r: any) => r.id), ...localityIds])
    // A place's label node and nested namesakes, already folded into its
    // locality hit, would otherwise come back through FTS as duplicates.
    const seen = new Set<string>((localityRows as any[]).flatMap((r: any) => r.absorbed_ids ?? []))
    for (const r of localityRows as any[]) delete r.absorbed_ids
    // A place named exactly what was typed leads even the codes: those come
    // from alt_name and short_name too, so "mitte" pinned a pub over the
    // Mitte district. A partial name still yields to an exact code — "bur" is
    // Burbank's airport, not Bury.
    const fold = (t: string) => t.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase()
    const typed = fold(sanitizedQuery)
    const exactLocalities = (localityRows as any[]).filter((r: any) => r.name && fold(r.name) === typed)
    // See STRONG_ROUTE_RANK: a route named for the place typed is not the
    // place, and goes after the places that are.
    const strongRoutes = (transitRouteRows as any[]).filter((r: any) => (r.text_rank ?? 0) >= STRONG_ROUTE_RANK)
    const weakRoutes = (transitRouteRows as any[]).filter((r: any) => (r.text_rank ?? 0) < STRONG_ROUTE_RANK)
    results = []
    for (const row of [...exactLocalities, ...(codesRows as any[]), ...(localityRows as any[]), ...strongRoutes, ...(nameAbbrevRows as any[]), ...(ftsRows as any[]), ...weakRoutes, ...(trigramRows as any[]), ...(transitStopRows as any[])]) {
      const r = row as any
      if (!seen.has(r.id)) {
        seen.add(r.id)
        if (pinnedIds.has(r.id)) r._pinned = true
        if (localityIds.has(r.id)) r._locality = true
        results.push(r)
      }
    }
    // Cross-source dedupe: an OSM route relation duplicated by a GTFS line
    // hit, or a GTFS stop duplicated by an OSM stop, is dropped here — before
    // the cap, so a duplicate never costs a slot.
    if ((transitRouteRows as any[]).length || (transitStopRows as any[]).length) {
      results = reconcileTransitHits(results)
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
  // Codes matches (IATA/ICAO) and localities are pinned at the top — they
  // should never be displaced by proximity.  Remaining results are re-ranked.
  if (results.length > 1 && (hasRoute || hasPointLocation)) {
    const pinned = results.filter((r: any) => r._pinned)
    const rest = results.filter((r: any) => !r._pinned)

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
  // Cap how long the search will wait for addresses. Pelias answers healthy
  // queries in well under a second, but its hang-backstop is 10s (see
  // geocode.service.ts) — and blocking the whole search on a slow-or-down
  // geocoder made a 300ms POI query take 10s. Addresses are supplementary to
  // POIs here, so if Pelias hasn't answered within this budget the POIs return
  // now and the (abandoned) Pelias fetch is cancelled by its own backstop.
  // The timer is cleared once the race settles: left dangling it would keep a
  // live timer per search for the full budget, which at any real query rate is
  // thousands of them outliving the requests that made them.
  //
  // The budget runs from the start of the search, since Pelias was asked in
  // parallel with everything above. Typeahead that doesn't look like an
  // address gets a much shorter one: there Pelias only adds streets that share
  // a word with the query, and for a place name like "charlotte" it took up
  // to 1.5s to find them.
  const wantsAddressFirst = addressLike || streetLike || isPostalShaped(sanitizedQuery)
  const addressBudget = autocomplete && !wantsAddressFirst
    ? Math.min(SEARCH_ADDRESS_BUDGET_MS, AUTOCOMPLETE_ADDRESS_BUDGET_MS)
    : SEARCH_ADDRESS_BUDGET_MS
  let budgetTimer: ReturnType<typeof setTimeout> | undefined
  const addressResults = await Promise.race([
    peliasPromise,
    new Promise<any[]>((resolve) => {
      budgetTimer = setTimeout(() => {
        peliasAbort.abort()
        resolve([])
      }, Math.max(0, addressBudget - (performance.now() - startedAt)))
    }),
  ]).finally(() => clearTimeout(budgetTimer))
  if (addressResults.length > 0) {
    // Dedup by id — Pelias OSM records carry the same node/way/relation id as
    // barrelman's rows, so a place already returned from PostGIS isn't repeated.
    const seenIds = new Set(results.map((r: any) => r.id))
    // A Pelias postal code is the same place as an OSM postal boundary for
    // that code, which carries the outline — keep the OSM one.
    const osmPostcodes = new Set(results.map((r: any) => r.tags?.postal_code).filter(Boolean))
    const fresh = addressResults.filter((a) =>
      !seenIds.has(a.id) && !(a._peliasLayer === 'postalcode' && osmPostcodes.has(a.name)))
    // Localities and postal codes lead in either order: "11211" is a ZIP code
    // before it is a house number.
    const localities = [
      ...results.filter((r: any) => r._locality),
      ...fresh.filter((a) => a._peliasLayer === 'postalcode'),
    ]
    const places = results.filter((r: any) => !r._locality)
    const addresses = fresh.filter((a) => a._peliasLayer !== 'postalcode')
    results = addressLike || streetLike
      ? [...localities, ...addresses, ...places]
      : [...localities, ...places, ...addresses]
    results = results.slice(0, limit)
  }

  // Clean up internal tags before returning
  for (const r of results) {
    delete (r as any)._pinned
    delete (r as any)._locality
    delete (r as any)._peliasLayer
  }

  searchCache.set(cacheKey, results)
  return results
}
