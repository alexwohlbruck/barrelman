/**
 * The locality layer of /search: countries, states, counties, cities, towns,
 * neighbourhoods and postal codes, found by name.
 *
 * These are in geo_places like everything else, but the general text layers
 * cannot surface them:
 *
 *   - A place name is made of common words. "New Jersey" matches ~350K rows
 *     through the main ts index, the ranked FTS query over them runs past the
 *     statement timeout, and the layer returns nothing at all.
 *   - When they do match, the 50 km proximity decay buries a city 800 km away
 *     under every cafe nearby that shares its name.
 *
 * So this layer searches only the ~0.1% of rows that are places (admin
 * boundaries, `place=*`, postal-code boundaries) through a partial index of
 * their own, ranks them by how important a place they are, and decays distance
 * on a scale that grows with that importance. search.service pins its hits to
 * the top of the list, the same way it pins an exact IATA code.
 */

import { db, maintenanceConnection } from '../db'
import { sql } from 'drizzle-orm'
import { buildTsQueryText } from '../lib/search-query'

/**
 * Rows the locality layer searches. Must match the WHERE of
 * geo_places_locality_ts_idx exactly — the planner only uses a partial index
 * when the query restates its predicate. `admin_level` is only ever set for
 * boundary=administrative (see osm2pgsql-flex.lua).
 */
const LOCALITY_PREDICATE = `ts IS NOT NULL AND name IS NOT NULL AND (admin_level IS NOT NULL OR tags->>'place' IS NOT NULL OR tags->>'boundary' = 'postal_code')`

/** Same, for postal-code boundaries looked up by code. Most carry no name
 *  (260 of 265 in Berlin), so they have no tsvector to match. */
const POSTAL_PREDICATE = `tags->>'boundary' = 'postal_code'`

/**
 * How important a place is (0-1) and how far, in km, its pull reaches before
 * proximity halves it. A country is worth surfacing from another continent; a
 * hamlet only from nearby. `place=*` is preferred over admin_level, whose
 * meaning varies by country (8 is a US city, a German Gemeinde, a French
 * commune).
 */
const IMPORTANCE = sql.raw(`CASE
  WHEN tags->>'place' IN ('continent', 'country') OR admin_level = 2 THEN 1.0
  WHEN tags->>'place' IN ('state', 'province', 'region') OR admin_level IN (3, 4) THEN 0.95
  -- A US city's boundary often says so only in border_type (Yonkers is an
  -- admin_level=7), and a city is a city whichever tag names it.
  WHEN tags->>'place' = 'city' OR tags->>'border_type' = 'city' THEN 0.9
  WHEN tags->>'place' IN ('county', 'municipality', 'borough', 'town', 'district')
    OR admin_level BETWEEN 5 AND 8 THEN 0.8
  WHEN tags->>'place' IN ('suburb', 'quarter', 'neighbourhood', 'village')
    OR admin_level BETWEEN 9 AND 11 OR tags->>'boundary' = 'postal_code' THEN 0.7
  WHEN tags->>'place' IN ('hamlet', 'island', 'archipelago') THEN 0.5
  ELSE 0.3
END`)

/**
 * Reach follows the importance a place ends up with, not its own tags: a US
 * city's boundary is a bare admin_level=8 (a town's 300 km) and only its label
 * node says `place=city`. Read from its own tags, Chicago scored 0.19 from
 * Brooklyn and never surfaced.
 *
 * Population stretches a settlement's reach further, at 1 km per 100 people: a city of 100K
 * reaches as far as its class already does, and only bigger ones go beyond.
 * Class alone ranks every "city" alike, so Austin, Arkansas (pop. 1,037) and
 * Austin, Texas (974,447) differed only by distance — and the Texas one was
 * never returned anywhere but Texas.
 */
const reachKm = (importance: ReturnType<typeof sql>, population: ReturnType<typeof sql>) => sql`GREATEST(CASE
  WHEN ${importance} >= 1.0 THEN 5000
  -- Not 5000 like a country: "new york" from Brooklyn is the city, not the
  -- state whose label node sits 313 km upstate.
  WHEN ${importance} >= 0.95 THEN 2000
  WHEN ${importance} >= 0.9 THEN 1000
  WHEN ${importance} >= 0.8 THEN 300
  WHEN ${importance} >= 0.7 THEN 50
  ELSE 20
END, CASE
  -- Settlements only. A state's reach is already wide, and stretched by its
  -- population (New York: 19.6M, so 196,000 km) it outranked the city of the
  -- same name from inside that city.
  WHEN ${importance} >= 0.95 THEN 0
  ELSE COALESCE(${population}, 0) / 100.0
END)`

/** A place's `population` tag, when it is a plain number. Free text ("approx.
 *  5000", "1,234") is left out rather than guessed at. */
const POPULATION = sql.raw(`CASE WHEN tags->>'population' ~ '^[0-9]{1,9}$' THEN (tags->>'population')::bigint END`)

/**
 * A place named exactly what was typed, and at least this populous, is
 * returned when nothing clears MIN_SCORE: typing "boulder" in Charlotte means
 * Boulder, Colorado, even though distance decays it below the threshold. The
 * floor keeps the exemption to places worth crossing a country for — "park"
 * must not pin Park, Kansas (pop. 120).
 */
const EXEMPT_POPULATION = 50_000

/** Most candidates the duplicate fold compares. It is a self-join, so a short
 *  prefix that names thousands of places ("park" matched 5,461) cost 5.4s;
 *  only the best few hundred can ever reach the result. */
const FOLD_CANDIDATES = 200

/** Closest names a qualified pass checks the state of. Enough for every
 *  Springfield in the US (about forty) with room to spare. */
const QUALIFIED_CANDIDATES = 60

/**
 * Lowest score a locality needs to be returned (and so pinned above every
 * other result). Score = name similarity × importance × distance decay.
 * Measured against: "brooklyn" → the borough (0.75); "charlotte" from NYC →
 * the city (0.49); "star" → Star Lake hamlet (0.25, left to the FTS layer so it
 * cannot sit above a Starbucks).
 */
const MIN_SCORE = 0.35

/** Shortest query the layer runs for: "ny" or "be" would pin whatever place
 *  happens to share two trigrams with it. */
export const LOCALITY_MIN_QUERY = 3

/** A query that could be a postal code: a digit, no more than ten characters,
 *  at most one space ("SW1A 1AA"). */
export function isPostalShaped(query: string): boolean {
  return /\d/.test(query) && query.length <= 10 && !/\s.*\s/.test(query)
}

export interface LocalityLayerParams {
  /** Sanitized query text (same form the geo_places layers receive). */
  query: string
  /** The tsquery text from buildTsQueryText, prefix-expanded for typeahead. */
  tsQueryText: string
  lat?: number
  lng?: number
  autocomplete?: boolean
  limit: number
}

let indexReady = false
let fuzzyReady = false
let indexProbedAt = 0

/** The layer's indexes can land after startup: a first import finishes, or
 *  the background build completes. Look again at most once a minute. The
 *  trigram index only enables the misspelling fallback, so the layer runs
 *  without it. */
async function localityIndexReady(): Promise<boolean> {
  if ((indexReady && fuzzyReady) || Date.now() - indexProbedAt < 60_000) return indexReady
  indexProbedAt = Date.now()
  const rows = await db.execute(sql`
    SELECT count(*) FILTER (WHERE i.indisvalid AND c.relname <> 'geo_places_locality_name_trgm_idx') = 2 AS ready,
           bool_or(i.indisvalid AND c.relname = 'geo_places_locality_name_trgm_idx') AS fuzzy
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    WHERE c.relname IN ('geo_places_locality_ts_idx', 'geo_places_postal_code_idx', 'geo_places_locality_name_trgm_idx')
  `).catch(() => null)
  // A failed check changes nothing. Read as "not ready", one timeout under load
  // would switch the whole layer off until the next check a minute later.
  const row = (rows as any[] | null)?.[0]
  if (row) {
    indexReady = Boolean(row.ready)
    fuzzyReady = Boolean(row.fuzzy)
  }
  return indexReady
}

/** Shortest query the misspelling fallback runs for. Below this, trigram
 *  similarity pairs a typo with too many unrelated places. */
const FUZZY_MIN_QUERY = 5

/**
 * Locality hits, best first, in the geo_places result shape.
 *
 * Duplicates are collapsed, because OSM maps most places twice or more: a
 * `place=city` label node inside the city's boundary relation, or a borough
 * and the identically named locality nested inside it (Neukölln). A node folds
 * into the smallest same-name area containing it, which carries the outline.
 * A nested area folds into a larger namesake only of the same importance: the
 * two Neuköllns are one place, but New York City is not New York State. The
 * surviving area keeps the importance of whatever it absorbed: NYC's relation
 * is admin_level 5 (0.8), but its label node is a `place=city` (0.9). Each
 * hit lists what it absorbed in `absorbed_ids`, so the merge can keep the
 * other layers from bringing those duplicates back.
 */
export async function searchLocalities({
  query, tsQueryText, lat, lng, autocomplete = false, limit,
}: LocalityLayerParams): Promise<any[]> {
  // Until the partial index exists, the query would fall back to the full ts
  // index and run into the very timeout this layer exists to avoid.
  if (query.length < LOCALITY_MIN_QUERY || !(await localityIndexReady())) return []

  const params = { query, tsQueryText, lat, lng, autocomplete, limit }
  const wordMatch = sql`ts @@ to_tsquery('simple', unaccent(${tsQueryText}))`
  const [words, ...qualified] = await Promise.all([
    localityQuery(params, wordMatch),
    ...qualifiedPasses(params),
  ])
  // A place named exactly what was typed settles how to read it: "north
  // carolina" is the state, not a town called North in some Carolina.
  const fold = (t: string) => t.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase()
  const exact = words.some((r: any) => r.name && fold(r.name) === fold(query))
  const rows = mergePasses(exact ? [words] : [words, ...qualified], limit)
  if (rows.length > 0 || !fuzzyReady || query.length < FUZZY_MIN_QUERY || isPostalShaped(query)) return rows
  // A place by that name that only scored too low ("hickory" from New York)
  // was spelled right, and a look-alike must not stand in for it.
  if (await placeNamed(wordMatch, query)) return rows
  // No place has that name: try it as a misspelling ("charlote"). The
  // trigram index holds only places (19 MB), so this costs milliseconds,
  // where the same lookup over every name in geo_places reads a 5 GB index.
  return localityQuery({ ...params, postal: false }, sql`name % ${query}`)
}

/**
 * The ways a query can split into a place and the state it is in: "charlotte
 * north carolina" is "charlotte" in "north carolina", or "charlotte north" in
 * "carolina". Up to three trailing words are tried as the state; a place name
 * shorter than three characters is not tried at all.
 */
export function qualifierSplits(query: string): { name: string; qual: string }[] {
  const words = query.split(/\s+/).filter(Boolean)
  const splits: { name: string; qual: string }[] = []
  for (let k = words.length - 1; k >= Math.max(1, words.length - 3); k--) {
    const name = words.slice(0, k).join(' ')
    if (name.length >= LOCALITY_MIN_QUERY) splits.push({ name, qual: words.slice(k).join(' ') })
  }
  return splits
}

/** A name that could be an airport's code standing for its city: "clt". */
const AIRPORT_CODE = /^[a-z]{3,4}$/i

/**
 * The passes that read the end of the query as a state: "charlotte nc",
 * "charlotte nohth caorinlna", "springfield il". One per split, each matching
 * the leading words as a name (by trigram, so a typo there still lands) and
 * scoring the trailing ones against the state each candidate is in.
 *
 * A leading word that is an airport code ("clt nc") also proposes the city
 * the airport is in, since that is how people abbreviate cities.
 *
 * Without the trigram index, the name has to match word for word.
 */
function qualifiedPasses(params: LocalityLayerParams): Promise<any[]>[] {
  if (isPostalShaped(params.query)) return []
  return qualifierSplits(params.query).flatMap(({ name, qual }) => {
    const tsName = buildTsQueryText(name.split(/\s+/), false)
    const passes = [
      localityQuery(
        { ...params, query: name, qualifier: qual, postal: false },
        fuzzyReady ? sql`name % ${name}` : sql`ts @@ to_tsquery('simple', unaccent(${tsName}))`,
      ),
    ]
    if (AIRPORT_CODE.test(name)) {
      passes.push(localityQuery(
        { ...params, query: name, qualifier: qual, postal: false, sim: sql`0.9::real` },
        sql`id IN (
          SELECT c.id FROM geo_places a
          CROSS JOIN LATERAL (
            SELECT id FROM geo_places
            WHERE geom_type = 'area' AND admin_level IS NOT NULL AND admin_level BETWEEN 5 AND 8
              AND ST_Intersects(geom, a.centroid)
            ORDER BY area_m2 LIMIT 1
          ) c
          WHERE a.codes IS NOT NULL AND a.codes @> ARRAY[${name.toLowerCase()}] AND a.tags ? 'iata'
          LIMIT 3
        )`,
      ))
    }
    return passes
  })
}

/**
 * Rows from several passes as one list: best score per place, best first.
 * A place one pass folded into another (Austin's label node, inside its
 * boundary) can come back on its own from a pass that never saw the
 * boundary, so whatever any row absorbed is dropped.
 */
export function mergePasses(passes: any[][], limit: number): any[] {
  const rows = passes.flat()
  const absorbed = new Set(rows.flatMap((r) => r.absorbed_ids ?? []))
  const best = new Map<string, any>()
  for (const row of rows) {
    if (absorbed.has(row.id)) continue
    const seen = best.get(row.id)
    if (!seen || row.text_rank > seen.text_rank) best.set(row.id, row)
  }
  return [...best.values()].sort((a, b) => b.text_rank - a.text_rank).slice(0, limit)
}

/** Whether any place's name matches the words, whatever its score. Read
 *  through the same partial index as the layer itself. */
async function placeNamed(wordMatch: ReturnType<typeof sql>, query: string): Promise<boolean> {
  const rows = await db.execute(sql`
    SELECT 1 FROM geo_places
    WHERE ${sql.raw(LOCALITY_PREDICATE)} AND ${wordMatch}
      AND geom_type <> 'line' AND similarity(name, ${query}) >= 0.3
    LIMIT 1
  `).catch(() => [] as any[])
  return (rows as any[]).length > 0
}

/** One pass of the locality layer, with `match` selecting the candidate rows
 *  (word match, or trigram similarity for a misspelling). */
async function localityQuery(
  {
    query, lat, lng, autocomplete, limit, postal = true, qualifier, sim,
  }: LocalityLayerParams & {
    postal?: boolean
    /** Trailing words naming the state the place is in ("nc"). */
    qualifier?: string
    /** Name similarity, when it isn't the name against the query. */
    sim?: ReturnType<typeof sql>
  },
  match: ReturnType<typeof sql>,
): Promise<any[]> {
  const hasPoint = lat != null && lng != null
  const point = hasPoint ? sql`ST_SetSRID(ST_MakePoint(${lng!}, ${lat!}), 4326)` : null
  const distanceKm = point
    ? sql`ST_Distance(centroid::geography, ${point}::geography) / 1000`
    : sql`0`
  const distanceSelect = point
    ? sql`ST_Distance(centroid::geography, ${point}::geography)`
    : sql`NULL::float`

  // How well the qualifier names the state or country a place is in: by code
  // ("NC") or by name, misspelt or not. "nohth caorinlna" is 0.24 from North
  // Carolina and 0.15 from South Carolina, so it ranks rather than cuts.
  //
  // Looked up only for the closest QUALIFIED_CANDIDATES names: "springfield"
  // has hundreds of trigram look-alikes, and testing each against a state's
  // outline took 0.9 s where the forty actual Springfields take tens of ms.
  const withQualSim = (candidates: ReturnType<typeof sql>) => qualifier
    ? sql`SELECT m.*, (
          SELECT max(GREATEST(similarity(s.name, ${qualifier}),
                              (lower(s.tags->>'ref') = lower(${qualifier}))::int::real))
          FROM geo_places s
          WHERE s.geom_type = 'area' AND s.admin_level IS NOT NULL AND s.admin_level IN (2, 4)
            AND ST_Intersects(s.geom, m.centroid)
        ) AS qual_sim
        FROM (SELECT * FROM (${candidates}) c WHERE sim >= 0.3
              -- Ties go to the bigger place: there are more than sixty exact
              -- Springfields, and the one in Missouri has to make the cut.
              ORDER BY sim DESC, importance DESC, population DESC NULLS LAST
              LIMIT ${QUALIFIED_CANDIDATES}) m`
    : sql`SELECT c.*, NULL::real AS qual_sim FROM (${candidates}) c`
  // A named state stands in for proximity: "springfield il" has said which
  // Springfield, wherever the map is.
  const score = (importance: ReturnType<typeof sql>, population: ReturnType<typeof sql>) => qualifier
    ? sql`sim * ${importance} * (0.4 + 0.6 * qual_sim)`
    : sql`sim * ${importance} / (1 + ${distanceKm} / ${reachKm(importance, population)})`

  const postalCode = query.toUpperCase()
  const postalMatch = autocomplete
    ? sql`upper(tags->>'postal_code') LIKE ${postalCode.replace(/[\\%_]/g, '\\$&') + '%'}`
    : sql`upper(tags->>'postal_code') = ${postalCode}`
  const postalBranch = postal && isPostalShaped(query)
    ? sql`
      UNION ALL
      SELECT id, osm_type, osm_id, COALESCE(name, tags->>'postal_code') AS name, name_abbrev,
             categories, tags, address, hours, phones, websites, geom_type, centroid, geom, area_m2,
             length(${postalCode})::float / greatest(length(tags->>'postal_code'), 1) AS sim,
             ${IMPORTANCE} AS importance, ${POPULATION} AS population
      FROM geo_places
      WHERE ${sql.raw(POSTAL_PREDICATE)} AND ${postalMatch}`
    : sql``

  const rows = await db.execute(sql`
    WITH RECURSIVE hits AS (
      SELECT * FROM (
        SELECT DISTINCT ON (id) * FROM (
          ${withQualSim(sql`
          SELECT id, osm_type, osm_id, name, name_abbrev, categories, tags,
                 address, hours, phones, websites, geom_type, centroid, geom, area_m2,
                 ${sim ?? sql`similarity(name, ${query})`} AS sim,
                 ${IMPORTANCE} AS importance, ${POPULATION} AS population
          FROM geo_places
          WHERE ${sql.raw(LOCALITY_PREDICATE)}
            AND ${match}
            -- A border way tagged with its admin_level is a segment of the line
            -- between two places, not a place: "united states" matched two of
            -- them. Outside the index predicate so the index still applies.
            AND geom_type <> 'line'
          ${postalBranch}
          `)}
        ) matched
        -- ts also matches parent_context, so "new york" reaches every
        -- neighbourhood *in* New York. The name has to be what matched.
        WHERE sim >= 0.3 ${qualifier ? sql`AND qual_sim >= 0.2` : sql``}
        ORDER BY id, sim DESC
      ) distinct_hits
      -- Only the best FOLD_CANDIDATES go on to the fold, ranked by their own
      -- score; the fold can raise a place's importance, never its similarity.
      ORDER BY ${score(sql`importance`, sql`population`)} DESC
      LIMIT ${FOLD_CANDIDATES}
    ),
    -- Which hit each duplicate folds into (see the doc comment above).
    absorbs AS (
      SELECT DISTINCT ON (h.id) h.id AS hit_id, o.id AS by_id, h.importance, h.population
      FROM hits h
      JOIN hits o ON o.id <> h.id AND o.geom_type = 'area'
        AND CASE
          -- A label node of the same name: inside its boundary, or just
          -- outside it — Charlotte's South End node sits 140 m past its own.
          WHEN h.geom_type <> 'area' AND lower(o.name) = lower(h.name)
            THEN ST_DWithin(o.geom, h.centroid, 0.003)
          -- A label node named apart from its boundary: "Mecklenburg" in
          -- "Mecklenburg County", "Yonkers" in "City of Yonkers". Only when
          -- the two are the same kind of place, so a village keeps its own
          -- result beside the Wisconsin "Town of Brooklyn" around it.
          WHEN h.geom_type <> 'area'
            THEN o.importance = h.importance
              AND (left(lower(o.name), length(h.name) + 1) = lower(h.name) || ' '
                OR right(lower(o.name), length(h.name) + 4) = ' of ' || lower(h.name))
              AND ST_Intersects(o.geom, h.centroid)
          -- A nested area of the same name and kind: the two Neuköllns.
          ELSE lower(o.name) = lower(h.name) AND o.area_m2 > h.area_m2
            AND o.importance = h.importance AND ST_Intersects(o.geom, h.centroid)
        END
      -- The namesake before a differently named boundary, then the smallest.
      ORDER BY h.id, lower(o.name) = lower(h.name) DESC, o.area_m2 ASC
    ),
    -- Followed to the outermost: Mitte's label nodes fold into the Ortsteil,
    -- which folds into the Bezirk, which has to end up holding all of them.
    folded (hit_id, root_id, importance, population) AS (
      SELECT hit_id, by_id, importance, population FROM absorbs
      UNION ALL
      SELECT f.hit_id, a.by_id, f.importance, f.population FROM folded f JOIN absorbs a ON a.hit_id = f.root_id
    ),
    merged AS (
      SELECT h.*, a.ids AS absorbed_ids,
        GREATEST(h.importance, a.importance) AS rank_importance,
        -- A city's population is as often on its label node as its boundary.
        GREATEST(h.population, a.population) AS rank_population
      FROM hits h
      LEFT JOIN LATERAL (
        SELECT array_agg(hit_id) AS ids, max(importance) AS importance, max(population) AS population
        FROM folded WHERE root_id = h.id
      ) a ON true
      WHERE h.id NOT IN (SELECT hit_id FROM absorbs)
    ),
    scored AS (
      SELECT id, osm_type, osm_id, name, name_abbrev, categories, tags,
             address, hours, phones, websites, geom_type, centroid, rank_importance,
             ${score(sql`rank_importance`, sql`rank_population`)} AS text_rank,
             ${distanceSelect} AS distance_m,
             absorbed_ids,
             rank_population,
             lower(unaccent(name)) = lower(unaccent(${query})) AS exact
      FROM merged
    ),
    top AS (
      (SELECT * FROM scored WHERE text_rank >= ${MIN_SCORE} ORDER BY text_rank DESC LIMIT ${limit})
      UNION ALL
      -- See EXEMPT_POPULATION: only when nothing else qualified, and only one.
      (SELECT * FROM scored
       WHERE exact AND rank_population >= ${EXEMPT_POPULATION}
         AND NOT EXISTS (SELECT 1 FROM scored WHERE text_rank >= ${MIN_SCORE})
       ORDER BY rank_population DESC LIMIT 1)
    )
    -- The state each place is in, as its address: three Charlottes read as
    -- one place listed three times until they say NC, VA and FL. Looked up
    -- for the returned rows only, through the admin boundary index. The
    -- postal code ("NC") where a country writes its states that way, as
    -- Pelias's own addresses do; the name everywhere else ("Bavaria").
    SELECT t.id, t.osm_type, t.osm_id, t.name, t.name_abbrev, t.categories, t.tags,
           CASE WHEN t.address IS NULL AND st.name IS NOT NULL
             THEN jsonb_build_object('state', st.name) ELSE t.address END AS address,
           t.hours, t.phones, t.websites, t.geom_type,
           ST_AsGeoJSON(t.centroid)::jsonb AS geometry,
           t.text_rank, t.distance_m, t.absorbed_ids
    FROM top t
    LEFT JOIN LATERAL (
      SELECT CASE
          WHEN s.tags->>'ISO3166-2' ~ '^(US|CA|AU)-' AND length(s.tags->>'ref') BETWEEN 2 AND 3
            THEN s.tags->>'ref'
          ELSE s.name
        END AS name
      FROM geo_places s
      WHERE s.geom_type = 'area' AND s.admin_level IS NOT NULL AND s.admin_level = 4
        -- Not for a state or country, which no state contains.
        AND s.id <> t.id AND t.rank_importance < 0.95
        AND ST_Intersects(s.geom, t.centroid)
      ORDER BY s.area_m2 LIMIT 1
    ) st ON true
    ORDER BY t.text_rank DESC
  `).catch(() => [] as any[])
  return Array.from(rows as any[])
}

/**
 * Build the small partial indexes the layer reads, in the background.
 *
 * CONCURRENTLY, because each build still scans the whole table (minutes on a
 * national import) and a plain build would hold off replication writes for
 * that long. That rules out the startup DDL batch — CONCURRENTLY cannot run in
 * a transaction — and leaves a failed build behind as an INVALID index that
 * IF NOT EXISTS would then skip forever, so an invalid one is dropped and
 * rebuilt.
 */
export async function ensureLocalityIndexes(): Promise<void> {
  const client = maintenanceConnection()
  try {
    const indexes = [
      ['geo_places_locality_ts_idx', `USING GIN (ts) WHERE ${LOCALITY_PREDICATE}`],
      ['geo_places_postal_code_idx', `(upper(tags->>'postal_code') text_pattern_ops) WHERE ${POSTAL_PREDICATE}`],
      // The misspelling fallback. Last, since the layer runs without it.
      ['geo_places_locality_name_trgm_idx', `USING GIN (name gin_trgm_ops) WHERE ${LOCALITY_PREDICATE}`],
    ]
    for (const [name, definition] of indexes) {
      const [existing] = await client<{ valid: boolean }[]>`
        SELECT i.indisvalid AS valid FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = ${name}
      `
      if (existing?.valid) continue
      if (existing) await client.unsafe(`DROP INDEX CONCURRENTLY IF EXISTS ${name}`)
      console.log(`[schema] building ${name}`)
      await client.unsafe(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${name} ON geo_places ${definition}`)
    }
    indexReady = true
    fuzzyReady = true
  } catch (err) {
    // Costs the locality layer, nothing else — search still runs without it.
    // Thrown from an un-awaited task it would take the process down instead.
    console.error('[schema] could not build the locality search indexes:', err)
  } finally {
    await client.end({ timeout: 5 })
  }
}

/** Test hook: the layer is a no-op until its index is confirmed. */
export function setLocalityIndexReady(ready: boolean): void {
  indexReady = ready
  fuzzyReady = ready
  indexProbedAt = ready ? 0 : Date.now()
}
