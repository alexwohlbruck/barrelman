/**
 * Boundary catalog — the "what regions exist?" index.
 *
 * Defining an import region used to mean hand-assembling a Geofabrik PBF URL, a
 * replication URL, a bounding box, a GTFS bbox, a TIGER FIPS code and a list of
 * OpenAddresses files. That is a lot of lookup for "I want Colorado", and every
 * field is silently wrong-able.
 *
 * The chicken-and-egg problem is that barrelman's own boundaries live in
 * `geo_places`, which does not exist until an import has already run. So the
 * catalog comes from outside: Geofabrik publishes `index-v1.json`, a GeoJSON
 * FeatureCollection describing every extract it offers (555 of them — countries,
 * their subregions, and all 53 US state-level extracts) with the real
 * MultiPolygon boundary, the download URLs, and ISO 3166 codes.
 *
 * That file is the perfect seed: it is the set of regions that are actually
 * importable (you cannot import a region without a PBF to import), it carries
 * true political geometry rather than a hand-typed rectangle, and it is a single
 * unauthenticated HTTP GET with no API key. We cache it in Postgres so resolving
 * a region afterwards is offline and instant.
 *
 * Refresh it with `bun run scripts/fetch-boundaries.ts` (or the "Fetch Boundary
 * Catalog" task in the admin console) before defining regions by name.
 */
import { connection as sql } from '../db'
import { US_STATES_BY_ISO, isoToStateSlug } from '../config/us-states'
import type { Bbox } from '../config/regions'
import type { RegionInput } from './region-store.service'

export const GEOFABRIK_INDEX_URL = 'https://download.geofabrik.de/index-v1.json'

/** OpenAddresses publishes one JSON source descriptor per dataset in this repo. */
const OA_CONTENTS_API =
  'https://api.github.com/repos/openaddresses/openaddresses/contents/sources'

export interface Boundary {
  /** Geofabrik extract id, e.g. "us/colorado" or "germany". */
  id: string
  /** Display name — Geofabrik's own, or a prettified id for the US extracts. */
  label: string
  /** Geofabrik's raw `name` property. */
  name: string
  /** Parent extract id ("north-america"), or null for continents. */
  parent: string | null
  /** ISO 3166-1 alpha-2 codes (countries), e.g. ["DE"]. */
  iso3166_1: string[]
  /** ISO 3166-2 subdivision codes, e.g. ["US-CO"]. */
  iso3166_2: string[]
  pbfUrl: string
  updatesUrl: string | null
  bbox: Bbox
}

// ── Schema ───────────────────────────────────────────────────────────────────

let schemaReady: Promise<void> | null = null

export function ensureBoundarySchema(): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS boundary_catalog (
          id          text PRIMARY KEY,
          label       text NOT NULL,
          name        text NOT NULL,
          parent      text,
          iso3166_1   text[] NOT NULL DEFAULT '{}',
          iso3166_2   text[] NOT NULL DEFAULT '{}',
          pbf_url     text NOT NULL,
          updates_url text,
          bbox        jsonb NOT NULL,
          -- The real political boundary. Kept (rather than just the bbox) so the
          -- catalog can answer "which region contains this point?" and so a
          -- future clipped-extract step has a polygon to clip against.
          geom        geometry(MultiPolygon, 4326),
          fetched_at  timestamptz NOT NULL DEFAULT now()
        )`
      await sql`
        CREATE INDEX IF NOT EXISTS boundary_catalog_geom_idx
          ON boundary_catalog USING GIST (geom)`
      await sql`
        CREATE INDEX IF NOT EXISTS boundary_catalog_label_idx
          ON boundary_catalog (lower(label))`
    })()
  }
  return schemaReady
}

// ── Ingest ───────────────────────────────────────────────────────────────────

/**
 * Geofabrik names its US extracts by path ("us/colorado") rather than with a
 * display name, unlike every other entry ("Germany", "Baden-Württemberg"). Only
 * those 53 need prettifying, so key off the slash rather than reformatting names
 * that are already correct — "Brandenburg (mit Berlin)" must survive untouched.
 */
function displayLabel(id: string, name: string): string {
  if (!name.includes('/')) return name
  const leaf = id.split('/').pop() ?? id
  return leaf
    .split('-')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ')
}

interface GeofabrikFeature {
  properties: {
    id: string
    name: string
    parent?: string
    'iso3166-1:alpha2'?: string[]
    'iso3166-2'?: string[]
    urls: Record<string, string>
  }
  geometry: unknown
}

export interface RefreshResult {
  count: number
  source: string
}

/**
 * Download the Geofabrik index and replace the cached catalog.
 *
 * Replace-in-a-transaction rather than upsert: extracts do occasionally get
 * renamed or retired upstream, and a stale row would keep offering a PBF URL
 * that now 404s at import time.
 */
export async function refreshBoundaryCatalog(
  fetchFn: typeof fetch = globalThis.fetch,
): Promise<RefreshResult> {
  await ensureBoundarySchema()

  const res = await fetchFn(GEOFABRIK_INDEX_URL)
  if (!res.ok) {
    throw new Error(`Geofabrik index returned ${res.status} ${res.statusText}`)
  }
  const index = (await res.json()) as { features?: GeofabrikFeature[] }
  const features = index.features ?? []
  if (!features.length) {
    throw new Error('Geofabrik index contained no features — refusing to wipe the catalog')
  }

  await sql.begin(async (rawTx) => {
    // postgres.js v3 types `TransactionSql` without its tagged-template call
    // signature, so the handle has to be re-typed to be usable as one.
    const tx = rawTx as unknown as typeof sql
    await tx`DELETE FROM boundary_catalog`
    for (const f of features) {
      const p = f.properties
      const pbf = p.urls?.pbf
      if (!pbf) continue // continents have geometry but nothing to download

      const updates = p.urls?.updates
      await tx`
        INSERT INTO boundary_catalog
          (id, label, name, parent, iso3166_1, iso3166_2, pbf_url, updates_url, geom, bbox)
        SELECT
          ${p.id},
          ${displayLabel(p.id, p.name)},
          ${p.name},
          ${p.parent ?? null},
          ${p['iso3166-1:alpha2'] ?? []},
          ${p['iso3166-2'] ?? []},
          ${pbf},
          ${updates ? (updates.endsWith('/') ? updates : `${updates}/`) : null},
          g.geom,
          jsonb_build_array(
            ST_XMin(g.geom), ST_YMin(g.geom), ST_XMax(g.geom), ST_YMax(g.geom)
          )
        FROM (
          SELECT ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(${JSON.stringify(f.geometry)}), 4326)) AS geom
        ) g`
    }
  })

  const [{ count }] = await sql<{ count: number }[]>`
    SELECT count(*)::int AS count FROM boundary_catalog`
  return { count, source: GEOFABRIK_INDEX_URL }
}

// ── Query ────────────────────────────────────────────────────────────────────

/* eslint-disable-next-line @typescript-eslint/no-explicit-any */
function rowToBoundary(r: any): Boundary {
  return {
    id: r.id,
    label: r.label,
    name: r.name,
    parent: r.parent,
    iso3166_1: r.iso3166_1 ?? [],
    iso3166_2: r.iso3166_2 ?? [],
    pbfUrl: r.pbf_url,
    updatesUrl: r.updates_url,
    bbox: r.bbox as Bbox,
  }
}

export async function countBoundaries(): Promise<number> {
  await ensureBoundarySchema()
  const [{ count }] = await sql<{ count: number }[]>`
    SELECT count(*)::int AS count FROM boundary_catalog`
  return count
}

/** Newest `fetched_at` in the catalog, or null when it has never been fetched. */
export async function catalogFetchedAt(): Promise<string | null> {
  await ensureBoundarySchema()
  const [row] = await sql<{ at: string | null }[]>`
    SELECT max(fetched_at)::text AS at FROM boundary_catalog`
  return row?.at ?? null
}

/**
 * Name search over the catalog. Exact and prefix matches rank above substring
 * hits, then shorter ids win so "Georgia" surfaces the country before
 * "us/georgia", and a typed ISO code ("US-CO", "DE") matches directly.
 */
export async function searchBoundaries(query: string, limit = 20): Promise<Boundary[]> {
  await ensureBoundarySchema()
  const q = query.trim()
  if (!q) return []
  const like = `%${q}%`
  const prefix = `${q}%`
  const rows = await sql`
    SELECT * FROM boundary_catalog
    WHERE label ILIKE ${like}
       OR id ILIKE ${like}
       OR upper(${q}) = ANY(iso3166_1)
       OR upper(${q}) = ANY(iso3166_2)
    ORDER BY
      (lower(label) = lower(${q})) DESC,
      (upper(${q}) = ANY(iso3166_1) OR upper(${q}) = ANY(iso3166_2)) DESC,
      (label ILIKE ${prefix}) DESC,
      length(id),
      label
    LIMIT ${limit}`
  return rows.map(rowToBoundary)
}

export async function getBoundary(id: string): Promise<Boundary | null> {
  await ensureBoundarySchema()
  const [row] = await sql`SELECT * FROM boundary_catalog WHERE id = ${id}`
  return row ? rowToBoundary(row) : null
}

// ── OpenAddresses discovery ──────────────────────────────────────────────────

export interface OaLookup {
  files: string[]
  /** Set when the lookup could not be completed; the region is still usable. */
  warning?: string
}

/**
 * List the OpenAddresses datasets covering a boundary.
 *
 * Pelias's `imports.openaddresses.files` wants CSV paths like
 * "us/co/denver.csv". Those map 1:1 onto source descriptors in the
 * openaddresses repo (`sources/us/co/denver.json`), so listing the directory
 * gives the exact file set — which matters because coverage is wildly uneven:
 * New York has a single `statewide` source, Colorado has 55 county-level ones
 * and no statewide file at all. Guessing "<state>/statewide.csv" silently yields
 * zero addresses for most states.
 *
 * Best-effort by design. GitHub's unauthenticated API allows 60 requests/hour,
 * and this is one request per region definition, but a failure here must not
 * block defining the region — it just leaves the address layer to be filled in
 * by hand. Set GITHUB_TOKEN to raise the limit.
 */
export async function lookupOpenAddresses(
  boundary: Boundary,
  fetchFn: typeof fetch = globalThis.fetch,
): Promise<OaLookup> {
  const stateIso = boundary.iso3166_2.find((c) => c.startsWith('US-'))
  const stateSlug = stateIso ? isoToStateSlug(stateIso) : null

  let dir: string | null = null
  if (stateSlug) dir = `us/${stateSlug}`
  else if (boundary.iso3166_1.length === 1) dir = boundary.iso3166_1[0].toLowerCase()

  if (!dir) {
    return {
      files: [],
      warning:
        'No ISO country/state code on this extract, so OpenAddresses coverage could not be looked up automatically. Add address files by hand if you need address search.',
    }
  }

  const headers: Record<string, string> = {
    Accept: 'application/vnd.github+json',
    'User-Agent': 'barrelman-boundary-catalog',
  }
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`

  try {
    const res = await fetchFn(`${OA_CONTENTS_API}/${dir}`, { headers })
    if (res.status === 404) {
      return { files: [], warning: `OpenAddresses has no "${dir}" directory — no address data for this region.` }
    }
    if (res.status === 403) {
      return {
        files: [],
        warning:
          'GitHub API rate limit reached while listing OpenAddresses sources. Set GITHUB_TOKEN and re-resolve, or fill in address files by hand.',
      }
    }
    if (!res.ok) {
      return { files: [], warning: `OpenAddresses lookup failed (HTTP ${res.status}).` }
    }

    const entries = (await res.json()) as Array<{ name: string; type: string }>
    const files = entries
      .filter((e) => e.type === 'file' && e.name.endsWith('.json'))
      .map((e) => `${dir}/${e.name.replace(/\.json$/, '.csv')}`)
      .sort()

    if (!files.length) {
      return { files: [], warning: `No OpenAddresses sources found under "${dir}".` }
    }
    return { files }
  } catch (err) {
    return {
      files: [],
      warning: `OpenAddresses lookup failed: ${err instanceof Error ? err.message : String(err)}`,
    }
  }
}

// ── Region derivation ────────────────────────────────────────────────────────

export interface DerivedRegion {
  region: RegionInput
  /** Human-readable notes about what was and wasn't auto-filled. */
  warnings: string[]
  /** Per-field provenance, shown in the console so nothing looks like magic. */
  sources: Record<string, string>
}

/**
 * "us/colorado" → "colorado"; "baden-wuerttemberg" stays as-is.
 *
 * Region keys go into the comma-separated REGIONS env var and are validated
 * against /^[a-z0-9][a-z0-9-]*$/ by the region API, so normalise to that shape.
 * The leaf rather than the full path keeps `REGIONS=colorado` readable; the
 * console lets an operator edit it before saving if it collides.
 */
export function suggestRegionKey(id: string): string {
  return (id.split('/').pop() ?? id)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/^-+|-+$/g, '')
}

/**
 * Turn a catalog entry into a ready-to-save region definition, filling every
 * field the import pipeline needs from the boundary's own metadata.
 */
export async function deriveRegion(
  boundary: Boundary,
  fetchFn: typeof fetch = globalThis.fetch,
): Promise<DerivedRegion> {
  const warnings: string[] = []
  const sources: Record<string, string> = {
    osmExtracts: 'Geofabrik index',
    bbox: 'Geofabrik boundary geometry',
    gtfsRegion: 'Derived from the boundary bbox',
  }

  const oa = await lookupOpenAddresses(boundary, fetchFn)
  if (oa.warning) warnings.push(oa.warning)
  if (oa.files.length) sources.openaddresses = 'OpenAddresses source listing'

  const tigerStates: number[] = []
  for (const iso of boundary.iso3166_2) {
    const state = US_STATES_BY_ISO[iso.toUpperCase()]
    if (state) tigerStates.push(state.fips)
  }
  if (tigerStates.length) sources.tigerStates = 'US Census FIPS codes'

  if (!boundary.updatesUrl) {
    warnings.push(
      'This extract has no Geofabrik replication URL, so incremental OSM updates are unavailable — use UPDATE_MODE=full.',
    )
  }

  // Who's-on-First place ids narrow Pelias's admin-hierarchy import. There is no
  // zero-config lookup from an ISO code to a WOF id, and the field is optional —
  // omitting it makes Pelias import the whole country's admin data, which is
  // slower but correct. So leave it empty rather than guess an id that would
  // silently import the wrong place.
  warnings.push(
    'Pelias `wofIds` left empty — Pelias will import the country\'s full admin hierarchy. Optional: narrow it with a Who\'s-on-First place id from spelunker.whosonfirst.org.',
  )

  const region: RegionInput = {
    key: suggestRegionKey(boundary.id),
    label: boundary.label,
    osmExtracts: [boundary.pbfUrl],
    osmReplication: boundary.updatesUrl ? [boundary.updatesUrl] : [],
    bbox: boundary.bbox,
    // The GTFS feed search is a bbox query; pass the boundary's own box rather
    // than a named token that would have to be hardcoded per region.
    gtfsRegion: boundary.bbox.join(','),
    pelias: {
      openaddresses: oa.files,
      wofIds: [],
      tigerStates,
    },
    enabled: true,
  }

  return { region, warnings, sources }
}
