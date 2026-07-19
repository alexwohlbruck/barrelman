/**
 * DB-backed region store — the source of truth for which geographic regions the
 * data pipeline imports (OSM, GTFS, GBFS, Pelias). Seeded from
 * config/regions.json on first boot, then editable from the admin console.
 *
 * `resolveRegions()` (src/config/regions.ts) reads from here, falling back to the
 * baked JSON file when the table is empty or the DB is unreachable — so importers
 * pick up exactly the regions an operator defined in the console, while a fresh
 * or DB-less environment still resolves the shipped defaults.
 *
 * jsonb columns are bound via `${JSON.stringify(x)}::jsonb`; postgres.js can't
 * bind a raw object, and it auto-parses jsonb back into JS values on read.
 */
import { connection as sql } from '../db'
import { loadFile, GLOBAL_KEY, type Bbox, type PeliasRegionConfig, type RegionDef, type RegionsFile } from '../config/regions'

/** A region as stored/edited, i.e. a RegionDef plus its key + store metadata. */
export interface ImportRegion extends RegionDef {
  key: string
  isGlobal: boolean
  enabled: boolean
}

/** Shape accepted when creating/updating a region from the console. */
export interface RegionInput {
  key: string
  label: string
  osmExtracts?: string[]
  osmReplication?: string[]
  bbox: Bbox
  gtfsRegion?: string
  pelias?: Partial<PeliasRegionConfig>
  enabled?: boolean
  isGlobal?: boolean
}

let schemaReady: Promise<void> | null = null

export function ensureRegionsSchema(): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS import_regions (
          key             text PRIMARY KEY,
          label           text NOT NULL,
          osm_extracts    jsonb NOT NULL DEFAULT '[]'::jsonb,
          osm_replication jsonb NOT NULL DEFAULT '[]'::jsonb,
          bbox            jsonb NOT NULL,
          gtfs_region     text NOT NULL DEFAULT '',
          pelias          jsonb NOT NULL DEFAULT '{"openaddresses":[],"wofIds":[],"tigerStates":[]}'::jsonb,
          is_global       boolean NOT NULL DEFAULT false,
          enabled         boolean NOT NULL DEFAULT true,
          sort_order      integer NOT NULL DEFAULT 0,
          created_at      timestamptz NOT NULL DEFAULT now(),
          updated_at      timestamptz NOT NULL DEFAULT now()
        )`
      await seedIfEmpty()
    })()
  }
  return schemaReady
}

/** One-time seed from config/regions.json so a fresh DB mirrors the shipped defaults. */
async function seedIfEmpty(): Promise<void> {
  const [{ count }] = await sql<{ count: number }[]>`SELECT count(*)::int AS count FROM import_regions`
  if (count > 0) return
  const file = loadFile()
  const seed: Array<{ key: string; def: RegionDef; isGlobal: boolean }> = [
    ...Object.entries(file.regions).map(([key, def]) => ({ key, def, isGlobal: false })),
    { key: GLOBAL_KEY, def: file.global, isGlobal: true },
  ]
  let order = 0
  for (const { key, def, isGlobal } of seed) {
    await sql`
      INSERT INTO import_regions
        (key, label, osm_extracts, osm_replication, bbox, gtfs_region, pelias, is_global, enabled, sort_order)
      VALUES
        (${key}, ${def.label}, ${JSON.stringify(def.osmExtracts)}::jsonb,
         ${JSON.stringify(def.osmReplication ?? [])}::jsonb, ${JSON.stringify(def.bbox)}::jsonb,
         ${def.gtfsRegion}, ${JSON.stringify(def.pelias)}::jsonb, ${isGlobal}, true, ${order++})
      ON CONFLICT (key) DO NOTHING`
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function rowToRegion(r: any): ImportRegion {
  return {
    key: r.key,
    label: r.label,
    osmExtracts: r.osm_extracts ?? [],
    osmReplication: r.osm_replication ?? [],
    bbox: r.bbox as Bbox,
    gtfsRegion: r.gtfs_region ?? '',
    pelias: r.pelias as PeliasRegionConfig,
    isGlobal: r.is_global,
    enabled: r.enabled,
  }
}

function normalizePelias(p?: Partial<PeliasRegionConfig>): PeliasRegionConfig {
  return {
    openaddresses: p?.openaddresses ?? [],
    wofIds: p?.wofIds ?? [],
    tigerStates: p?.tigerStates ?? [],
    ...(p?.countryCode ? { countryCode: p.countryCode } : {}),
  }
}

export async function listRegions(): Promise<ImportRegion[]> {
  await ensureRegionsSchema()
  const rows = await sql`SELECT * FROM import_regions ORDER BY is_global ASC, sort_order ASC, key ASC`
  return rows.map(rowToRegion)
}

export async function getRegion(key: string): Promise<ImportRegion | null> {
  await ensureRegionsSchema()
  const [row] = await sql`SELECT * FROM import_regions WHERE key = ${key}`
  return row ? rowToRegion(row) : null
}

export async function createRegion(input: RegionInput): Promise<ImportRegion> {
  await ensureRegionsSchema()
  const pelias = normalizePelias(input.pelias)
  const [row] = await sql`
    INSERT INTO import_regions
      (key, label, osm_extracts, osm_replication, bbox, gtfs_region, pelias, is_global, enabled)
    VALUES
      (${input.key}, ${input.label}, ${JSON.stringify(input.osmExtracts ?? [])}::jsonb,
       ${JSON.stringify(input.osmReplication ?? [])}::jsonb, ${JSON.stringify(input.bbox)}::jsonb,
       ${input.gtfsRegion ?? ''}, ${JSON.stringify(pelias)}::jsonb, ${input.isGlobal ?? false}, ${input.enabled ?? true})
    RETURNING *`
  return rowToRegion(row)
}

/**
 * Full-object update (the console edits the whole region form). `key` and
 * `is_global` are structural and never changed here.
 */
export async function updateRegion(key: string, input: RegionInput): Promise<ImportRegion | null> {
  await ensureRegionsSchema()
  const pelias = normalizePelias(input.pelias)
  const [row] = await sql`
    UPDATE import_regions SET
      label           = ${input.label},
      osm_extracts    = ${JSON.stringify(input.osmExtracts ?? [])}::jsonb,
      osm_replication = ${JSON.stringify(input.osmReplication ?? [])}::jsonb,
      bbox            = ${JSON.stringify(input.bbox)}::jsonb,
      gtfs_region     = ${input.gtfsRegion ?? ''},
      pelias          = ${JSON.stringify(pelias)}::jsonb,
      enabled         = ${input.enabled ?? true},
      updated_at      = now()
    WHERE key = ${key}
    RETURNING *`
  return row ? rowToRegion(row) : null
}

export async function deleteRegion(key: string): Promise<boolean> {
  await ensureRegionsSchema()
  const rows = await sql`DELETE FROM import_regions WHERE key = ${key} RETURNING key`
  return rows.length > 0
}

/**
 * Assemble the RegionsFile shape resolveRegions() consumes. Returns null when the
 * store isn't usable as the source of truth (no rows, or no global row), so the
 * caller falls back to the baked JSON.
 */
export async function loadRegionsFromDb(): Promise<RegionsFile | null> {
  await ensureRegionsSchema()
  const rows = await sql`SELECT * FROM import_regions`
  if (!rows.length) return null
  const regions: Record<string, RegionDef> = {}
  let global: RegionDef | null = null
  for (const r of rows.map(rowToRegion)) {
    const def: RegionDef = {
      label: r.label,
      osmExtracts: r.osmExtracts,
      osmReplication: r.osmReplication,
      bbox: r.bbox,
      gtfsRegion: r.gtfsRegion,
      pelias: r.pelias,
    }
    if (r.isGlobal) global = def
    else regions[r.key] = def
  }
  if (!global) return null
  return { regions, global }
}
