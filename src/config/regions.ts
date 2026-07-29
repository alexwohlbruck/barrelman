/**
 * Unified region selection for the data pipeline.
 *
 * Which geographic regions we import (OSM, GTFS, GBFS, Pelias geocoder) is
 * driven by ONE env var — REGIONS — resolved against config/regions.json:
 *
 *   REGIONS=north-carolina,nyc-metro   # dev: import just these regions
 *   REGIONS=global                     # prod: planet / all feeds
 *
 * Every importer reads from here instead of its own ad-hoc GEOFABRIK_URL /
 * GTFS_REGION / --bbox flag, so dev coverage is configured in exactly one place.
 *
 * Usable two ways:
 *   - import { resolveRegions } from './config/regions'   (TS importers)
 *   - bun run src/config/regions.ts <command>             (bash scripts; see CLI below)
 */
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'

export type Bbox = [west: number, south: number, east: number, north: number]

export interface PeliasRegionConfig {
  /** OpenAddresses CSV paths (e.g. "us/ny/statewide.csv"). */
  openaddresses: string[]
  /** Who's-on-First place ids to import (admin hierarchy), usually US states. */
  wofIds: string[]
  /** TIGER state FIPS codes for address interpolation. */
  tigerStates: number[]
  /** Geonames country filter (global only). */
  countryCode?: string
}

export interface RegionDef {
  label: string
  osmExtracts: string[]
  osmReplication?: string[]
  bbox: Bbox
  gtfsRegion: string
  pelias: PeliasRegionConfig
}

export interface RegionsFile {
  regions: Record<string, RegionDef>
  global: RegionDef
}

export const GLOBAL_KEY = 'global'

/** Read the baked config/regions.json — the seed + fallback for the DB store. */
export function loadFile(): RegionsFile {
  const here = dirname(fileURLToPath(import.meta.url))
  // src/config/regions.ts → ../../config/regions.json
  const path = resolve(here, '../../config/regions.json')
  return JSON.parse(readFileSync(path, 'utf8')) as RegionsFile
}

export interface ResolvedRegions {
  /** True when REGIONS=global (prod) — planet OSM / all feeds. */
  isGlobal: boolean
  /** The region keys that were selected. */
  keys: string[]
  /** The resolved region definitions. */
  regions: RegionDef[]
  /** All OSM extract URLs across the selected regions (deduped). */
  osmExtracts: string[]
  /** All OSM replication URLs across the selected regions (deduped). */
  osmReplication: string[]
  /** GTFS region tokens across the selected regions (deduped). */
  gtfsRegions: string[]
  /** Pelias OpenAddresses CSV paths across the selected regions (deduped). */
  peliasOpenaddresses: string[]
  /** Pelias Who's-on-First place ids across the selected regions (deduped). */
  peliasWofIds: string[]
  /** Pelias TIGER state FIPS codes across the selected regions (deduped). */
  peliasTigerStates: number[]
  /** Union bbox [west, south, east, north] covering all selected regions. */
  bbox: Bbox
}

const uniq = (xs: string[]) => Array.from(new Set(xs))

/**
 * Load the region definitions, preferring the DB store (editable from the admin
 * console) and falling back to the baked JSON when the table is empty or the DB
 * is unreachable. The DB module is imported lazily so file-only / no-DB contexts
 * (and the fallback path itself) never require a database connection.
 */
async function loadRegions(): Promise<RegionsFile> {
  try {
    const { loadRegionsFromDb } = await import('../services/region-store.service')
    const fromDb = await loadRegionsFromDb()
    if (fromDb) return fromDb
  } catch {
    // DB unavailable / table missing — use the shipped defaults.
  }
  return loadFile()
}

/**
 * Resolve region definitions + an explicit/env REGIONS selection into concrete
 * data sources. Pure: no I/O — see {@link resolveRegions} for the loading wrapper.
 */
export function resolveFromFile(file: RegionsFile, value = process.env.REGIONS): ResolvedRegions {
  const raw = (value ?? 'north-carolina,nyc-metro').trim()

  if (raw === GLOBAL_KEY || raw === '') {
    const g = file.global
    return {
      isGlobal: raw === GLOBAL_KEY,
      keys: [GLOBAL_KEY],
      regions: [g],
      osmExtracts: g.osmExtracts,
      osmReplication: g.osmReplication ?? [],
      gtfsRegions: [g.gtfsRegion],
      peliasOpenaddresses: g.pelias.openaddresses,
      peliasWofIds: g.pelias.wofIds,
      peliasTigerStates: g.pelias.tigerStates,
      bbox: g.bbox,
    }
  }

  const keys = raw.split(',').map((s) => s.trim()).filter(Boolean)
  const regions: RegionDef[] = []
  for (const key of keys) {
    const def = file.regions[key]
    if (!def) {
      const known = Object.keys(file.regions).join(', ')
      throw new Error(`Unknown region "${key}". Known regions: ${known}, ${GLOBAL_KEY}`)
    }
    regions.push(def)
  }

  const bbox: Bbox = regions.reduce<Bbox>(
    (acc, r) => [
      Math.min(acc[0], r.bbox[0]),
      Math.min(acc[1], r.bbox[1]),
      Math.max(acc[2], r.bbox[2]),
      Math.max(acc[3], r.bbox[3]),
    ],
    [180, 90, -180, -90],
  )

  return {
    isGlobal: false,
    keys,
    regions,
    osmExtracts: uniq(regions.flatMap((r) => r.osmExtracts)),
    osmReplication: uniq(regions.flatMap((r) => r.osmReplication ?? [])),
    gtfsRegions: uniq(regions.map((r) => r.gtfsRegion)),
    peliasOpenaddresses: uniq(regions.flatMap((r) => r.pelias.openaddresses)),
    peliasWofIds: uniq(regions.flatMap((r) => r.pelias.wofIds)),
    peliasTigerStates: Array.from(new Set(regions.flatMap((r) => r.pelias.tigerStates))),
    bbox,
  }
}

/**
 * Resolve the REGIONS env var (or an explicit value) into concrete data sources.
 * Defaults to 'north-carolina,nyc-metro' when unset (the standard dev regions).
 * Reads the DB region store (console-editable) with the baked JSON as fallback.
 */
export async function resolveRegions(value = process.env.REGIONS): Promise<ResolvedRegions> {
  return resolveFromFile(await loadRegions(), value)
}

// ── CLI ──────────────────────────────────────────────────────────────────────
// Lets bash scripts read resolved values without duplicating the registry, e.g.:
//   for url in $(bun run src/config/regions.ts osm-extracts); do …; done
//   BBOX=$(bun run src/config/regions.ts bbox)
//   if [ "$(bun run src/config/regions.ts is-global)" = "true" ]; then …; fi
if (import.meta.main) {
  const cmd = process.argv[2]
  const r = await resolveRegions()
  const out = (xs: string[]) => xs.join('\n')
  switch (cmd) {
    case 'keys': console.log(out(r.keys)); break
    case 'osm-extracts': console.log(out(r.osmExtracts)); break
    case 'osm-replication': console.log(out(r.osmReplication)); break
    case 'gtfs-regions': console.log(out(r.gtfsRegions)); break
    case 'openaddresses': console.log(out(r.peliasOpenaddresses)); break
    case 'wof-ids': console.log(out(r.peliasWofIds)); break
    case 'tiger-states': console.log(out(r.peliasTigerStates.map(String))); break
    case 'bbox': console.log(r.bbox.join(',')); break
    case 'is-global': console.log(String(r.isGlobal)); break
    case 'summary':
      console.log(`Regions: ${r.keys.join(', ')}`)
      console.log(`OSM extracts: ${r.osmExtracts.length}`)
      console.log(`GTFS regions: ${r.gtfsRegions.join(', ')}`)
      console.log(`OpenAddresses: ${r.peliasOpenaddresses.join(', ')}`)
      console.log(`Bbox: ${r.bbox.join(',')}`)
      break
    default:
      console.error('Usage: bun run src/config/regions.ts <keys|osm-extracts|osm-replication|gtfs-regions|openaddresses|bbox|is-global|summary>')
      process.exit(1)
  }
  // resolveRegions may have opened a DB handle (region store); exit so the CLI
  // doesn't hang waiting on an idle connection.
  process.exit(0)
}
