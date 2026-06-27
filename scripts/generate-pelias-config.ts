/**
 * Generate pelias/pelias.json from the unified REGIONS config.
 *
 *   REGIONS=north-carolina,nyc-metro bun run scripts/generate-pelias-config.ts
 *   REGIONS=global                   bun run scripts/generate-pelias-config.ts
 *
 * The non-imports settings (logger, esclient, api services) are preserved from
 * the existing pelias.json; only the `imports` block is regenerated so the
 * geocoder's coverage always matches the rest of the data pipeline.
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, resolve } from 'node:path'
import { resolveRegions } from '../src/config/regions'

const here = dirname(fileURLToPath(import.meta.url))
const configPath = resolve(here, '../pelias/pelias.json')

const r = resolveRegions()
const base = JSON.parse(readFileSync(configPath, 'utf8'))

const osmFilename = (url: string) => url.split('/').pop()!

base.imports = {
  adminLookup: { enabled: true },
  geonames: {
    datapath: '/data/geonames',
    countryCode: r.isGlobal ? 'ALL' : 'US',
  },
  openstreetmap: {
    download: r.osmExtracts.map((sourceURL) => ({ sourceURL })),
    leveldbpath: '/tmp',
    datapath: '/data/openstreetmap',
    import: r.osmExtracts.map((url) => ({ filename: osmFilename(url) })),
  },
  openaddresses: {
    datapath: '/data/openaddresses',
    files: r.peliasOpenaddresses,
  },
  polyline: { datapath: '/data/polylines', files: ['extract.0sv'] },
  whosonfirst: {
    datapath: '/data/whosonfirst',
    countryCode: 'US',
    ...(r.peliasWofIds.length ? { importPlace: r.peliasWofIds } : {}),
  },
  interpolation: {
    download: {
      tiger: {
        datapath: '/data/tiger',
        states: r.peliasTigerStates.map((state_code) => ({ state_code })),
      },
    },
  },
}

writeFileSync(configPath, JSON.stringify(base, null, 2) + '\n')
console.log(`Wrote ${configPath}`)
console.log(`  regions: ${r.keys.join(', ')}`)
console.log(`  osm extracts: ${r.osmExtracts.length}`)
console.log(`  openaddresses files: ${r.peliasOpenaddresses.length}`)
console.log(`  wof places: ${r.peliasWofIds.length}, tiger states: ${r.peliasTigerStates.join(',')}`)
