#!/usr/bin/env bun
/**
 * Generate MOTIS config.yml from the gtfs_feeds table.
 *
 * Reads all imported feeds and their GTFS-RT URLs from the database,
 * then writes a config.yml that MOTIS can load. Run this after updating
 * RT URLs without re-downloading all feeds.
 *
 * Usage:
 *   bun run import/generate-motis-config.ts [--output ./motis/config.yml]
 *   bun run import/generate-motis-config.ts --street-routing --osm-path /osm-data/region.osm.pbf
 *   bun run import/generate-motis-config.ts --street-area /gtfs-data/motis-street-area.geojson
 *
 * --street-area writes only the GeoJSON area the bikeshare systems MOTIS polls
 * cover (see getGbfsStreetArea), for rebuild-motis.sh to clip its street
 * extract to, and exits 3 when there is none.
 */

import { parseArgs } from 'util'
import { writeFileSync, mkdirSync } from 'fs'
import { join } from 'path'
import { ensureGtfsSchema } from '../src/db'
import { generateMotisConfig, getGbfsStreetArea } from '../src/services/gtfs.service'

const { values: args } = parseArgs({
  options: {
    output: { type: 'string', default: './motis/config.yml' },
    'street-routing': { type: 'boolean', default: false },
    'osm-path': { type: 'string', default: '/osm-data/region.osm.pbf' },
    'include-gbfs': { type: 'boolean', default: undefined },
    'no-gbfs': { type: 'boolean', default: false },
    // GTFS-RT poll interval (seconds). Omitted → MOTIS_RT_UPDATE_INTERVAL env, else 60.
    'rt-update-interval': { type: 'string', default: undefined },
    'street-area': { type: 'string', default: undefined },
  },
})

const outputPath = args.output!

async function writeStreetArea(path: string) {
  const geometry = await getGbfsStreetArea()
  if (!geometry) {
    console.log('No GBFS system with a location in this instance\'s regions — no street area')
    process.exit(3)
  }
  mkdirSync(join(path, '..'), { recursive: true })
  writeFileSync(path, JSON.stringify({ type: 'Feature', properties: {}, geometry }))
  console.log(`✓ Wrote the GBFS street area to ${path}`)
  process.exit(0)
}

async function main() {
  await ensureGtfsSchema()
  if (args['street-area']) return writeStreetArea(args['street-area'])

  const enableStreetRouting = args['street-routing'] ?? false
  const osmPath = args['osm-path']!
  const includeGbfs = args['no-gbfs'] ? false : (args['include-gbfs'] ?? enableStreetRouting)
  const rtUpdateInterval = args['rt-update-interval'] != null
    ? Number(args['rt-update-interval'])
    : undefined

  console.log('Generating MOTIS config from database...')
  if (enableStreetRouting) {
    console.log(`  Street routing enabled (OSM: ${osmPath})`)
  }
  if (includeGbfs) {
    console.log('  GBFS feeds included')
  }

  const configYaml = await generateMotisConfig({
    enableStreetRouting,
    osmPath,
    includeGbfs,
    rtUpdateInterval,
  })

  mkdirSync(join(outputPath, '..'), { recursive: true })
  writeFileSync(outputPath, configYaml)
  console.log(`✓ Wrote MOTIS config to ${outputPath}`)

  // Summary
  // Count the entries themselves, not their quoted keys: GTFS and GBFS blocks
  // both key on `    "<id>":`, so matching that reported every GBFS system as a
  // timetable dataset — "1334 datasets" on an instance with zero GTFS feeds.
  const lines = configYaml.split('\n')
  const datasetCount = lines.filter(l => l.startsWith('      path: "')).length
  const rtCount = lines.filter(l => l.trim().startsWith('- url:')).length
  const gbfsCount = lines.filter(l => l.startsWith('      url: "')).length
  console.log(`  ${datasetCount} datasets, ${rtCount} GTFS-RT feed URLs`)
  if (datasetCount === 0) {
    console.log('  No GTFS feeds — timetable block omitted. MOTIS cannot import a')
    console.log('  dataset without one; run the transit import (scripts/download-gtfs.sh) first.')
  }
  // A config with datasets but no RT URLs is valid YAML and imports cleanly,
  // so nothing downstream complains. MOTIS simply polls nothing and every
  // departure comes back with realTime: false. Warn here, because the next
  // step bakes this file into the dataset at /data/data/config.yml. Only a
  // full `motis import` undoes that; restarting MOTIS does not.
  if (datasetCount > 0 && rtCount === 0) {
    console.warn('  ⚠ No GTFS-RT URLs in this config. Realtime will be off for every feed.')
    console.warn('    If it was working before, gtfs_feeds.rt_urls is likely empty:')
    console.warn('    bun run import/backfill-rt-urls.ts, then regenerate and rebuild MOTIS.')
  }
  if (includeGbfs) {
    console.log(`  ${gbfsCount} GBFS feeds`)
  }

  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
