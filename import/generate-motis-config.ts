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
 */

import { parseArgs } from 'util'
import { writeFileSync, mkdirSync } from 'fs'
import { join } from 'path'
import { ensureGtfsSchema } from '../src/db'
import { generateMotisConfig } from '../src/services/gtfs.service'

const { values: args } = parseArgs({
  options: {
    output: { type: 'string', default: './motis/config.yml' },
    'street-routing': { type: 'boolean', default: false },
    'osm-path': { type: 'string', default: '/osm-data/region.osm.pbf' },
    'include-gbfs': { type: 'boolean', default: undefined },
    'no-gbfs': { type: 'boolean', default: false },
    // GTFS-RT poll interval (seconds). Omitted → MOTIS_RT_UPDATE_INTERVAL env, else 60.
    'rt-update-interval': { type: 'string', default: undefined },
  },
})

const outputPath = args.output!

async function main() {
  await ensureGtfsSchema()

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
  if (includeGbfs) {
    console.log(`  ${gbfsCount} GBFS feeds`)
  }

  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
