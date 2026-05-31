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
 */

import { parseArgs } from 'util'
import { writeFileSync, mkdirSync } from 'fs'
import { join } from 'path'
import { ensureGtfsSchema } from '../src/db'
import { generateMotisConfig } from '../src/services/gtfs.service'

const { values: args } = parseArgs({
  options: {
    output: { type: 'string', default: './motis/config.yml' },
  },
})

const outputPath = args.output!

async function main() {
  await ensureGtfsSchema()

  console.log('Generating MOTIS config from database...')
  const configYaml = await generateMotisConfig()

  mkdirSync(join(outputPath, '..'), { recursive: true })
  writeFileSync(outputPath, configYaml)
  console.log(`✓ Wrote MOTIS config to ${outputPath}`)

  // Summary
  const lines = configYaml.split('\n')
  const datasetCount = lines.filter(l => l.match(/^\s{4}\d+:$/)).length
  const rtCount = lines.filter(l => l.trim().startsWith('- url:')).length
  console.log(`  ${datasetCount} datasets, ${rtCount} GTFS-RT feed URLs`)

  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
