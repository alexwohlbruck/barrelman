#!/usr/bin/env bun
/**
 * GTFS Import Pipeline
 *
 * Downloads GTFS feeds from Transitland, parses stop/route data,
 * imports into PostGIS, and computes walking transfers.
 *
 * Usage:
 *   bun run import/import-gtfs.ts --region nc --api-key tlk_xxx --output-dir ./data/gtfs
 */

import { parseArgs } from 'util'
import { existsSync, mkdirSync, writeFileSync, readdirSync } from 'fs'
import { join, basename } from 'path'
import { injectFaresV2 } from './inject-fares-v2'
import { importFeedFile, injectTransfersTxt } from './feed-import'
import { ensureGtfsSchema } from '../src/db'
import {
  fetchFeedList,
  computeAllTransfers,
  generateTransfersTxt,
  generateMotisConfig,
  sanitizeGtfsZip,
} from '../src/services/gtfs.service'

// ── CLI args ────────────────────────────────────────────────────────

const { values: args } = parseArgs({
  options: {
    region: { type: 'string', default: 'nc' },
    'api-key': { type: 'string' },
    'output-dir': { type: 'string', default: './data/gtfs' },
    'motis-config': { type: 'string', default: './motis/config.yml' },
    'skip-download': { type: 'boolean', default: false },
    'skip-transfers': { type: 'boolean', default: false },
    'max-feeds': { type: 'string' },
    'transfer-distance': { type: 'string', default: '500' },
    'transfer-concurrency': { type: 'string', default: '8' },
  },
})

const region = args.region!
const apiKey = args['api-key']!
const outputDir = args['output-dir']!
const skipDownload = args['skip-download']!
const skipTransfers = args['skip-transfers']!
const maxFeeds = args['max-feeds'] ? parseInt(args['max-feeds']) : undefined
const motisConfigPath = args['motis-config']!
const transferDistance = parseInt(args['transfer-distance']!)
const transferConcurrency = parseInt(args['transfer-concurrency']!)

if (!apiKey && !skipDownload) {
  console.error('Error: --api-key is required (unless --skip-download is set)')
  process.exit(1)
}

// ── Main pipeline ───────────────────────────────────────────────────

async function main() {
  console.log(`\n=== GTFS Import Pipeline ===`)
  console.log(`Region: ${region}`)
  console.log(`Output: ${outputDir}`)
  console.log(`Transfer distance: ${transferDistance}m`)
  console.log(`Transfer concurrency: ${transferConcurrency}`)
  console.log('')

  // Ensure schema exists
  console.log('Ensuring GTFS schema...')
  await ensureGtfsSchema()

  // Create output directory
  mkdirSync(outputDir, { recursive: true })

  let feedFiles: string[] = []

  if (!skipDownload) {
    // Step 1: Fetch feed list from Transitland
    console.log(`\nFetching feed list from Transitland (region: ${region})...`)
    let feeds = await fetchFeedList(region, apiKey)
    console.log(`Found ${feeds.length} GTFS feeds`)

    const rtCount = feeds.filter(f => f.rtUrls?.length).length
    console.log(`  ${rtCount} feeds have GTFS-RT URLs`)

    if (maxFeeds) {
      feeds = feeds.slice(0, maxFeeds)
      console.log(`Limited to ${maxFeeds} feeds`)
    }

    // Step 2: Download each feed
    for (let i = 0; i < feeds.length; i++) {
      const feed = feeds[i]
      const filename = `${feed.feedId.replace(/[^a-zA-Z0-9_-]/g, '_')}.zip`
      const filepath = join(outputDir, filename)

      console.log(`\n[${i + 1}/${feeds.length}] Downloading ${feed.name || feed.feedId}...`)
      console.log(`  URL: ${feed.url}`)
      if (feed.rtUrls?.length) {
        console.log(`  RT: ${feed.rtUrls.length} realtime feed(s)`)
      }

      try {
        const response = await fetch(feed.url)
        if (!response.ok) {
          console.error(`  ✗ Download failed: ${response.status}`)
          continue
        }

        const rawBuffer = await response.arrayBuffer()
        console.log(`  ✓ Downloaded ${(rawBuffer.byteLength / 1024 / 1024).toFixed(1)} MB`)

        // Strip GTFS-Flex extension files that crash MOTIS
        const { buffer, removedFiles } = await sanitizeGtfsZip(rawBuffer)
        if (removedFiles.length > 0) {
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files: ${removedFiles.join(', ')}`)
        }

        writeFileSync(filepath, Buffer.from(buffer))
        feedFiles.push(filepath)

        // Step 3: Parse and import
        await importFeedFile(filepath, feed)
      } catch (err) {
        console.error(`  ✗ Error: ${err instanceof Error ? err.message : err}`)
      }
    }
  } else {
    // Skip download — import existing files
    console.log('\nSkipping download, importing existing GTFS files...')
    feedFiles = readdirSync(outputDir)
      .filter(f => f.endsWith('.zip'))
      .map(f => join(outputDir, f))

    for (const filepath of feedFiles) {
      const feedId = basename(filepath, '.zip')

      // Sanitize existing files too (they may pre-date the flex strip)
      try {
        const existingBuffer = await Bun.file(filepath).arrayBuffer()
        const { buffer: cleanBuffer, removedFiles } = await sanitizeGtfsZip(existingBuffer)
        if (removedFiles.length > 0) {
          writeFileSync(filepath, Buffer.from(cleanBuffer))
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files from ${basename(filepath)}`)
        }
      } catch (err) {
        console.error(`  ⚠ Flex sanitization failed for ${basename(filepath)}: ${err}`)
      }

      await importFeedFile(filepath, {
        feedId,
        onestopId: feedId,
        name: feedId,
        url: '',
        region,
      })
    }
  }

  // Step 4: Compute walking transfers
  if (!skipTransfers && feedFiles.length > 0) {
    console.log('\n=== Computing Walking Transfers ===')
    console.log(`Max distance: ${transferDistance}m`)

    const transfers = await computeAllTransfers(
      transferDistance,
      transferConcurrency,
      globalThis.fetch,
      (completed, total) => {
        if (completed % 100 === 0 || completed === total) {
          console.log(`  ${completed}/${total} pairs computed`)
        }
      },
    )

    console.log(`Computed ${transfers.length} transfer pairs`)

    // Write feed-specific transfers.txt into each feed ZIP
    // Only include transfers where BOTH stops belong to the target feed,
    // preventing stop ID collisions across different transit agencies.
    if (transfers.length > 0) {
      for (const filepath of feedFiles) {
        const feedId = basename(filepath, '.zip')
        const transfersTxt = generateTransfersTxt(transfers, feedId)
        const transferCount = transfersTxt.trim().split('\n').length - 1 // minus header
        try {
          await injectTransfersTxt(filepath, transfersTxt)
          console.log(`  ✓ Injected ${transferCount} transfers into ${basename(filepath)}`)
        } catch (err) {
          console.error(`  ✗ Failed to inject into ${basename(filepath)}: ${err}`)
        }
      }
    }
  }

  // Step 4b: Synthesize GTFS Fares v2 from v1 fare data so MOTIS (which
  // reads v2 only) can price itineraries from the agency's own feed.
  console.log('\n=== Converting Fares v1 → v2 ===')
  for (const filepath of feedFiles) {
    try {
      const status = await injectFaresV2(filepath)
      if (status.startsWith('converted')) {
        console.log(`  ✓ ${basename(filepath)}: ${status}`)
      }
    } catch (err) {
      console.error(`  ✗ ${basename(filepath)}: ${err}`)
    }
  }

  // Step 5: Generate MOTIS config with GTFS-RT feeds
  console.log('\n=== Generating MOTIS Config ===')
  try {
    const configYaml = await generateMotisConfig()
    mkdirSync(join(motisConfigPath, '..'), { recursive: true })
    writeFileSync(motisConfigPath, configYaml)
    console.log(`✓ Wrote MOTIS config to ${motisConfigPath}`)

    // Count RT-enabled feeds
    const rtLines = configYaml.split('\n').filter(l => l.trim().startsWith('- url:'))
    if (rtLines.length > 0) {
      console.log(`  ${rtLines.length} GTFS-RT feed URLs configured`)
    } else {
      console.log('  No GTFS-RT feeds found for this region')
    }
  } catch (err) {
    console.error(`✗ Failed to generate MOTIS config: ${err instanceof Error ? err.message : err}`)
  }

  console.log('\n=== Import Complete ===')
  console.log(`Processed ${feedFiles.length} feeds`)
  console.log(`Output directory: ${outputDir}`)
  console.log(`MOTIS config: ${motisConfigPath}`)
  console.log('')
  console.log('Next steps:')
  // NOT `docker compose restart motis`: `motis server` only serves the
  // pre-built dataset at /data/data and never re-imports, so a restart keeps
  // serving the old schedules and the feeds just downloaded never reach riders.
  console.log('  1. Rebuild the MOTIS dataset so these feeds take effect')
  console.log('     bash scripts/rebuild-motis.sh   (a restart alone keeps the old timetable)')
  console.log('')

  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
