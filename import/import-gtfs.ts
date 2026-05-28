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
import JSZip from 'jszip'
import { ensureGtfsSchema } from '../src/db'
import {
  fetchFeedList,
  parseStops,
  parseRoutes,
  parseAgencies,
  deriveStopRoutes,
  importStops,
  importRoutes,
  importStopRoutes,
  recordFeed,
  clearFeed,
  computeAllTransfers,
  generateTransfersTxt,
  type GtfsFeedInfo,
} from '../src/services/gtfs.service'

// ── CLI args ────────────────────────────────────────────────────────

const { values: args } = parseArgs({
  options: {
    region: { type: 'string', default: 'nc' },
    'api-key': { type: 'string' },
    'output-dir': { type: 'string', default: './data/gtfs' },
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

      try {
        const response = await fetch(feed.url)
        if (!response.ok) {
          console.error(`  ✗ Download failed: ${response.status}`)
          continue
        }

        const buffer = await response.arrayBuffer()
        writeFileSync(filepath, Buffer.from(buffer))
        console.log(`  ✓ Downloaded ${(buffer.byteLength / 1024 / 1024).toFixed(1)} MB`)

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

  console.log('\n=== Import Complete ===')
  console.log(`Processed ${feedFiles.length} feeds`)
  console.log(`Output directory: ${outputDir}`)
  console.log('')
  console.log('Next steps:')
  console.log('  1. Restart MOTIS to load updated GTFS data')
  console.log('     docker compose restart motis')
  console.log('')

  process.exit(0)
}

// ── Feed import ─────────────────────────────────────────────────────

async function importFeedFile(filepath: string, feedInfo: GtfsFeedInfo) {
  try {
    const buffer = await Bun.file(filepath).arrayBuffer()
    const zip = await JSZip.loadAsync(buffer)

    // Read required files from ZIP
    const stopsContent = await readZipEntry(zip, 'stops.txt')
    const routesContent = await readZipEntry(zip, 'routes.txt')
    const agencyContent = await readZipEntry(zip, 'agency.txt')

    if (!stopsContent) {
      console.log(`  ⚠ No stops.txt found, skipping`)
      return
    }

    // Clear existing data for this feed
    await clearFeed(feedInfo.feedId)

    // Parse and import stops
    const stops = parseStops(stopsContent, feedInfo.feedId)
    const stopsImported = await importStops(stops)
    console.log(`  ✓ Imported ${stopsImported} stops`)

    // Parse and import routes
    let routesImported = 0
    if (routesContent) {
      const agencyMap = agencyContent ? parseAgencies(agencyContent) : new Map()
      const routes = parseRoutes(routesContent, feedInfo.feedId, agencyMap)
      routesImported = await importRoutes(routes)
      console.log(`  ✓ Imported ${routesImported} routes`)
    }

    // Derive and import stop→route associations
    let stopRoutesImported = 0
    const tripsContent = await readZipEntry(zip, 'trips.txt')
    const stopTimesContent = await readZipEntry(zip, 'stop_times.txt')
    if (tripsContent && stopTimesContent) {
      const associations = deriveStopRoutes(tripsContent, stopTimesContent, feedInfo.feedId)
      stopRoutesImported = await importStopRoutes(associations)
      console.log(`  ✓ Imported ${stopRoutesImported} stop-route associations`)
    }

    // Record feed in tracking table
    await recordFeed(feedInfo, stopsImported, routesImported)
  } catch (err) {
    console.error(`  ✗ Import error: ${err instanceof Error ? err.message : err}`)
  }
}

async function readZipEntry(zip: JSZip, filename: string): Promise<string | null> {
  const entry = zip.file(filename)
  if (!entry) return null
  return await entry.async('string')
}

/**
 * Inject transfers.txt into an existing GTFS ZIP.
 * Replaces any existing transfers.txt.
 */
async function injectTransfersTxt(zipPath: string, transfersTxt: string): Promise<void> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  zip.file('transfers.txt', transfersTxt)

  const updatedBuffer = await zip.generateAsync({ type: 'nodebuffer' })
  writeFileSync(zipPath, updatedBuffer)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
