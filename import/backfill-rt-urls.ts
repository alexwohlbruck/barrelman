#!/usr/bin/env bun
/**
 * Backfill GTFS-RT URLs
 *
 * Discovers realtime vehicle position, trip update, and alert URLs
 * from Transitland for all existing feeds that don't have RT URLs yet.
 *
 * For each feed, queries Transitland to resolve the numeric feed ID
 * to a real onestop_id, then looks up the associated GTFS-RT feed.
 *
 * Usage:
 *   bun run import/backfill-rt-urls.ts
 *   bun run import/backfill-rt-urls.ts --feed-id 886   # single feed
 *   bun run import/backfill-rt-urls.ts --dry-run        # preview only
 */

import { parseArgs } from 'util'
import { discoverRtUrls } from '../src/services/gtfs.service'

const { values: args } = parseArgs({
  options: {
    'feed-id': { type: 'string' },
    'dry-run': { type: 'boolean', default: false },
  },
})

const feedId = args['feed-id']
const dryRun = args['dry-run']!

async function main() {
  const apiKey = process.env.TRANSITLAND_API_KEY
  if (!apiKey) {
    console.error('Error: TRANSITLAND_API_KEY environment variable is required')
    console.error('Set it in .env or pass it directly')
    process.exit(1)
  }

  console.log('\n=== GTFS-RT URL Backfill ===')
  if (feedId) {
    console.log(`Target feed: ${feedId}`)
  } else {
    console.log('Target: all feeds without RT URLs')
  }
  if (dryRun) {
    console.log('Mode: dry run (no database updates)')
  }
  console.log('')

  const result = await discoverRtUrls(
    feedId,
    apiKey,
    globalThis.fetch,
    (checked, total, fid, found) => {
      const status = found ? 'found RT URLs' : 'no RT feed'
      console.log(`  [${checked}/${total}] Feed ${fid}: ${status}`)
    },
    dryRun,
  )

  console.log('')
  console.log('=== Results ===')
  console.log(`  Feeds checked: ${result.checked}`)
  console.log(`  Feeds updated: ${result.updated}`)
  console.log(`  Errors: ${result.errors}`)

  if (result.updated > 0 && !dryRun) {
    console.log('')
    console.log('Next steps:')
    console.log('  1. Regenerate MOTIS config to include RT feeds:')
    console.log('     bun run import/import-gtfs.ts --skip-download --skip-transfers')
    console.log('  2. Restart MOTIS: docker compose restart motis')
  }

  if (dryRun && result.updated > 0) {
    console.log('')
    console.log('(Dry run -- no changes were written to the database)')
  }

  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
