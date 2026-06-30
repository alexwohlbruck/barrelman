#!/usr/bin/env bun
/**
 * Apply config/transit-overrides.json display overrides (route colours/names,
 * stop names) to already-imported feeds, without a full re-import. Idempotent.
 *
 * Usage:
 *   bun run import/apply-overrides.ts
 */
import { applyDisplayOverrides, getOverriddenFeedIds } from '../src/services/gtfs.service'

async function main() {
  const feedIds = getOverriddenFeedIds()
  if (feedIds.length === 0) {
    console.log('No feeds in config/transit-overrides.json')
    process.exit(0)
  }
  console.log(`Applying overrides for ${feedIds.length} feed(s)...`)
  let total = 0
  for (const feedId of feedIds) {
    // Match by feed_id (the override block key); onestop_id matching also works
    // via applyDisplayOverrides, but here we pass the key as the feedId.
    const patched = await applyDisplayOverrides({
      feedId,
      onestopId: feedId,
      name: feedId,
      url: '',
    })
    console.log(`  feed ${feedId}: ${patched} row(s) patched`)
    total += patched
  }
  console.log(`Done. ${total} row(s) patched.`)
  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
