/**
 * Fetch the boundary catalog — the region index you search when defining a region.
 *
 * Downloads Geofabrik's `index-v1.json` (every extract it publishes, with real
 * political geometry and download URLs) into the `boundary_catalog` table. Run
 * this once before defining regions by name; re-run occasionally to pick up new
 * or renamed extracts.
 *
 *   bun run scripts/fetch-boundaries.ts
 *   bun run scripts/fetch-boundaries.ts --search colorado
 *
 * No API key, no configuration — one unauthenticated HTTP GET.
 */
import { parseArgs } from 'node:util'
import {
  refreshBoundaryCatalog,
  searchBoundaries,
  countBoundaries,
  catalogFetchedAt,
} from '../src/services/boundary-catalog.service'

const { values: args } = parseArgs({
  options: {
    search: { type: 'string' },
    'skip-fetch': { type: 'boolean', default: false },
    help: { type: 'boolean', default: false },
  },
  allowPositionals: true,
})

if (args.help) {
  console.log(`Usage: bun run scripts/fetch-boundaries.ts [options]

  --search <query>   After fetching, print matching boundaries
  --skip-fetch       Don't re-download; just search the cached catalog
  --help             Show this message
`)
  process.exit(0)
}

try {
  if (!args['skip-fetch']) {
    console.log('Fetching boundary catalog from Geofabrik...')
    const { count, source } = await refreshBoundaryCatalog()
    console.log(`✓ Cached ${count} boundaries from ${source}`)
  } else {
    const count = await countBoundaries()
    const at = await catalogFetchedAt()
    console.log(`Using cached catalog: ${count} boundaries${at ? ` (fetched ${at})` : ''}`)
    if (!count) {
      console.error('Catalog is empty — run without --skip-fetch first.')
      process.exit(1)
    }
  }

  if (args.search) {
    const results = await searchBoundaries(args.search, 15)
    console.log(`\nMatches for "${args.search}":\n`)
    if (!results.length) {
      console.log('  (none)')
    }
    for (const b of results) {
      const iso = [...b.iso3166_1, ...b.iso3166_2].join(', ')
      console.log(`  ${b.label}`)
      console.log(`    id:     ${b.id}${b.parent ? `  (in ${b.parent})` : ''}${iso ? `  [${iso}]` : ''}`)
      console.log(`    bbox:   ${b.bbox.map((n) => n.toFixed(2)).join(', ')}`)
      console.log(`    pbf:    ${b.pbfUrl}`)
      console.log('')
    }
    console.log('Create a region from one of these in the admin console (Regions → Add by name),')
    console.log('or preview the derived config with:')
    console.log(`  curl -XPOST localhost:5001/admin/boundaries/resolve -H 'content-type: application/json' \\`)
    console.log(`    -H "Authorization: Bearer $BARRELMAN_ADMIN_KEY" -d '{"id":"<id>"}'`)
  }

  process.exit(0)
} catch (err) {
  console.error(`\n✗ ${err instanceof Error ? err.message : String(err)}`)
  process.exit(1)
}
