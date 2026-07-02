/**
 * Backfill gtfs_transfers from the transfers.txt already inside local feed
 * zips — for databases imported before agency transfers were captured.
 * New imports get this automatically (import-gtfs.ts).
 *
 * Usage: bun run import/backfill-transfers.ts [--dir ./data/gtfs-processed]
 */
import { readdirSync, existsSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { parseArgs } from 'util'
import { ensureGtfsSchema, connection } from '../src/db'
import { parseTransfers, importTransfers } from '../src/services/gtfs.service'

const { values } = parseArgs({
  args: Bun.argv.slice(2),
  // Default to the fully preprocessed zips (they carry the injected
  // transfers.txt); fall back to raw for layouts predating the transform stage.
  options: {
    dir: {
      type: 'string',
      default: existsSync('./data/gtfs-processed') ? './data/gtfs-processed' : './data/gtfs',
    },
  },
})

await ensureGtfsSchema()

let feeds = 0
let rows = 0
for (const f of readdirSync(values.dir!).filter((x) => x.endsWith('.zip'))) {
  const feedId = basename(f, '.zip')
  try {
    const zip = await JSZip.loadAsync(await Bun.file(join(values.dir!, f)).arrayBuffer())
    const entry = zip.file('transfers.txt') ?? zip.file(/(^|\/)transfers\.txt$/)[0]
    if (!entry) continue
    const transfers = parseTransfers(await entry.async('string'), feedId)
    const n = await importTransfers(transfers)
    if (n > 0) {
      feeds++
      rows += n
      console.log(`  ✓ ${feedId}: ${n} transfers`)
    }
  } catch (err) {
    console.error(`  ✗ ${feedId}: ${err}`)
  }
}
console.log(`Done: ${rows} transfers across ${feeds} feeds`)
await connection.end()
