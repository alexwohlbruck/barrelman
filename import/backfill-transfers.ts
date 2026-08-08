/**
 * Backfill gtfs_transfers from the transfers.txt already inside local feed
 * zips — for databases imported before agency transfers were captured.
 * New imports get this automatically (import-gtfs.ts).
 *
 * Usage: bun run import/backfill-transfers.ts [--dir ./data/gtfs]
 */
import { readdirSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { parseArgs } from 'util'
import { ensureGtfsSchema, connection } from '../src/db'
import { parseTransfers, importTransfers } from '../src/services/gtfs.service'

const { values } = parseArgs({
  args: Bun.argv.slice(2),
  options: { dir: { type: 'string', default: './data/gtfs' } },
})

await ensureGtfsSchema()

let feeds = 0
let rows = 0
const zipFiles = readdirSync(values.dir!).filter((x) => x.endsWith('.zip'))
for (const f of zipFiles) {
  const feedId = basename(f, '.zip')
  console.log(`[${zipFiles.indexOf(f) + 1}/${zipFiles.length}] ${feedId}`)
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
