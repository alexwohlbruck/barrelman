/**
 * Import the Open Landmarks dataset from a shell.
 *
 *   bun run landmarks:import        update: one request when the release
 *                                   hasn't moved, only new models downloaded
 *   bun run landmarks:import:full   full: re-read the release, download and
 *                                   re-verify every model, rewrite every row
 *
 * The same import the API runs at startup and the console's "Import Open
 * Landmarks" tasks run; see src/services/openlandmarks.service.ts. Run it
 * where the API runs (`docker compose exec barrelman bun run landmarks:import`),
 * so it reads the same DATABASE_URL and writes the same model cache.
 */
import { importOpenLandmarks } from '../src/services/openlandmarks.service'
import { connection } from '../src/db'

const args = process.argv.slice(2)
const unknown = args.filter((a) => a !== '--full')
if (unknown.length) {
  console.error(`Unknown argument: ${unknown.join(' ')}\nUsage: bun scripts/import-open-landmarks.ts [--full]`)
  process.exit(2)
}
const full = args.includes('--full')

try {
  const r = await importOpenLandmarks({ full, log: (line) => console.log(line) })
  for (const s of r.skipped) console.warn(`skipped ${s}`)
  if (!r.unchanged)
    console.log(`${full ? 'Full import' : 'Update'}: ${r.landmarks} landmarks, ${r.downloaded} files downloaded, ${r.superseded} superseded`)
} catch (err) {
  console.error('Open Landmarks import failed:', err)
  process.exitCode = 1
} finally {
  await connection.end({ timeout: 5 })
}
