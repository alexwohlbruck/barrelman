/**
 * Import 3D landmarks from every source, from a shell.
 *
 *   bun run landmarks:import        update: one request per source whose
 *                                   release hasn't moved, only new models
 *                                   downloaded
 *   bun run landmarks:import:full   full: re-read every release, download and
 *                                   re-verify every model, rewrite every row
 *
 * The same import the API runs at startup and the console's "Import 3D
 * landmarks" tasks run; see src/services/landmark-import.service.ts. Run it
 * where the API runs (`docker compose exec barrelman bun run landmarks:import`),
 * so it reads the same DATABASE_URL and writes the same model cache.
 */
import { importLandmarks } from '../src/services/landmark-import.service'
import { connection } from '../src/db'

const args = process.argv.slice(2)
const unknown = args.filter((a) => a !== '--full')
if (unknown.length) {
  console.error(`Unknown argument: ${unknown.join(' ')}\nUsage: bun scripts/import-landmarks.ts [--full]`)
  process.exit(2)
}
const full = args.includes('--full')

try {
  const r = await importLandmarks({ full, log: (line) => console.log(line) })
  for (const s of r.sources) {
    for (const skip of s.skipped) console.warn(`${s.source} skipped ${skip}`)
    if (!s.off && !s.unchanged)
      console.log(`${s.source}: ${full ? 'full import' : 'update'}, ${s.landmarks} landmarks, ${s.downloaded} files downloaded`)
  }
} catch (err) {
  console.error('Landmark import failed:', err)
  process.exitCode = 1
} finally {
  await connection.end({ timeout: 5 })
}
