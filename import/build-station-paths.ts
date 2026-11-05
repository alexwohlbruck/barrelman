/**
 * Build Station Paths: the level-aware walking graph of every rail station
 * complex, and a time for every platform-to-platform and platform-to-entrance
 * pair, each tagged with how it was made.
 *
 *   bun run import/build-station-paths.ts
 *
 * Builds station_complexes, station_platforms and station_paths as *_next
 * tables and swaps them in. update-osm.sh runs it after each update and the
 * full import after the database phase. Exits 1 on failure, which those
 * scripts report as a warning; the previous build keeps serving.
 */
import { buildStationPaths } from '../src/services/station-paths.service'

const log = (line: string) => console.log(`[${new Date().toISOString().slice(0, 19).replace('T', ' ')}] ${line}`)

try {
  log('Building station paths...')
  const started = Date.now()
  await buildStationPaths({ log })
  log(`Station paths built in ${Math.round((Date.now() - started) / 1000)}s.`)
  process.exit(0)
} catch (err) {
  console.error('Station paths build failed:', err)
  process.exit(1)
}
