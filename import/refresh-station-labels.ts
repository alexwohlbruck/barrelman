/**
 * Refresh Station Labels: rebuild the direction each platform is signed for
 * and each station entrance's corner label and entry/exit flags.
 *
 *   bun run import/refresh-station-labels.ts [--feed <feed_id>] [--directions-only]
 *
 * Fetches the agency open data that applies to the feeds on this instance,
 * then derives the rest from OSM. update-osm.sh runs it after each update, the
 * GTFS import after each feed. Exits 1 on failure, which update-osm.sh reports
 * as a warning.
 */
import { ensureStationLabelSchema } from '../src/db'
import { refreshDirectionLabels, refreshEntranceLabels } from '../src/services/station-labels.service'

const args = process.argv.slice(2)
const flag = (name: string) => {
  const i = args.indexOf(name)
  return i === -1 ? undefined : args[i + 1]
}
const log = (line: string) => console.log(`[${new Date().toISOString().slice(0, 19).replace('T', ' ')}] ${line}`)

try {
  await ensureStationLabelSchema()
  log('Refreshing direction labels...')
  await refreshDirectionLabels({ feedId: flag('--feed'), log })
  if (!args.includes('--directions-only')) {
    log('Refreshing entrance labels...')
    await refreshEntranceLabels({ log })
  }
  log('Station labels refreshed.')
  process.exit(0)
} catch (err) {
  console.error('Station labels refresh failed:', err)
  process.exit(1)
}
