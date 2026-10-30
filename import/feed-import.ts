/**
 * Per-zip GTFS feed import — the orchestration shared by every path that
 * lands a feed ZIP in data/gtfs and needs it reflected in PostGIS.
 *
 * Extracted from import/import-gtfs.ts so other entry points (the portolan
 * sync wrapper, future re-importers) can run the exact same sequence without
 * duplicating it. import-gtfs.ts runs main() at module load, so it cannot be
 * imported for its helpers directly.
 */

import { writeFileSync } from 'fs'
import JSZip from 'jszip'
import { db } from '../src/db'
import {
  parseStops,
  parseRoutes,
  parseAgencies,
  parseShapes,
  deriveStopRoutes,
  parseGtfsRecords,
  parseStopParents,
  deriveTripPatterns,
  importTripPatterns,
  parseTransfers,
  importTransfers,
  deriveRouteShapes,
  deriveBikesAllowed,
  importStops,
  importRoutes,
  importStopRoutes,
  updateRouteCentroids,
  importShapes,
  updateRouteShapes,
  updateBikesAllowed,
  recordFeed,
  clearFeed,
  type GtfsFeedInfo,
} from '../src/services/gtfs.service'

/**
 * A feed with no fixed-route timetable for the router: GTFS-Flex-only feeds,
 * whose stop_times point at service areas rather than stops, and zips with
 * nothing at their root. Not an error in the feed's own terms, and nothing to
 * import, so the run skips it rather than counting it as a failure.
 */
export class NoFixedRouteService extends Error {}

/**
 * Parse a GTFS ZIP on disk and import stops, routes, stop-route associations,
 * trip patterns, agency transfers, and shapes into PostGIS, then record the
 * feed in gtfs_feeds. Validates before replacing existing rows in one transaction.
 */
export async function importFeedFile(filepath: string, feedInfo: GtfsFeedInfo) {
  const { stops, routes, associations, patterns, transfers, shapes, routeShapes, bikesAllowed } =
    await prepareFeedFile(filepath, feedInfo)

  // Readers see either the previous complete feed or the new complete feed.
  // A failed batch rolls back every table, including the feed's import marker.
  const counts = await db.transaction(async tx => {
    await clearFeed(feedInfo.feedId, tx)
    const stopsImported = await importStops(stops, tx)
    const routesImported = await importRoutes(routes, tx)
    const stopRoutesImported = await importStopRoutes(associations, tx)
    await updateRouteCentroids(feedInfo.feedId, tx)
    const patternsImported = await importTripPatterns(feedInfo.feedId, patterns, tx)
    const transfersImported = await importTransfers(transfers, tx)
    const shapesImported = await importShapes(shapes, feedInfo.feedId, tx)
    await updateRouteShapes(routeShapes, feedInfo.feedId, tx)
    await updateBikesAllowed(bikesAllowed, feedInfo.feedId, tx)
    await recordFeed(feedInfo, stopsImported, routesImported, tx)
    return { stopsImported, routesImported, stopRoutesImported, patternsImported, transfersImported, shapesImported }
  })
  console.log(`  ✓ Imported ${counts.stopsImported} stops, ${counts.routesImported} routes, ${counts.stopRoutesImported} stop-route associations`)
  console.log(`  ✓ Imported ${counts.patternsImported} trip patterns, ${counts.transfersImported} agency transfers, ${counts.shapesImported} shapes`)
}

/**
 * Parse and validate a GTFS ZIP without touching the database. Throws on a feed
 * that would replace a working one with something the router can't use.
 */
export async function prepareFeedFile(filepath: string, feedInfo: GtfsFeedInfo) {
  const zip = await JSZip.loadAsync(await Bun.file(filepath).arrayBuffer())
  // Do all parsing before replacing a working feed. Stops without a timetable
  // look valid on the map but disappear from the router's station candidates.
  const required = async (name: string) => {
    const content = await readZipEntry(zip, name)
    if (!content?.trim()) throw new NoFixedRouteService(`${filepath}: missing or empty ${name}`)
    return content
  }
  const [stopsContent, routesContent, tripsContent, stopTimesContent, agencyContent, transfersContent, shapesContent] = await Promise.all([
    required('stops.txt'), required('routes.txt'), required('trips.txt'), required('stop_times.txt'),
    readZipEntry(zip, 'agency.txt'), readZipEntry(zip, 'transfers.txt'), readZipEntry(zip, 'shapes.txt'),
  ])
  const stops = parseStops(stopsContent, feedInfo.feedId)
  const routes = parseRoutes(routesContent, feedInfo.feedId, agencyContent ? parseAgencies(agencyContent) : new Map())
  const trips = parseGtfsRecords(tripsContent)
  const stopTimes = parseGtfsRecords(stopTimesContent)
  const tripIds = new Set(trips.map(t => t.trip_id))
  if (stopTimes.some(st => !tripIds.has(st.trip_id))) {
    throw new Error(`${filepath}: stop_times.txt references unknown trips`)
  }
  const associations = deriveStopRoutes(trips, stopTimes, feedInfo.feedId)
  if (!stops.length || !routes.length || !associations.length) {
    throw new NoFixedRouteService(`${filepath}: no stop served by a route (stops, routes or stop_times referencing them)`)
  }
  const stopIds = new Set(stops.map(s => s.stopId))
  const routeIds = new Set(routes.map(r => r.routeId))
  if (associations.some(a => !stopIds.has(a.stopId) || !routeIds.has(a.routeId))) {
    throw new Error(`${filepath}: timetable references unknown stops or routes`)
  }
  const patterns = deriveTripPatterns(trips, stopTimes, parseStopParents(stopsContent), feedInfo.feedId)
  const transfers = transfersContent ? parseTransfers(transfersContent, feedInfo.feedId) : []
  const shapes = shapesContent ? parseShapes(shapesContent) : new Map<string, [number, number][]>()
  const routeShapes = deriveRouteShapes(tripsContent)
  const bikesAllowed = deriveBikesAllowed(tripsContent)

  return { stops, routes, associations, patterns, transfers, shapes, routeShapes, bikesAllowed }
}

export async function readZipEntry(zip: JSZip, filename: string): Promise<string | null> {
  const entry = zip.file(filename)
  if (!entry) return null
  return await entry.async('string')
}

/** GTFS transfer_type 3: "Transfers forbidden between routes at these stops." */
const TRANSFER_FORBIDDEN = '3'

/**
 * Merge computed walking transfers into a GTFS ZIP's transfers.txt.
 *
 * This used to REPLACE the file, which threw away the only authoritative
 * record of which station pairs sit inside fare control. The MTA's subway
 * feed declares 613 transfers and connects Borough Hall to Jay St-MetroTech
 * in none of them — they are 240m apart, and walking between them means
 * leaving the paid area and paying a second fare. Overwriting that with
 * "every stop pair within 500m, timed by GraphHopper" asserted the transfer
 * the agency spent the file denying.
 *
 * So the feed's own rows win, and computed rows only fill gaps:
 *
 *   - a pair the feed already states, in either direction, is left alone —
 *     the agency's time is better than ours
 *   - a pair the feed FORBIDS (transfer_type 3, which portolan derives for
 *     gated stations) is never re-added by a computed row
 *   - everything else is added, which is what the computation is for: bus
 *     stops outside a station entrance that no agency bothers to file
 */
export async function injectTransfersTxt(zipPath: string, transfersTxt: string): Promise<void> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const entry = zip.file('transfers.txt') ?? zip.file(/(^|\/)transfers\.txt$/)[0]
  const existing = entry ? await entry.async('string') : null
  // stops.txt resolves platforms to their station, which a prohibition is
  // declared between — see mergeTransfersTxt.
  const stopsEntry = zip.file('stops.txt') ?? zip.file(/(^|\/)stops\.txt$/)[0]
  const stops = stopsEntry ? await stopsEntry.async('string') : null
  zip.file('transfers.txt', mergeTransfersTxt(existing, transfersTxt, stops))

  // JSZip defaults to STORE, so a feed re-serialized without this lands
  // uncompressed on the volume MOTIS imports from. The subway feed goes from
  // 5.6 MB to 43 MB that way.
  const updatedBuffer = await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' })
  writeFileSync(zipPath, updatedBuffer)
}

/**
 * Merge a computed transfers.txt over a feed's own. Exported for testing.
 *
 * Both sides are the narrow 4-column shape barrelman and portolan write
 * (from_stop_id, to_stop_id, transfer_type, min_transfer_time); a feed
 * carrying extra columns keeps them, because its rows pass through as
 * written rather than being re-serialised.
 *
 * `stopsTxt` is the feed's stops.txt, used to resolve a platform to its
 * parent station. Prohibitions are declared between the STATIONS a rider
 * recognises (the MTA forbids 423 -> A41) while computed transfers are
 * between PLATFORMS (423N -> A41S), so matching ids exactly would let every
 * platform pairing under a forbidden station back in — which is precisely
 * the phantom the prohibition exists to stop. Omit it and matching falls
 * back to exact ids.
 */
export function mergeTransfersTxt(
  existing: string | null,
  computed: string,
  stopsTxt?: string | null,
): string {
  const parse = (text: string) => {
    const lines = text.split(/\r?\n/).filter(l => l.trim() !== '')
    if (!lines.length) return { header: '', rows: [] as string[][], raw: [] as string[] }
    const header = lines[0]
    const cols = header.split(',').map(c => c.trim().replace(/^\ufeff/, ''))
    const idx = (name: string) => cols.indexOf(name)
    const iFrom = idx('from_stop_id'), iTo = idx('to_stop_id'), iType = idx('transfer_type')
    const rows: string[][] = []
    const raw: string[] = []
    for (const line of lines.slice(1)) {
      const cells = line.split(',')
      rows.push([cells[iFrom] ?? '', cells[iTo] ?? '', (cells[iType] ?? '').trim()])
      raw.push(line)
    }
    return { header, rows, raw }
  }

  if (!existing || existing.trim() === '') return computed

  const feed = parse(existing)
  if (!feed.header) return computed

  // stop_id -> parent_station, for the platform/station resolution above.
  const parent = new Map<string, string>()
  if (stopsTxt && stopsTxt.trim() !== '') {
    const lines = stopsTxt.split(/\r?\n/).filter(l => l.trim() !== '')
    const cols = (lines[0] ?? '').split(',').map(c => c.trim().replace(/^\ufeff/, ''))
    const iId = cols.indexOf('stop_id')
    const iParent = cols.indexOf('parent_station')
    if (iId !== -1 && iParent !== -1) {
      for (const line of lines.slice(1)) {
        const cells = line.split(',')
        const id = (cells[iId] ?? '').trim()
        const p = (cells[iParent] ?? '').trim()
        if (id && p) parent.set(id, p)
      }
    }
  }
  const station = (id: string) => parent.get(id) ?? id

  // Both directions are keyed: a feed that states A→B has said what the
  // connection is, and a computed B→A would contradict its own half.
  const known = new Set<string>()
  const forbidden = new Set<string>()
  feed.rows.forEach(([from, to, type]) => {
    known.add(`${from}\u0000${to}`)
    known.add(`${to}\u0000${from}`)
    if (type === TRANSFER_FORBIDDEN) {
      // Keyed on stations, so one row covers every platform under it.
      forbidden.add(`${station(from)}\u0000${station(to)}`)
      forbidden.add(`${station(to)}\u0000${station(from)}`)
    }
  })

  const added: string[] = []
  const comp = parse(computed)
  comp.rows.forEach(([from, to], i) => {
    const key = `${from}\u0000${to}`
    if (!from || !to || known.has(key)) return
    if (forbidden.has(`${station(from)}\u0000${station(to)}`)) return
    known.add(key)
    added.push(comp.raw[i])
  })

  return [feed.header, ...feed.raw, ...added].join('\n') + '\n'
}
