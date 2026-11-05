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
  groupStopTimes,
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
  storedLicense,
  type GtfsFeedInfo,
} from '../src/services/gtfs.service'
import { refreshDirectionLabels } from '../src/services/station-labels.service'
import { storeAgencyStationTimes } from '../src/services/station-paths.service'
import { agencyStationTimes } from '../src/lib/station-graph/agency'
import { SOURCES, type PathSource } from '../src/lib/station-graph/types'

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
    // clearFeed drops the gtfs_feeds row, licence included. A caller that did
    // not read the licence from Transitland keeps the one on record.
    if (feedInfo.license === undefined) {
      const stored = await storedLicense(feedInfo.feedId, tx)
      if (stored) feedInfo = { ...feedInfo, license: stored.license, licenseFetchedAt: stored.fetchedAt }
    }
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

  // Enrichment, so a failure costs the feed its signed directions, not its import.
  await refreshDirectionLabels({ feedId: feedInfo.feedId, log: (line) => console.log(line) })
    .catch((err) => console.warn(`  ! direction labels: ${(err as Error).message}`))
  await importPathways(filepath, feedInfo.feedId)
    .catch((err) => console.warn(`  ! pathways: ${(err as Error).message}`))
}

/**
 * A feed's in-station times from its pathways.txt, for the next station paths
 * build to use where the OSM graph has no path. Enrichment: most feeds have none.
 */
async function importPathways(filepath: string, feedId: string) {
  const zip = await JSZip.loadAsync(await Bun.file(filepath).arrayBuffer())
  const pathways = await readZipEntry(zip, 'pathways.txt')
  if (!pathways?.trim()) return
  const [stops, levels] = await Promise.all([readZipEntry(zip, 'stops.txt'), readZipEntry(zip, 'levels.txt')])
  const agency = agencyStationTimes({
    stops: parseGtfsRecords(stops ?? ''),
    pathways: parseGtfsRecords(pathways),
    levels: levels?.trim() ? parseGtfsRecords(levels) : [],
  })
  const stored = await storeAgencyStationTimes(feedId, agency)
  console.log(`  ✓ ${stored} in-station times from pathways.txt`)
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
  // Grouped by trip as it is read: a record per row of a large stop_times.txt
  // is what ran the US import out of memory.
  const stopTimes = groupStopTimes(stopTimesContent)
  const tripIds = new Set(trips.map(t => t.trip_id))
  if ([...stopTimes.keys()].some(id => !tripIds.has(id))) {
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
export async function injectTransfersTxt(
  zipPath: string,
  transfersTxt: string,
  stationTimes?: Map<string, StationTime>,
): Promise<void> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const entry = zip.file('transfers.txt') ?? zip.file(/(^|\/)transfers\.txt$/)[0]
  const existing = entry ? await entry.async('string') : null
  // stops.txt resolves platforms to their station, which a prohibition is
  // declared between — see mergeTransfersTxt.
  const stopsEntry = zip.file('stops.txt') ?? zip.file(/(^|\/)stops\.txt$/)[0]
  const stops = stopsEntry ? await stopsEntry.async('string') : null
  zip.file('transfers.txt', mergeTransfersTxt(existing, transfersTxt, stops, stationTimes))

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
 *
 * `stationTimes` (keyed by transferKey) are in-station times from the station
 * paths build. They are the exception to "the feed wins": a station-level row
 * takes the median of its platforms' times. Against the feed's flat default
 * (the MTA's 180 s on every connected pair) any tier wins; against a time the
 * agency set for that pair, only a walked one (graph, bridged) does, and the
 * platform rows under it take the agency's time too. A forbidden transfer is
 * never touched, and nor is a timed or in-seat one.
 */
export interface StationTime {
  seconds: number
  source: PathSource
}

const WALKED: ReadonlySet<PathSource> = new Set(['graph', 'bridged'])

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b)
  const m = s.length >> 1
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

/** The best source present, and the median of its times. */
function bestOf(times: StationTime[]): StationTime {
  const best = times.reduce((x, y) => (SOURCES.indexOf(y.source) < SOURCES.indexOf(x.source) ? y : x)).source
  return { seconds: median(times.filter((t) => t.source === best).map((t) => t.seconds)), source: best }
}

/**
 * A feed's blanket transfer time: what it files on a station's transfer to
 * itself (the MTA's 180 s), else a value most of its timed rows share. A row
 * carrying it says "connected", not "this long".
 */
export function flatTransferTime(rows: Array<{ from: string; to: string; seconds: number }>): number | null {
  const mode = (xs: number[]) => {
    const counts = new Map<number, number>()
    for (const x of xs) counts.set(x, (counts.get(x) ?? 0) + 1)
    const [value, n] = [...counts].sort((a, b) => b[1] - a[1])[0] ?? [null, 0]
    return { value, share: xs.length ? n / xs.length : 0 }
  }
  const self = mode(rows.filter((r) => r.from === r.to).map((r) => r.seconds))
  if (self.value !== null) return self.value
  const cross = mode(rows.map((r) => r.seconds))
  return rows.length >= 5 && cross.share >= 0.5 ? cross.value : null
}

export function mergeTransfersTxt(
  existing: string | null,
  computed: string,
  stopsTxt?: string | null,
  stationTimes?: Map<string, StationTime>,
): string {
  const parse = (text: string) => {
    const lines = text.split(/\r?\n/).filter(l => l.trim() !== '')
    if (!lines.length) return { header: '', rows: [] as string[][], raw: [] as string[], iType: -1, iMin: -1 }
    const header = lines[0]
    const cols = header.split(',').map(c => c.trim().replace(/^\ufeff/, ''))
    const idx = (name: string) => cols.indexOf(name)
    const iFrom = idx('from_stop_id'), iTo = idx('to_stop_id'), iType = idx('transfer_type')
    const iMin = idx('min_transfer_time')
    const rows: string[][] = []
    const raw: string[] = []
    for (const line of lines.slice(1)) {
      const cells = line.split(',')
      rows.push([cells[iFrom] ?? '', cells[iTo] ?? '', (cells[iType] ?? '').trim()])
      raw.push(line)
    }
    return { header, rows, raw, iType, iMin }
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

  // Station pairs whose agency time outranks an estimate, for the platform rows under them.
  const agencyTimed = new Map<string, number>()
  if (stationTimes?.size && feed.iMin !== -1 && feed.iType !== -1) {
    const minOf = (i: number) => {
      const v = (feed.raw[i].split(',')[feed.iMin] ?? '').trim()
      return v === '' ? null : Number(v)
    }
    const flat = flatTransferTime(feed.rows.flatMap(([from, to, type], i) => {
      const seconds = minOf(i)
      return type === '2' && seconds !== null && Number.isFinite(seconds) ? [{ from, to, seconds }] : []
    }))
    const byStation = new Map<string, StationTime[]>()
    for (const [key, time] of stationTimes) {
      const [from, to] = key.split('\u0000')
      const k = `${station(from)}\u0000${station(to)}`
      byStation.set(k, [...(byStation.get(k) ?? []), time])
    }
    feed.rows.forEach(([from, to, type], i) => {
      if (!['', '0', '2'].includes(type) || feed.raw[i].includes('"')) return
      const exact = stationTimes.get(`${from}\u0000${to}`)
      const stationLevel = !parent.has(from) && !parent.has(to) ? byStation.get(`${from}\u0000${to}`) : undefined
      const time = exact ?? (stationLevel?.length ? bestOf(stationLevel) : undefined)
      if (!time) return
      const own = type === '2' ? minOf(i) : null
      if (own !== null && own !== flat && from !== to && !WALKED.has(time.source)) {
        agencyTimed.set(`${station(from)}\u0000${station(to)}`, own)
        return
      }
      const cells = feed.raw[i].split(',')
      cells[feed.iType] = '2'
      cells[feed.iMin] = String(Math.round(time.seconds))
      feed.raw[i] = cells.join(',')
    })
  }

  const added: string[] = []
  const comp = parse(computed)
  comp.rows.forEach(([from, to], i) => {
    const key = `${from}\u0000${to}`
    if (!from || !to || known.has(key)) return
    if (forbidden.has(`${station(from)}\u0000${station(to)}`)) return
    known.add(key)
    const agency = agencyTimed.get(`${station(from)}\u0000${station(to)}`)
    const walked = stationTimes?.get(key)
    if (agency !== undefined && !(walked && WALKED.has(walked.source)) && comp.iMin !== -1) {
      const cells = comp.raw[i].split(',')
      cells[comp.iMin] = String(agency)
      added.push(cells.join(','))
    } else {
      added.push(comp.raw[i])
    }
  })

  return [feed.header, ...feed.raw, ...added].join('\n') + '\n'
}
