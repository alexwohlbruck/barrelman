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
 * Parse a GTFS ZIP on disk and import stops, routes, stop-route associations,
 * trip patterns, agency transfers, and shapes into PostGIS, then record the
 * feed in gtfs_feeds. Clears any existing rows for the feed first.
 */
export async function importFeedFile(filepath: string, feedInfo: GtfsFeedInfo) {
  try {
    const buffer = await Bun.file(filepath).arrayBuffer()
    const zip = await JSZip.loadAsync(buffer)

    // Read required files from ZIP
    const stopsContent = await readZipEntry(zip, 'stops.txt')
    const routesContent = await readZipEntry(zip, 'routes.txt')
    const agencyContent = await readZipEntry(zip, 'agency.txt')

    if (!stopsContent) {
      console.log(`  ⚠ No stops.txt found, skipping`)
      return
    }

    // Clear existing data for this feed
    await clearFeed(feedInfo.feedId)

    // Parse and import stops
    const stops = parseStops(stopsContent, feedInfo.feedId)
    const stopsImported = await importStops(stops)
    console.log(`  ✓ Imported ${stopsImported} stops`)

    // Parse and import routes
    let routesImported = 0
    if (routesContent) {
      const agencyMap = agencyContent ? parseAgencies(agencyContent) : new Map()
      const routes = parseRoutes(routesContent, feedInfo.feedId, agencyMap)
      routesImported = await importRoutes(routes)
      console.log(`  ✓ Imported ${routesImported} routes`)
    }

    // Derive and import stop→route associations
    let stopRoutesImported = 0
    const tripsContent = await readZipEntry(zip, 'trips.txt')
    const stopTimesContent = await readZipEntry(zip, 'stop_times.txt')
    if (tripsContent && stopTimesContent) {
      // Parse the (large) files once and share the records across both
      // derivers, rather than re-parsing stop_times.txt per call.
      const tripRecords = parseGtfsRecords(tripsContent)
      const stopTimeRecords = parseGtfsRecords(stopTimesContent)

      const associations = deriveStopRoutes(tripRecords, stopTimeRecords, feedInfo.feedId)
      stopRoutesImported = await importStopRoutes(associations)
      console.log(`  ✓ Imported ${stopRoutesImported} stop-route associations`)

      // Each route's representative point, for search proximity ranking.
      await updateRouteCentroids(feedInfo.feedId)

      // Trip patterns — the ordered station sequence each route runs, powering
      // "every line that serves this board→alight directly" alternate lookups.
      const stopParents = parseStopParents(stopsContent)
      const patterns = deriveTripPatterns(tripRecords, stopTimeRecords, stopParents, feedInfo.feedId)
      const patternsImported = await importTripPatterns(feedInfo.feedId, patterns)
      console.log(`  ✓ Imported ${patternsImported} trip patterns`)
    }

    // Agency transfers — station-complex membership + min connection times
    const transfersContent = await readZipEntry(zip, 'transfers.txt')
    if (transfersContent) {
      const transfers = parseTransfers(transfersContent, feedInfo.feedId)
      const transfersImported = await importTransfers(transfers)
      if (transfersImported > 0) {
        console.log(`  ✓ Imported ${transfersImported} agency transfers`)
      }
    }

    // Parse and import shapes (for route-snapped vehicle interpolation)
    const shapesContent = await readZipEntry(zip, 'shapes.txt')
    if (shapesContent) {
      const shapes = parseShapes(shapesContent)
      const shapesImported = await importShapes(shapes, feedInfo.feedId)
      console.log(`  ✓ Imported ${shapesImported} shapes`)

      // Link routes to their canonical shape_id and bikes_allowed
      if (tripsContent) {
        const routeShapes = deriveRouteShapes(tripsContent)
        await updateRouteShapes(routeShapes, feedInfo.feedId)
        console.log(`  ✓ Linked ${routeShapes.size} routes to shapes`)

        const bikesAllowed = deriveBikesAllowed(tripsContent)
        const bikeRoutes = [...bikesAllowed.values()].filter(v => v > 0).length
        if (bikeRoutes > 0) {
          await updateBikesAllowed(bikesAllowed, feedInfo.feedId)
          console.log(`  ✓ ${bikeRoutes} routes with bikes allowed`)
        }
      }
    }

    // Record feed in tracking table
    await recordFeed(feedInfo, stopsImported, routesImported)
  } catch (err) {
    console.error(`  ✗ Import error: ${err instanceof Error ? err.message : err}`)
  }
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
  zip.file('transfers.txt', mergeTransfersTxt(existing, transfersTxt))

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
 */
export function mergeTransfersTxt(existing: string | null, computed: string): string {
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

  // Both directions are keyed: a feed that states A→B has said what the
  // connection is, and a computed B→A would contradict its own half.
  const known = new Set<string>()
  const forbidden = new Set<string>()
  feed.rows.forEach(([from, to, type]) => {
    known.add(`${from}\u0000${to}`)
    known.add(`${to}\u0000${from}`)
    if (type === TRANSFER_FORBIDDEN) {
      forbidden.add(`${from}\u0000${to}`)
      forbidden.add(`${to}\u0000${from}`)
    }
  })

  const added: string[] = []
  const comp = parse(computed)
  comp.rows.forEach(([from, to], i) => {
    const key = `${from}\u0000${to}`
    if (!from || !to || known.has(key) || forbidden.has(key)) return
    known.add(key)
    added.push(comp.raw[i])
  })

  return [feed.header, ...feed.raw, ...added].join('\n') + '\n'
}
