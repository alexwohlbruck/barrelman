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

/**
 * Inject transfers.txt into an existing GTFS ZIP.
 * Replaces any existing transfers.txt.
 */
export async function injectTransfersTxt(zipPath: string, transfersTxt: string): Promise<void> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  zip.file('transfers.txt', transfersTxt)

  // JSZip defaults to STORE, so a feed re-serialized without this lands
  // uncompressed on the volume MOTIS imports from. The subway feed goes from
  // 5.6 MB to 43 MB that way.
  const updatedBuffer = await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' })
  writeFileSync(zipPath, updatedBuffer)
}
