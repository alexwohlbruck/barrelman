#!/usr/bin/env bun
/**
 * GBFS System Catalog Importer
 *
 * Fetches the MobilityData GBFS systems catalog (1,050+ systems globally),
 * resolves each system's auto-discovery URL to discover feed endpoints,
 * and imports station locations into the barrelman DB.
 *
 * Usage:
 *   bun run import/import-gbfs-systems.ts [--country US] [--bbox "-74.3,40.5,-73.7,40.9"]
 */

import { parse } from 'csv-parse/sync'
import { db } from '../src/db'
import { sql } from 'drizzle-orm'
import { ensureGbfsSchema } from '../src/db'

// ── CLI args ────────────────────────────────────────────────────────

const args = process.argv.slice(2)
function getArg(name: string): string | undefined {
  const idx = args.indexOf(`--${name}`)
  return idx >= 0 && idx + 1 < args.length ? args[idx + 1] : undefined
}

const countryFilter = getArg('country')?.toUpperCase()
const bboxArg = getArg('bbox')
const bbox = bboxArg
  ? (() => {
      const [west, south, east, north] = bboxArg.split(',').map(Number)
      return { north, south, east, west }
    })()
  : null

console.log('GBFS Systems Importer')
console.log(`  Country filter: ${countryFilter || 'none (all countries)'}`)
console.log(`  Bounding box: ${bbox ? `${bbox.south},${bbox.west} → ${bbox.north},${bbox.east}` : 'none'}`)

// ── Ensure schema ───────────────────────────────────────────────────

await ensureGbfsSchema()

// ── Fetch systems catalog ───────────────────────────────────────────

const SYSTEMS_CSV_URL =
  'https://raw.githubusercontent.com/MobilityData/gbfs/master/systems.csv'

console.log('\nFetching MobilityData systems catalog...')
const csvResponse = await fetch(SYSTEMS_CSV_URL)
if (!csvResponse.ok) {
  console.error(`Failed to fetch systems.csv: ${csvResponse.status}`)
  process.exit(1)
}

const csvText = await csvResponse.text()
const records = parse(csvText, {
  columns: true,
  skip_empty_lines: true,
  trim: true,
  relax_column_count: true,
})

console.log(`  Found ${records.length} systems in catalog`)

// ── Filter ──────────────────────────────────────────────────────────

let filtered = records.filter((r: any) => r['Auto-Discovery URL'])

if (countryFilter) {
  filtered = filtered.filter((r: any) =>
    r['Country Code']?.toUpperCase() === countryFilter,
  )
  console.log(`  After country filter (${countryFilter}): ${filtered.length}`)
}

// Note: systems.csv doesn't have lat/lon columns, so bbox filtering
// happens at the station level after import. Use --country to narrow.
if (bbox) {
  console.log(`  Note: bbox filtering will be applied to stations after import (catalog has no coordinates)`)
}

console.log(`\nImporting ${filtered.length} systems...\n`)

// ── Import each system ──────────────────────────────────────────────

let imported = 0
let stationsImported = 0
let failed = 0

for (const row of filtered) {
  const systemId = row['System ID']
  const name = row.Name || null
  const discoveryUrl = row['Auto-Discovery URL']
  const countryCode = row['Country Code'] || null
  // Catalog doesn't have coordinates — we'll derive from first station later
  let lat: number | null = null
  let lon: number | null = null

  process.stdout.write(`  ${systemId}... `)

  try {
    // Fetch auto-discovery document
    const gbfsResponse = await fetch(discoveryUrl, {
      signal: AbortSignal.timeout(10_000),
    })
    if (!gbfsResponse.ok) {
      console.log(`✗ discovery ${gbfsResponse.status}`)
      failed++
      continue
    }

    const gbfsData = await gbfsResponse.json() as any

    // Extract feed URLs from the auto-discovery document
    // GBFS v3: data.feeds[], GBFS v2: data.{lang}.feeds[]
    let feeds: Array<{ name: string; url: string }> = []
    if (gbfsData.data?.feeds) {
      feeds = gbfsData.data.feeds
    } else if (gbfsData.data) {
      // v2 format: pick first language
      const firstLang = Object.keys(gbfsData.data)[0]
      if (firstLang) feeds = gbfsData.data[firstLang]?.feeds ?? []
    }

    const feedUrls: Record<string, string> = {}
    for (const feed of feeds) {
      feedUrls[feed.name] = feed.url
    }

    const hasStations = !!feedUrls.station_information
    const hasFreeFloating = !!feedUrls.vehicle_status || !!feedUrls.free_bike_status

    // Fetch vehicle types if available
    let vehicleTypes: any[] = []
    if (feedUrls.vehicle_types) {
      try {
        const vtRes = await fetch(feedUrls.vehicle_types, {
          signal: AbortSignal.timeout(5_000),
        })
        if (vtRes.ok) {
          const vtData = await vtRes.json() as any
          vehicleTypes = vtData?.data?.vehicle_types ?? []
        }
      } catch { /* non-fatal */ }
    }

    const ttl = gbfsData.ttl ?? gbfsData.data?.ttl ?? 300

    // UPSERT system
    const safeSysId = systemId.replace(/'/g, "''")
    const safeName = (name || '').replace(/'/g, "''")
    const safeOperator = (row.Operator || '').replace(/'/g, "''")
    const safeUrl = discoveryUrl.replace(/'/g, "''")
    const safeCountry = (countryCode || '').replace(/'/g, "''")

    await db.execute(sql.raw(`
      INSERT INTO gbfs_systems (system_id, name, operator, url, country_code, lat, lon,
        vehicle_types, has_stations, has_free_floating, feed_urls, ttl)
      VALUES (
        '${safeSysId}', '${safeName}', '${safeOperator}', '${safeUrl}',
        '${safeCountry}', ${lat ?? 'NULL'}, ${lon ?? 'NULL'},
        '${JSON.stringify(vehicleTypes).replace(/'/g, "''")}'::jsonb,
        ${hasStations}, ${hasFreeFloating},
        '${JSON.stringify(feedUrls).replace(/'/g, "''")}'::jsonb,
        ${ttl}
      )
      ON CONFLICT (system_id) DO UPDATE SET
        name = EXCLUDED.name,
        operator = EXCLUDED.operator,
        url = EXCLUDED.url,
        country_code = EXCLUDED.country_code,
        lat = EXCLUDED.lat,
        lon = EXCLUDED.lon,
        vehicle_types = EXCLUDED.vehicle_types,
        has_stations = EXCLUDED.has_stations,
        has_free_floating = EXCLUDED.has_free_floating,
        feed_urls = EXCLUDED.feed_urls,
        ttl = EXCLUDED.ttl,
        imported_at = NOW()
    `))

    // Import stations if available
    let stationCount = 0
    if (feedUrls.station_information) {
      try {
        const stationRes = await fetch(feedUrls.station_information, {
          signal: AbortSignal.timeout(10_000),
        })
        if (stationRes.ok) {
          const stationData = await stationRes.json() as any
          const stations = stationData?.data?.stations ?? []

          for (const s of stations) {
            const safeStationId = (s.station_id || '').replace(/'/g, "''")
            const safeStationName = (s.name || '').replace(/'/g, "''")
            const stLat = s.lat ?? s.latitude
            const stLon = s.lon ?? s.longitude
            if (!stLat || !stLon) continue

            // Bbox filter at station level (catalog has no system-level coords)
            if (bbox) {
              if (stLat < bbox.south || stLat > bbox.north ||
                  stLon < bbox.west || stLon > bbox.east) continue
            }

            // Derive system center from first station
            if (lat === null) { lat = stLat; lon = stLon }

            await db.execute(sql.raw(`
              INSERT INTO gbfs_stations (system_id, station_id, name, lat, lon, capacity)
              VALUES ('${safeSysId}', '${safeStationId}', '${safeStationName}',
                      ${stLat}, ${stLon}, ${s.capacity ?? 'NULL'})
              ON CONFLICT (system_id, station_id) DO UPDATE SET
                name = EXCLUDED.name,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                capacity = EXCLUDED.capacity,
                updated_at = NOW()
            `))
            stationCount++
          }
          stationsImported += stationCount

          // Update system coordinates (derived from first station)
          if (lat !== null && lon !== null) {
            await db.execute(sql.raw(`
              UPDATE gbfs_systems SET lat = ${lat}, lon = ${lon}
              WHERE system_id = '${safeSysId}'
            `))
          }
        }
      } catch { /* non-fatal */ }
    }

    const vTypes = vehicleTypes.map((v: any) => v.form_factor || 'unknown').join(', ')
    console.log(`✓ ${stationCount} stations${vTypes ? ` [${vTypes}]` : ''}`)
    imported++
  } catch (err) {
    console.log(`✗ ${err instanceof Error ? err.message : err}`)
    failed++
  }
}

console.log(`\nDone: ${imported} systems imported, ${stationsImported} stations, ${failed} failed`)
