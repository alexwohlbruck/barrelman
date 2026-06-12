/**
 * Convert GTFS Fares v1 to Fares v2 inside feed ZIPs.
 *
 * MOTIS (nigiri) computes itinerary fares from GTFS Fares v2 only
 * (fare_products / fare_leg_rules / fare_transfer_rules), but most agencies
 * that publish fares at all still ship v1 (fare_attributes + fare_rules).
 * This step synthesizes the v2 files from the agency's own v1 data — fares
 * stay feed-sourced, nothing is hand-maintained.
 *
 * Supported v1 shapes:
 *  - Flat fares (no fare_rules): every fare row becomes a rider category
 *    (first row = default, typically the adult fare) priced over all legs.
 *  - Route-scoped fares (fare_rules with route_id): each fare becomes a
 *    network + product; routes map via route_networks. A route claimed by
 *    several fares keeps the first (agencies list the base fare first).
 *  - Free-transfer windows (transfers/transfer_duration) become
 *    fare_transfer_rules with no transfer product (= free) limited by count
 *    and duration.
 *
 * Zone-based feeds (origin_id/destination_id/contains_id) are skipped —
 * v1 zone semantics don't map mechanically onto v2 areas.
 *
 * Usage: bun run import/inject-fares-v2.ts [--dir ./data/gtfs] [--dry-run]
 */
import { readdirSync, writeFileSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { parseArgs } from 'util'

// ── CSV helpers ─────────────────────────────────────────────────────

/** Minimal RFC-4180-ish parser: quoted fields, embedded commas, CRLF. */
export function parseCsv(text: string): Record<string, string>[] {
  const rows: string[][] = []
  let field = ''
  let row: string[] = []
  let inQuotes = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++ } else inQuotes = false
      } else field += c
    } else if (c === '"') inQuotes = true
    else if (c === ',') { row.push(field); field = '' }
    else if (c === '\n') {
      row.push(field); field = ''
      rows.push(row); row = []
    } else if (c !== '\r') field += c
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row) }
  if (!rows.length) return []
  const header = rows[0].map((h) => h.trim().replace(/^﻿/, ''))
  return rows.slice(1).filter((r) => r.some((v) => v !== '')).map((r) => {
    const obj: Record<string, string> = {}
    header.forEach((h, i) => { obj[h] = (r[i] ?? '').trim() })
    return obj
  })
}

function toCsv(header: string[], rows: string[][]): string {
  const esc = (v: string) => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v)
  return [header.join(','), ...rows.map((r) => r.map(esc).join(','))].join('\n') + '\n'
}

// ── Conversion ──────────────────────────────────────────────────────

export interface ConversionResult {
  skipped?: 'no-v1-fares' | 'already-v2' | 'zone-based'
  /** Synthesized v2 files to add to the zip. */
  files?: Record<string, string>
  productCount?: number
  shape?: 'flat' | 'route-scoped'
}

/**
 * Build Fares v2 files from a feed's v1 fare data.
 *
 * @param fareAttributes contents of fare_attributes.txt (or null)
 * @param fareRules contents of fare_rules.txt (or null)
 * @param hasV2 whether the zip already ships fare_leg_rules/fare_products
 */
export function convertFaresV1toV2(
  fareAttributes: string | null,
  fareRules: string | null,
  hasV2: boolean,
): ConversionResult {
  if (hasV2) return { skipped: 'already-v2' }
  if (!fareAttributes) return { skipped: 'no-v1-fares' }

  const fares = parseCsv(fareAttributes)
  if (!fares.length) return { skipped: 'no-v1-fares' }

  const rules = fareRules ? parseCsv(fareRules) : []
  const zoneBased = rules.some(
    (r) => r.origin_id || r.destination_id || r.contains_id,
  )
  if (zoneBased) return { skipped: 'zone-based' }

  const files: Record<string, string> = {}
  const products: string[][] = []
  const legRules: string[][] = []
  const transferRules: string[][] = []

  /** v1 transfers: '' = unlimited, '0' = none, N = N transfers allowed. */
  const transferRow = (fare: Record<string, string>, legGroup: string) => {
    const t = fare.transfers ?? ''
    if (t === '0') return
    const count = t === '' ? '-1' : t
    const duration = fare.transfer_duration || ''
    // duration_limit_type 1 = departure of first leg → departure of next
    transferRules.push([
      legGroup, legGroup, count, duration, duration ? '1' : '', '0', '',
    ])
  }

  const routeRules = rules.filter((r) => r.route_id)
  if (routeRules.length) {
    // Route-scoped: each fare is a network of its routes.
    const networks: string[][] = []
    const routeNetworks: string[][] = []
    const claimedRoutes = new Set<string>()
    const faresWithRoutes = new Set(routeRules.map((r) => r.fare_id))

    for (const fare of fares) {
      if (!faresWithRoutes.has(fare.fare_id)) continue
      const netId = `net_${fare.fare_id}`
      const legGroup = `lg_${fare.fare_id}`
      let claimed = 0
      for (const r of routeRules) {
        if (r.fare_id !== fare.fare_id) continue
        if (claimedRoutes.has(r.route_id)) continue // first fare wins
        claimedRoutes.add(r.route_id)
        routeNetworks.push([netId, r.route_id])
        claimed++
      }
      if (!claimed) continue
      networks.push([netId, fare.fare_id])
      products.push([`fp_${fare.fare_id}`, fare.fare_id, '', '', fare.price, fare.currency_type || 'USD'])
      legRules.push([legGroup, netId, '', '', '', '', `fp_${fare.fare_id}`, ''])
      transferRow(fare, legGroup)
    }
    if (!products.length) return { skipped: 'no-v1-fares' }
    files['networks.txt'] = toCsv(['network_id', 'network_name'], networks)
    files['route_networks.txt'] = toCsv(['network_id', 'route_id'], routeNetworks)
  } else {
    // Flat: fare rows are rider categories over all legs; first row is the
    // default (agencies list the standard adult fare first).
    const categories: string[][] = []
    fares.forEach((fare, i) => {
      const rc = `rc_${fare.fare_id}`
      categories.push([rc, fare.fare_id, i === 0 ? '1' : '0', ''])
      products.push([`fp_${fare.fare_id}`, fare.fare_id, rc, '', fare.price, fare.currency_type || 'USD'])
      legRules.push(['lg_all', '', '', '', '', '', `fp_${fare.fare_id}`, ''])
    })
    transferRow(fares[0], 'lg_all')
    files['rider_categories.txt'] = toCsv(
      ['rider_category_id', 'rider_category_name', 'is_default_fare_category', 'eligibility_url'],
      categories,
    )
  }

  files['fare_products.txt'] = toCsv(
    ['fare_product_id', 'fare_product_name', 'rider_category_id', 'fare_media_id', 'amount', 'currency'],
    products,
  )
  files['fare_leg_rules.txt'] = toCsv(
    ['leg_group_id', 'network_id', 'from_area_id', 'to_area_id', 'from_timeframe_group_id', 'to_timeframe_group_id', 'fare_product_id', 'rule_priority'],
    legRules,
  )
  if (transferRules.length) {
    files['fare_transfer_rules.txt'] = toCsv(
      ['from_leg_group_id', 'to_leg_group_id', 'transfer_count', 'duration_limit', 'duration_limit_type', 'fare_transfer_type', 'fare_product_id'],
      transferRules,
    )
  }

  return {
    files,
    productCount: products.length,
    shape: routeRules.length ? 'route-scoped' : 'flat',
  }
}

// ── ZIP plumbing ────────────────────────────────────────────────────

async function readEntry(zip: JSZip, name: string): Promise<string | null> {
  // Feeds sometimes nest files in a folder — match by suffix.
  const entry = zip.file(name) ?? zip.file(new RegExp(`(^|/)${name}$`))[0]
  return entry ? await entry.async('string') : null
}

/** Convert one feed zip in place. Returns a human-readable status line. */
export async function injectFaresV2(zipPath: string, dryRun = false): Promise<string> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const hasV2 =
    zip.file(/(^|\/)fare_leg_rules\.txt$/).length > 0 ||
    zip.file(/(^|\/)fare_products\.txt$/).length > 0
  const result = convertFaresV1toV2(
    await readEntry(zip, 'fare_attributes.txt'),
    await readEntry(zip, 'fare_rules.txt'),
    hasV2,
  )

  if (result.skipped) return result.skipped
  if (!dryRun) {
    for (const [name, content] of Object.entries(result.files!)) {
      zip.file(name, content)
    }
    writeFileSync(zipPath, await zip.generateAsync({ type: 'nodebuffer' }))
  }
  return `converted (${result.shape}, ${result.productCount} products)`
}

// ── Runner ──────────────────────────────────────────────────────────

if (import.meta.main) {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      dir: { type: 'string', default: './data/gtfs' },
      'dry-run': { type: 'boolean', default: false },
    },
  })

  const dir = values.dir!
  const zips = readdirSync(dir).filter((f) => f.endsWith('.zip'))
  console.log(`Scanning ${zips.length} feeds in ${dir}${values['dry-run'] ? ' (dry run)' : ''}`)

  let converted = 0
  const counts: Record<string, number> = {}
  for (const f of zips) {
    try {
      const status = await injectFaresV2(join(dir, f), values['dry-run'])
      counts[status.split(' ')[0]] = (counts[status.split(' ')[0]] || 0) + 1
      if (status.startsWith('converted')) {
        converted++
        console.log(`  ✓ ${basename(f)}: ${status}`)
      } else if (status === 'zone-based') {
        console.log(`  ~ ${basename(f)}: skipped (zone-based v1 fares)`)
      }
    } catch (err) {
      console.error(`  ✗ ${basename(f)}: ${err}`)
    }
  }
  console.log(`\nDone: ${converted} feeds converted`, counts)
}
