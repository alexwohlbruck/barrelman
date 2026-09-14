/**
 * Default GTFS `bikes_allowed` to "allowed" inside feed ZIPs.
 *
 * GTFS has three states — 0 or empty "no information", 1 "allowed", 2 "not
 * allowed" — but MOTIS collapses them to a boolean and reads "no information"
 * as no. Most agencies omit the column entirely, so carriage looks prohibited
 * everywhere and `requireBikeTransport` returns nothing at all: of the feeds
 * we import, only two declare a single `bikes_allowed=1` trip between them.
 *
 * Rail, subway, tram and ferry operators generally permit a bike unless they
 * say otherwise, so an omitted column is far more often "unstated" than
 * "forbidden". This fills in that case and leaves an explicit 2 alone, so a
 * feed that really does forbid carriage keeps forbidding it and stays the
 * authority on its own services.
 *
 * Buses are the exception and are left unstated: most carry only a folding
 * bike, or a front rack holding two, and GTFS cannot express either. Guessing
 * "allowed" there routes riders onto a bus that will turn them away. Pass
 * `allowBus` for a network where every bus really does take a full-size bike.
 *
 * Usage: bun run import/inject-bikes-allowed.ts [--dir ./data/gtfs] [--dry-run] [--allow-bus]
 */
import { readdirSync, writeFileSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { parseArgs } from 'util'

import { parseCsvRows, serializeCsvRows, headerIndex } from './csv'

export interface BikesAllowedOptions {
  /** Fill bus trips too, rather than leaving their policy unstated. */
  allowBus?: boolean
}

export interface BikesAllowedResult {
  skipped?: 'no-trips' | 'empty-trips'
  /** Rewritten trips.txt, absent when nothing needed changing. */
  tripsTxt?: string
  /** Trips switched from "no information" to "allowed". */
  filled?: number
  /** Trips left alone because the feed forbids carriage on them. */
  forbidden?: number
  /** Trips the feed already declared as allowed. */
  declared?: number
  /** Trips left unstated because they run on a bus route. */
  bus?: number
  /** Whether the column had to be added rather than filled in. */
  addedColumn?: boolean
}

/** GTFS "not allowed". Everything else is unstated or allowed. */
const FORBIDDEN = '2'

/**
 * Whether a `route_type` denotes a bus.
 *
 * 3 and 11 are the basic bus and trolleybus; the extended set adds the 700s
 * for bus service and the 800s for trolleybus.
 */
function isBusRouteType(value: string): boolean {
  const type = Number(value.trim())
  return type === 3 || type === 11 || (type >= 700 && type <= 899)
}

/** route_ids in `routes.txt` that a bus runs on. */
function busRouteIds(routesTxt: string | null): Set<string> {
  const ids = new Set<string>()
  if (!routesTxt) return ids

  const rows = parseCsvRows(routesTxt)
  if (rows.length < 2) return ids

  const index = headerIndex(rows[0])
  const idColumn = index.get('route_id')
  const typeColumn = index.get('route_type')
  if (idColumn === undefined || typeColumn === undefined) return ids

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i]
    if (row.length <= typeColumn) continue
    if (isBusRouteType(row[typeColumn] ?? '')) ids.add((row[idColumn] ?? '').trim())
  }
  return ids
}

/**
 * Fill unstated `bikes_allowed` with "allowed", preserving explicit refusals
 * and leaving bus trips unstated.
 *
 * @param tripsTxt contents of trips.txt, or null when the feed has none
 * @param routesTxt contents of routes.txt, used to spot bus trips. Without
 *   it every route is treated as non-bus, since route type is unknowable.
 */
export function defaultBikesAllowed(
  tripsTxt: string | null,
  routesTxt: string | null = null,
  options: BikesAllowedOptions = {},
): BikesAllowedResult {
  if (!tripsTxt) return { skipped: 'no-trips' }

  const rows = parseCsvRows(tripsTxt)
  if (rows.length < 2) return { skipped: 'empty-trips' }

  const header = rows[0].map((h) => h.trim())
  let column = header.indexOf('bikes_allowed')
  const addedColumn = column === -1
  if (addedColumn) {
    column = header.length
    rows[0] = [...rows[0], 'bikes_allowed']
  }

  const buses = options.allowBus ? new Set<string>() : busRouteIds(routesTxt)
  const routeColumn = header.indexOf('route_id')

  let filled = 0
  let forbidden = 0
  let declared = 0
  let bus = 0

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i]
    // parseCsvRows keeps blank lines as empty rows; leave them as they were.
    if (row.length === 0 || (row.length === 1 && row[0] === '')) continue

    // A short row is one that ends before the column — pad so the value
    // lands under the right header rather than in whatever came last, and
    // so a row we leave unstated still has a cell to be unstated in.
    while (row.length <= column) row.push('')

    const current = (row[column] ?? '').trim()
    if (current === FORBIDDEN) {
      forbidden++
      continue
    }
    if (current === '1') {
      declared++
      continue
    }
    if (
      buses.size > 0 &&
      routeColumn !== -1 &&
      buses.has((row[routeColumn] ?? '').trim())
    ) {
      bus++
      continue
    }
    row[column] = '1'
    filled++
  }

  if (filled === 0) return { forbidden, declared, filled, bus, addedColumn }
  return {
    tripsTxt: serializeCsvRows(rows),
    filled,
    forbidden,
    declared,
    bus,
    addedColumn,
  }
}

async function readEntry(zip: JSZip, name: string): Promise<string | null> {
  // Feeds sometimes nest files in a folder — match by suffix.
  const entry = zip.file(name) ?? zip.file(new RegExp(`(^|/)${name}$`))[0]
  return entry ? await entry.async('string') : null
}

/** Rewrite one feed zip in place. Returns a human-readable status line. */
export async function injectBikesAllowed(
  zipPath: string,
  options: BikesAllowedOptions & { dryRun?: boolean } = {},
): Promise<string> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const result = defaultBikesAllowed(
    await readEntry(zip, 'trips.txt'),
    await readEntry(zip, 'routes.txt'),
    options,
  )
  if (result.skipped) return result.skipped

  const notes = [
    result.forbidden ? `${result.forbidden} left forbidden` : '',
    result.bus ? `${result.bus} bus left unstated` : '',
  ].filter(Boolean)
  const suffix = notes.length ? ` (${notes.join(', ')})` : ''

  if (!result.tripsTxt) return `unchanged${suffix}`

  if (!options.dryRun) {
    // Write back to wherever it was found, so a nested feed stays nested.
    const entry = zip.file('trips.txt') ?? zip.file(/(^|\/)trips\.txt$/)[0]
    zip.file(entry.name, result.tripsTxt)
    // DEFLATE because JSZip defaults to STORE, which balloons the feed.
    writeFileSync(
      zipPath,
      await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' }),
    )
  }

  return `filled ${result.filled}${suffix}`
}

// ── Runner ──────────────────────────────────────────────────────────

if (import.meta.main) {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      dir: { type: 'string', default: './data/gtfs' },
      'dry-run': { type: 'boolean', default: false },
      'allow-bus': { type: 'boolean', default: false },
    },
  })

  const dir = values.dir!
  const zips = readdirSync(dir).filter((f) => f.endsWith('.zip'))
  console.log(`Scanning ${zips.length} feeds in ${dir}${values['dry-run'] ? ' (dry run)' : ''}`)

  let changed = 0
  for (const f of zips) {
    try {
      const status = await injectBikesAllowed(join(dir, f), {
        dryRun: values['dry-run'],
        allowBus: values['allow-bus'],
      })
      if (status.startsWith('filled')) {
        changed++
        console.log(`  ✓ ${basename(f)}: ${status}`)
      }
    } catch (err) {
      console.error(`  ✗ ${basename(f)}: ${err}`)
    }
  }
  console.log(`\nDone: ${changed} feeds updated`)
}
