/**
 * Default GTFS `bikes_allowed` to "allowed" inside feed ZIPs.
 *
 * GTFS has three states — 0 or empty "no information", 1 "allowed", 2 "not
 * allowed" — but MOTIS collapses them to a boolean and reads "no information"
 * as no. Most agencies omit the column entirely, so carriage looks prohibited
 * everywhere and `requireBikeTransport` returns nothing at all: of the feeds
 * we import, only two declare a single `bikes_allowed=1` trip between them.
 *
 * Operators generally permit a bike unless they say otherwise, so an omitted
 * column is far more often "unstated" than "forbidden". This fills in that
 * case and leaves an explicit 2 alone, so a feed that really does forbid
 * carriage keeps forbidding it and stays the authority on its own services.
 *
 * Usage: bun run import/inject-bikes-allowed.ts [--dir ./data/gtfs] [--dry-run]
 */
import { readdirSync, writeFileSync } from 'fs'
import { join, basename } from 'path'
import JSZip from 'jszip'
import { parseArgs } from 'util'

import { parseCsvRows, serializeCsvRows } from './csv'

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
  /** Whether the column had to be added rather than filled in. */
  addedColumn?: boolean
}

/** GTFS "not allowed". Everything else is unstated or allowed. */
const FORBIDDEN = '2'

/**
 * Fill unstated `bikes_allowed` with "allowed", preserving explicit refusals.
 *
 * @param tripsTxt contents of trips.txt, or null when the feed has none
 */
export function defaultBikesAllowed(tripsTxt: string | null): BikesAllowedResult {
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

  let filled = 0
  let forbidden = 0
  let declared = 0

  for (let i = 1; i < rows.length; i++) {
    const row = rows[i]
    // parseCsvRows keeps blank lines as empty rows; leave them as they were.
    if (row.length === 0 || (row.length === 1 && row[0] === '')) continue

    // A short row is one that ends before the column — pad so the value
    // lands under the right header rather than in whatever came last.
    while (row.length < column) row.push('')

    const current = (row[column] ?? '').trim()
    if (current === FORBIDDEN) {
      forbidden++
      continue
    }
    if (current === '1') {
      declared++
      continue
    }
    row[column] = '1'
    filled++
  }

  if (filled === 0) return { forbidden, declared, filled, addedColumn }
  return { tripsTxt: serializeCsvRows(rows), filled, forbidden, declared, addedColumn }
}

async function readEntry(zip: JSZip, name: string): Promise<string | null> {
  // Feeds sometimes nest files in a folder — match by suffix.
  const entry = zip.file(name) ?? zip.file(new RegExp(`(^|/)${name}$`))[0]
  return entry ? await entry.async('string') : null
}

/** Rewrite one feed zip in place. Returns a human-readable status line. */
export async function injectBikesAllowed(zipPath: string, dryRun = false): Promise<string> {
  const buffer = await Bun.file(zipPath).arrayBuffer()
  const zip = await JSZip.loadAsync(buffer)

  const result = defaultBikesAllowed(await readEntry(zip, 'trips.txt'))
  if (result.skipped) return result.skipped
  if (!result.tripsTxt) return `unchanged (${result.forbidden} forbidden)`

  if (!dryRun) {
    // Write back to wherever it was found, so a nested feed stays nested.
    const entry = zip.file('trips.txt') ?? zip.file(/(^|\/)trips\.txt$/)[0]
    zip.file(entry.name, result.tripsTxt)
    // DEFLATE because JSZip defaults to STORE, which balloons the feed.
    writeFileSync(
      zipPath,
      await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' }),
    )
  }

  const kept = result.forbidden ? `, ${result.forbidden} left forbidden` : ''
  return `filled ${result.filled}${kept}`
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

  let changed = 0
  for (const f of zips) {
    try {
      const status = await injectBikesAllowed(join(dir, f), values['dry-run'])
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
