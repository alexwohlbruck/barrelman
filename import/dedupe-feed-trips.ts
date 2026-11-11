/**
 * Drop trips one feed repeats from another feed of the same agency.
 *
 * Some agencies publish a second feed that is a superset of their main one:
 * RTD's flex feed carries every fixed-route trip of its regular feed plus a
 * handful of flex services. MOTIS loads both, so every Denver departure shows
 * twice. A trip counts as repeated when both feeds have the same trip_id
 * calling at the same stops; the two feeds may be different versions, so times
 * can differ. It is removed from the feed that has more trips, which keeps the
 * main feed whole and leaves the other with only what it adds. Routes left without trips go too, so the realtime matcher can't
 * add the dropped trips back as new ones. Feeds that are exact copies of each
 * other are left alone.
 *
 * Usage: bun run import/dedupe-feed-trips.ts [--dir ./data/gtfs] [--dry-run]
 */
import { readdirSync, renameSync, writeFileSync } from 'fs'
import { basename, join } from 'path'
import { parseArgs } from 'util'
import JSZip from 'jszip'
import { headerIndex, parseCsvRows, serializeCsvRows, splitCsvLines } from '../src/lib/csv'

/** Each trip's stops in order, keyed by trip_id. */
export function tripSignatures(stopTimesTxt: string): Map<string, string> {
  const lines = splitCsvLines(stopTimesTxt)
  const col = headerIndex(lines.next().value ?? [])
  const [trip, stop, seq] = ['trip_id', 'stop_id', 'stop_sequence'].map(c => col.get(c) ?? -1)
  const calls = new Map<string, [number, string][]>()
  for (const cells of lines) {
    const id = cells[trip!]?.trim()
    if (!id) continue
    let list = calls.get(id)
    if (!list) calls.set(id, (list = []))
    list.push([Number(cells[seq!]), cells[stop!]?.trim() ?? ''])
  }
  const out = new Map<string, string>()
  for (const [id, list] of calls) out.set(id, list.sort((a, b) => a[0] - b[0]).map(c => c[1]).join(' '))
  return out
}

/** Trips of `a` that `b` runs identically. */
export function sharedTrips(a: Map<string, string>, b: Map<string, string>): Set<string> {
  const shared = new Set<string>()
  for (const [id, sig] of a) if (b.get(id) === sig) shared.add(id)
  return shared
}

/** Which of two feeds sharing trips loses them, or null for exact copies. */
export function strippedSide(aTrips: number, bTrips: number, shared: number): 'a' | 'b' | null {
  if (!shared || aTrips === bTrips) return null
  return aTrips > bTrips ? 'a' : 'b'
}

/** Rows of a CSV whose `column` passes `keep`; the header always stays. */
function filterRows(text: string, column: string, keep: (value: string) => boolean): { text: string; removed: number } {
  const rows = parseCsvRows(text)
  const col = headerIndex(rows[0] ?? []).get(column)
  if (col === undefined) return { text, removed: 0 }
  const kept = [rows[0]!, ...rows.slice(1).filter(r => !r.some(v => v !== '') || keep((r[col] ?? '').trim()))]
  return { text: serializeCsvRows(kept), removed: rows.length - kept.length }
}

/** stop_times.txt without the given trips, line by line: it is too large to parse into rows. */
function withoutTripsStopTimes(text: string, tripIds: Set<string>): string {
  const firstBreak = text.indexOf('\n')
  const col = headerIndex(parseCsvRows(text.slice(0, firstBreak))[0] ?? []).get('trip_id') ?? 0
  const out: string[] = [text.slice(0, firstBreak + 1)]
  let start = firstBreak + 1
  while (start < text.length) {
    let end = text.indexOf('\n', start)
    if (end === -1) end = text.length
    const line = text.slice(start, end + 1)
    start = end + 1
    const cells = line.includes('"') ? parseCsvRows(line)[0] ?? [] : line.split(',', col + 1)
    if (!tripIds.has((cells[col] ?? '').trim())) out.push(line)
  }
  return out.join('')
}

async function entry(zip: JSZip, name: string) {
  const file = zip.file(name) ?? zip.file(new RegExp(`(^|/)${name}$`))[0]
  return file ? { name: file.name, text: await file.async('string') } : null
}

/** Remove trips from a feed zip, with their stop times, frequencies and any route left empty. */
export async function stripTrips(zip: JSZip, tripIds: Set<string>) {
  const trips = await entry(zip, 'trips.txt')
  if (!trips) return
  const rows = parseCsvRows(trips.text)
  const col = headerIndex(rows[0] ?? [])
  const tripCol = col.get('trip_id')!, routeCol = col.get('route_id')!
  const routesLeft = new Set(rows.slice(1).filter(r => !tripIds.has((r[tripCol] ?? '').trim())).map(r => (r[routeCol] ?? '').trim()))
  zip.file(trips.name, filterRows(trips.text, 'trip_id', id => !tripIds.has(id)).text)

  const stopTimes = await entry(zip, 'stop_times.txt')
  if (stopTimes) zip.file(stopTimes.name, withoutTripsStopTimes(stopTimes.text, tripIds))
  const frequencies = await entry(zip, 'frequencies.txt')
  if (frequencies) zip.file(frequencies.name, filterRows(frequencies.text, 'trip_id', id => !tripIds.has(id)).text)
  const routes = await entry(zip, 'routes.txt')
  if (routes) zip.file(routes.name, filterRows(routes.text, 'route_id', id => routesLeft.has(id)).text)
}

async function agencyNames(zip: JSZip): Promise<string[]> {
  const agency = await entry(zip, 'agency.txt')
  if (!agency) return []
  const rows = parseCsvRows(agency.text)
  const col = headerIndex(rows[0] ?? []).get('agency_name')
  return col === undefined ? [] : rows.slice(1).map(r => (r[col] ?? '').trim().toLowerCase()).filter(Boolean)
}

/** Dedupe every pair of zips that name a common agency. */
export async function dedupeFeedTrips(zipPaths: string[], options: { dryRun?: boolean } = {}) {
  const byAgency = new Map<string, string[]>()
  for (const path of zipPaths) {
    try {
      const zip = await JSZip.loadAsync(await Bun.file(path).arrayBuffer())
      for (const name of new Set(await agencyNames(zip))) byAgency.set(name, [...(byAgency.get(name) ?? []), path])
    } catch {}
  }

  const pairs = new Set<string>()
  for (const paths of byAgency.values()) {
    for (let i = 0; i < paths.length; i++) for (let j = i + 1; j < paths.length; j++) pairs.add([paths[i], paths[j]].sort().join('\n'))
  }

  let removed = 0
  for (const pair of pairs) {
    const [a, b] = pair.split('\n') as [string, string]
    const zips = await Promise.all([a, b].map(async p => JSZip.loadAsync(await Bun.file(p).arrayBuffer())))
    const sigs = await Promise.all(zips.map(async z => tripSignatures((await entry(z, 'stop_times.txt'))?.text ?? '')))
    const shared = sharedTrips(sigs[0]!, sigs[1]!)
    const side = strippedSide(sigs[0]!.size, sigs[1]!.size, shared.size)
    if (!side) {
      if (shared.size) console.log(`  – ${basename(a)} and ${basename(b)} are copies of each other (${shared.size} trips); left alone`)
      continue
    }
    const [path, zip, other] = side === 'a' ? [a, zips[0]!, b] : [b, zips[1]!, a]
    console.log(`  ✓ ${basename(path)}: dropped ${shared.size} trips ${basename(other)} already runs`)
    removed += shared.size
    if (options.dryRun) continue
    await stripTrips(zip, shared)
    const tmp = `${path}.tmp-${process.pid}`
    writeFileSync(tmp, await zip.generateAsync({ type: 'nodebuffer', compression: 'DEFLATE' }))
    renameSync(tmp, path)
  }
  console.log(`  repeated trips: ${removed} dropped`)
  return removed
}

if (import.meta.main) {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      dir: { type: 'string', default: './data/gtfs' },
      'dry-run': { type: 'boolean', default: false },
    },
  })
  const dir = values.dir!
  await dedupeFeedTrips(readdirSync(dir).filter(f => f.endsWith('.zip')).map(f => join(dir, f)), { dryRun: values['dry-run'] })
  process.exit(0)
}
