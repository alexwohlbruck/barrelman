import JSZip from 'jszip'
import type { GtfsRtUrl } from '../src/services/gtfs.service'
import { selectRtUrls } from '../src/services/rt-url-selection'
import { parseCsvRows, headerIndex } from '../src/lib/csv'

export async function columnValues(zip: JSZip, name: string, column: string): Promise<Set<string>> {
  const file = zip.file(name)
  if (!file) return new Set()
  const rows = parseCsvRows(await file.async('string'))
  const col = rows.length ? headerIndex(rows[0]!).get(column) : undefined
  if (col === undefined) return new Set()
  return new Set(rows.slice(1).map(cells => cells[col]?.trim() ?? '').filter(Boolean))
}

/**
 * Drop the discovered realtime URLs that don't serve this feed, so neither the
 * alignment sample nor MOTIS sees another agency's trips.
 */
export async function selectRealtime(
  buffer: ArrayBuffer | Uint8Array,
  feed: { feedId: string; onestopId?: string; name?: string; rtUrls?: GtfsRtUrl[] },
): Promise<GtfsRtUrl[] | undefined> {
  if (!feed.rtUrls?.length) return feed.rtUrls
  const zip = await JSZip.loadAsync(buffer)
  const { kept, dropped } = await selectRtUrls(feed.rtUrls, await columnValues(zip, 'routes.txt', 'route_id'), fetch, {
    trips: await columnValues(zip, 'trips.txt', 'trip_id'),
    stops: await columnValues(zip, 'stops.txt', 'stop_id'),
  })
  const label = feed.name || feed.onestopId || feed.feedId
  for (const { url, reason } of dropped) {
    console.log(`  ⚠ ${label}: dropped realtime ${url.split('?')[0]} (${reason})`)
  }
  return kept
}
