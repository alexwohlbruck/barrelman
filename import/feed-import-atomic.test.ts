import { afterEach, describe, expect, it, spyOn } from 'bun:test'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import JSZip from 'jszip'
import { getTableName } from 'drizzle-orm'
import { db } from '../src/db'
import { importFeedFile, NoFixedRouteService } from './feed-import'

const dirs: string[] = []
afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true })
})
const feed = { feedId: 'fixture', onestopId: 'f-fixture', name: 'Fixture', url: 'https://example.com/gtfs.zip' }
const files: Record<string, string> = {
  'stops.txt': 'stop_id,stop_name,stop_lat,stop_lon\n635N,Union Square,40.734673,-73.989951\n',
  'routes.txt': 'route_id,route_short_name,route_type\n4,4,1\n',
  'trips.txt': 'route_id,trip_id,service_id\n4,express,weekday\n',
  'stop_times.txt': 'trip_id,stop_id,arrival_time,departure_time,stop_sequence\nexpress,635N,18:00:00,18:00:00,1\n',
}
async function fixture(overrides: Record<string, string | null> = {}): Promise<string> {
  const dir = mkdtempSync(join(tmpdir(), 'gtfs-feed-'))
  dirs.push(dir)
  const zip = new JSZip()
  for (const [name, content] of Object.entries({ ...files, ...overrides })) {
    if (content != null) zip.file(name, content)
  }
  const path = join(dir, 'fixture.zip')
  await Bun.write(path, await zip.generateAsync({ type: 'uint8array' }))
  return path
}

describe('complete GTFS feed replacement', () => {
  it('rejects a stops-only ZIP before opening a write transaction', async () => {
    const transaction = spyOn(db, 'transaction')
    try {
      await expect(importFeedFile(await fixture({ 'trips.txt': null }), feed)).rejects.toThrow('missing or empty trips.txt')
      expect(transaction).not.toHaveBeenCalled()
    } finally { transaction.mockRestore() }
  })

  it('rejects a timetable with no stop-route associations before writing', async () => {
    const transaction = spyOn(db, 'transaction')
    try {
      // Skipped as having nothing to import, not failed: GTFS-Flex-only feeds
      // look like this, and failing the run on them blocked every other feed.
      await expect(importFeedFile(await fixture({ 'stop_times.txt': 'trip_id,stop_id\n' }), feed)).rejects.toBeInstanceOf(NoFixedRouteService)
      expect(transaction).not.toHaveBeenCalled()
    } finally { transaction.mockRestore() }
  })

  it('rejects unresolved trip, stop, and route references before writing', async () => {
    const transaction = spyOn(db, 'transaction')
    try {
      for (const [overrides, error] of [
        [{ 'trips.txt': 'route_id,trip_id\n4,other\n' }, 'unknown trips'],
        [{ 'stops.txt': 'stop_id,stop_name,stop_lat,stop_lon\nelsewhere,Other,40,-74\n' }, 'unknown stops or routes'],
        [{ 'routes.txt': 'route_id,route_short_name,route_type\n5,5,1\n' }, 'unknown stops or routes'],
      ] as const) {
        await expect(importFeedFile(await fixture(overrides), feed)).rejects.toThrow(error)
      }
      expect(transaction).not.toHaveBeenCalled()
    } finally { transaction.mockRestore() }
  })

  it('reads files that open with a UTF-8 byte-order mark', async () => {
    // Windows-written feeds do. Unstripped, the first column's key became
    // "\ufefftrip_id", every stop_time lost its trip, and 122 US feeds were
    // rejected as referencing unknown trips.
    const bom = '\ufeff'
    const transaction = spyOn(db, 'transaction').mockImplementation(async () => ({}) as any)
    try {
      const withBom = Object.fromEntries(Object.entries(files).map(([name, content]) => [name, bom + content]))
      await importFeedFile(await fixture(withBom), feed)
      expect(transaction).toHaveBeenCalled()
    } finally { transaction.mockRestore() }
  })

  it('writes the complete feed through one transaction', async () => {
    const inserted = new Map<string, any[]>()
    const write = {
      where: async () => undefined,
      onConflictDoNothing: async () => undefined,
      onConflictDoUpdate: async () => undefined,
      then: (resolve: (value?: unknown) => void) => resolve(),
    }
    const transaction = spyOn(db, 'transaction').mockImplementation(async (work: any) => work({
      delete: () => write,
      insert: (table: any) => ({ values: (values: any) => {
        inserted.set(getTableName(table), Array.isArray(values) ? values : [values])
        return write
      } }),
      update: () => ({ set: () => write }),
      execute: async () => [],
    }))
    try {
      await importFeedFile(await fixture(), feed)
      expect(transaction).toHaveBeenCalledTimes(1)
      expect(inserted.get('gtfs_stop_routes')).toEqual([{ feedId: 'fixture', stopId: '635N', routeId: '4' }])
      expect(inserted.get('gtfs_feeds')?.[0]).toMatchObject({ feedId: 'fixture', stopCount: 1, routeCount: 1 })
    } finally { transaction.mockRestore() }
  })

  it('propagates a database failure from the transaction to its caller', async () => {
    const failure = new Error('write failed')
    const transaction = spyOn(db, 'transaction').mockImplementation(async (work: any) => {
      // Exercise a real writer helper: it must use the supplied transaction,
      // rather than a second connection whose writes would escape rollback.
      return work({ delete: () => ({ where: async () => { throw failure } }) })
    })
    try {
      await expect(importFeedFile(await fixture(), feed)).rejects.toBe(failure)
      expect(transaction).toHaveBeenCalledTimes(1)
    } finally { transaction.mockRestore() }
  })
})
