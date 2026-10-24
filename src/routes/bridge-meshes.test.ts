import { test, expect, afterEach } from 'bun:test'
import { mkdtemp, readdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createBridgeMeshRoutes } from './bridge-meshes'
import { writeBridgeMesh } from '../lib/bridge-mesh/files'

const directories: string[] = []
afterEach(async () => { for (const dir of directories.splice(0)) await rm(dir, { recursive: true, force: true }) })

test('stores meshes gzipped and serves them immutable, inflated for a client that refuses gzip', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bridge-mesh-'))
  directories.push(dir)
  const bytes = new Uint8Array([66, 82, 77, 49, 1, 2, 3])
  const file = await writeBridgeMesh(bytes, dir)
  expect(await writeBridgeMesh(bytes, dir)).toBe(file)
  expect(await readdir(dir)).toEqual([`${file}.gz`])
  const app = createBridgeMeshRoutes(dir)
  const res = await app.handle(new Request(`http://localhost/tiles/bridges/models/${file}`))
  expect(res.status).toBe(200)
  expect(res.headers.get('cache-control')).toContain('immutable')
  expect(new Uint8Array(await res.arrayBuffer())).toEqual(bytes)
  const compressed = await app.handle(new Request(`http://localhost/tiles/bridges/models/${file}`, { headers: { 'accept-encoding': 'gzip' } }))
  expect(compressed.headers.get('content-encoding')).toBe('gzip')
  expect(Bun.gunzipSync(new Uint8Array(await compressed.arrayBuffer()))).toEqual(bytes)
  const refused = await app.handle(new Request(`http://localhost/tiles/bridges/models/${file}`, { headers: { 'accept-encoding': 'gzip;q=0' } }))
  expect(refused.headers.get('content-encoding')).toBeNull()
})

test('missing meshes are not cached, and arbitrary filenames are rejected', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bridge-mesh-'))
  directories.push(dir)
  const app = createBridgeMeshRoutes(dir)
  const missing = await app.handle(new Request(`http://localhost/tiles/bridges/models/${'0'.repeat(64)}.brm`))
  expect(missing.status).toBe(404)
  expect(missing.headers.get('cache-control')).not.toContain('immutable')
  const invalid = await app.handle(new Request('http://localhost/tiles/bridges/models/secret.env'))
  expect(invalid.status).toBeGreaterThanOrEqual(400)
})

test('requires tile authentication when configured', async () => {
  const old = process.env.BARRELMAN_API_KEY
  process.env.BARRELMAN_API_KEY = 'test-service-secret'
  try {
    const res = await createBridgeMeshRoutes('/nonexistent').handle(new Request(`http://localhost/tiles/bridges/models/${'0'.repeat(64)}.brm`))
    expect(res.status).toBe(401)
  } finally { if (old === undefined) delete process.env.BARRELMAN_API_KEY; else process.env.BARRELMAN_API_KEY = old }
})

test('indexes the meshes a tile meets with their bounds, revalidated by ETag', async () => {
  const file = `${'a'.repeat(64)}.brm`
  const asked: number[][] = []
  const app = createBridgeMeshRoutes('/nonexistent', async (z, x, y) => { asked.push([z, x, y]); return [[file, -74.00123456789, 40.7, -73.99, 40.71], ['../secret', 0, 0, 1, 1]] })
  const res = await app.handle(new Request('http://localhost/tiles/bridges/index/14/4824/6159'))
  expect(res.status).toBe(200)
  expect(asked).toEqual([[14, 4824, 6159]])
  expect(res.headers.get('cache-control')).toContain('stale-while-revalidate')
  expect(await res.json()).toEqual({ decks: [[file, -74.0012346, 40.7, -73.99, 40.71]] })
  const again = await app.handle(new Request('http://localhost/tiles/bridges/index/14/4824/6159', { headers: { 'if-none-match': res.headers.get('etag')! } }))
  expect(again.status).toBe(304)
  const weak = await app.handle(new Request('http://localhost/tiles/bridges/index/14/4824/6159', { headers: { 'if-none-match': `"other", W/${res.headers.get('etag')}` } }))
  expect(weak.status).toBe(304)
})

test('indexes only at their zoom', async () => {
  const app = createBridgeMeshRoutes('/nonexistent', async () => [])
  expect((await app.handle(new Request('http://localhost/tiles/bridges/index/15/1/1'))).status).toBe(400)
  expect(await (await app.handle(new Request('http://localhost/tiles/bridges/index/14/1/1'))).json()).toEqual({ decks: [] })
})
