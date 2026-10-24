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

test('bundles the meshes a tile meets, revalidated by ETag', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bridge-mesh-'))
  directories.push(dir)
  const a = new Uint8Array([66, 82, 77, 49, 9]), b = new Uint8Array([66, 82, 77, 49, 1, 2, 3, 4])
  const byName = new Map([[await writeBridgeMesh(a, dir), a], [await writeBridgeMesh(b, dir), b]])
  const names = [...byName.keys()].sort()
  const asked: number[][] = []
  const app = createBridgeMeshRoutes(dir, async (z, x, y) => { asked.push([z, x, y]); return names })
  const res = await app.handle(new Request('http://localhost/tiles/bridges/tiles/14/4823/6160', { headers: { 'accept-encoding': 'gzip' } }))
  expect(res.status).toBe(200)
  expect(asked).toEqual([[14, 4823, 6160]])
  expect(res.headers.get('content-encoding')).toBe('gzip')
  const bytes = Bun.gunzipSync(new Uint8Array(await res.arrayBuffer()))
  const view = new DataView(bytes.buffer, bytes.byteOffset)
  expect(view.getUint32(0, true)).toBe(0x31545242)
  expect(view.getUint32(4, true)).toBe(2)
  const first = Buffer.from(bytes.subarray(8, 40)).toString('hex')
  expect(`${first}.brm`).toBe(names[0])
  expect(bytes.subarray(44, 44 + view.getUint32(40, true))).toEqual(byName.get(names[0])!)
  const tag = res.headers.get('etag')!
  const again = await app.handle(new Request('http://localhost/tiles/bridges/tiles/14/4823/6160', { headers: { 'if-none-match': tag } }))
  expect(again.status).toBe(304)
})

test('bundles only at their zoom', async () => {
  const app = createBridgeMeshRoutes('/nonexistent', async () => [])
  expect((await app.handle(new Request('http://localhost/tiles/bridges/tiles/15/1/1'))).status).toBe(400)
  const empty = await app.handle(new Request('http://localhost/tiles/bridges/tiles/14/1/1'))
  expect(empty.status).toBe(200)
  expect(new Uint8Array(await empty.arrayBuffer()).byteLength).toBe(8)
})
