import { test, expect, afterEach } from 'bun:test'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createBridgeMeshRoutes } from './bridge-meshes'
import { writeBridgeMesh } from '../lib/bridge-mesh/files'

const directories: string[] = []
afterEach(async () => { for (const dir of directories.splice(0)) await rm(dir, { recursive: true, force: true }) })

test('serves immutable precomputed bytes and their precompressed version', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'bridge-mesh-'))
  directories.push(dir)
  const bytes = new Uint8Array([66, 82, 77, 49, 1, 2, 3])
  const file = await writeBridgeMesh(bytes, dir)
  expect(await writeBridgeMesh(bytes, dir)).toBe(file)
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
