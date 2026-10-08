/**
 * /tiles/landmarks: the placement tiles and the models they point at.
 *
 * The tile query and the model index are injected, so these run without a
 * database. What they hold the routes to is the contract a map client relies
 * on: empty tiles are 204 not errors, models are served as immutable glTF,
 * a name that is not one we serve never reaches the filesystem, and the whole
 * group sits behind the same guard as every other tile.
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test'
import { mkdtempSync, writeFileSync, rmSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import Elysia from 'elysia'
import { createLandmarkRoutes } from './landmarks'
import { createTileRoutes } from './tiles'

const get = (path: string, headers?: Record<string, string>) =>
  new Request(`http://localhost${path}`, { headers })

const savedApiKey = process.env.BARRELMAN_API_KEY
beforeEach(() => {
  // No auth configured reads as local development: open. bun loads the
  // service key from .env, so clear it.
  delete process.env.BARRELMAN_API_KEY
})
afterEach(() => {
  if (savedApiKey === undefined) delete process.env.BARRELMAN_API_KEY
  else process.env.BARRELMAN_API_KEY = savedApiKey
})

const MODEL = 'eiffel-tower.0123456789ab.glb'

let dir: string
beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'landmarks-'))
  writeFileSync(join(dir, 'model.glb'), 'glTF-bytes')
})
afterEach(() => rmSync(dir, { recursive: true, force: true }))

function app(tile: (z: number, x: number, y: number) => Promise<Uint8Array> = async () => new Uint8Array()) {
  const calls: number[][] = []
  const routes = createLandmarkRoutes({
    tile: async (z, x, y) => {
      calls.push([z, x, y])
      return tile(z, x, y)
    },
    modelPath: (name) => (name === MODEL ? join(dir, 'model.glb') : null),
  })
  return { app: new Elysia().use(routes), calls }
}

describe('landmark tiles', () => {
  test('serves a tile as protobuf', async () => {
    const { app: a, calls } = app(async () => new Uint8Array([0x1a, 0x02]))
    const res = await a.handle(get('/tiles/landmarks/14/8296/5636'))
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toBe('application/x-protobuf')
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
    expect(new Uint8Array(await res.arrayBuffer())).toEqual(new Uint8Array([0x1a, 0x02]))
    expect(calls).toEqual([[14, 8296, 5636]])
  })

  test('an empty tile is 204, not an error', async () => {
    const res = await app().app.handle(get('/tiles/landmarks/14/1/1'))
    expect(res.status).toBe(204)
  })

  test('accepts a format suffix on y', async () => {
    const { app: a, calls } = app()
    await a.handle(get('/tiles/landmarks/14/1/2.mvt'))
    expect(calls).toEqual([[14, 1, 2]])
  })

  test('rejects coordinates that are not integers before querying', async () => {
    const { app: a, calls } = app()
    const res = await a.handle(get('/tiles/landmarks/14/1/x'))
    expect(res.status).toBe(400)
    expect(calls).toEqual([])
  })

  test('outranks the Martin proxy for its own name', async () => {
    // Mounted the way src/index.ts mounts them: landmarks first, tiles after.
    let proxied = false
    const both = new Elysia()
      .use(createLandmarkRoutes({ tile: async () => new Uint8Array([1]) }))
      .use(createTileRoutes({ fetchTile: async () => ((proxied = true), new Response('martin')) }))
    const res = await both.handle(get('/tiles/landmarks/14/1/1'))
    expect(res.headers.get('content-type')).toBe('application/x-protobuf')
    expect(proxied).toBe(false)
  })
})

describe('landmark models', () => {
  test('serves a known model as immutable glTF', async () => {
    const res = await app().app.handle(get(`/tiles/landmarks/models/${MODEL}`))
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toBe('model/gltf-binary')
    expect(res.headers.get('cache-control')).toContain('immutable')
    expect(await res.text()).toBe('glTF-bytes')
  })

  test('404s a name it does not serve', async () => {
    const res = await app().app.handle(get('/tiles/landmarks/models/eiffel-tower.ffffffffffff.glb'))
    expect(res.status).toBe(404)
    // An import may publish that name any moment; a CDN must not keep the miss.
    expect(res.headers.get('cache-control')).toBe('no-store')
  })

  test('404s a traversal attempt', async () => {
    const res = await app().app.handle(get('/tiles/landmarks/models/..%2F..%2F.env'))
    expect(res.status).toBe(404)
  })
})

describe('auth', () => {
  test('refuses an anonymous caller once a service key is configured', async () => {
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    const { app: a, calls } = app()
    expect((await a.handle(get('/tiles/landmarks/14/1/1'))).status).toBe(401)
    expect((await a.handle(get(`/tiles/landmarks/models/${MODEL}`))).status).toBe(401)
    expect(calls).toEqual([])
  })

  test('accepts the key on the URL, as a map library sends it', async () => {
    process.env.BARRELMAN_API_KEY = 'svc_secret'
    const res = await app().app.handle(get(`/tiles/landmarks/models/${MODEL}?api_key=svc_secret`))
    expect(res.status).toBe(200)
  })
})
