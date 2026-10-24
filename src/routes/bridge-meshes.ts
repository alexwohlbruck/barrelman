import Elysia, { t } from 'elysia'
import { join } from 'node:path'
import { apiAuthAfter } from '../middleware/api-auth'
import { tileAuthHandler, acceptsGzip } from './tiles'
import { createHash } from 'node:crypto'
import { connection } from '../db'
import { BRIDGE_MESH_FILE, bridgeMeshDirectory } from '../lib/bridge-mesh/files'
import { BUNDLE_ZOOM, packBundle } from '../lib/bridge-mesh/bundle'

/** The mesh files of the decks a tile meets, sorted; a deck within a twentieth of a tile of its edge counts, as clients pad a deck's bounds. */
export type DecksIn = (z: number, x: number, y: number) => Promise<string[]>
const decksIn: DecksIn = async (z, x, y) => {
  const rows: Array<{ mesh: string }> = await connection`
    WITH t AS (SELECT ST_TileEnvelope(${z}, ${x}, ${y}) AS env)
    SELECT DISTINCT mesh FROM bridge_decks, t
    WHERE geom && ST_Transform(ST_Expand(env, (ST_XMax(env) - ST_XMin(env)) * 0.05), 4326) AND mesh IS NOT NULL
    ORDER BY mesh`
  return rows.map(r => r.mesh)
}

/** Bundles held, by their contents' hash, up to this many bytes. */
const BUNDLE_CACHE_BYTES = 128 * 1024 * 1024
/** Bundles change only when decks are rebuilt: a client reuses its copy while it checks for a new one. */
const BUNDLE_CACHE_CONTROL = 'public, max-age=600, stale-while-revalidate=604800'

export function createBridgeMeshRoutes(directory = bridgeMeshDirectory(), lookup: DecksIn = decksIn) {
  const bundles = new Map<string, Uint8Array<ArrayBuffer>>()
  let held = 0
  const remember = (tag: string, body: Uint8Array<ArrayBuffer>) => {
    bundles.set(tag, body)
    held += body.byteLength
    for (const [k, v] of bundles) {
      if (held <= BUNDLE_CACHE_BYTES) break
      bundles.delete(k)
      held -= v.byteLength
    }
  }
  const bundle = async (names: string[]) => {
    const meshes = []
    for (const name of names) {
      const file = Bun.file(join(directory, `${name}.gz`))
      if (await file.exists()) meshes.push({ name, bytes: Bun.gunzipSync(new Uint8Array(await file.arrayBuffer())) })
      else if (await Bun.file(join(directory, name)).exists()) meshes.push({ name, bytes: new Uint8Array(await Bun.file(join(directory, name)).arrayBuffer()) })
    }
    return Bun.gzipSync(packBundle(meshes) as Uint8Array<ArrayBuffer>, { level: 3 })
  }

  return new Elysia({ prefix: '/tiles/bridges' })
    .onBeforeHandle(tileAuthHandler)
    .onAfterHandle(apiAuthAfter)
    .get('/tiles/:z/:x/:y', async ({ params, set, headers }) => {
      const [z, x, y] = [params.z, params.x, params.y.replace(/\.brt$/, '')].map(Number)
      if (z !== BUNDLE_ZOOM || !Number.isInteger(x) || !Number.isInteger(y) || x < 0 || y < 0 || x >= 2 ** z || y >= 2 ** z) {
        set.headers['cache-control'] = 'no-store'
        set.status = 400
        return { error: `Bridge mesh bundles are cut at zoom ${BUNDLE_ZOOM}` }
      }
      const names = (await lookup(z, x, y)).filter(name => BRIDGE_MESH_FILE.test(name))
      const tag = `"${createHash('sha256').update(names.join(',')).digest('hex').slice(0, 32)}"`
      set.headers.etag = tag
      set.headers['cache-control'] = BUNDLE_CACHE_CONTROL
      set.headers['access-control-allow-origin'] = '*'
      set.headers.vary = 'accept-encoding'
      if (headers['if-none-match'] === tag) {
        set.status = 304
        return ''
      }
      let body = bundles.get(tag)
      if (!body) remember(tag, body = await bundle(names))
      set.headers['content-type'] = 'application/octet-stream'
      if (acceptsGzip(headers['accept-encoding'])) {
        set.headers['content-encoding'] = 'gzip'
        return new Response(body, { headers: set.headers as Record<string, string> })
      }
      return new Response(Bun.gunzipSync(body as Uint8Array<ArrayBuffer>), { headers: set.headers as Record<string, string> })
    }, {
      params: t.Object({ z: t.String(), x: t.String(), y: t.String() }),
      detail: { tags: ['Tiles'], summary: 'Bridge meshes by tile', description: `Every precomputed deck mesh a zoom-${BUNDLE_ZOOM} tile meets, in one BRT1 bundle (u32 magic, u32 count, then per mesh a 32-byte sha256 name, u32 length and the BRM1 bytes, each padded to 4). Lets a client load a tile's decks alongside its vector tiles. Revalidate with the ETag.` },
    })
    .get('/models/:file', async ({ params, set, headers }) => {
      if (!BRIDGE_MESH_FILE.test(params.file)) {
        set.headers['cache-control'] = 'no-store'
        set.status = 400
        return { error: 'Invalid bridge mesh filename' }
      }
      const compressed = Bun.file(join(directory, `${params.file}.gz`))
      // Meshes baked before gzip-only storage also have the plain file.
      const plain = Bun.file(join(directory, params.file))
      const stored = await compressed.exists() ? compressed : await plain.exists() ? plain : null
      if (!stored) {
        set.headers['cache-control'] = 'no-store'
        set.status = 404
        return { error: 'Bridge mesh not found' }
      }
      const gzip = stored === compressed && acceptsGzip(headers['accept-encoding'])
      if (gzip) set.headers['content-encoding'] = 'gzip'
      set.headers.vary = 'accept-encoding'
      set.headers['content-type'] = 'application/octet-stream'
      set.headers['cache-control'] = 'public, max-age=31536000, immutable'
      set.headers['access-control-allow-origin'] = '*'
      if (gzip || stored === plain) return stored
      return new Response(Bun.gunzipSync(new Uint8Array(await compressed.arrayBuffer())), { headers: set.headers as Record<string, string> })
    }, {
      params: t.Object({ file: t.String({ pattern: '^[0-9a-f]{64}\\.brm$', description: 'Content-addressed mesh filename from bridge_decks.mesh' }) }),
      detail: { tags: ['Tiles'], summary: 'Precomputed bridge mesh', description: 'Downloads a versioned BRM1 binary mesh, including deck structure and road markings. Referenced by the mesh property of bridge_decks tiles. Built during bridge imports; no geometry is generated at request time.' },
    })
}

export const bridgeMeshRoutes = createBridgeMeshRoutes()
