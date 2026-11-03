import Elysia, { t } from 'elysia'
import { join } from 'node:path'
import { apiAuthAfter } from '../middleware/api-auth'
import { tileAuthHandler, acceptsGzip } from './tiles'
import { createHash } from 'node:crypto'
import { BRIDGE_MESH_FILE, bridgeMeshDirectory } from '../lib/bridge-mesh/files'
import { decksIn, type DecksIn } from '../lib/bridge-mesh/deck-lookup'

/** The one zoom deck indexes are cut at: the lowest the bridge_decks tiles carry. */
export const INDEX_ZOOM = 14

/** An index changes only when decks are rebuilt: a client reuses its copy while it checks for a new one. */
const INDEX_CACHE_CONTROL = 'public, max-age=600, stale-while-revalidate=604800'

export function createBridgeMeshRoutes(directory = bridgeMeshDirectory(), lookup: DecksIn = decksIn) {
  return new Elysia({ prefix: '/tiles/bridges' })
    .onBeforeHandle(tileAuthHandler)
    .onAfterHandle(apiAuthAfter)
    .get('/index/:z/:x/:y', async ({ params, set, headers }) => {
      const [z, x, y] = [params.z, params.x, params.y.replace(/\.json$/, '')].map(Number)
      if (z !== INDEX_ZOOM || !Number.isInteger(x) || !Number.isInteger(y) || x < 0 || y < 0 || x >= 2 ** z || y >= 2 ** z) {
        set.headers['cache-control'] = 'no-store'
        set.status = 400
        return { error: `Bridge deck indexes are cut at zoom ${INDEX_ZOOM}` }
      }
      const decks = (await lookup(z, x, y)).filter(([file]) => BRIDGE_MESH_FILE.test(file))
      const body = JSON.stringify({ decks: decks.map(([file, ...box]) => [file, ...box.map(v => Math.round(v * 1e7) / 1e7)]) })
      const tag = `"${createHash('sha256').update(body).digest('hex').slice(0, 32)}"`
      set.headers.etag = tag
      set.headers['cache-control'] = INDEX_CACHE_CONTROL
      set.headers['access-control-allow-origin'] = '*'
      // A CDN that compresses the body weakens the tag it passes back (W/"…").
      if (headers['if-none-match']?.split(',').some(t => t.trim().replace(/^W\//, '') === tag)) {
        set.status = 304
        return ''
      }
      set.headers['content-type'] = 'application/json'
      return body
    }, {
      params: t.Object({ z: t.String(), x: t.String(), y: t.String() }),
      detail: { tags: ['Tiles'], summary: 'Bridge deck mesh index by tile', description: `The precomputed deck meshes a zoom-${INDEX_ZOOM} tile meets, as \`{ decks: [[file, west, south, east, north], ...] }\` with each deck's bounds in degrees. Lets a client fetch the meshes a view needs alongside its vector tiles rather than after them. Revalidate with the ETag.` },
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
