import Elysia, { t } from 'elysia'
import { join } from 'node:path'
import { apiAuthAfter } from '../middleware/api-auth'
import { tileAuthHandler, acceptsGzip } from './tiles'
import { createHash } from 'node:crypto'
import { connection } from '../db'
import { BRIDGE_MESH_FILE, bridgeMeshDirectory } from '../lib/bridge-mesh/files'

/** The one zoom deck indexes are cut at: the lowest the bridge_decks tiles carry. */
export const INDEX_ZOOM = 14

/** A deck mesh a tile meets: its file, and its deck's bounds in degrees. */
export type IndexedDeck = [file: string, west: number, south: number, east: number, north: number]
export type DecksIn = (z: number, x: number, y: number) => Promise<IndexedDeck[]>

/** The meshed decks a tile meets, a tenth of a tile beyond its edge included, as clients pad a deck's bounds by 100 m. */
const decksIn: DecksIn = async (z, x, y) => {
  const rows: Array<{ mesh: string; w: number; s: number; e: number; n: number }> = await connection`
    WITH t AS (SELECT ST_TileEnvelope(${z}, ${x}, ${y}) AS env)
    SELECT mesh, ST_XMin(geom) AS w, ST_YMin(geom) AS s, ST_XMax(geom) AS e, ST_YMax(geom) AS n FROM bridge_decks, t
    WHERE geom && ST_Transform(ST_Expand(env, (ST_XMax(env) - ST_XMin(env)) * 0.1), 4326) AND mesh IS NOT NULL
    ORDER BY mesh`
  return rows.map(r => [r.mesh, r.w, r.s, r.e, r.n])
}

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
      if (headers['if-none-match'] === tag) {
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
