import Elysia, { t } from 'elysia'
import { join } from 'node:path'
import { apiAuthAfter } from '../middleware/api-auth'
import { tileAuthHandler, acceptsGzip } from './tiles'
import { BRIDGE_MESH_FILE, bridgeMeshDirectory } from '../lib/bridge-mesh/files'

export function createBridgeMeshRoutes(directory = bridgeMeshDirectory()) {
  return new Elysia({ prefix: '/tiles/bridges/models' })
    .onBeforeHandle(tileAuthHandler)
    .onAfterHandle(apiAuthAfter)
    .get('/:file', async ({ params, set, headers }) => {
      if (!BRIDGE_MESH_FILE.test(params.file)) {
        set.headers['cache-control'] = 'no-store'
        set.status = 400
        return { error: 'Invalid bridge mesh filename' }
      }
      const file = Bun.file(join(directory, params.file))
      if (!(await file.exists())) {
        set.headers['cache-control'] = 'no-store'
        set.status = 404
        return { error: 'Bridge mesh not found' }
      }
      const compressed = Bun.file(join(directory, `${params.file}.gz`))
      const gzip = acceptsGzip(headers['accept-encoding']) && await compressed.exists()
      if (gzip) set.headers['content-encoding'] = 'gzip'
      set.headers.vary = 'accept-encoding'
      set.headers['content-type'] = 'application/octet-stream'
      set.headers['cache-control'] = 'public, max-age=31536000, immutable'
      set.headers['access-control-allow-origin'] = '*'
      return gzip ? compressed : file
    }, {
      params: t.Object({ file: t.String({ pattern: '^[0-9a-f]{64}\\.brm$', description: 'Content-addressed mesh filename from bridge_decks.mesh' }) }),
      detail: { tags: ['Tiles'], summary: 'Precomputed bridge mesh', description: 'Downloads a versioned BRM1 binary mesh, including deck structure and road markings. Referenced by the mesh property of bridge_decks tiles. Built during bridge imports; no geometry is generated at request time.' },
    })
}

export const bridgeMeshRoutes = createBridgeMeshRoutes()
