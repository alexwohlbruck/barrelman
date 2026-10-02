import Elysia, { t } from 'elysia'
import { apiAuthAfter } from '../middleware/api-auth'
import { tileAuthHandler } from './tiles'
import { landmarkTile, modelPath } from '../services/landmarks.service'

const Z_RE = /^\d{1,2}$/
const XY_RE = /^\d{1,10}$/
const Y_RE = /^\d{1,10}(\.[A-Za-z0-9]+)?$/

const CORS = 'access-control-allow-origin'

/**
 * Placement tiles are a few hundred bytes and change whenever the catalog
 * does — a new model is a new file name inside them — so they are kept
 * briefly, unlike the immutable models they point at.
 */
const TILE_CACHE = 'public, max-age=300, stale-while-revalidate=3600'

/**
 * 3D landmarks, as a tile layer plus the models it points at.
 *
 * Under /tiles, and metered as tiles: a map pulls both on the same terms as
 * the rest of its sources. Registered as its own instance, but the static
 * `landmarks` segment outranks the Martin proxy's `/:source`, so the two
 * never compete for a path.
 */
export function createLandmarkRoutes(
  deps: {
    tile?: (z: number, x: number, y: number) => Promise<Uint8Array>
    modelPath?: (name: string) => string | null
  } = {},
) {
  const tile = deps.tile ?? landmarkTile
  const pathFor = deps.modelPath ?? modelPath

  return new Elysia({ prefix: '/tiles/landmarks' })
    .onBeforeHandle(tileAuthHandler)
    .onAfterHandle(apiAuthAfter)
    .get(
      '/:z/:x/:y',
      async ({ params, set }) => {
        const { z, x, y } = params
        if (!Z_RE.test(z) || !XY_RE.test(x) || !Y_RE.test(y)) {
          set.status = 400
          return { error: 'Invalid tile coordinates' }
        }
        const bytes = await tile(Number(z), Number(x), Number(y.replace(/\.[A-Za-z0-9]+$/, '')))
        // An empty tile is the common answer — most of the world has no
        // landmark — and 204 is how MapLibre is told so without logging it.
        if (!bytes.length) {
          return new Response(null, {
            status: 204,
            headers: { 'cache-control': TILE_CACHE, [CORS]: '*' },
          })
        }
        return new Response(bytes as Uint8Array<ArrayBuffer>, {
          headers: {
            'content-type': 'application/x-protobuf',
            'cache-control': TILE_CACHE,
            [CORS]: '*',
          },
        })
      },
      {
        params: t.Object({
          z: t.String({ description: 'Zoom level' }),
          x: t.String({ description: 'Tile X coordinate' }),
          y: t.String({ description: 'Tile Y coordinate (optionally suffixed, e.g. "42.mvt")' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: '3D landmark placements',
          description:
            'Vector tile with one layer, `landmarks`: a point per 3D landmark whose model overhangs the tile. ' +
            'Properties: `id`, `name`, `model` (a file name under /tiles/landmarks/models), `bearing` (degrees ' +
            'clockwise from north), `scale`, `elevation` and `height` (metres), `minzoom`, `wikidata`, ' +
            '`attribution` (a credit to show while the model is drawn, where its licence asks for one), and ' +
            '`replaces` — space-separated OSM refs (`way/5013364`) of the buildings and building parts the ' +
            'model stands in for, which a client should stop extruding. Empty below zoom 12, and 204 where ' +
            'there are no landmarks.',
        },
      },
    )
    .get(
      '/models/:file',
      async ({ params, set }) => {
        const path = pathFor(params.file)
        const file = path ? Bun.file(path) : null
        if (!file || !(await file.exists())) {
          set.status = 404
          return { error: 'Model not found' }
        }
        // The name carries a content hash, so a URL never changes meaning and
        // can be cached for good. Elysia drops the type Bun.file() would infer
        // when a bare Response is returned, so it is set explicitly.
        return new Response(file, {
          headers: {
            'content-type': 'model/gltf-binary',
            'cache-control': 'public, max-age=31536000, immutable',
            [CORS]: '*',
          },
        })
      },
      {
        params: t.Object({
          file: t.String({ description: 'Model file name from a landmark tile, e.g. "eiffel-tower.3fa9c2d1e0b4.glb"' }),
        }),
        detail: {
          tags: ['Tiles'],
          summary: '3D landmark model',
          description:
            'A landmark model as binary glTF. Y up, -Z north, +X east, in metres, with the origin at the ' +
            'landmark\'s anchor on the ground — place it with the tile feature\'s position, `bearing` and ' +
            '`scale` and nothing else. Names are content-addressed and served as immutable.',
        },
      },
    )
}

export const landmarkRoutes = createLandmarkRoutes()
