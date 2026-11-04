/**
 * TileJSON for our tile sources, with tile URLs that point at this API.
 *
 * Martin already serves a TileJSON per source (`GET /<source>`), with the zoom
 * range, bounds and `vector_layers` it read from the table or archive. That
 * document names Martin's own internal address, though, and carries whatever
 * attribution the archive was built with. So it is fetched, kept briefly, and
 * reissued with our URLs and the credits from config/tile-attribution.ts.
 */

import { serverOrigin } from '../config/accounts.config'
import { tileAttribution } from '../config/tile-attribution'
import { resolveTileBundle } from '../config/tile-bundles'
import { LANDMARKS_LAYER, LANDMARKS_MIN_TILE_ZOOM } from './landmarks.service'

export interface TileJson {
  tilejson: '3.0.0'
  name: string
  tiles: string[]
  attribution: string
  minzoom?: number
  maxzoom?: number
  bounds?: number[]
  center?: number[]
  vector_layers?: unknown[]
  description?: string
}

/** The parts of Martin's TileJSON we carry over. Its tile URLs and attribution are replaced. */
type MartinTileJson = Partial<Pick<TileJson, 'minzoom' | 'maxzoom' | 'bounds' | 'center' | 'vector_layers'>>

export type MartinFetcher = (url: string) => Promise<Response>

/**
 * The key to write into tile URLs, or null for none.
 *
 * Only a key the caller already put in this request's URL is echoed. A Bearer
 * header is a secret the caller kept out of URLs on purpose, and a TileJSON is
 * cached, logged and shared like any other public document, so writing that
 * key into one would publish it. A caller who authenticates by header adds
 * the key to tile requests itself.
 */
export function urlKeyFor(
  headers: Record<string, string | undefined>,
  query: Record<string, string | undefined>,
): string | null {
  if (headers['authorization']) return null
  const key = query.api_key || query.token
  return key ? key : null
}

/** `?api_key=…` for a key, escaped, or nothing. */
export function keyQuery(key: string | null): string {
  return key ? `?api_key=${encodeURIComponent(key)}` : ''
}

/**
 * Martin's TileJSON changes only when a source is added or rebuilt, and a map
 * fetches it once per load, so a minute in memory is plenty and spares Martin
 * one request per map view.
 */
const CACHE_TTL_MS = 60_000

interface CacheEntry {
  at: number
  json: MartinTileJson
}

export class TileJsonError extends Error {
  constructor(readonly status: number, message: string) {
    super(message)
  }
}

/** Landmarks are drawn by barrelman itself rather than Martin, so their TileJSON is written here. */
function landmarkTileJson(): MartinTileJson & Pick<TileJson, 'description'> {
  return {
    minzoom: LANDMARKS_MIN_TILE_ZOOM,
    // Placements are points, so a client over-zooms the z16 tile rather than
    // paying for deeper tiles that carry the same features.
    maxzoom: 16,
    bounds: [-180, -85.0511, 180, 85.0511],
    vector_layers: [{ id: LANDMARKS_LAYER, minzoom: LANDMARKS_MIN_TILE_ZOOM, maxzoom: 16, fields: {} }],
    description:
      'Each placement carries an `attribution` property. Where it is set, show it while that model is drawn.',
  }
}

export function createTileJsonService(deps: { fetchMartin: MartinFetcher; martinUrl: () => string; now?: () => number }) {
  const now = deps.now ?? Date.now
  const cache = new Map<string, CacheEntry>()

  async function martinTileJson(martinSource: string): Promise<MartinTileJson> {
    const hit = cache.get(martinSource)
    if (hit && now() - hit.at < CACHE_TTL_MS) return hit.json

    const response = await deps.fetchMartin(`${deps.martinUrl()}/${martinSource}`)
    if (!response.ok) {
      throw new TileJsonError(response.status === 404 ? 404 : 502, response.status === 404 ? 'Unknown tile source' : `TileJSON fetch failed: ${response.statusText}`)
    }
    const body = (await response.json()) as Record<string, unknown>
    const json: MartinTileJson = {}
    for (const field of ['minzoom', 'maxzoom', 'bounds', 'center', 'vector_layers'] as const) {
      if (body[field] !== undefined) (json as Record<string, unknown>)[field] = body[field]
    }
    cache.set(martinSource, { at: now(), json })
    return json
  }

  /**
   * TileJSON for a public source name: one Martin source, a bundle, a
   * comma-joined composite, or `landmarks`.
   */
  async function tileJson(source: string, key: string | null, origin: string = serverOrigin): Promise<TileJson> {
    const upstream = source === 'landmarks' ? landmarkTileJson() : await martinTileJson(resolveTileBundle(source))
    return {
      tilejson: '3.0.0',
      name: source,
      tiles: [`${origin}/tiles/${source}/{z}/{x}/{y}${keyQuery(key)}`],
      attribution: tileAttribution(source),
      ...upstream,
    }
  }

  return { tileJson, clear: () => cache.clear() }
}
