import { LRUCache } from 'lru-cache'
import { connection } from '../../db'
import { mercator, metresPerUnit, unprojected, type Bounds } from '../bridge-mesh/decks'
import { Dem, DEM_ZOOM } from '../bridge-decks/dem'
import { tunnelsIn } from '../tunnel-cuts/way-rows'
import { deckSource, settledTunnels, tunnelSources, TUNNEL_MARGIN, type DeckRow, type HeightSource } from './sources'

/** The terrain tunnels are settled under, kept across tiles: a decoded tile is about 1 MB. */
const dem = new Dem(undefined, { zoom: DEM_ZOOM, minZoom: DEM_ZOOM - 5, capacity: 48, concurrency: 8 })

/** The decks and tunnels lines in a tile may ride. */
export type LineSources = (z: number, x: number, y: number) => Promise<HeightSource[]>

/** Metres past a tile its decks are read: as far as a line in its buffer reaches. */
const DECK_MARGIN = 60

/** Mercator `bounds` grown by `metres`, and the same box in degrees. */
export function grown(bounds: Bounds, metres: number): { bounds: Bounds; degrees: [number, number, number, number] } {
  const pad = metres / metresPerUnit((bounds.minY + bounds.maxY) / 2)
  const out = { minX: bounds.minX - pad, minY: bounds.minY - pad, maxX: bounds.maxX + pad, maxY: bounds.maxY + pad }
  const [w, north] = unprojected([out.minX, out.minY])
  const [e, south] = unprojected([out.maxX, out.maxY])
  return { bounds: out, degrees: [w, south, e, north] }
}

export function tileBounds(z: number, x: number, y: number): Bounds {
  const n = 2 ** z
  return { minX: x / n, minY: y / n, maxX: (x + 1) / n, maxY: (y + 1) / n }
}

/** The decks and tunnels a line within `box` may ride. */
export async function sourcesIn(box: Bounds): Promise<HeightSource[]> {
  const decks = grown(box, DECK_MARGIN).degrees
  const tunnels = grown(box, TUNNEL_MARGIN)
  const [w, s, e, n] = tunnels.degrees
  const [deckRows, ways, cuts] = await Promise.all([
    connection<DeckRow[]>`
      SELECT ways::text[] AS ways, kind, edges, heights, ground, step, ST_AsGeoJSON(geom)::json->'coordinates' AS coords
      FROM bridge_decks WHERE geom && ST_MakeEnvelope(${decks[0]}, ${decks[1]}, ${decks[2]}, ${decks[3]}, 4326)`,
    tunnelsIn(connection, tunnels.degrees, { underground: true }),
    connection<Array<{ portal: number[] }>>`
      SELECT ST_AsGeoJSON(ST_PointN(geom, at + 1))::json->'coordinates' AS portal
      FROM tunnel_cuts WHERE geom && ST_MakeEnvelope(${w}, ${s}, ${e}, ${n}, 4326)`,
  ])
  const fromDecks = deckRows.map(row => deckSource(row, mercator)).filter((s): s is HeightSource => s !== null)
  const portals = cuts.flatMap(c => (c.portal ? [mercator(c.portal)] : []))
  const sunk = tunnelSources(ways, tunnels.bounds, portals)
  const points = sunk.flatMap(t => t.points)
  const loaded = await dem.load(points).then(() => true, error => (console.warn('[line elevation] terrain:', error), false))
  return [...fromDecks, ...(loaded ? settledTunnels(sunk, p => dem.at(p)) : sunk)]
}

export const readLineSources: LineSources = (z, x, y) => sourcesIn(tileBounds(z, x, y))

/** Sources by tile, read once however many ask at the same time, and kept a while. */
export function cachedLineSources(read: LineSources, { max = 2000, ttl = 600_000 } = {}): LineSources {
  const cache = new LRUCache<string, HeightSource[]>({ max, ttl })
  const pending = new Map<string, Promise<HeightSource[]>>()
  return (z, x, y) => {
    const key = `${z}/${x}/${y}`
    const hit = cache.get(key)
    if (hit) return Promise.resolve(hit)
    let job = pending.get(key)
    if (!job) {
      job = read(z, x, y).then(sources => {
        cache.set(key, sources)
        return sources
      }).finally(() => pending.delete(key))
      pending.set(key, job)
    }
    return job
  }
}
