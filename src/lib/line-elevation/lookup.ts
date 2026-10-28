import { LRUCache } from 'lru-cache'
import { connection } from '../../db'
import { mercator, metresPerUnit, unprojected, type Bounds } from '../bridge-mesh/decks'
import { tunnelsIn } from '../tunnel-cuts/way-rows'
import { deckSource, tunnelSources, TUNNEL_MARGIN, type DeckRow, type HeightSource } from './sources'

/** The decks and tunnels lines in a tile may ride. */
export type LineSources = (z: number, x: number, y: number) => Promise<HeightSource[]>

/** Metres past a tile its decks are read: as far as a line in its buffer reaches. */
const DECK_MARGIN = 60

/** A tile's mercator bounds grown by `metres`, and the same box in degrees. */
export function tileBox(z: number, x: number, y: number, metres: number): { bounds: Bounds; degrees: [number, number, number, number] } {
  const n = 2 ** z
  const pad = metres / metresPerUnit((y + 0.5) / n)
  const bounds = { minX: x / n - pad, minY: y / n - pad, maxX: (x + 1) / n + pad, maxY: (y + 1) / n + pad }
  const [w, north] = unprojected([bounds.minX, bounds.minY])
  const [e, south] = unprojected([bounds.maxX, bounds.maxY])
  return { bounds, degrees: [w, south, e, north] }
}

export const readLineSources: LineSources = async (z, x, y) => {
  const decks = tileBox(z, x, y, DECK_MARGIN).degrees
  const tunnels = tileBox(z, x, y, TUNNEL_MARGIN)
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
  return [...fromDecks, ...tunnelSources(ways, tunnels.bounds, portals)]
}

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
