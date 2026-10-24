import { connection } from '../../db'
import { BRIDGE_MESH_FILE } from './files'

/** A deck mesh a tile meets: its file, and its deck's bounds in degrees. */
export type IndexedDeck = [file: string, west: number, south: number, east: number, north: number]
export type DecksIn = (z: number, x: number, y: number) => Promise<IndexedDeck[]>

/** The meshed decks a tile meets, a tenth of a tile beyond its edge included, as clients pad a deck's bounds by 100 m. */
export const decksIn: DecksIn = async (z, x, y) => {
  const rows: Array<{ mesh: string; w: number; s: number; e: number; n: number }> = await connection`
    WITH t AS (SELECT ST_TileEnvelope(${z}, ${x}, ${y}) AS env)
    SELECT mesh, ST_XMin(geom) AS w, ST_YMin(geom) AS s, ST_XMax(geom) AS e, ST_YMax(geom) AS n FROM bridge_decks, t
    WHERE geom && ST_Transform(ST_Expand(env, (ST_XMax(env) - ST_XMin(env)) * 0.1), 4326) AND mesh IS NOT NULL
    ORDER BY mesh`
  return rows.map(r => [r.mesh, r.w, r.s, r.e, r.n])
}

/** The mesh files of the decks a tile meets. */
export const deckFilesIn = async (z: number, x: number, y: number) => (await decksIn(z, x, y)).map(([file]) => file).filter(file => BRIDGE_MESH_FILE.test(file))

