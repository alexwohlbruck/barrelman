/**
 * Decks and tunnel cuts as their tiles serve them (`bridge_deck_tiles`,
 * `tunnel_cut_tiles`): lines as steps in 1e-7°, profiles in decimetres.
 */
import type { Deck } from '../bridge-decks/build'
import { mercator, type Point } from '../bridge-decks/profile'

type Properties = Record<string, string | number | boolean>

export function parseLine(line: string): Point[] {
  let x = 0, y = 0
  return line.split(';').map(pair => {
    const [dx, dy] = pair.split(',').map(Number)
    x += dx; y += dy
    return mercator(x / 1e7, y / 1e7)
  })
}

export const parseProfile = (values: string): number[] => values.split(',').map(v => Number(v) / 10)

export function servedDeck(p: Properties): Deck | null {
  const points = parseLine(String(p.line))
  const heights = parseProfile(String(p.heights))
  const ground = parseProfile(String(p.ground))
  if (points.length < 2 || heights.length !== points.length || ground.length !== points.length) return null
  const sides = p.left_edges && p.right_edges ? [parseProfile(String(p.left_edges)), parseProfile(String(p.right_edges))] as [number[], number[]] : null
  return {
    id: String(p.id), bridge: String(p.bridge), ways: [], kind: p.kind as Deck['kind'], layer: Number(p.layer),
    edges: [Number(p.left_edge), Number(p.right_edge)], sides,
    caps: p.caps ? parseProfile(String(p.caps)) as Deck['caps'] : null,
    grounded: [p.start_grounded === true, p.end_grounded === true],
    length: Number(p.length), points, heights, ground,
    piers: p.piers ? parseProfile(String(p.piers)) : [], midpoint: points[points.length >> 1],
  }
}

export type ServedCut = {
  id: string
  kind: 'road' | 'path' | 'rail'
  points: Point[]
  at: number
  open: number
  floor: number[]
  walls: [number[], number[]]
  crown: number
  headroom: number
}

export function servedCut(p: Properties): ServedCut | null {
  const points = parseLine(String(p.line))
  const floor = parseProfile(String(p.floor))
  const walls = [parseProfile(String(p.wall_left)), parseProfile(String(p.wall_right))] as [number[], number[]]
  if (floor.length !== points.length || walls.some(w => w.length !== points.length)) return null
  return { id: String(p.id), kind: p.kind as ServedCut['kind'], points, at: Number(p.at), open: Number(p.open), floor, walls, crown: Number(p.crown), headroom: Number(p.headroom) }
}
