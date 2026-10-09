/**
 * Bridge ways to finished decks: joined, fitted to their outlines, set on the
 * ground at the ends that meet it, and given a height every STEP metres.
 */
import {
  absorbPaths,
  along,
  anchor,
  beside,
  besideGround,
  chains,
  edgePoints,
  fitEdges,
  heightAt,
  joinNeighbours,
  lngLat,
  resample,
  smooth,
  solve,
  type Chain,
  type Point,
  type Way,
} from './profile'

/** Metres between height samples. */
export const STEP = 5

export type Ground = { load(points: Point[]): Promise<void>; at(p: Point): number }

export type DeckInput = {
  ways: Way[]
  /** Way ends (by `${x},${y}`) that meet a road or railway on the ground. */
  onGround: Set<string>
  /** man_made=bridge outlines. */
  outlines: Array<{ id: string; rings: Point[][] }>
  /** Carriageway outline points, for a road deck with no bridge outline. */
  kerbs: Point[]
  /** Mapped deck heights in metres above EGM96: on a node of a way, or on the way as a whole. */
  anchors: Array<{ way: number; point?: Point; ele: number }>
}

export type Deck = {
  id: string
  ways: number[]
  kind: Way['kind']
  layer: number
  outline: string | null
  edges: [number, number]
  grounded: [boolean, boolean]
  length: number
  /** Resampled every STEP metres; heights and ground are per point, metres above EGM96. */
  points: Point[]
  heights: number[]
  ground: number[]
}

const key = (p: Point) => `${p[0]},${p[1]}`

function inside([x, y]: Point, ring: Point[]): boolean {
  let hit = false
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [xi, yi] = ring[i]
    const [xj, yj] = ring[j]
    if (yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) hit = !hit
  }
  return hit
}

/** Gaps filled from the nearest known sample; null if there are none. */
function filled(values: number[]): number[] | null {
  const known = values.flatMap((v, i) => (Number.isNaN(v) ? [] : [i]))
  if (!known.length) return null
  return values.map((v, i) => (Number.isNaN(v) ? values[known.reduce((b, k) => (Math.abs(k - i) < Math.abs(b - i) ? k : b))] : v))
}

/** Each road deck fitted to the bridge outline it lies in, else to its kerbs. */
function fitted(decks: Chain[], outlines: DeckInput['outlines'], kerbs: Point[]): Array<Chain & { outline: string | null }> {
  const owner = new Map<Chain, string>()
  let out: Array<Chain & { outline: string | null }> = decks.map(d => ({ ...d, outline: null }))
  for (const { id, rings } of outlines) {
    const within = out.filter(d => d.kind === 'road' && !owner.has(d) && d.points.filter(p => inside(p, rings[0])).length * 2 >= d.points.length)
    if (!within.length) continue
    const fit = fitEdges(within, rings.flat())
    out = out.map(d => {
      const k = within.indexOf(d)
      if (k < 0) return d
      const deck = { ...fit[k], outline: id }
      owner.set(deck, id)
      return deck
    })
  }
  const loose = out.filter(d => d.kind === 'road' && !d.outline)
  const fit = fitEdges(loose, kerbs)
  return out.map(d => {
    const k = loose.indexOf(d)
    return k < 0 ? d : { ...fit[k], outline: null }
  })
}

export async function buildDecks(input: DeckInput, ground: Ground, toEgm96: (lng: number, lat: number) => number): Promise<Deck[]> {
  const joined = fitted(chains(input.ways), input.outlines, input.kerbs)
  const outlineOf = new Map(joined.map(c => [c.ways[0], c.outline]))
  const decks = absorbPaths(joined)
  const ends = decks.map(c => [c.points[0], c.points[c.points.length - 1]])
  // An end lands where a road on the ground meets it, rests where it meets
  // another deck, and lands at a dead end.
  const grounded = decks.map((c, k) => ends[k].map(p => {
    if (input.onGround.has(key(p))) return true
    return !decks.some((o, j) => j !== k && beside(o.points, p).distance < Math.max(1, ...o.edges))
  }) as [boolean, boolean])

  const shaped = decks.map(c => ({ chain: { ...c, points: resample(c.points, STEP) }, source: c }))
  const edges = shaped.map(({ chain }) => edgePoints(chain))
  await ground.load(shaped.flatMap(({ chain }, k) => [...chain.points, ...edges[k][0], ...edges[k][1]]))

  const solved = shaped.flatMap(({ chain, source }, k) => {
    const d = along(chain.points)
    const read = (points: Point[]) => points.map(p => ground.at(p))
    const g = filled(besideGround(read(chain.points), read(edges[k][0]), read(edges[k][1]), d))
    return g ? [{ chain, source, d, g, grounded: grounded[k], z: solve({ ...chain, grounded: grounded[k] }, g) }] : []
  })
  // An end resting on another deck takes that deck's height there; twice, so
  // a height carries through a ramp joining a ramp.
  for (let pass = 0; pass < 2; pass++)
    for (const s of solved) {
      const resting = [0, 1].map(i => {
        if (s.grounded[i]) return null
        const p = s.chain.points[i ? s.chain.points.length - 1 : 0]
        for (const o of solved) {
          if (o === s) continue
          const near = beside(o.chain.points, p)
          if (near.distance > Math.max(...o.chain.edges) + 1) continue
          return heightAt(o.d, o.z, o.d[near.segment - 1] + (o.d[near.segment] - o.d[near.segment - 1]) * near.t)
        }
        return null
      }) as [number | null, number | null]
      if (resting[0] !== null || resting[1] !== null) s.z = solve({ ...s.chain, grounded: s.grounded }, s.g, resting)
    }
  for (const s of solved) {
    s.z = smooth(s.z, s.d, s.g)
    const mapped = input.anchors.filter(a => s.source.ways.includes(a.way)).flatMap(a => {
      const p = a.point ?? midpoint(input.ways.find(w => w.id === a.way)!.points)
      const near = beside(s.chain.points, p)
      if (near.distance > 20) return []
      const at = s.d[near.segment - 1] + (s.d[near.segment] - s.d[near.segment - 1]) * near.t
      const ele = a.ele - toEgm96(...lngLat(p))
      const g = heightAt(s.d, s.g, at)
      // An `ele` below the ground or far above it is the ground's height or a typo, not the deck's.
      return ele >= g - 2 && ele <= g + 80 ? [{ at, ele }] : []
    })
    s.z = anchor(s.z, s.d, s.g, mapped, s.grounded)
  }
  joinNeighbours(solved)

  return solved.map(s => {
    const datum = s.chain.points.map(p => toEgm96(...lngLat(p)))
    const ways = [...s.source.ways].sort((a, b) => a - b)
    return {
      id: `way/${ways[0]}`,
      ways: s.source.ways,
      kind: s.chain.kind,
      layer: s.chain.layer,
      outline: outlineOf.get(s.source.ways[0]) ?? null,
      edges: s.chain.edges.map(e => Math.round(e * 100) / 100) as [number, number],
      grounded: s.grounded,
      length: Math.round(s.d[s.d.length - 1] * 100) / 100,
      points: s.chain.points,
      heights: s.z.map((z, i) => Math.round((z + datum[i]) * 100) / 100),
      ground: s.g.map((g, i) => Math.round((g + datum[i]) * 100) / 100),
    }
  })
}

const midpoint = (points: Point[]): Point => points[Math.floor(points.length / 2)]
