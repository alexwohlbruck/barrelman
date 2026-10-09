/**
 * Bridge ways to finished decks: joined, fitted to their outlines, set on the
 * ground at the ends that meet it, lifted over what they cross, and given a
 * height every STEP metres and piers where they stand high.
 *
 * Everything comes from OSM and the terrain by rule, so a run is idempotent
 * and can be repeated whenever the map changes.
 */
import {
  absorbPaths,
  along,
  beside,
  besideGround,
  chains,
  crossings,
  edgePoints,
  fitEdges,
  heightAt,
  joinNeighbours,
  lngLat,
  resample,
  smooth,
  solve,
  type Chain,
  type Need,
  type Point,
  type Way,
} from './profile'

/** Metres between height samples. */
export const STEP = 6
/** Room a deck's underside leaves over what it crosses, plus the slab, in metres. */
export const CLEARANCE = { road: 6, rail: 8, path: 4, water: 4, deck: 6.5 } as const
/** Distance between piers, and the least height of deck over ground that has them, in metres. */
export const PIER_SPACING = 30
export const PIER_MIN = 4

export type Ground = { load(points: Point[]): Promise<void>; at(p: Point): number }

export type Crossed = { kind: 'road' | 'rail' | 'path' | 'water'; points: Point[] }

export type DeckInput = {
  ways: Way[]
  /** Way ends (by `${x},${y}`) that meet a road or railway on the ground. */
  onGround: Set<string>
  /** man_made=bridge outlines. */
  outlines: Array<{ id: string; rings: Point[][] }>
  /** Carriageway outline points, for a road deck with no bridge outline. */
  kerbs: Point[]
  /** Ways on the ground a deck may pass over. */
  crossed: Crossed[]
  /** Water areas, as outer rings. */
  water: Point[][]
  /** A wikidata id per way, where the bridge is tagged with one. */
  wikidata: Map<number, string>
}

export type Deck = {
  /** This deck, by where it lies: survives ways being split, joined or renumbered. */
  id: string
  /** The bridge it belongs to: its outline, its wikidata item, or where it lies. */
  bridge: string
  ways: number[]
  kind: Way['kind']
  layer: number
  edges: [number, number]
  grounded: [boolean, boolean]
  length: number
  /** Resampled every STEP metres; heights and ground per point, metres above sea level. */
  points: Point[]
  heights: number[]
  ground: number[]
  /** Distances along the deck in metres. */
  piers: number[]
  midpoint: Point
}

const key = (p: Point) => `${p[0]},${p[1]}`

export function inside([x, y]: Point, ring: Point[]): boolean {
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

type Fitted = Chain & { outline: string | null }

/** Each road deck fitted to the bridge outline it lies in, else to its kerbs. */
function fitted(decks: Chain[], outlines: DeckInput['outlines'], kerbs: Point[]): Fitted[] {
  let out: Fitted[] = decks.map(d => ({ ...d, outline: null }))
  for (const { id, rings } of outlines) {
    const within = out.filter(d => d.kind === 'road' && !d.outline && d.points.filter(p => inside(p, rings[0])).length * 2 >= d.points.length)
    if (!within.length) continue
    const fit = fitEdges(within, rings.flat())
    out = out.map(d => (within.includes(d) ? { ...fit[within.indexOf(d)], outline: id } : d))
  }
  const loose = out.filter(d => d.kind === 'road' && !d.outline)
  const fit = fitEdges(loose, kerbs)
  return out.map(d => (loose.includes(d) ? { ...fit[loose.indexOf(d)], outline: null } : d))
}

const bearing = (points: Point[]) => {
  const [a, b] = [points[0], points[points.length - 1]]
  const deg = (Math.atan2(b[0] - a[0], -(b[1] - a[1])) * 180) / Math.PI
  return (deg + 360) % 180
}

/** A key for a place and heading, coarse enough to survive a way being redrawn. */
const placeKey = (p: Point, heading: number, digits: number) => {
  const [lng, lat] = lngLat(p)
  return `${lng.toFixed(digits)},${lat.toFixed(digits)}@${Math.round(heading / 15) % 12}`
}

type Solved = { chain: Fitted; d: number[]; g: number[]; z: number[]; grounded: [boolean, boolean]; needs: Need[] }

type Box = [number, number, number, number]
const boxOf = (points: Point[]): Box => {
  let [x0, y0, x1, y1] = [Infinity, Infinity, -Infinity, -Infinity]
  for (const [x, y] of points) [x0, y0, x1, y1] = [Math.min(x0, x), Math.min(y0, y), Math.max(x1, x), Math.max(y1, y)]
  return [x0, y0, x1, y1]
}
const meet = (a: Box, b: Box) => a[0] <= b[2] && b[0] <= a[2] && a[1] <= b[3] && b[1] <= a[3]
const boxes = new WeakMap<Point[], Box>()
const box = (points: Point[]) => boxes.get(points) ?? (boxes.set(points, boxOf(points)), boxes.get(points)!)

/** What a deck passes over, as heights its surface must reach. */
function needs(s: Pick<Solved, 'chain' | 'd' | 'g'>, input: DeckInput, below: Solved[]): Need[] {
  const out: Need[] = []
  const reach = Math.max(...s.chain.edges)
  const own = box(s.chain.points)
  for (const c of input.crossed)
    if (meet(own, box(c.points)))
      for (const at of crossings(s.chain.points, c.points, reach)) out.push({ at, height: heightAt(s.d, s.g, at) + CLEARANCE[c.kind] })
  for (const lower of below.filter(o => meet(own, box(o.chain.points))))
    for (const at of crossings(s.chain.points, lower.chain.points, reach)) {
      const p = s.chain.points[Math.min(s.chain.points.length - 1, Math.round(at / STEP))]
      const near = beside(lower.chain.points, p)
      const z = heightAt(lower.d, lower.z, lower.d[near.segment - 1] + (lower.d[near.segment] - lower.d[near.segment - 1]) * near.t)
      out.push({ at, height: z + CLEARANCE.deck })
    }
  const water = input.water.filter(ring => meet(own, box(ring)))
  if (water.length)
    s.chain.points.forEach((p, i) => {
      if (water.some(ring => inside(p, ring))) out.push({ at: s.d[i], height: s.g[i] + CLEARANCE.water })
    })
  return out
}

/**
 * Piers every PIER_SPACING metres where a deck stands high. A deck running
 * beside one that already has piers takes the same row, so twin carriageways
 * stand on shared bents rather than two staggered sets.
 */
function piers(solved: Solved[]): number[][] {
  const out = solved.map(() => [] as number[])
  const order = solved.map((_, k) => k).sort((a, b) => solved[b].d[solved[b].d.length - 1] - solved[a].d[solved[a].d.length - 1])
  const high = (s: Solved, at: number) => heightAt(s.d, s.z, at) - heightAt(s.d, s.g, at) >= PIER_MIN
  for (const [n, k] of order.entries()) {
    const s = solved[k]
    const total = s.d[s.d.length - 1]
    const shared: number[] = []
    for (const j of order.slice(0, n)) {
      const o = solved[j]
      if (o.chain.kind === 'rail' || s.chain.kind === 'rail') continue
      for (const at of out[j]) {
        const p = o.chain.points[Math.min(o.chain.points.length - 1, Math.round(at / STEP))]
        const near = beside(s.chain.points, p)
        if (!near.alongside || near.distance > Math.max(...s.chain.edges) + Math.max(...o.chain.edges) + 3) continue
        const mine = s.d[near.segment - 1] + (s.d[near.segment] - s.d[near.segment - 1]) * near.t
        if (high(s, mine)) shared.push(mine)
      }
    }
    for (let at = PIER_SPACING / 2; at < total; at += PIER_SPACING)
      if (high(s, at) && !shared.some(x => Math.abs(x - at) < PIER_SPACING * 0.75)) shared.push(at)
    out[k] = shared.sort((a, b) => a - b).map(x => Math.round(x * 10) / 10)
  }
  return out
}

export async function buildDecks(input: DeckInput, ground: Ground): Promise<Deck[]> {
  const decks = absorbPaths(fitted(chains(input.ways), input.outlines, input.kerbs)) as Fitted[]
  // An end lands where a road on the ground meets it, rests where it meets
  // another deck, and lands at a dead end.
  const grounded = decks.map((c, k) => [c.points[0], c.points[c.points.length - 1]].map(p => {
    if (input.onGround.has(key(p))) return true
    return !decks.some((o, j) => j !== k && beside(o.points, p).distance < Math.max(1, ...o.edges))
  }) as [boolean, boolean])

  const shaped = decks.map(c => ({ ...c, points: resample(c.points, STEP) }))
  const edges = shaped.map(edgePoints)
  await ground.load(shaped.flatMap((c, k) => [...c.points, ...edges[k][0], ...edges[k][1]]))

  // Lower layers first, so a deck over another clears it.
  const solved: Solved[] = []
  for (const k of shaped.map((_, k) => k).sort((a, b) => shaped[a].layer - shaped[b].layer)) {
    const chain = shaped[k]
    const d = along(chain.points)
    const read = (points: Point[]) => points.map(p => ground.at(p))
    const g = filled(besideGround(read(chain.points), read(edges[k][0]), read(edges[k][1]), d))
    if (!g) continue
    const need = needs({ chain, d, g }, input, solved.filter(o => o.chain.layer < chain.layer))
    solved.push({ chain, d, g, grounded: grounded[k], needs: need, z: solve({ ...chain, grounded: grounded[k] }, g, need) })
  }
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
      if (resting[0] !== null || resting[1] !== null) s.z = solve({ ...s.chain, grounded: s.grounded }, s.g, s.needs, resting)
    }
  for (const s of solved) s.z = smooth(s.z, s.d, s.g)
  joinNeighbours(solved)
  const rows = piers(solved)

  const wiki = (s: Solved) => s.chain.ways.map(w => input.wikidata.get(w)).find(Boolean)
  const used = new Map<string, number>()
  return solved.map((s, k) => {
    const midpoint = s.chain.points[Math.floor(s.chain.points.length / 2)]
    const heading = bearing(s.chain.points)
    const place = `${placeKey(midpoint, heading, 4)}/${s.chain.layer}`
    const n = used.get(place) ?? 0
    used.set(place, n + 1)
    const round = (v: number) => Math.round(v * 100) / 100
    return {
      id: n ? `${place}/${n}` : place,
      bridge: s.chain.outline ?? (wiki(s) ? `wikidata/${wiki(s)}` : `at/${placeKey(midpoint, heading, 3)}`),
      ways: s.chain.ways,
      kind: s.chain.kind,
      layer: s.chain.layer,
      edges: s.chain.edges.map(round) as [number, number],
      grounded: s.grounded,
      length: round(s.d[s.d.length - 1]),
      points: s.chain.points,
      heights: s.z.map(round),
      ground: s.g.map(round),
      piers: rows[k],
      midpoint,
    }
  })
}
