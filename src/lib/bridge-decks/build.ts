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
  box,
  chains,
  covers,
  crossings,
  edgePoints,
  fitEdges,
  grow,
  heightAt,
  inside,
  joinNeighbours,
  lngLat,
  meet,
  resample,
  smooth,
  solve,
  steady,
  widen,
  type Chain,
  type Need,
  type Point,
  type Way,
} from './profile'
import { inOutline, shapeDecks, type Outline } from './shape'

/** Metres between height samples. */
export const STEP = 6
/** What a row holds: 2 adds each side's edge at every sample, and its end caps, where a deck follows its outline. */
export const FORMAT = 2
/** Room a deck's underside leaves over what it crosses, plus the slab, in metres. */
export const CLEARANCE = { road: 6, rail: 8, path: 4, water: 4, deck: 6.5 } as const
/** Distance between piers, and the least height of deck over ground that has them, in metres. */
export const PIER_SPACING = 30
export const PIER_MIN = 4
/** Rounds of settling decks against the decks they rest on and run beside. */
const SETTLE = 3
/** Most that easing moves two joined decks apart, in metres. */
const EASED = 0.5
/**
 * Decks whose terrain is loaded at once. Few enough that their tiles fit the
 * terrain cache, so a dense cell never holds every tile it touches.
 */
const GROUND_BATCH = 16

export type Ground = { load(points: Point[]): Promise<void>; at(p: Point): number }

export type Crossed = { kind: 'road' | 'rail' | 'path' | 'water'; points: Point[] }

export type DeckInput = {
  ways: Way[]
  /** Way ends (by `${x},${y}`) that meet a road or railway on the ground. */
  onGround: Set<string>
  /** man_made=bridge outlines. */
  outlines: Outline[]
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
  /** The widest each side reaches; `sides` gives each sample's where the deck follows an outline. */
  edges: [number, number]
  sides: [number[], number[]] | null
  /** Metres each side stops short of the start and end, [start left, start right, end left, end right]; negative runs on past. */
  caps: [number, number, number, number] | null
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

/** Gaps filled from the nearest known sample; null if there are none. */
function filled(values: number[]): number[] | null {
  const known = values.flatMap((v, i) => (Number.isNaN(v) ? [] : [i]))
  if (!known.length) return null
  return values.map((v, i) => (Number.isNaN(v) ? values[known.reduce((b, k) => (Math.abs(k - i) < Math.abs(b - i) ? k : b))] : v))
}

type Fitted = Chain & { outline: string | null; caps?: [number, number, number, number] }

/**
 * Each road deck named for the bridge outline it lies in, which shapes it
 * later, and fitted to that outline meanwhile (`fit`); a road deck in none
 * fitted to its kerbs instead. Rail and path decks keep their own width.
 */
function fitted(decks: Chain[], outlines: Outline[], kerbs: Point[]): Fitted[] {
  let out: Fitted[] = decks.map(d => ({ ...d, outline: null }))
  for (const { id, rings } of outlines) {
    const mine = out.filter(d => d.kind === 'road' && !d.outline && inOutline(d.points, rings))
    if (!mine.length) continue
    const fit = fitEdges(mine, rings.flat(), decks)
    out = out.map(d => (mine.includes(d) ? { ...d, fit: fit[mine.indexOf(d)].edges, outline: id } : d))
  }
  const loose = out.filter(d => d.kind === 'road' && !d.outline)
  const fit = fitEdges(loose, kerbs, decks)
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

/**
 * Ids from place keys, with decks that share one numbered by exactly where
 * they lie, then by their ways, rather than by build order. Neighbouring cells are built from
 * different sets of ways, and an id that followed build order could name
 * different decks in each, so one cell's write would replace the other's deck.
 */
export function numbered(places: string[], midpoints: Point[], ways: number[][]): string[] {
  const groups = new Map<string, number[]>()
  places.forEach((p, k) => groups.set(p, [...(groups.get(p) ?? []), k]))
  const sorted = ways.map(w => [...w].sort((a, b) => a - b))
  const byWays = (a: number[], b: number[]) => {
    for (let i = 0; i < Math.min(a.length, b.length); i++) if (a[i] !== b[i]) return a[i] - b[i]
    return a.length - b.length
  }
  const ids = [...places]
  for (const [place, members] of groups) {
    if (members.length < 2) continue
    members.sort((a, b) => midpoints[a][0] - midpoints[b][0] || midpoints[a][1] - midpoints[b][1] || byWays(sorted[a], sorted[b]))
    members.forEach((k, n) => (ids[k] = n ? `${place}/${n}` : place))
  }
  return ids
}

type Solved = {
  chain: Fitted
  d: number[]
  g: number[]
  z: number[]
  grounded: [boolean, boolean]
  needs: Need[]
  /** The decks its ends rest on, and where. */
  on: Array<{ deck: Solved; at: Point }>
  /** The deck's own ends and OSM nodes, before resampling, so ends that share a node can be told apart from ends that merely lie close. */
  ends: [Point, Point]
  nodes: Set<string>
}

/** What a deck passes over, as heights its surface must reach. */
function needs(s: Pick<Solved, 'chain' | 'd' | 'g'>, input: DeckInput, below: Solved[]): Need[] {
  const out: Need[] = []
  const reach = Math.max(...s.chain.edges)
  const own = box(s.chain.points)
  for (const c of input.crossed)
    if (meet(own, box(c.points)))
      for (const at of crossings(s.chain.points, c.points, reach, s.d)) out.push({ at, height: heightAt(s.d, s.g, at) + CLEARANCE[c.kind] })
  for (const lower of below.filter(o => meet(own, box(o.chain.points))))
    for (const at of crossings(s.chain.points, lower.chain.points, reach, s.d)) {
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
      if (o.chain.kind === 'rail' || s.chain.kind === 'rail' || !out[j].length) continue
      if (!meet(grow(box(o.chain.points), Math.max(...s.chain.edges) + Math.max(...o.chain.edges) + 3), box(s.chain.points))) continue
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

/** Ground under each deck, loaded a few decks at a time and nearby decks together. */
async function groundUnder(shaped: Chain[], edges: Array<[Point[], Point[]]>, ground: Ground): Promise<Array<number[] | null>> {
  const out: Array<number[] | null> = shaped.map(() => null)
  // Z-order of the midpoints at about a tile's size, so consecutive batches mostly share tiles.
  const morton = ([x, y]: Point) => {
    const [xi, yi] = [Math.floor(x * 2 ** 15), Math.floor(y * 2 ** 15)]
    let m = 0
    for (let b = 14; b >= 0; b--) m = m * 4 + ((yi >> b) & 1) * 2 + ((xi >> b) & 1)
    return m
  }
  const place = shaped.map(c => morton(c.points[Math.floor(c.points.length / 2)]))
  const order = shaped.map((_, k) => k).sort((a, b) => place[a] - place[b])
  const read = (points: Point[]) => points.map(p => ground.at(p))
  for (let i = 0; i < order.length; i += GROUND_BATCH) {
    const batch = order.slice(i, i + GROUND_BATCH)
    await ground.load(batch.flatMap(k => [...shaped[k].points, ...edges[k][0], ...edges[k][1]]))
    for (const k of batch)
      out[k] = filled(besideGround(read(shaped[k].points), read(edges[k][0]), read(edges[k][1]), along(shaped[k].points)))
  }
  return out
}

/**
 * The deck an end rests on, of those within reach of it: one it shares an
 * OSM node with, else one of its own layer, and of those the nearest. Taking
 * whichever came first let a ramp settle on a lower deck it merely passes
 * over rather than the one it joins.
 */
function restingOn(s: Solved, end: 0 | 1, solved: Solved[]): { height: number; on: Solved } | null {
  const p = s.chain.points[end ? s.chain.points.length - 1 : 0]
  const node = key(s.ends[end])
  let best: { o: Solved; near: ReturnType<typeof beside>; rank: number } | null = null
  for (const o of solved) {
    if (o === s) continue
    const reach = Math.max(...o.chain.edges) + 1
    if (!covers(grow(box(o.chain.points), reach), p)) continue
    const near = beside(o.chain.points, p)
    if (near.distance > reach) continue
    const rank = (o.nodes.has(node) ? 0 : 2) + (o.chain.layer === s.chain.layer ? 0 : 1)
    if (!best || rank < best.rank || (rank === best.rank && near.distance < best.near.distance)) best = { o, near, rank }
  }
  if (!best) return null
  const { o, near } = best
  return { height: heightAt(o.d, o.z, o.d[near.segment - 1] + (o.d[near.segment] - o.d[near.segment - 1]) * near.t), on: o }
}

export async function buildDecks(input: DeckInput, ground: Ground): Promise<Deck[]> {
  const decks = absorbPaths(fitted(chains(input.ways), input.outlines, input.kerbs)) as Fitted[]
  // An end lands where a road on the ground meets it, rests where it meets
  // another deck, and lands at a dead end. A deck in an outline reaches as far
  // as the outline fitted it, not just its carriageway: shaping comes later
  // and needs to know which ends land.
  const grounded = decks.map((c, k) => [c.points[0], c.points[c.points.length - 1]].map(p => {
    if (input.onGround.has(key(p))) return true
    return !decks.some((o, j) => {
      const reach = Math.max(1, ...(o.fit ?? o.edges))
      return j !== k && covers(grow(box(o.points), reach), p) && beside(o.points, p).distance < reach
    })
  }) as [boolean, boolean])

  const resampled = decks.map(c => ({ ...c, points: resample(c.points, STEP) }))
  const shapes = shapeDecks(resampled, input.outlines, grounded)
  // A deck in an outline that could not be shaped to it keeps the edges fitted to it.
  const shaped: Fitted[] = resampled.map(({ fit, ...c }, k) => {
    const shape = shapes[k]
    if (!shape) return widen(fit ? { ...c, edges: fit } : c, STEP)
    return widen({ ...c, sides: shape.sides, caps: shape.caps, edges: [Math.max(...shape.sides[0]), Math.max(...shape.sides[1])] }, STEP)
  })
  const edges = shaped.map(edgePoints)
  const under = await groundUnder(shaped, edges, ground)

  // Lower layers first, so a deck over another clears it.
  const solved: Solved[] = []
  for (const k of shaped.map((_, k) => k).sort((a, b) => shaped[a].layer - shaped[b].layer)) {
    const chain = shaped[k]
    const d = along(chain.points)
    const g = under[k]
    if (!g) continue
    const need = needs({ chain, d, g }, input, solved.filter(o => o.chain.layer < chain.layer))
    const own = decks[k].points
    const z = solve({ ...chain, grounded: grounded[k] }, g, need)
    solved.push({
      chain, d, g, grounded: grounded[k], needs: need, z, on: [],
      ends: [own[0], own[own.length - 1]], nodes: new Set(own.map(key)),
    })
  }
  // Settle the decks against each other: an end resting on another deck takes
  // that deck's height there, and decks side by side are joined at the higher
  // one. Some rounds, so a height carries through a ramp joining a ramp, and a
  // deck lifted to its neighbour can lift the next one along. A join is not
  // arched over again: that would carry a lift along the whole deck, and from
  // it to the next, ratcheting a whole interchange up.
  for (let round = 0; round < SETTLE; round++) {
    for (const s of solved) {
      const rests = ([0, 1] as const).map(i => (s.grounded[i] ? null : restingOn(s, i, solved)))
      s.on = rests.flatMap((r, i) => (r ? [{ deck: r.on, at: s.chain.points[i ? s.chain.points.length - 1 : 0] }] : []))
      if (!s.on.length) continue
      const resting = rests.map(r => r?.height ?? null) as [number | null, number | null]
      s.z = solve({ ...s.chain, grounded: s.grounded }, s.g, s.needs, resting)
    }
    for (let pass = 0; pass < SETTLE; pass++) joinNeighbours(solved)
  }
  for (const s of solved) s.z = smooth(s.z, s.d, steady(s.g, s.d))
  // Easing moves twins a few centimetres apart; side by side, they meet exactly.
  joinNeighbours(solved, undefined, EASED)
  const rows = piers(solved)

  const wiki = (s: Solved) => s.chain.ways.map(w => input.wikidata.get(w)).find(Boolean)
  const placed = solved.map(s => {
    const midpoint = s.chain.points[Math.floor(s.chain.points.length / 2)]
    const heading = bearing(s.chain.points)
    return { midpoint, heading, place: `${placeKey(midpoint, heading, 4)}/${s.chain.layer}` }
  })
  const ids = numbered(placed.map(p => p.place), placed.map(p => p.midpoint), solved.map(s => s.chain.ways))
  return solved.map((s, k) => {
    const { midpoint, heading } = placed[k]
    const round = (v: number) => Math.round(v * 100) / 100
    return {
      id: ids[k],
      bridge: s.chain.outline ?? (wiki(s) ? `wikidata/${wiki(s)}` : `at/${placeKey(midpoint, heading, 3)}`),
      ways: s.chain.ways,
      kind: s.chain.kind,
      layer: s.chain.layer,
      edges: s.chain.edges.map(round) as [number, number],
      sides: s.chain.sides ? (s.chain.sides.map(side => side.map(round)) as [number[], number[]]) : null,
      caps: s.chain.caps ? (s.chain.caps.map(round) as [number, number, number, number]) : null,
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
