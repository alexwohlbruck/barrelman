import {
  jointAreas,
  wayEnds,
  grownRing,
  inside,
  deckMeshOut,
  endsMade,
  landedOn,
  placedPiers,
  sideAt,
  smoothstep,
  touchdown,
  type UnderWay,
  UNDER_SLACK,
  edgePoints,
  joinNeighbours,
  coveredSides,
  innerSides,
  mouthSides,
  rounded,
  beside,
  meets,
  boundsOf,
  COVER,
  holds,
  MAX_REACH,
  deckMesh,
  along,
  metresPerUnit,
  type Bounds,
  type Chain,
  type Sides,
  type DeckSurface,
  type Top,
  type Mesh,
  type Piece,
  type Point,
  linesOf,
  polygonsOf,
  mercator,
  tileBounds,
} from './decks'
import type { Deck } from '../bridge-decks/build'
import { abutmentMesh } from './abutments'
import { closedAreas } from './closing'
import { AT_GRADE, joinedLines, layPaint, layWays, paintOf, type Paint } from './paint'
const WIDTH: Record<string, number> = { motorway: 12, trunk: 11, primary: 10, secondary: 9, tertiary: 8, minor: 7, service: 5, track: 4, path: 3, rail: 5, transit: 5 }

type Rgb = [number, number, number]

export type DeckPalette = {
  surface: Rgb
  concrete: Rgb
  parapet: Rgb
  /** A rail deck's surface. */
  track: Rgb
  /** The top of a road deck that has its carriageway laid on it: beside the carriageway the colour of the ground's paths, as its sidewalks are. */
  deck: Rgb
  /** Below street zoom, a deck's surface: the basemap's colour for its minor, major and highway roads, and paths. */
  lowRoad: Rgb
  lowMajor: Rgb
  lowHighway: Rgb
  lowPath: Rgb
  /** A sidewalk or footpath laid on a deck, and a rail. */
  walkway: Rgb
  rail: Rgb
  white: [number, number, number]
  yellow: [number, number, number]
  green: [number, number, number]
  red: [number, number, number]
  route: [number, number, number]
  routeCasing: [number, number, number]
}

function touchdowns(surfaces: DeckSurface[]): Bounds[] {
  return surfaces.flatMap(s => {
    const total = s.d[s.d.length - 1]
    const low = (i: number) => Math.min(s.d[i], total - s.d[i]) <= AT_GRADE.reach
    const n = s.points.length
    let [head, tail] = [0, n - 1]
    while (head < n && low(head)) head++
    while (tail >= head && low(tail)) tail--
    return [s.points.slice(0, head), s.points.slice(tail + 1)].flatMap(run => (run.length ? [boundsOf(run, s.width / 2 + COVER)] : []))
  })
}

/** Metres a bridge carriageway laid on a deck is grown by: enough to close the hairline where it meets the road beyond, too little to show as a kerb step. */
const SURFACE_OVERLAP = 0.08

/** Metres from a deck's end within which a way on the ground is the road carrying on from it, and the room an abutment keeps from any other. */
const OWN_ROAD = 2
const ABUTMENT_ROOM = 1

/** The share of a road deck's length that must lie in carriageway outlines for it to be drawn concrete. */
const PAVED = 0.8
/** How far past a bridge's end its paint may run on the ground, in metres: Barrelman gives it the street's first 15. */
const PAST_END = 18
/** Coloured lanes joined across a bridge's ends, and how far apart two corners may lie and be one lane's, in metres. */
const LANE_COLORS = ['green', 'red']
const LANE_JOIN = 2.5
/** Road classes whose decks are paved edge to edge, as the road beyond. */
const HIGHWAY = /^(motorway|trunk)$|_link$/

export type MeshFeature = { properties: Record<string, any>; geometry: any; _x?: number; _y?: number; _z?: number }
export type BakeInput = {
  decks: Deck[]
  own?: string[]
  outlines: Point[][][]
  roads: MeshFeature[]
  surfaces: MeshFeature[]
  paint: MeshFeature[]
  ground: (point: Point) => number | null
}
export type BakedBridge = { mesh: Mesh; ranges: Ranges; origin: Point; under: UnderWay[]; tracks: UnderWay[]; stands: Array<{ chain: Chain; surface: DeckSurface }> }

export function bakeBridges(input: BakeInput, palette: DeckPalette): BakedBridge {
  return new MeshBaker(input, palette).build()
}

class MeshBaker {
  private origin: Point
  private stands: Array<{ chain: Chain; surface: DeckSurface }> = []
  private tracks: UnderWay[] = []
  private under: UnderWay[] | null = null
  private drawn = new Map<string, number | null>()
  constructor(private input: BakeInput, private palette: DeckPalette) {
    this.origin = input.decks[0]?.midpoint ?? [0, 0]
  }
  private drawnGround(p: Point) {
    const key = p.join(',')
    if (!this.drawn.has(key)) this.drawn.set(key, this.input.ground(p))
    return this.drawn.get(key)!
  }
  build(): BakedBridge {
    const solved = this.input.decks.map(d => ({
      chain: { points: d.points, edges: d.edges, sides: d.sides ?? undefined, caps: d.caps ?? undefined,
        grounded: d.grounded, kind: d.kind, layer: d.layer, width: d.edges[0] + d.edges[1], road: this.input.roads.find(r => d.ways.includes(Number(r.properties.id)))?.properties.class } as Chain,
      z: [...d.heights], groundAt: [...d.ground], piers: [...d.piers], wayEnds: undefined as [Point, Point] | undefined,
    }))
    const own = new Set(this.input.decks.flatMap((d, i) => !this.input.own || this.input.own.includes(d.id) ? [i] : []))
    const roads = this.input.roads.filter(r => r.properties.brunnel === 'bridge' && !['path', 'rail', 'transit', 'bridge'].includes(r.properties.class)).flatMap(r =>
      linesOf(r.geometry).map(line => {
        const points = line.map(mercator)
        return { piece: { points, road: `${r.properties.class}${Number(r.properties.ramp) === 1 ? '_link' : ''}` } as Piece, bounds: boundsOf(points, MAX_REACH) }
      }))
    for (const s of solved) s.chain.road = classUnder(s.chain, roads) ?? s.chain.road
    const local = 0
    const joints = solved.flatMap(({ chain }) => wayEnds(chain))
    const paint = this.input.paint.filter(f => f.properties.bridge).map(paintOf)
    const ways = this.input.roads.filter(f => f.properties.brunnel === 'bridge').flatMap(f => {
      const kind = f.properties.class === 'rail' ? 'rail' as const : 'path' as const
      return f.properties.class === 'rail' || f.properties.class === 'path'
        ? linesOf(f.geometry).map(line => ({ kind, points: line.map(mercator) })) : []
    })
    for (const s of solved) {
      Object.assign(s, rounded(s.chain, s.z, s.groundAt))
      s.z = landedOn(s.chain, s.z, this.input.ground)
      s.wayEnds = [s.chain.points[0], s.chain.points[s.chain.points.length - 1]]
      const { shift, ...made } = endsMade(s.chain, s.z, s.groundAt, this.input.ground)
      Object.assign(s, made, { piers: s.piers.map(at => at + shift) })
    }
    const open = joinNeighbours(solved)
    const bridgeAreas = this.input.outlines.map(rings => ({ rings, bounds: boundsOf(rings[0]) }))
    const inner = innerSides(solved, bridgeAreas)
    const mouths = mouthSides(solved)
    const onDecks = coveredSides(solved).map((sides, k) => sides.map((side, s) => side.map((c, i) => c || inner[k][s][i] || mouths[k][s][i])) as Sides)
    // Decks built here stand their piers clear of the roads, railways and paths beneath.
    const unplaced = solved.filter(s => !s.piers)
    if (unplaced.length) {
      const beneath = this.carriageways(false, unplaced.map(({ chain }) => boundsOf(chain.points, Math.max(...chain.edges))))
        .flatMap(c => c.areas.map(rings => ({ rings, bounds: boundsOf(rings[0]) })))
      const ways = this.waysNear(unplaced.map(({ chain }) => boundsOf(chain.points, Math.max(...chain.edges))))
      for (const s of unplaced) s.piers = placedPiers(s.chain, s.z, s.groundAt, at => this.bentClear(s.chain, at, ways, beneath))
    }
    // Solved in true metres; drawn over terrain that is stretched.
    const exaggeration = 1
    for (const s of solved) {
      s.z = s.z.map(z => z * exaggeration)
      s.groundAt = s.groundAt.map(g => g * exaggeration)
    }
    // A deck's end is biased toward the eye against the ground as drawn, which may stand higher than the ground it was solved on.
    // Anywhere else, the drawn ground under either edge reaching up near the deck, as beside a cutting, is met the same way.
    const biasGround = solved.map(({ chain, z, groundAt }) => {
      const d = along(chain.points)
      const [lefts, rights] = edgePoints(chain)
      return groundAt.map((g, i) => {
        const w = smoothstep(Math.max(0, 1 - Math.min(chain.grounded[0] ? d[i] : Infinity, chain.grounded[1] ? d[d.length - 1] - d[i] : Infinity) / AT_GRADE.reach))
        const drawn = w > 0 ? this.drawnGround(chain.points[i]) : null
        const ended = drawn === null ? g : g + (Math.max(g, Math.min(drawn, z[i])) - g) * w
        const beside = Math.max(...[lefts[i], rights[i]].map(q => this.drawnGround(q) ?? -Infinity))
        return Math.max(ended, Math.min(beside, z[i]))
      })
    })

    const p = this.palette
    const mesh = deckMeshOut()
    // A road deck carrying a carriageway outline is concrete, the carriageway laid on it in asphalt at its own width.
    const paved = this.carriageways()
    const outlines = paved.flatMap(c => c.areas.map(rings => ({ rings, bounds: boundsOf(rings[0]) })))
    // Concrete only on a street deck whose carriageway runs its length; a highway deck, or any other, is the road itself, in asphalt.
    const concrete = (chain: Chain) => chain.kind === 'road' && !HIGHWAY.test(chain.road ?? '') &&
      chain.points.filter(q => outlines.some(o => holds(o.bounds, q) && inside(q, o.rings))).length >= chain.points.length * PAVED
    const concreteAt = new Map(solved.map(({ chain }) => [chain, concrete(chain)]))
    const colors = (chain: Chain) =>
      chain.kind === 'rail' ? { surface: p.track, low: p.track } : chain.kind === 'path' ? { surface: p.walkway, low: p.lowPath }
      : { surface: concreteAt.get(chain) ? p.deck : p.surface, low: lowRoad(p, chain.road) }
    const tops = solved.map(() => [] as Top[])
    for (const [k, { chain, groundAt, z, piers }] of solved.entries())
      deckMesh(chain, z, groundAt, this.origin, { ...colors(chain), concrete: p.concrete, parapet: p.parapet }, own.has(k) ? mesh : deckMeshOut(), open[k], piers, tops[k], biasGround[k], onDecks[k])

    const surfaces: DeckSurface[] = solved.map(({ chain, z, groundAt }, k) => {
      const width = 2 * Math.max(...chain.edges) + 1
      return {
        points: chain.points, z, d: along(chain.points), width, bounds: boundsOf(chain.points, width),
        bias: z.map((h, i) => touchdown(h - biasGround[k][i])), low: colors(chain).low, ground: groundAt, kind: chain.kind, edges: edgePoints(chain), tops: tops[k], sidewalks: !!concreteAt.get(chain),
      }
    })
    const owned = new Set(surfaces.filter((_, k) => own.has(k)))
    const paintMesh: typeof layPaint = (out, paint, decks, colors, origin, options = {}) => layPaint(out, paint, decks, colors, origin, { ...options, on: d => owned.has(d) && (!options.on || options.on(d)) })
    this.stands = solved.flatMap(({ chain }, k) => own.has(k) ? [{ chain, surface: surfaces[k] }] : [])
    this.tracks = ways.flatMap(w => (w.kind === 'rail' ? [{ points: w.points, half: 1, bounds: boundsOf(w.points, 3) }] : []))
    this.under = null
    const grounded = solved.filter(({ chain }, k) => own.has(k) && chain.kind === 'road' && (chain.grounded[0] || chain.grounded[1]))
    const onGround = this.carriageways(false, grounded.map(({ chain }) => boundsOf(chain.points)))
      .flatMap(c => c.areas.map(rings => ({ rings, bounds: boundsOf(rings[0]) })))
    const frame = { origin: this.origin, color: p.concrete, out: mesh }
    for (const { chain, z, groundAt, wayEnds } of grounded) {
      const clear = ([0, 1] as const).map(end => this.abutmentClear(wayEnds?.[end] ?? chain.points[end ? chain.points.length - 1 : 0], onGround))
      abutmentMesh(chain, z, groundAt, frame, (end, footprint) => clear[end](footprint), exaggeration)
    }
    const ranges = { paintFrom: mesh.position.length / 3, railsFrom: 0, routesFrom: 0 }
    const tar = { ...p, asphalt: p.surface }
    const ends = touchdowns([...owned])
    // On the ground as the map draws it, so what lies there sits on the drawn terrain, not the DEM read for the decks.
    const ground = { at: (q: Point) => this.drawnGround(q), near: ends, low: p.lowRoad }
    // A bridge's paint runs on past its ends onto the ground, where the deck draws it in the ground's place.
    const pastEnds = solved.flatMap(({ chain }, k) => !own.has(k) || chain.kind !== 'road' ? [] : wayEnds(chain).map(end => ({ ...end, reach: PAST_END, half: end.half + COVER })))
    const past = pastEnds.map(({ p: at, out, half }) => {
      const scale = metresPerUnit(at[1])
      return boundsOf([at, [at[0] + (out[0] * PAST_END) / scale, at[1] + (out[1] * PAST_END) / scale]], half)
    })
    const onward = { ...ground, near: [...ends, ...past] }
    // An asphalt deck is its carriageway already; only a concrete one, and the ground past the ends, need it laid on.
    paintMesh(mesh, paved, surfaces, tar, this.origin, { ground, on: deck => !!deck.sidewalks })
    const atGrade = this.carriageways(false, ends)
    // Laid as one area, less the bridge's own, so pieces mapped apart leave no bars or notches on a deck's end.
    const joined = closedAreas(atGrade.flatMap(c => c.areas), paved.flatMap(c => c.areas), ends)
    paintMesh(mesh, [{ props: { kind: 'surface', pattern: 'fill', color: 'asphalt' }, runs: [], areas: joined }], surfaces, tar, this.origin, { atGrade: true })
    // The road on the ground and the bridge's own each end square to their own way; where those bend at the bridge node a wedge opens between them.
    const meeting = [...joints, ...solved.slice(0, local).flatMap(({ chain }) => wayEnds(chain))]
    const outers = (list: Paint[]) => list.flatMap(c => c.areas.map(rings => rings[0]))
    const wedges = jointAreas(meeting, outers([...paved, ...atGrade]))
    paintMesh(mesh, [{ props: { kind: 'surface', pattern: 'fill', color: 'asphalt' }, runs: [], areas: wedges.map(w => [w]) }], surfaces, tar, this.origin, { ground })
    layWays(mesh, ways.filter(w => w.kind === 'path'), surfaces, p, this.origin, d => owned.has(d))
    const { bridge: bridgePaint, ground: beyond } = joinedLines(paint, this.paintAtGrade(ends))
    // Paint is drawn as a decal, last on top: coloured lanes, then white areas, then lines.
    const coloured = (f: Paint) => LANE_COLORS.includes(f.props.color)
    for (const tier of [coloured, (f: Paint) => !coloured(f)]) {
      paintMesh(mesh, bridgePaint.filter(tier), surfaces, p, this.origin, { ground: onward, past: pastEnds, parts: 'areas' })
      paintMesh(mesh, beyond.filter(tier), surfaces, p, this.origin, { atGrade: true, parts: 'areas' })
      if (tier !== coloured) continue
      // Coloured lanes on the bridge and on the ground meet at the joint the same way: each lane's own wedge, in its colour.
      for (const color of LANE_COLORS) {
        const joins = jointAreas(meeting, outers([...paint, ...beyond].filter(f => f.props.color === color && f.areas.length)), LANE_JOIN)
        if (joins.length) paintMesh(mesh, [{ props: { kind: 'lane', pattern: 'fill', color }, runs: [], areas: joins.map(w => [w]) }], surfaces, p, this.origin, { ground })
      }
    }
    paintMesh(mesh, bridgePaint, surfaces, p, this.origin, { ground: onward, past: pastEnds, parts: 'lines' })
    paintMesh(mesh, beyond, surfaces, p, this.origin, { atGrade: true, parts: 'lines' })
    ranges.railsFrom = mesh.position.length / 3
    layWays(mesh, ways.filter(w => w.kind === 'rail'), surfaces, p, this.origin, d => owned.has(d))
    ranges.routesFrom = mesh.position.length / 3
    return { mesh, ranges, origin: this.origin, stands: this.stands, under: this.groundWays(), tracks: this.tracks }
  }
  private groundWays(): UnderWay[] {
    this.under ??= this.waysNear(this.stands.flatMap(s => (s.surface.bounds ? [s.surface.bounds] : [])))
    return this.under
  }

  /** The ways on the ground meeting these boxes. */
  private waysNear(boxes: Bounds[]): UnderWay[] {
    const ground = this.input.roads.filter(f => !['bridge', 'tunnel'].includes(f.properties.brunnel))
    return ground.flatMap(f => linesOf(f.geometry).flatMap(line => {
      const points = line.map(mercator)
      if (points.length < 2) return []
      const half = (WIDTH[f.properties?.class] ?? 6) / 2
      const bounds = boundsOf(points, half + UNDER_SLACK)
      return boxes.some(d => meets(d, bounds)) ? [{ points, half, bounds }] : []
    }))
  }

  /** The terrain as drawn at a point; where none is drawn yet, the fixed-zoom ground stretched as the terrain is. */

  private bentClear(chain: Chain, at: number, ways: UnderWay[], beneath: Array<{ rings: Point[][]; bounds: Bounds }>): boolean {
    const d = along(chain.points)
    let k = 1
    while (k < chain.points.length - 1 && d[k] < at) k++
    const t = Math.max(0, Math.min(1, (at - d[k - 1]) / (d[k] - d[k - 1] || 1)))
    const [a, b] = [chain.points[k - 1], chain.points[k]]
    const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1
    const scale = 1 / metresPerUnit(a[1])
    const [l, r] = [sideAt(chain, 0, k - 1, t), sideAt(chain, 1, k - 1, t)]
    const bent: Point[] = []
    for (let across = -r; across <= l; across += 1)
      bent.push([a[0] + (b[0] - a[0]) * t - ((b[1] - a[1]) / len) * across * scale, a[1] + (b[1] - a[1]) * t + ((b[0] - a[0]) / len) * across * scale])
    return !bent.some(q =>
      ways.some(w => holds(w.bounds, q) && beside(w.points, q).distance < w.half + ABUTMENT_ROOM) ||
      beneath.some(r => holds(r.bounds, q) && inside(q, r.rings)))
  }

  /** Whether an abutment's footprint keeps off the ways and carriageways on the ground, but for the road carrying on from its way's end. */
  private abutmentClear(end: Point, rings: Array<{ rings: Point[][]; bounds: Bounds }>): (footprint: Point[]) => boolean {
    const scale = metresPerUnit(end[1])
    const touches = (q: Point) => Math.hypot(q[0] - end[0], q[1] - end[1]) * scale < OWN_ROAD
    const ways = this.groundWays().filter(w => !touches(w.points[0]) && !touches(w.points[w.points.length - 1]))
    const surfaces = rings.filter(r => !(holds(r.bounds, end) && inside(end, r.rings)))
    return footprint => !footprint.some(q =>
      ways.some(w => holds(w.bounds, q) && beside(w.points, q).distance < w.half + ABUTMENT_ROOM) ||
      surfaces.some(r => holds(r.bounds, q) && inside(q, r.rings)))
  }

  private paintAtGrade(ends: Bounds[]): Paint[] {
    if (!ends.length) return []
    return this.input.paint.filter(f => !f.properties.bridge)
      .filter(f => this.meetsAny(f, ends))
      .map(paintOf)
  }

  /** Carriageways as asphalt to lay: the bridges' own, or the roads on the ground over the given boxes, where decks end on them. */
  private carriageways(bridge = true, over: Bounds[] = []): Paint[] {
    if (!bridge && !over.length) return []
    return this.input.surfaces.filter(f => !!f.properties.bridge === bridge && f.properties.kind !== 'island')
      .filter(f => bridge || this.meetsAny(f, over))
      .map(f => paintOf({ geometry: f.geometry, _x: f._x, _y: f._y, _z: f._z, properties: { kind: 'surface', pattern: 'fill', color: 'asphalt' } }))
      // Grown a little, so a bridge's carriageway and the road beyond overlap where they meet rather than leave a crack.
      .map(c => ({ ...c, areas: c.areas.map(rings => [grownRing(rings[0], SURFACE_OVERLAP), ...rings.slice(1)]) }))
  }

  /** Whether a feature's geometry meets one of the boxes, its tile tested first. */
  private meetsAny(f: any, boxes: Bounds[]): boolean {
    const tile = tileBounds(f)
    if (tile && !boxes.some(b => meets(b, tile))) return false
    const coords = [...linesOf(f.geometry), ...polygonsOf(f.geometry).flat()].flat().map(mercator)
    return coords.length > 0 && boxes.some(b => meets(b, boundsOf(coords)))
  }

}
type Ranges = { paintFrom: number; railsFrom: number; routesFrom: number }

/** The basemap's colour below street zoom for a road deck of a class. */
function lowRoad(p: DeckPalette, road?: string): Rgb {
  const base = road?.replace(/_link$/, '')
  return base === 'motorway' ? p.lowHighway : base === 'trunk' || base === 'primary' ? p.lowMajor : p.lowRoad
}

function classUnder(chain: Chain, roads: Array<{ piece: Piece; bounds: Bounds }>): string | undefined {
  if (chain.kind !== 'road') return undefined
  const middle = chain.points[chain.points.length >> 1]
  let best: { road?: string; distance: number } = { distance: Math.min(MAX_REACH, Math.max(...chain.edges) + COVER) }
  for (const { piece, bounds } of roads) {
    if (!holds(bounds, middle)) continue
    const { distance } = beside(piece.points, middle)
    if (distance < best.distance) best = { road: piece.road, distance }
  }
  return best.road
}

