/**
 * Tunnel cuts for an area: every portal where a tunnel comes out of the
 * ground, and the cut dug down to it, solved from OSM and the terrain.
 *
 * Pure: the caller reads the ways, surfaces, buildings and walls around an
 * area, loads the ground `groundPoints` asks for, and stores what `solveCuts`
 * returns. A client draws each cut's walls and carves its floor into the
 * terrain straight from these rows, with no solving of its own.
 */
import { along, boundsOf, chains, densify, edgePoints, holds, meets, metresPerUnit, type Bounds, type Piece, type Point } from '../bridge-mesh/decks'
import { BORE, COVER_AT, CUT_MAX, KERB, PROFILES, RIM, approach, emerge, faceWall, groupPortals, inside, measureEdges, portalLine, solveCut, splitAtFace, truncate, type BoreKind, type Cut, type Opening } from './tunnels'

/** OpenMapTiles road classes, as the basemap the client draws tunnels from classes them. */
export type RoadClass = 'motorway' | 'trunk' | 'primary' | 'secondary' | 'tertiary' | 'minor' | 'service' | 'path' | 'rail' | 'transit'

/** Widths by class, in metres, for a way with no surface to measure. */
export const WIDTH: Record<RoadClass, number> = {
  motorway: 11, trunk: 10, primary: 9, secondary: 8, tertiary: 7, minor: 6, service: 4, path: 2, rail: 5, transit: 5,
}

const HIGHWAY: Record<string, RoadClass> = {
  motorway: 'motorway', motorway_link: 'motorway', trunk: 'trunk', trunk_link: 'trunk', primary: 'primary', primary_link: 'primary',
  secondary: 'secondary', secondary_link: 'secondary', tertiary: 'tertiary', tertiary_link: 'tertiary',
  unclassified: 'minor', residential: 'minor', living_street: 'minor', road: 'minor', service: 'service',
  pedestrian: 'path', path: 'path', footway: 'path', cycleway: 'path', steps: 'path', bridleway: 'path', corridor: 'path',
}
const RAILWAY: Record<string, RoadClass> = {
  rail: 'rail', narrow_gauge: 'rail', preserved: 'rail', funicular: 'rail', subway: 'transit', light_rail: 'transit', monorail: 'transit', tram: 'transit',
}

/** A way's class, or null for one the basemap draws no road for. */
export const classOf = (highway: string | null, railway: string | null): RoadClass | null =>
  (highway && HIGHWAY[highway]) || (railway && RAILWAY[railway]) || null

export const kindOf = (c: RoadClass): BoreKind => (c === 'path' ? 'path' : c === 'rail' || c === 'transit' ? 'rail' : 'road')

/** A highway or railway way, in mercator units. `layer` is OSM's, 0 where untagged. */
export type Way = { points: Point[]; cls: RoadClass; layer: number; brunnel: 'tunnel' | 'bridge' | 'ford' | null }

/** What is read around an area. Buildings are outlines whose faces a covered portal opens at. */
export type Surroundings = {
  ways: Way[]
  /** Outlines of the carriageways at grade, which a cut is fitted to. */
  surfaces: Point[][]
  buildings: Point[][][]
  /** Walls mapped across a bore, as its headwall. */
  walls: Point[][]
}

/** Metres between samples along a cut, and how far down the approach its width is measured. */
export const SAMPLE = 3
const MEASURE = 40

/** One portal's cut, as stored and served. `points` run from just before the cut opens, through the portal (`at`), into the bore. */
export type TunnelCut = {
  id: string
  kind: BoreKind
  points: Point[]
  at: number
  edges: [number, number]
  cut: Cut
  face?: Point
  piers?: Array<[number, number]>
}

/** The tunnels mapped underground: a passage under a building is a tunnel with no layer, so only a negative one counts. */
const underground = (w: Way) => w.brunnel === 'tunnel' && w.layer < 0

/** Each portal's opening, before the ground is read: its approach, bore, measured edges and headwall. `read` is the area whose ways were read. */
export function openings({ ways, surfaces, buildings, walls }: Surroundings, read: Bounds): Opening[] {
  const tunnels: Piece[] = ways.filter(underground).map(w => ({ points: w.points, layer: w.layer, width: WIDTH[w.cls], kind: kindOf(w.cls) }))
  if (!tunnels.length) return []
  const mid = (read.minY + read.maxY) / 2
  const tolerance = 1.5 / metresPerUnit(mid)
  // One tunnel's ways may step between layers; each portal reads its own way's layer.
  const layerAt = (q: Point, kind: BoreKind) => {
    let best = { layer: -1, apart: tolerance }
    for (const t of tunnels)
      for (const e of [t.points[0], t.points.at(-1)!]) {
        const apart = Math.hypot(e[0] - q[0], e[1] - q[1])
        if (t.kind === kind && apart < best.apart) best = { layer: t.layer, apart }
      }
    return best.layer
  }
  // An end outside what was read may run on into another tunnel.
  const bores = chains(tunnels.map(t => ({ ...t, layer: -1 })), p => !holds(read, p), tolerance)
  const portals = bores.flatMap(bore => [0, 1].filter(e => bore.grounded[e]).map(e => {
    const line = e ? [...bore.points].reverse() : bore.points
    // Where both ends open, each draws its bore halfway, so the two never overlap.
    const reach = bore.grounded[0] && bore.grounded[1] ? Math.min(BORE, along(line).at(-1)! / 2) : BORE
    return { line, reach, width: bore.width, kind: bore.kind, layer: layerAt(line[0], bore.kind) }
  }))
  if (!portals.length) return []
  // Each portal reads only what lies within a cut's length of it.
  const boxed = <T>(items: T[], points: (t: T) => Point[]) => items.map(item => ({ item, bounds: boundsOf(points(item)) }))
  const lines = (kind: BoreKind) => boxed(ways.filter(w => w.brunnel === null && kindOf(w.cls) === kind).map(w => w.points), l => l)
  const roads = { road: lines('road'), path: lines('path'), rail: lines('rail') }
  const rings = boxed(surfaces.filter(ring => ring.length > 1), r => r)
  const footprints = buildings.map(rings => ({ rings, bounds: boundsOf(rings[0]) }))
  const headwalls = boxed(walls.filter(line => line.length > 1), l => l)
  const within = <T>(all: Array<{ item: T; bounds: Bounds }>, b: Bounds) => all.filter(x => meets(x.bounds, b)).map(x => x.item)

  const found: Opening[] = []
  for (const { line, reach, width, kind, layer } of portals) {
    // As far as the portal may move out, and the stretch past it the width is measured along.
    const bounds = boundsOf([line[0]], CUT_MAX + MEASURE + 20)
    const here = footprints.filter(f => meets(f.bounds, bounds))
    const covered = (q: Point) => here.some(f => holds(f.bounds, q) && inside(q, f.rings))
    const road = approach(line[0], line[1], within(roads[kind], bounds), tolerance, PROFILES[kind].reach)
    // A headwall mapped across the bore is where it opens, and which way it faces.
    const face = faceWall(line, within(headwalls, bounds))
    const split = face ? splitAtFace(line, face) : { out: [line[0]], bore: line }
    const outward = [...split.out, ...road.points.slice(1)]
    const moved = outward.length > 1 ? emerge(outward, split.bore, covered) : null
    if (!moved || moved.out.length < 2) continue
    const out = { ...road, points: moved.out }
    // Measured running in, as the bore does, so its left is the bore's.
    const measured = kind === 'road' ? measureEdges(densify(truncate(out.points, MEASURE), SAMPLE).reverse(), within(rings, bounds)) : null
    found.push({ out, bore: truncate(moved.bore, reach), edges: measured ?? [width / 2, width / 2], kind, layer, face: moved.out === outward ? face?.along : undefined })
  }
  return groupPortals(found)
}

/** Metres past a cut's line its ground is read, beyond the furthest rim. */
const GROUND_PAD = 25

/** Points whose terrain tiles cover all the ground `solveCuts` reads for these openings: each line's box corners, as a cut spans at most two tiles a side. */
export function groundPoints(found: Opening[]): Point[] {
  return found.flatMap(({ out, bore }) => {
    const b = boundsOf(portalLine(out.points, bore, SAMPLE).points, GROUND_PAD)
    return [[b.minX, b.minY], [b.maxX, b.minY], [b.minX, b.maxY], [b.maxX, b.maxY]] as Point[]
  })
}

/** A portal's cut from the ground along it, kept from just before it opens; null where there is no cut to dig. `ground` is NaN where there is no terrain. */
export function solvePortal({ out, bore, edges, kind, face, piers }: Opening, ground: (p: Point) => number): TunnelCut | null {
  const line = portalLine(out.points, bore, SAMPLE)
  const { points, at } = line
  const centre = points.map(ground)
  if (centre.some(Number.isNaN)) return null
  const sides = RIM.map(r => edgePoints({ points, edges: [edges[0] + KERB + r, edges[1] + KERB + r] } as any))
  const rims = [0, 1].map(k => points.map((_, i) => {
    const known = sides.map(side => ground(side[k][i])).filter(g => !Number.isNaN(g))
    return known.length ? Math.min(...known) : centre[i]
  })) as [number[], number[]]
  const d = along(points)
  const cover = COVER_AT.map(m => {
    let i = at
    while (i < points.length - 1 && d[i] - d[at] < m) i++
    return ground(points[i])
  }).filter(g => !Number.isNaN(g))
  const cut = solveCut(d, at, centre, rims, cover.length ? Math.max(...cover) : centre[at], out.junction, PROFILES[kind])
  if (!cut || cut.open >= at) return null
  // Nothing before the vertex ahead of where it opens is drawn or carved; that one sets the walls' direction there.
  const from = Math.max(0, cut.open - 1)
  const trim = <T>(xs: T[]) => xs.slice(from)
  const id = `${kind}:${points[at].map(v => Math.round(v * 2 ** 30)).join(',')}`
  return {
    id, kind, points: trim(points), at: at - from, edges, face, piers: piers?.length ? piers : undefined,
    cut: { ...cut, floor: trim(cut.floor), walls: [trim(cut.walls[0]), trim(cut.walls[1])], open: cut.open - from },
  }
}

/** Every cut over the openings whose portal `owns` keeps, one per portal. */
export function solveCuts(found: Opening[], ground: (p: Point) => number, owns: (p: Point) => boolean): TunnelCut[] {
  const cuts = new Map<string, TunnelCut>()
  for (const opening of found) {
    const cut = solvePortal(opening, ground)
    if (cut && owns(cut.points[cut.at])) cuts.set(cut.id, cut)
  }
  return [...cuts.values()]
}
