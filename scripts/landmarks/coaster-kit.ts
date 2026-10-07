/**
 * Shared kit for the roller coaster generators (Carowinds): the plan from an
 * OSM track chain, a vertical profile built from a list of named elements, an
 * energy check, banking from speed and curvature, and the swept track,
 * supports and station canopy. A generator is its data plus one call to
 * `buildCoaster`. A development tool for authoring models; nothing in the API
 * runs it.
 *
 * Profile. OSM has the plan but no heights, so the height of the track is
 * given as a list of elements in ride order, each at a distance along the OSM
 * polyline (`s`, metres from its first node) with a height above the local
 * ground (`z`). Every element is a crest, a valley or the end of a level run.
 * Between two elements the track is a circular arc out of the first, a
 * straight, and an arc into the second (the common tangent of the two
 * circles), which is how a coaster's hills are really drawn: rounded tops,
 * straight legs, tight valleys. Each arc's radius comes from the train's speed
 * there (a crest just under weightless, a valley at about 3.5 g) unless the
 * element sets `r`, and shrinks when two elements are too close for it.
 *
 * Energy. The train's head of energy starts at the lift crest and falls by
 * `friction` metres for every metre of track (B&M hypers lose roughly 2 to
 * 3 % of the lift height per 100 m); a block brake resets it. The report
 * flags any stretch where the train would stall, which means a hill is too
 * high or too late for the energy left.
 *
 * Run a generator with REPORT=1 for the element table and the energy check,
 * COASTER_DUMP=<file> for the centreline in the park frame (for matching
 * photos), CLEARANCE=1 for near-misses between stretches of track.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, type Swatch } from './palette'

const G = 9.81
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

/** OSM coordinates are projected about this point (equirectangular, metres): the park frame. */
export const LAT0 = 35.103, LON0 = -80.943
export const MX = 111320 * Math.cos((LAT0 * Math.PI) / 180), MY = 110574

/** A named element of the profile. */
export interface Element {
  name: string
  /** Distance along the OSM polyline, metres from its first node. */
  s: number
  /** Height of the running surface above the local ground, metres. */
  z: number
  /** Radius of the vertical curve here, metres; from the speed when omitted. */
  r?: number
  /**
   * Pitch of the straight from here to the next element, degrees (a drop's
   * published angle). The two arcs are then sized together to give exactly
   * this angle, keeping their ratio, instead of from the speed.
   */
  pitch?: number
}

export interface CoasterSpec {
  name: string
  /** Output GLB, relative to scripts/landmarks/. */
  out: string
  /** The circuit in the park frame, node by node in the direction of travel. */
  chain: [number, number][]
  /** Centre of the footprint in the park frame; the model's origin. */
  anchor: [number, number]
  /** Terrain grid: rows south to north, columns west to east, metres above `base`. */
  ground: { x0: number; y0: number; step: number; rows: number[][]; base: number }
  profile: Element[]
  /** Chain lift, polyline s: level banking, no energy check. */
  lift: [number, number]
  /** Brake runs, polyline s, at height z above the ground: level banking; `head` is the energy (m, v²/2g) a run lets the train leave with. */
  brakes: { from: number; to: number; z: number; head: number }[]
  /** Banking set by hand, degrees, signed by the direction of the turn there (0 holds the track level, e.g. down a drop). */
  banks?: { from: number; to: number; deg: number; name?: string }[]
  /** Stretches allowed below the ground (a trench or tunnel), polyline s. */
  belowGrade?: [number, number][]
  /** Losses per metre of track: [rolling, drag]; head lost per metre = a + b × head (drag goes with v²). */
  losses: [number, number]
  /** Head (v²/2g, m) the chain gives the train over the lift crest; 1 m is about 4.4 m/s. */
  liftHead?: number
  track: { W: number; RAIL: number; DEPTH: number; SPINE: number }
  station: { ring: [number, number][]; posts: [number, number][]; roofZ: number }
  /** `spine: 'deck'` (opt-in) draws the spine in the deck's material, for a track painted one colour. */
  colours: { deck: [string, number]; spine: [string, number] | 'deck'; supports: [string, number] | 'trim' }
  /** Support radii for low, middle and high track. */
  supportR: [number, number, number]
  meta: { height: number; trackLength: number }
  /** Opt-in: further chain lifts (a mine train's second lift), polyline s; they hold the train at lift speed like `lift`. */
  extraLifts?: [number, number][]
  /** Opt-in: loops and corkscrews; see `Inversion`. */
  inversions?: Inversion[]
  /** Opt-in: a wooden coaster's trestle (walls of bents and ledgers) instead of tubular supports; see `Trestle`. */
  trestle?: Trestle
  /** Opt-in: walls under the station roof, for a station that is an enclosed building. */
  stationWalls?: boolean
  /** Opt-in: covered stretches (themed tunnels, sheds over the track), polyline s; a roofed box over the track. */
  covers?: { from: number; to: number; height?: number; width?: number }[]
  /**
   * Opt-in: LSM/LIM launches, polyline s, ending `z` m above the ground. Over
   * [from, to] the head rises evenly (constant thrust) to `head` (v²/2g, m) above
   * the track. With launches, the energy walk starts at the end of `lift`, which
   * is then the tyre-driven stretch out of the station, not a chain.
   */
  launches?: { from: number; to: number; z: number; head: number; name?: string }[]
  /** Opt-in: an inverted coaster; see `InvertedOpts`. */
  inverted?: InvertedOpts
  /** Opt-in: box-section supports (square, flat-shaded) instead of round tubes. */
  boxSupports?: boolean
  /**
   * Opt-in: hand banking that rolls faster than the 4°/m cap, for the half
   * inversions OSM draws as a tight hairpin (an Immelmann, a batwing, a
   * cutback): the bank runs smoothly from the automatic one to `deg` and back
   * over `ease` m either side of [from, to], applied after the cap. `deg` is
   * signed by the turn, as in `banks`; 180 holds the train upside down.
   */
  rolls?: { from: number; to: number; deg: number; ease?: number; name?: string }[]
  /**
   * Opt-in: a shuttle (a Boomerang): the chain is open, not a circuit. The
   * track starts at the first node and ends at the last, the plan doesn't
   * curl round to join them, and REPORT also checks the run back from the far
   * end, with `liftHead` over its top as for the first.
   */
  shuttle?: boolean
  /**
   * Opt-in, for small rides: supports every `supportEvery` m of plan instead
   * of 17, and a cross-section every `sampleDeg`° of turn or pitch instead of
   * 9 (a kids' coaster's tight helices otherwise spend the budget on the track).
   * `supportMinZ` is the lowest track (m above the ground) that gets a
   * support, instead of 2.2, for a kiddie track that runs at knee height.
   */
  supportEvery?: number
  sampleDeg?: number
  supportMinZ?: number
  /**
   * Opt-in: a helix carried from a central mast, as on Vekoma's family
   * coasters: a column at `at` (park frame) rising `top` m above the ground,
   * with a spoke from just under its head to the track every `every` m of
   * plan (default 8) between `from` and `to` (polyline s). The spokes meet
   * the spine, so they suit an inverted track best.
   */
  hubs?: { at: [number, number]; top: number; from: number; to: number; every?: number }[]
  /**
   * Opt-in: hand-placed bents over a stretch whose real supports are a few
   * big frames far apart, like a giga's lift and drop, instead of one every
   * 17 m. Between `from` and `to` (polyline s) the regular supports are left
   * out; at each `at` (polyline s) a column stands from the ground to the
   * spine, with one strut from the same head to a foot `spread` × the height
   * away (default 0.35): to the `left` or `right` of the direction of travel,
   * or raked `back` or `ahead` along it. A level tie joins column and strut
   * at `tie` of the height (default 0.3; 0 for none).
   */
  bents?: { from: number; to: number; at: { s: number; strut: 'left' | 'right' | 'back' | 'ahead'; spread?: number; tie?: number }[] }[]
  /**
   * Opt-in: the park frame's origin as [lon, lat], for a park away from
   * Carowinds (LAT0/LON0). The kit only uses it to print the anchor; the
   * chain, anchor and ground are given in that frame as usual.
   */
  origin?: [number, number]
  /**
   * Opt-in: further geometry drawn with the track's frames, such as themed
   * tunnels or a sign in a loop. Called once the track, supports and station
   * are built; it may draw into the kit's own parts (`parts`) or return new
   * ones, each with its own material. See `CoasterKit`.
   */
  extras?: (kit: CoasterKit) => { part: Part; material: Swatch }[]
}

/** A frame on the track: centreline point, tangent, the riders' left and up; model frame (origin at the anchor, z above the datum). */
export type TrackFrame = { p: V3; t: V3; left: V3; up: V3; s: number }

/** What `extras` gets to draw with. */
export interface CoasterKit {
  /** The frame on the drawn track (inversions included) nearest a polyline s. */
  at: (ps: number) => TrackFrame
  /** Ground height in the model frame at a model-frame plan point. */
  ground: (x: number, y: number) => number
  /** A park-frame plan point in the model frame. */
  toModel: (q: [number, number]) => [number, number]
  /** The kit's parts, to add to: deck (running surface), spine, supports, stone (station), roof. */
  parts: { deck: Part; spine: Part; supports: Part; stone: Part; roof: Part }
  /** Each inversion's dense frames, in ride order. */
  inversions: { inv: Inversion; pts: TrackFrame[] }[]
  track: CoasterSpec['track']
}

/**
 * An inverted coaster: the train hangs under the rails. The box spine sits
 * above them, the deck (the rails) faces down, and the supports reach over
 * the track: a column beside it with an arm across (B&M's L) where it is low,
 * a portal of two columns and a crossbeam where it is high, and a plain
 * column wherever the track is rolled far enough for its spine to face down.
 * `clear` is the lowest the rails may come above the ground, for the riders'
 * feet (about 4 m; trenches are `belowGrade`); `reach` is how far the columns
 * stand out from the track's centre.
 */
export interface InvertedOpts { clear?: number; reach?: number }

/**
 * An inversion takes over the track between `from` and `to` (polyline s).
 * The profile should hold the track level there (two elements at the same
 * height), and the banking is forced to 0 over the window, so the inversion
 * starts and ends level and C1. The base is a cubic Hermite from the entry
 * point and heading to the exit point and heading, so the OSM trace inside
 * the window (a zigzag for a loop, an S for a corkscrew) is ignored.
 *
 * - `loop`: a vertical loop `height` m above its entry, drawn by integrating
 *   a curvature that runs from a wide entry radius to `rTop` at the top
 *   (a teardrop, as real loops are). Level lead-ins make up any length the
 *   window has to spare. `offset` shifts the exit sideways by that much (the
 *   entry by half of it back), so the loop clears itself; the shift fades in
 *   and out over `lead` m of track either side.
 * - `roll`: a corkscrew, the track rolling `turns` full turns about an axis
 *   `radius` m above it (left or right), with the roll rate eased in and out.
 * - `path`: any other element (a cobra roll, a sidewinder), drawn through
 *   `points` given in the park frame as [x, y, height above the ground]:
 *   a Hermite per span, each point's heading from its neighbours, the track's
 *   own at the ends. The riders' up is where the train pushes them (no
 *   lateral g, from the speed the energy leaves), smoothed, carried from
 *   the entry by a rotation-minimising frame and eased onto the exit's.
 */
export type Inversion =
  | { kind: 'loop'; name?: string; from: number; to: number; height: number; rTop?: number; offset?: number }
  | { kind: 'roll'; name?: string; from: number; to: number; radius: number; turns?: number; dir: 'left' | 'right' }
  | { kind: 'path'; name?: string; from: number; to: number; points: [number, number, number][] }

/**
 * A wooden trestle, as coney-island-cyclone.ts draws one: a bent (a box the
 * width of the track, `along` m long) every `every` m from the ground, or
 * from a lower stretch of track, up to the deck; and ledgers (beams) between
 * neighbouring bents every `levelStep` m of height. What is left between them
 * reads as the large openings of the lattice.
 */
export interface Trestle { every?: number; along?: number; levelStep?: number; ledgerH?: number }

export async function buildCoaster(spec: CoasterSpec) {
  const { W, RAIL, DEPTH, SPINE } = spec.track
  const [AX, AY] = spec.anchor

  // ----------------------------------------------------------- terrain ----
  function groundRaw(x: number, y: number) {
    const g = spec.ground
    const fx = Math.max(0, Math.min((x - g.x0) / g.step, g.rows[0].length - 1.001))
    const fy = Math.max(0, Math.min((y - g.y0) / g.step, g.rows.length - 1.001))
    const i = Math.floor(fx), j = Math.floor(fy), u = fx - i, v = fy - j, R = g.rows
    return (R[j][i] * (1 - u) + R[j][i + 1] * u) * (1 - v) + (R[j + 1][i] * (1 - u) + R[j + 1][i + 1] * u) * v
  }

  // -------------------------------------------------------------- plan ----
  const pts = spec.chain.map(([x, y]) => [x - AX, y - AY] as [number, number])
  const n = pts.length
  const polyS = [0]
  for (let i = 1; i < n; i++) polyS.push(polyS[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]))
  const polyTotal = polyS[n - 1] + Math.hypot(pts[0][0] - pts[n - 1][0], pts[0][1] - pts[n - 1][1])

  // Dense centripetal Catmull–Rom through the plan points (closed).
  const dense: { x: number; y: number; knot: number }[] = []
  for (let i = 0; i < n; i++) {
    let p0 = pts[(i - 1 + n) % n], p1 = pts[i], p2 = pts[(i + 1) % n], p3 = pts[(i + 2) % n]
    // A shuttle's ends don't lean towards each other: mirror the neighbour instead.
    const mirror = (a: number[], b: number[]) => [2 * a[0] - b[0], 2 * a[1] - b[1]] as [number, number]
    if (spec.shuttle && i === 0) p0 = mirror(p1, p2)
    if (spec.shuttle && i === n - 2) p3 = mirror(p2, p1)
    const d = (a: number[], b: number[]) => Math.max(Math.hypot(a[0] - b[0], a[1] - b[1]) ** 0.5, 1e-3)
    const t0 = 0, t1 = t0 + d(p0, p1), t2 = t1 + d(p1, p2), t3 = t2 + d(p2, p3)
    const SUB = Math.max(4, Math.ceil(Math.hypot(p2[0] - p1[0], p2[1] - p1[1]) / 0.5))
    for (let k = 0; k < SUB; k++) {
      const t = t1 + ((t2 - t1) * k) / SUB
      const L = (a: number[], b: number[], ta: number, tb: number) => [0, 1].map((c) => ((tb - t) * a[c] + (t - ta) * b[c]) / (tb - ta))
      const A1 = L(p0, p1, t0, t1), A2 = L(p1, p2, t1, t2), A3 = L(p2, p3, t2, t3)
      const C = L(L(A1, A2, t0, t2), L(A2, A3, t1, t3), t1, t2)
      dense.push({ x: C[0], y: C[1], knot: k === 0 ? i : -1 })
    }
  }
  const dS = [0]
  for (let i = 1; i <= dense.length; i++) {
    const a = dense[i - 1], b = dense[i % dense.length]
    dS.push(dS[i - 1] + Math.hypot(b.x - a.x, b.y - a.y))
  }
  const TOTAL = dS[dense.length]
  const knotS = pts.map((_, i) => dS[dense.findIndex((p) => p.knot === i)])
  /** Spline distance for a polyline distance, through the shared nodes. */
  function splineS(ps: number) {
    ps = ((ps % polyTotal) + polyTotal) % polyTotal
    const ks = [...knotS, TOTAL], ps0 = [...polyS, polyTotal]
    let j = 0
    while (j < ks.length - 2 && ps0[j + 1] <= ps) j++
    return ks[j] + ((ps - ps0[j]) / (ps0[j + 1] - ps0[j])) * (ks[j + 1] - ks[j])
  }
  function planAt(s: number): [number, number] {
    s = ((s % TOTAL) + TOTAL) % TOTAL
    let lo = 0, hi = dense.length
    while (hi - lo > 1) {
      const mid = (lo + hi) >> 1
      if (dS[mid] <= s) lo = mid
      else hi = mid
    }
    const a = dense[lo], b = dense[(lo + 1) % dense.length], t = (s - dS[lo]) / (dS[lo + 1] - dS[lo] || 1)
    return [a.x + (b.x - a.x) * t, a.y + (b.y - a.y) * t]
  }
  // The model's datum: the lowest ground under the track.
  let DATUM = Infinity
  for (const p of dense) DATUM = Math.min(DATUM, groundRaw(p.x + AX, p.y + AY))
  const ground = (x: number, y: number) => groundRaw(x + AX, y + AY) - DATUM
  const groundAt = (s: number) => ground(...planAt(s))
  const inRange = (s: number, [a, b]: [number, number]) => {
    const A = splineS(a), B = splineS(b)
    return A <= B ? s >= A && s <= B : s >= A || s <= B
  }

  // ----------------------------------------------------------- profile ----
  // Elements in spline distance, heights in the model frame.
  const E = spec.profile.map((e) => ({ ...e, ss: splineS(e.s), zz: e.z + groundAt(splineS(e.s)) }))
  const liftTop = E.reduce((a, b) => (b.zz > a.zz ? b : a))
  /** Height (model frame) of the element nearest a polyline s: the crest of an extra lift. */
  const heightOfElementNear = (ps: number) => E.reduce((a, b) => (Math.abs(b.ss - splineS(ps)) < Math.abs(a.ss - splineS(ps)) ? b : a)).zz
  // Energy resets: the lift crest, and the end of each brake run.
  // A launched coaster's walk starts where the tyres let go (the end of `lift`), not at its highest element.
  const launched = !!spec.launches?.length
  const liftEnd = launched ? E.reduce((a, b) => (Math.abs(b.ss - splineS(spec.lift[1])) < Math.abs(a.ss - splineS(spec.lift[1])) ? b : a)) : liftTop
  const resets = [
    launched ? { at: splineS(spec.lift[1]), e: liftEnd.zz + (spec.liftHead ?? 1) } : { at: liftTop.ss, e: liftTop.zz },
    ...spec.brakes.map((b) => ({ at: splineS(b.to), e: b.z + groundAt(splineS(b.to)) + b.head })),
    ...(spec.launches ?? []).map((l) => ({ at: splineS(l.to), e: l.z + groundAt(splineS(l.to)) + l.head })),
    ...(spec.extraLifts ?? []).map((l) => ({ at: splineS(l[1]), e: heightOfElementNear(l[1]) + (spec.liftHead ?? 1) })),
  ]
  /** Head of energy at s (m above the model datum); lenFactor turns plan length into track length. */
  function energyAt(s: number, lenFactor = 1.1) {
    let best = resets[0], run = Infinity
    for (const r of resets) {
      const d = (((s - r.at) % TOTAL) + TOTAL) % TOTAL
      if (d < run) { run = d; best = r }
    }
    return best.e - (spec.losses[0] + spec.losses[1] * 25) * run * lenFactor
  }
  // Radius of each element's arc, from the speed there unless given.
  const radius = E.map((e, i) => {
    if (e.r) return e.r
    const prev = E[(i - 1 + E.length) % E.length], next = E[(i + 1) % E.length]
    const crest = e.zz >= prev.zz && e.zz >= next.zz
    const v2 = Math.max(4, 2 * G * (energyAt(e.ss) - e.zz))
    return crest ? Math.max(16, Math.min(140, v2 / (G * 1.15))) : Math.max(10, Math.min(140, v2 / (G * 2.5)))
  })
  // One segment between elements a and b: arc, tangent, arc.
  type Seg = { s0: number; s1: number; z0: number; z1: number; ra: number; rb: number; th: number; up: boolean }
  const segs: Seg[] = E.map((a, i) => {
    const b = E[(i + 1) % E.length]
    let s1 = b.ss
    if (s1 <= a.ss) s1 += TOTAL
    const D = s1 - a.ss, H = Math.abs(b.zz - a.zz), up = b.zz > a.zz
    // Radii at the higher end are the crest's, at the lower the valley's.
    let ra = radius[i], rb = radius[(i + 1) % E.length]
    if (H < 1e-6) return { s0: a.ss, s1, z0: a.zz, z1: b.zz, ra: 0, rb: 0, th: 0, up }
    const R0 = ra + rb, Rmax = (D * D + H * H) / (2 * H)
    if (a.pitch) {
      // Arc, straight at the given pitch, arc: D = R sinθ + (H − R(1 − cosθ)) / tanθ.
      const t = (a.pitch * Math.PI) / 180
      const R = (D - H / Math.tan(t)) / (Math.sin(t) - (1 - Math.cos(t)) / Math.tan(t))
      if (R > 0 && R < Rmax) { ra *= R / R0; rb *= R / R0 }
      else console.warn(`${a.name}: ${a.pitch}° does not fit ${D.toFixed(0)} m for ${H.toFixed(0)} m; move the elements`)
    } else if (R0 > Rmax) { ra *= (Rmax / R0) * 0.999; rb *= (Rmax / R0) * 0.999 }
    const R = ra + rb, A = D, B = R - H
    const th = Math.asin(Math.min(1, R / Math.hypot(A, B))) - Math.atan2(B, A)
    return { s0: a.ss, s1, z0: a.zz, z1: b.zz, ra, rb, th, up }
  })
  function segZ(g: Seg, s: number) {
    if (s < g.s0) s += TOTAL
    const x = s - g.s0, D = g.s1 - g.s0
    if (g.ra === 0 && g.rb === 0) return g.z0 + ((g.z1 - g.z0) * x) / D
    // Work descending from the high end: h(x) from the crest at x = 0 to the valley at x = D.
    const hi = g.up ? g.z1 : g.z0, lo = g.up ? g.z0 : g.z1
    const rc = g.up ? g.rb : g.ra, rv = g.up ? g.ra : g.rb
    const xc = g.up ? D - x : x
    const sn = Math.sin(g.th), cs = Math.cos(g.th)
    if (xc <= rc * sn) return hi - rc + Math.sqrt(Math.max(0, rc * rc - xc * xc))
    if (xc >= D - rv * sn) return lo + rv - Math.sqrt(Math.max(0, rv * rv - (D - xc) ** 2))
    const z0 = hi - rc * (1 - cs)
    return z0 - Math.tan(g.th) * (xc - rc * sn)
  }
  function segFor(s: number) {
    s = ((s % TOTAL) + TOTAL) % TOTAL
    for (const g of segs) if ((s >= g.s0 && s < g.s1) || (g.s1 > TOTAL && s + TOTAL < g.s1)) return g
    return segs[0]
  }
  // How far into a below-grade stretch s is, 0 → 1 over its first and last 10 m (smoothstep).
  const belowW = (s: number) => {
    let w = 0
    for (const [f, t] of spec.belowGrade ?? []) {
      const A = splineS(f), B = splineS(t)
      const d = Math.min((((s - A) % TOTAL) + TOTAL) % TOTAL, (((B - s) % TOTAL) + TOTAL) % TOTAL)
      if (!inRange(s, [f, t])) continue
      const u = Math.min(1, d / 10)
      w = Math.max(w, u * u * (3 - 2 * u))
    }
    return w
  }
  const heightRaw = (s: number) => segZ(segFor(s), s)
  // Never under the ground, except where the ride really is. A smooth
  // maximum (k = 1.5 m) so the clamp itself can't put a kink in the track.
  const smax = (a: number, b: number, k = 1.5) => { const h = Math.max(0, Math.min(1, 0.5 + (0.5 * (a - b)) / k)); return b + (a - b) * h + k * h * (1 - h) }
  const heightAt = (s: number) => {
    const z = heightRaw(s)
    // In a trench the floor drops away smoothly instead of switching off.
    return smax(z, groundAt(s) + (spec.inverted ? spec.inverted.clear ?? 4 : 0.6) - 12 * belowW(s))
  }

  // --------------------------------------------- frames along the track --
  const STEP = 1
  const N = Math.round(TOTAL / STEP)
  const heading: number[] = [], zs: number[] = []
  for (let i = 0; i < N; i++) {
    const s = (i * TOTAL) / N
    const [x0, y0] = planAt(s - 1), [x1, y1] = planAt(s + 1)
    heading.push(Math.atan2(y1 - y0, x1 - x0))
    zs.push(heightAt(s))
  }
  // Real 3D length, to refine the friction estimate.
  let len3 = 0
  const cum3 = [0]
  for (let i = 1; i <= N; i++) {
    const s0 = ((i - 1) * TOTAL) / N, s1 = (i * TOTAL) / N
    len3 += Math.hypot(s1 - s0, zs[i % N] - zs[i - 1])
    cum3.push(len3)
  }
  const lenFactor = len3 / TOTAL
  const onLift = (s: number) => inRange(s, spec.lift) || (spec.extraLifts ?? []).some((l) => inRange(s, l))
  const levelAt = (s: number) => onLift(s) || spec.brakes.some((b) => inRange(s, [b.from, b.to]))
  const onChainOrBrakes = levelAt
  // Head of energy along the track: walk on from the lift crest, losing
  // rolling resistance (a per metre) and air drag (b × head per metre, since
  // drag goes with v²); the chain holds the train at lift speed and the brake
  // runs set it to their exit speed.
  const head = new Array<number>(N).fill(0)
  {
    const [a, b] = spec.losses
    const i0 = Math.round((launched ? splineS(spec.lift[1]) : liftTop.ss) / STEP) % N
    let e = zs[i0] + (spec.liftHead ?? 1)
    // A launch's head on entry, so its thrust can raise the head evenly to the target.
    const launchIn = new Map<object, { h: number; A: number; len: number }>()
    for (let k = 0; k < N; k++) {
      const i = (i0 + k) % N, s = (i * TOTAL) / N
      if (onLift(s)) e = zs[i] + (spec.liftHead ?? 1)
      for (const l of spec.launches ?? []) {
        if (!inRange(s, [l.from, l.to])) continue
        const A = splineS(l.from), len = (splineS(l.to) - A + TOTAL) % TOTAL || 1
        if (!launchIn.has(l)) launchIn.set(l, { h: e - zs[i], A, len })
        const st = launchIn.get(l)!, u = Math.min(1, ((s - A + TOTAL) % TOTAL) / len)
        e = Math.max(e, zs[i] + st.h + (l.head - st.h) * u)
      }
      const br = spec.brakes.find((x) => inRange(s, [x.from, x.to]))
      if (br) e = Math.min(e, zs[i] + br.head)
      head[i] = e - zs[i]
      const dl = Math.hypot(TOTAL / N, zs[(i + 1) % N] - zs[i])
      e -= (a + b * Math.max(0, head[i])) * dl
    }
  }
  const turn = (i: number) => {
    let d = heading[(i + 8) % N] - heading[(i - 8 + N) % N]
    while (d > Math.PI) d -= 2 * Math.PI
    while (d < -Math.PI) d += 2 * Math.PI
    return d / (16 * STEP)
  }
  // Bank angle: atan(v²κ / g), from the speed the energy leaves; level on the
  // lift and brakes. κ is the lateral curvature the rider feels: the plan
  // turn per metre of track, times cos² of the pitch, so a steep drop that
  // happens to curve in plan doesn't corkscrew (its turn is mostly a twist of
  // heading about a near-vertical track, not a sideways push).
  const pitchAt = (i: number) => Math.atan2(zs[(i + 1) % N] - zs[(i - 1 + N) % N], 2 * STEP)
  const rawBank = heading.map((_, i) => {
    const s = (i * TOTAL) / N
    if (levelAt(s)) return 0
    const v2 = 2 * G * Math.max(0, head[i])
    return Math.atan((v2 * turn(i) * Math.cos(pitchAt(i)) ** 2) / G)
  })
  // Smooth it twice over ±20 m (a wide, C1 kernel), then cap the roll rate at
  // 4° per metre so the ribbon never twists abruptly.
  const blur = (a: number[], r: number) => a.map((_, i) => {
    let sum = 0, w = 0
    for (let k = -r; k <= r; k++) { const wk = r + 1 - Math.abs(k); sum += a[(i + k + N) % N] * wk; w += wk }
    return sum / w
  })
  let bank = blur(blur(rawBank, 20), 20).map((b) => Math.max(-1.45, Math.min(1.45, b)))
  // Hand-set banking (overbanked turns), eased in and out over 30 m.
  // An inversion starts and ends level (see `Inversion`).
  const invBanks = (spec.inversions ?? []).map((v) => ({ from: v.from, to: v.to, deg: 0, name: v.name }))
  for (const o of [...(spec.banks ?? []), ...invBanks]) {
    const A = splineS(o.from), B = splineS(o.to)
    let len = B - A
    if (len < 0) len += TOTAL
    let turnSum = 0
    for (let d = 0; d <= len; d++) turnSum += turn(Math.round(((A + d) % TOTAL) / STEP) % N)
    const sign = Math.sign(turnSum) || 1
    const EASE = 30
    for (let d = -EASE; d <= len + EASE; d++) {
      const i = Math.round((((A + d) % TOTAL) + TOTAL) % TOTAL / STEP) % N
      const w = d < 0 ? 0.5 - 0.5 * Math.cos((Math.PI * (d + EASE)) / EASE) : d > len ? 0.5 - 0.5 * Math.cos((Math.PI * (len + EASE - d)) / EASE) : 1
      bank[i] = bank[i] * (1 - w) + sign * ((o.deg * Math.PI) / 180) * w
    }
  }
  // Roll-rate cap, both ways round so it doesn't lag.
  const MAX_ROLL = (4 * Math.PI) / 180
  for (let pass = 0; pass < 3; pass++) {
    for (let i = 1; i < 2 * N; i++) { const a = bank[(i - 1) % N], k = i % N; bank[k] = Math.max(a - MAX_ROLL, Math.min(a + MAX_ROLL, bank[k])) }
    for (let i = 2 * N - 2; i >= 0; i--) { const a = bank[(i + 1) % N], k = i % N; bank[k] = Math.max(a - MAX_ROLL, Math.min(a + MAX_ROLL, bank[k])) }
  }
  // Fast hand rolls (half inversions drawn as hairpins), after the cap; see `rolls`.
  for (const o of spec.rolls ?? []) {
    const A = splineS(o.from), B = splineS(o.to), EASE = o.ease ?? 20
    const len = (B - A + TOTAL) % TOTAL
    let turnSum = 0
    for (let d = 0; d <= len; d++) turnSum += turn(Math.round(((A + d) % TOTAL) / STEP) % N)
    const sign = Math.sign(turnSum) || 1
    for (let d = -EASE; d <= len + EASE; d++) {
      const i = Math.round((((A + d) % TOTAL) + TOTAL) % TOTAL / STEP) % N
      const w = d < 0 ? 0.5 - 0.5 * Math.cos((Math.PI * (d + EASE)) / EASE) : d > len ? 0.5 - 0.5 * Math.cos((Math.PI * (len + EASE - d)) / EASE) : 1
      bank[i] = bank[i] * (1 - w) + sign * ((o.deg * Math.PI) / 180) * w
    }
  }
  type Sample = { p: V3; t: V3; left: V3; up: V3; s: number }
  const fine: Sample[] = []
  for (let i = 0; i < N; i++) {
    const s = (i * TOTAL) / N
    const [x, y] = planAt(s)
    const [xa, ya] = planAt(s + 0.8), [xb, yb] = planAt(s - 0.8)
    const t = unit([xa - xb, ya - yb, heightAt(s + 0.8) - heightAt(s - 0.8)])
    const up0 = unit(sub([0, 0, 1], mul(t, t[2])))
    const left0 = cross(up0, t)
    const c = Math.cos(bank[i]), sn = Math.sin(bank[i])
    fine.push({ p: [x, y, zs[i]], t, left: unit(sub(mul(left0, c), mul(up0, sn))), up: unit(add(mul(up0, c), mul(left0, sn))), s })
  }
  // Keep a sample wherever the track has turned, pitched or rolled enough, and at least every 12 m.
  const TURN = Math.cos(((spec.sampleDeg ?? 9) * Math.PI) / 180), ROLL = Math.cos((12 * Math.PI) / 180)
  const inv = buildInversions()
  // A shuttle's track ends at the last node: drop the closing stretch back to the first.
  const SHUTTLE_END = spec.shuttle ? knotS[n - 1] : Infinity
  const path = spec.shuttle ? inv.path.filter((q) => q.s <= SHUTTLE_END + 1e-6) : inv.path
  const samples: Sample[] = [path[0]]
  for (let i = 1; i < path.length; i++) {
    const last = samples[samples.length - 1], q = path[i]
    if (Math.hypot(...sub(q.p, last.p)) >= 12 || dot(q.t, last.t) < TURN || dot(q.up, last.up) < ROLL) samples.push(q)
  }

  // -------------------------------------------------------- inversions ----
  /**
   * The centreline with each inversion's window replaced by its own dense
   * samples (every 0.5 m). Without inversions it is `fine`.
   */
  function buildInversions() {
    const list = spec.inversions ?? []
    if (!list.length) return { path: fine, windows: [] as { a: number; b: number; inv: Inversion; pts: Sample[]; meta: Record<string, number> }[] }
    const Z: V3 = [0, 0, 1]
    const idx = (ps: number) => Math.round(splineS(ps) / STEP) % N
    const windows: { a: number; b: number; inv: Inversion; pts: Sample[]; meta: Record<string, number> }[] = []
    for (const v of list) {
      const a = idx(v.from), b = idx(v.to)
      const A = fine[a], B = fine[b]
      const chord = Math.hypot(...sub(B.p, A.p))
      const TA = mul(A.t, chord), TB = mul(B.t, chord)
      const herm = (u: number): V3 => {
        const h00 = 2 * u ** 3 - 3 * u * u + 1, h10 = u ** 3 - 2 * u * u + u, h01 = -2 * u ** 3 + 3 * u * u, h11 = u ** 3 - u * u
        return add(add(mul(A.p, h00), mul(TA, h10)), add(mul(B.p, h01), mul(TB, h11)))
      }
      const sAt = (u: number) => A.s + u * ((B.s - A.s + TOTAL) % TOTAL)
      const raw: { p: V3; up: V3; u: number }[] = []
      const meta: Record<string, number> = {}
      if (v.kind === 'loop') {
        const f = unit([B.p[0] - A.p[0], B.p[1] - A.p[1], 0]), l: V3 = [-f[1], f[0], 0]
        const D = dot(sub(B.p, A.p), f)
        // Radius from rBot at the entry (ψ = 0) to rTop at the top (ψ = π): R = rTop + (rBot − rTop)·((1 + cos ψ)/2)^1.5.
        const P = 1.5, rTop = v.rTop ?? Math.max(4, v.height * 0.24)
        const rBot = rTop + ((v.height - 2 * rTop) * (P + 1)) / 2
        const loop: [number, number, number][] = [[0, 0, 0]]
        let x = 0, z = 0
        const K = 4000
        for (let k = 0; k < K; k++) {
          const psi = ((k + 0.5) / K) * 2 * Math.PI, R = rTop + (rBot - rTop) * ((1 + Math.cos(psi)) / 2) ** P, ds = (R * 2 * Math.PI) / K
          x += Math.cos(psi) * ds; z += Math.sin(psi) * ds
          loop.push([x, z, ((k + 1) / K) * 2 * Math.PI])
        }
        const dx = x
        let kx = 1, lead = (D - dx) / 2
        if (lead < 0) { kx = D / dx; lead = 0; console.warn(`${v.name ?? 'loop'}: needs ${dx.toFixed(1)} m of window, has ${D.toFixed(1)}; squeezed`) }
        // Local path: lead-in, loop, lead-out, as (x along, z up, ψ).
        const loc: [number, number, number][] = []
        for (let d = 0; d < lead; d += 0.5) loc.push([d, 0, 0])
        let last = loc.length ? loc[loc.length - 1] : [-1, 0, 0]
        for (const [px, pz, ps] of loop) {
          const q: [number, number, number] = [lead + px * kx, pz, ps]
          if (Math.hypot(q[0] - last[0], q[1] - last[1]) >= 0.5) { loc.push(q); last = q }
        }
        for (let d = lead + dx * kx + 0.5; d <= D; d += 0.5) loc.push([d, 0, 2 * Math.PI])
        let tot = 0
        const cum = loc.map((q, i) => (i ? (tot += Math.hypot(q[0] - loc[i - 1][0], q[1] - loc[i - 1][1])) : 0))
        const off = v.offset ?? 0
        loc.forEach((q, i) => {
          const u = cum[i] / tot
          const p = add(add(herm(u), mul(f, q[0] - D * u)), add(mul(Z, q[1]), mul(l, -off * Math.sin(2 * Math.PI * u) * Math.sin(Math.PI * u) ** 2)))
          // Riders' up points into the loop: the local tangent turned 90° up.
          const n = loc[Math.min(i + 1, loc.length - 1)], pr = loc[Math.max(i - 1, 0)]
          const cx = n[0] - pr[0], cz = n[1] - pr[1], cl = Math.hypot(cx, cz) || 1
          raw.push({ p, up: add(mul(f, -cz / cl), mul(Z, cx / cl)), u })
        })
        Object.assign(meta, { rTop, rBot, dx: dx * kx, D, arc: tot, height: v.height })
      } else if (v.kind === 'path') {
        // Through the points: a Hermite per span, scaled by its chord.
        const P: V3[] = [A.p, ...v.points.map(([x, y, h]): V3 => [x - AX, y - AY, ground(x - AX, y - AY) + h]), B.p]
        const T = P.map((p, i) => (i === 0 ? A.t : i === P.length - 1 ? B.t : unit(sub(P[i + 1], P[i - 1]))))
        const curve: V3[] = []
        for (let i = 0; i + 1 < P.length; i++) {
          const L = Math.hypot(...sub(P[i + 1], P[i])), K = Math.max(4, Math.ceil(L / 0.1))
          for (let k = 0; k < K; k++) {
            const u = k / K, h00 = 2 * u ** 3 - 3 * u * u + 1, h10 = u ** 3 - 2 * u * u + u, h01 = -2 * u ** 3 + 3 * u * u, h11 = u ** 3 - u * u
            curve.push(add(add(mul(P[i], h00), mul(T[i], L * h10)), add(mul(P[i + 1], h01), mul(T[i + 1], L * h11))))
          }
        }
        curve.push(B.p)
        // Every 0.5 m along it.
        const q: V3[] = [curve[0]]
        let acc = 0
        for (let i = 1; i < curve.length; i++) {
          acc += Math.hypot(...sub(curve[i], curve[i - 1]))
          if (acc >= 0.5 || i === curve.length - 1) { q.push(curve[i]); acc = 0 }
        }
        const K = q.length, DS = 0.5
        // Spread the bends: the points are guides, not knots. Two passes of a
        // ±2 m moving average, the ends held where the track joins.
        for (let pass = 0; pass < 3; pass++) {
          const o = q.map((p) => [...p] as V3)
          // Run the window right up to the joins over ghost points on the
          // track either side, and fade the smoothing in over the first and
          // last 4 m, so the curve still leaves and meets the track on the
          // Hermite's tangent; stopping the window short of the ends left a
          // kink 3 m in from each join.
          const at = (j: number): V3 => (j < 0 ? add(o[0], mul(A.t, j * DS)) : j >= K ? add(o[K - 1], mul(B.t, (j - K + 1) * DS)) : o[j])
          for (let i = 1; i < K - 1; i++) {
            let c: V3 = [0, 0, 0]
            for (let k = -4; k <= 4; k++) c = add(c, at(i + k))
            const u = Math.min(1, Math.min(i, K - 1 - i) / 8), w = u * u * (3 - 2 * u)
            q[i] = add(o[i], mul(sub(mul(c, 1 / 9), o[i]), w))
          }
        }
        const t = q.map((_, i) => (i === 0 ? A.t : i === K - 1 ? B.t : unit(sub(q[i + 1], q[i - 1]))))
        // Speed from the head at the entry, less the height gained and the losses on the way.
        const sp: number[] = []
        let e = head[a] + A.p[2]
        for (let i = 0; i < K; i++) {
          sp.push(2 * G * Math.max(0.5, e - q[i][2]))
          e -= (spec.losses[0] + spec.losses[1] * Math.max(0, e - q[i][2])) * DS
        }
        // Rotation-minimising frame from the entry's (double reflection).
        const ups: V3[] = [A.up]
        for (let i = 0; i + 1 < K; i++) {
          const v1 = sub(q[i + 1], q[i]), c1 = dot(v1, v1) || 1e-9
          const rL = sub(ups[i], mul(v1, (2 / c1) * dot(v1, ups[i]))), tL = sub(t[i], mul(v1, (2 / c1) * dot(v1, t[i])))
          const v2 = sub(t[i + 1], tL), c2 = dot(v2, v2)
          const r = c2 < 1e-12 ? rL : sub(rL, mul(v2, (2 / c2) * dot(v2, rL)))
          ups.push(unit(sub(r, mul(t[i + 1], dot(r, t[i + 1])))))
        }
        // Where the train pushes the riders: v²κ + g, across the track; as a weighted vector so weak pushes count little.
        const vec = q.map((_, i) => {
          const i0 = Math.max(0, i - 3), i1 = Math.min(K - 1, i + 3)
          const k = mul(sub(t[i1], t[i0]), 1 / ((i1 - i0) * DS || 1))
          const F = add(mul(k, sp[i]), [0, 0, G] as V3), Fp = sub(F, mul(t[i], dot(F, t[i])))
          const left = cross(ups[i], t[i])
          return [dot(Fp, ups[i]), dot(Fp, left)]
        })
        const R = 8 // ±4 m
        let prev = 0
        const phi = vec.map((_, i) => {
          let cx = 0, cy = 0
          for (let k = -R; k <= R; k++) { const j = Math.max(0, Math.min(K - 1, i + k)), w = R + 1 - Math.abs(k); cx += vec[j][0] * w; cy += vec[j][1] * w }
          let f = Math.atan2(cy, cx)
          while (f - prev > Math.PI) f -= 2 * Math.PI
          while (f - prev < -Math.PI) f += 2 * Math.PI
          return (prev = f)
        })
        // Ease onto the entry's frame and the exit's.
        const lastLeft = cross(ups[K - 1], t[K - 1])
        let fB = Math.atan2(dot(B.up, lastLeft), dot(B.up, ups[K - 1]))
        while (fB - phi[K - 1] > Math.PI) fB -= 2 * Math.PI
        while (fB - phi[K - 1] < -Math.PI) fB += 2 * Math.PI
        const d0 = -phi[0], d1 = fB - phi[K - 1]
        let arc = 0
        q.forEach((p, i) => {
          const u = i / (K - 1), f = phi[i] + d0 * (1 - u) + d1 * u
          const left = cross(ups[i], t[i])
          raw.push({ p, up: add(mul(ups[i], Math.cos(f)), mul(left, Math.sin(f))), u })
          if (i) arc += Math.hypot(...sub(p, q[i - 1]))
        })
        Object.assign(meta, { arc, D: chord, top: Math.max(...q.map((p, i) => p[2] - ground(p[0], p[1]))) })
      } else {
        const K = Math.max(8, Math.ceil(chord / 0.5)), turns = v.turns ?? 1, sg = v.dir === 'left' ? 1 : -1
        for (let k = 0; k <= K; k++) {
          const u = k / K
          const e = 1e-3, t0 = unit(sub(herm(Math.min(1, u + e)), herm(Math.max(0, u - e))))
          const up0 = unit(sub(Z, mul(t0, t0[2]))), left0 = cross(up0, t0)
          const al = sg * 2 * Math.PI * turns * (u - Math.sin(2 * Math.PI * u) / (2 * Math.PI))
          const upA = add(mul(up0, Math.cos(al)), mul(left0, Math.sin(al)))
          raw.push({ p: add(herm(u), mul(sub(up0, upA), v.radius)), up: upA, u })
        }
        Object.assign(meta, { radius: v.radius, turns, D: chord })
      }
      // Frames from the dense points.
      const pts: Sample[] = raw.map((r, i) => {
        const t = unit(sub(raw[Math.min(i + 1, raw.length - 1)].p, raw[Math.max(i - 1, 0)].p))
        const left = unit(cross(r.up, t))
        return { p: r.p, t, left, up: cross(t, left), s: sAt(r.u) }
      })
      if (dot(A.t, unit([B.p[0] - A.p[0], B.p[1] - A.p[1], 0])) < Math.cos((4 * Math.PI) / 180) && v.kind === 'loop')
        console.warn(`${v.name ?? 'loop'}: the track enters ${((Math.acos(dot(A.t, unit([B.p[0] - A.p[0], B.p[1] - A.p[1], 0]))) * 180) / Math.PI).toFixed(0)}° off the loop's line; move from/to onto a straight`)
      windows.push({ a, b, inv: v, pts, meta })
    }
    windows.sort((x, y) => x.a - y.a)
    const out: Sample[] = []
    let k = 0, w = 0
    while (k < N) {
      const win = windows[w]
      if (win && k === win.a) { out.push(...win.pts); k = win.b + 1; w++ }
      else out.push(fine[k++])
    }
    return { path: out, windows }
  }
  const inWindow = (s: number) => inv.windows.some((w) => inRange(s, [w.inv.from - 2, w.inv.to + 2]))
  const M = samples.length

  // ------------------------------------------------------------- track ----
  const deck = new Part(), white = new Part(), roofPart = new Part(), trimPart = new Part()
  const spine = spec.colours.spine === 'deck' ? deck : new Part()
  function face(p: Part, P: V3[], Nn: V3[] | V3) {
    const ns = (Array.isArray(Nn[0]) ? Nn : [Nn, Nn, Nn, Nn]) as V3[]
    const f = cross(sub(P[1], P[0]), sub(P[2], P[0]))
    const avg = ns.reduce(add, [0, 0, 0] as V3)
    const [a, b, c, d] = dot(f, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
    p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [ns[a], ns[b], ns[c]])
    p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [ns[a], ns[c], ns[d]])
  }
  function section(q: Sample) {
    const at = (l: number, u: number) => add(q.p, add(mul(q.left, l), mul(q.up, u)))
    return { L: at(W / 2, 0), R: at(-W / 2, 0), Lr: at(W / 2 - 0.15, -RAIL), Rr: at(-W / 2 + 0.15, -RAIL), Lb: at(SPINE / 2, -DEPTH), Rb: at(-SPINE / 2, -DEPTH) }
  }
  // An inverted track is the same section turned half over about the tangent: spine above, deck below.
  const flip = (q: Sample): Sample => (spec.inverted ? { ...q, up: mul(q.up, -1), left: mul(q.left, -1) } : q)
  for (let i = 0; i < (spec.shuttle ? M - 1 : M); i++) {
    const a = flip(samples[i]), b = flip(samples[(i + 1) % M])
    const A = section(a), B = section(b)
    const sideN = (q: Sample, sg: number) => unit(add(mul(q.left, sg), mul(q.up, -0.25)))
    face(deck, [A.L, A.R, B.R, B.L], [a.up, a.up, b.up, b.up])
    face(deck, [A.L, B.L, B.Lr, A.Lr], [sideN(a, 1), sideN(b, 1), sideN(b, 1), sideN(a, 1)])
    face(deck, [A.R, A.Rr, B.Rr, B.R], [sideN(a, -1), sideN(a, -1), sideN(b, -1), sideN(b, -1)])
    const lN = (q: Sample, sg: number) => unit(add(mul(q.left, sg), mul(q.up, -0.5)))
    face(spine, [A.Lr, B.Lr, B.Lb, A.Lb], [lN(a, 1), lN(b, 1), lN(b, 1), lN(a, 1)])
    face(spine, [A.Rr, A.Rb, B.Rb, B.Rr], [lN(a, -1), lN(a, -1), lN(b, -1), lN(b, -1)])
    const da = mul(a.up, -1), db = mul(b.up, -1)
    face(spine, [A.Lb, B.Lb, B.Rb, A.Rb], [da, db, db, da])
  }

  // ---------------------------------------------------------- supports ----
  /** A six-sided tube from a to b, radius r, smooth-shaded, open ends. */
  function tube(p: Part, a: V3, b: V3, r: number) {
    if (spec.boxSupports) return box(p, a, b, r)
    const ax = unit(sub(b, a))
    const ref: V3 = Math.abs(ax[2]) < 0.9 ? [0, 0, 1] : [1, 0, 0]
    const u = unit(cross(ax, ref)), v = cross(ax, u)
    const ring = (c: V3, k: number) => {
      const ang = (k / 6) * 2 * Math.PI, nrm = add(mul(u, Math.cos(ang)), mul(v, Math.sin(ang)))
      return { P: add(c, mul(nrm, r)), N: nrm }
    }
    for (let k = 0; k < 6; k++) {
      const a0 = ring(a, k), a1 = ring(a, k + 1), b0 = ring(b, k), b1 = ring(b, k + 1)
      face(p, [a0.P, a1.P, b1.P, b0.P], [a0.N, a1.N, b1.N, b0.N])
    }
  }
  /** A square box from a to b, `r` from its axis to each face, flat-shaded, open ends; one face kept level where it can be. */
  function box(p: Part, a: V3, b: V3, r: number) {
    const ax = unit(sub(b, a))
    const ref: V3 = Math.abs(ax[2]) < 0.9 ? [0, 0, 1] : [1, 0, 0]
    const u = unit(cross(ax, ref)), v = cross(ax, u)
    const n = [u, v, mul(u, -1), mul(v, -1)]
    for (let k = 0; k < 4; k++) {
      const c0 = mul(add(n[k], n[(k + 3) % 4]), r), c1 = mul(add(n[k], n[(k + 1) % 4]), r)
      face(p, [add(a, c0), add(a, c1), add(b, c1), add(b, c0)], n[k])
    }
  }
  /**
   * Legs for an inverted track at q, `zr` m up (see `InvertedOpts`): segments
   * to draw as supports, or null where none fits clear of the track and of
   * the trains hanging under it.
   */
  function invertedLegs(q: Sample, zr: number): [V3, V3][] | null {
    const reach = spec.inverted?.reach ?? W / 2 + 2.8
    const top = add(q.p, mul(q.up, DEPTH - 0.3))
    const gz = (x: number, y: number) => ground(x, y) - 1
    // Clear of every other stretch of track, and of the train 2.5 m under it.
    const clear = (a: V3, b: V3) => {
      const L = Math.hypot(...sub(b, a))
      for (let d = 0; d <= L; d += 1.5) {
        const c = add(a, mul(sub(b, a), d / Math.max(L, 1e-6)))
        for (const f of samples) {
          const ds = Math.abs(f.s - q.s)
          if (Math.min(ds, TOTAL - ds) < 14) continue
          if (Math.hypot(...sub(f.p, c)) < W * 0.75 + 0.9 || Math.hypot(...sub(add(f.p, mul(f.up, -2.5)), c)) < 2.6) return false
        }
      }
      return true
    }
    // Rolled over far enough that the spine faces down: a column straight up to it.
    if (q.up[2] < -0.3) {
      const leg: [V3, V3] = [[top[0], top[1], gz(top[0], top[1])], top]
      return top[2] - leg[0][2] > 2 && clear(...leg) ? [leg] : null
    }
    if (q.up[2] < 0.5) return null
    const across = unit([q.left[0], q.left[1], 0]), zBeam = top[2] + 0.9
    const at = (sg: number): V3 => [q.p[0] + across[0] * reach * sg, q.p[1] + across[1] * reach * sg, zBeam]
    if (zr >= 18) {
      // A portal: two columns and a crossbeam over the spine.
      const L = at(1), R = at(-1)
      const legs: [V3, V3][] = [[[L[0], L[1], gz(L[0], L[1])], L], [[R[0], R[1], gz(R[0], R[1])], R], [L, R]]
      if (legs.every(([a, b]) => clear(a, b))) return legs
    }
    // An L: a column on the side the track leans to, an arm across, and a short drop onto the spine.
    const lean = Math.sign(dot(q.up, across)) || 1
    for (const sg of [lean, -lean]) {
      const C = at(sg), over: V3 = [top[0], top[1], zBeam]
      const legs: [V3, V3][] = [[[C[0], C[1], gz(C[0], C[1])], C], [C, over], [over, top]]
      if (legs.every(([a, b]) => clear(a, b))) return legs
    }
    return null
  }
  function clearOf(q: V3, s0: number, skip = 14) {
    let best = Infinity
    for (const f of samples) {
      const ds = Math.abs(f.s - s0)
      if (Math.min(ds, TOTAL - ds) < skip) continue
      best = Math.min(best, Math.hypot(...sub(f.p, q)))
    }
    return best
  }
  function legClear(a: V3, b: V3, s0: number) {
    const L = Math.hypot(...sub(b, a))
    for (let d = 0; d <= L; d += 1.5) if (clearOf(add(a, mul(sub(b, a), d / L)), s0) < W * 0.75 + 0.9) return false
    return true
  }
  const supports: { s: number; legs: number }[] = []
  // Stretches carried by hand-placed bents instead (see `bents`), in spline s.
  const bentRuns = (spec.bents ?? []).map((b) => [splineS(b.from), splineS(b.to)])
  const inBents = (s: number) => bentRuns.some(([a, b]) => (a <= b ? s >= a && s <= b : s >= a || s <= b))
  for (let s = 4; s < TOTAL - 4 && !spec.trestle; s += spec.supportEvery ?? 17) {
    for (const off of [0, 4, -4, 8, -8]) {
      const q = fine[Math.round((s + off) / STEP) % N]
      const zr = q.p[2] - ground(q.p[0], q.p[1])
      if (zr < (spec.supportMinZ ?? 2.2)) break
      if (inv.windows.length && inWindow(q.s)) break
      if (bentRuns.length && inBents(q.s)) break
      if (q.s > SHUTTLE_END - 2) break
      if (spec.inverted) {
        if (zr < 3) break
        const legs = invertedLegs(q, zr)
        if (!legs) continue
        const r = zr < 16 ? spec.supportR[0] : zr < 45 ? spec.supportR[1] : spec.supportR[2]
        for (const [a, b] of legs) tube(white, a, b, r)
        supports.push({ s: q.s, legs: legs.length })
        break
      }
      const top = add(q.p, mul(q.up, -DEPTH + 0.3))
      // Low track: one column straight down. High track: an A-frame splayed across the track.
      const across = unit([q.left[0], q.left[1], 0])
      const legs: [V3, V3][] = []
      if (zr < 16) legs.push([[top[0], top[1], ground(top[0], top[1]) - 1], top])
      else {
        const spread = Math.min(0.3 * zr, 22)
        for (const sg of [1, -1]) {
          const fx = top[0] + across[0] * spread * sg, fy = top[1] + across[1] * spread * sg
          legs.push([[fx, fy, ground(fx, fy) - 1], top])
        }
      }
      if (!legs.every(([a, b]) => legClear(a, b, q.s))) continue
      const r = zr < 16 ? spec.supportR[0] : zr < 45 ? spec.supportR[1] : spec.supportR[2]
      for (const [a, b] of legs) tube(white, a, b, r)
      supports.push({ s: q.s, legs: legs.length })
      break
    }
  }
  // Inversions stand on columns from the ground to the outside of the track
  // where it is on its side: the front and back of a loop, and each quarter
  // turn of a corkscrew where the spine faces outwards.
  for (const w of inv.windows) {
    const pts = w.pts
    const picks: Sample[] = []
    for (let i = 1; i < pts.length - 1; i++) {
      const sideways = Math.abs(pts[i].up[2]) < 0.25 && Math.abs(pts[i - 1].up[2]) >= Math.abs(pts[i].up[2]) && Math.abs(pts[i + 1].up[2]) > Math.abs(pts[i].up[2])
      if (sideways && (w.inv.kind !== 'loop' || Math.abs(pts[i].t[2]) > 0.9)) picks.push(pts[i])
    }
    for (const q of picks) {
      const top = add(q.p, mul(q.up, spec.inverted ? DEPTH - 0.3 : -DEPTH + 0.3))
      const foot: V3 = [top[0], top[1], ground(top[0], top[1]) - 1]
      if (top[2] - foot[2] < 3) continue
      tube(white, foot, top, spec.supportR[1])
      supports.push({ s: q.s, legs: 1 })
    }
  }
  // Hand-placed bents: a column and one strut from the same head, tied once; see `bents`.
  for (const run of spec.bents ?? []) {
    for (const b of run.at) {
      const q = fine[Math.round(splineS(b.s) / STEP) % N]
      const top = add(q.p, mul(q.up, -DEPTH + 0.3)), g = ground(top[0], top[1]), h = top[2] - g
      if (h < 3) continue
      const flat = (v: V3) => unit([v[0], v[1], 0])
      const dir = b.strut === 'left' ? flat(q.left) : b.strut === 'right' ? mul(flat(q.left), -1) : b.strut === 'ahead' ? flat(q.t) : mul(flat(q.t), -1)
      const off = (b.spread ?? 0.35) * h, fx = top[0] + dir[0] * off, fy = top[1] + dir[1] * off
      const col: [V3, V3] = [[top[0], top[1], g - 1], top], strut: [V3, V3] = [[fx, fy, ground(fx, fy) - 1], top]
      const r = h < 16 ? spec.supportR[0] : h < 45 ? spec.supportR[1] : spec.supportR[2]
      tube(white, ...col, r)
      tube(white, ...strut, r * 0.8)
      const tie = b.tie ?? 0.3
      if (tie > 0) {
        const z = g + tie * h, k = (z - strut[0][2]) / (top[2] - strut[0][2])
        tube(white, [top[0], top[1], z], add(strut[0], mul(sub(top, strut[0]), k)), r * 0.6)
      }
      if (![col, strut].every(([a, c]) => legClear(a, c, q.s))) console.warn(`bent at s ${b.s} meets other track`)
      supports.push({ s: q.s, legs: tie > 0 ? 3 : 2 })
    }
  }
  // Masts carrying a helix on spokes; see `hubs`.
  for (const h of spec.hubs ?? []) {
    const hx = h.at[0] - AX, hy = h.at[1] - AY, g = ground(hx, hy)
    const head: V3 = [hx, hy, g + h.top]
    tube(white, [hx, hy, g - 1], head, spec.supportR[2])
    const A = splineS(h.from), len = (splineS(h.to) - A + TOTAL) % TOTAL, EVERY = h.every ?? 8
    for (let d = 0; d <= len + 1e-6; d += EVERY) {
      const q = fine[Math.round((A + d) / STEP) % N]
      // The spine: above the rails on an inverted track, under them otherwise.
      tube(white, [hx, hy, head[2] - 0.6], add(q.p, mul(q.up, spec.inverted ? DEPTH - 0.3 : -DEPTH + 0.3)), spec.supportR[0])
    }
    supports.push({ s: A, legs: 1 })
  }

  // ----------------------------------------------------------- trestle ----
  if (spec.trestle) {
    const T = spec.trestle, EVERY = T.every ?? 5, ALONG = T.along ?? 2.4, STEPZ = T.levelStep ?? 3.8, LH = T.ledgerH ?? 1.4
    /**
     * A vertical box between two plan points, `w` across, from z0 to z1. Its
     * top slopes across by `slope` (rise per metre to the left, the track's
     * bank), so it meets the underside of a banked track along its width.
     */
    const post = (a: [number, number], b: [number, number], w: number, z0: number, z1: number, slope = 0) => {
      const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1, nx = -dy / l, ny = dx / l
      const c: [number, number][] = [[a[0] + (nx * w) / 2, a[1] + (ny * w) / 2], [b[0] + (nx * w) / 2, b[1] + (ny * w) / 2], [b[0] - (nx * w) / 2, b[1] - (ny * w) / 2], [a[0] - (nx * w) / 2, a[1] - (ny * w) / 2]]
      const out: V3[] = [[nx, ny, 0], [dx / l, dy / l, 0], [-nx, -ny, 0], [-dx / l, -dy / l, 0]]
      // Corners 0 and 1 are on the left (+n) side, 2 and 3 on the right.
      const zt = [1, 1, -1, -1].map((sg) => z1 + (sg * slope * w) / 2)
      for (let i = 0; i < 4; i++) { const j = (i + 1) % 4; face(white, [[c[i][0], c[i][1], z0], [c[j][0], c[j][1], z0], [c[j][0], c[j][1], zt[j]], [c[i][0], c[i][1], zt[i]]], out[i]) }
    }
    /** A horizontal beam from a to b: its long sides and top. */
    const ledger = (a: [number, number], b: [number, number], w: number, z0: number, z1: number) => {
      const dx = b[0] - a[0], dy = b[1] - a[1], l = Math.hypot(dx, dy) || 1, nx = ((-dy / l) * w) / 2, ny = ((dx / l) * w) / 2
      const P = (q: [number, number], k: number, z: number): V3 => [q[0] + nx * k, q[1] + ny * k, z]
      face(white, [P(a, 1, z0), P(b, 1, z0), P(b, 1, z1), P(a, 1, z1)], [nx, ny, 0])
      face(white, [P(a, -1, z0), P(a, -1, z1), P(b, -1, z1), P(b, -1, z0)], [-nx, -ny, 0])
      face(white, [P(a, 1, z1), P(b, 1, z1), P(b, -1, z1), P(a, -1, z1)], [0, 0, 1])
    }
    type Bent = { x: number; y: number; top: number; mid: number; slope: number; floor: number; dir: [number, number]; s: number }
    const bents: Bent[] = []
    for (let k = 0, count = Math.round(TOTAL / EVERY); k < count; k++) {
      const q = fine[Math.round((k * TOTAL) / count / STEP) % N]
      // Stand under the middle of the track's underside where it really is: a
      // banked track's underside moves sideways and down from the centreline.
      // The bent's top follows the bank (`slope`) and sits 0.25 m up into the
      // spine, so it meets the track along its whole width.
      const u = add(q.p, mul(q.up, -DEPTH))
      const [x, y] = [u[0], u[1]]
      const lh = Math.hypot(q.left[0], q.left[1]) || 1, slope = q.left[2] / lh
      const mid = u[2] + 0.25, top = mid - (Math.abs(slope) * (W - 0.4)) / 2
      // Stand on the ground, or on a lower stretch of track passing under.
      let floor = ground(x, y) - 1
      for (const o of fine) {
        const ds = Math.abs(o.s - q.s)
        if (Math.min(ds, TOTAL - ds) < 8 || o.p[2] >= q.p[2] - DEPTH - 1.5) continue
        if (Math.hypot(o.p[0] - x, o.p[1] - y) < W * 0.8) floor = Math.max(floor, o.p[2])
      }
      const l = Math.hypot(q.t[0], q.t[1]) || 1
      bents.push({ x, y, top, mid, slope, floor, dir: [q.t[0] / l, q.t[1] / l], s: q.s })
    }
    for (const b of bents) {
      if (b.top - b.floor < 0.6) continue
      const h = ALONG / 2
      post([b.x - b.dir[0] * h, b.y - b.dir[1] * h], [b.x + b.dir[0] * h, b.y + b.dir[1] * h], W - 0.4, b.floor, b.mid, b.slope)
    }
    // Ledgers between neighbouring bents at common levels, clear of the deck above and the floor below.
    for (let k = 0; k < bents.length; k++) {
      const a = bents[k], b = bents[(k + 1) % bents.length]
      for (let lv = STEPZ; lv < Math.max(a.top, b.top); lv += STEPZ) {
        const z1 = lv + LH / 2, z0 = lv - LH / 2
        if (z1 > Math.min(a.top, b.top) - 1.2 || z0 < Math.max(a.floor, b.floor) + 0.8) continue
        ledger([a.x, a.y], [b.x, b.y], W - 0.8, z0, z1)
      }
    }
    supports.push(...bents.filter((b) => b.top - b.floor >= 0.6).map((b) => ({ s: b.s, legs: 1 })))
  }

  // ------------------------------------------------------------ covers ----
  // A covered stretch: two walls either side of the track and a roof.
  for (const c of spec.covers ?? []) {
    const A = splineS(c.from), B = splineS(c.to), w = (c.width ?? W + 2.4) / 2, h = c.height ?? 4
    let len = B - A
    if (len < 0) len += TOTAL
    const ring: { p: V3; l: V3; base: number }[] = []
    for (let d = 0; d <= len + 1e-6; d += Math.max(1, len / Math.ceil(len / 6))) {
      const q = fine[Math.round((A + d) / STEP) % N], l = unit([q.left[0], q.left[1], 0])
      ring.push({ p: q.p, l, base: Math.min(q.p[2] - DEPTH, ground(q.p[0], q.p[1])) - 0.5 })
    }
    for (let i = 0; i + 1 < ring.length; i++) {
      const a = ring[i], b = ring[i + 1]
      const top = (q: typeof a) => q.p[2] + h
      for (const sg of [1, -1]) {
        const pa = add(a.p, mul(a.l, w * sg)), pb = add(b.p, mul(b.l, w * sg))
        face(trimPart, [[pa[0], pa[1], a.base], [pb[0], pb[1], b.base], [pb[0], pb[1], top(b)], [pa[0], pa[1], top(a)]], mul(add(a.l, b.l), 0.5 * sg))
      }
      const al = add(a.p, mul(a.l, w + 0.4)), ar = add(a.p, mul(a.l, -w - 0.4)), bl = add(b.p, mul(b.l, w + 0.4)), br = add(b.p, mul(b.l, -w - 0.4))
      face(roofPart, [[al[0], al[1], top(a)], [ar[0], ar[1], top(a)], [br[0], br[1], top(b)], [bl[0], bl[1], top(b)]], [0, 0, 1])
    }
  }

  // ----------------------------------------------------------- station ----
  {
    const poly = spec.station.ring.map(([x, y]) => [x - AX, y - AY] as [number, number])
    const area = poly.reduce((s, p, i) => s + p[0] * poly[(i + 1) % poly.length][1] - poly[(i + 1) % poly.length][0] * p[1], 0)
    const ring = area < 0 ? [...poly].reverse() : poly
    const g = Math.max(...ring.map(([x, y]) => ground(x, y)))
    const ROOF_T = 0.9, z0 = g + spec.station.roofZ - ROOF_T, z1 = g + spec.station.roofZ
    for (let i = 0; i < ring.length; i++) {
      const a = ring[i], b = ring[(i + 1) % ring.length]
      face(trimPart, [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]], unit([b[1] - a[1], -(b[0] - a[0]), 0]))
    }
    // Ear-clip the roof and its soffit.
    const idx = ring.map((_, i) => i)
    const cz = (o: number[], a: number[], b: number[]) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
    const up: [V3, V3, V3] = [[0, 0, 1], [0, 0, 1], [0, 0, 1]], dn: [V3, V3, V3] = [[0, 0, -1], [0, 0, -1], [0, 0, -1]]
    let guard = 0
    while (idx.length > 3 && guard++ < 1000) {
      for (let i = 0; i < idx.length; i++) {
        const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
        const A = ring[ia], B = ring[ib], C = ring[ic]
        if (cz(A, B, C) <= 0) continue
        if (!idx.every((j) => j === ia || j === ib || j === ic || !(cz(A, B, ring[j]) >= 0 && cz(B, C, ring[j]) >= 0 && cz(C, A, ring[j]) >= 0))) continue
        const T = [A, B, C]
        roofPart.tri(...(T.map(([x, y]) => [x, y, z1]) as [V3, V3, V3]), undefined, undefined, undefined, up)
        trimPart.tri(...(T.reverse().map(([x, y]) => [x, y, z0]) as [V3, V3, V3]), undefined, undefined, undefined, dn)
        idx.splice(i, 1)
        break
      }
    }
    const [a, b, c] = idx.map((i) => ring[i])
    roofPart.tri([a[0], a[1], z1], [b[0], b[1], z1], [c[0], c[1], z1], undefined, undefined, undefined, up)
    for (const [x, y] of spec.station.posts) {
      const px = x - AX, py = y - AY
      tube(trimPart, [px, py, ground(px, py) - 1], [px, py, z0], 0.35)
    }
    if (spec.stationWalls) {
      // An enclosed station: walls from the lowest ground under it up to the roof.
      const zb = Math.min(...ring.map(([x, y]) => ground(x, y))) - 1
      for (let i = 0; i < ring.length; i++) {
        const a = ring[i], b = ring[(i + 1) % ring.length]
        face(trimPart, [[a[0], a[1], zb], [b[0], b[1], zb], [b[0], b[1], z0], [a[0], a[1], z0]], unit([b[1] - a[1], -(b[0] - a[0]), 0]))
      }
    }
  }

  // ------------------------------------------------------------ output ----
  const extraParts = spec.extras?.({
    at: (ps: number) => {
      const s = splineS(ps)
      return path.reduce((a, b) => (Math.min(Math.abs(b.s - s), TOTAL - Math.abs(b.s - s)) < Math.min(Math.abs(a.s - s), TOTAL - Math.abs(a.s - s)) ? b : a))
    },
    ground,
    toModel: ([x, y]) => [x - AX, y - AY],
    parts: { deck, spine, supports: white, stone: trimPart, roof: roofPart },
    inversions: inv.windows.map((w) => ({ inv: w.inv, pts: w.pts })),
    track: spec.track,
  }) ?? []
  const parts = [
    { part: deck, material: finish(...spec.colours.deck) },
    ...(spec.colours.spine === 'deck' ? [] : [{ part: spine, material: finish(...spec.colours.spine) }]),
    { part: white, material: spec.colours.supports === 'trim' ? PALETTE.trim : finish(...spec.colours.supports) },
    { part: trimPart, material: PALETTE.stone },
    { part: roofPart, material: PALETTE.roof },
    ...extraParts,
  ]
  const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
  let maxZ = 0, steep = 0
  for (let i = 0; i < N; i++) {
    const a = fine[i].p, b = fine[(i + 1) % N].p
    maxZ = Math.max(maxZ, a[2])
    steep = Math.max(steep, (Math.atan2(a[2] - b[2], Math.hypot(b[0] - a[0], b[1] - a[1])) * 180) / Math.PI)
  }
  console.log(`track ${len3.toFixed(0)} m (published ${spec.meta.trackLength}; ${TOTAL.toFixed(0)} m in plan), ${M} sections, crest ${maxZ.toFixed(1)} m, steepest ${steep.toFixed(0)}°, max bank ${((Math.max(...bank.map(Math.abs)) * 180) / Math.PI).toFixed(0)}°`)
  console.log(`${supports.length} supports, ${supports.reduce((s, x) => s + x.legs, 0)} legs; datum ${DATUM.toFixed(1)} m on the grid`)
  console.log(parts.map(({ part, material }) => `${material.name} ${part.triangles}`).join(', '))
  if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
  const glb = writeGlb(spec.name, parts, {
    license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
    bearing: 0, elevation: 0, height: spec.meta.height, trackLength: spec.meta.trackLength,
  })
  if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
  const out = new URL(spec.out, import.meta.url).pathname
  await Bun.write(out, glb)
  console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
  if (spec.origin) {
    const [lon0, lat0] = spec.origin, mx = 111320 * Math.cos((lat0 * Math.PI) / 180)
    console.log(`anchor ${(lon0 + AX / mx).toFixed(7)}, ${(lat0 + AY / MY).toFixed(7)}`)
  } else console.log(`anchor ${(LON0 + AX / MX).toFixed(7)}, ${(LAT0 + AY / MY).toFixed(7)}`)

  // ------------------------------------------------------------ report ----
  const idxOf = (s: number) => Math.round((((s % TOTAL) + TOTAL) % TOTAL) / STEP) % N
  /** Vertical load at a sample, in g: cos(pitch) plus v²κ/g, κ the pitch change per metre of track (±3 m). */
  const gAt = (i: number) => {
    const th = (k: number) => Math.atan2(zs[(k + 1) % N] - zs[(k - 1 + N) % N], 2 * STEP)
    const a = th((i - 3 + N) % N), b = th((i + 3) % N), l = cum3[Math.min(i + 3, N)] - cum3[Math.max(i - 3, 0)] || 6
    return Math.cos(th(i)) + (2 * G * Math.max(0, head[i]) * ((b - a) / l)) / G
  }
  if (process.env.REPORT) {
    console.log('\nelement                          s (m)    %     height  speed   bank  vertical g')
    for (const e of E) {
      const i = idxOf(e.ss)
      const z = zs[i] - groundAt(e.ss)
      const v = Math.sqrt(2 * G * Math.max(0, head[i]))
      console.log(`${e.name.padEnd(32)} ${e.s.toFixed(0).padStart(5)}  ${((e.s / polyTotal) * 100).toFixed(1).padStart(5)}  ${z.toFixed(1).padStart(6)}  ${(v * 3.6).toFixed(0).padStart(4)} km/h  ${((bank[i] * 180) / Math.PI).toFixed(0).padStart(4)}°  ${gAt(i).toFixed(1).padStart(5)}`)
    }
    // Energy check: every stretch off the lift and brakes needs speed in hand.
    let worst = Infinity, worstS = 0, vmax = 0
    for (let i = 0; i < N; i++) {
      const s = (i * TOTAL) / N
      if (levelAt(s)) continue
      if (head[i] < worst) { worst = head[i]; worstS = s }
      vmax = Math.max(vmax, Math.sqrt(2 * G * Math.max(0, head[i])))
    }
    const toPoly = (s: number) => { let k = 0; while (k < n - 1 && knotS[k + 1] <= s) k++; return polyS[k] + ((s - knotS[k]) / ((knotS[k + 1] ?? TOTAL) - knotS[k])) * ((polyS[k + 1] ?? polyTotal) - polyS[k]) }
    // Average loss, in the brief's terms: % of the lift height lost to rolling
    // resistance and drag per 100 m of track, from the lift crest to the first brakes.
    let lossSum = 0, runTo = 0
    {
      const i0 = Math.round(liftTop.ss / STEP) % N
      for (let k = 0; k < N; k++) {
        const i = (i0 + k) % N, s = (i * TOTAL) / N
        if (k > 5 && spec.brakes.some((b) => inRange(s, [b.from, b.to]))) break
        const dl = cum3[i + 1] - cum3[i]
        lossSum += (spec.losses[0] + spec.losses[1] * Math.max(0, head[i])) * dl
        runTo += dl
      }
    }
    const lost = lossSum
console.log(`\nenergy: lowest head off the lift and brakes ${worst.toFixed(1)} m (${Math.sqrt(2 * G * Math.max(0, worst)).toFixed(1)} m/s) at s ${toPoly(worstS).toFixed(0)}; top speed ${(vmax * 3.6).toFixed(0)} km/h; losses average ${((lost / runTo) * 100 / (liftTop.zz - groundAt(liftTop.ss)) * 100).toFixed(1)} % of the lift height per 100 m to the first brakes`)
    if (worst < 0.8) console.log('  WARNING: the train would stall or crawl here; lower the hill or move it earlier')
    // Duration estimate: lift at 5 m/s, brakes and station at 4 m/s, elsewhere the energy speed.
    let t = 0
    for (let i = 0; i < N; i++) {
      const s = (i * TOTAL) / N, ds = cum3[i + 1] - cum3[i]
      const v = onLift(s) ? 5 : levelAt(s) ? 6 : Math.max(4, Math.sqrt(2 * G * Math.max(0, head[i])))
      t += ds / v
    }
    // Inversions: speed and vertical load at the top, load at the entry.
    for (const w of inv.windows) {
      const h0 = head[w.a], z0 = zs[w.a], v2 = (z: number) => 2 * G * Math.max(0, h0 - (z - z0))
      const top = w.pts.reduce((m, q) => (q.p[2] > m.p[2] ? q : m))
      const vt = Math.sqrt(v2(top.p[2]))
      if (w.inv.kind === 'loop') {
        const { rTop, rBot } = w.meta
        console.log(`loop "${w.inv.name ?? ''}": ${w.meta.height} m tall, ${w.meta.dx.toFixed(1)} m long in a ${w.meta.D.toFixed(1)} m window; entry ${(Math.sqrt(v2(z0)) * 3.6).toFixed(0)} km/h, ${(1 + v2(z0) / (G * rBot)).toFixed(1)} g (radius ${rBot.toFixed(1)} m); top ${(vt * 3.6).toFixed(0)} km/h, ${(v2(top.p[2]) / (G * rTop) - 1).toFixed(1)} g into the seat (radius ${rTop.toFixed(1)} m)`)
        if (v2(top.p[2]) < G * rTop) console.log('  WARNING: too slow over the top; lower the loop or widen rTop')
      } else if (w.inv.kind === 'path') console.log(`path "${w.inv.name ?? ''}": ${w.meta.arc.toFixed(0)} m of track over ${w.meta.D.toFixed(1)} m, ${w.meta.top.toFixed(1)} m up; top ${(vt * 3.6).toFixed(0)} km/h`)
      else console.log(`roll "${w.inv.name ?? ''}": ${w.meta.turns} turn(s), radius ${w.meta.radius} m over ${w.meta.D.toFixed(1)} m; top ${(top.p[2] - groundAt(top.s)).toFixed(1)} m up at ${(vt * 3.6).toFixed(0)} km/h`)
      // Smoothness inside: the largest turn of the tangent per metre, and at the joins.
      let worstT = 0
      for (let i = 1; i < w.pts.length; i++) {
        const d = Math.hypot(...sub(w.pts[i].p, w.pts[i - 1].p)) || 1
        worstT = Math.max(worstT, (Math.acos(Math.min(1, dot(w.pts[i].t, w.pts[i - 1].t))) * 180) / Math.PI / d)
      }
      const before = fine[(w.a - 1 + N) % N], after = fine[(w.b + 1) % N]
      const j0 = (Math.acos(Math.min(1, dot(before.t, w.pts[0].t))) * 180) / Math.PI, j1 = (Math.acos(Math.min(1, dot(after.t, w.pts[w.pts.length - 1].t))) * 180) / Math.PI
      console.log(`  tangent turns at most ${worstT.toFixed(1)}° per m inside; joins ${j0.toFixed(1)}° and ${j1.toFixed(1)}°`)
    }
    console.log(`  ride time on track ≈ ${Math.floor(t / 60)}:${String(Math.round(t % 60)).padStart(2, '0')} (station dwell not included)`)
    if (spec.shuttle) {
      // Both runs of a shuttle on the drawn track, inversions included. The
      // first is let go where `lift` (the catch car) lets the train go near the
      // first node's top, with `liftHead`; the run back is let go the same
      // height below the last node's top. Each ends where it runs out of energy
      // on the far climb, where the lift or the brakes take the train.
      const z0 = path[0].p[2], i0 = Math.max(0, path.findIndex((q) => !inRange(q.s, spec.lift)))
      const drop = z0 - path[i0].p[2], zEnd = path[path.length - 1].p[2]
      const back = [...path].reverse()
      const runs = { forward: path.slice(i0), back: back.slice(Math.max(0, back.findIndex((q) => q.p[2] <= zEnd - drop))) }
      for (const [k, order] of Object.entries(runs)) {
        let e = order[0].p[2] + (spec.liftHead ?? 1), vmax = 0, low = Infinity, lowAt = 0, stop = order.length - 1
        for (let i = 0; i < order.length; i++) {
          const z = order[i].p[2], h = e - z
          if (i > 10 && h <= 0) { stop = i; break }
          vmax = Math.max(vmax, Math.sqrt(2 * G * Math.max(0, h)))
          const crest = order.slice(Math.max(0, i - 6), i + 7).every((o) => o.p[2] <= z + 1e-6)
          if (i > 10 && crest && h < low) { low = h; lowAt = order[i].s }
          if (i + 1 < order.length) e -= (spec.losses[0] + spec.losses[1] * Math.max(0, h)) * Math.hypot(...sub(order[i + 1].p, order[i].p))
        }
        const q = order[stop]
        console.log(`shuttle ${k}: let go ${(order[0].p[2] - ground(order[0].p[0], order[0].p[1])).toFixed(1)} m up; top speed ${(vmax * 3.6).toFixed(0)} km/h; slowest crest ${Math.sqrt(2 * G * Math.max(0, low)).toFixed(1)} m/s at s ${toPoly(lowAt).toFixed(0)}; runs out ${(q.p[2] - ground(q.p[0], q.p[1])).toFixed(1)} m up at s ${toPoly(q.s).toFixed(0)}`)
        if (low < 0.8) console.log('  WARNING: too slow over a crest on this run')
      }
    }
  }
  // Smoothness: pitch and roll per metre along the track. The profile is C1 by
  // construction (arcs meet straights on their tangent), so a spike here is a
  // bug or a clamp, and shows as a kink on the map.
  const pitchDeg = zs.map((_, i) => (Math.atan2(zs[(i + 1) % N] - zs[(i - 1 + N) % N], 2 * STEP) * 180) / Math.PI)
  const toPolyS = (s: number) => { let k = 0; while (k < n - 1 && knotS[k + 1] <= s) k++; return polyS[k] + ((s - knotS[k]) / ((knotS[k + 1] ?? TOTAL) - knotS[k])) * ((polyS[k + 1] ?? polyTotal) - polyS[k]) }
  if (process.env.REPORT) {
    let dp = 0, dpS = 0, db = 0, dbS = 0
    for (let i = 0; i < N; i++) {
      // A shuttle has no track between its ends.
      if (spec.shuttle && ((i + 2) * TOTAL) / N > SHUTTLE_END) continue
      // Per metre of track, not of plan: a steep stretch covers more track per plan metre.
      const dl = Math.hypot(STEP, zs[(i + 1) % N] - zs[i])
      const a = Math.abs(pitchDeg[(i + 1) % N] - pitchDeg[i]) / dl, b = (Math.abs(bank[(i + 1) % N] - bank[i]) * 180) / Math.PI
      if (a > dp) { dp = a; dpS = i * STEP }
      if (b > db) { db = b; dbS = i * STEP }
    }
    console.log(`smoothness: steepest pitch change ${dp.toFixed(1)}° per m at s ${toPolyS(dpS).toFixed(0)}; fastest roll ${db.toFixed(1)}° per m at s ${toPolyS(dbS).toFixed(0)}`)
  }
  // PROFILE_SVG=<file>: height, pitch and bank along the track, elements marked.
  if (process.env.PROFILE_SVG) {
    const Wd = 1600, rows = [{ k: 'height above datum (m)', v: zs, g: zs.map((_, i) => groundAt(i * STEP)), lo: 0, hi: Math.ceil(Math.max(...zs) / 10) * 10 }, { k: 'pitch (°)', v: pitchDeg, lo: -90, hi: 90 }, { k: 'bank (°)', v: bank.map((b) => (b * 180) / Math.PI), lo: -100, hi: 100 }]
    const Hr = 220, X = (i: number) => 50 + ((Wd - 70) * i) / N
    let svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${Wd}" height="${rows.length * (Hr + 30) + 20}" font-family="sans-serif" font-size="11"><rect width="100%" height="100%" fill="#fff"/>`
    rows.forEach((r: any, j) => {
      const top = 20 + j * (Hr + 30), Y = (v: number) => top + Hr - ((v - r.lo) / (r.hi - r.lo)) * Hr
      svg += `<text x="50" y="${top - 5}" font-size="13">${spec.name}: ${r.k}</text><rect x="50" y="${top}" width="${Wd - 70}" height="${Hr}" fill="none" stroke="#ccc"/>`
      for (let t = r.lo; t <= r.hi; t += (r.hi - r.lo) / 4) svg += `<line x1="50" x2="${Wd - 20}" y1="${Y(t)}" y2="${Y(t)}" stroke="#eee"/><text x="8" y="${Y(t) + 4}">${t.toFixed(0)}</text>`
      for (const e of E) svg += `<line x1="${X(e.ss / STEP)}" x2="${X(e.ss / STEP)}" y1="${top}" y2="${top + Hr}" stroke="#f3c" stroke-opacity="0.3"/>` + (j === 0 ? `<text transform="translate(${X(e.ss / STEP) + 3},${top + 12}) rotate(90)" fill="#b0a">${e.name}</text>` : '')
      if (r.g) svg += `<polyline fill="none" stroke="#8a6" points="${r.g.map((v: number, i: number) => `${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(' ')}"/>`
      svg += `<polyline fill="none" stroke="#235" stroke-width="1.3" points="${r.v.map((v: number, i: number) => `${X(i).toFixed(1)},${Y(v).toFixed(1)}`).join(' ')}"/>`
    })
    await Bun.write(process.env.PROFILE_SVG, svg + '</svg>')
  }
  if (process.env.COASTER_DUMP) {
    const datumAbs = spec.ground.base + DATUM
    await Bun.write(process.env.COASTER_DUMP, JSON.stringify({
      name: spec.name, anchor: spec.anchor, datumAbs, chain: spec.chain,
      centreline: path.filter((_, i) => i % 2 === 0).map((f) => [+(f.p[0] + AX).toFixed(1), +(f.p[1] + AY).toFixed(1), +(f.p[2] + datumAbs).toFixed(1)]),
      elements: E.map((e) => ({ name: e.name, s: e.s, pct: +((e.s / polyTotal) * 100).toFixed(1), z: e.z })),
    }))
  }
  if (process.env.CLEARANCE) {
    for (let i = 0; i < N; i += 2) for (let k = i + 20; k < N; k += 2) {
      const ds = Math.abs(fine[i].s - fine[k].s)
      if (Math.min(ds, TOTAL - ds) < 20) continue
      const a = fine[i].p, b = fine[k].p
      const d = Math.hypot(a[0] - b[0], a[1] - b[1]), dz = Math.abs(a[2] - b[2])
      if (d < W + 1 && dz < 4.5) console.log(`clash s=${fine[i].s.toFixed(0)} & ${fine[k].s.toFixed(0)} at (${(a[0] + AX).toFixed(0)}, ${(a[1] + AY).toFixed(0)}) plan ${d.toFixed(1)} dz ${dz.toFixed(1)}`)
    }
  }
  if (process.env.PROFILE_DUMP) {
    for (let i = 0; i < N; i += 10) console.log(`s ${fine[i].s.toFixed(0)} (${(fine[i].p[0] + AX).toFixed(0)}, ${(fine[i].p[1] + AY).toFixed(0)}) z ${(zs[i] - groundAt(fine[i].s)).toFixed(1)} head ${head[i].toFixed(1)} bank ${((bank[i] * 180) / Math.PI).toFixed(0)}`)
  }
  return { triangles, bytes: glb.length }
}
