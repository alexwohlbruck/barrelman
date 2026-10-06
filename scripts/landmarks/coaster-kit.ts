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
import { PALETTE, finish } from './palette'

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
  colours: { deck: [string, number]; spine: [string, number]; supports: [string, number] | 'trim' }
  /** Support radii for low, middle and high track. */
  supportR: [number, number, number]
  meta: { height: number; trackLength: number }
}

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
    const p0 = pts[(i - 1 + n) % n], p1 = pts[i], p2 = pts[(i + 1) % n], p3 = pts[(i + 2) % n]
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
  // Energy resets: the lift crest, and the end of each brake run.
  const resets = [
    { at: liftTop.ss, e: liftTop.zz },
    ...spec.brakes.map((b) => ({ at: splineS(b.to), e: b.z + groundAt(splineS(b.to)) + b.head })),
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
    return smax(z, groundAt(s) + 0.6 - 12 * belowW(s))
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
  const levelAt = (s: number) => inRange(s, spec.lift) || spec.brakes.some((b) => inRange(s, [b.from, b.to]))
  const onChainOrBrakes = levelAt
  // Head of energy along the track: walk on from the lift crest, losing
  // rolling resistance (a per metre) and air drag (b × head per metre, since
  // drag goes with v²); the chain holds the train at lift speed and the brake
  // runs set it to their exit speed.
  const head = new Array<number>(N).fill(0)
  {
    const [a, b] = spec.losses
    const i0 = Math.round(liftTop.ss / STEP) % N
    let e = zs[i0] + (spec.liftHead ?? 1)
    for (let k = 0; k < N; k++) {
      const i = (i0 + k) % N, s = (i * TOTAL) / N
      if (inRange(s, spec.lift)) e = zs[i] + (spec.liftHead ?? 1)
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
  for (const o of spec.banks ?? []) {
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
  const TURN = Math.cos((9 * Math.PI) / 180), ROLL = Math.cos((12 * Math.PI) / 180)
  const samples: Sample[] = [fine[0]]
  for (let i = 1; i < N; i++) {
    const last = samples[samples.length - 1], q = fine[i]
    if (Math.hypot(...sub(q.p, last.p)) >= 12 || dot(q.t, last.t) < TURN || dot(q.up, last.up) < ROLL) samples.push(q)
  }
  const M = samples.length

  // ------------------------------------------------------------- track ----
  const deck = new Part(), spine = new Part(), white = new Part(), roofPart = new Part(), trimPart = new Part()
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
  for (let i = 0; i < M; i++) {
    const a = samples[i], b = samples[(i + 1) % M]
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
  for (let s = 4; s < TOTAL - 4; s += 17) {
    for (const off of [0, 4, -4, 8, -8]) {
      const q = fine[Math.round((s + off) / STEP) % N]
      const zr = q.p[2] - ground(q.p[0], q.p[1])
      if (zr < 2.2) break
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
  }

  // ------------------------------------------------------------ output ----
  const parts = [
    { part: deck, material: finish(...spec.colours.deck) },
    { part: spine, material: finish(...spec.colours.spine) },
    { part: white, material: spec.colours.supports === 'trim' ? PALETTE.trim : finish(...spec.colours.supports) },
    { part: trimPart, material: PALETTE.stone },
    { part: roofPart, material: PALETTE.roof },
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
  console.log(`anchor ${(LON0 + AX / MX).toFixed(7)}, ${(LAT0 + AY / MY).toFixed(7)}`)

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
      const v = inRange(s, spec.lift) ? 5 : levelAt(s) ? 6 : Math.max(4, Math.sqrt(2 * G * Math.max(0, head[i])))
      t += ds / v
    }
    console.log(`  ride time on track ≈ ${Math.floor(t / 60)}:${String(Math.round(t % 60)).padStart(2, '0')} (station dwell not included)`)
  }
  // Smoothness: pitch and roll per metre along the track. The profile is C1 by
  // construction (arcs meet straights on their tangent), so a spike here is a
  // bug or a clamp, and shows as a kink on the map.
  const pitchDeg = zs.map((_, i) => (Math.atan2(zs[(i + 1) % N] - zs[(i - 1 + N) % N], 2 * STEP) * 180) / Math.PI)
  const toPolyS = (s: number) => { let k = 0; while (k < n - 1 && knotS[k + 1] <= s) k++; return polyS[k] + ((s - knotS[k]) / ((knotS[k + 1] ?? TOTAL) - knotS[k])) * ((polyS[k + 1] ?? polyTotal) - polyS[k]) }
  if (process.env.REPORT) {
    let dp = 0, dpS = 0, db = 0, dbS = 0
    for (let i = 0; i < N; i++) {
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
      centreline: fine.filter((_, i) => i % 2 === 0).map((f) => [+(f.p[0] + AX).toFixed(1), +(f.p[1] + AY).toFixed(1), +(f.p[2] + datumAbs).toFixed(1)]),
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
