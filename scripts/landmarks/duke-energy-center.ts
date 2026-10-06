/**
 * Duke Energy Center (550 South Tryon), Charlotte — original procedural
 * geometry, CC0-1.0.
 * bun scripts/landmarks/duke-energy-center.ts [out.glb]
 *
 * Anchor: the centroid of OSM way/1550692284 (-80.84874, 35.22416). Bearing
 * 50.3°, the azimuth of the tower's faces in OSM, so model x and y run along
 * the faces. Coordinates below are OSM vertices rotated into that frame.
 *
 * The tower is a 48.6 m square with chamfered corners. Its crown is a gable
 * laid along the diagonal: the two corners on the ridge (true north and south)
 * rise to 240 m and the other two drop to 180 m, so every face ends in a
 * single steep slope framed in pale stone. Inside the screen walls the roof
 * climbs from the low corners to a flat deck at about 219 m, leaving the top
 * open, with a beam spanning the ridge, as the OSM parts record. The 60 m
 * parking and office podium sits on the north-west side.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]
// The curtain walls are slate `window` glass broken by pale trim: the fins,
// and floor lines every four floors. Tower and podium glass are one material.
const glass = new Part(), stone = new Part(), metal = new Part()
const slope = new Part(), roof = new Part(), garden = new Part(), podGlass = glass

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return v.map(n => n / l) as V3 }
const add = (a: XY, b: XY, k = 1): XY => [a[0] + b[0] * k, a[1] + b[1] * k]
const lerp2 = (a: XY, b: XY, t: number): XY => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]

/** Offset a counter-clockwise ring inwards along each corner's bisector. */
function inset(ring: XY[], d: number): XY[] {
  return ring.map((b, i) => {
    const a = ring[(i + ring.length - 1) % ring.length], c = ring[(i + 1) % ring.length]
    const u = unit([b[1] - a[1], a[0] - b[0], 0]), v = unit([c[1] - b[1], b[0] - c[0], 0])
    const k = 1 + u[0] * v[0] + u[1] * v[1]
    return [b[0] - (u[0] + v[0]) * d / k, b[1] - (u[1] + v[1]) * d / k]
  })
}

/**
 * A vertical prism over a counter-clockwise plan ring, with per-point bottom
 * and top heights. Walls meeting at less than `crease` degrees share normals,
 * so a 45° plan chamfer reads as a rounded edge.
 */
function prism(p: Part, ring: XY[], zb: (q: XY) => number, zt: (q: XY) => number,
  opts: { top?: boolean; bottom?: boolean; crease?: number } = {}) {
  const { top = true, bottom = false, crease = 50 } = opts
  const n = ring.length
  const en = ring.map((a, i) => {
    const b = ring[(i + 1) % n]
    return unit([b[1] - a[1], a[0] - b[0], 0])
  })
  const cos = Math.cos(crease * Math.PI / 180)
  const vn = (i: number, e: number): V3 => {
    const other = e === i ? en[(i + n - 1) % n] : en[(i + 1) % n]
    const own = en[e]
    return own[0] * other[0] + own[1] * other[1] > cos ? unit([own[0] + other[0], own[1] + other[1], 0]) : own
  }
  for (let i = 0; i < n; i++) {
    const j = (i + 1) % n, a = ring[i], b = ring[j]
    const na = vn(i, i), nb = vn(j, i)
    const A: V3 = [a[0], a[1], zb(a)], B: V3 = [b[0], b[1], zb(b)]
    const At: V3 = [a[0], a[1], zt(a)], Bt: V3 = [b[0], b[1], zt(b)]
    p.tri(A, B, Bt, undefined, undefined, undefined, [na, nb, nb])
    p.tri(A, Bt, At, undefined, undefined, undefined, [na, nb, na])
  }
  const pts = (z: (q: XY) => number) => ring.map(q => [q[0], q[1], z(q)] as V3)
  if (top) { const t = pts(zt); for (let i = 1; i < n - 1; i++) p.tri(t[0], t[i], t[i + 1]) }
  if (bottom) { const t = pts(zb); for (let i = 1; i < n - 1; i++) p.tri(t[0], t[i + 1], t[i]) }
}

/** Walls only, from a ring's bottom heights to its top heights. */
const walls = (p: Part, ring: XY[], zb: (q: XY) => number, zt: (q: XY) => number) =>
  prism(p, ring, zb, zt, { top: false, crease: 0 })

/** Fan cap of a convex (or star-shaped about its centroid) ring. */
function cap(p: Part, ring: XY[], z: (q: XY) => number, star = false) {
  const pts = ring.map(q => [q[0], q[1], z(q)] as V3)
  if (!star) { for (let i = 1; i < pts.length - 1; i++) p.tri(pts[0], pts[i], pts[i + 1]); return }
  const c: XY = [ring.reduce((s, q) => s + q[0], 0) / ring.length, ring.reduce((s, q) => s + q[1], 0) / ring.length]
  const C: V3 = [c[0], c[1], z(c)]
  for (let i = 0; i < pts.length; i++) p.tri(C, pts[i], pts[(i + 1) % pts.length])
}

/**
 * A bevelled strip standing proud of a face between s0 and s1 along edge
 * a→b: depth runs from the outer line (n = 0) back to the glass (n = depth).
 */
function strip(a: XY, b: XY, s0: number, s1: number, depth: number, bevel: number): XY[] {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const d: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], inward: XY = [-d[1], d[0]]
  const at = (s: number, n: number) => add(add(a, d, s), inward, n)
  return [at(s0 + bevel, 0), at(s1 - bevel, 0), at(s1, bevel), at(s1, depth), at(s0, depth), at(s0, bevel)]
}

const RECESS = .7
const flat = (z: number) => () => z

// ---------------------------------------------------------------- tower
const CX = 34.5, CY = .25, H = 24.35, CH = 4
const TOP = 240, LOW = 180
const D = 2 * H - CH
/** Distance along the low diagonal, zero on the ridge between the high corners. */
const sOf = (q: XY) => (q[0] - CX) + (q[1] - CY)
const rim = (q: XY) => TOP - (TOP - LOW) * Math.min(1, Math.abs(sOf(q)) / D)

const P = (u: number, v: number): XY => [CX + u, CY + v]
// Counter-clockwise from the south-west chamfer. The two high chamfers carry
// a midpoint on the ridge so the rim peaks there.
const faceEnds = { south: [0, 1], east: [3, 4], north: [5, 6], west: [8, 9] }
const ring10: XY[] = [
  P(-H + CH, -H), P(H - CH, -H), P(H - CH / 2, -H + CH / 2), P(H, -H + CH),
  P(H, H - CH), P(H - CH, H), P(-H + CH, H), P(-H + CH / 2, H - CH / 2), P(-H, H - CH), P(-H, -H + CH),
]

// The inner deck sits DROP below the rim plane and levels off at DECK, which
// happens at |s| = K. Every ring is split there so walls and roof agree.
const DECK = 219, DROP = 3
const K = D * (TOP - DROP - DECK) / (TOP - LOW)
function split(ring: XY[], cuts: number[]): XY[] {
  const out: XY[] = []
  ring.forEach((a, i) => {
    const b = ring[(i + 1) % ring.length]
    out.push(a)
    const sa = sOf(a), sb = sOf(b)
    const ts = cuts.map(c => (c - sa) / (sb - sa)).filter(t => t > 1e-3 && t < 1 - 1e-3).sort((x, y) => x - y)
    for (const t of ts) out.push(lerp2(a, b, t))
  })
  return out
}
const outer = split(ring10, [K, -K])
const shell = inset(outer, RECESS)
const T = 1.4
const inner = inset(outer, RECESS + T)
const deck = (q: XY) => Math.min(DECK, rim(q) - DROP)

// Glass curtain wall up to the sloping rim, and the screen's inner face.
walls(glass, shell, flat(0), rim)
// It runs down into the solid tower so every tread below meets it.
walls(slope, [...inner].reverse(), flat(LOW - 6), rim)
// Screen top: a stone coping between the glass line and the inner face.
for (let i = 0; i < shell.length; i++) {
  const j = (i + 1) % shell.length
  const v = (q: XY): V3 => [q[0], q[1], rim(q)]
  stone.quad(v(shell[i]), v(shell[j]), v(inner[j]), v(inner[i]))
}
// The roof inside the screens: two slopes rising from the low corners to a
// flat deck. Clip the convex inner ring into three convex pieces.
function clip(ring: XY[], keep: (s: number) => number): XY[] {
  const out: XY[] = []
  ring.forEach((a, i) => {
    const b = ring[(i + 1) % ring.length], fa = keep(sOf(a)), fb = keep(sOf(b))
    if (fa >= 0) out.push(a)
    if ((fa >= 0) !== (fb >= 0)) out.push(lerp2(a, b, fa / (fa - fb)))
  })
  return out
}
// The slopes are stepped, as on the real crown: flat treads, each at the
// height the slope reaches at its outer edge, with glazed risers between.
const sMax = Math.max(...inner.map(q => Math.abs(sOf(q))))
const STEPS = 5
cap(roof, clip(clip(inner, s => s + K), s => K - s), deck)
for (const side of [1, -1]) {
  const at = (s: number): XY => [CX + s / 2, CY + s / 2]
  for (let k = 0; k < STEPS; k++) {
    const c0 = K + (sMax - K) * k / STEPS, c1 = K + (sMax - K) * (k + 1) / STEPS
    const z = deck(at(side * c1)), zUp = deck(at(side * c0))
    const band = clip(clip(inner, s => side * s - c0), s => c1 - side * s)
    cap(roof, band, flat(z))
    // Riser along s = c0, facing out towards the low corner.
    const ends = band.filter(q => Math.abs(side * sOf(q) - c0) < 1e-6)
    if (ends.length === 2) {
      let [a, b] = ends
      const n: XY = [side, side]
      if ((b[0] - a[0]) * n[1] - (b[1] - a[1]) * n[0] < 0) [a, b] = [b, a]
      slope.quad([b[0], b[1], z], [a[0], a[1], z], [a[0], a[1], zUp], [b[0], b[1], zUp])
    }
  }
}

// Stone coping band hugging the sloped top of every face and chamfer.
const COPE = 1.8
for (let i = 0; i < outer.length; i++) {
  const j = (i + 1) % outer.length
  prism(stone, [outer[i], outer[j], shell[j], shell[i]], q => rim(q) - COPE, rim, { crease: 0 })
}
// Stone plinth where the tower meets the street (the podium covers the west).
for (let i = 0; i < outer.length; i++) {
  const j = (i + 1) % outer.length
  if (outer[i][0] < CX - H + .1 && outer[j][0] < CX - H + .1) continue
  prism(stone, [outer[i], outer[j], shell[j], shell[i]], flat(0), flat(4), { crease: 0 })
}
// Corner frames and slim vertical fins on the four broad faces.
const PODIUM = 60
const FW = 2.6, FIN = 1.3, BAYS = 3
for (const [name, [i, j]] of Object.entries(faceEnds)) {
  const a = ring10[i], b = ring10[j]
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const z0 = name === 'west' ? PODIUM : 0
  const top = (q: XY) => rim(q) - COPE + .1
  prism(stone, strip(a, b, 0, FW, RECESS, .45), flat(z0), top, { top: false })
  prism(stone, strip(a, b, L - FW, L, RECESS, .45), flat(z0), top, { top: false })
  const bay = (L - 2 * FW) / BAYS
  for (let k = 1; k < BAYS; k++) {
    const s = FW + bay * k
    prism(metal, strip(a, b, s - FIN / 2, s + FIN / 2, RECESS * .85, .3), flat(z0 + 4), top, { top: false })
  }
}
// Pale floor lines across the curtain wall every four floors, flush with the
// stone frames and stopping under the sloped coping. Each is a front, a top
// and a soffit; the ends butt into the next face's line.
const LINE_EVERY = 16, LINE = .7
for (let z = LINE_EVERY; z + LINE < TOP - COPE; z += LINE_EVERY) {
  for (let i = 0; i < outer.length; i++) {
    const j = (i + 1) % outer.length
    // the west face is inside the podium up to its roof
    if (z < PODIUM + 1 && outer[i][0] < CX - H + .1 && outer[j][0] < CX - H + .1) continue
    const room = (t: number) => rim(lerp2(outer[i], outer[j], t)) - COPE - (z + LINE)
    let t0 = 0, t1 = 1
    if (room(0) < 0 && room(1) < 0) continue
    if (room(0) < 0 || room(1) < 0) {
      // the coping crosses this line part-way along the edge: find where
      let lo = 0, hi = 1
      const inside = room(0) >= 0
      for (let k = 0; k < 30; k++) { const m = (lo + hi) / 2; if ((room(m) >= 0) === inside) lo = m; else hi = m }
      if (inside) t1 = lo; else t0 = hi
    }
    const a = lerp2(outer[i], outer[j], t0), b = lerp2(outer[i], outer[j], t1)
    const ai = lerp2(shell[i], shell[j], t0), bi = lerp2(shell[i], shell[j], t1)
    const v = (q: XY, h: number): V3 => [q[0], q[1], h]
    metal.quad(v(a, z), v(b, z), v(b, z + LINE), v(a, z + LINE))
    metal.quad(v(a, z + LINE), v(b, z + LINE), v(bi, z + LINE), v(ai, z + LINE))
    metal.quad(v(ai, z), v(bi, z), v(b, z), v(a, z))
  }
}
// The beam spanning the open crown between the two high corners.
{
  const a = ring10[2], b = ring10[7], w = 1.4
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const d: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], side: XY = [-d[1], d[0]]
  const beam: XY[] = [add(a, side, -w), add(b, side, -w), add(b, side, w), add(a, side, w)]
  prism(stone, beam, flat(233), flat(237.5), { bottom: true, crease: 0 })
}

// ---------------------------------------------------------------- podium
const podium: XY[] = [
  [-58.3, -24.9], [1.9, -24.5], [1.9, -21.3], [7.1, -21.3], [7.1, -19.9],
  [12, -19.9], [12, 23.9], [-58.3, 23.8],
]
const podShell = inset(podium, RECESS)
walls(podGlass, podShell, flat(0), flat(PODIUM))
// Glass between a stone plinth with broad bays and a deep panelled top band.
const BAND = 9, PLINTH = 7
for (let i = 0; i < podium.length; i++) {
  const j = (i + 1) % podium.length, a = podium[i], b = podium[j]
  // The edge buried in the tower carries no facade.
  if (a[0] >= 12 && b[0] >= 12) continue
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  prism(stone, [a, b, podShell[j], podShell[i]], flat(PODIUM - BAND), flat(PODIUM + .9), { crease: 0 })
  prism(stone, [a, b, podShell[j], podShell[i]], flat(PLINTH), flat(PLINTH + 1.6), { crease: 0 })
  if (L < 8) {
    prism(stone, [a, b, podShell[j], podShell[i]], flat(0), flat(PLINTH), { crease: 0 })
    continue
  }
  const count = Math.round(L / 9.5)
  for (let k = 0; k <= count; k++) {
    const s = L * k / count, w = k === 0 || k === count ? 1.8 : .9
    prism(stone, strip(a, b, Math.max(0, s - w), Math.min(L, s + w), RECESS, .35), flat(0), flat(PLINTH), { top: false })
  }
  // Slim fins over the glass on the plinth's bay lines, and a stone band
  // where the parking floors give way to offices.
  for (let k = 1; k < count; k++) {
    const s = L * k / count
    prism(metal, strip(a, b, s - .55, s + .55, RECESS * .85, .25), flat(PLINTH + 1.6), flat(PODIUM - BAND + .1), { top: false })
  }
  prism(stone, [a, b, podShell[j], podShell[i]], flat(33), flat(34.6), { crease: 0 })
  // Corner frames run the full height, as on the tower.
  prism(stone, strip(a, b, 0, 1.8, RECESS, .35), flat(PLINTH), flat(PODIUM - BAND + .1), { top: false })
  prism(stone, strip(a, b, L - 1.8, L, RECESS, .35), flat(PLINTH), flat(PODIUM - BAND + .1), { top: false })
}
// Roof: a parapet ring, pale membrane, and the planted terrace.
const parapet = inset(podium, 1.2)
for (let i = 0; i < podium.length; i++) {
  const j = (i + 1) % podium.length
  const v = (q: XY, z: number): V3 => [q[0], q[1], z]
  stone.quad(v(podShell[i], PODIUM + .9), v(podShell[j], PODIUM + .9), v(parapet[j], PODIUM + .9), v(parapet[i], PODIUM + .9))
  stone.quad(v(parapet[j], PODIUM + .9), v(parapet[i], PODIUM + .9), v(parapet[i], PODIUM), v(parapet[j], PODIUM))
}
cap(roof, parapet, flat(PODIUM), true)
cap(garden, [[-50, -15], [2, -15], [2, 16], [-50, 16]], flat(PODIUM + .05))

// ---------------------------------------------------------------- write
const parts = [
  { part: glass, material: PALETTE.window },
  { part: stone, material: PALETTE.stone },
  { part: metal, material: PALETTE.trim },
  // The crown's stepped risers and the screens' inner faces: structural
  // glazing, seen from both sides.
  { part: slope, material: { ...PALETTE.glass, doubleSided: true } },
  { part: roof, material: PALETTE.roof },
  { part: garden, material: finish('duke-roof-garden', 0xa7b593) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Duke Energy Center', parts, {
  license: 'CC0-1.0', bearing: 50.3, elevation: 0, anchor: [35.22415675, -80.84873993], height: TOP,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  osm: 'way/1550692284 with its building:parts',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/duke-energy-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
