/**
 * Duke Energy Center (550 South Tryon), Charlotte — original procedural
 * geometry, CC0-1.0.
 * bun scripts/landmarks/duke-energy-center.ts [out.glb]
 *
 * Anchor: the centroid of OSM way/1550692284 (-80.84874, 35.22416). Bearing
 * 50.3°, the azimuth of the tower's faces in OSM, so model x and y run along
 * the faces: -y faces south-west, +x south-east, +y north-east and -x
 * north-west. Coordinates below are OSM vertices rotated into that frame.
 *
 * The tower is a 48.6 m square with chamfered corners. Its crown is a gable
 * laid along the diagonal: the two corners on the ridge (true north and south)
 * rise to 240 m and the other two drop to 180 m, so every face ends in a
 * single steep slope, and seen across a low corner the crown reads as a V.
 * Inside the screen walls the roof climbs from the low corners to a flat deck
 * at about 219 m, leaving the top open, with a beam spanning the ridge, as the
 * OSM parts record. The 60 m parking and office podium sits on the north-west
 * side, its south-west wall in line with the tower's.
 *
 * What makes it recognisable, each drawn as plain geometry:
 * - every face is a light, reflective glass panel inside a broad pale stone
 *   frame that runs up both edges and along the sloped top;
 * - the corners are recessed slots, full height, between the two faces'
 *   frames; their darker glass is the stripe the tower lights at night;
 * - the two south faces carry stone sunshades that step out floor by floor
 *   from the low (east and west) corners, so they make a staircase triangle
 *   that widens downwards (photos from Tryon St and from the south-west).
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish, windowVariant } from './palette'

type XY = [number, number]
const glass = new Part(), slotGlass = new Part(), stone = new Part(), trim = new Part()
const roof = new Part(), garden = new Part()


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
// Counter-clockwise from the west chamfer's south end. The two high chamfers
// carry a midpoint on the ridge so the rim peaks there.
const ring10: XY[] = [
  P(-H + CH, -H), P(H - CH, -H), P(H - CH / 2, -H + CH / 2), P(H, -H + CH),
  P(H, H - CH), P(H - CH, H), P(-H + CH, H), P(-H + CH / 2, H - CH / 2), P(-H, H - CH), P(-H, -H + CH),
]
type Face = { name: string; i: number; j: number; base: number; shades?: 'start' | 'end' }
const PODIUM = 60, BASE = 6
const faces: Face[] = [
  // The sunshades start at each south face's low corner: the west end of
  // the south-west face, the east end of the south-east face.
  { name: 'south-west', i: 0, j: 1, base: BASE, shades: 'start' },
  { name: 'south-east', i: 3, j: 4, base: BASE, shades: 'end' },
  { name: 'north-east', i: 5, j: 6, base: BASE },
  { name: 'north-west', i: 8, j: 9, base: PODIUM },
]
// The corner chamfers, each [start, (ridge midpoint), end] along the ring.
const corners = [[1, 2, 3], [4, 5], [6, 7, 8], [9, 0]]

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
/** The screen walls above the roof are this thick, frame front to inner face. */
const SCREEN = 2.1
const inner = inset(outer, SCREEN)
const deck = (q: XY) => Math.min(DECK, rim(q) - DROP)

// Frame proportions. The coping is measured vertically; on a face sloping
// 56° that leaves a band about as broad as the side frames.
const FW = 4.6, BEV = .45, COPE = 6.5, SLOT = 1.8
const under = (q: XY) => rim(q) - COPE

/** A face's local frame: s along it from ring[i], n inwards from its line. */
function frameOf(f: Face) {
  const a = ring10[f.i], b = ring10[f.j]
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const d: XY = [(b[0] - a[0]) / L, (b[1] - a[1]) / L], inward: XY = [-d[1], d[0]]
  const at = (s: number, n: number) => add(add(a, d, s), inward, n)
  return { L, at }
}

/**
 * One face's stone frame at one end. The outer edge is bevelled, and the side
 * towards the corner is cut back along the slot, so the slot's two walls are
 * the frames' own sides.
 */
function cornerFrame(at: (s: number, n: number) => XY, corner: number, dir: 1 | -1): XY[] {
  const k = SLOT / Math.SQRT2
  const sn: XY[] = [
    [FW - BEV, 0], [.3, 0], [.21, .21], [k, k], [FW, k], [FW, BEV],
  ]
  const ring = sn.map(([s, n]) => at(corner - dir * s, n))
  return dir === 1 ? ring : ring.reverse()
}

// Glass, frames and base on each face. Every face edge runs from a high
// corner to a low one, so the rim is linear along it and a face's glass is a
// single sloped-top panel.
for (const f of faces) {
  const { L, at } = frameOf(f)
  const z0 = f.base
  const g = [at(FW, RECESS), at(L - FW, RECESS)]
  glass.quad([g[0][0], g[0][1], z0], [g[1][0], g[1][1], z0], [g[1][0], g[1][1], under(g[1])], [g[0][0], g[0][1], under(g[0])])
  // Frames run from the street (they are buried in the podium on its side).
  prism(stone, cornerFrame(at, 0, -1), flat(0), q => under(q) + .1, { top: false })
  prism(stone, cornerFrame(at, L, 1), flat(0), q => under(q) + .1, { top: false })
  if (f.base === BASE) prism(stone, [at(FW, 0), at(L - FW, 0), at(L - FW, RECESS + .1), at(FW, RECESS + .1)], flat(0), flat(BASE), { crease: 0 })
}

// The corner slots: a recessed strip of darker glass between the frames,
// from the street to the coping that bridges over it.
for (const c of corners) {
  const a = ring10[c[0]], b = ring10[c[c.length - 1]]
  const m = unit([(b[1] - a[1]), -(b[0] - a[0]), 0])
  // inward normal of the chamfer a→b (left of the counter-clockwise edge)
  const inw: XY = [-m[0], -m[1]]
  const back = c.map(i => add(ring10[i], inw, SLOT))
  for (let k = 0; k + 1 < back.length; k++) {
    const p = back[k], q = back[k + 1]
    slotGlass.quad([p[0], p[1], 0], [q[0], q[1], 0], [q[0], q[1], under(q) + .3], [p[0], p[1], under(p) + .3])
  }
}

// The coping: a broad stone band along the sloped top of every face, carried
// across the slots as a lintel, and capped back to the screens' inner face.
for (let i = 0; i < outer.length; i++) {
  const j = (i + 1) % outer.length
  prism(stone, [outer[i], outer[j], inner[j], inner[i]], under, rim, { crease: 0, bottom: true })
}
// The screens' inner face, glazed like the faces, down into the solid tower
// so every tread below meets it.
walls(glass, [...inner].reverse(), flat(LOW - 6), rim)

// The roof inside the screens: two slopes rising from the low corners to a
// flat deck. Clip the convex inner ring into convex pieces.
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
    const ends = band.filter(q => Math.abs(side * sOf(q) - c0) < 1e-6)
    if (ends.length === 2) {
      let [a, b] = ends
      const n: XY = [side, side]
      if ((b[0] - a[0]) * n[1] - (b[1] - a[1]) * n[0] < 0) [a, b] = [b, a]
      glass.quad([b[0], b[1], z], [a[0], a[1], z], [a[0], a[1], zUp], [b[0], b[1], zUp])
    }
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

// The sunshades: stone ledges flush with the frames, so the bounds stay the
// OSM footprint. One per floor and a half (the real ones are one a floor,
// which shimmers at map distance), each longer than the one above, so their
// free ends step down diagonally across the face. They reach the full width
// above the podium roof and run full width from there to the lobby.
const SHADE_TOP = LOW - COPE - 3, SHADE_FULL = 66, SHADE_EVERY = 5.6, SHADE = 1, SHADE_OUT = 0
const shadeLen = (z: number, span: number) =>
  Math.min(span, 2.5 + (span - 2.5) * (SHADE_TOP - z) / (SHADE_TOP - SHADE_FULL))
const shadeZ: number[] = []
for (let z = SHADE_TOP; z > BASE + 4; z -= SHADE_EVERY) shadeZ.push(z)
/** The s-range a face's sunshade covers at height z, or null. */
function shadeSpan(f: Face, L: number, z: number): [number, number] | null {
  if (!f.shades || z > SHADE_TOP + SHADE) return null
  const len = shadeLen(z, L - 2 * FW)
  return f.shades === 'start' ? [FW, FW + len] : [L - FW - len, L - FW]
}
for (const f of faces) {
  if (!f.shades) continue
  const { L, at } = frameOf(f)
  for (const z of shadeZ) {
    const [s0, s1] = shadeSpan(f, L, z)!
    // Butt into the frame, open at the free end.
    prism(stone, [at(s0, -SHADE_OUT), at(s1, -SHADE_OUT), at(s1, RECESS), at(s0, RECESS)],
      flat(z), flat(z + SHADE), { crease: 0, bottom: true })
  }
}

// Faint pale floor lines across the glass every four floors, stopping under
// the coping and short of the sunshades.
const LINE_EVERY = 16.8, LINE = .5
for (const f of faces) {
  const { L, at } = frameOf(f)
  for (let z = LINE_EVERY; z < TOP; z += LINE_EVERY) {
    if (z < f.base + 1) continue
    // where the line clears the coping: under() is linear along the face
    const u0 = under(at(FW, RECESS)), u1 = under(at(L - FW, RECESS))
    const clear = (s: number) => u0 + (u1 - u0) * (s - FW) / (L - 2 * FW) - (z + LINE)
    let s0 = FW, s1 = L - FW
    if (clear(s0) < 0 && clear(s1) < 0) continue
    if (clear(s0) < 0) s0 = FW + (L - 2 * FW) * (-clear(FW)) / (clear(L - FW) - clear(FW))
    if (clear(s1) < 0) s1 = FW + (L - 2 * FW) * (clear(FW)) / (clear(FW) - clear(L - FW))
    // Skip the stretch a nearby sunshade already covers.
    const near = shadeZ.find(zs => Math.abs(zs - z) < SHADE_EVERY / 2)
    const sh = near !== undefined ? shadeSpan(f, L, near) : null
    if (sh) { if (f.shades === 'start') s0 = Math.max(s0, sh[1]); else s1 = Math.min(s1, sh[0]) }
    if (s1 - s0 < 1) continue
    prism(trim, [at(s0, RECESS - .2), at(s1, RECESS - .2), at(s1, RECESS), at(s0, RECESS)], flat(z), flat(z + LINE), { crease: 0 })
  }
}

// ---------------------------------------------------------------- podium
const podium: XY[] = [
  [-58.3, -24.9], [1.9, -24.5], [1.9, -21.3], [7.1, -21.3], [7.1, -19.9],
  [12, -19.9], [12, 23.9], [-58.3, 23.8],
]
const podShell = inset(podium, RECESS)
walls(glass, podShell, flat(0), flat(PODIUM))
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
  // Slim pale mullions on the plinth's bay lines, and a stone band where the
  // parking floors give way to offices.
  for (let k = 1; k < count; k++) {
    const s = L * k / count
    prism(trim, strip(a, b, s - .55, s + .55, RECESS * .85, .25), flat(PLINTH + 1.6), flat(PODIUM - BAND + .1), { top: false })
  }
  prism(stone, [a, b, podShell[j], podShell[i]], flat(33), flat(34.6), { crease: 0 })
  // Corner frames run the full height, as on the tower.
  prism(stone, strip(a, b, 0, 1.8, RECESS, .35), flat(PLINTH), flat(PODIUM - BAND + .1), { top: false })
  prism(stone, strip(a, b, L - 1.8, L, RECESS, .35), flat(PLINTH), flat(PODIUM - BAND + .1), { top: false })
}
// Roof: a parapet ring, membrane, and the planted terrace.
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
  // Light, reflective curtain wall, as the tower reads in daylight.
  { part: glass, material: windowVariant(2, 0xa9bfd1) },
  // The corner slots' deeper glass: the stripe the tower lights at night.
  { part: slotGlass, material: PALETTE.window },
  { part: stone, material: PALETTE.stone },
  { part: trim, material: PALETTE.trim },
  { part: roof, material: PALETTE.roof },
  { part: garden, material: finish('duke-roof-garden', 0xa7b593) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Duke Energy Center', parts, {
  license: 'CC0-1.0', bearing: 50.3, elevation: 0, anchor: [35.22415675, -80.84873993], height: TOP,
  frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  osm: 'way/1550692284 with its building:parts',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/duke-energy-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
