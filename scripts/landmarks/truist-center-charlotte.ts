/**
 * Truist Center (formerly Hearst Tower, 2002), 214 N Tryon St, Charlotte —
 * original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/truist-center-charlotte.ts [out.glb]
 *
 * Map frame: x east, y north, z up, metres; origin at the anchor on the
 * ground. Placed at bearing 48°, so the tower's faces, square to Uptown's
 * street grid, are square to this frame.
 *
 * A square tower of pale stone on the south half of the block. Each corner
 * is notched: the stone face stops short of the corner, steps back, and a
 * blue glazed bay turns the corner inside the notch. The step grows
 * going up, so the faces widen slightly towards the top. Above the notch
 * heads the middle of each face carries on as a stone block ending in a tall
 * pointed gable; around and behind the four gables a steep hip roof of pale
 * glass with pale ribs climbs to a flat square with a small lantern, and
 * silver horns flare from the notch heads. Podiums fill the rest of the
 * block north and south.
 *
 * Sources: OSM's outline (way/131139746, 200 m) for the footprint and the
 * peak; OSM's building:parts carry no heights and poor footprints, so every
 * other dimension is measured from Google's photorealistic 3D renders (five
 * views, overlaid on this model at the same framing). Colours from those
 * renders and Commons photos.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, windowVariant } from './palette'

type XY = [number, number]
const at = (p: XY, z: number): V3 => [p[0], p[1], z]
const rot = (p: XY, s: number): XY => {
  let [x, y] = p
  for (let k = 0; k < s; k++) [x, y] = [-y, x]
  return [x, y]
}
const L = (a: XY, b: XY, t: number): XY => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]

const stone = new Part(), notch = new Part()
// The north podium's skylights read as one broad roof at map scale.
const roof = new Part(), deck = new Part(), skylight = deck

// ---------- facades ----------
// Windows are slate panels on the stone (STYLE.md, Windows): one panel per
// bay, three floors tall, with stone showing between bays as piers and
// between groups as spandrels. Groups count from the ground, so they line up
// across every face. The corner notches are blue glazing broken by pale
// floor lines every three floors.
const FLOOR = 4.2, GROUP = 3 * FLOOR, SPANDREL = 3, PIER = 2, BAY = 4.1, PROUD = .04
const windows = new Part(), floorLines = new Part()

/** A point on the quad a0→b0 (at z0), a1→b1 (at z1): `u` along, `v` up, both 0–1. */
function onQuad(a0: XY, b0: XY, z0: number, a1: XY, b1: XY, z1: number, u: number, v: number): V3 {
  const lo = L(a0, b0, u), hi = L(a1, b1, u)
  return [...L(lo, hi, v), z0 + (z1 - z0) * v] as V3
}
/** The quad's outward normal, horizontal. */
function faceNormal(a: XY, b: XY): XY {
  const l = Math.hypot(b[0] - a[0], b[1] - a[1])
  return [(b[1] - a[1]) / l, -(b[0] - a[0]) / l]
}
/**
 * Rectangles on the wall a0→b0 at z0 up to a1→b1 at z1, standing PROUD off
 * it: `bays` across, each `pier` short of its bay, between heights lo and hi.
 */
function onWall(part: Part, a0: XY, b0: XY, z0: number, a1: XY, b1: XY, z1: number,
  bays: number, pier: number, lo: number, hi: number) {
  const n = faceNormal(a0, b0), len = Math.hypot(b0[0] - a0[0], b0[1] - a0[1])
  const off = (p: V3): V3 => [p[0] + n[0] * PROUD, p[1] + n[1] * PROUD, p[2]]
  const v0 = (lo - z0) / (z1 - z0), v1 = (hi - z0) / (z1 - z0)
  for (let k = 0; k < bays; k++) {
    const u0 = (k + pier / 2 / (len / bays)) / bays, u1 = (k + 1 - pier / 2 / (len / bays)) / bays
    const q = (u: number, v: number) => off(onQuad(a0, b0, z0, a1, b1, z1, u, v))
    part.quad(q(u0, v0), q(u1, v0), q(u1, v1), q(u0, v1))
  }
}
/** A stone wall carrying window panels in bays about BAY wide. */
function stoneFace(a0: XY, b0: XY, z0: number, a1: XY, b1: XY, z1: number) {
  stone.quad(at(a0, z0), at(b0, z0), at(b1, z1), at(a1, z1))
  const len = Math.hypot(b0[0] - a0[0], b0[1] - a0[1])
  if (len < PIER + 2) return
  const bays = Math.max(1, Math.round(len / BAY))
  for (let g = Math.floor(z0 / GROUP); g * GROUP < z1; g++) {
    const lo = Math.max(g * GROUP + SPANDREL / 2, z0 + 1), hi = Math.min((g + 1) * GROUP - SPANDREL / 2, z1 - 1)
    if (hi - lo >= 4) onWall(windows, a0, b0, z0, a1, b1, z1, bays, PIER, lo, hi)
  }
}
/** A pane of the notch glazing, with a pale floor line every group. */
function notchPane(a0: XY, b0: XY, z0: number, a1: XY, b1: XY, z1: number) {
  notch.quad(at(a0, z0), at(b0, z0), at(b1, z1), at(a1, z1))
  for (let g = Math.ceil(z0 / GROUP); g * GROUP < z1 - 2; g++)
    onWall(floorLines, a0, b0, z0, a1, b1, z1, 1, 0, g * GROUP - .35, g * GROUP + .35)
}

const wall = (part: Part, a: XY, b: XY, z0: number, z1: number) =>
  part.quad(at(a, z0), at(b, z0), at(b, z1), at(a, z1))
/** Fan cap over a ring that is star-shaped about `c`. */
function cap(part: Part, ring: XY[], z: number, c: XY) {
  ring.forEach((a, i) => part.tri(at(c, z), at(a, z), at(ring[(i + 1) % ring.length], z)))
}
/** Unit normal of the plane through three points. */
function normal(a: V3, b: V3, c: V3): V3 {
  const u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], v = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
  const n: V3 = [u[1] * v[2] - u[2] * v[1], u[2] * v[0] - u[0] * v[2], u[0] * v[1] - u[1] * v[0]]
  const l = Math.hypot(...n)
  return [n[0] / l, n[1] / l, n[2] / l]
}
function triFacing(part: Part, a: V3, b: V3, c: V3, n: V3) {
  const m = normal(a, b, c)
  if (m[0] * n[0] + m[1] * n[1] + m[2] * n[2] >= 0) part.tri(a, b, c)
  else part.tri(a, c, b)
}

// ---------- the tower ----------
const TY = -17           // tower centre, south of the anchor
const A = 16.5           // half-width of each face's stone
const G = 7              // a notch's glazing, seen square on
const D0 = 0.9, D1 = 3.9 // the notch's step back at its foot and head: the flare
const Z_NOTCH0 = 50      // notches start above the base
const Z_HOOD = 165       // notch heads; the crown starts
const PEAK = 200
const T = (p: XY): XY => [p[0], p[1] + TY]
const C = T([0, 0])

const CH = .5 // chamfer on the convex stone edges

/**
 * The south-east corner's run of the plan at step `d`: the stone face's
 * chamfered end, the step back, the two panes of the glazed notch, the step
 * out and the next face's chamfered end.
 */
const corner = (d: number): XY[] => {
  const h = A + G + d
  return [[A - CH, -h], [A, -h + CH], [A, -h + d], [h - d, -h + d], [h - d, -A], [h - CH, -A], [h, -A + CH]]
}
/** A wall over the whole shaft, from edge a0→b0 at the feet to a1→b1 at the hood. */
const shaftWall = (part: Part, a0: XY, b0: XY, a1: XY, b1: XY) =>
  part.quad(at(a0, Z_NOTCH0), at(b0, Z_NOTCH0), at(b1, Z_HOOD), at(a1, Z_HOOD))

// base: the full square with chamfered corners, up to the notch feet
{
  const h = A + G + D0
  const ring: XY[] = []
  for (let s = 0; s < 4; s++) {
    const a = T(rot([-h + CH, -h], s)), b = T(rot([h - CH, -h], s)), c = T(rot([h, -h + CH], s))
    stoneFace(a, b, 0, a, b, Z_NOTCH0)
    wall(stone, b, c, 0, Z_NOTCH0)
    ring.push(a, b)
  }
  cap(deck, ring, Z_NOTCH0, C)
}
// shaft: stone faces, the notches' stepped returns, their glazed corners
for (let s = 0; s < 4; s++) {
  const r = (p: XY) => T(rot(p, s))
  const lo = corner(D0).map(r), hi = corner(D1).map(r)
  const h0 = A + G + D0, h1 = A + G + D1
  stoneFace(r([-A + CH, -h0]), r([A - CH, -h0]), Z_NOTCH0, r([-A + CH, -h1]), r([A - CH, -h1]), Z_HOOD)
  shaftWall(stone, lo[0], lo[1], hi[0], hi[1])
  shaftWall(stone, lo[1], lo[2], hi[1], hi[2])
  notchPane(lo[2], lo[3], Z_NOTCH0, hi[2], hi[3], Z_HOOD)
  notchPane(lo[3], lo[4], Z_NOTCH0, hi[3], hi[4], Z_HOOD)
  shaftWall(stone, lo[4], lo[5], hi[4], hi[5])
  shaftWall(stone, lo[5], lo[6], hi[5], hi[6])
  // the notch head, closed over at the hood
  cap(deck, [hi[2], hi[3], hi[4], r([A, -A])], Z_HOOD, r([A, -A]))
}

// ---------- the crown ----------
// The middle of each face rises past the hood as a stone block eight metres
// deep that ends in a tall pointed gable: a pale stone frame round glass,
// with mullions converging on the apex. Around and behind the gables a hip
// roof of pale glass with stone ribs climbs from the notch heads to a 28 m
// square and a small lantern. Silver horns flare from the notch heads.
const BLOCK_A = 12.6, BLOCK_DEPTH = 8
const Z_EAVE = 183, Z_APEX = 196   // the gable's springing and apex
const SCREEN = 1
const Z_ROOF = 196, ROOF_TOP = 14, LANTERN = 6
// The horns and lantern are pale metal, drawn in the floor lines' trim.
const silver = floorLines

type V = V3
const add = (a: V, b: V, k = 1): V => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const unit = (a: V): V => { const l = Math.hypot(...a); return [a[0] / l, a[1] / l, a[2] / l] }
const cross3 = (a: V, b: V): V => [a[1] * b[2] - a[2] * b[1], a[2] * b[0] - a[0] * b[2], a[0] * b[1] - a[1] * b[0]]

/**
 * A strip raised `t` off a plane with outward normal `n`: the polygon `pts`
 * (on the plane, convex) lifted, plus its side walls down to the plane.
 */
function raised(part: Part, pts: V[], n: V, t: number) {
  const top = pts.map((p) => add(p, n, t))
  for (let i = 1; i < top.length - 1; i++) triFacing(part, top[0], top[i], top[i + 1], n)
  const c = pts.reduce((m, p) => add(m, p, 1 / pts.length), [0, 0, 0] as V)
  pts.forEach((a, i) => {
    const b = pts[(i + 1) % pts.length], e = add(add(a, b, 1), c, -2)
    const out = unit(add(e, n, -(e[0] * n[0] + e[1] * n[1] + e[2] * n[2])))
    triFacing(part, a, b, top[(i + 1) % pts.length], out)
    triFacing(part, a, top[(i + 1) % pts.length], top[i], out)
  })
}
/** A raised rib of width `w` along a→b on a plane with normal `n`. */
function rib(part: Part, a: V, b: V, n: V, w: number, t: number) {
  const side = unit(cross3(n, add(b, a, -1)))
  raised(part, [add(a, side, -w / 2), add(b, side, -w / 2), add(b, side, w / 2), add(a, side, w / 2)], n, t)
}

{
  const h = A + G + D1
  const slope = (Z_APEX - Z_EAVE) / BLOCK_A
  const zf = Z_EAVE + slope * CH // the gable's foot, over the chamfers
  for (let s = 0; s < 4; s++) {
    const r = (p: XY) => T(rot(p, s))
    const r3 = (x: number, y: number, z: number): V => [...r([x, y]), z] as V
    const out: V = [...rot([0, -1], s), 0] as V
    const yb = -h + BLOCK_DEPTH
    // the block: chamfered front corners, walls to the eave, a gabled top
    const fl = r([-BLOCK_A + CH, -h]), fr = r([BLOCK_A - CH, -h])
    const cl = r([-BLOCK_A, -h + CH]), cr = r([BLOCK_A, -h + CH])
    const bl = r([-BLOCK_A, yb]), br = r([BLOCK_A, yb])
    stoneFace(fl, fr, Z_HOOD, fl, fr, zf)
    stone.quad(at(fr, Z_HOOD), at(cr, Z_HOOD), at(cr, Z_EAVE), at(fr, zf))
    stone.quad(at(cl, Z_HOOD), at(fl, Z_HOOD), at(fl, zf), at(cl, Z_EAVE))
    wall(stone, cr, br, Z_HOOD, Z_EAVE)
    wall(stone, bl, cl, Z_HOOD, Z_EAVE)
    wall(stone, br, bl, Z_HOOD, Z_EAVE)
    // the block's flat top behind the gable
    stone.quad(at(fl, Z_EAVE), at(fr, Z_EAVE), at(br, Z_EAVE), at(bl, Z_EAVE))
    stone.tri(at(fr, Z_EAVE), at(cr, Z_EAVE), at(br, Z_EAVE))
    stone.tri(at(fl, Z_EAVE), at(bl, Z_EAVE), at(cl, Z_EAVE))
    // The gable is a screen a metre thick standing on the block's front,
    // free of the roof behind it: a solid triangular prism.
    const ridgeF = r3(0, -h, Z_APEX), ridgeB = r3(0, -h + SCREEN, Z_APEX)
    for (const sg of [-1, 1]) {
      const fF = r3(sg * (BLOCK_A - CH), -h, zf), fB = r3(sg * (BLOCK_A - CH), -h + SCREEN, zf)
      const hint = add([0, 0, 1], [...rot([sg, 0], s), 0] as V, slope)
      triFacing(stone, fF, ridgeF, ridgeB, hint)
      triFacing(stone, fF, ridgeB, fB, hint)
    }
    triFacing(stone, r3(BLOCK_A - CH, -h + SCREEN, zf), r3(-BLOCK_A + CH, -h + SCREEN, zf), ridgeB, [-out[0], -out[1], 0])
    // and the chamfers' sloped tops, from the eave up to the gable's foot
    for (const sg of [-1, 1]) triFacing(stone, r3(sg * (BLOCK_A - CH), -h, zf), r3(sg * BLOCK_A, -h + CH, Z_EAVE), r3(sg * (BLOCK_A - CH), -h + SCREEN, zf), [0, 0, 1])
    // the gable's face: glass inside a pale frame
    const gl = r3(-BLOCK_A + CH, -h, zf), gr = r3(BLOCK_A - CH, -h, zf)
    triFacing(roof, gl, gr, ridgeF, out)
    const FRAME = 1.5, half = BLOCK_A - CH, rise = Z_APEX - zf
    const cosA = half / Math.hypot(half, rise), sinA = rise / Math.hypot(half, rise)
    const innerApex = r3(0, -h, Z_APEX - FRAME / cosA)
    for (const sg of [-1, 1]) {
      const footO = r3(sg * half, -h, zf), footI = r3(sg * (half - FRAME / sinA), -h, zf)
      raised(stone, [footO, ridgeF, innerApex, footI], out, .35)
    }
    // a sill across the gable's foot, and mullions converging on the apex
    raised(stone, [r3(-half, -h, zf), r3(half, -h, zf), r3(half - 1.2 / Math.tan(Math.atan2(rise, half)), -h, zf + 1.2), r3(-half + 1.2 / Math.tan(Math.atan2(rise, half)), -h, zf + 1.2)], out, .3)
    for (const x of [-5, 0, 5]) rib(stone, r3(x, -h, zf + 1.2), r3(x * .12, -h, Z_APEX - FRAME / cosA - 1), out, .6, .25)
    // the shaft's stone shoulders either side of the block, at the hood
    deck.quad(at(r([BLOCK_A, -h]), Z_HOOD), at(r([A - CH, -h]), Z_HOOD), at(r([A, -h + CH]), Z_HOOD), at(r([BLOCK_A, -h + CH]), Z_HOOD))
    deck.quad(at(r([BLOCK_A, -h + CH]), Z_HOOD), at(r([A, -h + CH]), Z_HOOD), at(r([A, yb]), Z_HOOD), at(r([BLOCK_A, yb]), Z_HOOD))
    deck.quad(at(r([-A + CH, -h]), Z_HOOD), at(r([-BLOCK_A, -h]), Z_HOOD), at(r([-BLOCK_A, yb]), Z_HOOD), at(r([-A, yb]), Z_HOOD))
    deck.tri(at(r([-A + CH, -h]), Z_HOOD), at(r([-A, yb]), Z_HOOD), at(r([-A, -h + CH]), Z_HOOD))
    // Horns: each notch head's two outer stone corners flare out and up
    // into a pointed silver horn.
    // Both lean towards the south-east corner, (+x, −y) in this frame.
    for (const c1 of [[A - .8, -h + .8], [h - .8, -A + .8]] as XY[]) {
      const dx = 1, dy = -1
      const sq: XY[] = [[c1[0] - .8, c1[1] - .8], [c1[0] + .8, c1[1] - .8], [c1[0] + .8, c1[1] + .8], [c1[0] - .8, c1[1] + .8]]
      const apex = r3(c1[0] + dx * 2, c1[1] + dy * 2, Z_HOOD + 6)
      const ring = sq.map((p) => r3(p[0], p[1], Z_HOOD - .3))
      const mid = r3(c1[0], c1[1], Z_HOOD)
      ring.forEach((a, i) => {
        const b = ring[(i + 1) % 4], e = add(add(a, b, .5), mid, -1)
        triFacing(silver, a, b, apex, unit([e[0], e[1], .4]))
      })
    }
  }
  // the hip roof: pale glass, stone ribs fanning from the eaves up to the hips
  const R = h - D1
  const base = [0, 1, 2, 3].map((s) => T(rot([R, -R], s)))
  const top = [0, 1, 2, 3].map((s) => T(rot([ROOF_TOP, -ROOF_TOP], s)))
  const P = (p: XY, z: number) => at(p, z)
  const lerp3 = (a: V, b: V, t: number): V => add(a, add(b, a, -1), t)
  for (let i = 0; i < 4; i++) {
    const j = (i + 3) % 4 // corners run SE, NE, NW, SW; face i spans corner j → i
    const b0 = P(base[j], Z_HOOD), b1 = P(base[i], Z_HOOD), t1 = P(top[i], Z_ROOF), t0 = P(top[j], Z_ROOF)
    roof.quad(b0, b1, t1, t0)
    const n = normal(b0, b1, t1)
    const lift = (p: V): V => add(p, n, .05)
    const mb = lerp3(b0, b1, .5)
    for (const [cb, ct] of [[b0, t0], [b1, t1]] as [V, V][]) {
      // the hip itself, half a rib on each facet
      const inward = unit(add(mb, cb, -1))
      raised(stone, [lift(cb), lift(ct), lift(add(ct, inward, .55)), lift(add(cb, inward, .55))], n, .25)
      for (const [e, k] of [[.22, .3], [.45, .55], [.68, .78], [.9, .97]]) rib(stone, lift(lerp3(cb, mb, e)), lift(lerp3(cb, ct, k)), n, .8, .25)
    }
  }
  cap(deck, top, Z_ROOF, C)
  const lr = [0, 1, 2, 3].map((s) => T(rot([LANTERN, -LANTERN], s)))
  lr.forEach((a, i) => wall(silver, a, lr[(i + 1) % 4], Z_ROOF, PEAK - 2))
  lr.forEach((a, i) => roof.tri(at(a, PEAK - 2), at(lr[(i + 1) % 4], PEAK - 2), at(C, PEAK)))
}

// ---------- podiums ----------
// Footprints from the OSM outline, heights from the renders. The north
// podium's roof is mostly skylights.
function block(ring: XY[], z1: number, c: XY, top = deck) {
  ring.forEach((a, i) => {
    const b = ring[(i + 1) % ring.length]
    stoneFace(a, b, 0, a, b, z1)
  })
  cap(top, ring, z1, c)
}
block([[-27.8, 10], [28, 10], [28.1, 48.9], [21.5, 56.7], [-22, 56.2], [-21.9, 50.3], [-27.6, 50.3]], 48, [0, 33], skylight)
block([[-27.9, -57], [8, -57], [8, -44], [-27.9, -44]], 52, [-10, -50])
block([[8, -57], [25.8, -57], [25.8, -44], [8, -44]], 42, [17, -50])

// ---------- write ----------
// Pale stone, slate windows, and the identity colour kept muted: the blue
// glazing of the corner notches. The crown's glass roof and gable glazing
// are pale structural glass.
const parts = [
  { part: stone, material: PALETTE.stone },
  { part: windows, material: PALETTE.window },
  { part: notch, material: windowVariant(2, 0x7590ad) },
  { part: floorLines, material: PALETTE.trim },
  { part: roof, material: PALETTE.glass },
  { part: deck, material: PALETTE.roof },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Truist Center', parts, {
  license: 'CC0-1.0', bearing: 48, elevation: 0, height: PEAK,
  frame: 'Y up, -Z north, +X east, metres; origin at the anchor on the ground',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = process.argv[2] ?? new URL('../../landmarks/models/truist-center-charlotte.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
