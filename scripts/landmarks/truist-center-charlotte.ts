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
 * dark blue glazed bay turns the corner inside the notch. The step grows
 * going up, so the faces widen slightly towards the top. Above the notch
 * heads the middle of each face carries on as a stone block ending in a tall
 * pointed gable; around and behind the four gables a steep hip roof of dark
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
import { Part, encodePng, writeGlb, type V3 } from './mesh'

type XY = [number, number]
const at = (p: XY, z: number): V3 => [p[0], p[1], z]
const rot = (p: XY, s: number): XY => {
  let [x, y] = p
  for (let k = 0; k < s; k++) [x, y] = [-y, x]
  return [x, y]
}
const L = (a: XY, b: XY, t: number): XY => [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]

const facade = new Part(), stone = new Part(), notch = new Part()
const roof = new Part(), deck = new Part(), skylight = new Part()

// ---------- textures ----------
// The stone faces are painted, not modelled: one tile is one bay of two
// windows between piers, a floor tall. Mipmapped, the grid averages to an
// even tone at map distance instead of shimmering as geometry would.
// Alpha marks the glass for night lighting (STYLE.md, Night): 0 on a window,
// 255 on stone. The material is opaque, so by day alpha is ignored.
const BAY = 8.25, FLOOR = 4.2
const STONE: [number, number, number] = [0xd9, 0xcf, 0xbf]
const WINDOW: [number, number, number] = [0x56, 0x66, 0x7a]
const STONE_GRID = (() => {
  const w = 64, h = 32, data = new Uint8Array(w * h * 4)
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) {
    const win = y >= 4 && y < 29 && ((x >= 12 && x < 26) || (x >= 38 && x < 52))
    data.set([...(win ? WINDOW : STONE), win ? 0 : 255], (y * w + x) * 4)
  }
  return encodePng(w, h, data)
})()
// The notch glazing: dark blue with a pale band at each floor. The band is
// spandrel, not glass, so only the blue is alpha 0.
const NOTCH_GRID = (() => {
  const w = 8, h = 32, data = new Uint8Array(w * h * 4)
  for (let y = 0; y < h; y++) for (let x = 0; x < w; x++)
    data.set(y < 4 ? [0xb9, 0xc3, 0xcc, 255] : [0x3f, 0x56, 0x70, 0], (y * w + x) * 4)
  return encodePng(w, h, data)
})()

/**
 * A wall from edge a0→b0 at z0 up to edge a1→b1 at z1, painted with a grid:
 * u counts `bay`s from u0 along the edge, v counts floors from the ground.
 */
function painted(part: Part, a0: XY, b0: XY, z0: number, a1: XY, b1: XY, z1: number, u0: number, bay: number) {
  const u1 = u0 + Math.hypot(b0[0] - a0[0], b0[1] - a0[1]) / bay
  part.quad(at(a0, z0), at(b0, z0), at(b1, z1), at(a1, z1),
    [[u0, z0 / FLOOR], [u1, z0 / FLOOR], [u1, z1 / FLOOR], [u0, z1 / FLOOR]])
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
    painted(facade, a, b, 0, a, b, Z_NOTCH0, -(h - CH) / BAY + .5, BAY)
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
  painted(facade, r([-A + CH, -h0]), r([A - CH, -h0]), Z_NOTCH0, r([-A + CH, -h1]), r([A - CH, -h1]), Z_HOOD, -(A - CH) / BAY + .5, BAY)
  shaftWall(stone, lo[0], lo[1], hi[0], hi[1])
  shaftWall(stone, lo[1], lo[2], hi[1], hi[2])
  painted(notch, lo[2], lo[3], Z_NOTCH0, hi[2], hi[3], Z_HOOD, 0, 2)
  painted(notch, lo[3], lo[4], Z_NOTCH0, hi[3], hi[4], Z_HOOD, 0, 2)
  shaftWall(stone, lo[4], lo[5], hi[4], hi[5])
  shaftWall(stone, lo[5], lo[6], hi[5], hi[6])
  // the notch head, closed over at the hood
  cap(deck, [hi[2], hi[3], hi[4], r([A, -A])], Z_HOOD, r([A, -A]))
}

// ---------- the crown ----------
// The middle of each face rises past the hood as a stone block eight metres
// deep that ends in a tall pointed gable: a pale stone frame round dark glass,
// with mullions converging on the apex. Around and behind the gables a hip
// roof of dark glass with pale ribs climbs from the notch heads to a 28 m
// square and a small lantern. Silver horns flare from the notch heads.
const BLOCK_A = 12.6, BLOCK_DEPTH = 8
const Z_EAVE = 183, Z_APEX = 196   // the gable's springing and apex
const SCREEN = 1
const Z_ROOF = 196, ROOF_TOP = 14, LANTERN = 6
const silver = new Part()

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
    painted(facade, fl, fr, Z_HOOD, fl, fr, zf, -(BLOCK_A - CH) / BAY + .5, BAY)
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
    // the gable's face: dark glass inside a pale frame
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
  // the hip roof: dark glass, pale ribs fanning from the eaves up to the hips
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
    painted(facade, a, b, 0, a, b, z1, 0, BAY)
  })
  cap(top, ring, z1, c)
}
block([[-27.8, 10], [28, 10], [28.1, 48.9], [21.5, 56.7], [-22, 56.2], [-21.9, 50.3], [-27.6, 50.3]], 48, [0, 33], skylight)
block([[-27.9, -57], [8, -57], [8, -44], [-27.9, -44]], 52, [-10, -50])
block([[8, -57], [25.8, -57], [25.8, -44], [8, -44]], 42, [17, -50])

// ---------- write ----------
const parts = [
  { part: facade, material: { name: 'window', color: 0xffffff, texture: { png: STONE_GRID } } },
  { part: notch, material: { name: 'window-2', color: 0xffffff, texture: { png: NOTCH_GRID } } },
  { part: stone, material: { name: 'stone', color: 0xd9cfbf } },
  { part: roof, material: { name: 'glass', color: 0x7a8ea6, roughness: .5 } },
  { part: silver, material: { name: 'horns', color: 0xdfe3e6, roughness: .5 } },
  { part: deck, material: { name: 'roof', color: 0xbdb9b1 } },
  { part: skylight, material: { name: 'skylights', color: 0xc4ccd2 } },
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
