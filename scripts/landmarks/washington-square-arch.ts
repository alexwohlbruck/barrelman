/**
 * Washington Square Arch — procedural, CC0-1.0, no textures or source mesh.
 * bun scripts/landmarks/washington-square-arch.ts
 *
 * Map frame: x = east along the arch's long axis, y = north up Fifth Avenue,
 * z up, metres. Placed at bearing 32°. The OSM envelope is 19.1 × 7.0 m and the
 * real height 23.5 m; the opening is the real 30 × 47 ft (9.0 × 14.3 m).
 *
 * Styled to landmarks/STYLE.md: warm ivory marble, bevelled edges, one bold
 * archivolt, paneled piers, broad spandrel reliefs, an entablature and an
 * attic with recessed panels. The two Washington statues stand at the foot of
 * the north face as chunky mirrored forms. Everything is mirrored about both
 * axes (the statues about x only) and stays inside the 19.1 × 7.0 outline.
 */
import { Part, addGltfTriangles, cross, sub, writeGlb, type V3 } from './mesh'

const marble = new Part()   // piers, spandrels, attic walls
const trim = new Part()     // mouldings, archivolt, keystone and raised reliefs
const recess = new Part()   // the coffered vault soffit, in shadow
const relief = new Part()   // carved frieze and upper panel fields
const statues = new Part()
const roof = new Part()

const X = 9.15        // body half-width
const D = 2.75        // body half-depth
const B = 0.35        // corner bevel of the body
const BJ = 0.3        // bevel on the passage jambs
const Z0 = 1.6        // top of the base course
const TOP = 15.7      // underside of the architrave
const SPRING = 9.8
const R = 4.5         // opening radius: 9.0 m wide, crown at 14.3 m
const RA = 5.7        // archivolt outer radius
const PROJ = 0.32     // archivolt and impost projection
const SEG = 12        // segments per semicircle
const PANEL = 0.3     // panel recess depth
const REVEAL = 0.12

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))

// Mirror applied to everything emitted through `q`, so one quarter of the
// arch is authored and the rest follows exactly.
let MX = 1, MY = 1
const m = (v: V3): V3 => [v[0] * MX, v[1] * MY, v[2]]
function mirrored(build: () => void, xs = [1, -1], ys = [1, -1]) {
  for (const sx of xs) for (const sy of ys) { MX = sx; MY = sy; build() }
  MX = 1; MY = 1
}

/** A quad with per-corner normals; winding follows the normals, so mirroring is safe. */
function q(p: Part, pts: V3[], ns: V3[]) {
  const P = pts.map(m), N = ns.map(n => unit(m(n)))
  const face = cross(sub(P[1], P[0]), sub(P[2], P[0]))
  const avg = N.reduce(add, [0, 0, 0] as V3)
  const [a, b, c, d] = dot(face, avg) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
  p.tri(P[a], P[b], P[c], undefined, undefined, undefined, [N[a], N[b], N[c]])
  p.tri(P[a], P[c], P[d], undefined, undefined, undefined, [N[a], N[c], N[d]])
}
const flat = (p: Part, pts: V3[], n: V3) => q(p, pts, [n, n, n, n])
function fan(p: Part, ring: V3[], n: V3) {
  for (let i = 1; i < ring.length - 1; i++) {
    const P = [ring[0], ring[i], ring[i + 1]].map(m), N = unit(m(n))
    const ok = dot(cross(sub(P[1], P[0]), sub(P[2], P[0])), N) >= 0
    p.tri(P[0], ok ? P[1] : P[2], ok ? P[2] : P[1], undefined, undefined, undefined, [N, N, N])
  }
}

/** Plan rim of a rectangle with chamfered corners, each point with its face normal. */
function rim(x0: number, x1: number, y0: number, y1: number, r: number, z: number) {
  r = Math.max(r, 0.04)
  const S: V3 = [0, -1, 0], E: V3 = [1, 0, 0], Nn: V3 = [0, 1, 0], W: V3 = [-1, 0, 0]
  return {
    pts: [[x0 + r, y0, z], [x1 - r, y0, z], [x1, y0 + r, z], [x1, y1 - r, z],
      [x1 - r, y1, z], [x0 + r, y1, z], [x0, y1 - r, z], [x0, y0 + r, z]] as V3[],
    ns: [S, S, E, E, Nn, Nn, W, W],
  }
}

/** A box with rounded vertical edges and optional rounded top and bottom edges. */
function rbox(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number,
  { r = 0.3, top = 0, bot = 0, caps = true } = {}) {
  const up: V3 = [0, 0, 1], down: V3 = [0, 0, -1]
  const rings: { pts: V3[]; ns: V3[] }[] = []
  if (bot > 0) rings.push({ ...rim(x0 + bot, x1 - bot, y0 + bot, y1 - bot, r - bot * .41, z0), ns: Array(8).fill(down) })
  rings.push(rim(x0, x1, y0, y1, r, z0 + bot))
  rings.push(rim(x0, x1, y0, y1, r, z1 - top))
  if (top > 0) rings.push({ ...rim(x0 + top, x1 - top, y0 + top, y1 - top, r - top * .41, z1), ns: Array(8).fill(up) })
  for (let k = 0; k < rings.length - 1; k++) for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8, a = rings[k], b = rings[k + 1]
    q(p, [a.pts[i], a.pts[j], b.pts[j], b.pts[i]], [a.ns[i], a.ns[j], b.ns[j], b.ns[i]])
  }
  if (caps) {
    fan(p, rings[rings.length - 1].pts, up)
    fan(p, rings[0].pts, down)
  }
}

type Hole = [number, number, number, number]
/**
 * A planar face (origin O, axes U and V, outward normal N) over [s0,s1]×[t0,t1],
 * with recessed panels: a chamfered reveal down to a back in `back`.
 */
function panelled(p: Part, back: Part, O: V3, U: V3, V: V3, N: V3,
  [s0, s1, t0, t1]: Hole, holes: Hole[] = [], depth = PANEL, bevel = REVEAL) {
  const P = (s: number, t: number, d = 0): V3 => add(add(add(O, mul(U, s)), mul(V, t)), mul(N, -d))
  const S = [...new Set([s0, s1, ...holes.flatMap(h => [h[0], h[1]])])].sort((a, b) => a - b)
  const T = [...new Set([t0, t1, ...holes.flatMap(h => [h[2], h[3]])])].sort((a, b) => a - b)
  for (let i = 0; i < S.length - 1; i++) for (let j = 0; j < T.length - 1; j++) {
    const sm = (S[i] + S[i + 1]) / 2, tm = (T[j] + T[j + 1]) / 2
    if (holes.some(h => sm > h[0] && sm < h[1] && tm > h[2] && tm < h[3])) continue
    flat(p, [P(S[i], T[j]), P(S[i + 1], T[j]), P(S[i + 1], T[j + 1]), P(S[i], T[j + 1])], N)
  }
  for (const [a, b, c, d] of holes) {
    const o = [P(a, c), P(b, c), P(b, d), P(a, d)]
    const i = [P(a + bevel, c + bevel, depth), P(b - bevel, c + bevel, depth),
      P(b - bevel, d - bevel, depth), P(a + bevel, d - bevel, depth)]
    flat(back, i, N)
    // The reveal's inner edge turns into the recess, so it reads as a soft lip.
    const toward = [V, mul(U, -1), mul(V, -1), U]
    for (let k = 0; k < 4; k++) {
      const l = (k + 1) % 4, n = unit(add(mul(N, .4), toward[k]))
      q(p, [o[k], o[l], i[l], i[k]], [N, N, n, n])
    }
  }
}

/** A vertical rounded strip joining two faces at a corner. */
function corner(p: Part, a: V3, na: V3, b: V3, nb: V3, z0: number, z1: number) {
  q(p, [[a[0], a[1], z0], [b[0], b[1], z0], [b[0], b[1], z1], [a[0], a[1], z1]], [na, nb, nb, na])
}

const Ny: V3 = [0, 1, 0], Nx: V3 = [1, 0, 0], NxIn: V3 = [-1, 0, 0]
const ex: V3 = [1, 0, 0], ey: V3 = [0, 1, 0], ez: V3 = [0, 0, 1]

// Paneled piers. The lower panels are shallow and self-coloured, as the real
// lower piers are near-plain ashlar; the upper panels are deeper, carved
// fields that frame the medallions beside the arch, clear of the archivolt.
const LOWER: Hole = [5.25, 8.4, 2.6, 8.3]
const UPPER: Hole = [5.9, 8.5, 10.4, 15.0]
const SIDE_LOWER: Hole = [-1.6, 1.6, 2.6, 8.3]
const SIDE_UPPER: Hole = [-1.6, 1.6, 10.4, 15.0]

// One quarter of the body: east pier, north face. Mirrored to all four.
mirrored(() => {
  // Front face of the pier below and beside the arch.
  panelled(marble, marble, [0, D, 0], ex, ez, Ny, [R + BJ, X - B, Z0, SPRING], [LOWER], .2)
  panelled(marble, relief, [0, D, 0], ex, ez, Ny, [RA, X - B, SPRING, TOP], [UPPER])
  // Spandrel above the archivolt, strip by strip so the opening is real.
  for (let i = 0; i < SEG / 2; i++) {
    const a = i * Math.PI / SEG, b = (i + 1) * Math.PI / SEG
    const xa = RA * Math.cos(a), xb = RA * Math.cos(b)
    flat(marble, [[xa, D, SPRING + RA * Math.sin(a)], [xb, D, SPRING + RA * Math.sin(b)],
      [xb, D, TOP], [xa, D, TOP]], Ny)
  }
  // Rounded outer and jamb corners.
  corner(marble, [X - B, D, 0], Ny, [X, D - B, 0], Nx, Z0, TOP)
  corner(marble, [R + BJ, D, 0], Ny, [R, D - BJ, 0], NxIn, Z0, SPRING)
  // Vault soffit, a true half-cylinder through the arch.
  for (let i = 0; i < SEG / 2; i++) {
    const a = i * Math.PI / SEG, b = (i + 1) * Math.PI / SEG
    const pa = (y: number): V3 => [R * Math.cos(a), y, SPRING + R * Math.sin(a)]
    const pb = (y: number): V3 => [R * Math.cos(b), y, SPRING + R * Math.sin(b)]
    const na: V3 = [-Math.cos(a), 0, -Math.sin(a)], nb: V3 = [-Math.cos(b), 0, -Math.sin(b)]
    q(recess, [pa(0), pb(0), pb(D), pa(D)], [na, nb, nb, na])
  }
  // The bold archivolt: a rounded ring standing proud of the face.
  const prof: [number, number, V3][] = [
    [R, 0, [-1, 0, 0]], [R, PROJ - .1, [-1, 0, 0]], [R + .1, PROJ, [0, 1, 0]],
    [RA - .15, PROJ, [0, 1, 0]], [RA, PROJ - .13, [1, 0, 0]], [RA, 0, [1, 0, 0]],
  ]
  for (let i = 0; i < SEG / 2; i++) {
    const a = i * Math.PI / SEG, b = (i + 1) * Math.PI / SEG
    const at = (r: number, dy: number, ang: number): V3 => [r * Math.cos(ang), D + dy, SPRING + r * Math.sin(ang)]
    // Profile normals are in (radial, y); turn the radial part to this angle.
    const nAt = (n: V3, ang: number): V3 => [n[0] * Math.cos(ang), n[1], n[0] * Math.sin(ang)]
    for (let k = 0; k < prof.length - 1; k++) {
      const [r0, d0, n0] = prof[k], [r1, d1, n1] = prof[k + 1]
      q(trim, [at(r0, d0, a), at(r1, d1, a), at(r1, d1, b), at(r0, d0, b)],
        [nAt(n0, a), nAt(n1, a), nAt(n1, b), nAt(n0, b)])
    }
  }
})

// East and west sides, and the passage jambs.
mirrored(() => {
  panelled(marble, marble, [X, 0, 0], ey, ez, Nx, [-D + B, D - B, Z0, SPRING], [SIDE_LOWER], .2)
  panelled(marble, relief, [X, 0, 0], ey, ez, Nx, [-D + B, D - B, SPRING, TOP], [SIDE_UPPER])
  flat(marble, [[R, -D + BJ, Z0], [R, D - BJ, Z0], [R, D - BJ, SPRING], [R, -D + BJ, SPRING]], NxIn)
}, [1, -1], [1])

mirrored(() => {
  // Plinth and base course under each pier; the passage stays open to the ground.
  rbox(trim, 4.2, 9.55, -3.5, 3.5, 0, 0.45, { r: .2, top: .12 })
  rbox(marble, 4.35, 9.35, -3.0, 3.0, 0.45, Z0, { r: .3, top: .2 })
  // Impost band wrapping each pier at the spring of the arch.
  rbox(trim, R - PROJ + .1, X + .15, -D - PROJ, D + PROJ, 9.0, SPRING, { r: .2, top: .1, bot: .1 })
  // Medallions in the upper panels: flush discs, the seals of the real arch.
  for (const sy of [1, -1]) {
    const cx = (UPPER[0] + UPPER[1]) / 2, cz = (UPPER[2] + UPPER[3]) / 2, y = sy * (D - PANEL + .14)
    const ringPts = Array.from({ length: 12 }, (_, i): V3 => {
      const a = i * Math.PI / 6
      return [cx + .82 * Math.cos(a), y, cz + .95 * Math.sin(a)]
    })
    fan(trim, ringPts, [0, sy, 0])
    for (let i = 0; i < 12; i++) {
      const a = ringPts[i], b = ringPts[(i + 1) % 12], o: V3 = [0, -sy * .14, 0]
      const n = unit([a[0] + b[0] - 2 * cx, 0, a[2] + b[2] - 2 * cz])
      flat(trim, [a, b, add(b, o), add(a, o)], n)
    }
  }
}, [1, -1], [1])

// Keystone at the crown, proud of the archivolt.
mirrored(() => rbox(trim, -.7, .7, D - .1, D + .4, SPRING + R, TOP, { r: .12 }), [1], [1, -1])

/** A flat relief shape raised a little off the face plane. */
function raised(p: Part, outline: [number, number][], y0: number, h: number) {
  // Walls need the outline counter-clockwise in (x, z) for outward normals.
  const area = outline.reduce((s, [x0, z0], i) => {
    const [x1, z1] = outline[(i + 1) % outline.length]
    return s + x0 * z1 - x1 * z0
  }, 0)
  const ring = area < 0 ? [...outline].reverse() : outline
  fan(p, ring.map(([x, z]): V3 => [x, y0 + h, z]), Ny)
  for (let i = 0; i < ring.length; i++) {
    const [x0, z0] = ring[i], [x1, z1] = ring[(i + 1) % ring.length]
    if (Math.hypot(x1 - x0, z1 - z0) < 1e-6) continue
    flat(p, [[x0, y0, z0], [x1, y0, z1], [x1, y0 + h, z1], [x0, y0 + h, z0]], unit([z1 - z0, 0, -(x1 - x0)]))
  }
}

// Spandrel reliefs: a reclining winged Victory in each, read as a soft
// crescent lying along the archivolt with one broad wing raised behind it.
mirrored(() => {
  const at = (a: number, r: number): [number, number] =>
    [Math.min(r * Math.cos(a), RA + .15), Math.min(SPRING + r * Math.sin(a), TOP - .3)]
  const steps = 5, a0 = 0.42, a1 = 1.36
  for (let k = 0; k < steps; k++) {
    const a = a0 + (a1 - a0) * k / steps, b = a0 + (a1 - a0) * (k + 1) / steps
    // Swells in the middle and tapers to both ends, like a reclining body.
    const w = (t: number) => .2 + .75 * Math.sin(Math.PI * t)
    raised(trim, [at(a, RA + .18), at(b, RA + .18), at(b, RA + .18 + w((k + 1) / steps)), at(a, RA + .18 + w(k / steps))], D, .14)
  }
  raised(trim, [[4.55, 13.6], [5.3, 14.1], [5.0, 15.1], [4.2, 15.35], [3.7, 14.55]], D, .09)
})

// Entablature: architrave, carved frieze, bed moulding and corona.
rbox(trim, -X - .1, X + .1, -D - .12, D + .12, TOP, 16.25, { r: .25, top: .08, bot: .1 })
rbox(relief, -X, X, -D, D, 16.25, 17.75, { r: B, caps: false })
rbox(trim, -X - .15, X + .15, -D - .2, D + .2, 17.75, 18.15, { r: .3, bot: .12, caps: false })
rbox(trim, -9.55, 9.55, -3.15, 3.15, 18.15, 19.0, { r: .4, top: .3, bot: .2 })

// Attic with its recessed inscription panel and two smaller side panels.
const AX = 9.05, AD = 2.7, A0 = 19.0, A1 = 22.85
mirrored(() => panelled(marble, relief, [0, AD, 0], ex, ez, Ny, [-AX + B, AX - B, A0, A1],
  [[-5.0, 5.0, 19.6, 22.25], [-8.3, -5.9, 19.95, 21.9], [5.9, 8.3, 19.95, 21.9]]), [1], [1, -1])
mirrored(() => panelled(marble, relief, [AX, 0, 0], ey, ez, Nx, [-AD + B, AD - B, A0, A1],
  [[-1.5, 1.5, 19.95, 21.9]]), [1, -1], [1])
mirrored(() => corner(marble, [AX - B, AD, 0], Ny, [AX, AD - B, 0], Nx, A0, A1))

// Attic coping: a rounded lip around a marble roof.
{
  const outer = rim(-9.3, 9.3, -2.95, 2.95, .4, A1)
  const lower = rim(-9.3, 9.3, -2.95, 2.95, .4, 23.2)
  const crest = rim(-9.0, 9.0, -2.65, 2.65, .28, 23.5)
  const inner = rim(-8.6, 8.6, -2.25, 2.25, .12, 23.3)
  const up: V3 = [0, 0, 1]
  for (let i = 0; i < 8; i++) {
    const j = (i + 1) % 8
    q(trim, [outer.pts[i], outer.pts[j], lower.pts[j], lower.pts[i]], [outer.ns[i], outer.ns[j], lower.ns[j], lower.ns[i]])
    q(trim, [lower.pts[i], lower.pts[j], crest.pts[j], crest.pts[i]], [lower.ns[i], lower.ns[j], up, up])
    const ii = mul(inner.ns[i], -1), ij = mul(inner.ns[j], -1)
    q(trim, [crest.pts[i], crest.pts[j], inner.pts[j], inner.pts[i]], [up, up, unit(add(up, ij)), unit(add(up, ii))])
  }
  fan(trim, outer.pts, [0, 0, -1])
  fan(roof, inner.pts, up)
}

// Washington at War (east) and at Peace (west) on the north face: chunky
// robed figures on pedestals, kept within the outline and mirrored.
function smooth(p: Part, build: (t: Part) => void, crease = 60) {
  const t = new Part()
  build(t)
  addGltfTriangles(p, new Float32Array(t.pos), Uint32Array.from({ length: t.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}
const ellipse = (cx: number, cy: number, z: number, rx: number, ry: number, n = 10): V3[] =>
  Array.from({ length: n }, (_, i) => [cx + rx * Math.cos(i * 2 * Math.PI / n + Math.PI / n), cy + ry * Math.sin(i * 2 * Math.PI / n + Math.PI / n), z])
for (const sx of [1, -1]) {
  const cx = sx * 6.85, cy = 3.08
  rbox(trim, cx - 1.25, cx + 1.25, D - .05, 3.45, 0.45, 2.9, { r: .2, top: .15 })
  smooth(statues, t => {
    // A cloaked column, broad rounded shoulders, a short neck and a big head.
    const rings = ([
      [2.9, .9, .37], [3.3, .84, .37], [5.2, .74, .37], [5.9, .84, .37], [6.35, .8, .36],
      [6.58, .52, .33], [6.68, .3, .26], [6.8, .36, .3], [7.12, .41, .33], [7.42, .35, .3], [7.58, .12, .14],
    ] as [number, number, number][]).map(([z, rx, ry]) => ellipse(cx, cy, z, rx, ry))
    t.loft(rings)
    t.cap(rings[rings.length - 1], true)
  })
}

// Colours: warm ivory marble per the brief, keeping the photos' warm-neutral
// hue (photo 02's sunlit pier and robe, photo 01's attic) at daylight value.
const parts = [
  { part: marble, material: { name: 'ivory-marble', color: 0xe3dccc } },
  { part: trim, material: { name: 'marble-mouldings', color: 0xebe5d7 } },
  { part: recess, material: { name: 'vault-soffit', color: 0xb0a592 } },
  { part: relief, material: { name: 'carved-relief', color: 0xd2c8b5 } },
  { part: statues, material: { name: 'statues', color: 0xccc1ad } },
  { part: roof, material: { name: 'attic-roof', color: 0xd6cfc0 } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Washington Square Arch', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 32, elevation: 0, footprint: [19.1, 7], height: 23.5,
  openingWidth: 2 * R, openingHeight: SPRING + R, northFace: 'War east; Peace west',
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/washington-square-arch.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
