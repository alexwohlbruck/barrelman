/**
 * Rose Center for Earth and Space (American Museum of Natural History) —
 * procedural, CC0-1.0.
 * bun scripts/landmarks/rose-center.ts
 *
 * Map frame: x east, y north, z up, metres; origin at the centre of the glass
 * cube, on the ground. Placed at bearing 27°, the street grid's, so +y faces
 * West 81st Street and +x runs along it towards Central Park West.
 *
 * What is modelled: the glass cube (36 m square, about 32 m to the top of its
 * frame), the white Hayden Sphere (27 m across, 87 ft) on its splayed legs,
 * and the granite podium on 81st Street with its low segmental arch over the
 * entrance, wrapping round the cube as the terrace deck it stands in. Sizes
 * come from photos (frontal, perspective-corrected) and aerial imagery; OSM
 * maps the whole museum as one outline with no parts, so it gives no Rose
 * Center footprint or heights of its own.
 *
 * The glass. The renderer has no blending, only opaque and alpha-masked
 * surfaces, so glass cannot be seen through. Instead each glass face is
 * single-sided and faces *into* the cube: with back faces culled, the walls
 * nearest the camera vanish and the far ones show their inside as a pale
 * blue-grey backdrop behind the sphere — from any side, at any bearing, since
 * which walls are "near" follows the camera. The roof glass faces down, so it
 * too is culled from above and the sphere shows through the roof frame. The
 * light frame (corners, a few broad divisions, the roof's edge and centre cross) is solid and
 * carries the cube's outline. The shadow pass draws both faces, so the cube
 * still casts a full shadow.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

const glass = new Part()      // inward-facing walls and roof
const frame = new Part()      // white steel frame
const sphere = new Part()     // Hayden Sphere and its legs
const granite = new Part()    // podium walls
const deck = new Part()       // terrace paving on the podium
const floor = new Part()      // the hall floor inside the cube
const entrance = new Part()   // dark glazing under the arch

const H = 18                  // cube half-width (36 m)
const TOP = 32                // top of the cube frame
const DECK = 6                // podium / terrace height
const FRONT = 29.5            // podium face on 81st Street (OSM outline edge)
const BACK = -21              // terrace edge behind the cube
const SIDE = 27               // podium half-length along the street
const R = 13.25               // sphere radius (87 ft diameter)
const CZ = 17.5               // sphere centre height

const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => { const l = Math.hypot(...a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }

/** A convex polygon facing `n`, whatever order its corners come in. */
function face(p: Part, pts: V3[], n: V3, normals?: V3[]) {
  const ok = dot(cross(sub(pts[1], pts[0]), sub(pts[2], pts[0])), n) >= 0
  const P = ok ? pts : [...pts].reverse()
  const N = normals ? (ok ? normals : [...normals].reverse()) : pts.map(() => n)
  for (let i = 1; i < P.length - 1; i++) p.tri(P[0], P[i], P[i + 1], undefined, undefined, undefined, [N[0], N[i], N[i + 1]])
}

/** An axis-aligned box; `skip` names faces left out (+x -x +y -y +z -z). */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, skip = '') {
  const c = (x: number, y: number, z: number): V3 => [x, y, z]
  if (!skip.includes('+x')) face(p, [c(x1, y0, z0), c(x1, y1, z0), c(x1, y1, z1), c(x1, y0, z1)], [1, 0, 0])
  if (!skip.includes('-x')) face(p, [c(x0, y0, z0), c(x0, y1, z0), c(x0, y1, z1), c(x0, y0, z1)], [-1, 0, 0])
  if (!skip.includes('+y')) face(p, [c(x0, y1, z0), c(x1, y1, z0), c(x1, y1, z1), c(x0, y1, z1)], [0, 1, 0])
  if (!skip.includes('-y')) face(p, [c(x0, y0, z0), c(x1, y0, z0), c(x1, y0, z1), c(x0, y0, z1)], [0, -1, 0])
  if (!skip.includes('+z')) face(p, [c(x0, y0, z1), c(x1, y0, z1), c(x1, y1, z1), c(x0, y1, z1)], [0, 0, 1])
  if (!skip.includes('-z')) face(p, [c(x0, y0, z0), c(x1, y0, z0), c(x1, y1, z0), c(x0, y1, z0)], [0, 0, -1])
}

// ── Glass: four walls and a roof, all facing inwards ─────────────────────────
// The walls run down to the hall floor; outside, the podium hides their foot.
const G0 = 0.3
face(glass, [[-H, H, G0], [H, H, G0], [H, H, TOP], [-H, H, TOP]], [0, -1, 0])
face(glass, [[-H, -H, G0], [H, -H, G0], [H, -H, TOP], [-H, -H, TOP]], [0, 1, 0])
face(glass, [[H, -H, G0], [H, H, G0], [H, H, TOP], [H, -H, TOP]], [-1, 0, 0])
face(glass, [[-H, -H, G0], [-H, H, G0], [-H, H, TOP], [-H, -H, TOP]], [1, 0, 0])
face(glass, [[-H, -H, TOP], [H, -H, TOP], [H, H, TOP], [-H, H, TOP]], [0, 0, -1])
face(floor, [[-H, -H, G0], [H, -H, G0], [H, H, G0], [-H, H, G0]], [0, 0, 1])

// ── Frame ─────────────────────────────────────────────────────────────────────
// Corner posts, three mullions a face (four bays of 9 m) and three levels of
// transom (three bays of about 8.7 m above the terrace), then the roof grid on
// the same lines. Members straddle the glass plane, so they read from inside
// and out.
const CW = 0.65               // corner post half-width
const MW = 0.3                // mullion / beam half-width
const DIV = [-9, 0, 9]
const LEVELS = [DECK, DECK + (TOP - DECK) / 3, DECK + 2 * (TOP - DECK) / 3]
for (const sx of [-1, 1]) for (const sy of [-1, 1])
  box(frame, sx * H - CW, sx * H + CW, sy * H - CW, sy * H + CW, DECK - 0.5, TOP + 0.3, '-z')
for (const d of DIV) {
  for (const s of [-1, 1]) {
    box(frame, d - MW, d + MW, s * H - MW, s * H + MW, DECK - 0.5, TOP, '-z+z')
    box(frame, s * H - MW, s * H + MW, d - MW, d + MW, DECK - 0.5, TOP, '-z+z')
  }
}
for (const z of LEVELS) for (const s of [-1, 1]) {
  box(frame, -H + CW, H - CW, s * H - MW, s * H + MW, z - MW, z + MW, '+x-x')
  box(frame, s * H - MW, s * H + MW, -H + CW, H - CW, z - MW, z + MW, '+y-y')
}
// Roof: a deeper edge beam all round and one cross over the middle. A full
// grid on the walls' lines read as a cage over the sphere from above.
const EB = 0.8
for (const s of [-1, 1]) {
  box(frame, -H + CW, H - CW, s * H - EB / 2, s * H + EB / 2, TOP - 1.2, TOP + 0.3, '+x-x')
  box(frame, s * H - EB / 2, s * H + EB / 2, -H + CW, H - CW, TOP - 1.2, TOP + 0.3, '+y-y')
}
box(frame, -H + EB / 2, H - EB / 2, -MW, MW, TOP - 0.8, TOP + 0.2, '+x-x')
box(frame, -MW, MW, -H + EB / 2, H - EB / 2, TOP - 0.8, TOP + 0.2, '+y-y')

// ── The Hayden Sphere ────────────────────────────────────────────────────────
{
  const AROUND = 24, UP = 12
  const at = (i: number, j: number): V3 => {
    const t = (i / AROUND) * 2 * Math.PI, f = -Math.PI / 2 + (j / UP) * Math.PI
    return [R * Math.cos(f) * Math.cos(t), R * Math.cos(f) * Math.sin(t), CZ + R * Math.sin(f)]
  }
  const nAt = (p: V3): V3 => unit([p[0], p[1], p[2] - CZ])
  for (let i = 0; i < AROUND; i++) for (let j = 0; j < UP; j++) {
    const a = at(i, j), b = at(i + 1, j), c = at(i + 1, j + 1), d = at(i, j + 1)
    const out = nAt([(a[0] + c[0]) / 2, (a[1] + c[1]) / 2, (a[2] + c[2]) / 2])
    if (j === 0) face(sphere, [a, c, d], out, [nAt(a), nAt(c), nAt(d)])
    else if (j === UP - 1) face(sphere, [a, b, c], out, [nAt(a), nAt(b), nAt(c)])
    else face(sphere, [a, b, c, d], out, [nAt(a), nAt(b), nAt(c), nAt(d)])
  }
}
// Three legs, splayed outwards from a close-set foot on the hall floor to the
// lower hemisphere — the V pair is what the 81st Street side sees.
{
  const SEG = 8, LR = 0.75
  for (let k = 0; k < 3; k++) {
    const t = Math.PI / 2 + Math.PI / 3 + (k * 2 * Math.PI) / 3 // two legs to the front, one behind
    const dir: V3 = [Math.cos(t), Math.sin(t), 0]
    const rTop = 8.2, zTop = CZ - Math.sqrt(R * R - rTop * rTop) + 0.6
    const foot: V3 = [dir[0] * 2.6, dir[1] * 2.6, G0 + 0.6] // tube end clear of the floor
    const head: V3 = [dir[0] * rTop, dir[1] * rTop, zTop]
    const axis = unit(sub(head, foot))
    const u = unit(cross(axis, [0, 0, 1]))
    const v = cross(u, axis)
    for (let i = 0; i < SEG; i++) {
      const a0 = (i / SEG) * 2 * Math.PI, a1 = ((i + 1) / SEG) * 2 * Math.PI
      const n0: V3 = [u[0] * Math.cos(a0) + v[0] * Math.sin(a0), u[1] * Math.cos(a0) + v[1] * Math.sin(a0), u[2] * Math.cos(a0) + v[2] * Math.sin(a0)]
      const n1: V3 = [u[0] * Math.cos(a1) + v[0] * Math.sin(a1), u[1] * Math.cos(a1) + v[1] * Math.sin(a1), u[2] * Math.cos(a1) + v[2] * Math.sin(a1)]
      const p = (c: V3, n: V3): V3 => [c[0] + n[0] * LR, c[1] + n[1] * LR, c[2] + n[2] * LR]
      face(sphere, [p(foot, n0), p(foot, n1), p(head, n1), p(head, n0)], unit([n0[0] + n1[0], n0[1] + n1[1], n0[2] + n1[2]]), [n0, n1, n1, n0])
    }
  }
}

// ── Podium and terrace deck ──────────────────────────────────────────────────
// A granite block from the street to the terrace, with the cube standing in a
// square hole through it. Its outer top edge is chamfered.
const CH = 0.45
{
  // Outer walls: east, west and south plain; north (81st Street) carries the arch.
  const xs = [-SIDE, SIDE], ys = [BACK, FRONT]
  for (const s of [-1, 1]) {
    const x = s * SIDE
    face(granite, [[x, BACK, 0], [x, FRONT, 0], [x, FRONT, DECK - CH], [x, BACK, DECK - CH]], [s, 0, 0])
  }
  face(granite, [[-SIDE, BACK, 0], [SIDE, BACK, 0], [SIDE, BACK, DECK - CH], [-SIDE, BACK, DECK - CH]], [0, -1, 0])
  // Chamfer ring round the top.
  const ring = (inset: number, z: number): V3[] => [
    [xs[0] + inset, ys[0] + inset, z], [xs[1] - inset, ys[0] + inset, z], [xs[1] - inset, ys[1] - inset, z], [xs[0] + inset, ys[1] - inset, z],
  ]
  const lo = ring(0, DECK - CH), hi = ring(CH, DECK)
  const S = Math.SQRT1_2
  const outs: V3[] = [[0, -S, S], [S, 0, S], [0, S, S], [-S, 0, S]]
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    face(granite, [lo[i], lo[j], hi[j], hi[i]], outs[i])
  }
  // Paving: the inset top, less the cube's square.
  const x0 = -SIDE + CH, x1 = SIDE - CH, y0 = BACK + CH, y1 = FRONT - CH
  face(deck, [[x0, H, DECK], [x1, H, DECK], [x1, y1, DECK], [x0, y1, DECK]], [0, 0, 1])
  face(deck, [[x0, y0, DECK], [x1, y0, DECK], [x1, -H, DECK], [x0, -H, DECK]], [0, 0, 1])
  face(deck, [[x0, -H, DECK], [-H, -H, DECK], [-H, H, DECK], [x0, H, DECK]], [0, 0, 1])
  face(deck, [[H, -H, DECK], [x1, -H, DECK], [x1, H, DECK], [H, H, DECK]], [0, 0, 1])
}
// 81st Street face: a broad, low segmental arch springing from the pavement,
// recessed over the glazed entrance.
{
  const SPAN = 24, RISE = 4.3, DEPTH = 1.2, SEG = 12
  const RA = (SPAN * SPAN / 4 + RISE * RISE) / (2 * RISE), ZC = RISE - RA
  const half = Math.asin(SPAN / 2 / RA)
  const arc: [number, number][] = Array.from({ length: SEG + 1 }, (_, i) => {
    const a = -half + (i / SEG) * 2 * half
    return [RA * Math.sin(a), ZC + RA * Math.cos(a)]
  })
  const Z1 = DECK - CH, n: V3 = [0, 1, 0]
  face(granite, [[-SIDE, FRONT, 0], [-SPAN / 2, FRONT, 0], [-SPAN / 2, FRONT, Z1], [-SIDE, FRONT, Z1]], n)
  face(granite, [[SPAN / 2, FRONT, 0], [SIDE, FRONT, 0], [SIDE, FRONT, Z1], [SPAN / 2, FRONT, Z1]], n)
  for (let i = 0; i < SEG; i++) {
    const [xa, za] = arc[i], [xb, zb] = arc[i + 1]
    face(granite, [[xa, FRONT, za], [xb, FRONT, zb], [xb, FRONT, Z1], [xa, FRONT, Z1]], n)
    // Soffit, facing down and out of the recess.
    const nr = unit([-(xa + xb) / 2, 0, -((za + zb) / 2 - ZC)])
    face(granite, [[xa, FRONT, za], [xb, FRONT, zb], [xb, FRONT - DEPTH, zb], [xa, FRONT - DEPTH, za]], nr)
  }
  face(entrance, arc.map(([x, z]): V3 => [x, FRONT - DEPTH, z]), n)
}

// Colours from the daylight photos: the steel is painted white, the glass a
// pale blue-grey (its far side, seen through the near one, a little deeper),
// the sphere's aluminium skin near white, the podium a dark warm granite.
const parts = [
  { part: glass, material: { name: 'glass', color: 0xb3c3cb } },
  { part: frame, material: { name: 'frame', color: 0xe4e7e8 } },
  { part: sphere, material: { name: 'sphere', color: 0xeceae5 } },
  { part: granite, material: { name: 'granite', color: 0x6e6a65 } },
  { part: deck, material: { name: 'terrace', color: 0xbdb9b1 } },
  { part: floor, material: { name: 'hall-floor', color: 0x9ea3a6 } },
  { part: entrance, material: { name: 'entrance', color: 0x4b535b } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Rose Center for Earth and Space', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 27, elevation: 0, cube: 2 * H, cubeTop: TOP, sphereDiameter: 2 * R,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/rose-center.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
