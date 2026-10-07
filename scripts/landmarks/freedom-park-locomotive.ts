/**
 * Gainesville Midland No. 301, Freedom Park, Charlotte — procedural, CC0-1.0.
 * bun scripts/landmarks/freedom-park-locomotive.ts
 *
 * Map frame: x to the engine's right, y forward along the track (towards the
 * pilot), z up, metres. Placed at bearing 307°: the engine faces north-west,
 * parallel to the fenced gravel bed it stands in (the inner ring way/1199662693
 * of the plaza multipolygon) and the footway beside it. The anchor is the OSM
 * node (node/8558009126, historic=locomotive), which sits at the cab, so the
 * engine-and-tender is centred on it.
 *
 * A 2-8-0 Consolidation, Baldwin 1920, built as Charlotte Harbor & Northern
 * No. 72, later Seaboard No. 930 and Gainesville Midland No. 301, on display
 * here since 1959. Published figures: 54 in drivers, 16 ft driving wheelbase,
 * 24 ft 6 in engine wheelbase. The rest (boiler, cab, tender) uses the usual
 * proportions of a Baldwin road engine of that size, checked against photos:
 * about 21 m over pilot and tender, 3 m wide, 4.6 m to the stack.
 *
 * It is shown with its tender only; the photos (2012–2018) show no caboose.
 *
 * Kept toy-like: a round boiler and smokebox, stack, bell, two domes, a boxy
 * headlight, a cab with windows, eight driving wheels as discs with a side
 * rod, a wedge pilot, and a plain tender on two trucks. No piping, handrails
 * or lettering.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const body = new Part()    // black: boiler, cab, tender, frame
const steel = new Part()   // wheels and rods, a shade lighter so the discs read
const glass = new Part()   // cab windows
const brass = new Part()   // bell
const plate = new Part()   // number plate on the smokebox door
const lamp = new Part()    // headlight lens

const TAU = Math.PI * 2
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const unit = (a: V3): V3 => { const l = Math.hypot(...a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }

/** A triangle whose winding agrees with its normals, whatever order it came in. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const f = cross(sub(b, a), sub(c, a))
  if (Math.hypot(...f) < 1e-10) return
  if (dot(f, add(add(n[0], n[1]), n[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
/** A flat convex polygon facing `n`. */
function face(p: Part, pts: V3[], n: V3) {
  for (let i = 1; i < pts.length - 1; i++) tri(p, pts[0], pts[i], pts[i + 1], [n, n, n])
}

/**
 * A box with its edges chamfered by `b`: rings of chamfered rectangles at the
 * bottom bevel, the faces and the top bevel.
 */
function box(p: Part, x0: number, x1: number, y0: number, y1: number, z0: number, z1: number, b = 0.08, bottom = true) {
  const ring = (d: number, z: number): V3[] => {
    const c = b
    const X0 = x0 + d, X1 = x1 - d, Y0 = y0 + d, Y1 = y1 - d
    return [[X0 + c, Y0, z], [X1 - c, Y0, z], [X1, Y0 + c, z], [X1, Y1 - c, z],
      [X1 - c, Y1, z], [X0 + c, Y1, z], [X0, Y1 - c, z], [X0, Y0 + c, z]]
  }
  const rings = b > 0 ? [ring(b, z0), ring(0, z0 + b), ring(0, z1 - b), ring(b, z1)] : [ring(0, z0), ring(0, z1)]
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2, cz = (z0 + z1) / 2
  for (let k = 0; k < rings.length - 1; k++)
    for (let i = 0; i < 8; i++) {
      const j = (i + 1) % 8
      const q = [rings[k][i], rings[k][j], rings[k + 1][j], rings[k + 1][i]]
      const m: V3 = [(q[0][0] + q[2][0]) / 2 - cx, (q[0][1] + q[2][1]) / 2 - cy, (q[0][2] + q[2][2]) / 2 - cz]
      const n = unit(cross(sub(q[1], q[0]), sub(q[3], q[0])))
      const N: V3 = dot(n, m) >= 0 ? n : [-n[0], -n[1], -n[2]]
      face(p, q, N)
    }
  face(p, rings[rings.length - 1], [0, 0, 1])
  if (bottom) face(p, rings[0], [0, 0, -1])
}

/** A round section from a to b, radius r0 to r1, smooth along its side, flat caps. */
function cyl(p: Part, a: V3, b: V3, r0: number, r1: number, seg: number, caps: [boolean, boolean] = [true, true]) {
  const T = unit(sub(b, a))
  const ref: V3 = Math.abs(T[2]) > 0.9 ? [1, 0, 0] : [0, 0, 1]
  const U = unit(cross(T, ref)), W = unit(cross(T, U))
  const dir = (i: number): V3 => { const t = (i / seg) * TAU + Math.PI / seg; return unit(add(add([0, 0, 0], U, Math.cos(t)), W, Math.sin(t))) }
  const slope = (r0 - r1) / Math.hypot(...sub(b, a))
  for (let i = 0; i < seg; i++) {
    const d0 = dir(i), d1 = dir(i + 1)
    const n0 = unit(add(d0, T, slope)), n1 = unit(add(d1, T, slope))
    const p00 = add(a, d0, r0), p01 = add(a, d1, r0), p10 = add(b, d0, r1), p11 = add(b, d1, r1)
    tri(p, p00, p01, p11, [n0, n1, n1])
    tri(p, p00, p11, p10, [n0, n1, n0])
  }
  if (caps[0] && r0 > 0) face(p, Array.from({ length: seg }, (_, i) => add(a, dir(i), r0)), [-T[0], -T[1], -T[2]])
  if (caps[1] && r1 > 0) face(p, Array.from({ length: seg }, (_, i) => add(b, dir(i), r1)), T)
}

/** A convex profile in the x–z plane, extruded along y from y0 to y1. */
function prismY(p: Part, prof: [number, number][], y0: number, y1: number) {
  const cx = prof.reduce((s, q) => s + q[0], 0) / prof.length, cz = prof.reduce((s, q) => s + q[1], 0) / prof.length
  for (let i = 0; i < prof.length; i++) {
    const [ax, az] = prof[i], [bx, bz] = prof[(i + 1) % prof.length]
    let n: V3 = unit([bz - az, 0, -(bx - ax)])
    if (dot(n, [(ax + bx) / 2 - cx, 0, (az + bz) / 2 - cz]) < 0) n = [-n[0], 0, -n[2]]
    face(p, [[ax, y0, az], [bx, y0, bz], [bx, y1, bz], [ax, y1, az]], n)
  }
  face(p, prof.map(([x, z]) => [x, y0, z] as V3), [0, -1, 0])
  face(p, prof.map(([x, z]) => [x, y1, z] as V3), [0, 1, 0])
}

// --- Dimensions -----------------------------------------------------------
// u runs back from the pilot; the model's y is forward, centred on the node.
const MID = 10.6
const Y = (u: number) => MID - u
const BOILER_Z = 2.8

// --- Running gear -----------------------------------------------------------
const R_DRIVER = 0.69                // 54 in drivers
const DRIVERS = [4.29, 5.92, 7.55, 9.17].map(Y) // 16 ft driving wheelbase
const PONY = Y(1.7)                  // 24 ft 6 in to the rear driver
const GAUGE = 0.78                   // wheel centre-plane, either side
function wheel(y: number, r: number, x: number, t = 0.16, seg = 14) {
  cyl(steel, [x - t / 2, y, r], [x + t / 2, y, r], r, r, seg)
}
for (const s of [-1, 1]) {
  for (const y of DRIVERS) wheel(y, R_DRIVER, s * GAUGE)
  wheel(PONY, 0.4, s * 0.74, 0.14, 12)
  // Side rod coupling the four drivers, and the main rod back from the crosshead.
  box(steel, s * 0.9 - 0.06, s * 0.9 + 0.06, DRIVERS[3] - 0.25, DRIVERS[0] + 0.25, 0.5, 0.72, 0.03)
  const c: V3 = [s * 1.0, Y(2.9), 1.05], d: V3 = [s * 1.0, DRIVERS[2], 0.62]
  cyl(steel, c, d, 0.1, 0.1, 6)
}

// --- Frame, cylinders, pilot --------------------------------------------------
box(body, -0.55, 0.55, Y(12.4), Y(1.0), 0.75, 1.65, 0.05)
for (const s of [-1, 1]) {
  // Cylinder with its steam chest above, between pony truck and first driver.
  cyl(body, [s * 1.2, Y(3.1), 1.05], [s * 1.2, Y(1.8), 1.05], 0.44, 0.44, 12)
  box(body, s > 0 ? 0.75 : -1.5, s > 0 ? 1.5 : -0.75, Y(3.05), Y(1.85), 1.35, 1.95, 0.06)
  // Running board along the boiler.
  box(body, s > 0 ? 0.95 : -1.55, s > 0 ? 1.55 : -0.95, Y(9.6), Y(3.05), 1.72, 1.86, 0.03)
}
// Buffer beam across the front.
box(body, -1.45, 1.45, Y(1.15), Y(0.8), 0.7, 1.3, 0.05)
// The pilot: a wedge sloping from the buffer beam down to the rail, pointed a
// little in plan like a cowcatcher.
{
  const top = 0.72, back = Y(1.0), toe = Y(-0.15), bot = 0.08
  const pts = (x: number): V3[] => [[x, back, top], [x, back, bot], [x, toe - Math.abs(x) * 0.25, bot]]
  const L = pts(-1.3), R = pts(1.3), C = pts(0)
  for (const [A, B] of [[L, C], [C, R]] as [V3[], V3[]][]) {
    const nSlope = unit(cross(sub(B[2], A[2]), sub(A[0], A[2])))
    face(body, [A[0], A[2], B[2], B[0]], dot(nSlope, [0, 1, 1]) > 0 ? nSlope : [-nSlope[0], -nSlope[1], -nSlope[2]])
    face(body, [A[1], B[1], B[2], A[2]], [0, 0, -1])
    face(body, [A[0], B[0], B[1], A[1]], [0, -1, 0])
  }
  face(body, L, [-1, 0, 0])
  face(body, R, [1, 0, 0])
}

// --- Boiler -------------------------------------------------------------------
// Smokebox, a touch larger than the boiler, with a door disc and number plate.
cyl(body, [0, Y(2.95), BOILER_Z], [0, Y(1.15), BOILER_Z], 0.98, 0.98, 16)
cyl(body, [0, Y(1.15), BOILER_Z], [0, Y(1.05), BOILER_Z], 0.74, 0.68, 16, [false, true])
cyl(plate, [0, Y(1.05), BOILER_Z + 0.05], [0, Y(1.02), BOILER_Z + 0.05], 0.2, 0.2, 12, [false, true])
// Boiler barrel, swelling slightly towards the firebox (a wagon-top).
cyl(body, [0, Y(9.2), BOILER_Z + 0.06], [0, Y(5.6), BOILER_Z + 0.04], 0.96, 0.92, 16, [false, false])
cyl(body, [0, Y(5.6), BOILER_Z + 0.04], [0, Y(2.95), BOILER_Z], 0.92, 0.88, 16, [false, false])
// Firebox below and behind it, mostly inside the cab.
box(body, -1.05, 1.05, Y(10.2), Y(8.6), 1.5, 3.3, 0.12)

// Stack, bell, the two domes and the headlight along the top.
const TOP = BOILER_Z + 0.88
cyl(body, [0, Y(2.0), TOP - 0.25], [0, Y(2.0), 4.62], 0.3, 0.34, 12, [false, false])
cyl(body, [0, Y(2.0), 4.62], [0, Y(2.0), 4.78], 0.4, 0.38, 12)
cyl(brass, [0, Y(3.75), TOP - 0.1], [0, Y(3.75), TOP + 0.28], 0.06, 0.06, 6, [false, false])
cyl(brass, [0, Y(3.75), TOP + 0.22], [0, Y(3.75), TOP + 0.62], 0.26, 0.14, 12)
for (const [u, h] of [[5.0, 0.62], [6.45, 0.66]]) {
  cyl(body, [0, Y(u), TOP - 0.2], [0, Y(u), TOP + h - 0.18], 0.44, 0.44, 14, [false, false])
  cyl(body, [0, Y(u), TOP + h - 0.18], [0, Y(u), TOP + h], 0.44, 0.24, 14)
}
box(body, -0.34, 0.34, Y(1.75), Y(1.05), TOP - 0.05, TOP + 0.6, 0.08)
face(lamp, [[-0.24, Y(1.03), TOP + 0.08], [0.24, Y(1.03), TOP + 0.08], [0.24, Y(1.03), TOP + 0.5], [-0.24, Y(1.03), TOP + 0.5]], [0, 1, 0])

// --- Cab ----------------------------------------------------------------------
const CAB0 = Y(12.3), CAB1 = Y(9.6), CABW = 1.5
box(body, -CABW, CABW, CAB0, CAB1, 1.55, 4.05, 0.06)
// Roof: a shallow arched sheet overhanging front and back.
{
  const n = 8
  const arc = Array.from({ length: n + 1 }, (_, i) => {
    const t = Math.PI * (1 - i / n)
    return [1.64 * Math.cos(t), 4.12 + 0.3 * Math.sin(t)] as [number, number]
  })
  prismY(body, [[1.64, 3.98], [-1.64, 3.98], ...arc], CAB0 - 0.3, CAB1 + 0.2)
}
// Windows: two on each side, and one either side of the boiler on the front.
for (const s of [-1, 1]) {
  const x = s * (CABW + 0.012)
  for (const [ya, yb] of [[CAB1 - 0.25, CAB1 - 1.15], [CAB1 - 1.4, CAB0 + 0.35]])
    face(glass, [[x, ya, 2.75], [x, yb, 2.75], [x, yb, 3.7], [x, ya, 3.7]], [s, 0, 0])
  face(glass, [[s * 0.95, CAB1 + 0.012, 3.15], [s * 1.35, CAB1 + 0.012, 3.15], [s * 1.35, CAB1 + 0.012, 3.75], [s * 0.95, CAB1 + 0.012, 3.75]], [0, 1, 0])
}

// --- Tender -------------------------------------------------------------------
const T0 = Y(21.2), T1 = Y(12.7)
box(body, -1.32, 1.32, T0 + 0.2, T1 - 0.2, 0.95, 1.4, 0.05)
box(body, -1.5, 1.5, T0, T1, 1.35, 3.55, 0.1)
// The coal space: a low raised coaming round the front of the top.
box(body, -1.5, 1.5, T1 - 2.8, T1, 3.5, 3.85, 0.08)
for (const ax of [T0 + 1.0, T0 + 2.6, T1 - 2.6, T1 - 1.0])
  for (const s of [-1, 1]) wheel(ax, 0.42, s * 0.74, 0.13, 12)
for (const c of [T0 + 1.8, T1 - 1.8])
  for (const s of [-1, 1]) box(steel, s > 0 ? 0.84 : -1.0, s > 0 ? 1.0 : -0.84, c - 1.25, c + 1.25, 0.28, 0.8, 0.05)

const parts = [
  // Black engine and tender. Kept a little off pure black, as the shared
  // palette never goes darker than this; the running gear one step lighter,
  // so the eight driving wheels read as discs against the body.
  { part: body, material: finish('locomotive-black', 0x3f4144, 0.6) },
  { part: steel, material: finish('locomotive-steel', 0x63676c, 0.55) },
  { part: glass, material: PALETTE.window },
  { part: brass, material: finish('brass', 0xc9a85a, 0.5) },
  { part: plate, material: PALETTE.trim },
  { part: lamp, material: PALETTE.entrance },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 3000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Gainesville Midland No. 301', parts.map(({ part, material }) => ({ part, material })), {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', length: 21.3, bearing: 307,
})
await Bun.write(new URL('../../landmarks/models/freedom-park-locomotive.glb', import.meta.url), glb)
console.log(`freedom-park-locomotive.glb: ${triangles} triangles, ${glb.length} bytes`)
