/**
 * Parachute Jump, Coney Island — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/parachute-jump.ts
 *
 * Map frame: x east, y north, z up, metres; origin at the centre of the
 * pavilion on the plaza. Placed at bearing 351.5°, the Coney Island street
 * grid (West 16th Street), so one hexagon face looks down the boardwalk axis
 * at the beach, as in the view from Steeplechase Pier.
 *
 * The real tower is six-sided (NRHP: "a hexagonal base, upon which stands a
 * six-sided steel structure"; the 30 m balcony is a hexagon in every photo),
 * so the plan is a hexagon with its corners on the x axis. Dimensions come from
 * photo 01 (Steeplechase Pier, 2016, face-on), scaled so the pavilion fills the
 * OSM outline way/248474742 (a 17 m circle) — which also puts the spire tip at
 * OSM's 80 m and the 12 arms at ~14 m, matching the 45 ft quoted for them.
 *
 * The lattice is not drawn member by member. Six broad corner legs and a few
 * broad horizontal bands make the shaft read as open steel; the canopy is a
 * solid scalloped ring on twelve chunky trussed arms.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'

const steel = new Part()      // the red tower, arms and rings
const gold = new Part()       // yellow: pavilion band, shaft collar, walkways, spire
const blue = new Part()       // pavilion piers
const pavRed = new Part()     // pavilion wall panels
const roof = new Part()       // pavilion roofs
const plaza = new Part()

const TAU = Math.PI * 2
const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
const mul = (a: V3, k: number): V3 => [a[0] * k, a[1] * k, a[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))
const polar = (r: number, a: number, z: number): V3 => [r * Math.cos(a), r * Math.sin(a), z]
const lerp = (a: number, b: number, t: number) => a + (b - a) * t
const HEX = Array.from({ length: 6 }, (_, k) => k * TAU / 6)

/** A flat quad facing `toward` whatever order its corners came in. */
function face(p: Part, q: V3[], toward?: V3) {
  const n = cross(sub(q[1], q[0]), sub(q[2], q[0]))
  const flip = toward && dot(n, toward) < 0
  const [a, b, c, d] = !flip ? q : q.length === 3 ? [q[0], q[2], q[1]] : [q[0], q[3], q[2], q[1]]
  p.tri(a, b, c)
  if (d) p.tri(a, c, d)
}
function fan(p: Part, ring: V3[], n: V3) {
  for (let i = 1; i < ring.length - 1; i++) face(p, [ring[0], ring[i], ring[i + 1]], n)
}

/**
 * A chamfered rectangular beam swept along a path. `w` is the width across
 * the path (along side), `h` its depth along `hint` (made perpendicular to
 * the path). Corners are mitred, so a hexagonal band keeps its width round a
 * corner. The eight-sided section gives every edge a ~0.2 m bevel facet that
 * catches the light.
 */
function beam(p: Part, path: V3[], w: number | number[], h: number | number[], hint: (i: number) => V3,
  closed = false, caps = true, bevel = true) {
  const n = path.length
  const W = (i: number) => (typeof w === 'number' ? w : w[i]) / 2
  const H = (i: number) => (typeof h === 'number' ? h : h[i]) / 2
  const seg = (i: number) => unit(sub(path[(i + 1) % n], path[i]))
  const normals: V3[][] = []
  const rings = path.map((pt, i) => {
    const prev = closed || i > 0 ? seg((i - 1 + n) % n) : seg(0)
    const next = closed || i < n - 1 ? seg(i) : seg(n - 2)
    const t = unit(add(prev, next))
    const up0 = hint(i)
    const up = unit(sub(up0, mul(t, dot(up0, t))))
    const side = unit(cross(t, up))
    // Mitre: widen the section where the path turns.
    const miter = 1 / Math.max(0.5, dot(t, next))
    const ww = W(i) * miter, hh = H(i), c = Math.min(0.22, ww * 0.3, hh * 0.3)
    const sec: [number, number][] = bevel
      ? [[ww - c, -hh], [ww, -hh + c], [ww, hh - c], [ww - c, hh], [-ww + c, hh], [-ww, hh - c], [-ww, -hh + c], [-ww + c, -hh]]
      : [[ww, -hh], [ww, hh], [-ww, hh], [-ww, -hh]]
    // Smooth normals round the section: near-flat on the broad faces, turning
    // through the chamfers, so each edge reads as a soft rounded bevel.
    const sp = (v: number) => Math.sign(v) * Math.pow(Math.abs(v), 4)
    normals.push(sec.map(([s, u]) => bevel ? unit(add(mul(side, sp(s / ww)), mul(up, sp(u / hh))))
      : unit(add(mul(side, Math.sign(s)), mul(up, Math.sign(u))))))
    return sec.map(([s, u]) => add(pt, add(mul(side, s), mul(up, u))))
  })
  const last = closed ? n : n - 1
  for (let k = 0; k < last; k++) {
    const k1 = (k + 1) % n
    const a = rings[k], b = rings[k1], na = normals[k], nb = normals[k1]
    const m = a.length
    for (let j = 0; j < m; j++) {
      const l = (j + 1) % m
      const q = [a[j], a[l], b[l], b[j]], qn = [na[j], na[l], nb[l], nb[j]]
      if (!bevel) {
        // A plain box keeps flat faces.
        face(p, q, add(add(qn[0], qn[1]), add(qn[2], qn[3])))
        continue
      }
      const g = cross(sub(q[1], q[0]), sub(q[2], q[0]))
      const want = add(add(qn[0], qn[1]), add(qn[2], qn[3]))
      const o = dot(g, want) >= 0 ? [0, 1, 2, 3] : [0, 3, 2, 1]
      p.tri(q[o[0]], q[o[1]], q[o[2]], undefined, undefined, undefined, [qn[o[0]], qn[o[1]], qn[o[2]]])
      p.tri(q[o[0]], q[o[2]], q[o[3]], undefined, undefined, undefined, [qn[o[0]], qn[o[2]], qn[o[3]]])
    }
  }
  if (!closed && caps) {
    fan(p, rings[0], mul(seg(0), -1))
    fan(p, rings[n - 1], seg(n - 2))
  }
}

/** A hexagon of circumradius `R` at height `z`, corners on the x axis. */
const hexRing = (R: number, z: number) => HEX.map(a => polar(R, a, z))

// ---------------------------------------------------------------- plaza
// The concrete landing platform the pavilion stands in, kept small (12 m)
// so it stays clear of the service building 14 m to the south.
{
  const SEG = 24, R = 12, Z = 0.12
  const ring = (r: number, z: number) => Array.from({ length: SEG }, (_, i) => polar(r, i * TAU / SEG, z))
  const top = ring(R - 0.15, Z), foot = ring(R, 0)
  for (let i = 0; i < SEG; i++) {
    const j = (i + 1) % SEG
    face(plaza, [foot[i], foot[j], top[j], top[i]], polar(1, (i + 0.5) * TAU / SEG, 0.4))
  }
  fan(plaza, top, [0, 0, 1])
}

// ------------------------------------------------------------- pavilion
// Lower storey: battered walls, each face a red panel flanked by yellow, with
// a broad blue pier at every corner (photo 01). Then a projecting red eave, a
// narrower yellow upper band carrying red roundels, and a low roof.
const P0 = 8.5, P1 = 8.0, PZ = 5.8         // circumradius at foot and top of wall
const EAVE = [5.8, 6.15, 8.85] as const
const BAND = [6.15, 7.4, 7.1] as const
{
  const PIER = 0.11                          // fraction of a face each corner pier takes
  const at = (R: number, k: number, t: number, z: number): V3 => {
    const a = polar(R, HEX[k], z), b = polar(R, HEX[(k + 1) % 6], z)
    return [lerp(a[0], b[0], t), lerp(a[1], b[1], t), z]
  }
  for (let k = 0; k < 6; k++) {
    const out = polar(1, HEX[k] + TAU / 12, 0.06)
    const strip = (part: Part, t0: number, t1: number) =>
      face(part, [at(P0, k, t0, 0), at(P0, k, t1, 0), at(P1, k, t1, PZ), at(P1, k, t0, PZ)], out)
    strip(gold, PIER, 0.33); strip(pavRed, 0.33, 0.67); strip(gold, 0.67, 1 - PIER)
    // The corner pier: a chamfer face across the hexagon corner, in blue,
    // standing a touch proud so it reads as the fluted pier it is.
    const j = (k + 1) % 6, c = polar(1, HEX[j], 0)
    const p = (R: number, z: number, side: number): V3 => {
      const corner = polar(R + 0.15, HEX[j], z)
      const along = side < 0 ? unit(sub(polar(R, HEX[k], z), polar(R, HEX[j], z)))
        : unit(sub(polar(R, HEX[(j + 1) % 6], z), polar(R, HEX[j], z)))
      return add(corner, mul(along, PIER * R * 1.05))
    }
    face(blue, [p(P0, 0, -1), p(P0, 0, 1), p(P1, PZ, 1), p(P1, PZ, -1)], c)
    // Pier sides back to the panel plane.
    face(blue, [at(P0, k, 1 - PIER, 0), p(P0, 0, -1), p(P1, PZ, -1), at(P1, k, 1 - PIER, PZ)], out)
    face(blue, [p(P0, 0, 1), at(P0, j, PIER, 0), at(P1, j, PIER, PZ), p(P1, PZ, 1)], polar(1, HEX[j] + TAU / 12, 0))
  }
  // Eave: a red hexagonal slab with bevelled edges.
  const [e0, e1, eR] = EAVE
  beam(pavRed, hexRing(eR - 0.3, (e0 + e1) / 2), 0.6, e1 - e0, () => [0, 0, 1], true)
  fan(roof, hexRing(eR - 0.55, e1 - 0.01), [0, 0, 1])
  fan(pavRed, hexRing(eR - 0.05, e0 + 0.02), [0, 0, -1])
  // Upper band.
  const [b0, b1, bR] = BAND
  const lo = hexRing(bR, b0), hi = hexRing(bR, b1)
  for (let k = 0; k < 6; k++) {
    const j = (k + 1) % 6
    face(gold, [lo[k], lo[j], hi[j], hi[k]], polar(1, HEX[k] + TAU / 12, 0))
    // Three flush red roundels per face (they are red-and-white striped discs;
    // at map distance they read as red).
    const n = polar(1, HEX[k] + TAU / 12, 0)
    for (const t of [0.22, 0.5, 0.78]) {
      const c = add(add(mul(lo[k], 1 - t), mul(lo[j], t)), [0, 0, (b1 - b0) / 2])
      const centre = add(c, mul(n, 0.04))
      const along = unit(sub(lo[j], lo[k]))
      const ring = Array.from({ length: 8 }, (_, i) => {
        const a = i * TAU / 8
        return add(centre, add(mul(along, 0.5 * Math.cos(a)), [0, 0, 0.5 * Math.sin(a)]))
      })
      fan(pavRed, ring, n)
    }
  }
  fan(roof, hi, [0, 0, 1])
}

// ---------------------------------------------------------------- shaft
// Silhouette half-width measured off photo 01 (face-on, so this is the
// circumradius to the legs' outer faces), smoothed.
const PROFILE: [number, number][] = [
  [7.4, 6.65], [13.5, 5.8], [19.8, 4.95], [26, 4.25], [32.3, 3.62], [38.6, 3.12],
  [44.8, 2.74], [51, 2.45], [56, 2.28], [59.6, 2.24],
]
const outerR = (z: number) => {
  for (let i = 0; i < PROFILE.length - 1; i++) {
    const [z0, r0] = PROFILE[i], [z1, r1] = PROFILE[i + 1]
    if (z <= z1) return lerp(r0, r1, (z - z0) / (z1 - z0))
  }
  return PROFILE[PROFILE.length - 1][1]
}
const LEG = (z: number) => lerp(1.05, 0.72, (z - 7.4) / 52)      // leg section
const legR = (z: number) => outerR(z) - LEG(z) / 2                // leg centre line
const SHAFT_TOP = 59.6

// Six legs, each a chamfered square column following the curved taper.
for (const a of HEX) {
  const zs = PROFILE.map(([z]) => z)
  const path = zs.map(z => polar(legR(z), a, z))
  const w = zs.map(LEG)
  beam(steel, path, w, w, () => polar(1, a, 0))
}

// Broad horizontal bands where photo 01 shows the strongest ring girders and
// platforms. Each runs between the leg centre lines, sunk a little inside the
// legs' outer faces so the corners stay crisp.
const band = (part: Part, z: number, h: number, d: number, out = 0) => {
  const r = legR(z) + out
  beam(part, HEX.map(a => polar(r, a, z)), d, h, () => [0, 0, 1], true, false, false)
}
for (const z of [8.3, 14, 21.5, 38.5, 46, 52.5]) band(steel, z, 0.85, 0.5)
// The balcony at ~30 m (photos 01, 04): a deeper hexagonal platform that
// stands out past the legs.
band(steel, 30.3, 0.55, 1.5, 0.35)
// The yellow collar at the head of the shaft (photos 02, 04).
band(gold, 58.6, 1.6, 0.8, 0.05)

// --------------------------------------------------------------- canopy
// Twelve arms on the hexagon's corners and face centres. Each is a truss:
// a bottom chord flaring out of the shaft like a trumpet, a top chord level
// to r = 10 m and then falling to the rim, and a post between them at the
// inner ring. Profiles are photo 01's canopy silhouette.
const BOTTOM: [number, number][] = [[1.9, 59.0], [2.9, 62.0], [5.5, 63.9], [10, 65.0], [15.6, 65.6]]
const TOP: [number, number][] = [[0.9, 71.8], [10, 71.8], [13, 69.8], [15.6, 68.1]]
const RIM = 15.6
for (let k = 0; k < 12; k++) {
  const a = k * TAU / 12
  const radial = polar(1, a, 0)
  beam(steel, BOTTOM.map(([r, z]) => polar(r, a, z)), BOTTOM.map((_, i) => lerp(0.95, 0.75, i / 4)), 0.85,
    () => [0, 0, 1], false, false)
  beam(steel, TOP.map(([r, z]) => polar(r, a, z)), 0.8, 0.75, () => [0, 0, 1], false, false)
  // The walkway along the top of each arm, picked out in yellow (photos 02, 04).
  beam(gold, TOP.slice(1).map(([r, z]) => polar(r, a, z + 0.42)), 0.55, 0.12, () => [0, 0, 1], false, false, false)
  // A post tying the chords under the inner top ring.
  beam(steel, [polar(10, a, 65.0), polar(10, a, 71.6)], 0.6, 0.6, () => radial, false, false)
}
// The inner ring at the edge of the flat top.
beam(steel, Array.from({ length: 12 }, (_, i) => polar(10, i * TAU / 12, 71.8)), 0.7, 0.7, () => [0, 0, 1], true)
// The rim: one solid ring, scalloped outward at each arm end where the real
// octagonal sub-frames hang, so the canopy keeps its flower outline (photo 06).
{
  const N = 60
  const path = Array.from({ length: N }, (_, i) => {
    const a = i * TAU / N
    return polar(RIM + 0.75 * Math.cos(12 * a), a, 66.5 - 0.3 * Math.cos(12 * a))
  })
  beam(steel, path, 1.2, 2.6, () => [0, 0, 1], true)
}

// Spire: a yellow pyramid base and mast to 80 m (photo 01).
{
  const base = (r: number, z: number) => Array.from({ length: 4 }, (_, i) => polar(r, i * TAU / 4 + TAU / 8, z))
  const b0 = base(1.1, 71.4), b1 = base(0.35, 74.6)
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    face(gold, [b0[i], b0[j], b1[j], b1[i]], polar(1, (i + 0.5) * TAU / 4 + TAU / 8, 0.3))
  }
  beam(gold, [[0, 0, 74.5], [0, 0, 80]], [0.42, 0.3], [0.42, 0.3], () => [1, 0, 0])
}

// Colours sampled from the photos, sunlit side. The tower is a crimson red
// (photo 04's sunlit legs #d4535e, photo 07's #b74351; set between them); the pavilion's red is
// deeper; the yellow is the pavilion band (#dfbc46 in photo 01); the blue
// piers are photo 01's shaded #1a4070 lifted to their sunlit value.
const parts = [
  { part: steel, material: { name: 'red-steel', color: 0xbd3a44, roughness: 0.7, doubleSided: true } },
  { part: gold, material: { name: 'yellow', color: 0xdfbc46, roughness: 0.7, doubleSided: true } },
  { part: pavRed, material: { name: 'pavilion-red', color: 0xa92e39, roughness: 0.8, doubleSided: true } },
  { part: blue, material: { name: 'pavilion-blue', color: 0x2a5b9e, roughness: 0.8, doubleSided: true } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  // The plaza is built but not written: the map draws the paving.
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Parachute Jump', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 351.5, elevation: 0, height: 80, canopyRadius: RIM + 1.35, shaftTop: SHAFT_TOP,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/parachute-jump.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
console.log(parts.map(({ part, material }) => `  ${material.name}: ${part.triangles}`).join('\n'))
