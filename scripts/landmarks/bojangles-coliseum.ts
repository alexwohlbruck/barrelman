/**
 * Bojangles Coliseum, Charlotte (1955, A. G. Odell Jr.) — procedural, CC0-1.0.
 * bun scripts/landmarks/bojangles-coliseum.ts
 *
 * Map frame: x east, y north, z up, metres; placed at bearing 0. The origin is
 * the centre of the circle fitted to the round part of the OSM outline
 * (way/323383462, r = 51.0 m), so the drum is centred on it.
 *
 * The building is its dome: a shallow steel spherical cap, 332 ft (101 m)
 * across, once the largest free-span dome in the world, rising to 112 ft
 * (34.1 m). Commons aerials (James Willamor, "Bojangles' Coliseum.jpg" and "Bojangles
 * Coliseum and The Park ... panoramio") put the tension ring at a little under
 * half that height and the cap's rise at about 0.37 of its radius, so:
 * - a cylindrical drum, r 50.2 m, to 14.8 m: a painted blue plinth, then the
 *   concourse glazing between a ring of concrete piers, then concrete fascia;
 * - the tension ring, a grey concrete band barely proud of the drum;
 * - the cap, from r 50.8 m at 15.7 m to the crown at 34.1 m, with the small
 *   vent ring at its top;
 * - the low entrance block that OSM draws as an annulus sector on the
 *   south-west side, out to r 62.5 m.
 */
import { Part, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

const domeP = new Part(), concrete = new Part(), blue = new Part()
const win = new Part(), roof = new Part(), door = new Part()

const unit = (v: V3): V3 => { const l = Math.hypot(...v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
/** A quad with per-corner normals (flat if omitted), counter-clockwise from its front. */
function quadN(p: Part, a: V3, b: V3, c: V3, d: V3, n?: V3[]) {
  p.tri(a, b, c, undefined, undefined, undefined, n && [n[0], n[1], n[2]])
  p.tri(a, c, d, undefined, undefined, undefined, n && [n[0], n[2], n[3]])
}
const rad = (deg: number) => (deg * Math.PI) / 180
/** A point at bearing `b` (degrees clockwise from north), radius `r`, height `z`. */
const P = (b: number, r: number, z: number): V3 => [r * Math.sin(rad(b)), r * Math.cos(rad(b)), z]
/** The outward horizontal normal at bearing `b`, optionally tilted up by `up`. */
const N = (b: number, up = 0): V3 => unit([Math.sin(rad(b)), Math.cos(rad(b)), up])

/**
 * A surface of revolution between bearings b0 and b1 (clockwise), through
 * profile points (r, z) with (out, up) normals, smooth around.
 */
function revolve(p: Part, b0: number, b1: number, seg: number, prof: [number, number][], nrm: [number, number][]) {
  for (let k = 0; k < seg; k++) {
    const a = b0 + ((b1 - b0) * k) / seg, b = b0 + ((b1 - b0) * (k + 1)) / seg
    for (let i = 0; i < prof.length - 1; i++) {
      const [r0, z0] = prof[i], [r1, z1] = prof[i + 1]
      const n0 = nrm[i], n1 = nrm[i + 1]
      const nn = (bb: number, n: [number, number]): V3 => unit([Math.sin(rad(bb)) * n[0], Math.cos(rad(bb)) * n[0], n[1]])
      // Bearings run clockwise from above, so b → a is counter-clockwise.
      quadN(p, P(b, r0, z0), P(a, r0, z0), P(a, r1, z1), P(b, r1, z1), [nn(b, n0), nn(a, n0), nn(a, n1), nn(b, n1)])
    }
  }
}

// ---------------------------------------------------------------------------
// Dimensions.

const R_WALL = 50.2          // drum face
const R_RIM = 51.0           // tension ring, the OSM circle
const Z_BLUE = 3.4           // top of the painted plinth
const Z_WIN0 = 3.9, Z_WIN1 = 9.6
const Z_WALL = 14.8          // underside of the ring
const Z_SPRING = 15.7        // where the cap springs from the ring
const R_SPRING = 50.8
const APEX = 34.1            // 112 ft
const RISE = APEX - Z_SPRING
const R_SPHERE = (R_SPRING ** 2 + RISE ** 2) / (2 * RISE)
const PIERS = 40
const VENT = 2.4             // radius of the vent ring at the crown
const FACETS = 3             // drum facets per bay, so the glazing follows the curve
const SEG = PIERS * FACETS

// The entrance block on the south-west, between these bearings (OSM).
const ANNEX0 = 203.5, ANNEX1 = 242.5, R_ANNEX = 62.3, Z_ANNEX = 7.2

// ---------------------------------------------------------------------------
// Drum: blue plinth, then concrete, with a glazing panel in every bay.

revolve(blue, 0, 360, SEG, [[R_WALL, 0], [R_WALL, Z_BLUE]], [[1, 0], [1, 0]])
revolve(concrete, 0, 360, SEG, [[R_WALL, Z_BLUE], [R_WALL, Z_WALL]], [[1, 0], [1, 0]])

const PIER_W = 0.95, PIER_D = 0.45, CH = 0.18
const bayDeg = 360 / PIERS
const inAnnex = (b: number) => b > ANNEX0 - 1 && b < ANNEX1 + 1
for (let k = 0; k < PIERS; k++) {
  const b0 = k * bayDeg, b1 = (k + 1) * bayDeg
  // Window panels, 4 cm proud of the drum, one facet per drum facet.
  const half = (PIER_W / 2 + 0.25) / R_WALL * (180 / Math.PI)
  const mid = (b0 + b1) / 2
  if (!inAnnex(mid)) {
    const r = R_WALL + 0.04
    const edges = Array.from({ length: FACETS + 1 }, (_, i) => b0 + (bayDeg * i) / FACETS)
    edges[0] += half; edges[FACETS] -= half
    for (let i = 0; i < FACETS; i++) {
      const a = edges[i], b = edges[i + 1]
      quadN(win, P(b, r, Z_WIN0), P(a, r, Z_WIN0), P(a, r, Z_WIN1), P(b, r, Z_WIN1), [N(b), N(a), N(a), N(b)])
    }
  }
  // A pier on the bay line: a bevelled concrete fin, full height of the drum.
  const c = b0, n = N(c)
  const t: V3 = [Math.cos(rad(c)), -Math.sin(rad(c)), 0] // clockwise tangent
  const at = (s: number, d: number, z: number): V3 => {
    const base = P(c, R_WALL - 0.05 + d, z)
    return [base[0] + t[0] * s, base[1] + t[1] * s, z]
  }
  const w = PIER_W / 2, D = PIER_D + 0.05
  const tn = (sgn: number): V3 => [t[0] * sgn, t[1] * sgn, 0]
  const prof: [number, number, V3][] = [
    [-w, 0, tn(-1)], [-w, D - CH, unit([n[0] - t[0], n[1] - t[1], 0])],
    [-w + CH, D, n], [w - CH, D, n], [w, D - CH, unit([n[0] + t[0], n[1] + t[1], 0])], [w, 0, tn(1)],
  ]
  for (let i = 0; i < prof.length - 1; i++) {
    const [s0, d0, n0] = prof[i], [s1, d1, n1] = prof[i + 1]
    const flat = i === 0 || i === prof.length - 2
    const nA = flat ? (i === 0 ? tn(-1) : tn(1)) : n0, nB = flat ? nA : n1
    quadN(concrete, at(s1, d1, 0), at(s0, d0, 0), at(s0, d0, Z_WALL), at(s1, d1, Z_WALL), [nB, nA, nA, nB])
  }
}

// ---------------------------------------------------------------------------
// The tension ring: a soffit, a short bevelled fascia, then the cap.

// The ring is grey concrete, so the cap's edge reads as the thin line it
// makes in the photos.
revolve(concrete, 0, 360, SEG, [
  [R_WALL, Z_WALL], [R_RIM - 0.25, Z_WALL], [R_RIM, Z_WALL + 0.25], [R_RIM, Z_SPRING - 0.3],
], [[0, -1], [0.7, -0.7], [1, 0], [1, 0]])
revolve(domeP, 0, 360, SEG, [[R_RIM, Z_SPRING - 0.3], [R_SPRING, Z_SPRING]], [[0.8, 0.6], [0.8, 0.6]])

// The cap: a spherical shell, rings closer together near the rim where it
// curves fastest.
{
  const zc = APEX - R_SPHERE
  const phi0 = Math.asin(R_SPRING / R_SPHERE)
  const RINGS = 11
  const prof: [number, number][] = [], nrm: [number, number][] = []
  for (let i = 0; i <= RINGS; i++) {
    const t = i / RINGS
    const phi = phi0 * (1 - t) ** 1.15
    if (phi * R_SPHERE < VENT && i < RINGS) continue
    const r = Math.max(R_SPHERE * Math.sin(phi), 0)
    prof.push([r, zc + R_SPHERE * Math.cos(phi)])
    nrm.push([Math.sin(phi), Math.cos(phi)])
    if (r < VENT) break
  }
  // Stop at the vent ring rather than closing at the crown.
  const last = prof.length - 1
  const phiV = Math.asin(VENT / R_SPHERE)
  prof[last] = [VENT, zc + R_SPHERE * Math.cos(phiV)]
  nrm[last] = [Math.sin(phiV), Math.cos(phiV)]
  revolve(domeP, 0, 360, 48, prof, nrm)

  // The vent ring at the crown: a low grey drum with a domed lid.
  const zV = prof[last][1]
  revolve(roof, 0, 360, 16, [[VENT, zV - 0.05], [VENT, zV + 0.5], [VENT - 0.35, zV + 0.75], [VENT * 0.45, zV + 0.95], [0, zV + 1.0]],
    [[1, 0], [1, 0], [0.5, 0.86], [0.1, 1], [0, 1]])
}

// ---------------------------------------------------------------------------
// The entrance block: an annulus sector off the drum, concrete with a glazed
// band and doors on its curved face, a flat roof behind a bevelled parapet.

{
  const seg = 8
  const zP = Z_ANNEX
  // Curved outer face.
  revolve(concrete, ANNEX0, ANNEX1, seg, [[R_ANNEX, 0], [R_ANNEX, zP - 0.4]], [[1, 0], [1, 0]])
  revolve(concrete, ANNEX0, ANNEX1, seg, [[R_ANNEX, zP - 0.4], [R_ANNEX - 0.4, zP]], [[0.7, 0.7], [0.7, 0.7]])
  // Glazing: a full-width band of panels, with doors in the middle three bays.
  const r = R_ANNEX + 0.04
  for (let k = 0; k < seg; k++) {
    const a = ANNEX0 + ((ANNEX1 - ANNEX0) * k) / seg, b = ANNEX0 + ((ANNEX1 - ANNEX0) * (k + 1)) / seg
    const gap = 0.45 / R_ANNEX * (180 / Math.PI)
    const part = k >= 3 && k <= 4 ? door : win
    const z0 = part === door ? 0.3 : 1.2
    quadN(part, P(b - gap, r, z0), P(a + gap, r, z0), P(a + gap, r, 5.6), P(b - gap, r, 5.6), [N(b), N(a), N(a), N(b)])
  }
  // Radial end walls, facing out of the sector.
  for (const [b, sgn] of [[ANNEX0, -1], [ANNEX1, 1]] as [number, number][]) {
    const tn: V3 = [Math.cos(rad(b)) * sgn, -Math.sin(rad(b)) * sgn, 0]
    const pts = [P(b, R_WALL, 0), P(b, R_ANNEX, 0), P(b, R_ANNEX, zP - 0.4), P(b, R_ANNEX - 0.4, zP), P(b, R_WALL, zP)]
    const [A, B, C, D, E] = sgn > 0 ? pts : [pts[1], pts[0], pts[4], pts[3], pts[2]]
    if (sgn > 0) { quadN(concrete, A, B, C, E, [tn, tn, tn, tn]); concrete.tri(C, D, E, undefined, undefined, undefined, [tn, tn, tn]) }
    else { quadN(concrete, A, B, C, E, [tn, tn, tn, tn]); concrete.tri(E, D, C, undefined, undefined, undefined, [tn, tn, tn]) }
  }
  // Roof, flat, between the drum and the parapet.
  for (let k = 0; k < seg; k++) {
    const a = ANNEX0 + ((ANNEX1 - ANNEX0) * k) / seg, b = ANNEX0 + ((ANNEX1 - ANNEX0) * (k + 1)) / seg
    quadN(roof, P(a, R_WALL, zP), P(b, R_WALL, zP), P(b, R_ANNEX - 0.4, zP), P(a, R_ANNEX - 0.4, zP))
  }
}

// ---------------------------------------------------------------------------

// Colours from the daylight Commons aerials and a Mapillary street view of the
// north side: a white cap and ring; light grey concrete piers and fascia; the
// plinth's painted blue, pulled to the palette's lightness; slate glazing.
const parts = [
  { part: domeP, material: finish('dome', 0xf1f2ef, 0.6) },
  { part: concrete, material: finish('concrete', 0xd6d5cf) },
  { part: blue, material: finish('coliseum-blue', 0x8db4c8) },
  { part: win, material: PALETTE.window },
  { part: roof, material: PALETTE.roof },
  { part: door, material: PALETTE.entrance },
]
const triangles = parts.reduce((s, { part }) => s + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join(', '))
if (triangles > 5000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Bojangles Coliseum', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at the ground anchor',
  bearing: 0, osm: 'way/323383462', diameter: 2 * R_RIM, height: APEX,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/bojangles-coliseum.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
