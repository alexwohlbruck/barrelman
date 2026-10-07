/**
 * Vest Station water towers, Beatties Ford Road at Patton Ave, Charlotte —
 * procedural, CC0-1.0.
 * bun scripts/landmarks/beatties-ford-water-tower.ts
 *
 * Map frame: x east, y north, z up, metres; origin on the ground midway
 * between the two towers' OSM outlines (way/869290547, a 19.7 m circle, and
 * way/869290548, 13.5 m), which stand 40 m apart on a north-south line at
 * the south end of the Vest Water Treatment Plant. Bearing 0.
 *
 * The plant on Beatties Ford Road is Vest Station (Charlotte Water Works,
 * 1924, a designated Charlotte-Mecklenburg historic landmark); the city's
 * Franklin plant is elsewhere, on Brookshire Boulevard. The landmark report
 * (Loken, 1990) singles out "its two towering water tanks".
 *
 * The south one, Charlotte Water's "Patton Avenue water tank", is the 1924
 * "elevated 1,000,000 gallon storage tank at the head of the distribution
 * system" (Historic Landmarks Commission survey). It is a tall cylinder with
 * a hemi-ellipsoidal bottom and a shallow conical roof with a rolled dark
 * eave and a ball finial, a walkway where cylinder meets bowl, eight battered
 * lattice columns, a wide riser, and rings of struts with crossed rods. A
 * 16 m tank, 12 m of cylinder and a 7.8 m bowl hold about 0.9 million gallons,
 * close to the published million. The heights come from the Levine Museum
 * photo, scaled by that 16 m: walkway at 24.4 m, roof apex at 40 m. It was
 * repainted pale grey with black steel around 2022 (Charlotte Water's 2022
 * lighting submittal, and a 2022 Mapillary frame from Brookshire Freeway).
 *
 * The north one is a smaller saucer-shaped tank on six short columns, 12.4 m
 * across; its date and capacity are not published, and its height (top
 * about 12 m) is scaled from the Levine photo.
 *
 * The plant building itself is not modelled: see the report that came with
 * this model.
 *
 * Chunky for the map (STYLE.md): columns 1 m square, rods 0.45 m; no ladders,
 * railings, lights or lettering. The black steel is pulled to a mid grey.
 *
 * Photos: Charlotte Water, Vest WTP tank lighting submittal to the Historic
 * Landmarks Commission (2022), photos of the existing tank; "An undated photo
 * of water towers at Vest Station", Charlotte Water Blog via Levine Museum
 * of the New South; Mapillary street frames (CC BY-SA 4.0), 2018 and 2022.
 */
import { Part, cross, sub, writeGlb, type V3 } from './mesh'
import { finish } from './palette'

const TAU = Math.PI * 2
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const unit = (a: V3): V3 => { const l = Math.hypot(...a) || 1; return [a[0] / l, a[1] / l, a[2] / l] }

/** A triangle whose winding agrees with its normals, whatever order it came in. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const f = cross(sub(b, a), sub(c, a))
  if (Math.hypot(...f) < 1e-9) return
  if (dot(f, add(add(n[0], n[1]), n[2])) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}

/**
 * Revolve a profile of [r, z] points about the z axis at (cx, cy). Each run
 * of the profile is smooth-shaded; a repeated point starts a new run, which
 * is how a crease (the eave, the walkway's edge) is made.
 */
function lathe(p: Part, profile: [number, number][], seg: number, cx = 0, cy = 0, a0 = 0) {
  const runs: [number, number][][] = [[]]
  for (const q of profile) {
    const run = runs.at(-1)!
    const last = run.at(-1)
    if (last && last[0] === q[0] && last[1] === q[1]) runs.push([q])
    else run.push(q)
  }
  for (const run of runs) {
    if (run.length < 2) continue
    // Normals in the (r, z) plane, averaged at interior points.
    const segN = run.slice(1).map((q, i) => {
      const dr = q[0] - run[i][0], dz = q[1] - run[i][1]
      const l = Math.hypot(dr, dz) || 1
      return [dz / l, -dr / l]
    })
    const nrm = run.map((_, i) => {
      const a = segN[Math.max(0, i - 1)], b = segN[Math.min(segN.length - 1, i)]
      const l = Math.hypot(a[0] + b[0], a[1] + b[1]) || 1
      return [(a[0] + b[0]) / l, (a[1] + b[1]) / l]
    })
    for (let k = 0; k < run.length - 1; k++) {
      for (let s = 0; s < seg; s++) {
        const t0 = a0 + (s / seg) * TAU, t1 = a0 + ((s + 1) / seg) * TAU
        const P = (i: number, t: number): V3 => [cx + run[i][0] * Math.cos(t), cy + run[i][0] * Math.sin(t), run[i][1]]
        const N = (i: number, t: number): V3 => [nrm[i][0] * Math.cos(t), nrm[i][0] * Math.sin(t), nrm[i][1]]
        tri(p, P(k, t0), P(k, t1), P(k + 1, t1), [N(k, t0), N(k, t1), N(k + 1, t1)])
        tri(p, P(k, t0), P(k + 1, t1), P(k + 1, t0), [N(k, t0), N(k + 1, t1), N(k + 1, t0)])
      }
    }
  }
}

/** Points along an ellipse quadrant from (r0, z0) round to the axis or the rim. */
function head(r: number, depth: number, zRim: number, up: boolean, steps: number): [number, number][] {
  const out: [number, number][] = []
  for (let i = 0; i <= steps; i++) {
    const a = (i / steps) * (Math.PI / 2)
    // up: from the rim (a=0) to the apex; down: from the apex to the rim.
    const t = up ? a : Math.PI / 2 - a
    out.push([r * Math.cos(t), zRim + (up ? 1 : -1) * depth * Math.sin(t)])
  }
  return out
}

/**
 * A square-section member from a to b, flat-shaded, half-width w. Its sides
 * face the radial and tangential directions of the tower centred at c, so columns read as
 * square from the street rather than twisting with their slope.
 */
function beam(p: Part, a: V3, b: V3, w: number, caps = false, c: [number, number] = [0, 0]) {
  const t = unit(sub(b, a))
  const mid: V3 = [(a[0] + b[0]) / 2 - c[0], (a[1] + b[1]) / 2 - c[1], 0]
  let u = Math.hypot(mid[0], mid[1]) > 0.01 ? unit(cross([0, 0, 1], mid)) : unit(cross(t, [1, 0, 0]))
  if (Math.abs(dot(u, t)) > 0.95) u = unit(cross(t, [0, 0, 1]))
  u = unit(sub(u, [t[0] * dot(u, t), t[1] * dot(u, t), t[2] * dot(u, t)]))
  const v = cross(t, u)
  const corner = (o: V3, i: number): V3 => {
    const su = i === 0 || i === 3 ? -w : w, sv = i < 2 ? -w : w
    return [o[0] + u[0] * su + v[0] * sv, o[1] + u[1] * su + v[1] * sv, o[2] + u[2] * su + v[2] * sv]
  }
  for (let i = 0; i < 4; i++) {
    const j = (i + 1) % 4
    const c = [corner(a, i), corner(a, j), corner(b, j), corner(b, i)]
    const ctr: V3 = [(c[0][0] + c[2][0]) / 2, (c[0][1] + c[2][1]) / 2, (c[0][2] + c[2][2]) / 2]
    const n = unit(sub(ctr, [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, (a[2] + b[2]) / 2]))
    tri(p, c[0], c[1], c[2], [n, n, n])
    tri(p, c[0], c[2], c[3], [n, n, n])
  }
  if (caps) for (const [o, s] of [[a, -1], [b, 1]] as [V3, number][]) {
    const n: V3 = [t[0] * s, t[1] * s, t[2] * s]
    const c = [0, 1, 2, 3].map((i) => corner(o, i))
    tri(p, c[0], c[1], c[2], [n, n, n])
    tri(p, c[0], c[2], c[3], [n, n, n])
  }
}


// ---------------------------------------------------------------- the towers

const tank = new Part()   // pale grey shells and roofs
const steel = new Part()  // columns, struts, rods, risers, walkways, eave

const SOUTH: [number, number] = [3.61, -19.95]
const NORTH: [number, number] = [-3.61, 19.95]

type Tower = {
  c: [number, number]
  legs: number
  rFoot: number          // column radius at the ground
  rTop: number           // and where it meets the tank
  zTop: number           // top of the columns
  tiers: number[]        // strut rings; crossed rods brace the panels between
  riser: number          // riser radius
  zRiser: number         // riser top, inside the tank
}

function frame(t: Tower) {
  const at = (i: number, z: number): V3 => {
    const a = (i / t.legs) * TAU + TAU / (t.legs * 2)
    const r = t.rFoot + (t.rTop - t.rFoot) * (z / t.zTop)
    return [t.c[0] + r * Math.cos(a), t.c[1] + r * Math.sin(a), z]
  }
  for (let i = 0; i < t.legs; i++) beam(steel, at(i, 0), at(i, t.zTop), 0.5, true, t.c)
  for (let k = 1; k < t.tiers.length; k++)
    for (let i = 0; i < t.legs; i++) beam(steel, at(i, t.tiers[k]), at((i + 1) % t.legs, t.tiers[k]), 0.35, false, t.c)
  for (let k = 0; k < t.tiers.length - 1; k++) {
    const z0 = t.tiers[k] + 0.4, z1 = t.tiers[k + 1] - 0.4
    for (let i = 0; i < t.legs; i++) {
      const j = (i + 1) % t.legs
      beam(steel, at(i, z0), at(j, z1), 0.22, false, t.c)
      beam(steel, at(j, z0), at(i, z1), 0.22, false, t.c)
    }
  }
  lathe(steel, [[t.riser, 0], [t.riser, t.zRiser]], 12, t.c[0], t.c[1])
}

/** A walkway: a flat ring with a deep fascia, which is how its railing reads. */
function walkway(c: [number, number], r0: number, r1: number, z: number, seg: number) {
  lathe(steel, [[r0, z - 0.25], [r1, z - 0.25], [r1, z - 0.25], [r1, z + 0.85], [r1, z + 0.85], [r0, z + 0.85]], seg, c[0], c[1])
}

// South: the 1924 million-gallon tank.
{
  const R = 8.0, Z_BOT = 16.6, Z_CYL = 24.4, Z_EAVE = 36.4, Z_APEX = 40.0, SEG = 20
  const bowl = head(R, Z_CYL - Z_BOT, Z_CYL, false, 6)
  lathe(tank, [...bowl, [R, Z_CYL], [R, Z_CYL], [R, Z_EAVE - 0.3]], SEG, ...SOUTH)
  // Roof: a shallow, slightly convex cone from the eave to the finial.
  lathe(tank, [[8.3, Z_EAVE + 0.2], [5.6, Z_EAVE + 1.45], [2.8, Z_EAVE + 2.75], [0.5, Z_APEX - 0.05], [0, Z_APEX]], SEG, ...SOUTH)
  // The rolled eave, dark like the steel.
  lathe(steel, [[R - 0.05, Z_EAVE - 0.5], [8.35, Z_EAVE - 0.4], [8.55, Z_EAVE - 0.1], [8.5, Z_EAVE + 0.15], [8.2, Z_EAVE + 0.3]], SEG, ...SOUTH)
  // Ball finial.
  lathe(tank, [[0, Z_APEX], [0.35, Z_APEX + 0.08], [0.5, Z_APEX + 0.4], [0.35, Z_APEX + 0.72], [0, Z_APEX + 0.8]], 10, ...SOUTH)
  walkway(SOUTH, R - 0.05, R + 0.8, Z_CYL, SEG)
  frame({ c: SOUTH, legs: 8, rFoot: 9.6, rTop: R + 0.05, zTop: Z_CYL + 0.3, tiers: [0, 6.4, 12.5, 18.6], riser: 1.1, zRiser: Z_BOT + 0.5 })
}

// North: the squat tank, a flattened saucer: a shallow lower head, a narrow
// band carrying the walkway, and a low dome with a finial.
{
  const R = 6.2, Z_BOT = 6.0, LOWER = 2.3, BAND = 1.1, UPPER = 2.7, SEG = 18
  const Z_EQ = Z_BOT + LOWER, Z_TOP = Z_EQ + BAND + UPPER
  const lower = head(R, LOWER, Z_EQ, false, 5)
  const upper = head(R, UPPER, Z_EQ + BAND, true, 6)
  lathe(tank, [...lower, [R, Z_EQ], [R, Z_EQ + BAND], [R, Z_EQ + BAND], ...upper], SEG, ...NORTH)
  lathe(tank, [[0.5, Z_TOP - 0.05], [0.5, Z_TOP + 0.4], [0.5, Z_TOP + 0.4], [0, Z_TOP + 0.65]], 10, ...NORTH)
  walkway(NORTH, R - 0.05, R + 0.6, Z_EQ + 0.15, SEG)
  frame({ c: NORTH, legs: 6, rFoot: 6.6, rTop: R + 0.05, zTop: Z_EQ + 0.3, tiers: [0, Z_BOT - 1.2], riser: 0.9, zRiser: Z_BOT + 0.4 })
}

const parts = [
  { part: tank, material: finish('tank-grey', 0xdcdfdc, 0.6) },
  { part: steel, material: finish('tower-steel', 0x6d7278, 0.7) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 2500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Vest Station water towers', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', height: 40.8, bearing: 0,
})
await Bun.write(new URL('../../landmarks/models/beatties-ford-water-tower.glb', import.meta.url), glb)
console.log(`beatties-ford-water-tower.glb: ${triangles} triangles, ${glb.length} bytes`)
