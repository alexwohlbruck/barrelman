/**
 * Elizabeth water tower, E 8th St at Pecan Ave, Charlotte — procedural, CC0-1.0.
 * bun scripts/landmarks/elizabeth-water-tower.ts
 *
 * Map frame: x east, y north, z up, metres; origin on the ground at the
 * centre of the OSM outline (way/870467714, man_made=water_tower, a 25.3 m
 * circle). Round, so bearing 0.
 *
 * One of the towers Charlotte put up in 1937 when it rebuilt its water
 * system, at 422 Pecan Avenue: "a huge silver orb perched on steel stilts"
 * (Charlotte-Mecklenburg Historic Landmarks Commission, The Elizabeth
 * Neighborhood). Today the orb is painted white and the steel is a dark navy.
 * It is a multi-column ("tin man") tank: an ellipsoidal tank whose heads are
 * joined by a short cylindrical band, with a walkway round that band, carried
 * on twelve plain columns that meet the shell at its equator, a wide central
 * riser, a ring of struts at mid-height and crossed rods between columns.
 *
 * No height or capacity is published. The tank's 25 m width is the OSM
 * outline (it matches the 26 m a 2022 Mapillary frame gives at 206 m away),
 * and the same frame puts the top about 2.0 tank widths above the street, so
 * the top is set at 50.5 m. The tank's own proportions (deep lower head,
 * shallower upper head, a short band between) come from the 2025 Commons
 * photo; at this size it holds about 1.5 million US gallons.
 *
 * Simplified for the map: slender 0.7 m columns, a 1.6 m riser, a strut ring at
 * mid-height and a light one under the tank, and no crossed rods, so the
 * white tank dominates as it does in the photos. No railings, ladders or
 * lettering. The navy steel is pulled to a light blue-grey slate so it is
 * not near-black on the map and does not outweigh the tank.
 *
 * Photos: "Elizabeth Water Tower at 8th St and Pecan St Early August 2025",
 * City Dweller 2, CC BY-SA 4.0, Wikimedia Commons; Mapillary street frames
 * (CC BY-SA 4.0) from Pecan Ave and E 8th St, 2023.
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
 * face the tower's radial and tangential directions, so columns read as
 * square from the street rather than twisting with their slope.
 */
function beam(p: Part, a: V3, b: V3, w: number, caps = false) {
  const t = unit(sub(b, a))
  const mid: V3 = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2, 0]
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

// ---------------------------------------------------------------- the tower

const tank = new Part()   // white shell
const steel = new Part()  // columns, struts, rods, riser, walkway

const R = 12.0            // tank radius; the OSM circle is 12.65 m with the walkway
const Z_BOT = 33.5        // underside of the lower head
const LOWER = 8.0         // lower head depth
const BAND = 2.5          // cylindrical band between the heads
const UPPER = 6.5         // upper head rise
const Z_EQ = Z_BOT + LOWER            // 41.5: bottom of the band, where the columns land
const Z_TOP = Z_EQ + BAND + UPPER     // 50.5
const SEG = 24
const LEGS = 12
const R_LEG_TOP = R - 0.1             // columns meet the shell at the band
const R_LEG_FOOT = R + 0.3            // and barely batter outwards

// Tank: lower head up to the band, the band, then the upper head; one smooth
// run, with a crease where the band meets each head.
const lower = head(R, LOWER, Z_EQ, false, 7)
const upper = head(R, UPPER, Z_EQ + BAND, true, 7)
lathe(tank, [...lower, [R, Z_EQ], [R, Z_EQ + BAND], [R, Z_EQ + BAND], ...upper], SEG)
// Finial: a small cap at the apex.
lathe(tank, [[0.9, Z_TOP - 0.05], [0.9, Z_TOP + 0.5], [0.9, Z_TOP + 0.5], [0.5, Z_TOP + 0.9], [0, Z_TOP + 1.0]], 12)

// Walkway round the top of the band: a flat ring with a deep fascia, which is
// how the railing reads from the street.
const W0 = R - 0.05, W1 = R + 0.65, ZW = Z_EQ + BAND - 0.2
lathe(steel, [[W0, ZW - 0.25], [W1, ZW - 0.25], [W1, ZW - 0.25], [W1, ZW + 0.9], [W1, ZW + 0.9], [W0, ZW + 0.9]], SEG)

// Central riser, 1.6 m across, from the ground into the lower head.
lathe(steel, [[0.8, 0], [0.8, Z_BOT + 0.6]], 12)

// Columns, 0.7 m square: slender, so the white tank dominates as it does in
// the photos.
const legAt = (i: number, z: number): V3 => {
  const t = (i / LEGS) * TAU + TAU / (LEGS * 2)
  const r = R_LEG_FOOT + (R_LEG_TOP - R_LEG_FOOT) * (z / Z_EQ)
  return [r * Math.cos(t), r * Math.sin(t), z]
}
for (let i = 0; i < LEGS; i++) beam(steel, legAt(i, 0), legAt(i, Z_EQ + 0.4), 0.35, true)

// Bracing kept sparse: one ring of struts at mid-height and a lighter one
// just under the tank. The real tower's crossed rods are thin enough to
// vanish at map distance; drawn at a readable size they made a dense cage
// that outweighed the tank, so they are left out.
const ring = (z: number, w: number) => {
  for (let i = 0; i < LEGS; i++) beam(steel, legAt(i, z), legAt((i + 1) % LEGS, z), w)
}
ring(Z_BOT / 2, 0.25)
ring(Z_BOT + 0.5, 0.18)

const parts = [
  { part: tank, material: finish('tank-white', 0xeceeea, 0.6) },
  { part: steel, material: finish('tower-steel', 0x7f93a8, 0.7) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 2500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Elizabeth water tower', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', height: Z_TOP + 1.0, bearing: 0,
})
await Bun.write(new URL('../../landmarks/models/elizabeth-water-tower.glb', import.meta.url), glb)
console.log(`elizabeth-water-tower.glb: ${triangles} triangles, ${glb.length} bytes`)
