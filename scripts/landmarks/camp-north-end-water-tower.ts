/**
 * Camp North End water tower, Statesville Ave, Charlotte — procedural, CC0-1.0.
 * bun scripts/landmarks/camp-north-end-water-tower.ts
 *
 * Map frame: x east, y north, z up, metres; origin on the ground at the
 * centre of the OSM outline (way/833631505, man_made=water_tower, historic,
 * height 35, a 9.9 m circle). Round, so bearing 0.
 *
 * The 1924 water tower of Albert Kahn's Ford Motor Company assembly plant,
 * later the Charlotte Army Missile Plant and now Camp North End, where it
 * stands beside the boiler house as the site's emblem. It is part of the
 * designated landmark (Charlotte-Mecklenburg Historic Landmarks Commission,
 * 2019) and held "more than 105,000 gallons" (Camp North End history).
 *
 * A classic four-post tank: a squat cylinder with a hemispherical bottom and
 * an almost flat conical roof, a walkway where cylinder meets bowl, four
 * battered columns, a slender riser with a collar under the bowl, and
 * crossed rods. 8.5 m across, 4.3 m of cylinder and a 4.2 m bowl hold about
 * 105,000 gallons; the top is OSM's 35 m. The proportions come from the 2018
 * landmark report photo. It is left unpainted and rusted, which is its
 * look; the rust is pulled to the palette's lightness rather than the dark
 * brown it photographs.
 *
 * Chunky for the map (STYLE.md); no ladder, railing or lettering.
 *
 * Photo: "Ford Motor Co. Boiler House and Water Tower, West Elevation,
 * Facing East", MacRostie Historic Advisors, May 2018, in the Historic
 * Landmarks Commission designation report for the Ford Motor Company
 * Assembly Plant.
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

const tank = new Part()   // rusted shell and roof
const steel = new Part()  // columns, struts, rods, riser, walkway

const R = 4.25, Z_TOP = 35, ROOF = 0.7
const Z_EAVE = Z_TOP - ROOF           // 34.3
const Z_CYL = Z_EAVE - 4.3            // 30.0: cylinder meets bowl, walkway
const Z_BOT = Z_CYL - 4.2             // 25.8: bottom of the bowl
const SEG = 16, LEGS = 4

const bowl = head(R, Z_CYL - Z_BOT, Z_CYL, false, 6)
lathe(tank, [...bowl, [R, Z_CYL], [R, Z_CYL], [R, Z_EAVE], [R, Z_EAVE], [R + 0.25, Z_EAVE], [R + 0.25, Z_EAVE], [R + 0.25, Z_EAVE + 0.15], [R + 0.25, Z_EAVE + 0.15], [0.35, Z_TOP - 0.05], [0, Z_TOP]], SEG)
// Vent cap at the apex.
lathe(tank, [[0.35, Z_TOP - 0.1], [0.35, Z_TOP + 0.35], [0.35, Z_TOP + 0.35], [0, Z_TOP + 0.55]], 8)

// Walkway at the bowl's rim.
lathe(steel, [[R - 0.05, Z_CYL - 0.2], [R + 0.7, Z_CYL - 0.2], [R + 0.7, Z_CYL - 0.2], [R + 0.7, Z_CYL + 0.75], [R + 0.7, Z_CYL + 0.75], [R - 0.05, Z_CYL + 0.75]], SEG)

// Riser with its collar under the bowl.
lathe(steel, [[0.45, 0], [0.45, Z_BOT - 2.6], [0.45, Z_BOT - 2.6], [0.95, Z_BOT - 2.5], [0.95, Z_BOT - 1.7], [0.95, Z_BOT - 1.7], [0.45, Z_BOT - 1.6], [0.45, Z_BOT + 0.4]], 10)

// Four columns, battered: 4.9 m from the centre at the ground (the OSM
// circle), under the walkway at the top.
const at = (i: number, z: number): V3 => {
  const a = (i / LEGS) * TAU + TAU / (LEGS * 2)
  const r = 4.85 + (R + 0.05 - 4.85) * (z / Z_CYL)
  return [r * Math.cos(a), r * Math.sin(a), z]
}
for (let i = 0; i < LEGS; i++) beam(steel, at(i, 0), at(i, Z_CYL + 0.2), 0.36, true)
const TIERS = [0, 8.6, 17.2, Z_BOT - 0.4]
for (let k = 1; k < TIERS.length; k++)
  for (let i = 0; i < LEGS; i++) beam(steel, at(i, TIERS[k]), at((i + 1) % LEGS, TIERS[k]), 0.28)
for (let k = 0; k < TIERS.length - 1; k++) {
  const z0 = TIERS[k] + 0.3, z1 = TIERS[k + 1] - 0.3
  for (let i = 0; i < LEGS; i++) {
    const j = (i + 1) % LEGS
    beam(steel, at(i, z0), at(j, z1), 0.17)
    beam(steel, at(j, z0), at(i, z1), 0.17)
  }
}

const parts = [
  { part: tank, material: finish('rust', 0xa26b50, 0.8) },
  { part: steel, material: finish('rust-dark', 0x8a6756, 0.8) },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
console.log(parts.map(({ part, material }) => `${material.name}: ${part.triangles}`).join('\n'))
if (triangles > 2500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Camp North End water tower', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres, origin at ground', height: Z_TOP + 0.55, bearing: 0,
})
await Bun.write(new URL('../../landmarks/models/camp-north-end-water-tower.glb', import.meta.url), glb)
console.log(`camp-north-end-water-tower.glb: ${triangles} triangles, ${glb.length} bytes`)
