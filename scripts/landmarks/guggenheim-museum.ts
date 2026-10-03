/**
 * Solomon R. Guggenheim Museum — original procedural geometry, CC0-1.0.
 * Run: bun scripts/landmarks/guggenheim-museum.ts
 * Map frame: x = v (into the block), y = u (uptown), z = metres above ground.
 * Catalog placement: 40.7829932, -73.958925; bearing 29°, elevation 0.
 * The OSM envelope is authoritative; individual masses are stylised estimates.
 */
import { Part, addGltfTriangles, writeGlb, type V3 } from './mesh'

const concrete = new Part()
const limestone = new Part()
const recess = new Part()
const glass = new Part()
const metal = new Part()
const roof = new Part()
const mortar = new Part()
const TAU = Math.PI * 2
const N = 64
const MAIN: [number, number] = [0.2, -10.8]

function smooth(target: Part, build: (p: Part) => void, crease = 32) {
  const p = new Part()
  build(p)
  addGltfTriangles(target, new Float32Array(p.pos),
    Uint32Array.from({ length: p.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

function ring(cx: number, cy: number, r: number, z: number, ry = r): V3[] {
  return Array.from({ length: N }, (_, i): V3 => {
    const a = i / N * TAU
    return [cx + r * Math.cos(a), cy + ry * Math.sin(a), z]
  })
}

function round(p: Part, cx: number, cy: number, levels: [number, number][], aspect = 1) {
  smooth(p, s => {
    const rings = levels.map(([z, r]) => ring(cx, cy, r, z, r * aspect))
    s.loft(rings)
    s.cap(rings[0], false)
    s.cap(rings[rings.length - 1], true)
  })
}

function box(p: Part, x0: number, y0: number, z0: number, x1: number, y1: number, z1: number) {
  const rings = [z0, z1].map((z): V3[] => [[x0, y0, z], [x1, y0, z], [x1, y1, z], [x0, y1, z]])
  p.loft(rings)
  p.cap(rings[0], false)
  p.cap(rings[1], true)
}

// Rear galleries, the avenue's long low fascia, and recessed entrance glazing.
box(concrete, 6.5, -28.1, 0, 21.9, 3.8, 7.4)
box(concrete, 6.5, 3.8, 0, 7.3, 32.9, 7.4)
box(concrete, -16.7, 0, 0, 10, 23.5, 1.1)
box(glass, -15.8, 0, 1.1, 5.8, 24, 4.5)
box(concrete, -17.1, -0.6, 4.5, 9.5, 25, 6.4)
box(roof, 6.8, -27.8, 7.4, 21.6, 3.7, 7.55)
for (let y = 1; y < 24; y += 3.4) box(metal, -15.88, y, 1.1, -15.7, y + 0.13, 4.5)

// The main bowl: recessed continuous shell with one uninterrupted rising ribbon.
// Unlike separate discs, the ribbon changes height around each revolution.
const zBase = 4.6, zRim = 26.4, pitch = 3.6
const radius = (z: number) => 12.1 + (z - zBase) / (zRim - zBase) * 4.55
round(concrete, ...MAIN, [[0, 11.9], [4.6, 12.1]])
round(recess, ...MAIN, [[zBase, radius(zBase)], [zRim, radius(zRim)]])
smooth(concrete, p => {
  const point = (a: number, z: number, offset: number): V3 =>
    [MAIN[0] + (radius(z) + offset) * Math.cos(a), MAIN[1] + (radius(z) + offset) * Math.sin(a), z]
  const end = (zRim - zBase) / pitch * TAU
  const steps = Math.ceil(end / TAU * N)
  for (let i = 0; i < steps; i++) {
    const a = i / steps * end, b = (i + 1) / steps * end
    const section = (t: number): V3[] => {
      const lo = zBase + t / TAU * pitch
      const hi = Math.min(zRim, lo + pitch - 0.46)
      // An overhanging lower lip casts the narrow ramp joint into shade.
      return [point(t, lo, 0), point(t, lo, 0.65), point(t, hi, 0.65), point(t, hi, 0)]
    }
    const u = section(a), v = section(b)
    for (let k = 0; k < 4; k++) {
      const j = (k + 1) % 4
      p.quad(u[k], v[k], v[j], u[j])
    }
    if (i === 0) p.quad(u[0], u[1], u[2], u[3])
    if (i === steps - 1) p.quad(v[3], v[2], v[1], v[0])
  }
})
round(concrete, ...MAIN, [[26.4, 17.3], [27.1, 17.3]])
round(roof, ...MAIN, [[27.1, 16.8], [27.16, 16.8]])

// Shallow glazed skylight, with the radial structure visible from above.
const dome: [number, number][] = [[27.17, 9.3], [27.5, 8.7], [28.05, 6.9], [28.45, 4], [28.65, 0]]
round(glass, ...MAIN, dome)
for (let i = 0; i < 12; i++) {
  const a = i / 12 * TAU
  const w = 0.09
  for (let j = 0; j < dome.length - 1; j++) {
    const [z0, r0] = dome[j], [z1, r1] = dome[j + 1]
    const v = (r: number, z: number, side: number): V3 =>
      [MAIN[0] + r * Math.cos(a) - side * w * Math.sin(a), MAIN[1] + r * Math.sin(a) + side * w * Math.cos(a), z + 0.045]
    metal.quad(v(r0, z0, -1), v(r0, z0, 1), v(r1, z1, 1), v(r1, z1, -1))
  }
}
round(metal, ...MAIN, [[28.65, 0.5], [28.73, 0.5]])

// The Monitor is a broad, lower drum, with a strong horizontal glazed belt.
const mx = -5.7, my = 21.3
round(concrete, mx, my, [[0, 9.6], [6.4, 9.6], [7, 10.8]])
round(glass, mx, my, [[7, 10.3], [9.2, 10.3]])
round(concrete, mx, my, [[9.2, 11], [11.8, 11.4], [12.3, 11.4]], 11.6 / 11.4)
round(roof, mx, my, [[12.3, 10.85], [12.4, 10.85]])
round(concrete, mx, my, [[12.4, 6.5], [13.25, 6.5]])
round(glass, mx, my, [[13.25, 5.9], [13.32, 5.9]])

// Slender circulation drum at the joint between the two Wright volumes.
round(concrete, 4.6, 7.8, [[0, 2.2], [27.1, 2.2]])

// Gwathmey Siegel annex: quiet stone slab, with modest punched openings.
box(limestone, 7.3, 3.8, 0, 21.89, 32.89, 41.1)
box(roof, 7.6, 4.1, 41.1, 21.6, 32.6, 41.16)
box(limestone, 7.3, 3.8, 41.1, 7.6, 32.9, 41.6)
box(limestone, 21.6, 3.8, 41.1, 21.9, 32.9, 41.6)
box(limestone, 7.6, 3.8, 41.1, 21.6, 4.1, 41.6)
box(limestone, 7.6, 32.6, 41.1, 21.6, 32.9, 41.6)
for (let floor = 0; floor < 9; floor++) {
  const z = 9.2 + floor * 3.45
  for (const y of [7.1, 11.9, 16.7, 21.5, 26.3, 30.3]) {
    if (y > 25) glass.quad([7.29, y, z], [7.29, y, z + 1.7], [7.29, y + 1.1, z + 1.7], [7.29, y + 1.1, z])
    // Window planes sit a centimetre proud of stone, inside the OSM envelope.
    // The east wall is inset below to leave room for that relief.
    glass.quad([21.9, y, z], [21.9, y + 1.1, z], [21.9, y + 1.1, z + 1.7], [21.9, y, z + 1.7])
  }
  for (const x of [10, 14.5, 19]) {
    glass.quad([x, 32.9, z], [x, 32.9, z + 1.7], [x + 1.1, 32.9, z + 1.7], [x + 1.1, 32.9, z])
  }
}

// Quiet limestone panel joints: no texture, and no competing window grid on
// the main avenue-facing wall. Relief stays within the measured envelope.
for (let z = 8; z < 41; z += 3.45)
  mortar.quad([7.295, 3.81, z], [7.295, 3.81, z + 0.04], [7.295, 32.88, z + 0.04], [7.295, 32.88, z])
for (let y = 8.65; y < 32; y += 4.85)
  mortar.quad([7.295, y, 7.5], [7.295, y, 41.1], [7.295, y + 0.035, 41.1], [7.295, y + 0.035, 7.5])

const parts = [
  { part: concrete, material: { name: 'concrete', color: 0xeeeae0 } },
  { part: limestone, material: { name: 'limestone', color: 0xcac7bd } },
  { part: recess, material: { name: 'recess', color: 0xaaa89e } },
  { part: glass, material: { name: 'glass', color: 0x839b9e, roughness: 0.38 } },
  { part: metal, material: { name: 'steel', color: 0xd0d4cb, doubleSided: true } },
  { part: roof, material: { name: 'roof', color: 0xd1cfc5 } },
  { part: mortar, material: { name: 'mortar', color: 0xbab8b0 } },
]
const triangles = parts.reduce((n, { part }) => n + part.triangles, 0)
if (triangles > 9000) throw new Error(`Triangle budget exceeded: ${triangles}`)
// Validate the authored geometry itself, not just the descriptive metadata.
const min = [Infinity, Infinity, Infinity], max = [-Infinity, -Infinity, -Infinity]
for (const { part } of parts) for (let i = 0; i < part.pos.length; i++) {
  if (!Number.isFinite(part.pos[i])) throw new Error('Non-finite vertex')
  min[i % 3] = Math.min(min[i % 3], part.pos[i])
  max[i % 3] = Math.max(max[i % 3], part.pos[i])
}
// glTF is (v, height, -u), so the north/south bounds swap signs.
const expected = [[-17.1, 0, -32.9], [21.9, 41.6, 28.1]]
for (let axis = 0; axis < 3; axis++)
  if (Math.abs(min[axis] - expected[0][axis]) > 1e-5 || Math.abs(max[axis] - expected[1][axis]) > 1e-5)
    throw new Error(`OSM envelope mismatch: ${JSON.stringify({ min, max })}`)
const glb = writeGlb('Solomon R. Guggenheim Museum', parts, {
  license: 'CC0-1.0', bearing: 29, elevation: 0,
  frame: 'Y up, -Z north, +X east, metres; origin at the anchor on the ground',
  footprint: { v: [-17.1, 21.9], u: [-28.1, 32.9] },
  note: 'Individual massing estimated within the supplied OSM envelope; spiral rotunda and Monitor below the 41.6 m annex.',
})
const out = new URL('../../landmarks/models/guggenheim-museum.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length / 1024).toFixed(1)} KiB)`)
