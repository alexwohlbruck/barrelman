/**
 * The Eiffel Tower, stylised.
 *
 *   bun scripts/landmarks/eiffel-tower.ts [out.glb]
 *
 * Writes `landmarks/models/eiffel-tower.glb` by default. The model is the
 * source of truth for its own asset: change the numbers here and regenerate,
 * rather than editing the GLB.
 *
 * Proportions follow the real tower — 125 m base, floors at 57.6, 115.7 and
 * 276 m, tip at 330 m — and the profile follows its near-exponential curve.
 * Detail does not: the ironwork is an alpha-masked lattice texture on a
 * handful of lofted faces rather than girders, so the whole tower is ~1.5k
 * triangles. The mask is what sells it from street level, and it carries into
 * a shadow pass that honours it, so the shadow is see-through too.
 *
 * Built with its faces on the axes; the catalog's `bearing` turns it to the
 * real tower's 44°.
 */
import { Part, lerp, square, writeGlb, encodePng, type V3 } from './mesh'

/** Half-width of the outer profile at height z — within a metre or two of the real floors. */
const halfWidth = (z: number) => 58.5 * Math.exp(-z / 91.3) + 4

const FLOOR_1 = 57.6
const FLOOR_2 = 115.7
const FLOOR_3 = 276
const TIP = 330

const lattice = new Part()
const platforms = new Part()
const crown = new Part()

// Legs: one lofted square section per corner, curving in and narrowing until
// they merge into the shaft at the second floor.
const LEG_STEPS = 10
for (const [sx, sy] of [[1, 1], [-1, 1], [-1, -1], [1, -1]]) {
  const rings: V3[][] = []
  for (let k = 0; k <= LEG_STEPS; k++) {
    const t = k / LEG_STEPS
    const z = FLOOR_2 * t
    const outer = halfWidth(z)
    const inner = outer - lerp(26, 9, Math.pow(t, 0.8))
    let ring: V3[] = [
      [inner * sx, inner * sy, z],
      [outer * sx, inner * sy, z],
      [outer * sx, outer * sy, z],
      [inner * sx, outer * sy, z],
    ]
    // Mirroring a quadrant flips its winding; put it back to counter-clockwise.
    if (sx * sy < 0) ring = ring.reverse()
    rings.push(ring)
  }
  lattice.loft(rings, 1)
  platforms.cap(rings[0], false)
}

// The shaft: one tapering square tube from the second floor to the third.
{
  const STEPS = 8
  const rings: V3[][] = []
  for (let k = 0; k <= STEPS; k++) {
    const z = lerp(FLOOR_2, FLOOR_3, k / STEPS)
    rings.push(square(halfWidth(z), z))
  }
  lattice.loft(rings, 1)
}

// The arches between the legs under the first floor — decorative on the real
// tower, and the single most recognisable line in it.
{
  const SEGMENTS = 14
  const APEX = 47
  const SPRING = 14
  for (let face = 0; face < 4; face++) {
    const rot = (face * Math.PI) / 2
    // s runs along the face, inset measures in from it.
    const place = (s: number, z: number, inset: number): V3 => {
      const d = halfWidth(z) - inset
      return [d * Math.cos(rot) - s * Math.sin(rot), d * Math.sin(rot) + s * Math.cos(rot), z]
    }
    const span = halfWidth(SPRING) - 20
    const sections: V3[][] = []
    for (let k = 0; k <= SEGMENTS; k++) {
      const a = Math.PI * (k / SEGMENTS)
      const s = -Math.cos(a) * span
      const z = SPRING + Math.sin(a) * (APEX - SPRING)
      sections.push([place(s, z - 1.3, 4.2), place(s, z - 1.3, 2.6), place(s, z + 1.3, 2.6), place(s, z + 1.3, 4.2)])
    }
    platforms.sweep(sections)
  }
}

// Decks. The first floor's frieze is the tower's widest solid band.
platforms.slab(FLOOR_1 - 1, FLOOR_1 + 6, halfWidth(FLOOR_1) + 1.5, 17)
platforms.slab(FLOOR_2 - 0.5, FLOOR_2 + 3.5, halfWidth(FLOOR_2) + 1.5, 8)
// The band where the shaft visibly steps, about two-thirds of the way up.
platforms.slab(188, 190, halfWidth(189) + 0.6, halfWidth(189) - 1.5)

// Third floor, lantern and antenna.
crown.slab(FLOOR_3, FLOOR_3 + 6, halfWidth(FLOOR_3) + 1.8)
crown.slab(FLOOR_3 + 6, FLOOR_3 + 16, 5.5)
crown.slab(FLOOR_3 + 16, 300, 3.2)
{
  const SIDES = 8
  const rings = ([[300, 1.3], [318, 0.8], [TIP, 0.25]] as const).map(([z, r]) =>
    Array.from({ length: SIDES }, (_, i): V3 => {
      const a = (i / SIDES) * Math.PI * 2
      return [Math.cos(a) * r, Math.sin(a) * r, z]
    }),
  )
  crown.loft(rings)
  crown.cap(rings[rings.length - 1], true)
}

/**
 * Rails down both edges, a tie top and bottom and an X brace per cell. The
 * strokes are thick on purpose: thin ones lose to the mip chain at map
 * distances and the tower dissolves into mist.
 */
function latticeMask(size = 128): Uint8Array {
  const px = new Uint8Array(size * size * 4)
  for (let y = 0; y < size; y++)
    for (let x = 0; x < size; x++) {
      const u = x / size, v = y / size
      const solid =
        u < 0.12 || u > 0.88 ||
        v < 0.07 || v > 0.93 ||
        Math.abs(u - v) < 0.06 || Math.abs(u - (1 - v)) < 0.06
      const o = (y * size + x) * 4
      px[o] = px[o + 1] = px[o + 2] = 255
      px[o + 3] = solid ? 255 : 0
    }
  return encodePng(size, size, px)
}

const glb = writeGlb(
  'Eiffel Tower',
  [
    {
      part: lattice,
      material: { name: 'ironwork', color: 0x8a6a50, doubleSided: true, mask: { png: latticeMask() } },
    },
    { part: platforms, material: { name: 'decks', color: 0x9c7b5e, doubleSided: true } },
    { part: crown, material: { name: 'crown', color: 0x7b5d45 } },
  ],
  { frame: 'Y up, -Z north, +X east, metres, origin at the anchor on the ground' },
)

const out = process.argv[2] ?? new URL('../../landmarks/models/eiffel-tower.glb', import.meta.url).pathname
await Bun.write(out, glb)
const triangles = lattice.triangles + platforms.triangles + crown.triangles
console.log(`${out}: ${triangles} triangles, ${(glb.length / 1024).toFixed(1)} KiB`)
