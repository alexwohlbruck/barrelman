/**
 * The Fillmore Charlotte and The Underground (Mill #2 of the AvidXchange
 * Music Factory) — procedural, CC0-1.0.
 * bun scripts/landmarks/fillmore-charlotte.ts
 *
 * Mill #2 of the John B. Ross and Company Mill is the west arm of the
 * complex, about 170 m long down Hamilton Street from NC Music Factory
 * Boulevard. The Charlotte-Mecklenburg Historic Landmarks Commission survey
 * puts The Fillmore's sign over the c.1946 section's west door, and the
 * Underground (2016, 750 people, in a former country bar) is a second venue
 * inside the same building with its own marquee on the west projection
 * (Mapillary, Hamilton Street). It is not a separate building, so it has no
 * model of its own.
 *
 * From south to north, as the ground falls ~7 m:
 * - the c.1920 mill: one storey on the boulevard, a raised basement further
 *   north, a low gable roof with a deep eave and segmental-arched windows;
 *   a flat c.1946 projection at its south-west corner;
 * - the north end of the c.1920 mill, two storeys, and west of it the
 *   one-storey c.1955 projections where The Underground's marquee is;
 * - the c.1946 section with The Fillmore's entrance (red doors under a
 *   marquee, a ramp, a row of tall steel windows) and the pale corrugated
 *   metal-clad volume that rises over its roof;
 * - the plain c.1960 hall at the north end, 125 × 160 ft, flat roofed.
 *
 * OSM: way/414800516 outlines both mills and the bridge between them; this
 * model covers its west arm and `music-factory-mill` the rest, so both are
 * placed at the same minzoom. Map frame: x east, y north, z up; bearing 0.
 */
import { Part, type V3 } from './mesh'
import { PALETTE } from './palette'
import { BRICK, DOORS, SILVER, plant, MILL2, MILL2_EAST, axes, build, centroid, clean, flatZ, gableZ, ground, inside, lowest, quad, solid, write, type Built, type Mats, type XY, type Zone } from './music-factory-mill'

export function buildFillmore(): Built {
  const wall = new Part(), roof = new Part(), eave = new Part(), win = new Part(), clad = new Part(), red = new Part()
  const m: Mats = { wall, roof, eave, win }
  const ax = axes(25.5), P = MILL2
  const zones: Zone[] = []
  // The two marquees, placed first so the window rows leave room for them.
  const FILLMORE_DOOR: [XY, XY, number] = [[-63.6, 98.1], [-57.2, 111.1], 0.5]
  const UNDERGROUND_DOOR: [XY, XY, number] = [[-103.2, 68.9], [-91.3, 94.6], 0.62]
  for (const [A, B, f, r] of [[...FILLMORE_DOOR, 2.6], [...UNDERGROUND_DOOR, 2.6]] as [XY, XY, number, number][])
    DOORS.push({ p: [A[0] + (B[0] - A[0]) * f, A[1] + (B[1] - A[1]) * f], r })
  // The c.1920 mill: low gable along the arm, deep eave, arched windows.
  const T0 = -86.9, T1 = -56.8, tc = (T0 + T1) / 2, ridge = 224.4, eaveZ = 222.4, k = (ridge - eaveZ) / ((T1 - T0) / 2)
  const mill = { rows: [[218.4, 220.9], [214.2, 216.4]] as [number, number][], pitch: 3.9 }
  // (The west half also takes the narrow jog north of the garage projection.)
  zones.push({ poly: ax.band(P, -21.5, 17.5, -1e4, tc), z: gableZ(ax, tc, ridge, k), eave: true, ...mill })
  zones.push({ poly: ax.band(P, -40, -21.5, T0, tc), z: gableZ(ax, tc, ridge, k), eave: true, ...mill })
  zones.push({ poly: ax.band(P, -40, 17.5, tc, 1e4), z: gableZ(ax, tc, ridge, k), eave: true, ...mill })
  // Its flat c.1946 garage projection at the south-west corner.
  zones.push({ poly: ax.band(P, -40, -21.5, -1e4, T0), z: flatZ(222.0), rows: [[218.2, 220.6]], pitch: 4.5, square: true })
  // The two-storey north end of the c.1920 mill.
  zones.push({ poly: ax.band(P, 17.5, 46.5, -92, 1e4), z: flatZ(223.6), rows: [[219.2, 221.6], [215.2, 217.6]], pitch: 4.0 })
  // The one-storey c.1955 projections on the west: The Underground.
  zones.push({ poly: ax.band(P, 17.5, 46.5, -1e4, -92), z: flatZ(219.8), rows: [[216.0, 218.2]], pitch: 7.5, winW: 2.2, square: true })
  // The c.1946 section: The Fillmore's entrance front.
  zones.push({ poly: ax.band(P, 46.5, 80), z: flatZ(221.2), rows: [[216.4, 219.0]], pitch: 4.2, winW: 2.2, square: true })
  // The c.1960 hall: blank brick.
  zones.push({ poly: ax.band(P, 80, 140), z: flatZ(220.2) })
  build(zones, m, [MILL2_EAST])
  plant(clad, [[-30, 120, 220.2, 25.5], [-20, 132, 220.2, 25.5], [-38, 140, 220.2, 25.5], [-60, 62, 223.6, 25.5], [-52, 50, 223.6, 25.5], [-78, 78, 219.8, 25.5]])

  // The metal-clad volume over the c.1946 section, seen above the parapets
  // from Hamilton Street and from the Fillmore's door.
  {
    const ring = clean([ax.at(52, -83), ax.at(76, -83), ax.at(76, -63), ax.at(52, -63)])
    solid(clad, ring, 220.8, 225.0, roof)
    solid(eave, clean([ax.at(51.8, -83.2), ax.at(76.2, -83.2), ax.at(76.2, -62.8), ax.at(51.8, -62.8)]), 225.0, 225.35, roof)
  }

  /** A door and marquee on wall A→B, centred at fraction f, w wide. */
  function marquee(A: XY, B: XY, f: number, w: number, sill: number, doorH: number) {
    const L = Math.hypot(B[0] - A[0], B[1] - A[1]), u: XY = [(B[0] - A[0]) / L, (B[1] - A[1]) / L]
    // Outward is whichever side of the wall is outside the building.
    const mid: XY = [(A[0] + B[0]) / 2, (A[1] + B[1]) / 2]
    const o: XY = inside(P, [mid[0] + u[1], mid[1] - u[0]]) ? [-u[1], u[0]] : [u[1], -u[0]]
    const at = (s: number, d: number, z: number): V3 => [A[0] + u[0] * s + o[0] * d, A[1] + u[1] * s + o[1] * d, z]
    const c = L * f, s0 = c - w / 2, s1 = c + w / 2, n: V3 = [o[0], o[1], 0]
    // Red double doors.
    quad(red, at(c - 2, 0.06, sill), at(c + 2, 0.06, sill), at(c + 2, 0.06, sill + doorH), at(c - 2, 0.06, sill + doorH), n)
    // The canopy: a slab out from the wall, and the sign band standing on it.
    const z0 = sill + doorH + 0.5, z1 = z0 + 0.45, D = 1.8
    const box = (d0: number, d1: number, a: number, b: number, za: number, zb: number, p: Part) => {
      quad(p, at(a, d1, za), at(b, d1, za), at(b, d1, zb), at(a, d1, zb), n)
      quad(p, at(a, d0, zb), at(b, d0, zb), at(b, d1, zb), at(a, d1, zb), [0, 0, 1])
      quad(p, at(a, d0, za), at(a, d1, za), at(b, d1, za), at(b, d0, za), [0, 0, -1])
      quad(p, at(a, d0, za), at(a, d0, zb), at(a, d1, zb), at(a, d1, za), [-u[0], -u[1], 0])
      quad(p, at(b, d0, za), at(b, d1, za), at(b, d1, zb), at(b, d0, zb), [u[0], u[1], 0])
    }
    box(0, D, s0, s1, z0, z1, roof)
    box(D - 0.5, D - 0.2, s0 + 0.2, s1 - 0.2, z1, z1 + 1.4, eave)
  }
  // The Fillmore's door, up its ramp, mid-way along the c.1946 front.
  marquee(...FILLMORE_DOOR, 6.0, ground([-60.4, 104.6]) + 1.2, 3.2)
  // The Underground's marquee on the west projection.
  marquee(...UNDERGROUND_DOOR, 5.0, ground([-96.8, 82.8]) + 0.2, 2.4)

  const anchor = centroid(MILL2), base = lowest(clean(MILL2))
  return {
    id: 'fillmore-charlotte', name: 'The Fillmore Charlotte', anchor, base, height: 225.35 - base,
    parts: [
      { part: wall, material: BRICK },
      { part: roof, material: PALETTE.roof },
      { part: eave, material: PALETTE.trim },
      { part: win, material: PALETTE.window },
      { part: clad, material: SILVER },
      // The Fillmore's red doors are its front: lit at night as the entrance.
      { part: red, material: { ...PALETTE.entrance, color: 0xc4473e } },
    ],
  }
}

if (import.meta.main) await write(buildFillmore())
