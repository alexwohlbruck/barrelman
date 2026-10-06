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
 *   flat marquee with a dark sign board, a ramp, a long row of tall steel
 *   windows in red-painted frames) and the pale corrugated metal-clad volume
 *   that rises over its roof;
 * - the plain c.1960 hall at the north end, 125 × 160 ft, flat roofed.
 *
 * OSM: way/414800516 outlines both mills and the bridge between them; this
 * model covers its west arm and `music-factory-mill` the rest, so both are
 * placed at the same minzoom. Map frame: x east, y north, z up; bearing 0.
 */
import { Part, type V3 } from './mesh'
import { PALETTE, finish } from './palette'
import { BRICK, DOORS, SILVER, plant, MILL2, MILL2_EAST, axes, build, centroid, clean, flatZ, gableZ, ground, inside, quad, solid, write, lowest, type Built, type Mats, type XY, type Zone } from './music-factory-mill'

export function buildFillmore(): Built {
  const wall = new Part(), roof = new Part(), win = new Part(), silver = new Part(), red = new Part(), doors = new Part()
  // Copings, eaves and flat decks share the cladding's pale metal, and the
  // sign boards take the roof grey, so the model stays at six materials with
  // the red frames and the lit doors.
  const m: Mats = { wall, roof, eave: silver, win, frame: red, deck: silver }
  const ax = axes(25.5), P = MILL2
  /** Which way a wall faces along Hamilton Street (west) or the courtyard (east). */
  const west = (o: XY) => o[0] * ax.r[0] + o[1] * ax.r[1] < -0.7, east = (o: XY) => o[0] * ax.r[0] + o[1] * ax.r[1] > 0.7, south = (o: XY) => o[0] * ax.u[0] + o[1] * ax.u[1] < -0.7
  const zones: Zone[] = []
  // The two entrances, placed first so the bays leave room for them.
  const FILLMORE_DOOR: [XY, XY, number] = [[-63.6, 98.1], [-57.2, 111.1], 0.5]
  const UNDERGROUND_DOOR: [XY, XY, number] = [[-103.2, 68.9], [-91.3, 94.6], 0.62]
  for (const [A, B, f, r] of [[...FILLMORE_DOOR, 2.3 + 0.6], [...UNDERGROUND_DOOR, 2.6]] as [XY, XY, number, number][])
    DOORS.push({ p: [A[0] + (B[0] - A[0]) * f, A[1] + (B[1] - A[1]) * f], r })
  // The c.1920 mill: low gable along the arm, deep eave, segmental-arched
  // windows between brick piers. Its lower storey on Hamilton Street is
  // mostly bricked up (the survey), and its south end entirely.
  const T0 = -86.9, T1 = -56.8, tc = (T0 + T1) / 2, ridge = 225.0, eaveZ = 222.4, k = (ridge - eaveZ) / ((T1 - T0) / 2)
  const mill = {
    rows: [[218.4, 221.2], [214.2, 216.6]] as [number, number][], pitch: 3.9, winW: 1.8, piers: true,
    infill: (o: XY, row: number, i: number) => south(o) || (west(o) && row === 1 && i % 4 !== 1),
  }
  // (The west half also takes the narrow jog north of the garage projection.)
  zones.push({ poly: ax.band(P, -21.5, 17.5, -1e4, tc), z: gableZ(ax, tc, ridge, k), eave: true, ...mill })
  zones.push({ poly: ax.band(P, -40, -21.5, T0, tc), z: gableZ(ax, tc, ridge, k), eave: true, ...mill })
  zones.push({ poly: ax.band(P, -40, 17.5, tc, 1e4), z: gableZ(ax, tc, ridge, k), eave: true, ...mill })
  // Its flat c.1946 garage projection at the south-west corner: garage and
  // steel doors, its south openings bricked up.
  zones.push({ poly: ax.band(P, -40, -21.5, -1e4, T0), z: flatZ(222.0), rows: [[218.2, 220.6]], pitch: 4.5, square: true, infill: (o) => !west(o) })
  // The two-storey north end of the c.1920 mill: nine bays of 1/1 windows.
  zones.push({ poly: ax.band(P, 17.5, 46.5, -92, 1e4), z: flatZ(223.6), rows: [[219.0, 221.8], [215.0, 217.6]], pitch: 4.0, winW: 1.7, piers: true })
  // The one-storey c.1955 projections on the west: The Underground. Largely
  // blank, a few steel windows.
  zones.push({ poly: ax.band(P, 17.5, 46.5, -1e4, -92), z: flatZ(219.8), rows: [[216.0, 218.2]], pitch: 7.5, winW: 2.2, square: true })
  // The c.1946 section: The Fillmore's front. A storey on a raised
  // basement, fifteen bays of tall 8-light steel windows in one continuous
  // run, their frames painted red on every face towards Hamilton Street,
  // the jogs included (2018 photo: the windows nearly as tall as the door
  // opening, the brick between them narrower than a window, their heads
  // level with the transom over the doors).
  zones.push({ poly: ax.band(P, 46.5, 80), z: flatZ(221.2), rows: [[215.0, 218.0], [211.0, 214.0]], pitch: 3.2, winW: 2.0, square: true, frame: (o) => !east(o) })
  // The c.1960 hall: blank brick, its roof dark rather than pale (2007 aerial).
  zones.push({ poly: ax.band(P, 80, 140), z: flatZ(220.2), roof })
  build(zones, m, [MILL2_EAST])
  plant(roof, [[-30, 120, 220.2, 25.5], [-20, 132, 220.2, 25.5], [-38, 140, 220.2, 25.5], [-60, 62, 223.6, 25.5], [-52, 50, 223.6, 25.5], [-78, 78, 219.8, 25.5]])

  // The pale metal-clad volume over the c.1946 section, seen above the
  // parapet from Hamilton Street and from the Fillmore's door.
  {
    const ring = clean([ax.at(52, -83), ax.at(76, -83), ax.at(76, -63), ax.at(52, -63)])
    solid(silver, ring, 220.5, 225.0, roof)
    solid(silver, clean([ax.at(51.8, -83.2), ax.at(76.2, -83.2), ax.at(76.2, -62.8), ax.at(51.8, -62.8)]), 225.0, 225.35, roof)
  }

  /**
   * An entrance on wall A→B at fraction f: doors `doorW` wide and `doorH`
   * tall from `sill`, a dark transom band over them, and a flat marquee
   * canopy `w` wide projecting `D` with a dark sign board on its face.
   */
  function marquee(A: XY, B: XY, f: number, w: number, sill: number, doorW: number, doorH: number, D: number, board: number) {
    const L = Math.hypot(B[0] - A[0], B[1] - A[1]), u: XY = [(B[0] - A[0]) / L, (B[1] - A[1]) / L]
    // Outward is whichever side of the wall is outside the building.
    const mid: XY = [(A[0] + B[0]) / 2, (A[1] + B[1]) / 2]
    const o: XY = inside(P, [mid[0] + u[1], mid[1] - u[0]]) ? [-u[1], u[0]] : [u[1], -u[0]]
    const at = (s: number, d: number, z: number): V3 => [A[0] + u[0] * s + o[0] * d, A[1] + u[1] * s + o[1] * d, z]
    const c = L * f, n: V3 = [o[0], o[1], 0]
    const panel = (p: Part, a: number, b: number, z0: number, z1: number) => quad(p, at(a, 0.06, z0), at(b, 0.06, z0), at(b, 0.06, z1), at(a, 0.06, z1), n)
    // Red doors, lit at night as the entrance, and the transom band over them.
    panel(doors, c - doorW / 2, c + doorW / 2, sill, sill + doorH)
    panel(roof, c - doorW / 2, c + doorW / 2, sill + doorH, sill + doorH + 0.9)
    const box = (d0: number, d1: number, a: number, b: number, za: number, zb: number, face: Part, rest: Part, top: Part) => {
      quad(face, at(a, d1, za), at(b, d1, za), at(b, d1, zb), at(a, d1, zb), n)
      quad(top, at(a, d0, zb), at(b, d0, zb), at(b, d1, zb), at(a, d1, zb), [0, 0, 1])
      quad(rest, at(a, d0, za), at(a, d1, za), at(b, d1, za), at(b, d0, za), [0, 0, -1])
      quad(rest, at(a, d0, za), at(a, d0, zb), at(a, d1, zb), at(a, d1, za), [-u[0], -u[1], 0])
      quad(rest, at(b, d0, za), at(b, d1, za), at(b, d1, zb), at(b, d0, zb), [u[0], u[1], 0])
    }
    // The canopy: one flat slab out from the wall, sitting on the transom,
    // its face the sign board (no lettering) and its soffit pale.
    const z0 = sill + doorH + 0.95
    box(0, D, c - w / 2, c + w / 2, z0, z0 + board, roof, silver, silver)
  }
  // The Fillmore's door, at the head of its ramp mid-way along the c.1946
  // front: a pair of double doors under the marquee (2018 photo).
  marquee(...FILLMORE_DOOR, 7.0, ground([-60.4, 104.6]) + 1.7, 4.6, 3.0, 2.4, 1.0)
  // The Underground's smaller marquee on the west projection.
  marquee(...UNDERGROUND_DOOR, 5.0, ground([-96.8, 82.8]) + 0.2, 2.4, 2.4, 1.6, 0.8)

  const anchor = centroid(MILL2), base = lowest(clean(MILL2))
  return {
    id: 'fillmore-charlotte', name: 'The Fillmore Charlotte', anchor, base, height: 225.35 - base,
    parts: [
      { part: wall, material: BRICK },
      { part: roof, material: PALETTE.roof },
      { part: win, material: PALETTE.window },
      { part: silver, material: SILVER },
      // The Fillmore's red window frames.
      { part: red, material: finish('fillmore-red', 0xc23a3f) },
      // Its red doors are its front: lit at night as the entrance.
      { part: doors, material: { ...PALETTE.entrance, color: 0xc4473e } },
    ],
  }
}

if (import.meta.main) await write(buildFillmore())
