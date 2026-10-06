/**
 * Ricochet, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-ricochet.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see the anchor), at
 * the lowest ground under it. Built with ./coaster-kit.ts.
 *
 * A Mack Wilde Maus, Compact Park model (RCDB 1478; 370 m of track, 14 m
 * tall, 45 km/h, 44 × 19 m). Single four-seat cars climb a chain lift, wind
 * through a stack of flat, unbanked hairpins across the top of the
 * structure, then drop and run three long straights of dips and hills, with
 * a hairpin at each end, each a level lower, back to the station.
 *
 * Plan: the OSM circuit (way/893956341, way/893956344, way/893956343,
 * way/650270624, way/893956342, way/886367052, in the direction of travel;
 * the spur way/893956939 is left out). OSM draws the lift and two lower
 * straights on almost the same line up the east side, and the two north
 * hairpins one over the other, so the levels are set to clear each other by
 * 3 m or more, as the photo from the Skytower shows them: the top level, a
 * long middle-level straight under it, then two wavy lower ones. (OSM's
 * layer tags on the lower ways don't follow that order and are ignored.)
 * Positions are metres along the polyline (362 m round) and % of it:
 *
 *   station                      s 0–10
 *   chain lift to 13.5 m         s 10–43 (3–12 %), up the east side
 *   top level, 7 hairpins        s 50–186 (14–51 %), easing down to 11.2 m
 *   middle level, north          s 198–223 (55–62 %), 8.7 m to 7.6 m
 *   north hairpin, 7.6 m         s 223–235 (62–65 %)
 *   dip and hill, south          s 245, s 253; south hairpin at 4.8 m, s 263–283
 *   dip and hill, north          s 292, s 300; north hairpin at 3 m, s 311–326
 *   brakes into the station      s 342–10 (94–3 %)
 *
 * The hairpins are drawn level, as Mack's are: `banks` holds the whole ride
 * at 0°. Losses are set so the cars top out at 44 km/h (published 45).
 *
 * Photos used for matching (Wikimedia Commons): "Ricochet (Full)"
 * (Coasterman1234, March 2007, CC BY-SA 3.0, from the Skytower; yellow track
 * then) and "Ricochet (Carowinds) 1" (Jeremy Thompson, June 2023, CC BY 2.0).
 *
 * Style: a narrow swept ribbon, green running surface on a deeper green spine
 * (the 2014 repaint), on blue columns; the station is its roof on posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), from the
// station's first node.
const CHAIN: [number, number][] = [
  [3.1, 124.3], [9.2, 121.5], [11.4, 120.4], [12.7, 120.7], [13.6, 121.6], [15.1, 123.9], [27.6, 151.9], [28.0, 153.7],
  [27.4, 154.9], [25.9, 155.6], [17.0, 159.9], [15.2, 160.2], [14.1, 159.3], [13.6, 157.5], [14.3, 156.2], [22.6, 151.9],
  [23.9, 150.8], [24.5, 149.4], [24.0, 147.9], [22.9, 146.7], [21.4, 147.0], [13.4, 151.0], [11.6, 151.5], [10.3, 151.2],
  [9.2, 149.9], [9.2, 148.5], [9.9, 147.1], [18.7, 142.6], [20.1, 141.6], [20.7, 140.7], [20.4, 139.1], [19.9, 138.0],
  [18.6, 137.5], [17.0, 138.0], [8.8, 142.2], [7.1, 142.5], [5.5, 142.0], [4.8, 140.6], [5.0, 138.9], [6.2, 137.2],
  [15.1, 133.3], [16.7, 132.2], [17.2, 130.5], [16.8, 128.9], [15.2, 127.8], [12.9, 128.8], [5.5, 132.9], [3.7, 133.3],
  [2.2, 132.2], [1.0, 130.6], [0.1, 127.5], [1.2, 124.3], [2.7, 122.4], [6.2, 121.0], [8.8, 121.1], [11.0, 123.3],
  [12.8, 125.5], [25.2, 152.3], [25.2, 153.7], [23.9, 155.4], [20.4, 157.0], [18.7, 157.4], [16.2, 155.9], [15.1, 153.6],
  [3.0, 131.0], [1.6, 126.2], [2.5, 125.1], [7.5, 122.9], [10.7, 121.8], [12.4, 122.1], [13.4, 122.9], [13.8, 124.5],
  [14.0, 126.1], [25.2, 150.6], [25.1, 152.3], [24.2, 153.6], [23.4, 154.5], [21.9, 155.6], [18.8, 156.9], [15.7, 157.0],
  [13.8, 155.4], [-0.3, 127.3], [0.2, 126.0], [1.1, 125.4],
]
// Terrain (Mapzen/AWS Terrarium, open data), 10 m cells from (-20, 100); rows
// south to north, columns west to east; metres above 191.7 m.
const GROUND = [
  [1.8, 1.8, 1.9, 1.9, 2.1, 1.9, 2.1, 2.3],
  [1.3, 1.6, 1.8, 1.6, 1.9, 1.8, 2.1, 2.1],
  [1.3, 1.3, 1.5, 1.6, 1.7, 2.0, 2.2, 2.1],
  [0.9, 1.4, 1.4, 1.5, 1.6, 1.9, 2.1, 2.0],
  [0.6, 1.4, 1.3, 1.5, 1.5, 1.8, 2.1, 1.9],
  [1.2, 1.4, 1.2, 1.5, 1.6, 1.7, 2.0, 1.9],
  [1.3, 0.8, 1.2, 1.4, 1.6, 1.7, 1.9, 2.0],
  [0.6, 1.0, 0.9, 1.1, 1.1, 1.3, 1.0, 1.1],
  [0.9, 1.3, 0.9, 1.1, 1.0, 0.8, 0.4, 0.0],
]
// The station (way/668700374).
const STATION: [number, number][] = [
  [11.1, 116.6], [9.7, 117.2], [0.7, 121.4], [-0.5, 121.9], [0.7, 124.5], [2.7, 123.5], [3.1, 124.3], [3.7, 125.7],
  [4.4, 125.3], [5.5, 124.8], [10.0, 122.8], [9.2, 121.5], [8.9, 120.5], [12.3, 119.0], [12.1, 118.6], [11.6, 117.5],
]

await buildCoaster({
  name: 'Ricochet',
  out: '../../landmarks/models/carowinds-ricochet.glb',
  chain: CHAIN,
  anchor: [14, 139],
  ground: { x0: -20, y0: 100, step: 10, rows: GROUND, base: 191.7 },
  // Heights above the local ground; s along the OSM polyline (362 m round).
  profile: [
    { name: 'station', s: 0, z: 1.2 },
    { name: 'station, out', s: 10, z: 1.2, r: 6 },
    { name: 'lift crest (45.9 ft)', s: 43, z: 13.5, r: 7 },
    { name: 'top level', s: 50, z: 13.3, r: 6 },
    { name: 'top level, south turn', s: 186, z: 11.2, r: 4 },
    { name: 'middle level, south end', s: 198, z: 8.7, r: 5 },
    { name: 'north hairpin, in', s: 223, z: 7.6, r: 6 },
    { name: 'north hairpin, out', s: 235, z: 7.6, r: 9 },
    { name: 'dip', s: 245, z: 4.2, r: 7 },
    { name: 'hill', s: 253, z: 5.6, r: 5 },
    { name: 'south hairpin, in', s: 263, z: 4.8, r: 5 },
    { name: 'south hairpin, out', s: 283, z: 4.8, r: 9 },
    { name: 'dip', s: 292, z: 1.5, r: 8 },
    { name: 'hill', s: 300, z: 3.4, r: 5 },
    { name: 'north hairpin, in', s: 311, z: 3.0, r: 5 },
    { name: 'north hairpin, out', s: 326, z: 3.0, r: 5 },
    { name: 'brakes', s: 342, z: 1.2, r: 6 },
  ],
  lift: [10, 44],
  brakes: [{ from: 342, to: 10, z: 1.2, head: 0.3 }],
  // Unbanked: the hairpins are flat.
  banks: [{ from: 0, to: 361, deg: 0, name: 'level throughout' }],
  losses: [0.012, 0.004],
  liftHead: 0.8,
  track: { W: 1.8, RAIL: 0.4, DEPTH: 0.8, SPINE: 0.7 },
  station: { ring: STATION, posts: [[1, 122], [10.5, 117.5], [4.5, 124.6], [11.5, 119.5]], roofZ: 4.5 },
  // Colours from the photos, pulled to the palette's lightness: green track,
  // its spine a shade deeper, blue columns.
  colours: { deck: ['ricochet-green', 0x5aa36a], spine: ['ricochet-green-2', 0x4a8f5b], supports: ['ricochet-blue', 0x5b86c4] },
  supportR: [0.45, 0.55, 0.6],
  meta: { height: 14, trackLength: 370 },
})
