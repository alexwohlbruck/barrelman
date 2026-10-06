/**
 * The Flying Cobras, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-flying-cobras.ts   (REPORT=1 for the element table)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the ride's footprint (see the anchor), at
 * the lowest ground under it. Built with ./coaster-kit.ts as a shuttle.
 *
 * A Vekoma Boomerang (RCDB 4269; 285 m of track, two spikes of 35.5 m,
 * 76 km/h, a cobra roll and a vertical loop, 88 × 30 m). The train is pulled
 * backwards up the rear spike behind the station and let go, runs forward
 * through the station, the cobra roll and the loop, climbs the front spike,
 * and runs the whole course again backwards.
 *
 * Plan: the OSM track (way/241408477, way/888642924, way/888642927, an open
 * line from the rear spike's top to the front spike's). The rear spike and
 * the station sit on the north-east line, the loop and the front spike on the
 * south-west one, and the two spikes stand side by side at the south-east
 * end. OSM sketches the cobra roll and the loop as zigzags; here the chain
 * keeps both straights and joins them with two helper nodes, and the two
 * elements are drawn as inversions over that join:
 *
 *   rear spike top, 35.5 m     s 0
 *   station                    s 36–57 (25–39 %), 1.8 m up
 *   cobra roll, 21 m           s 57–89 (39–61 %), its hoods over OSM's two tips
 *   vertical loop, 18 m        s 90–106 (62–73 %), where OSM zigzags
 *   front spike top, 35.4 m    s 146
 *
 * Each spike is a straight ramp of 53° from the station level, as the photos
 * show (steep ramps, not vertical towers), running straight to its end. The cobra roll is drawn through
 * points: a half loop up to a hood 21 m up, half a corkscrew down to the
 * middle at 15 m, and the same mirrored across the line between the two
 * straights, its tips where OSM puts them. Its height and the loop's are not
 * published; they are the highest the energy carries the train over on both
 * runs at a safe speed (the run back is the slower), and in the photo from
 * the south-west the hoods stand a little above the loop, as here. The
 * train is let go with its middle 7 m below the spike's top (`lift` takes
 * in both tops), and the losses give 80 km/h at the station (published 76).
 *
 * Photos used for matching (Wikimedia Commons): "Flying Cobras 1" (Jeremy
 * Thompson, June 2023, CC BY 2.0) and "The Flying Cobras' blue track and
 * Copperhead Strike's copper track over the garden path" (ThrillZing, October
 * 2024, CC BY 4.0).
 *
 * Style: the kit's swept ribbon, blue running surface on a deeper blue spine,
 * on white supports. The colours are 2017–2025's; RCDB says the ride was
 * repainted for 2026, and no photo of the new scheme was found.
 */
import { buildCoaster } from './coaster-kit'

// The plan in the park frame (see coaster-kit), from the rear spike's top.
// Nodes 4 and 5 are helpers that keep the two straights straight into the
// cobra roll; the roll itself replaces the stretch between them.
const CHAIN: [number, number][] = [
  [55.5, -59.2], [32.6, -42.0], [26.3, -37.3], [10.2, -25.3], [5.3, -21.8],
  [-4.4, -39.4], [1.1, -41.7], [14.7, -46.9], [26.7, -51.5], [53.9, -63.1],
]
// Terrain (Mapzen/AWS Terrarium, open data), 10 m cells from (-40, -90); rows
// south to north, columns west to east; metres above 193.3 m.
const GROUND = [
  [1.6, 0.1, 0.0, 1.3, 1.9, 1.9, 2.0, 2.1, 2.3, 2.4, 2.4, 2.5, 2.3],
  [1.4, 1.1, 1.2, 1.6, 1.8, 1.8, 2.2, 2.2, 2.3, 2.4, 2.3, 2.1, 2.1],
  [2.7, 2.5, 2.2, 1.6, 1.9, 1.7, 2.0, 2.1, 2.2, 2.2, 2.2, 2.0, 2.1],
  [2.0, 1.9, 1.5, 1.5, 1.6, 1.7, 1.8, 1.9, 1.8, 1.9, 1.8, 1.8, 1.6],
  [1.4, 1.6, 1.5, 1.6, 1.9, 1.8, 1.7, 1.8, 1.7, 1.7, 1.8, 1.3, 1.6],
  [1.5, 2.0, 1.7, 1.7, 1.7, 1.8, 1.6, 1.8, 1.8, 1.8, 1.7, 1.6, 1.5],
  [1.9, 1.7, 1.9, 1.7, 1.8, 1.6, 1.6, 1.7, 1.7, 1.9, 1.4, 1.5, 1.5],
  [1.6, 1.6, 1.8, 1.8, 1.8, 1.6, 1.8, 2.0, 0.7, 0.4, 1.6, 1.5, 1.8],
  [1.8, 1.6, 1.7, 2.0, 2.1, 2.3, 1.8, 1.0, 0.1, 0.7, 0.5, 1.7, 2.0],
  [1.6, 1.7, 1.8, 1.6, 3.1, 4.2, 4.0, 0.7, 1.3, 0.5, 1.1, 1.8, 2.0],
  [1.8, 1.4, 1.2, 1.2, 2.5, 4.3, 4.6, 1.7, 0.2, 1.4, 1.7, 1.8, 2.0],
]
// The station canopy (way/241408487).
const STATION: [number, number][] = [
  [7.9, -28.3], [10.2, -25.3], [11.6, -23.4], [12.3, -22.5], [14.4, -24.0], [13.6, -24.9], [19.5, -29.2],
  [21.6, -30.7], [27.6, -35.3], [26.3, -37.3], [23.6, -40.3], [16.7, -35.1], [15.2, -33.9],
]
// The cobra roll, [x, y, height above the ground]: up the first half loop to
// its tip and hood, half a corkscrew down to the middle, then the same mirrored.
const COBRA: [number, number, number][] = [
  [2.9, -20.2, 3.2], [-2.5, -16.6, 7.3], [-6.0, -14.5, 12.5], [-6.3, -15.0, 17.3], [-4.5, -17.6, 20.6], [-1.2, -21.6, 21], [1.1, -25.2, 18.7],
  [0.4, -30.7, 15.2],
  [-3.9, -34.2, 18.7], [-8.2, -34.1, 21], [-13.3, -33.4, 20.6], [-16.5, -33.3, 17.3], [-16.7, -33.8, 12.5], [-13.1, -35.7, 7.3], [-7.2, -38.4, 3.2],
]

await buildCoaster({
  name: 'The Flying Cobras',
  out: '../../landmarks/models/carowinds-flying-cobras.glb',
  chain: CHAIN,
  shuttle: true,
  anchor: [19, -39],
  ground: { x0: -40, y0: -90, step: 10, rows: GROUND, base: 193.3 },
  // Heights above the local ground; s along the polyline above (146 m to the
  // front spike's end, 150 m round).
  profile: [
    { name: 'station', s: 34, z: 1.8, r: 15 },
    { name: 'cobra roll, in', s: 56.6, z: 1.8 },
    { name: 'cobra roll, out', s: 88.7, z: 1.8 },
    { name: 'loop, out', s: 107, z: 1.8, r: 15, pitch: 53 },
    // Both spikes run straight to their ends; their common crest lies in the
    // 4 m gap between the two ends, which isn't drawn.
    { name: 'spike tops, in the gap', s: 147.7, z: 38, r: 1, pitch: 53 },
  ],
  inversions: [
    { kind: 'path', name: 'cobra roll', from: 56.6, to: 88.7, points: COBRA },
    { kind: 'loop', name: 'vertical loop', from: 89.5, to: 106, height: 18, rTop: 5, offset: 2 },
  ],
  // The catch car holds the train near the rear spike's top and the chain
  // on the front spike takes it where it runs out of speed: one range over
  // both tops, across the gap between them.
  lift: [130, 5.3],
  brakes: [],
  // Let go from rest; seven cars on a 53° ramp put the train's middle about
  // 7 m below the top, which is where `lift` ends.
  liftHead: 0.3,
  losses: [0.013, 0.0007],
  track: { W: 2.4, RAIL: 0.55, DEPTH: 1.5, SPINE: 1.0 },
  station: { ring: STATION, posts: [[9, -27], [13.2, -23.6], [26.5, -35.8], [16, -34.5]], roofZ: 5.5 },
  // Colours from the photos, pulled to the palette's lightness: blue track,
  // its spine a shade deeper, white supports.
  colours: { deck: ['cobras-blue', 0x4c78c4], spine: ['cobras-blue-2', 0x3f68b0], supports: 'trim' },
  supportR: [0.55, 0.7, 0.8],
  meta: { height: 35.5, trackLength: 285 },
})
