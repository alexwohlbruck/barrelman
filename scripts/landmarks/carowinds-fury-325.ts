/**
 * Fury 325, Carowinds: procedural, CC0-1.0.
 * bun scripts/landmarks/carowinds-fury-325.ts   (REPORT=1 for the element table and energy check)
 *
 * Map frame: x east, y north, z up, metres; bearing 0, so the frame is true
 * north. The origin is the middle of the track's footprint (see ANCHOR), at the
 * lowest ground under it. Built with ./coaster-kit.ts.
 *
 * Plan: the circuit is the chain of OSM `roller_coaster=track` ways named
 * Fury 325 (way/670967416 … way/895332251, all oneway, in the direction of
 * travel), from the station at the west end of the entrance plaza. The
 * transfer spur to the train shed (way/888530850) is left out.
 *
 * Profile: a B&M giga of 2,012 m. Element heights are the published ones
 * (RCDB 12273, Wikipedia, Coasterpedia: a 325 ft lift, an 81° drop of
 * 320 ft, the 190 ft barrel turn, the 157 ft overbanked horseshoe at 91°,
 * the 101 ft second banked turn, the 111 ft camelback). Each element is put
 * on the OSM plan by its shape (the barrel turn is the tight turnaround
 * north-east of the drop, the horseshoe the hairpin at the south end, the
 * hive dive the tunnel way/888530541 under the entrance path) and checked
 * against photos matched by camera position. The drop and pull-out radii
 * keep the bottom of the drop under about 4.7 g. Positions are metres along
 * the OSM polyline (1,805 m round) and % of it:
 *
 *   lift 33° from s 105 (6%) to the crest at s 248 (14%), 99 m
 *   first drop, 81°, foot at s 336 (19%)
 *   barrel turn, 57 m, s 406–422 (22–23%), banked 70°
 *   s-curve crest over the north entrance, 22 m, s 570 (32%)
 *   banked turn left, 13 m, s 685 (38%), then a long climb
 *   overbanked horseshoe, 48 m, s 815 (45%), banked 91°
 *   hive dive under the entrance path, s 926 (51%)
 *   second banked turn left, 31 m, s 1010 (56%)
 *   camelback, 34 m, s 1140 (63%)
 *   helix carousel, 18 m falling to 7 m, s 1270–1400 (70–78%)
 *   camelbacks, 18 m at s 1475 (82%) and 14 m under the lift at s 1575 (87%)
 *   final brakes from s 1640 (91%)
 *
 * Photos used for matching (Wikimedia Commons): "Fury 325's long teal
 * airtime hills and white supports stretching over the lawn" and "Fury
 * 325's drop curving down over the trees", ThrillZing, October 2024, CC BY
 * 4.0; "Carowinds aerial view, September 2017", Pi.1415926535, CC BY-SA 3.0.
 *
 * Style: the track is one broad swept ribbon, banked from the train's speed
 * and the plan curvature, with the overbanked turns set by hand. Teal running
 * surface and rails over the lime box spine, as the photos show. The white
 * tubular supports are chunky columns, single where the track is low and an
 * A-frame of two splayed legs where it is high, except on the lift and the
 * top of the drop: there, as in the photos, four big bents about 40 m apart,
 * each a column and one strut with a single tie. The station is its flat
 * canopy roof on posts.
 */
import { buildCoaster } from './coaster-kit'

// The OSM circuit, node by node, in the park frame (see coaster-kit), starting
// at the station exit (the first node of way/670967416).
const CHAIN: [number, number][] = [
  [38.8, 272.9], [56.5, 280.2], [62.0, 282.2], [80.3, 288.7], [202.2, 333.9], [215.9, 339.9], [266.7, 357.9], [284.7, 365.0],
  [294.9, 368.9], [301.6, 371.8], [309.3, 376.0], [316.4, 380.5], [319.2, 382.9], [322.0, 385.4], [324.5, 388.1], [327.0, 391.3],
  [329.4, 394.6], [331.8, 398.3], [335.0, 404.1], [337.4, 407.9], [339.2, 411.7], [342.6, 416.7], [345.5, 419.4], [352.2, 424.2],
  [355.7, 425.7], [360.6, 426.9], [363.4, 426.9], [365.7, 426.6], [368.1, 426.1], [370.9, 424.8], [373.3, 423.0], [375.7, 421.2],
  [378.3, 417.7], [379.6, 414.9], [381.0, 411.8], [381.7, 406.9], [382.1, 401.2], [381.6, 394.3], [380.8, 389.2], [379.8, 380.5],
  [379.0, 372.7], [379.3, 361.2], [380.0, 352.8], [381.5, 345.4], [383.4, 338.7], [386.1, 330.6], [388.8, 323.8], [392.3, 317.4],
  [395.4, 312.9], [399.9, 307.3], [405.2, 302.1], [410.2, 298.4], [414.8, 295.8], [421.1, 293.2], [428.1, 291.2], [433.1, 289.4],
  [440.3, 287.8], [447.4, 285.9], [456.2, 283.5], [464.2, 280.5], [468.9, 277.6], [473.1, 274.4], [476.4, 271.0], [479.2, 267.1],
  [481.6, 263.1], [483.2, 258.9], [484.1, 254.3], [484.4, 249.1], [484.0, 245.0], [483.4, 240.8], [482.1, 236.4], [480.2, 231.0],
  [473.1, 214.8], [461.9, 192.3], [455.9, 181.0], [451.9, 172.7], [449.8, 166.7], [448.8, 161.8], [447.8, 154.1], [447.8, 149.2],
  [448.1, 143.6], [449.0, 137.7], [449.9, 133.1], [451.9, 127.0], [454.7, 121.4], [460.0, 112.3], [467.2, 101.9], [472.4, 94.7],
  [475.3, 89.5], [477.2, 85.6], [478.5, 80.8], [478.9, 77.3], [478.5, 74.0], [477.2, 70.1], [475.2, 66.7], [472.9, 64.2],
  [470.4, 62.6], [467.3, 61.4], [463.9, 61.1], [460.9, 61.6], [457.2, 63.1], [454.3, 65.5], [452.4, 68.0], [451.4, 71.1],
  [451.4, 74.9], [452.1, 82.2], [453.1, 88.7], [454.9, 99.5], [457.0, 108.9], [459.4, 117.4], [462.0, 128.5], [463.1, 136.8],
  [464.4, 147.4], [464.6, 149.0], [466.4, 161.5], [466.7, 163.2], [468.9, 179.2], [470.6, 193.6], [471.3, 198.8], [472.0, 203.5],
  [473.4, 209.6], [474.2, 214.7], [474.5, 223.6], [474.1, 227.4], [473.4, 230.5], [472.2, 234.6], [470.9, 237.7], [469.2, 240.6],
  [466.4, 244.2], [464.1, 247.0], [460.4, 250.1], [452.7, 255.0], [446.8, 259.1], [439.7, 263.6], [432.5, 269.1], [426.3, 274.2],
  [419.6, 280.2], [411.9, 286.7], [402.5, 295.0], [396.6, 300.5], [389.4, 307.6], [381.1, 316.2], [374.0, 323.6], [368.0, 329.7],
  [362.6, 334.7], [356.8, 340.0], [351.6, 344.5], [344.9, 349.7], [342.0, 351.7], [333.2, 357.9], [325.8, 363.9], [321.6, 367.2],
  [317.6, 370.9], [314.7, 374.1], [313.0, 376.5], [311.1, 379.4], [309.1, 383.4], [308.2, 386.5], [307.6, 389.8], [307.3, 392.4],
  [307.4, 396.7], [308.0, 400.6], [309.3, 404.9], [312.1, 409.9], [314.6, 413.4], [317.0, 416.0], [320.8, 418.9], [323.3, 420.4],
  [326.9, 422.1], [331.2, 423.3], [334.5, 423.8], [338.4, 423.9], [343.6, 423.6], [348.3, 422.3], [351.7, 421.0], [354.6, 419.3],
  [358.3, 416.5], [361.6, 413.7], [364.3, 410.2], [366.6, 406.5], [368.5, 402.3], [369.7, 399.0], [370.5, 395.4], [370.8, 389.2],
  [370.7, 385.0], [370.1, 380.4], [369.0, 376.8], [367.2, 372.4], [364.9, 367.8], [362.2, 363.8], [358.4, 359.6], [355.2, 356.8],
  [351.9, 354.6], [348.2, 352.7], [339.1, 350.4], [335.2, 350.5], [330.6, 351.0], [326.7, 351.8], [322.6, 352.9], [317.8, 354.6],
  [308.3, 358.9], [293.1, 365.5], [279.8, 370.4], [265.2, 374.7], [258.8, 376.3], [253.1, 378.1], [248.9, 379.2], [245.3, 379.8],
  [241.5, 380.0], [237.2, 379.9], [232.6, 379.0], [229.4, 377.9], [224.7, 375.6], [220.9, 373.3], [218.5, 369.4], [214.1, 363.5],
  [210.0, 355.7], [207.3, 350.6], [203.7, 343.5], [196.8, 332.0], [193.6, 325.4], [191.1, 321.3], [188.4, 316.8], [183.8, 314.2],
  [178.9, 311.1], [171.1, 308.0], [153.5, 300.5], [140.7, 296.0], [121.0, 287.5], [86.9, 274.1], [70.7, 267.5], [30.8, 251.7],
  [26.0, 250.3], [23.2, 250.4], [20.6, 251.4], [17.7, 253.8], [15.9, 256.4], [15.6, 259.6], [16.2, 263.0], [18.3, 265.5],
  [22.3, 266.8],
]
// Terrain (Mapzen/AWS Terrarium, open data), 20 m cells from (-20, 40); rows
// south to north, columns west to east; metres above 183.1 m.
const GROUND = [
  [10.7, 10.9, 11.2, 10.6, 10.8, 10.9, 11.4, 11.8, 11.8, 10.8, 10.0, 12.2, 12.4, 12.6, 12.6, 12.6, 12.4, 12.5, 12.1, 12.1, 11.6, 11.7, 11.6, 11.5, 10.4, 9.8, 9.3],
  [10.7, 10.5, 10.8, 10.6, 10.5, 10.4, 11.1, 11.5, 12.0, 11.7, 12.3, 12.2, 12.4, 12.5, 12.8, 12.4, 11.4, 11.2, 11.7, 11.9, 11.8, 11.6, 11.3, 10.2, 9.8, 9.5, 9.2],
  [10.2, 10.6, 10.9, 11.1, 10.4, 10.6, 11.3, 11.8, 11.2, 11.5, 11.7, 12.0, 12.9, 12.8, 13.0, 12.7, 11.4, 10.5, 12.0, 12.0, 11.6, 11.1, 10.4, 10.3, 9.7, 9.2, 9.4],
  [10.4, 10.5, 10.7, 10.7, 11.0, 11.4, 11.4, 11.7, 11.6, 11.5, 11.7, 12.3, 12.3, 12.9, 12.9, 13.0, 9.1, 12.3, 12.1, 11.2, 10.8, 10.8, 10.8, 10.4, 9.6, 9.2, 9.1],
  [9.9, 10.1, 10.3, 10.8, 10.8, 10.9, 11.4, 11.3, 11.2, 11.3, 12.1, 11.7, 12.6, 12.8, 12.9, 9.9, 13.0, 13.0, 12.3, 11.9, 11.1, 11.1, 10.5, 10.2, 8.9, 9.0, 9.3],
  [9.2, 9.9, 10.1, 10.7, 10.5, 10.9, 11.4, 10.8, 10.7, 11.0, 11.5, 11.4, 12.1, 12.3, 10.5, 12.5, 13.0, 13.0, 12.6, 12.7, 11.2, 10.9, 10.8, 8.8, 8.8, 9.0, 8.5],
  [9.9, 9.8, 10.2, 10.5, 10.7, 10.5, 10.7, 10.7, 10.4, 10.6, 9.9, 9.7, 10.1, 10.6, 10.1, 12.9, 13.0, 13.1, 12.7, 12.3, 11.6, 11.1, 9.5, 9.1, 9.2, 8.6, 8.2],
  [9.5, 9.5, 9.6, 9.0, 9.0, 10.5, 10.5, 10.3, 9.6, 7.1, 6.2, 6.4, 6.6, 8.8, 11.0, 12.1, 12.8, 12.8, 12.4, 12.1, 11.7, 10.3, 9.5, 9.6, 9.2, 8.0, 7.9],
  [8.7, 8.7, 9.0, 9.1, 9.1, 9.2, 10.3, 9.9, 10.3, 9.1, 7.5, 6.3, 6.4, 8.0, 9.4, 10.1, 11.1, 12.8, 12.3, 11.8, 11.9, 10.5, 9.8, 9.7, 9.5, 8.3, 7.3],
  [8.6, 8.7, 8.8, 9.5, 9.4, 9.8, 11.0, 10.4, 8.0, 8.3, 7.2, 6.2, 6.5, 7.0, 8.2, 9.2, 10.6, 12.5, 11.1, 11.7, 11.9, 10.5, 10.0, 10.0, 9.6, 8.6, 7.5],
  [8.8, 8.8, 8.6, 8.7, 8.6, 8.8, 8.3, 10.4, 8.7, 7.1, 7.0, 6.4, 6.1, 6.4, 7.5, 8.7, 9.9, 11.5, 11.0, 10.7, 10.5, 10.6, 10.2, 9.9, 9.3, 8.9, 7.8],
  [7.9, 8.5, 8.7, 8.5, 8.6, 8.3, 8.3, 9.5, 8.9, 6.3, 6.1, 5.1, 6.0, 5.9, 7.1, 8.8, 10.6, 12.4, 11.9, 10.3, 10.4, 10.4, 9.9, 9.8, 9.6, 8.9, 8.0],
  [8.6, 8.0, 8.7, 8.7, 8.4, 8.4, 8.1, 7.7, 10.1, 8.0, 8.2, 6.5, 6.4, 5.6, 6.8, 8.6, 11.4, 13.6, 10.4, 10.1, 10.3, 10.3, 10.4, 9.7, 9.4, 8.9, 8.6],
  [8.8, 8.1, 8.1, 8.0, 8.2, 8.1, 8.0, 7.8, 7.5, 6.9, 6.3, 6.4, 6.8, 7.1, 8.0, 8.6, 9.6, 12.3, 9.8, 9.8, 10.4, 10.4, 10.5, 10.5, 9.4, 9.2, 9.0],
  [7.5, 6.9, 6.8, 7.8, 8.0, 8.1, 8.0, 7.5, 6.9, 6.7, 7.0, 6.9, 7.2, 7.2, 7.4, 8.9, 9.4, 9.6, 9.5, 9.9, 10.3, 10.5, 10.4, 10.0, 9.6, 9.3, 9.2],
  [6.3, 5.7, 4.8, 4.8, 7.8, 8.0, 7.8, 7.5, 6.6, 6.5, 6.8, 7.2, 7.4, 7.6, 7.7, 7.9, 8.0, 9.4, 10.1, 10.5, 10.5, 10.7, 12.1, 10.5, 9.9, 9.8, 9.7],
  [5.2, 4.6, 3.6, 2.9, 3.0, 5.4, 7.7, 7.3, 4.7, 6.7, 6.7, 7.1, 7.6, 7.8, 8.0, 8.1, 8.3, 8.6, 8.5, 9.0, 10.1, 10.2, 10.4, 10.5, 10.2, 10.1, 10.2],
  [3.6, 3.3, 2.7, 2.0, 1.9, 1.8, 3.0, 5.7, 6.1, 6.6, 7.2, 7.5, 7.7, 7.9, 8.0, 7.8, 7.9, 8.2, 8.6, 8.7, 8.8, 9.0, 10.0, 10.6, 10.2, 10.0, 10.0],
  [2.5, 2.3, 1.8, 1.5, 1.5, 1.9, 2.6, 3.0, 3.2, 4.9, 6.3, 9.4, 9.7, 8.2, 8.1, 7.9, 7.6, 7.8, 8.1, 8.3, 9.7, 10.3, 9.8, 9.6, 9.8, 10.5, 11.3],
  [0.9, 1.0, 1.3, 1.3, 1.4, 1.9, 2.5, 2.9, 3.6, 3.9, 5.7, 7.4, 8.6, 10.4, 9.9, 8.2, 7.9, 7.5, 7.5, 7.8, 8.2, 8.6, 9.8, 12.7, 12.6, 11.9, 12.0],
  [0.0, 0.9, 1.5, 1.5, 1.5, 1.7, 2.1, 2.4, 2.8, 3.6, 4.9, 6.1, 7.9, 9.4, 10.3, 10.2, 8.8, 7.8, 7.2, 7.3, 7.6, 8.4, 9.8, 11.2, 13.0, 15.8, 14.3],
]
// The canopy over the platform (way/543823785).
const STATION: [number, number][] = [
  [52.8, 284.4], [44.3, 281.1], [38.9, 278.8], [38.8, 272.9], [38.5, 272.0], [38.7, 271.6], [39.9, 269.4],
  [37.8, 265.4], [38.3, 264.9], [40.6, 262.1], [44.3, 262.8], [46.9, 263.2], [47.8, 268.1], [48.6, 268.4],
  [49.2, 268.6], [57.9, 271.8], [57.8, 273.9], [57.4, 279.1], [56.5, 280.2],
]

await buildCoaster({
  name: 'Fury 325',
  out: '../../landmarks/models/carowinds-fury-325.glb',
  chain: CHAIN,
  anchor: [250, 244],
  ground: { x0: -20, y0: 40, step: 20, rows: GROUND, base: 183.1 },
  // Heights above the local ground; s along the OSM polyline (1,805 m round).
  profile: [
    { name: 'station', s: 0, z: 4 },
    { name: 'transfer track', s: 30, z: 4.5 },
    { name: 'foot of the lift', s: 105, z: 5, r: 20 },
    { name: 'lift crest (325 ft)', s: 248, z: 99, r: 40, pitch: 81 },
    { name: 'first drop, foot (81°)', s: 336, z: 4, r: 60 },
    { name: 'barrel turn (190 ft), in', s: 406, z: 57, r: 28 },
    { name: 'barrel turn (190 ft), out', s: 422, z: 57, r: 30 },
    { name: 'valley after the barrel turn', s: 494, z: 5 },
    { name: 's-curve over the north entrance', s: 570, z: 22 },
    { name: 'valley', s: 628, z: 7 },
    { name: 'banked turn left', s: 685, z: 13 },
    { name: 'overbanked horseshoe (157 ft)', s: 815, z: 47.9 },
    { name: 'hive dive, under the entrance path', s: 926, z: -1 },
    { name: 'second banked turn left (101 ft)', s: 1010, z: 30.8 },
    { name: 'valley', s: 1085, z: 6 },
    { name: 'camelback (111 ft)', s: 1140, z: 33.8 },
    { name: 'valley', s: 1200, z: 6 },
    { name: 'helix carousel, top', s: 1270, z: 18 },
    { name: 'helix carousel, low', s: 1400, z: 7 },
    { name: 'camelback', s: 1475, z: 18 },
    { name: 'valley', s: 1520, z: 6 },
    { name: 'camelback under the lift', s: 1575, z: 14 },
    { name: 'final brakes', s: 1640, z: 6, r: 20 },
    { name: 'final brakes, out', s: 1740, z: 5 },
  ],
  lift: [98, 248],
  brakes: [{ from: 1640, to: 98, z: 5, head: 0.5 }],
  banks: [
    // The drop runs straight and level; the turn comes after the pull-out.
    { from: 250, to: 305, deg: 0, name: 'first drop' },
    { from: 398, to: 428, deg: 70, name: 'barrel turn' },
    { from: 800, to: 840, deg: 91, name: 'overbanked horseshoe' },
  ],
  belowGrade: [[895, 960]],
  losses: [0.009, 0.0003],
  track: { W: 4.0, RAIL: 0.7, DEPTH: 2.3, SPINE: 1.8 },
  station: { ring: STATION, posts: [[41, 264], [56.5, 272.5], [40, 277.5], [53, 282.5]], roofZ: 8.5 },
  // Colours from the photos, pulled to the palette's lightness: the teal-blue
  // running surface and rails, the lime-yellow box spine, white supports.
  colours: { deck: ['fury-teal', 0x5aaac8], spine: ['fury-lime', 0xc6d566], supports: 'trim' },
  supportR: [0.95, 1.1, 1.3],
  // The lift and the top of the drop stand on four big bents about 40 m
  // apart, not a run of A-frames: on the lift a column with one strut
  // splayed to the north (left of travel), and over the drop a column with
  // a strut raked back towards the lift. Matched to the drop and flag photos.
  bents: [{
    from: 112, to: 296,
    at: [
      { s: 132, strut: 'left', tie: 0 },
      { s: 171, strut: 'left' },
      { s: 211, strut: 'left' },
      { s: 274, strut: 'back', spread: 0.28, tie: 0.35 },
    ],
  }],
  meta: { height: 99, trackLength: 2012 },
})
