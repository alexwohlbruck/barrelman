/**
 * Incredicoaster (California Screamin' until 2018), Disney California
 * Adventure, Anaheim: procedural, CC0-1.0.
 * bun scripts/landmarks/dlr-incredicoaster.ts   (REPORT=1 for the element table)
 *
 * Built with ./coaster-kit.ts. Map frame: x east, y north, z up, metres;
 * bearing 0. The park frame is equirectangular about 33.805 N, 117.920 W
 * (`origin`), and the model's origin is the middle of the track's footprint
 * (ANCHOR) on the ground. The ground is taken as flat: the pier is level and
 * Paradise Bay's water is the map's.
 *
 * Plan (OSM, exact where tagged): the chain of `roller_coaster=track` ways in
 * ride order from the station exit, 1,566 m of polyline (published track
 * 1,851 m). The station has two loading tracks; the circuit takes the western
 * one (way/108039685 …), and the eastern one (way/108039686 …) is drawn as a
 * level stub beside it. The `service=siding` spurs to the train shed are left
 * out, and so is the shed. The OSM trace draws the vertical loop as a zigzag
 * (way/265898491, `note=loop`); the kit draws the loop over its own window.
 *
 * Published (Wikipedia, RCDB 731, Coasterpedia): Intamin/Stengel, 2001;
 * 6,072 ft of track; 120 ft high; 108 ft drop; 55 mph, reached by the LIM
 * launch (0–55 mph in 4 s) and again at the foot of the drop; one vertical
 * loop; an LIM-driven (not chain) lift midway; four mid-course block brakes;
 * white track and supports, oxide-red "scream tubes" (2018).
 *
 * Ride order and where it sits on the plan (s = metres along the OSM
 * polyline, and % of it):
 *
 *   station, left turn west          s 0–20 (0–1%)
 *   LIM launch along Paradise Bay    s 40–140 (3–9%), at water level
 *   first hill, in a red tube        s 147–208 (9–13%), steep climb, crest about s 185
 *   dive right, block brake 1        s 272–318 (17–20%), the north leg at the west end
 *   sweeping right turn, left turn   s 320–440 (20–28%), low, under the outbound track
 *   LIM lift, red tube over its top  s 455–528 (29–34%), crest 120 ft
 *   drop, 108 ft                     foot about s 572 (37%)
 *   right-hand 3/4 turn, raised      s 600–714 (38–46%), over the later spiral
 *   vertical loop                    s 775–845 window (49–54%), beside the lift
 *   block brake 2, third tube        s 855–925 (55–59%)
 *   right turn, block brake 3        s 925–1150 (59–73%)
 *   two bunny hops over Midway Mania s 1150–1230 (73–79%)
 *   block brake 4, 270° spiral down  s 1235–1385 (79–88%), left-handed, under the 3/4 turn
 *   last hop, left turns, brakes     s 1390–1525 (89–97%)
 *
 * Heights: the lift crest and drop are published; the rest were estimated
 * from the photos below (fitted cameras from their GPS and compass tags) and
 * checked against the energy left. See the profile's comments; the first
 * hill, the loop and the 3/4 turn are measured, the brake runs and hops are
 * estimates.
 *
 * Identifying features: the white lattice structure (drawn as the kit's
 * trestle, broad walls with large openings, per STYLE.md's "never lattice"),
 * the white track, the red scream tubes over the first hill, over the lift's
 * crest and after the loop, and the gold disc behind the loop. The disc
 * carried a Mickey head until 2009 and a sunburst until 2018; since 2018 it
 * is the gold Pixar Pier emblem (photos 2018 and 2023), drawn here as a plain
 * gold disc with a red centre band, without lettering.
 *
 * Photos (Wikimedia Commons):
 * - "View of Incredicoaster and Pixar Pal-A-Round (Disney California Adventure
 *   Park) July 2023.JPG" and "… July 2023 (2).JPG", "View of Incredicoaster
 *   Start & Pixar Pal-A-Round … July 2023.jpg", "Incredicoaster & Pixar
 *   Pal-A-Round (Disney) July 2023.JPG": Benoît Prieur, CC0 (iPhone, GPS).
 * - "Incredicoaster.jpg", "Pixar Pier (31653565838).jpg", "Incredicoaster
 *   (31653564558).jpg": Mignon Pelletier, CC BY-SA 2.0.
 * - "Disney California Adventure (51241258541).jpg", "Disney California
 *   Adventure (51240588787).jpg", "IncrediCoaster station.jpg": Jeremy
 *   Thompson, CC BY 2.0.
 * - "Incredicoaster 2 2023-06-03.jpeg": FASTILY, CC BY-SA 4.0.
 */
import { buildCoaster, type CoasterKit, type TrackFrame } from './coaster-kit'
import { Part, cross, sub, type V3 } from './mesh'
import { finish } from './palette'

// The OSM circuit in the park frame, from the station exit (the merge node of
// the two loading tracks, node/2103683094), in the direction of travel.
const CHAIN: [number, number][] = [
  [-34.9, -27.4], [-31.7, -17.8], [-31.5, -16.2], [-31.7, -14.6], [-32.1, -12.8], [-32.9, -11.5], [-34.9, -9.5], [-35.6, -9.1],
  [-36.8, -9.2], [-40.4, -9.5], [-46.0, -10.2], [-51.2, -10.8], [-58.1, -11.8], [-95.2, -16.3], [-161.8, -24.5], [-177.2, -26.4],
  [-221.5, -31.9], [-231.5, -33.1], [-240.5, -33.8], [-246.0, -33.4], [-257.0, -29.5], [-262.6, -25.9], [-269.8, -18.4], [-274.6, -8.4],
  [-281.6, 26.6], [-281.3, 34.1], [-280.1, 38.3], [-277.7, 42.1], [-271.2, 47.0], [-267.5, 48.7], [-264.4, 49.3], [-258.8, 49.4],
  [-253.1, 47.5], [-248.9, 44.7], [-243.7, 38.7], [-241.6, 32.0], [-242.2, 24.9], [-244.5, 19.7], [-251.2, 7.3], [-254.0, 1.1],
  [-255.6, -7.2], [-253.9, -15.5], [-250.0, -23.1], [-247.1, -26.8], [-230.8, -35.8], [-172.1, -68.5], [-151.1, -79.6], [-138.7, -84.8],
  [-135.9, -85.0], [-130.6, -85.4], [-117.8, -83.8], [-107.3, -78.2], [-99.2, -72.1], [-95.2, -68.1], [-90.5, -64.4], [-85.9, -61.8],
  [-82.0, -60.4], [-77.0, -59.5], [-72.0, -59.9], [-67.5, -61.4], [-62.6, -64.3], [-59.7, -66.7], [-57.2, -69.7], [-55.1, -74.2],
  [-53.9, -78.4], [-53.4, -83.6], [-54.0, -88.7], [-55.8, -94.8], [-58.1, -99.3], [-61.3, -103.0], [-64.7, -105.7], [-70.8, -108.3],
  [-76.3, -109.2], [-81.8, -108.3], [-87.4, -106.4], [-118.8, -89.8], [-131.7, -83.1], [-151.5, -72.5], [-166.0, -64.8], [-154.4, -72.9],
  [-164.4, -67.9], [-224.2, -35.6], [-241.0, -25.4], [-247.9, -19.5], [-265.9, -0.1], [-272.4, 6.8], [-282.6, 18.6], [-285.7, 26.6],
  [-284.8, 35.7], [-282.4, 43.4], [-275.0, 50.0], [-265.7, 53.5], [-255.6, 53.4], [-250.9, 51.3], [-245.7, 48.1], [-240.4, 41.2],
  [-239.0, 37.7], [-237.2, 29.5], [-237.6, 24.9], [-239.2, 14.3], [-240.1, 4.6], [-238.4, -1.8], [-235.3, -8.2], [-232.1, -12.0],
  [-226.6, -16.5], [-166.5, -49.1], [-143.6, -61.4], [-127.1, -70.2], [-102.5, -90.1], [-90.3, -100.5], [-85.4, -104.0], [-79.6, -105.7],
  [-74.5, -105.4], [-68.7, -104.3], [-62.9, -101.5], [-58.5, -97.5], [-56.0, -93.6], [-54.6, -89.0], [-53.9, -83.6], [-54.4, -78.3],
  [-55.6, -74.4], [-57.4, -70.2], [-60.0, -67.1], [-62.8, -64.7], [-67.6, -61.8], [-72.1, -60.4], [-77.2, -60.1], [-81.8, -60.9],
  [-86.2, -62.5], [-90.8, -65.6], [-94.8, -70.3], [-99.0, -76.4], [-105.0, -84.2], [-108.6, -90.0], [-109.2, -95.8], [-108.5, -99.9],
  [-106.9, -103.3], [-104.0, -106.6], [-100.9, -108.6], [-97.1, -110.0], [-90.2, -110.1], [-68.3, -109.5], [-56.5, -109.4], [-54.3, -108.8],
  [-51.8, -106.9], [-49.9, -104.6], [-48.5, -101.8], [-48.2, -96.7], [-48.6, -68.5], [-47.9, -65.1], [-47.8, -63.0], [-48.2, -61.5],
  [-49.5, -55.1], [-49.0, -52.9], [-43.2, -35.7], [-41.8, -33.7], [-39.8, -31.8], [-36.7, -29.4],
]
// The eastern loading track (way/108039686, 598399296, 597969172, 265898490, 597969174, 200375174).
const EAST_TRACK: [number, number][] = [
  [-47.9, -65.1], [-46.4, -63.7], [-45.1, -62.7], [-41.0, -58.7], [-39.7, -57.4], [-39.3, -56.2], [-33.7, -38.9], [-33.4, -36.3],
  [-33.8, -34.3], [-34.9, -29.6], [-34.9, -27.4],
]
// The loading platform's roof (way/194882116).
const STATION: [number, number][] = [
  [-46.7, -35.2], [-42.1, -36.7], [-40.8, -37.1], [-37.1, -38.3], [-35.2, -38.9], [-31.4, -40.1], [-33.2, -45.7], [-34.8, -50.9],
  [-36.7, -57.0], [-39.3, -56.2], [-42.3, -55.3], [-44.3, -61.5], [-52.7, -58.8], [-53.6, -58.6], [-54.3, -58.3], [-55.4, -58.0],
  [-61.0, -56.1], [-60.7, -55.0], [-59.9, -52.6], [-59.7, -51.9], [-59.4, -50.9], [-59.0, -49.6], [-58.0, -49.9], [-57.0, -50.2],
  [-55.4, -50.8], [-54.4, -51.1], [-53.3, -51.4], [-54.1, -53.6], [-52.7, -54.0], [-50.7, -54.7], [-49.5, -55.1], [-48.4, -55.5],
  [-47.7, -53.4], [-49.0, -52.9], [-50.0, -52.6], [-51.4, -52.1], [-52.4, -51.8], [-52.1, -50.7], [-50.2, -45.3], [-48.3, -40.1],
]
const ANCHOR: [number, number] = [-158, -28]
const FLAT = Array.from({ length: 7 }, () => Array(10).fill(0))

// Colours from the photos, pulled to the palette's lightness: white track,
// cream-white structure (`trim`), oxide-red tubes, the gold disc.
const RED = finish('incredi-red', 0xc84a42)
const GOLD = finish('pier-gold', 0xd2ac6c)
const STATION_Z = 1.2 // deck height through the station and along the launch

/**
 * The scream tubes, polyline s: round tubes the train runs through. `slots`
 * metres at the far end are open hoops, as the lift tube's drop end is (the
 * dark striped end in the photos).
 */
const TUBES: { from: number; to: number; name: string; slots?: number }[] = [
  { from: 158, to: 222, name: 'first hill' },
  { from: 500, to: 563, name: 'lift crest', slots: 14 },
  { from: 897, to: 927, name: 'after the loop' },
]

function extras(k: CoasterKit) {
  const red = new Part(), gold = new Part()
  const add = (a: V3, b: V3): V3 => [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
  const mul = (a: V3, s: number): V3 => [a[0] * s, a[1] * s, a[2] * s]
  const unit = (a: V3): V3 => mul(a, 1 / (Math.hypot(...a) || 1))
  const SIDES = 10

  // ---- scream tubes: a round shell centred over the train, open ends, a
  // band every 9 m (the tubes' joints) and the inside showing at each mouth.
  const R = 2.7, UP = 1.3
  for (const t of TUBES) {
    const end = t.to - (t.slots ?? 0)
    const frames: TrackFrame[] = []
    for (let s = t.from; s <= end + 1e-6; s += Math.max(1, (end - t.from) / Math.ceil((end - t.from) / 5))) frames.push(k.at(s))
    const ring = (f: TrackFrame, r: number) => Array.from({ length: SIDES }, (_, i) => {
      const a = (i / SIDES) * 2 * Math.PI, n = add(mul(f.left, Math.cos(a)), mul(f.up, Math.sin(a)))
      return { p: add(add(f.p, mul(f.up, UP)), mul(n, r)), n }
    })
    const shell = (p: Part, A: ReturnType<typeof ring>, B: ReturnType<typeof ring>, inside = false) => {
      for (let i = 0; i < SIDES; i++) {
        const j = (i + 1) % SIDES
        const nn = (q: { n: V3 }) => (inside ? mul(q.n, -1) : q.n)
        const quad: [typeof A[0], typeof A[0], typeof A[0], typeof A[0]] = [A[i], A[j], B[j], B[i]]
        // Wind each face to its normal.
        const f = cross(sub(quad[1].p, quad[0].p), sub(quad[2].p, quad[0].p))
        const want = nn(quad[0])
        const ok = f[0] * want[0] + f[1] * want[1] + f[2] * want[2] >= 0
        const o = ok ? [0, 1, 2, 3] : [0, 3, 2, 1]
        p.tri(quad[o[0]].p, quad[o[1]].p, quad[o[2]].p, undefined, undefined, undefined, [nn(quad[o[0]]), nn(quad[o[1]]), nn(quad[o[2]])])
        p.tri(quad[o[0]].p, quad[o[2]].p, quad[o[3]].p, undefined, undefined, undefined, [nn(quad[o[0]]), nn(quad[o[2]]), nn(quad[o[3]])])
      }
    }
    for (let i = 0; i + 1 < frames.length; i++) shell(red, ring(frames[i], R), ring(frames[i + 1], R))
    // Open hoops over the slotted end.
    for (let s = end + 2; s + 1 <= t.to + 1e-6; s += 2) shell(red, ring(k.at(s), R), ring(k.at(s + 1), R))
    // The inside, a few metres in from each mouth, in the tube's red.
    for (const [a, b] of t.slots ? [[0, 1]] : [[0, 1], [frames.length - 2, frames.length - 1]]) shell(red, ring(frames[a], R - 0.15), ring(frames[b], R - 0.15), true)
    // Bands at the joints, a shade proud of the shell.
    let acc = 0
    for (let i = 1; i < frames.length - 1; i++) {
      acc += Math.hypot(...sub(frames[i].p, frames[i - 1].p))
      if (acc < 18) continue
      acc = 0
      const f = frames[i], g: TrackFrame = { ...f, p: add(f.p, mul(f.t, 0.7)) }
      shell(gold, ring(f, R + 0.12), ring(g, R + 0.12))
    }
  }

  // ---- the disc behind the loop, on the side away from the bay.
  for (const w of k.inversions) {
    if (w.inv.kind !== 'loop') continue
    const pts = w.pts
    const top = pts.reduce((m, q) => (q.p[2] > m.p[2] ? q : m))
    const xs = pts.map((q) => q.p)
    const cx = (Math.min(...xs.map((p) => p[0])) + Math.max(...xs.map((p) => p[0]))) / 2
    const cy = (Math.min(...xs.map((p) => p[1])) + Math.max(...xs.map((p) => p[1]))) / 2
    // The loop's plane: its forward axis is the plan direction entry → exit.
    const fwd = unit([pts[pts.length - 1].p[0] - pts[0].p[0], pts[pts.length - 1].p[1] - pts[0].p[1], 0])
    let side: V3 = [-fwd[1], fwd[0], 0]
    if (side[1] > 0) side = mul(side, -1) // to the south, away from Paradise Bay
    const DR = DISC.radius, OFF = DISC.offset, T = 0.6
    const c: V3 = add([cx, cy, top.p[2] - DISC.below], mul(side, OFF))
    const ax: V3 = fwd, up: V3 = [0, 0, 1]
    const N = 24
    const rim = (r: number, d: number) => Array.from({ length: N }, (_, i) => {
      const a = (i / N) * 2 * Math.PI
      return add(add(c, mul(side, d)), add(mul(ax, r * Math.cos(a)), mul(up, r * Math.sin(a))))
    })
    // Two faces and the edge, flat-shaded; the red band on the bay side.
    const front = rim(DR, -T / 2), back = rim(DR, T / 2)
    const nF = mul(side, -1), nB = side
    for (let i = 0; i < N; i++) {
      const j = (i + 1) % N, cF = add(c, mul(side, -T / 2)), cB = add(c, mul(side, T / 2))
      gold.tri(cF, front[j], front[i], undefined, undefined, undefined, [nF, nF, nF])
      gold.tri(cB, back[i], back[j], undefined, undefined, undefined, [nB, nB, nB])
      const ne = unit(sub(add(front[i], front[j]), mul(cF, 2)))
      gold.tri(front[i], front[j], back[j], undefined, undefined, undefined, [ne, ne, ne])
      gold.tri(front[i], back[j], back[i], undefined, undefined, undefined, [ne, ne, ne])
    }
    // The emblem's lettering, abstracted to a red band across the middle.
    const bw = DR * 0.62, bh = DR * 0.22, d = -T / 2 - 0.05
    const P = (u: number, v: number) => add(add(c, mul(side, d)), add(mul(ax, u), mul(up, v)))
    const q = [P(-bw, -bh), P(bw, -bh), P(bw, bh), P(-bw, bh)]
    red.tri(q[0], q[2], q[1], undefined, undefined, undefined, [nF, nF, nF])
    red.tri(q[0], q[3], q[2], undefined, undefined, undefined, [nF, nF, nF])
    // Two posts under it.
    for (const u of [-0.45, 0.45]) {
      const foot = add(c, add(mul(ax, u * DR), mul(up, -Math.sqrt(1 - u * u) * DR)))
      post(k.parts.supports, [foot[0], foot[1], k.ground(foot[0], foot[1]) - 0.5], foot, 0.35)
    }
  }

  // ---- the eastern loading track: a level stub of deck and spine.
  {
    const { W, DEPTH } = k.track
    const pts = EAST_TRACK.map((q) => k.toModel(q))
    for (let i = 0; i + 1 < pts.length; i++) {
      const [ax_, ay] = pts[i], [bx, by] = pts[i + 1], l = Math.hypot(bx - ax_, by - ay) || 1
      const nx = (-(by - ay) / l) * (W / 2), ny = ((bx - ax_) / l) * (W / 2), z = STATION_Z
      const A: V3 = [ax_ + nx, ay + ny, z], B: V3 = [bx + nx, by + ny, z], C: V3 = [bx - nx, by - ny, z], D: V3 = [ax_ - nx, ay - ny, z]
      k.parts.deck.quad(D, C, B, A)
      const dn = (p: V3): V3 => [p[0], p[1], z - DEPTH]
      k.parts.deck.quad(A, B, dn(B), dn(A))
      k.parts.deck.quad(C, D, dn(D), dn(C))
    }
  }
  return [{ part: red, material: RED }, { part: gold, material: GOLD }]
}

/** A square post from a to b, half-width r. */
function post(p: Part, a: V3, b: V3, r: number) {
  const c: [number, number][] = [[-r, -r], [r, -r], [r, r], [-r, r]]
  for (let i = 0; i < 4; i++) {
    const [x0, y0] = c[i], [x1, y1] = c[(i + 1) % 4]
    p.quad([a[0] + x0, a[1] + y0, a[2]], [a[0] + x1, a[1] + y1, a[2]], [b[0] + x1, b[1] + y1, b[2]], [b[0] + x0, b[1] + y0, b[2]])
  }
}

/** The disc: radius, how far its centre sits below the loop's top, and its offset from the loop's plane. */
const DISC = { radius: 8.8, below: 5.3, offset: 3.2 }

await buildCoaster({
  name: 'Incredicoaster',
  out: '../../landmarks/models/dlr-incredicoaster.glb',
  origin: [-117.92, 33.805],
  chain: CHAIN,
  anchor: ANCHOR,
  ground: { x0: -330, y0: -150, step: 40, rows: FLAT, base: 0 },
  // Heights above the ground; s along the OSM polyline (1,566 m round).
  profile: [
    { name: 'station exit', s: 0, z: STATION_Z },
    { name: 'launch, hold', s: 41, z: STATION_Z },
    { name: 'launch, end', s: 150, z: STATION_Z, r: 30 },
    { name: 'first hill (in the tube)', s: 190, z: 27, r: 28 },
    { name: 'dive right, foot', s: 250, z: 6, r: 20 },
    { name: 'block brake 1, in', s: 284, z: 11, r: 20 },
    { name: 'block brake 1, out', s: 316, z: 11, r: 20 },
    { name: 'sweeping right turn, low', s: 395, z: 2 },
    { name: 'left turn', s: 440, z: 2, r: 20 },
    { name: 'LIM lift crest (120 ft)', s: 538, z: 36.6, r: 20, pitch: 50 },
    { name: 'drop, foot (108 ft)', s: 584, z: 3.7, r: 20 },
    { name: '3/4 right turn', s: 634, z: 19, r: 30 },
    { name: '3/4 right turn, out', s: 718, z: 18.5 },
    { name: 'dive, foot', s: 758, z: 1.5, r: 22 },
    { name: 'loop, entry', s: 780, z: 1.5 },
    { name: 'loop, exit', s: 852, z: 1.5 },
    { name: 'block brake 2, in', s: 876, z: 10, r: 18 },
    { name: 'block brake 2, out', s: 894, z: 10, r: 18 },
    { name: 'valley after the third tube', s: 990, z: 4.5 },
    { name: 'right turn', s: 1035, z: 7 },
    { name: 'valley', s: 1075, z: 4 },
    { name: 'block brake 3, in', s: 1118, z: 11, r: 20 },
    { name: 'block brake 3, out', s: 1150, z: 11, r: 20 },
    { name: 'bunny hop, valley', s: 1166, z: 9, r: 14 },
    { name: 'bunny hop 1', s: 1182, z: 13 },
    { name: 'bunny hop, valley', s: 1198, z: 9, r: 14 },
    { name: 'bunny hop 2', s: 1214, z: 12.5 },
    { name: 'block brake 4, in', s: 1238, z: 11, r: 20 },
    { name: 'block brake 4, out', s: 1270, z: 11, r: 20 },
    { name: 'spiral, foot', s: 1386, z: 1.5 },
    { name: 'last hop', s: 1403, z: 3 },
    { name: 'left turn, low', s: 1420, z: 1.2, r: 16 },
    { name: 'final brakes', s: 1446, z: STATION_Z },
  ],
  // The tyres out of the station; the launch takes over from there.
  lift: [1530, 45],
  liftHead: 0.4,
  launches: [
    { from: 41, to: 150, z: STATION_Z, head: 31.5, name: 'LIM launch, 55 mph' },
    { from: 455, to: 538, z: 36.6, head: 0.8, name: 'LIM lift' },
  ],
  // Block brakes trim rather than stop in normal running: each lets the train
  // through at the speed the next stretch needs.
  brakes: [
    { from: 286, to: 314, z: 11, head: 1.2 },
    { from: 878, to: 892, z: 10, head: 7 },
    { from: 1120, to: 1148, z: 11, head: 4 },
    { from: 1240, to: 1268, z: 11, head: 2.5 },
    { from: 1448, to: 1530, z: STATION_Z, head: 0.4 },
  ],
  inversions: [{ kind: 'loop', name: 'vertical loop', from: 780, to: 852, height: 24.5, rTop: 8, offset: 3 }],
  losses: [0.006, 0.0003],
  track: { W: 2.8, RAIL: 0.45, DEPTH: 1.2, SPINE: 1.1 },
  trestle: { every: 8, along: 1.8, levelStep: 5, ledgerH: 1.1 },
  sampleDeg: 15,
  station: { ring: STATION, posts: [[-34, -41], [-37, -55], [-45, -59], [-58, -55], [-48, -37]], roofZ: 7 },
  colours: { deck: ['incredi-white', 0xeeece6], spine: 'deck', supports: 'trim' },
  supportR: [0.5, 0.6, 0.7],
  meta: { height: 36.6, trackLength: 1851 },
  extras,
})
