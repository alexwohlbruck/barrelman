/**
 * Rock 'n' Roller Coaster Starring The Muppets, Disney's Hollywood Studios —
 * procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/wdw-rock-n-roller-coaster.ts
 *
 * Map frame: x east, y north, z up, metres, bearing 0. The anchor is the
 * area centroid of the OSM outline (way/357021885, 7,147 m², no height tags),
 * so the outline is drawn in its own local frame and shifted by `C` at the end.
 *
 * What it is: the G-Force Records building at the end of Sunset Boulevard,
 * with the giant Stratocaster leaning against its east front, and behind it
 * the plain show building that holds the indoor coaster. The coaster is
 * entirely indoors, so there is no track to draw. The guitar's strings run
 * on past the fretboard, sweep up over the plaza and come down onto the
 * upside-down limousine under the entrance arch; that sweep, the arch and the
 * limo are modelled too, because the strings are half of what people see.
 *
 * Today (Muppets re-theme, reopened 26 May 2026):
 * - the building is painted dark grey, where it was salmon-tan stucco
 *   (Wikipedia; Commons photo of the entrance taken 3 Oct 2026);
 * - the guitar is still a red Stratocaster with a white pickguard and dark
 *   fretboard, now with psychedelic Electric Mayhem bands of blue, purple,
 *   pink, green and yellow running round its edges (same photo; WDWNT
 *   2026-04: "the guitar will still be red", "the neck won't be painted").
 *   The model keeps the red body and draws the edge bands as a blue outer
 *   band and a pink inner band: six materials leave no room for the rest.
 * - the limo is repainted in brighter colours; drawn in the band blue.
 *
 * Evidence:
 * - OSM (map call, /tmp/disney-work/dhs/map.osm): the outline, exact. The
 *   rounded bump at its east end is the guitar's planter (it matches the
 *   retaining wall way/357022892 and the guitar in the NAIP aerial).
 *   way/357021883 (4 × 4 m) sits where the arch and the limo are; the model
 *   covers it. way/357022423 is the standby-queue canopy north-east of the
 *   guitar, which the model does not draw.
 * - Published: the guitar is 40 ft (12.2 m) tall (WDWNT, WDWMagic); the
 *   coaster reaches 80 ft (24 m) (Wikipedia, RCDB); the show building is
 *   68,000 sq ft.
 * - USGS NAIP orthophoto (public domain), winter, sun ~40–45° high in the
 *   south: the roof plan and steps; shadows give the show box ~31 m of
 *   shadow (→ ~28 m) and the connector ~7 m (→ ~7–8 m), and the strings'
 *   S-curve in plan from the neck, south along the front, east to the arch.
 * - A 2008 aerial from the east (Flickr 7426555140, CC BY-SA 2.0): the show
 *   box in two heights, the south half ~18% taller than the north half.
 * - Photos (Wikimedia Commons / Openverse, credits in the report): the guitar
 *   traced face-on from wwarby's photo (Flickr 1477821882, CC BY 2.0) and
 *   scaled so the fretboard end stands 40 ft above the keys; facade steps and
 *   heights from that photo, paulo guereta's two 2017 photos and the 2026
 *   Muppets entrance photo; arch and limo from ThrillZing (2006) and the May
 *   2023 CC0 photos.
 *
 * Estimated: every height (no OSM tags). Show box 28 m (south) / 24 m
 * (north) from the NAIP shadow, the aerial and the 80 ft coaster inside it;
 * G-Force front block 11 m, measured against the 40 ft guitar in front of it;
 * the steps north of it 9 and 7 m, the connector 8 m. The arch size (11 × 9.8 m) and the limo from photos with
 * people for scale. The strings' height (~17 m at the top of the sweep) from
 * the side photo. Invented: the arch colour after the re-theme (no photo
 * found; drawn pale as before), the back of the show box (no photo; plain),
 * and the exact pose of the keys under the guitar.
 */
import { Part, addGltfTriangles, cross, len, writeGlb, type V3 } from './mesh'
import { PALETTE, finish } from './palette'

type XY = [number, number]

const wall = new Part(), roof = new Part(), trim = new Part()
const red = new Part(), blue = new Part(), pink = new Part()

const C: XY = [-45.126, 10.735] // outline centroid in the outline's local frame
const W = (x: number, y: number, z: number): V3 => [x - C[0], y - C[1], z]

const unit = (v: V3): V3 => { const l = len(v) || 1; return [v[0] / l, v[1] / l, v[2] / l] }
const add = (a: V3, b: V3, k = 1): V3 => [a[0] + b[0] * k, a[1] + b[1] * k, a[2] + b[2] * k]
const dot = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]

/** A triangle whose winding is made to agree with the normals it is given. */
function tri(p: Part, a: V3, b: V3, c: V3, n: V3[]) {
  const face = cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [c[0] - a[0], c[1] - a[1], c[2] - a[2]])
  if (len(face) < 1e-9) return
  const avg = add(add(n[0], n[1]), n[2])
  if (dot(face, avg) >= 0) p.tri(a, b, c, undefined, undefined, undefined, n)
  else p.tri(a, c, b, undefined, undefined, undefined, [n[0], n[2], n[1]])
}
function quad(p: Part, a: V3, b: V3, c: V3, d: V3, n: V3 | V3[]) {
  const N = Array.isArray(n[0]) ? (n as V3[]) : [n as V3, n as V3, n as V3, n as V3]
  tri(p, a, b, c, [N[0], N[1], N[2]])
  tri(p, a, c, d, [N[0], N[2], N[3]])
}
/** Both faces of a thin surface, for strings seen from above and below. */
function sheet(p: Part, a: V3, b: V3, c: V3, d: V3) {
  const n = unit(cross([b[0] - a[0], b[1] - a[1], b[2] - a[2]], [d[0] - a[0], d[1] - a[1], d[2] - a[2]]))
  quad(p, a, b, c, d, n)
  quad(p, a, b, c, d, [-n[0], -n[1], -n[2]])
}
/** Flat-built geometry re-added with smooth shading below a crease angle. */
function smooth(target: Part, build: (m: Part) => void, crease = 50) {
  const m = new Part()
  build(m)
  addGltfTriangles(target, new Float32Array(m.pos), Uint32Array.from({ length: m.pos.length / 3 }, (_, i) => i), { creaseDegrees: crease })
}

// ---------------------------------------------------------------------------
// 2D polygons: orientation, inset and ear-clipping.

const area2 = (q: XY[]) => q.reduce((s, p, i) => { const r = q[(i + 1) % q.length]; return s + p[0] * r[1] - r[0] * p[1] }, 0)
const ccw = (q: XY[]) => (area2(q) < 0 ? [...q].reverse() : q)

/** Moves each vertex along its corner bisector so the edges shift by `d` (+ out). */
function offset(q: XY[], d: number): XY[] {
  const n = q.length
  return q.map((p, i) => {
    const a = q[(i - 1 + n) % n], b = q[(i + 1) % n]
    const e1 = unit([p[0] - a[0], p[1] - a[1], 0]), e2 = unit([b[0] - p[0], b[1] - p[1], 0])
    const n1: XY = [e1[1], -e1[0]], n2: XY = [e2[1], -e2[0]]
    const m = unit([n1[0] + n2[0], n1[1] + n2[1], 0])
    const k = Math.min(2.5, 1 / Math.max(0.4, m[0] * n1[0] + m[1] * n1[1]))
    return [p[0] + m[0] * d * k, p[1] + m[1] * d * k] as XY
  })
}

function earclip(q: XY[]): [number, number, number][] {
  const idx = q.map((_, i) => i), out: [number, number, number][] = []
  const crossz = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (p: XY, a: XY, b: XY, c: XY) => crossz(a, b, p) > 1e-9 && crossz(b, c, p) > 1e-9 && crossz(c, a, p) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let i = 0; i < idx.length; i++) {
      const ia = idx[(i - 1 + idx.length) % idx.length], ib = idx[i], ic = idx[(i + 1) % idx.length]
      const a = q[ia], b = q[ib], c = q[ic]
      if (crossz(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== ia && j !== ib && j !== ic && inside(q[j], a, b, c))) continue
      out.push([ia, ib, ic]); idx.splice(i, 1); cut = true; break
    }
    if (!cut) idx.splice(0, 1) // degenerate sliver: drop a vertex rather than loop
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

// ---------------------------------------------------------------------------
// Building blocks: an OSM-derived footprint extruded with walls, a rounded
// bevel at the parapet and a flat roof.

const UP: V3 = [0, 0, 1]
function block(footprint: XY[], h: number, o: { bev?: number; walls?: Part; z0?: number } = {}) {
  const q = ccw(footprint), bev = o.bev ?? 0.5, p = o.walls ?? wall, z0 = o.z0 ?? 0
  const inner = offset(q, -bev)
  for (let i = 0; i < q.length; i++) {
    const j = (i + 1) % q.length, a = q[i], b = q[j]
    const n = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    quad(p, W(a[0], a[1], z0), W(b[0], b[1], z0), W(b[0], b[1], h - bev), W(a[0], a[1], h - bev), n)
    const m = unit(add(n, UP))
    quad(p, W(a[0], a[1], h - bev), W(b[0], b[1], h - bev), W(inner[j][0], inner[j][1], h), W(inner[i][0], inner[i][1], h), [n, n, m, m])
  }
  for (const [i, j, k] of earclip(inner)) tri(roof, W(inner[i][0], inner[i][1], h), W(inner[j][0], inner[j][1], h), W(inner[k][0], inner[k][1], h), [UP, UP, UP])
}
/** A horizontal reveal band standing just proud of a block's walls, light grey as painted. */
function band(footprint: XY[], z0: number, z1: number, edges?: number[]) {
  const q = ccw(footprint), outer = offset(q, 0.1)
  for (let i = 0; i < q.length; i++) {
    if (edges && !edges.includes(i)) continue
    const j = (i + 1) % q.length, a = outer[i], b = outer[j], c = q[i], d = q[j]
    const n = unit([b[1] - a[1], -(b[0] - a[0]), 0])
    quad(roof, W(a[0], a[1], z0), W(b[0], b[1], z0), W(b[0], b[1], z1), W(a[0], a[1], z1), n)
    quad(roof, W(c[0], c[1], z1), W(d[0], d[1], z1), W(b[0], b[1], z1), W(a[0], a[1], z1), UP)
    quad(roof, W(a[0], a[1], z0), W(b[0], b[1], z0), W(d[0], d[1], z0), W(c[0], c[1], z0), [0, 0, -1])
  }
}

// The show building: the coaster box, its south half taller (the 2008
// aerial from the east shows the step plainly).
const BOX_S = 28, BOX_N = 24
block([[-123.9, -1.5], [-68.4, -1.5], [-68.4, 27.5], [-123.9, 27.5]], BOX_S)
block([[-123.9, 27.5], [-68.4, 27.5], [-68.5, 56.6], [-121.7, 56.5], [-121.6, 28.0], [-123.9, 28.0]], BOX_N)

// The low connector between the show box and G-Force Records.
block([[-70.7, -10.6], [-19.3, -10.6], [-19.3, 9.7], [-54.7, 9.5], [-54.7, 15.0], [-62.3, 14.9],
  [-62.3, 10.0], [-68.3, 9.9], [-68.4, -1.5], [-70.7, -1.5]], 8)

// G-Force Records: the front block the guitar leans against, every jog of
// the OSM outline kept (the 4 m pier carrying the poster is one).
const FRONT_H = 11
const front: XY[] = [[-30.2, -26.4], [-26.2, -26.4], [-26.2, -29.7], [25.3, -29.8], [27.2, -28.4], [27.3, -26.8],
  [25.9, -25.2], [29.0, -25.3], [29.1, -18.3], [31.5, -18.4], [31.5, -13.6], [33.7, -13.6], [33.8, -9.6],
  [31.5, -9.6], [31.6, -5.4], [33.9, -5.4], [33.9, 6.6], [31.5, 6.6], [31.5, 8.8], [-13.2, 8.8], [-13.2, 9.7],
  [-19.3, 9.7], [-19.3, -14.6], [-25.8, -14.6], [-25.8, -18.4], [-30.2, -18.4]]
block(front, FRONT_H)
// Its two pale reveal lines, on the faces guests see from the plaza.
for (const z of [4.2, 7.9]) band(front, z, z + 0.4, [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])

// Stepping down to the north (NAIP shadows): the strip behind the queue
// entrance, then the last block.
const strip: XY[] = [[-1.2, 8.8], [31.5, 8.8], [31.4, 13.7], [31.6, 22.6], [8.0, 22.6], [-1.3, 22.7]]
block(strip, 9)
band(strip, 7.9, 8.3, [1, 2])
block([[8.0, 22.6], [31.6, 22.6], [31.8, 31.0], [8.3, 31.4], [8.0, 27.1]], 7)

// The queue entrance north of the guitar: a back wall under a deep flat
// silver canopy reaching out to the OSM line.
block([[31.5, 6.6], [35.6, 6.6], [35.6, 13.65], [31.4, 13.7], [31.5, 8.8]], 5.8, { bev: 0.3 })
block([[35.6, 6.6], [43.0, 6.6], [43.1, 12.0], [40.0, 12.1], [40.1, 13.5], [35.6, 13.65]], 5.8, { bev: 0.3, z0: 4.9, walls: roof })

// The show poster on the 4 m pier south of the guitar.
quad(blue, W(33.86, -13.3, 3.4), W(33.86, -9.9, 3.4), W(33.86, -9.9, 9.2), W(33.86, -13.3, 9.2), [1, 0, 0])

// ---------------------------------------------------------------------------
// The planter and the piano keys the guitar stands on.

const PLANTER: XY[] = [[34.0, -4.0], [36.1, -4.0], [38.7, -4.8], [40.4, -4.9], [41.7, -4.6], [43.2, -3.3],
  [44.4, -2.2], [44.7, -0.1], [44.9, 1.9], [44.3, 3.0], [43.4, 4.2], [42.9, 5.0], [43.0, 6.6], [34.0, 6.6]]
block(PLANTER, 0.9, { bev: 0.15, walls: roof })

const KEY_C: XY = [39.6, 0.8]
{
  // White keys fanned round the guitar's foot, black keys in twos and threes.
  const a0 = -88, a1 = 88, n = 12, step = (a1 - a0) / n
  const key = (p: Part, aa: number, ab: number, r0: number, r1: number, z0: number, z1: number) => {
    const at = (a: number, r: number, z: number) => {
      const t = (a * Math.PI) / 180
      return W(KEY_C[0] + r * Math.cos(t), KEY_C[1] + r * Math.sin(t), z)
    }
    const ring = [at(aa, r0, 0), at(ab, r0, 0), at(ab, r1, 0), at(aa, r1, 0)]
    const top = ring.map((v) => [v[0], v[1], z1] as V3), bot = ring.map((v) => [v[0], v[1], z0] as V3)
    for (let i = 0; i < 4; i++) {
      const j = (i + 1) % 4
      const n = unit(cross([top[j][0] - top[i][0], top[j][1] - top[i][1], 0], UP))
      quad(p, bot[i], bot[j], top[j], top[i], [-n[0], -n[1], 0])
    }
    quad(p, top[0], top[1], top[2], top[3], UP)
  }
  for (let i = 0; i < n; i++) key(trim, a0 + i * step + 0.6, a0 + (i + 1) * step - 0.6, 1.6, 5.0, 0.9, 1.25)
  const black = [1, 2, 4, 5, 6, 8, 9, 11]
  for (const i of black) key(wall, a0 + i * step - step * 0.3, a0 + i * step + step * 0.3, 2.6, 5.0, 1.0, 1.5)
}

// ---------------------------------------------------------------------------
// The guitar. Traced face-on from wwarby's photo in its own plane: a runs
// north along the front, b up the face, d out of it (east, tilted up). The
// face leans back 10° towards the building. 36 px/m puts the fretboard's end
// 40 ft above the keys, the published height.

const TILT = (10 * Math.PI) / 180
const GA: V3 = [0, 1, 0], GB: V3 = [-Math.sin(TILT), 0, Math.cos(TILT)], GN: V3 = [Math.cos(TILT), 0, Math.sin(TILT)]
const G0: V3 = [40.0, 0.3, 1.25] // foot of the guitar on the keys, in the outline's frame
const T = 2.0 // body thickness
const G = (a: number, b: number, d: number): V3 => {
  const p = add(add(add(G0, GA, a), GB, b), GN, d)
  return W(p[0], p[1], p[2])
}
const gdir = (a: number, b: number, d: number): V3 => unit(add(add(add([0, 0, 0], GA, a), GB, b), GN, d))
// Crop pixels of the traced photo (2× of the 1024 px original at 280,170) → plane metres.
const px = (cx: number, cy: number): XY => [(280 + cx / 2 - 640) / 36, (605 - (170 + cy / 2)) / 36]

const BODY: XY[] = ccw(([
  [150, 450], [175, 440], [215, 447], [270, 458], [330, 470], [390, 482], [440, 492], [500, 470], [555, 425],
  [565, 390], [560, 340], [552, 290], [552, 245], [565, 215], [590, 200], [615, 200], [638, 212], [650, 240],
  [660, 280], [675, 320], [700, 360], [735, 390], [790, 412], [860, 438], [940, 462], [1020, 488], [1080, 520],
  [1125, 560], [1152, 610], [1162, 670], [1155, 730], [1135, 780], [1100, 815], [1050, 838], [980, 852],
  [900, 862], [820, 868], [740, 868], [670, 860], [610, 845], [550, 815], [480, 770], [410, 722], [340, 672],
  [270, 620], [210, 568], [165, 515], [145, 478],
] as XY[]).map(([x, y]) => px(x, y)))

const PICKGUARD: XY[] = ccw(([
  [405, 495], [470, 470], [540, 440], [575, 415], [590, 330], [600, 265], [612, 238], [630, 250], [650, 300],
  [680, 350], [720, 385], [780, 420], [860, 455], [950, 490], [1030, 530], [1055, 560], [1050, 585], [1010, 600],
  [940, 615], [880, 640], [840, 700], [800, 760], [760, 815], [730, 835], [690, 830], [640, 790], [580, 720],
  [510, 640], [450, 570], [415, 525],
] as XY[]).map(([x, y]) => px(x, y)))

// Body: back face, a rounded rim, front face. The rim and the front edge
// carry the Electric Mayhem blue; the back stays red.
smooth(red, (m) => {
  const R = 0.45
  const prof: { o: number; d: number; n: XY }[] = [
    { o: -R, d: -T / 2, n: [0, -1] }, { o: -R * 0.29, d: -T / 2 + R * 0.29, n: [0.71, -0.71] }, { o: 0, d: -T / 2 + R, n: [1, 0] },
  ]
  const rings = prof.map((s) => offset(BODY, s.o))
  for (let k = 0; k < prof.length - 1; k++)
    for (let i = 0; i < BODY.length; i++) {
      const j = (i + 1) % BODY.length
      const p = (r: XY[], s: typeof prof[0], ii: number) => G(r[ii][0], r[ii][1], s.d)
      const nn = (s: typeof prof[0], ii: number) => {
        const q = BODY, a = q[(ii - 1 + q.length) % q.length], b = q[(ii + 1) % q.length]
        const t = unit([b[0] - a[0], b[1] - a[1], 0]), out: XY = [t[1], -t[0]]
        return gdir(out[0] * s.n[0], out[1] * s.n[0], s.n[1])
      }
      quad(m, p(rings[k], prof[k], i), p(rings[k], prof[k], j), p(rings[k + 1], prof[k + 1], j), p(rings[k + 1], prof[k + 1], i),
        [nn(prof[k], i), nn(prof[k], j), nn(prof[k + 1], j), nn(prof[k + 1], i)])
    }
  const back = rings[0], nb = gdir(0, 0, -1)
  for (const [i, j, k] of earclip(back)) tri(m, G(back[i][0], back[i][1], -T / 2), G(back[j][0], back[j][1], -T / 2), G(back[k][0], back[k][1], -T / 2), [nb, nb, nb])
}, 60)
smooth(blue, (m) => {
  const R = 0.45
  const prof: { o: number; d: number; n: XY }[] = [
    { o: 0, d: -T / 2 + R, n: [1, 0] }, { o: 0, d: T / 2 - R, n: [1, 0] },
    { o: -R * 0.29, d: T / 2 - R * 0.29, n: [0.71, 0.71] }, { o: -R, d: T / 2, n: [0, 1] },
  ]
  const rings = prof.map((s) => offset(BODY, s.o))
  for (let k = 0; k < prof.length - 1; k++)
    for (let i = 0; i < BODY.length; i++) {
      const j = (i + 1) % BODY.length
      const p = (r: XY[], s: typeof prof[0], ii: number) => G(r[ii][0], r[ii][1], s.d)
      const nn = (s: typeof prof[0], ii: number) => {
        const q = BODY, a = q[(ii - 1 + q.length) % q.length], b = q[(ii + 1) % q.length]
        const t = unit([b[0] - a[0], b[1] - a[1], 0]), out: XY = [t[1], -t[0]]
        return gdir(out[0] * s.n[0], out[1] * s.n[0], s.n[1])
      }
      quad(m, p(rings[k], prof[k], i), p(rings[k], prof[k], j), p(rings[k + 1], prof[k + 1], j), p(rings[k + 1], prof[k + 1], i),
        [nn(prof[k], i), nn(prof[k], j), nn(prof[k + 1], j), nn(prof[k + 1], i)])
    }
}, 60)
{
  // Front face: the red core, then the blue and pink bands laid just proud of
  // it round the edge, as the re-theme paints them.
  const face = offset(BODY, -0.45), nf = gdir(0, 0, 1)
  for (const [i, j, k] of earclip(face)) tri(red, G(face[i][0], face[i][1], T / 2), G(face[j][0], face[j][1], T / 2), G(face[k][0], face[k][1], T / 2), [nf, nf, nf])
  const ringBand = (p: (i: number) => Part, o0: number, o1: number, d: number) => {
    const r0 = offset(BODY, o0), r1 = offset(BODY, o1)
    for (let i = 0; i < BODY.length; i++) {
      const j = (i + 1) % BODY.length
      quad(p(i), G(r0[i][0], r0[i][1], d), G(r0[j][0], r0[j][1], d), G(r1[j][0], r1[j][1], d), G(r1[i][0], r1[i][1], d), nf)
    }
  }
  // Blue outside pink round the treble horn and the lower bout; the bass
  // horn on the left runs the other way, pink at its edge.
  const left = (i: number) => BODY[i][0] < -2.5
  ringBand((i) => (left(i) ? pink : blue), -0.45, -0.72, T / 2 + 0.03)
  ringBand((i) => (left(i) ? blue : pink), -0.72, -1.0, T / 2 + 0.05)
}

// Pickguard: a white plate standing proud of the face.
const PG_D = T / 2 + 0.14
{
  const q = PICKGUARD, nf = gdir(0, 0, 1)
  for (let i = 0; i < q.length; i++) {
    const j = (i + 1) % q.length
    const t = unit([q[j][0] - q[i][0], q[j][1] - q[i][1], 0]), n = gdir(t[1], -t[0], 0)
    quad(trim, G(q[i][0], q[i][1], T / 2), G(q[j][0], q[j][1], T / 2), G(q[j][0], q[j][1], PG_D), G(q[i][0], q[i][1], PG_D), n)
  }
  for (const [i, j, k] of earclip(q)) tri(trim, G(q[i][0], q[i][1], PG_D), G(q[j][0], q[j][1], PG_D), G(q[k][0], q[k][1], PG_D), [nf, nf, nf])
}

/** A box in the guitar plane: centre (a, b), long axis `u`, half-sizes, depth d0–d1. */
function gbox(p: Part, c: XY, u: XY, hl: number, hw: number, d0: number, d1: number) {
  const v: XY = [-u[1], u[0]]
  const corner = (s: number, t: number, d: number) => G(c[0] + u[0] * s * hl + v[0] * t * hw, c[1] + u[1] * s * hl + v[1] * t * hw, d)
  const ring = (d: number) => [corner(-1, -1, d), corner(1, -1, d), corner(1, 1, d), corner(-1, 1, d)]
  const lo = ring(d0), hi = ring(d1)
  const sides = [gdir(-u[1] * 0 + -v[0], -v[1], 0), gdir(u[0], u[1], 0), gdir(v[0], v[1], 0), gdir(-u[0], -u[1], 0)]
  // side i runs from corner i to i+1: (-1,-1)→(1,-1) faces −v, (1,-1)→(1,1) faces +u, …
  sides[0] = gdir(-v[0], -v[1], 0)
  for (let i = 0; i < 4; i++) { const j = (i + 1) % 4; quad(p, lo[i], lo[j], hi[j], hi[i], sides[i]) }
  quad(p, hi[0], hi[1], hi[2], hi[3], gdir(0, 0, 1))
}

// Strings and neck run from the bridge to the nut (the fretboard's end).
const BRIDGE = px(845, 748), NUT = px(70, 60)
const AX: XY = (() => { const d: XY = [NUT[0] - BRIDGE[0], NUT[1] - BRIDGE[1]]; const l = Math.hypot(d[0], d[1]); return [d[0] / l, d[1] / l] })()
const PERP: XY = [-AX[1], AX[0]]
const NECK_LEN = 7.9, NECK_W = 0.68, FB_D = T / 2 + 0.5, STR_D = FB_D + 0.18
{
  const mid: XY = [NUT[0] - AX[0] * NECK_LEN / 2, NUT[1] - AX[1] * NECK_LEN / 2]
  gbox(wall, mid, AX, NECK_LEN / 2, NECK_W, 0.2, FB_D)
  // Frets at true guitar spacing from the nut; the scale fills the fretboard
  // by the 21st fret, the first 15 drawn.
  const L = NECK_LEN / (1 - Math.pow(2, -21 / 12))
  for (let n = 1; n <= 15; n++) {
    const s = L * (1 - Math.pow(2, -n / 12))
    gbox(roof, [NUT[0] - AX[0] * s, NUT[1] - AX[1] * s], AX, 0.09, NECK_W, FB_D - 0.02, FB_D + 0.06)
  }
}
// Bridge, the three knobs, the jack plate and the whammy bar.
gbox(roof, BRIDGE, PERP, 1.25, 0.42, T / 2, STR_D + 0.05)
for (const [x, y] of [[855, 605], [925, 575], [995, 565]] as XY[]) {
  const c = px(x, y)
  smooth(trim, (m) => {
    const n = 12, r = 0.36, d0 = PG_D, d1 = PG_D + 0.45
    for (let i = 0; i < n; i++) {
      const t0 = (i / n) * 2 * Math.PI, t1 = ((i + 1) / n) * 2 * Math.PI
      const P = (t: number, d: number) => G(c[0] + r * Math.cos(t), c[1] + r * Math.sin(t), d)
      quad(m, P(t0, d0), P(t1, d0), P(t1, d1), P(t0, d1), [gdir(Math.cos(t0), Math.sin(t0), 0), gdir(Math.cos(t1), Math.sin(t1), 0), gdir(Math.cos(t1), Math.sin(t1), 0), gdir(Math.cos(t0), Math.sin(t0), 0)])
      const nf = gdir(0, 0, 1)
      tri(m, G(c[0], c[1], d1), P(t0, d1), P(t1, d1), [nf, nf, nf])
    }
  }, 50)
}
{
  const c = px(1045, 660), n = 14, nf = gdir(0, 0, 1)
  const P = (t: number, d: number) => G(c[0] + 1.05 * Math.cos(t), c[1] + 0.45 * Math.sin(t), d)
  for (let i = 0; i < n; i++) {
    const t0 = (i / n) * 2 * Math.PI, t1 = ((i + 1) / n) * 2 * Math.PI
    quad(roof, P(t0, T / 2), P(t1, T / 2), P(t1, T / 2 + 0.1), P(t0, T / 2 + 0.1), gdir(Math.cos(t0), Math.sin(t0), 0))
    tri(roof, G(c[0], c[1], T / 2 + 0.1), P(t0, T / 2 + 0.1), P(t1, T / 2 + 0.1), [nf, nf, nf])
  }
}
{
  const a = px(900, 690), b = px(835, 405), d: XY = [b[0] - a[0], b[1] - a[1]], l = Math.hypot(d[0], d[1])
  gbox(roof, [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2], [d[0] / l, d[1] / l], l / 2, 0.09, STR_D, STR_D + 0.18)
}

// ---------------------------------------------------------------------------
// The strings: six flat strips from the bridge up the neck, then on past the
// nut in one sweep that curves back over the front, turns east and comes down
// onto the limo under the arch (path from the NAIP aerial, heights from the
// photos). Crossbars every few metres hold them, as on the real one.

const NUT_W = G(NUT[0], NUT[1], STR_D), BR_W = G(BRIDGE[0], BRIDGE[1], STR_D)
const TAN = gdir(AX[0], AX[1], 0)
const sweepCtrl: V3[] = [
  NUT_W,
  add(NUT_W, TAN, 2.6),
  W(36.6, -12.4, 16.4),
  W(33.6, -15.6, 17.0),
  W(31.8, -19.4, 16.4),
  W(32.2, -22.8, 14.7),
  W(34.4, -24.7, 12.2),
  W(37.4, -25.0, 9.4),
  W(40.0, -24.4, 7.2),
  W(42.6, -24.0, 6.35),
  W(46.0, -24.0, 6.25),
]
function catmull(ctrl: V3[], per: number): V3[] {
  const pts: V3[] = []
  const P = (i: number) => ctrl[Math.max(0, Math.min(ctrl.length - 1, i))]
  for (let i = 0; i < ctrl.length - 1; i++) {
    const p0 = i === 0 ? add(ctrl[0], TAN, -2.6) : P(i - 1), p1 = P(i), p2 = P(i + 1), p3 = P(i + 2)
    for (let k = 0; k < per; k++) {
      const t = k / per, t2 = t * t, t3 = t2 * t
      pts.push([0, 1, 2].map((c) => 0.5 * (2 * p1[c] + (-p0[c] + p2[c]) * t + (2 * p0[c] - 5 * p1[c] + 4 * p2[c] - p3[c]) * t2 + (-p0[c] + 3 * p1[c] - 3 * p2[c] + p3[c]) * t3)) as V3)
    }
  }
  pts.push(ctrl[ctrl.length - 1])
  return pts
}
{
  const path = [BR_W, ...catmull(sweepCtrl, 3)]
  // Frames by parallel transport, starting square across the neck.
  let S = gdir(PERP[0], PERP[1], 0)
  const frames: V3[] = []
  for (let k = 0; k < path.length; k++) {
    const t = unit(k === 0 ? [path[1][0] - path[0][0], path[1][1] - path[0][1], path[1][2] - path[0][2]]
      : k === path.length - 1 ? [path[k][0] - path[k - 1][0], path[k][1] - path[k - 1][1], path[k][2] - path[k - 1][2]]
        : [path[k + 1][0] - path[k - 1][0], path[k + 1][1] - path[k - 1][1], path[k + 1][2] - path[k - 1][2]])
    S = unit(add(S, t, -dot(S, t)))
    frames.push(S)
  }
  const span = 0.52, w = 0.055
  for (let s = 0; s < 6; s++) {
    const o = -span + (2 * span * s) / 5
    for (let k = 0; k < path.length - 1; k++) {
      const a = path[k], b = path[k + 1], fa = frames[k], fb = frames[k + 1]
      sheet(roof, add(a, fa, o - w), add(b, fb, o - w), add(b, fb, o + w), add(a, fa, o + w))
    }
  }
  // Crossbars beyond the nut, about every 2.4 m.
  let run = 0
  for (let k = 2; k < path.length - 1; k++) {
    run += len([path[k][0] - path[k - 1][0], path[k][1] - path[k - 1][1], path[k][2] - path[k - 1][2]])
    if (run < 2.4) continue
    run = 0
    const c = path[k], f = frames[k]
    const t = unit([path[k + 1][0] - path[k - 1][0], path[k + 1][1] - path[k - 1][1], path[k + 1][2] - path[k - 1][2]])
    const nb = unit(cross(t, f))
    const corner = (u: number, v: number, z: number) => add(add(add(c, f, u), t, v), nb, z)
    const hw = span + 0.12, ht = 0.1, hz = 0.12
    const lo = [corner(-hw, -ht, -hz - 0.04), corner(hw, -ht, -hz - 0.04), corner(hw, ht, -hz - 0.04), corner(-hw, ht, -hz - 0.04)]
    const hi = [corner(-hw, -ht, -0.04), corner(hw, -ht, -0.04), corner(hw, ht, -0.04), corner(-hw, ht, -0.04)]
    for (let i = 0; i < 4; i++) { const j = (i + 1) % 4; sheet(wall, lo[i], lo[j], hi[j], hi[i]) }
    sheet(wall, lo[0], lo[1], lo[2], lo[3])
  }
}

// ---------------------------------------------------------------------------
// The entrance arch, G-Force Records' gateway, with the upside-down limo
// hanging from the strings under it. Spans north–south across the way in.

{
  const x0 = 42.7, x1 = 44.3, yS = -29.6, yN = -18.4, pw = 2.3, H = 9.8
  block([[x0 - 0.15, yS], [x1 + 0.15, yS], [x1 + 0.15, yS + pw], [x0 - 0.15, yS + pw]], H, { walls: trim, bev: 0.3 })
  block([[x0 - 0.15, yN - pw], [x1 + 0.15, yN - pw], [x1 + 0.15, yN], [x0 - 0.15, yN]], H, { walls: trim, bev: 0.3 })
  // The segmental span: soffit and top both rise gently to the middle.
  const n = 10, ya = yS + pw, yb = yN - pw
  const soffit = (t: number) => 6.7 + 0.8 * Math.sin(Math.PI * t), top = (t: number) => 9.0 + 0.5 * Math.sin(Math.PI * t)
  for (let i = 0; i < n; i++) {
    const t0 = i / n, t1 = (i + 1) / n, y0 = ya + (yb - ya) * t0, y1 = ya + (yb - ya) * t1
    for (const [x, nx] of [[x1, 1], [x0, -1]] as [number, number][])
      quad(trim, W(x, y0, soffit(t0)), W(x, y1, soffit(t1)), W(x, y1, top(t1)), W(x, y0, top(t0)), [nx, 0, 0])
    quad(trim, W(x0, y0, top(t0)), W(x1, y0, top(t0)), W(x1, y1, top(t1)), W(x0, y1, top(t1)), UP)
    quad(trim, W(x0, y0, soffit(t0)), W(x1, y0, soffit(t0)), W(x1, y1, soffit(t1)), W(x0, y1, soffit(t1)), [0, 0, -1])
  }
  // The limo, wheels up: body, then its cabin hanging below.
  smooth(blue, (m) => {
    const box = (xa: number, xb: number, ya: number, yb: number, za: number, zb: number) => {
      const c = [W(xa, ya, za), W(xb, ya, za), W(xb, yb, za), W(xa, yb, za)], d = c.map((v) => [v[0], v[1], v[2] + zb - za] as V3)
      const ns: V3[] = [[0, -1, 0], [1, 0, 0], [0, 1, 0], [-1, 0, 0]]
      for (let i = 0; i < 4; i++) { const j = (i + 1) % 4; quad(m, c[i], c[j], d[j], d[i], ns[i]) }
      quad(m, d[0], d[1], d[2], d[3], UP); quad(m, c[0], c[1], c[2], c[3], [0, 0, -1])
    }
    box(39.6, 46.4, -25.1, -22.9, 4.8, 5.9)
    box(41.2, 44.6, -24.85, -23.15, 4.1, 4.8)
  }, 30)
  // Its wheels in the air, which is what says "upside down".
  for (const [x, y] of [[40.6, -25.15], [40.6, -22.85], [45.3, -25.15], [45.3, -22.85]] as XY[]) {
    const n = 8, r = 0.42, zc = 5.9
    for (let i = 0; i < n; i++) {
      const t0 = (i / n) * 2 * Math.PI, t1 = ((i + 1) / n) * 2 * Math.PI
      const P = (t: number, dy: number) => W(x + r * Math.cos(t), y + dy, zc + r * Math.sin(t))
      quad(wall, P(t0, -0.2), P(t1, -0.2), P(t1, 0.2), P(t0, 0.2), [Math.cos((t0 + t1) / 2), 0, Math.sin((t0 + t1) / 2)])
      for (const dy of [-0.2, 0.2]) tri(wall, W(x, y + dy, zc), P(t0, dy), P(t1, dy), [[0, Math.sign(dy), 0], [0, Math.sign(dy), 0], [0, Math.sign(dy), 0]])
    }
  }
}

// ---------------------------------------------------------------------------

// Materials: the building's new dark grey, pulled lighter as STYLE asks for a
// large dark surface but kept clearly grey, since it is the re-theme's look;
// it also draws the fretboard and the black keys. Roofs, strings and the
// guitar's chrome share `roof`; the pickguard, white keys, reveal lines and
// the arch share `trim`. The guitar keeps its saturated red and its two band
// colours: it is the sign, and its colours are what it is.
const parts = [
  { part: wall, material: finish('rnrc-grey', 0x737881) },
  { part: roof, material: PALETTE.roof },
  { part: trim, material: PALETTE.trim },
  { part: red, material: finish('guitar-red', 0xc8343c, 0.5) },
  { part: blue, material: finish('guitar-blue', 0x3f9bd8, 0.5) },
  { part: pink, material: finish('guitar-pink', 0xd06cc6, 0.5) },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb("Rock 'n' Roller Coaster Starring The Muppets", parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 0, elevation: 0, height: BOX_S,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/wdw-rock-n-roller-coaster.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
