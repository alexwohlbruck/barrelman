/**
 * Truist Field (Charlotte Knights) — procedural, CC0-1.0, no textures.
 * bun scripts/landmarks/truist-field.ts
 *
 * Map frame: x across the park (first-base side +x), y from home plate toward
 * centre field, z up, metres. Placed at bearing 83.2°, the line from home
 * plate to second base in OSM (nodes 9602382279 → 9602382281), so the
 * outfield and the Uptown skyline beyond it lie along +y. The anchor is the
 * centroid of the stadium outline (relation/3417038, outer way/255166737).
 *
 * The park is mapped in detail as building:parts, and the model is built from
 * them rather than redrawn: each part's polygon is extruded to its OSM height
 * in brick and tan precast as the photos show, the seating ring
 * (way/1553955043) becomes a raked bowl of green seats rising away from the
 * field, and the tan grandstand (way/1553955036) carries a recessed band of
 * suite windows and a flat canopy roof reaching out over the seats. The field
 * is flat colour layers from the OSM pitch, grass and infield polygons, a
 * few decimetres up so they sit over the map's own pitch fill. Six chunky
 * light towers stand round the bowl. The outfield side stays low, as it is,
 * so the skyline view over centre field is what the shape says.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
// OSM geometry in the model frame (metres; +y from home plate toward centre field).
// Converted from OSM by projecting about the anchor and rotating by the bearing.
const OSM: Record<string, XY[]> = {
  // way/255166737
  '255166737': [[-3.1,-105.0],[-5.0,-102.1],[-6.4,-102.5],[-7.0,-102.7],[-7.6,-102.7],[-9.0,-102.7],[-10.5,-102.2],[-11.1,-102.0],[-11.6,-101.7],[-12.9,-100.6],[-15.7,-102.2],[-25.4,-87.4],[-20.7,-84.4],[-24.5,-78.4],[-27.9,-77.6],[-30.8,-79.3],[-35.1,-72.8],[-33.9,-70.4],[-43.8,-60.1],[-71.3,-25.1],[-73.1,-26.6],[-80.3,-17.9],[-75.0,-13.5],[-70.7,-18.8],[-66.8,-15.5],[-80.2,0.8],[-93.1,34.2],[-91.7,36.4],[-95.5,38.6],[-91.1,46.0],[-87.2,43.7],[-81.1,47.9],[-86.3,54.8],[-70.2,67.0],[-66.6,62.3],[-29.0,90.9],[-26.8,88.2],[-22.7,89.2],[-9.7,92.0],[1.7,89.8],[4.9,89.1],[5.5,91.5],[20.2,88.6],[25.2,81.6],[28.8,83.8],[47.1,56.3],[48.6,57.3],[49.9,58.2],[66.6,32.9],[74.4,21.0],[92.9,-7.1],[80.6,-26.1],[86.2,-29.7],[99.2,-38.0]],
  // way/255166738 leisure=pitch
  '255166738': [[58.8,32.7],[38.4,13.0],[35.9,3.1],[17.0,-20.4],[19.7,-22.6],[17.9,-24.7],[16.7,-23.6],[2.9,-40.9],[4.2,-41.9],[8.6,-45.2],[6.3,-48.0],[4.7,-46.6],[2.5,-49.1],[-1.2,-51.8],[-4.5,-53.1],[-6.9,-46.2],[-7.6,-46.5],[-9.9,-46.6],[-12.2,-46.5],[-14.3,-46.1],[-16.1,-45.3],[-18.6,-43.6],[-20.7,-41.6],[-29.2,-47.8],[-30.6,-46.2],[-26.3,-42.6],[-27.0,-41.7],[-25.9,-40.7],[-39.7,-23.8],[-40.9,-24.7],[-42.8,-22.2],[-40.2,-19.9],[-42.7,-16.8],[-43.3,-17.3],[-55.8,-2.0],[-54.9,-1.3],[-64.5,10.7],[-67.6,19.4],[-83.6,35.1],[-81.7,38.1],[-76.6,41.8],[-60.5,54.1],[-55.6,58.0],[-56.7,61.3],[-23.7,86.0],[-10.0,88.7],[21.8,82.6],[47.2,44.2],[51.8,43.6]],
  // way/1216128113 area=yes
  '1216128113': [[-7.0,-90.7],[-8.8,-90.7],[-10.5,-91.2],[-11.9,-92.3],[-12.9,-93.7],[-13.5,-95.5],[-13.4,-97.4],[-12.7,-99.1],[-11.5,-100.6],[-11.1,-100.8],[-10.2,-101.3],[-9.8,-101.5],[-7.9,-101.8],[-7.8,-101.8],[-6.7,-101.6],[-6.0,-101.4],[-4.4,-100.5],[-3.2,-99.1],[-2.6,-97.4],[-2.5,-95.7],[-2.9,-93.9],[-3.9,-92.4],[-5.3,-91.3]],
  // way/1216128114 area=yes
  '1216128114': [[-11.4,-90.5],[-10.3,-90.0],[-9.1,-89.6],[-7.5,-89.6],[-6.2,-89.8],[-5.1,-90.2],[-4.6,-80.0],[-13.3,-80.3]],
  // way/1216128120 area=yes
  '1216128120': [[77.0,-23.7],[87.7,-7.8],[88.3,-6.8],[86.5,-4.1],[85.0,-5.3],[76.9,-10.7],[71.5,-7.1],[65.0,-3.0],[65.5,-0.8],[64.4,-0.5],[59.2,0.9],[57.3,-6.9],[61.8,-10.7],[51.0,-24.2],[53.6,-26.3],[58.3,-23.2],[62.5,-29.6],[73.8,-21.7]],
  // way/1553955046
  '1553955046': [[-12.9,-100.6],[-11.6,-101.7],[-11.1,-102.0],[-10.5,-102.2],[-9.0,-102.7],[-7.6,-102.7],[-7.0,-102.7],[-6.4,-102.5],[-5.0,-102.1],[-3.8,-101.3],[-2.5,-99.8],[-1.6,-97.9],[-1.5,-95.1],[-2.3,-92.8],[-3.6,-91.2],[-5.1,-90.2],[-6.2,-89.8],[-7.5,-89.6],[-9.1,-89.6],[-10.3,-90.0],[-11.4,-90.5],[-13.0,-91.8],[-14.1,-93.8],[-14.6,-96.0],[-14.2,-98.5]],
  // way/906204302
  '906204302': [[-12.9,-16.3],[-13.4,-15.5],[-13.5,-14.6],[-13.4,-13.7],[-13.0,-12.9],[-12.4,-12.3],[-11.7,-11.9],[-10.8,-11.7],[-9.9,-11.8],[-9.2,-12.2],[-8.5,-12.8],[-8.1,-13.7],[-8.0,-14.6],[-8.2,-15.6],[-8.7,-16.4],[-9.5,-16.9],[-10.3,-17.2],[-11.3,-17.2],[-12.2,-16.9]],
  // way/906204303
  '906204303': [[-6.9,-30.4],[-6.8,-30.7],[-6.5,-32.0],[-6.6,-33.3],[-7.0,-34.6],[-7.8,-35.7],[-8.9,-36.4],[-10.2,-36.9],[-11.5,-36.9],[-12.8,-36.5],[-13.9,-35.7],[-14.7,-34.7],[-15.2,-33.4],[-15.3,-32.4],[-15.2,-31.3],[-14.8,-30.3],[-38.5,-6.1],[-36.8,-1.4],[-34.0,3.3],[-30.3,7.2],[-26.1,10.7],[-21.1,13.1],[-16.5,14.5],[-10.2,15.1],[-3.4,14.2],[1.8,12.4],[5.9,10.0],[9.3,7.1],[12.6,3.1],[15.7,-2.2],[17.4,-6.1]],
  // way/906204304
  '906204304': [[-8.2,-28.8],[5.2,-15.5],[4.9,-15.2],[4.6,-14.7],[4.3,-14.1],[4.2,-13.3],[4.3,-12.5],[4.5,-11.7],[4.8,-10.9],[5.2,-10.5],[-8.9,3.5],[-9.5,3.2],[-10.3,2.9],[-11.0,2.9],[-11.7,3.0],[-12.2,3.1],[-12.7,3.5],[-26.6,-10.5],[-26.1,-11.6],[-25.9,-13.2],[-26.2,-14.6],[-26.7,-15.5],[-13.3,-28.7],[-12.4,-28.3],[-11.3,-28.0],[-10.3,-28.0],[-9.2,-28.3]],
  // way/906204305
  '906204305': [[-16.9,-39.2],[-36.3,-15.1],[-39.4,-9.0],[-57.8,14.2],[-60.7,23.0],[-71.5,34.0],[-72.0,34.9],[-72.3,36.0],[-72.2,37.3],[-71.8,38.1],[-71.0,39.3],[-50.5,55.0],[-48.0,60.8],[-21.6,81.2],[-9.6,83.4],[18.4,77.8],[47.5,33.9],[47.7,32.8],[47.4,31.3],[47.0,30.4],[32.9,16.3],[30.7,6.5],[13.1,-17.2],[-4.0,-38.2],[-5.4,-39.7],[-7.4,-40.8],[-10.2,-41.6],[-11.9,-41.6],[-14.1,-41.0],[-15.6,-40.5]],
  // way/1553955034 building:part=yes height=13
  '1553955034': [[53.6,-26.3],[49.6,-29.0],[55.1,-37.5],[63.9,-31.8],[62.5,-29.6],[58.3,-23.2]],
  // way/1553955035 building:part=yes height=8
  '1553955035': [[63.9,-31.8],[62.5,-29.6],[73.8,-21.7],[77.0,-23.7],[80.6,-26.1],[86.2,-29.7],[99.2,-38.0],[-3.1,-105.0],[-5.0,-102.1],[-3.8,-101.3],[-2.5,-99.8],[-1.6,-97.9],[-1.5,-95.1],[-2.3,-92.8],[-3.6,-91.2],[-5.1,-90.2],[-4.6,-80.0],[-13.3,-80.3],[-11.4,-90.5],[-13.0,-91.8],[-14.1,-93.8],[-14.6,-96.0],[-14.2,-98.5],[-12.9,-100.6],[-15.7,-102.2],[-25.4,-87.4],[-20.7,-84.4],[-24.5,-78.4],[-27.9,-77.6],[-23.8,-74.7],[-18.7,-76.5],[-2.6,-76.7],[0.4,-75.5],[8.1,-86.8],[34.8,-68.9],[27.9,-58.4],[56.6,-39.5],[55.1,-37.5]],
  // way/1553955036 building:part=yes height=12
  '1553955036': [[8.1,-86.8],[34.8,-68.9],[27.9,-58.4],[56.6,-39.5],[55.1,-37.5],[49.6,-29.0],[53.6,-26.3],[51.0,-24.2],[61.8,-10.7],[57.3,-6.9],[54.6,-4.5],[51.6,-1.9],[12.4,-50.2],[4.1,-57.4],[-5.3,-60.7],[-15.5,-60.7],[-25.4,-57.0],[-33.3,-49.9],[-63.3,-12.7],[-66.8,-15.5],[-70.7,-18.8],[-67.9,-22.3],[-71.3,-25.1],[-43.8,-60.1],[-33.9,-70.4],[-23.8,-74.7],[-18.7,-76.5],[-2.6,-76.7],[0.4,-75.5]],
  // way/1553955037 building:part=yes height=12 min_height=6
  '1553955037': [[-71.3,-25.1],[-73.1,-26.6],[-80.3,-17.9],[-75.0,-13.5],[-70.7,-18.8],[-67.9,-22.3]],
  // way/1553955038 building:part=yes height=5
  '1553955038': [[-91.7,36.4],[-90.7,35.8],[-86.4,43.2],[-87.2,43.7],[-91.1,46.0],[-95.5,38.6]],
  // way/1553955039 building:part=yes height=6
  '1553955039': [[-66.6,62.3],[-60.5,54.1],[-76.6,41.8],[-81.1,47.9],[-86.3,54.8],[-70.2,67.0]],
  // way/1553955040 building:part=yes height=11
  '1553955040': [[57.3,-6.9],[59.2,0.9],[64.4,-0.5],[65.2,3.3],[65.5,4.4],[68.6,17.9],[63.5,19.0],[66.4,32.0],[66.6,32.9],[49.9,58.2],[47.1,56.3],[63.1,32.1],[54.6,-4.5]],
  // way/1553955041 building:part=yes height=11
  '1553955041': [[71.5,-7.1],[72.0,-6.8],[81.8,-0.4],[85.0,-5.3],[76.9,-10.7]],
  // way/1553955042 building:part=yes height=13
  '1553955042': [[65.2,3.3],[65.5,4.4],[74.4,11.1],[81.5,1.1],[71.5,-6.1]],
  // way/1553955043 building:part=yes height=3
  '1553955043': [[58.8,32.7],[38.4,13.0],[35.9,3.1],[17.0,-20.4],[19.7,-22.6],[17.9,-24.7],[4.2,-41.9],[8.6,-45.2],[6.3,-48.0],[4.7,-46.6],[2.5,-49.1],[-1.2,-51.8],[-4.5,-53.1],[-6.9,-46.2],[-7.6,-46.5],[-9.9,-46.6],[-12.2,-46.5],[-14.3,-46.1],[-16.1,-45.3],[-18.6,-43.6],[-20.7,-41.6],[-29.2,-47.8],[-30.6,-46.2],[-26.3,-42.6],[-27.0,-41.7],[-40.9,-24.7],[-42.8,-22.2],[-40.2,-19.9],[-42.7,-16.8],[-43.3,-17.3],[-55.8,-2.0],[-54.9,-1.3],[-64.5,10.7],[-67.6,19.4],[-83.6,35.1],[-81.7,38.1],[-76.6,41.8],[-81.1,47.9],[-87.2,43.7],[-86.4,43.2],[-90.7,35.8],[-91.7,36.4],[-93.1,34.2],[-80.2,0.8],[-66.8,-15.5],[-63.3,-12.7],[-33.3,-49.9],[-25.4,-57.0],[-15.5,-60.7],[-5.3,-60.7],[4.1,-57.4],[12.4,-50.2],[51.6,-1.9],[54.6,-4.5],[63.1,32.1],[47.1,56.3],[28.8,83.8],[25.2,81.6],[20.2,88.6],[5.5,91.5],[4.9,89.1],[1.7,89.8],[1.6,89.2],[-9.7,91.5],[-22.6,88.6],[-22.7,89.2],[-26.8,88.2],[-29.0,90.9],[-66.6,62.3],[-60.5,54.1],[-55.6,58.0],[-56.7,61.3],[-23.7,86.0],[-10.0,88.7],[21.8,82.6],[47.2,44.2],[51.8,43.6]],
  // way/1553955044 building:part=yes
  '1553955044': [[-40.9,-24.7],[-27.0,-41.7],[-25.9,-40.7],[-39.7,-23.8]],
  // way/1553955045 building:part=yes
  '1553955045': [[4.2,-41.9],[2.9,-40.9],[16.7,-23.6],[17.9,-24.7]],
  // way/1553955047 building:part=yes height=13
  '1553955047': [[-23.8,-74.7],[-27.9,-77.6],[-30.8,-79.3],[-35.1,-72.8],[-33.9,-70.4]],
  // way/1553955048 building:part=yes height=11 min_height=9
  '1553955048': [[80.6,-26.1],[92.9,-7.1],[74.4,21.0],[73.9,20.6],[71.5,19.0],[86.5,-4.1],[88.3,-6.8],[87.7,-7.8],[77.0,-23.7]],
  // way/1553955049 building:part=yes height=3
  '1553955049': [[73.9,20.6],[66.4,32.0],[66.6,32.9],[74.4,21.0]],
  // way/1553955050 building:part=yes height=5
  '1553955050': [[72.0,-6.8],[65.8,-2.8],[66.2,-0.9],[65.5,-0.8],[65.0,-3.0],[71.5,-7.1]],
  // way/1553955051 building:part=yes height=6
  '1553955051': [[-73.1,-26.6],[-71.3,-25.1],[-67.9,-22.3],[-70.7,-18.8],[-75.0,-13.5],[-80.3,-17.9]],
  // way/1553955052 building:part=yes height=11
  '1553955052': [[1.7,89.8],[1.6,89.2],[-9.7,91.5],[-22.6,88.6],[-22.7,89.2],[-9.7,92.0]],
  // way/1553955053 building:part=yes height=7
  '1553955053': [[80.7,-21.1],[81.6,-21.7],[82.2,-20.8],[81.3,-20.2]],
  // way/1553955054 building:part=yes height=2
  '1553955054': [[51.0,-24.2],[53.6,-26.3],[58.3,-23.2],[62.5,-29.6],[73.8,-21.7],[77.0,-23.7],[80.6,-26.1],[86.2,-29.7],[95.4,-15.3],[92.4,-10.9],[90.7,-12.2],[87.7,-7.8],[88.3,-6.8],[86.5,-4.1],[85.0,-5.3],[76.9,-10.7],[71.5,-7.1],[65.0,-3.0],[65.5,-0.8],[64.4,-0.5],[59.2,0.9],[57.3,-6.9],[61.8,-10.7]],
  // way/1553955055 building:part=yes height=9 min_height=7
  '1553955055': [[80.9,-21.0],[81.6,-21.5],[82.0,-20.8],[81.3,-20.4]],
  // way/1553955056 building:part=yes height=9 min_height=7
  '1553955056': [[84.0,-16.3],[84.7,-16.8],[85.1,-16.1],[84.4,-15.7]],
  // way/1553955057 building:part=yes height=7
  '1553955057': [[83.8,-16.4],[84.8,-17.0],[85.3,-16.1],[84.4,-15.5]],
  // way/1553955058 building:part=yes height=9 min_height=7
  '1553955058': [[87.2,-11.5],[87.9,-12.0],[88.3,-11.3],[87.6,-10.8]],
  // way/1553955059 building:part=yes height=7
  '1553955059': [[87.0,-11.5],[87.9,-12.2],[88.5,-11.3],[87.5,-10.6]],
  // way/1553955060 building:part=yes height=9
  '1553955060': [[88.3,-3.4],[89.2,-2.9],[88.7,-2.0],[87.8,-2.6]],
  // way/1553955061 building:part=yes height=9
  '1553955061': [[84.8,2.0],[85.7,2.6],[85.1,3.4],[84.2,2.8]],
  // way/1553955062 building:part=yes height=9
  '1553955062': [[77.7,12.8],[78.6,13.4],[78.0,14.2],[77.2,13.6]],
  // way/1553955063 building:part=yes height=9
  '1553955063': [[81.2,7.4],[82.1,8.0],[81.6,8.8],[80.7,8.2]],
  // way/1553955064 building:part=yes height=9
  '1553955064': [[74.1,18.2],[75.0,18.8],[74.5,19.6],[73.6,19.1]],
  // way/1553955065 building:part=yes height=3
  '1553955065': [[-12.9,-100.6],[-11.6,-101.7],[-11.1,-102.0],[-10.5,-102.2],[-9.0,-102.7],[-7.6,-102.7],[-7.0,-102.7],[-6.4,-102.5],[-5.0,-102.1],[-3.8,-101.3],[-2.5,-99.8],[-1.6,-97.9],[-1.5,-95.1],[-2.3,-92.8],[-3.6,-91.2],[-5.1,-90.2],[-4.6,-80.0],[-13.3,-80.3],[-11.4,-90.5],[-13.0,-91.8],[-14.1,-93.8],[-14.6,-96.0],[-14.2,-98.5]],
  // way/1553955066 building:part=yes height=7
  '1553955066': [[-10.5,-102.2],[-10.2,-101.3],[-11.1,-100.8],[-11.6,-101.7],[-11.1,-102.0]],
  // way/1553955067 building:part=yes height=7
  '1553955067': [[-7.6,-102.7],[-7.0,-102.7],[-6.4,-102.5],[-6.7,-101.6],[-7.8,-101.8]],
}
const BASES = {home: [-11.1, -32.1], first: [8.3, -12.9], second: [-11.1, 6.7], third: [-30.0, -12.8], mound: [-10.7, -14.5]} as Record<string, XY>

// ---------------------------------------------------------------------------
// Materials. Colours from the daylight photos: the sunlit red brick of the
// Mint Street front, the tan precast of the upper walls, dark green seats.

const brick = new Part(), tan = new Part(), glass = new Part(), roof = new Part()
const seats = new Part(), concourse = new Part(), wallGreen = new Part()
const canopyTop = new Part(), grass = new Part(), dirt = new Part(), metal = new Part(), lamps = new Part(), board = new Part()

// ---------------------------------------------------------------------------
// 2D helpers

const area2 = (p: XY[]) => p.reduce((s, a, i) => { const b = p[(i + 1) % p.length]; return s + a[0] * b[1] - b[0] * a[1] }, 0) / 2
const ccw = (p: XY[]): XY[] => (area2(p) < 0 ? [...p].reverse() : p)
const centroid = (p: XY[]): XY => {
  let a = 0, x = 0, y = 0
  p.forEach((u, i) => { const v = p[(i + 1) % p.length], c = u[0] * v[1] - v[0] * u[1]; a += c; x += (u[0] + v[0]) * c; y += (u[1] + v[1]) * c })
  return [x / (3 * a), y / (3 * a)]
}
const unit2 = (v: XY): XY => { const l = Math.hypot(v[0], v[1]) || 1; return [v[0] / l, v[1] / l] }

/** Ear clipping for a simple counter-clockwise polygon; returns index triples. */
function triangulate(p: XY[]): [number, number, number][] {
  const idx = p.map((_, i) => i), out: [number, number, number][] = []
  const cr = (a: XY, b: XY, c: XY) => (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
  const inside = (q: XY, a: XY, b: XY, c: XY) => cr(a, b, q) > 1e-9 && cr(b, c, q) > 1e-9 && cr(c, a, q) > 1e-9
  let guard = 0
  while (idx.length > 3 && guard++ < 10000) {
    let cut = false
    for (let k = 0; k < idx.length; k++) {
      const i0 = idx[(k + idx.length - 1) % idx.length], i1 = idx[k], i2 = idx[(k + 1) % idx.length]
      const a = p[i0], b = p[i1], c = p[i2]
      if (cr(a, b, c) <= 1e-9) continue
      if (idx.some((j) => j !== i0 && j !== i1 && j !== i2 && inside(p[j], a, b, c))) continue
      out.push([i0, i1, i2]); idx.splice(k, 1); cut = true; break
    }
    if (!cut) { idx.splice(0, 1) } // degenerate sliver: drop a vertex rather than loop
  }
  if (idx.length === 3) out.push([idx[0], idx[1], idx[2]])
  return out
}

function distToRing(q: XY, ring: XY[]) {
  let best = Infinity, at: XY = ring[0]
  ring.forEach((a, i) => {
    const b = ring[(i + 1) % ring.length], d: XY = [b[0] - a[0], b[1] - a[1]]
    const t = Math.max(0, Math.min(1, ((q[0] - a[0]) * d[0] + (q[1] - a[1]) * d[1]) / (d[0] ** 2 + d[1] ** 2 || 1)))
    const c: XY = [a[0] + d[0] * t, a[1] + d[1] * t], l = Math.hypot(q[0] - c[0], q[1] - c[1])
    if (l < best) { best = l; at = c }
  })
  return { d: best, at }
}

/** Mitred inward offset of a counter-clockwise ring, for top bevels. */
function inset(p: XY[], d: number): XY[] {
  return p.map((v, i) => {
    const a = p[(i + p.length - 1) % p.length], b = p[(i + 1) % p.length]
    const n1 = unit2([v[1] - a[1], a[0] - v[0]]), n2 = unit2([b[1] - v[1], v[0] - b[0]]) // outward
    const m = unit2([n1[0] + n2[0], n1[1] + n2[1]])
    const k = d / Math.max(0.5, m[0] * n1[0] + m[1] * n1[1])
    return [v[0] - m[0] * k, v[1] - m[1] * k] as XY
  })
}

// ---------------------------------------------------------------------------
// 3D helpers

const v3 = (p: XY, z: number): V3 => [p[0], p[1], z]
const dot3 = (a: V3, b: V3) => a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
const crs = (a: V3, b: V3, c: V3): V3 => {
  const u = [b[0] - a[0], b[1] - a[1], b[2] - a[2]], w = [c[0] - a[0], c[1] - a[1], c[2] - a[2]]
  return [u[1] * w[2] - u[2] * w[1], u[2] * w[0] - u[0] * w[2], u[0] * w[1] - u[1] * w[0]]
}
/** A quad turned to face `want`; optional per-corner normals for soft edges. */
function face(p: Part, pts: V3[], want: V3, ns?: V3[]) {
  let P = pts, N = ns
  if (dot3(crs(P[0], P[1], P[2]), want) < 0 || (P.length === 4 && dot3(crs(P[0], P[2], P[3]), want) < 0)) {
    P = [...P].reverse(); N = N && [...N].reverse()
  }
  p.tri(P[0], P[1], P[2], undefined, undefined, undefined, N && [N[0], N[1], N[2]])
  if (P.length === 4) p.tri(P[0], P[2], P[3], undefined, undefined, undefined, N && [N[0], N[2], N[3]])
}
const UP: V3 = [0, 0, 1], DOWN: V3 = [0, 0, -1]

function cap(p: Part, ring: XY[], z: number | ((i: number) => number), up = true, tris = triangulate(ring)) {
  const Z = typeof z === 'number' ? () => z : z
  for (const [a, b, c] of tris) {
    const A = v3(ring[a], Z(a)), B = v3(ring[b], Z(b)), C = v3(ring[c], Z(c))
    if (up) p.tri(A, B, C); else p.tri(A, C, B)
  }
}

type Band = { z0: number; z1: number; mat: Part; windows?: { glass: Part; bay?: number; pier?: number; depth?: number } }

/** One edge's wall band, plain or as recessed window bays between piers. */
function wallBand(a: XY, b: XY, band: Band) {
  const L = Math.hypot(b[0] - a[0], b[1] - a[1])
  const u = unit2([b[0] - a[0], b[1] - a[1]]), n: V3 = [u[1], -u[0], 0]
  const at = (s: number, z: number, d = 0): V3 => [a[0] + u[0] * s - n[0] * d, a[1] + u[1] * s - n[1] * d, z]
  const { z0, z1, mat, windows: w } = band
  const pier = w?.pier ?? 1.2, bay = w?.bay ?? 7, D = w?.depth ?? 0.55
  if (!w || L < pier + 3) { face(mat, [at(0, z0), at(L, z0), at(L, z1), at(0, z1)], n); return }
  const nb = Math.max(1, Math.round((L - pier) / bay)), step = (L - pier) / nb
  for (let k = 0; k <= nb; k++) {
    const s = k * step
    face(mat, [at(s, z0), at(s + pier, z0), at(s + pier, z1), at(s, z1)], n)
    if (k === nb) break
    const s0 = s + pier, s1 = (k + 1) * step
    face(w.glass, [at(s0, z0, D), at(s1, z0, D), at(s1, z1, D), at(s0, z1, D)], n)
    face(mat, [at(s0, z0), at(s0, z0, D), at(s0, z1, D), at(s0, z1)], [u[0], u[1], 0])
    face(mat, [at(s1, z0), at(s1, z0, D), at(s1, z1, D), at(s1, z1)], [-u[0], -u[1], 0])
    face(mat, [at(s0, z0), at(s1, z0), at(s1, z0, D), at(s0, z0, D)], UP)
    face(mat, [at(s0, z1), at(s1, z1), at(s1, z1, D), at(s0, z1, D)], DOWN)
  }
}

/**
 * Extrude a footprint: wall bands bottom to top, then a soft bevel rolling
 * from the wall face onto a flat roof.
 */
function extrude(ringIn: XY[], bands: Band[], top: Part | null, bevel = 0.4, edgeFilter?: (i: number) => boolean) {
  const ring = ccw(ringIn)
  const h = bands[bands.length - 1].z1
  ring.forEach((a, i) => {
    if (edgeFilter && !edgeFilter(i)) return
    const b = ring[(i + 1) % ring.length]
    for (const band of bands) {
      const z1 = band.z1 === h ? h - bevel : band.z1
      if (z1 > band.z0 + 1e-3) wallBand(a, b, { ...band, z1 })
    }
  })
  if (!top) return
  const inn = inset(ring, bevel), last = bands[bands.length - 1].mat
  ring.forEach((a, i) => {
    const j = (i + 1) % ring.length, b = ring[j]
    const u = unit2([b[0] - a[0], b[1] - a[1]]), n: V3 = [u[1], -u[0], 0]
    const s: V3 = [n[0] * 0.5, n[1] * 0.5, 0.85]
    face(last, [v3(a, h - bevel), v3(b, h - bevel), v3(inn[j], h), v3(inn[i], h)], s, [n, n, s, s].map((x) => x) as V3[])
  })
  cap(top, inn, h, true, triangulate(ring))
}

/** A flat slab between two heights (canopies), bevelled on top. */
function slab(ringIn: XY[], z0: number, z1: number, side: Part, top: Part, bevel = 0.3) {
  const ring = ccw(ringIn)
  extrude(ring, [{ z0, z1, mat: side }], top, bevel)
  cap(side, ring, z0, false)
}

// ---------------------------------------------------------------------------
// The field: flat colour layers, stacked a decimetre apart so they never
// fight, over the OSM pitch.

const pitch = ccw(OSM['255166738'])
cap(dirt, pitch, 0.35)                      // warning track and foul ground
cap(grass, ccw(OSM['906204305']), 0.45)     // outfield and foul grass
cap(dirt, ccw(OSM['906204303']), 0.55)      // the infield skin and home circle
cap(grass, ccw(OSM['906204304']), 0.65)     // infield grass
cap(dirt, ccw(OSM['906204302']), 0.75)      // the mound

// ---------------------------------------------------------------------------
// The seating bowl: way/1553955043 rings the field as one C-shaped polygon.
// Each vertex is raised by its distance from the field, so the triangles
// between the front row and the back form the rake. The grandstand round the
// infield rises to the concourse at 6.6 m; the outfield seats stay low.

const FRONT = 1.6                           // top of the field wall
const bowl = ccw(OSM['1553955043'])
const fromField = bowl.map((p) => distToRing(p, pitch).d)
const rise = (p: XY) => { const t = Math.max(0, Math.min(1, (p[1] - 0) / 40)); return 5.0 * (1 - t) + 2.6 * t }
// Front-row corners sit on the field wall; the back row is at full height,
// so every triangle spanning the band is seating and only the deep corners
// behind it read as concourse.
const rake = (d: number) => Math.max(0, Math.min(1, (d - 1) / 6))
const bowlZ = bowl.map((p, i) => FRONT + rise(p) * rake(fromField[i]))
const atTop = bowl.map((_, i) => rake(fromField[i]) >= 1)
for (const [a, b, c] of triangulate(bowl)) {
  const P = [a, b, c].map((i) => v3(bowl[i], bowlZ[i]))
  const flat = atTop[a] && atTop[b] && atTop[c]
  ;(flat ? concourse : seats).tri(P[0], P[1], P[2])
}
bowl.forEach((a, i) => {
  const j = (i + 1) % bowl.length, b = bowl[j]
  const front = fromField[i] < 2 && fromField[j] < 2
  const u = unit2([b[0] - a[0], b[1] - a[1]]), n: V3 = [u[1], -u[0], 0]
  face(front ? wallGreen : brick, [v3(a, 0), v3(b, 0), v3(b, bowlZ[j]), v3(a, bowlZ[i])], n)
})

// Dugouts: low flat roofs at the field wall.
for (const id of ['1553955044', '1553955045'])
  extrude(OSM[id], [{ z0: 0, z1: FRONT + 0.2, mat: wallGreen }], roof, 0.2)

// ---------------------------------------------------------------------------
// The grandstand: brick concourse base, tan suite level with its band of
// windows, and a flat canopy roof reaching out over the seats.

const stand = ccw(OSM['1553955036'])
extrude(stand, [
  { z0: 0, z1: 6.2, mat: brick },
  { z0: 6.2, z1: 7.0, mat: tan },
  { z0: 7.0, z1: 10.6, mat: tan, windows: { glass, bay: 6.5, pier: 1.1, depth: 0.6 } },
  { z0: 10.6, z1: 12, mat: tan },
], null)
{
  // The canopy: the stand's own outline with its field-side corners carried
  // 3.5 m out over the seats, towards the nearest point of the field.
  const nearBowl = stand.map((p) => distToRing(p, bowl).d < 2.5)
  const canopy = stand.map((p, i): XY => {
    if (!nearBowl[i]) return p
    const { at } = distToRing(p, pitch), d = unit2([at[0] - p[0], at[1] - p[1]])
    return [p[0] + d[0] * 3.5, p[1] + d[1] * 3.5]
  })
  slab(canopy, 12, 13.1, canopyTop, canopyTop, 0.35) // white fascia, as in the photos
}

// ---------------------------------------------------------------------------
// The rest of the parts, at their OSM heights.

const BR = (h: number, cream = 0.7): Band[] => [{ z0: 0, z1: h - cream, mat: brick }, { z0: h - cream, z1: h, mat: tan }]
const stack: [string, Band[]][] = [
  // The brick concourse block along Mint Street, with its gates and shop fronts.
  ['1553955035', [
    { z0: 0, z1: 1.0, mat: brick },
    { z0: 1.0, z1: 4.6, mat: brick, windows: { glass, bay: 7, pier: 1.6, depth: 0.5 } },
    { z0: 4.6, z1: 7.2, mat: brick }, { z0: 7.2, z1: 8, mat: tan }]],
  ['1553955034', [{ z0: 0, z1: 6, mat: brick }, { z0: 6, z1: 13, mat: tan }]],
  ['1553955047', [{ z0: 0, z1: 6, mat: brick }, { z0: 6, z1: 13, mat: tan }]],
  ['1553955037', [{ z0: 0, z1: 6, mat: brick }, { z0: 6, z1: 12, mat: tan }]], // over way/1553955051
  ['1553955038', BR(5)], ['1553955039', BR(6)], ['1553955049', BR(3, 0.5)], ['1553955050', BR(5)],
  ['1553955065', BR(3, 0.5)],
  // The right-field club: brick base and a glazed upper floor.
  ['1553955040', [
    { z0: 0, z1: 4, mat: brick },
    { z0: 4, z1: 9.4, mat: tan, windows: { glass, bay: 6, pier: 1.2, depth: 0.5 } },
    { z0: 9.4, z1: 11, mat: tan }]],
  ['1553955041', [{ z0: 0, z1: 5, mat: brick }, { z0: 5, z1: 11, mat: tan }]],
  ['1553955054', [{ z0: 0, z1: 2, mat: concourse }]],
]
for (const [id, bands] of stack) extrude(OSM[id], bands, roof)

// The pavilion with a pyramid roof (way/1553955042: 13 m, 3 m of roof).
{
  const r = ccw(OSM['1553955042'])
  extrude(r, [{ z0: 0, z1: 5, mat: brick }, { z0: 5, z1: 10, mat: tan }], null, 0.01)
  const c = centroid(r), apex = v3(c, 13)
  r.forEach((a, i) => roof.tri(v3(a, 10), v3(r[(i + 1) % r.length], 10), apex))
}

// The plaza canopy on its posts (way/1553955048, 9–11 m).
slab(OSM['1553955048'], 9, 11, tan, roof, 0.25)
for (const id of ['1553955060', '1553955061', '1553955062', '1553955063', '1553955064'])
  extrude(OSM[id], [{ z0: 0, z1: 9, mat: tan }], null, 0.01)
for (const id of ['1553955053', '1553955057', '1553955059'])
  extrude(OSM[id], [{ z0: 0, z1: 7, mat: brick }, { z0: 7, z1: 9, mat: tan }], null, 0.01)

// The scoreboard in centre field (way/1553955052, 11 m).
extrude(OSM['1553955052'], [{ z0: 0, z1: 11, mat: board }], board, 0.3)

// The ring over the entrance plaza (relation/21303960, 7–9 m) on its two posts.
{
  const outer = OSM['1553955046'], hole = OSM['1216128113']
  const c = centroid(ccw(outer))
  const R = outer.reduce((s, p) => s + Math.hypot(p[0] - c[0], p[1] - c[1]), 0) / outer.length
  const r = hole.reduce((s, p) => s + Math.hypot(p[0] - c[0], p[1] - c[1]), 0) / hole.length
  const SEG = 20, at = (rad: number, k: number): XY => [c[0] + rad * Math.cos((k * 2 * Math.PI) / SEG), c[1] + rad * Math.sin((k * 2 * Math.PI) / SEG)]
  for (let k = 0; k < SEG; k++) {
    const o0 = at(R, k), o1 = at(R, k + 1), i0 = at(r, k), i1 = at(r, k + 1)
    const mo = unit2([(o0[0] + o1[0]) / 2 - c[0], (o0[1] + o1[1]) / 2 - c[1]])
    face(brick, [v3(o0, 7), v3(o1, 7), v3(o1, 9), v3(o0, 9)], [mo[0], mo[1], 0])
    face(brick, [v3(i0, 7), v3(i1, 7), v3(i1, 9), v3(i0, 9)], [-mo[0], -mo[1], 0])
    face(tan, [v3(o0, 9), v3(o1, 9), v3(i1, 9), v3(i0, 9)], UP)
    face(brick, [v3(o0, 7), v3(o1, 7), v3(i1, 7), v3(i0, 7)], DOWN)
  }
  for (const id of ['1553955066', '1553955067']) extrude(OSM[id], [{ z0: 0, z1: 7, mat: brick }], null, 0.01)
}

// ---------------------------------------------------------------------------
// Light towers: six, round the bowl, each a stout tapering pole carrying one
// broad lamp bank tilted down at the infield.

function rayHit(o: XY, d: XY, ring: XY[]) {
  let best = Infinity
  ring.forEach((a, i) => {
    const b = ring[(i + 1) % ring.length], e: XY = [b[0] - a[0], b[1] - a[1]]
    const den = d[0] * e[1] - d[1] * e[0]
    if (Math.abs(den) < 1e-9) return
    const t = ((a[0] - o[0]) * e[1] - (a[1] - o[1]) * e[0]) / den
    const s = ((a[0] - o[0]) * d[1] - (a[1] - o[1]) * d[0]) / den
    if (t > 0 && s >= 0 && s <= 1) best = Math.min(best, t)
  })
  return best
}
const outline = OSM['255166737'], home = BASES.home, target: XY = [BASES.mound[0], BASES.mound[1] + 10]
function tower(base: XY, H: number) {
  const SEG = 8, r0 = 1.4, r1 = 0.9
  const ring = (r: number, z: number) => Array.from({ length: SEG }, (_, k): V3 => [base[0] + r * Math.cos((k + 0.5) * 2 * Math.PI / SEG), base[1] + r * Math.sin((k + 0.5) * 2 * Math.PI / SEG), z])
  const lo = ring(r0, 0), hi = ring(r1, H - 2)
  for (let k = 0; k < SEG; k++) {
    const l = (k + 1) % SEG, m = (k + 0.5 + 0.5) * 2 * Math.PI / SEG
    const na: V3 = [Math.cos((k + 0.5) * 2 * Math.PI / SEG), Math.sin((k + 0.5) * 2 * Math.PI / SEG), 0.04]
    const nb: V3 = [Math.cos((l + 0.5) * 2 * Math.PI / SEG), Math.sin((l + 0.5) * 2 * Math.PI / SEG), 0.04]
    face(metal, [lo[k], lo[l], hi[l], hi[k]], [Math.cos(m), Math.sin(m), 0], [na, nb, nb, na])
  }
  // The lamp bank: a broad, shallow box facing the infield, tilted 18°.
  const f = unit2([target[0] - base[0], target[1] - base[1]]), s: XY = [f[1], -f[0]]
  const W = 5.2, T = 1.0, Hh = 3.4, tilt = (18 * Math.PI) / 180, zc = H
  const P = (a: number, b: number, c: number): V3 => {
    // a across, b toward the field, c up — tilted about the across axis.
    const bb = b * Math.cos(tilt) - c * Math.sin(tilt), cc = c * Math.cos(tilt) + b * Math.sin(tilt)
    return [base[0] + s[0] * a + f[0] * bb, base[1] + s[1] * a + f[1] * bb, zc + cc]
  }
  const fn: V3 = [f[0] * Math.cos(tilt), f[1] * Math.cos(tilt), -Math.sin(tilt)]
  face(lamps, [P(-W, T, -Hh), P(W, T, -Hh), P(W, T, Hh), P(-W, T, Hh)], fn)
  face(metal, [P(-W, -T, -Hh), P(W, -T, -Hh), P(W, -T, Hh), P(-W, -T, Hh)], [-fn[0], -fn[1], -fn[2]])
  face(metal, [P(-W, -T, Hh), P(W, -T, Hh), P(W, T, Hh), P(-W, T, Hh)], UP)
  face(metal, [P(-W, -T, -Hh), P(W, -T, -Hh), P(W, T, -Hh), P(-W, T, -Hh)], DOWN)
  face(metal, [P(-W, -T, -Hh), P(-W, T, -Hh), P(-W, T, Hh), P(-W, -T, Hh)], [-s[0], -s[1], 0])
  face(metal, [P(W, -T, -Hh), P(W, T, -Hh), P(W, T, Hh), P(W, -T, Hh)], [s[0], s[1], 0])
}
for (const deg of [-62, -36, -12, 12, 36, 62]) {
  const a = (deg * Math.PI) / 180, d: XY = [Math.sin(a), Math.cos(a)]
  const t = rayHit(home, d, outline) - 6
  tower([home[0] + d[0] * t, home[1] + d[1] * t], 36)
}

// ---------------------------------------------------------------------------

const parts = [
  { part: brick, material: { name: 'brick', color: 0xb05a43 } },
  { part: tan, material: { name: 'tan-precast', color: 0xd6bf98 } },
  { part: glass, material: { name: 'window', color: 0x7d8b95 } },
  { part: roof, material: { name: 'roof', color: 0xbdb9b1 } },
  { part: canopyTop, material: { name: 'canopy', color: 0xd9d6ce } },
  { part: concourse, material: { name: 'concourse', color: 0xc9c2b4 } },
  { part: seats, material: { name: 'seats', color: 0x3e5a4c } },
  { part: wallGreen, material: { name: 'field-wall', color: 0x2e4a3e } },
  // grass and dirt are built but not written: the map draws the field
  // (STYLE.md, "Don't model the ground").
  { part: metal, material: { name: 'metal', color: 0xb5b9bc } },
  { part: lamps, material: { name: 'lamps', color: 0xe4e4dc } },
  { part: board, material: { name: 'scoreboard', color: 0x2b3338 } },
]
const triangles = parts.reduce((sum, { part }) => sum + part.triangles, 0)
if (triangles > 6500) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb = writeGlb('Truist Field', parts, {
  license: 'CC0-1.0', frame: 'Y up, -Z north, +X east, metres; origin at ground anchor',
  bearing: 83.2, elevation: 0,
})
if (glb.length > 256000) throw new Error(`File budget exceeded: ${glb.length}`)
const out = new URL('../../landmarks/models/truist-field.glb', import.meta.url).pathname
await Bun.write(out, glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes`)
