/**
 * Empire State Building — original procedural geometry, CC0-1.0.
 * bun scripts/landmarks/empire-state-building.ts
 *
 * x = v, y = u, z = metres up. Anchor 40.7485288,-73.9859714;
 * bearing 29°, elevation 0. OSM tier envelopes are preserved. The western
 * 23.8 m annex is excluded. No textures: broad bays and bevels are geometry.
 */
import { Part, writeGlb, type V3 } from './mesh'

type XY = [number, number]
type Envelope = [number, number, number, number]
type Rim = { points: XY[]; normals: V3[] }
const stone = new Part(), granite = new Part(), glass = new Part()
const roof = new Part(), garden = new Part(), steel = new Part(), dark = new Part()
const CX = 28.2, CY = 1.85
const BEVEL = .52
const RECESS = .65
const unit = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const up: V3 = [0, 0, 1]
const at = (ring: XY[], z: number): V3[] => ring.map(([x, y]) => [x, y, z])

function quad(p: Part, a: V3, b: V3, c: V3, d: V3, na: V3, nb: V3, nc = nb, nd = na) {
  p.tri(a, b, c, undefined, undefined, undefined, [na, nb, nc])
  p.tri(a, c, d, undefined, undefined, undefined, [na, nc, nd])
}

function shoulders([a, b, c, d]: Envelope, dx: number, dy: number): XY[] {
  return [[a + dx, c], [b - dx, c], [b - dx, c + dy * .45],
    [b - dx * .45, c + dy * .45], [b - dx * .45, c + dy], [b, c + dy],
    [b, d - dy], [b - dx * .45, d - dy], [b - dx * .45, d - dy * .45],
    [b - dx, d - dy * .45], [b - dx, d], [a + dx, d],
    [a + dx, d - dy * .45], [a + dx * .45, d - dy * .45],
    [a + dx * .45, d - dy], [a, d - dy], [a, c + dy],
    [a + dx * .45, c + dy], [a + dx * .45, c + dy * .45], [a + dx, c + dy * .45]]
}
function chamfer([a, b, c, d]: Envelope, n: number): XY[] {
  return [[a + n, c], [b - n, c], [b, c + n], [b, d - n],
    [b - n, d], [a + n, d], [a, d - n], [a, c + n]]
}

/** Trim every plan corner inside its envelope, with analytic rounded normals. */
function soften(ring: XY[], radius = BEVEL): Rim {
  const points: XY[] = [], normals: V3[] = []
  for (let i = 0; i < ring.length; i++) {
    const a = ring[(i + ring.length - 1) % ring.length], b = ring[i], c = ring[(i + 1) % ring.length]
    const l0 = Math.hypot(b[0]-a[0], b[1]-a[1]), l1 = Math.hypot(c[0]-b[0], c[1]-b[1])
    const u: XY = [(b[0]-a[0])/l0, (b[1]-a[1])/l0], v: XY = [(c[0]-b[0])/l1, (c[1]-b[1])/l1]
    const r = Math.min(radius, l0 * .2, l1 * .2)
    // Spend bevel triangles on exposed convex edges, not internal notches.
    if (u[0]*v[1]-u[1]*v[0] < -1e-5) {
      points.push(b); normals.push(unit([u[1]+v[1],-u[0]-v[0],0])); continue
    }
    if (Math.abs(u[0]*v[1]-u[1]*v[0]) < 1e-5) {
      points.push(b); normals.push([u[1], -u[0], 0]); continue
    }
    points.push([b[0]-u[0]*r,b[1]-u[1]*r], [b[0]+v[0]*r,b[1]+v[1]*r])
    normals.push([u[1],-u[0],0], [v[1],-v[0],0])
  }
  return {points,normals}
}

/** Convex and concave offset corners both follow the inward angle bisector. */
function inset(ring: XY[], distance: number): XY[] {
  return ring.map((b,i) => {
    const a=ring[(i+ring.length-1)%ring.length], c=ring[(i+1)%ring.length]
    const u=unit([b[1]-a[1],a[0]-b[0],0]), v=unit([c[1]-b[1],b[0]-c[0],0])
    const d=1+u[0]*v[0]+u[1]*v[1]
    return [b[0]-(u[0]+v[0])*distance/d,b[1]-(u[1]+v[1])*distance/d]
  })
}
function cap(p: Part, ring: XY[], z: number, upward = true) {
  const centre: V3 = [ring.reduce((s,v)=>s+v[0],0)/ring.length,ring.reduce((s,v)=>s+v[1],0)/ring.length,z]
  const points=at(ring,z)
  for(let i=0;i<ring.length;i++) {
    const j=(i+1)%ring.length
    if(upward) p.tri(centre,points[i],points[j]); else p.tri(centre,points[j],points[i])
  }
}
function wall(p: Part, rim: Rim, z0: number, z1: number) {
  const a=at(rim.points,z0), b=at(rim.points,z1)
  for(let i=0;i<a.length;i++) { const j=(i+1)%a.length; quad(p,a[i],a[j],b[j],b[i],rim.normals[i],rim.normals[j]) }
}
function solid(p: Part, rim: Rim, z0: number, z1: number) {
  wall(p,rim,z0,z1); cap(p,rim.points,z0,false); cap(p,rim.points,z1)
}

/** Continuous recessed glazing, framed by real soft stone bevels. */
function facade(rim: Rim, bottom: number, top: number, podium = false) {
  for(let i=0;i<rim.points.length;i++) {
    const a=rim.points[i], b=rim.points[(i+1)%rim.points.length]
    const length=Math.hypot(b[0]-a[0],b[1]-a[1]), ux=(b[0]-a[0])/length, uy=(b[1]-a[1])/length
    const n: V3=[uy,-ux,0]
    const v=(s:number,z:number,depth=0):V3=>[a[0]+ux*s+uy*depth,a[1]+uy*s-ux*depth,z]
    const panel=(p:Part,s0:number,s1:number,z0:number,z1:number)=>p.quad(v(s0,z0),v(s1,z0),v(s1,z1),v(s0,z1))
    // No bands on short notch returns or softened corners.
    const count=length<5?0:Math.max(1,Math.min(podium?6:7,Math.round(length/(podium?16:7.4))))
    if(!count) {
      quad(stone,v(0,bottom),v(length,bottom),v(length,top),v(0,top),rim.normals[i],rim.normals[(i+1)%rim.points.length]); continue
    }
    const spacing=length/count, width=spacing*.69, lo=bottom+.7, hi=top-.7
    panel(podium?granite:stone,0,length,bottom,lo)
    panel(stone,0,length,hi,top)
    let last=0
    for(let j=0;j<count;j++) {
      const s0=spacing*(j+.5)-width/2,s1=s0+width
      panel(stone,last,s0,lo,hi)
      const arch=podium?Math.min(2.3,width*.28):0
      // Four broad arc segments read as an arch without fine window detail.
      const outer: XY[]=arch ? [[s0,lo],[s1,lo],...Array.from({length:5},(_,k):XY=>{
        const angle=k*Math.PI/4
        return [(s0+s1)/2+width/2*Math.cos(angle),hi-arch+arch*Math.sin(angle)]
      })] : [[s0,lo],[s1,lo],[s1,hi],[s0,hi]]
      const inner=inset(outer,BEVEL)
      const back=inner.map(([s,z])=>v(s,z,-RECESS))
      for(let k=1;k<back.length-1;k++) glass.tri(back[0],back[k],back[k+1])
      for(let k=0;k<outer.length;k++) {
        const l=(k+1)%outer.length
        // The inner normal turns into the recess; the outer stays on the pier.
        const ds=outer[l][0]-outer[k][0], dz=outer[l][1]-outer[k][1], edge=Math.hypot(ds,dz)
        const innerN=unit([n[0]*.4-ux*dz/edge,n[1]*.4-uy*dz/edge,ds/edge])
        quad(stone,v(...outer[k]),v(...outer[l]),back[l],back[k],n,n,innerN,innerN)
      }
      if(arch) {
        for(let k=2;k<outer.length-1;k++) {
          const a=outer[k],b=outer[k+1]
          if(Math.abs(a[1]-hi)<1e-6)stone.tri(v(...a),v(b[0],hi),v(...b))
          else if(Math.abs(b[1]-hi)<1e-6)stone.tri(v(...a),v(a[0],hi),v(...b))
          else stone.quad(v(...a),v(a[0],hi),v(b[0],hi),v(...b))
        }
      }
      last=s1
    }
    panel(stone,last,length,lo,hi)
  }
}

/** Two smooth chamfers form a rounded coping above each tinted roof. */
function terrace(rim: Rim, bottom: number, top: number, coping=stone, surface=roof) {
  const middle=inset(rim.points,.5), inner=inset(rim.points,1.15)
  const a=at(rim.points,bottom), b=at(middle,top), c=at(inner,top-.6)
  for(let i=0;i<a.length;i++) {
    const j=(i+1)%a.length, ni=rim.normals[i], nj=rim.normals[j]
    quad(coping,a[i],a[j],b[j],b[i],ni,nj,up,up)
    quad(coping,b[i],b[j],c[j],c[i],up,up,[-nj[0],-nj[1],0],[-ni[0],-ni[1],0])
  }
  cap(surface,inner,top-.6)
}
function tier(ring: XY[], bottom:number, top:number) {
  const rim=soften(ring)
  facade(rim,bottom,top-.7)
  terrace(rim,top-.7,top)
}

const base:XY[]=[[-37.1,-25.3],[92.5,-25.3],[92.5,30.5],[88.9,34.1],[-31.8,34.1],[-37.1,28.8],[-37.1,0]]
const baseRim=soften(base)
solid(granite,baseRim,0,2)
facade(baseRim,2,19.5,true)
terrace(baseRim,19.5,20.5)
const tiers = [
  { z0: 20.5, z1: 75, bounds: [-19.2, 75, -23.4, 26.9] as Envelope, notch: [15, 8] },
  { z0: 75, z1: 90, bounds: [-8, 63.3, -23.4, 26.9] as Envelope, notch: [8.5, 7] },
  { z0: 90, z1: 115, bounds: [-6.4, 61.6, -18.7, 22.5] as Envelope, notch: [8, 6] },
  { z0: 115, z1: 255, bounds: [-.4, 55.5, -18.7, 22.5] as Envelope, notch: [6.6, 5.3] },
  { z0: 255, z1: 290, bounds: [1.6, 53.5, -16.7, 20.5] as Envelope, notch: [5.7, 4.6] },
  { z0: 290, z1: 310, bounds: [5.2, 50.5, -12.5, 16.2] as Envelope, notch: [4, 3.2] },
]
for(const t of tiers) tier(shoulders(t.bounds,t.notch[0],t.notch[1]),t.z0,t.z1)
tier(chamfer([6.2,49.5,-11.6,15.2],2.2),310,320)
tier(chamfer([9.2,46.5,-8.6,12.2],2),320,330)
// A single planted lower terrace, confined to the exposed western wing.
cap(garden,[[ -17.5,-13.5],[-9.5,-13.5],[-9.5,17],[-17.5,17]],74.42)

// Broad observation belt with a silver coping and pale stepped cap.
solid(steel,soften(chamfer([12.5,43.9,-6.3,10],2)),330,334.6)
solid(steel,soften(chamfer([12,44.4,-6.8,10.5],2)),334.6,334.8)
terrace(soften(chamfer([12,44.4,-6.8,10.5],2)),334.8,335.5,steel,steel)
terrace(soften(chamfer([10,45.7,-7.8,11.4],2)),330,331.15,stone)

// The original measured mooring-mast profile and antenna height.
const mastSections=[
  [335.5,6.6,6.55],[346,6.6,6.55],[352,4.05,4.05],[373.5,4.05,4.05],
  [377.5,3.8,3.8],[381,2.8,2.8],[386,2.2,2.2],[390,1.9,1.9],
] as const
const mastRings=mastSections.map(([z,rx,ry])=>{
  const cx=z>=352?28.15:CX
  return at(chamfer([cx-rx,cx+rx,CY-ry,CY+ry],rx*.29),z)
})
// Four cream corner fins frame the broad grey mooring-mast faces.
for(let k=0;k<mastRings.length-1;k++)for(let i=0;i<8;i++) {
  const j=(i+1)%8,p=k<3&&i%2===1?stone:steel
  p.quad(mastRings[k][i],mastRings[k][j],mastRings[k+1][j],mastRings[k+1][i])
}
steel.cap(mastRings.at(-1)!,true)
for(let k=0;k<3;k++)for(let i=0;i<8;i+=2) {
  const j=(i+1)%8, lower=mastRings[k],upper=mastRings[k+1]
  const mix=(a:V3,b:V3,t:number):V3=>[CX+(a[0]+(b[0]-a[0])*t-CX)*1.002,CY+(a[1]+(b[1]-a[1])*t-CY)*1.002,a[2]]
  dark.quad(mix(lower[i],lower[j],.22),mix(lower[i],lower[j],.78),mix(upper[i],upper[j],.78),mix(upper[i],upper[j],.22))
}
function pole(p:Part,sections:[number,number][]) {
  const rings=sections.map(([z,r])=>Array.from({length:8},(_,i):V3=>{
    const a=i*Math.PI/4+Math.PI/8;return [28.15+r*Math.cos(a),CY+r*Math.sin(a),z]
  }))
  p.loft(rings);p.cap(rings[0],false);p.cap(rings.at(-1)!,true)
}
pole(dark,[[373.5,4.45],[376.5,4.45]])
pole(steel,[[376.5,4.5],[377.5,4.5]])
pole(steel,[[387,2.6],[390,2.6],[391,2.05],[409,1.55],[422,1.1],[435,.58],[443.2,.16]])
for(const z of [393,419])pole(steel,[[z,z<410?2.2:1.35],[z+1,z<410?2.2:1.35]])

// sRGB authoring colours, converted by the shared writer.
const parts=[
  {part:stone,material:{name:'limestone',color:0xe7dfcd}},
  {part:granite,material:{name:'granite',color:0xc5bcaa}},
  {part:glass,material:{name:'window-bands',color:0x829097,roughness:.85}},
  {part:roof,material:{name:'terracotta-terraces',color:0xc8968a}},
  {part:garden,material:{name:'roof-garden',color:0x94a77f}},
  {part:steel,material:{name:'aluminium-steel',color:0xadb7bb,roughness:.65}},
  {part:dark,material:{name:'crown-recesses',color:0x73818a}},
]
const triangles=parts.reduce((n,{part})=>n+part.triangles,0)
if(triangles>5000)throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb=writeGlb('Empire State Building',parts,{
  license:'CC0-1.0',bearing:29,elevation:0,anchor:[40.7485288,-73.9859714],height:443.2,
  frame:'Y up, -Z north, +X east, metres; origin at the ground anchor',
  excludedAnnex:'way/265260949',footprint:{x:[-37.1,92.5],y:[-25.3,34.1]},
  note:'Measured OSM envelopes; bevelled architectural geometry and broad recessed bays',
})
if(glb.length>256000)throw new Error(`File budget exceeded: ${glb.length}`)
const out=new URL('../../landmarks/models/empire-state-building.glb',import.meta.url).pathname
await Bun.write(out,glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length/1024).toFixed(1)} KiB)`)
