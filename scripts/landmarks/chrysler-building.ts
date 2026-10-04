/**
 * Chrysler Building — procedural landmark, authored in x=v, y=u, z=height.
 * Run: bun scripts/landmarks/chrysler-building.ts [out.glb]
 * The catalog supplies the 29° bearing; the mesh kit writes the Y-up frame.
 *
 * Setbacks use the supplied OSM envelopes. Their small plan notches are
 * approximated conservatively because the survey supplies ranges, not nodes.
 * All facade detail is painted at metre scale; only silhouette is geometry.
 */
import { Part, encodePng, writeGlb, type MaterialSpec, type V3 } from './mesh'

const CX = -8.4, CY = 7
const masonry = new Part(), banded = new Part(), crown = new Part()
const stone = new Part(), roofs = new Part(), steel = new Part(), shadow = new Part(), reflection = new Part()
type XY = [number, number]
const rect = (x0: number, y0: number, x1: number, y1: number): XY[] =>
  [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]
const ring = (xy: XY[], z: number): V3[] => xy.map(([x, y]) => [x, y, z])
const mix = (a: number, b: number, t: number) => a + (b - a) * t
const crownDepth = (r: number) => r * (r < 3 ? .39 : .82)

/** Ear clipping keeps roofs of the cross-shaped shaft out of its notches. */
function cap(p: Part, xy: XY[], z: number, up = true) {
  const ids = xy.map((_, i) => i)
  const cross = (a: XY, b: XY, c: XY) => (b[0]-a[0])*(c[1]-a[1])-(b[1]-a[1])*(c[0]-a[0])
  while (ids.length > 2) {
    let clipped = false
    for (let k = 0; k < ids.length; k++) {
      const ia = ids[(k+ids.length-1)%ids.length], ib = ids[k], ic = ids[(k+1)%ids.length]
      const a = xy[ia], b = xy[ib], c = xy[ic]
      if (cross(a,b,c) <= 1e-8) continue
      if (ids.some(i => i!==ia && i!==ib && i!==ic && cross(a,b,xy[i]) >= 0 && cross(b,c,xy[i]) >= 0 && cross(c,a,xy[i]) >= 0)) continue
      const v = [a,b,c].map(([x,y]): V3 => [x,y,z])
      if (up) p.tri(v[0],v[1],v[2]); else p.tri(v[2],v[1],v[0])
      ids.splice(k,1); clipped = true; break
    }
    if (!clipped) throw new Error('Non-simple footprint')
  }
}
function plain(p: Part, xy: XY[], lo: number, hi: number) {
  p.loft([ring(xy,lo), ring(xy,hi)])
  cap(p,xy,hi); cap(p,xy,lo,false)
}

// Each 256px facade tile is four bays by four floors, with a consistent
// worldwide floor datum. UVs repeat at 1.8m bays and 3.7m floors, not per wall.
const BAY = 1.8, FLOOR = 3.7
function walls(xy: XY[], lo: number, hi: number, striped = true) {
  for (let e=0; e<xy.length; e++) {
    const a=xy[e], b=xy[(e+1)%xy.length]
    const length=Math.hypot(b[0]-a[0],b[1]-a[1])
    const point=(t: number,z: number): V3 => [mix(a[0],b[0],t),mix(a[1],b[1],t),z]
    const sections = striped && length>9 ? [0,.22,.78,1] : [0,1]
    for (let k=0;k<sections.length-1;k++) {
      const l=sections[k], r=sections[k+1]
      const p=striped && (sections.length===2 || k!==1) ? banded : masonry
      p.quad(point(l,lo),point(r,lo),point(r,hi),point(l,hi),
        [[l*length/(4*BAY),-lo/(4*FLOOR)],[r*length/(4*BAY),-lo/(4*FLOOR)],
         [r*length/(4*BAY),-hi/(4*FLOOR)],[l*length/(4*BAY),-hi/(4*FLOOR)]])
    }
  }
}
function block(x0:number,y0:number,x1:number,y1:number,lo:number,hi:number) {
  const xy=rect(x0,y0,x1,y1)
  walls(xy,lo,hi-.45)
  plain(stone,xy,hi-.45,hi)
  cap(roofs,xy,hi+.008)
  if (lo===0) cap(stone,xy,0,false)
}

// Street podium, north/south wings and their 50/70/80/90m terraces.
// These cover the outline and every low-rise OSM part, including the eastern
// 15m wing and the asymmetry of the southern street frontage.
block(-40.5,-6.6,20.5,19,0,15)
block(-40.5,18.8,23.3,37.5,0,50)
block(-40.5,-25.3,12.6,-6.6,0,50)
block(-34.8,-19,12.6,-6.6,50,70)
block(-34.8,18.8,17.4,31.7,50,70)
block(-32,-16.5,14.1,-6.4,70,80)
block(-32,18.8,14.7,29,70,80)
block(-22.7,-13.4,5.8,-10.7,80,90)
block(-22.6,24.2,6,26,80,90)

const shaft=rect(-22.7,-10.7,6,24.2)
walls(shaft,0,185)
cap(stone,shaft,0,false)
plain(stone,shaft,184.5,185.3)
cap(roofs,shaft,185.31)
const upper: XY[] = [[-17.8,-10.7],[1,-10.7],[1,-2.9],[6,-2.9],
  [6,16.4],[.9,16.4],[.9,24.2],[-18.1,24.2],[-18.1,16.6],[-22.7,16.6],[-22.7,-2.5],[-17.8,-2.5]]
walls(upper,185.3,199.5)
plain(stone,upper,199.5,200)
cap(roofs,upper,200.01)

/** Four intersecting shallow circular vaults. The arch faces have exact
 * circular elevations; the short returns meet behind their adjacent faces.
 * The shallow returns keep each perpendicular ridge behind the next tier.
 * Artwork crosses the returns in the adjacent face's projection. Each band
 * starts on the preceding arch rather than an arbitrary horizontal ledge.
 */
function vault(r:number,apex:number,base:number,p:Part,atlas:number|null, previous?: {r:number,apex:number}) {
  const spring=apex-r, N=32, depth=crownDepth(r)
  for (let face=0;face<4;face++) {
    const angle=face*Math.PI/2, c=Math.cos(angle), s=Math.sin(angle)
    const at=(u:number,d:number,z:number): V3 => [CX+u*c-d*s,CY+u*s+d*c,z]
    const uv=(u:number,z:number): number[] => {
      if(atlas===null) return [(u+r)/(4*BAY),-z/(4*FLOOR)]
      return [((atlas%2)*256+3+(u/r+1)*.5*250)/512,
        (Math.floor(atlas/2)*256+3+(1-(z-spring)/r)*.5*250)/512]
    }
    const low=atlas===null?base:Math.max(base,spring-r*.96)
    for(let j=0;j<N;j++) {
      const u0=-r*Math.cos(j*Math.PI/N),u1=-r*Math.cos((j+1)*Math.PI/N)
      const z0=spring+Math.sqrt(Math.max(0,r*r-u0*u0)),z1=spring+Math.sqrt(Math.max(0,r*r-u1*u1))
      const lower=(u:number)=>previous?previous.apex-previous.r+Math.sqrt(Math.max(0,previous.r*previous.r-u*u)):low
      const l0=Math.min(z0,lower(u0)),l1=Math.min(z1,lower(u1))
      p.quad(at(u1,depth,l1),at(u0,depth,l0),at(u0,depth,z0),at(u1,depth,z1),
        [uv(u1,l1),uv(u0,l0),uv(u0,z0),uv(u1,z1)])
      // The perpendicular barrel's exposed return belongs to the adjacent
      // sunburst: project that face's artwork across the fold as well.
      const sign=(u0+u1)<0?1:-1
      p.quad(at(u0,depth,z0),at(u0,0,z0),at(u1,0,z1),at(u1,depth,z1),
        [uv(sign*depth,z0),uv(0,z0),uv(0,z1),uv(sign*depth,z1)])
    }
    if(atlas===null) {
      masonry.quad(at(-r,0,base),at(-r,depth,base),at(-r,depth,spring),at(-r,0,spring))
      masonry.quad(at(r,depth,base),at(r,0,base),at(r,0,spring),at(r,depth,spring))
    }
  }
}

// Arched masonry neck with nested steel mouldings; the 200m joint is the
// same 28.6m width as the shaft. The OSM 227/228/229m roofs describe these
// overlapping arch surrounds, not three additional steel sunburst tiers.
vault(14.3,229,200,masonry,null)

// The central masonry arches are narrower than their steel surrounds.
// Keeping the infill forward preserves the tall pale piers under the crown.
for(let face=0;face<4;face++) {
  const a=face*Math.PI/2,c=Math.cos(a),s=Math.sin(a),r=14.3,inner=8.2
  const depth=face%2?14.3:14.9
  const at=(u:number,z:number):V3=>[CX+u*c-depth*s,CY+u*s+depth*c,z]
  const brickUV=(u:number,z:number)=>[(u+inner)/(4*BAY),-z/(4*FLOOR)]
  const metalUV=(u:number,z:number)=>[(3+(u/r+1)*125)/512,(3+Math.min(.995,(1-(z-214.7)/r)*.5)*250)/512]
  const innerTop=(u:number)=>218.8+Math.sqrt(Math.max(0,inner*inner-u*u))
  for(let j=0;j<32;j++) {
    const u0=-inner*Math.cos(j*Math.PI/32),u1=-inner*Math.cos((j+1)*Math.PI/32),z0=innerTop(u0),z1=innerTop(u1)
    masonry.quad(at(u1,200),at(u0,200),at(u0,z0),at(u1,z1),[brickUV(u1,200),brickUV(u0,200),brickUV(u0,z0),brickUV(u1,z1)])
  }
  // Include the infill endpoints explicitly so no triangle crosses its reveal.
  const cuts=[...Array.from({length:33},(_,j)=>-r*Math.cos(j*Math.PI/32)),-inner,inner].sort((a,b)=>a-b)
  for(let j=0;j<cuts.length-1;j++) {
    const u0=cuts[j],u1=cuts[j+1],inside=Math.abs((u0+u1)/2)<inner
    const l0=inside?innerTop(u0):200,l1=inside?innerTop(u1):200
    const h0=214.7+Math.sqrt(Math.max(0,r*r-u0*u0)),h1=214.7+Math.sqrt(Math.max(0,r*r-u1*u1))
    crown.quad(at(u1,l1),at(u0,l0),at(u0,h0),at(u1,h1),[metalUV(u1,l1),metalUV(u0,l0),metalUV(u0,h0),metalUV(u1,h1)])
    const inset=(u:number,z:number):V3=>[CX+u*c-crownDepth(r)*s,CY+u*s+crownDepth(r)*c,z]
    crown.quad(at(u0,h0),inset(u0,h0),inset(u1,h1),at(u1,h1),[metalUV(u0,h0),metalUV(u0,h0),metalUV(u1,h1),metalUV(u1,h1)])
  }
}

// Seven stainless sunbursts, from the paired round-roof OSM envelopes.
// The four-way symmetry averages small discrepancies between opposite faces.
const tiers = [
  {r:11.1,apex:235,atlas:0}, {r:10.25,apex:242,atlas:0},
  {r:8.65,apex:249,atlas:1}, {r:6.95,apex:256,atlas:1},
  {r:4.95,apex:262,atlas:2}, {r:4.05,apex:267,atlas:2},
  {r:2.95,apex:272,atlas:3},
]
for(let i=0;i<tiers.length;i++) {
  const t=tiers[i]
  vault(t.r,t.apex,200,crown,t.atlas,i?tiers[i-1]:{r:14.3,apex:229})
}

/** Circular raised mouldings are silhouette-scale, unlike the painted ribs. */
function archRim(r:number,apex:number,width:number) {
  const spring=apex-r
  for(let f=0;f<4;f++) {
    const angle=f*Math.PI/2,c=Math.cos(angle),s=Math.sin(angle)
    const at=(u:number,d:number,z:number): V3 => [CX+u*c-d*s,CY+u*s+d*c,z]
    for(let j=0;j<32;j++) {
      const a=j*Math.PI/32,b=(j+1)*Math.PI/32
      const v=(angle:number,rad:number,depth:number):V3 => at(Math.cos(angle)*rad,depth,spring+Math.sin(angle)*rad)
      steel.quad(v(a,r,crownDepth(r)+.035),v(b,r,crownDepth(r)+.035),v(b,r-width,crownDepth(r)+.035),v(a,r-width,crownDepth(r)+.035))
    }
  }
}
archRim(14.3,229,.24)
archRim(13.9,228.6,.15)
for(const t of tiers) archRim(t.r,t.apex,.14)

/** Closed faceted forms along a direction: used for eagles and radiator caps. */
function ornament(cx:number,cy:number,dx:number,dy:number,z:number,scale=1) {
  const l=Math.hypot(dx,dy); dx/=l; dy/=l
  const at=(along:number,across:number,h:number):V3 => [cx+(along*dx-across*dy)*scale,cy+(along*dy+across*dx)*scale,z+h*scale]
  const sections = [
    [-1.4,.82,-.65,.25],[0,.7,-.42,.7],[1.7,.48,.15,1.25],
    [2.55,.58,.38,1.65],[3.15,.35,.42,1.42],[3.7,.08,.10,.8],
  ]
  const rr=sections.map(([d,w,lo,hi])=>[at(d,-w,lo),at(d,w,lo),at(d,w,hi),at(d,-w,hi)])
  steel.loft(rr); steel.cap(rr[0],false); steel.cap(rr[rr.length-1],true)
  // Swept-back cheek/wing blades make the diagonal bird projections legible.
  for(const side of [-1,1]) {
    const a=at(1.7,side*.42,.5),b=at(-1.4,side*1.4,-.15),c=at(-.5,side*.62,-1.05),d=at(.55,side*.28,-.4)
    steel.tri(a,b,c); steel.tri(a,c,d); steel.tri(c,b,a); steel.tri(d,c,a)
    const eye=at(2.62,side*.582,1.11)
    const v=(dd:number,hh:number):V3=>[eye[0]+dd*dx,eye[1]+dd*dy,eye[2]+hh]
    shadow.quad(v(-.13,-.07),v(.13,-.07),v(.13,.07),v(-.13,.07))
    shadow.quad(v(.13,-.07),v(-.13,-.07),v(-.13,.07),v(.13,.07))
  }
}

// The eight mapped eagle heads are paired around four recessed corners.
// Together their swept steel shoulders read as four bold diagonal groups.
for(const [x,y,dx,dy] of [
  [-22.7,-2.5,-1,-1],[-17.8,-10.7,-1,-1], [6,-2.9,1,-1],[1,-10.7,1,-1],
  [-22.7,16.6,-1,1],[-18.1,24.2,-1,1],[6,16.4,1,1],[.9,24.2,1,1],
]) ornament(x,y,dx,dy,199.5,1.035)
// Radiator-cap ornaments occupy all four 89–91m OSM envelopes.
for(const [x0,y0,x1,y1,dx,dy] of [
  [-26.2,23.8,-20,29.9,-1,1],[3.7,23.4,9.8,29.7,1,1],
  [-26.5,-17.1,-20.4,-10.8,-1,-1],[3.3,-17.1,9.6,-11,1,-1],
]) {
  const x=(x0+x1)/2,y=(y0+y1)/2
  plain(stone,rect(x0,y0,x1,y1),89,89.35)
  const rings=[[89.35,2.5],[90.5,2.2],[91,1.35]].map(([z,r])=>
    Array.from({length:16},(_,i):V3=>[x+r*Math.cos(i*Math.PI/8),y+r*Math.sin(i*Math.PI/8),z]))
  steel.loft(rings); steel.cap(rings[2],true)
  ornament(x,y,dx,dy,90,.64)
}

// A 2.3m base tapering continuously to a sharp 319m tip; no telescoping mast.
const needle = [[271.8,1.15],[282,.87],[301,.36],[315,.09],[319,0]]
const nr=needle.map(([z,r])=>Array.from({length:12},(_,k):V3=>[CX+r*Math.cos(k*Math.PI/6),CY+r*Math.sin(k*Math.PI/6),z]))
for(let j=0;j<12;j++) {
  const p=j<6?reflection:steel,next=(j+1)%12
  for(let k=0;k<nr.length-2;k++) p.quad(nr[k][j],nr[k][next],nr[k+1][next],nr[k+1][j])
  p.tri(nr[nr.length-2][j],nr[nr.length-2][next],[CX,CY,319])
}

/** Deterministic painted masonry: four-by-four small punched windows. */
function facadeTexture(stripes:boolean) {
  const size=256,px=new Uint8Array(size*size*4)
  for(let y=0;y<size;y++) for(let x=0;x<size;x++) {
    const u=(x%64)/64,v=(y%64)/64, bay=Math.floor(x/64)
    let rgb=[228,226,220]
    // Fine, staggered brick courses. Restrained contrast survives minification.
    const course=Math.floor(y/3), mortar=y%3===0 || (x+(course%2)*6)%12===0
    if(mortar) rgb=[219,219,215]
    if(stripes && (u<.055 || u>.945)) rgb=[133,139,139]
    if(stripes && v>.39 && v<.48) rgb=[154,158,157]
    if(u>.23 && u<.77 && v>.14 && v<.65) {
      rgb=[55+bay*3,69+bay*2,77+bay*2]
      if(v<.17 || u<.26 || u>.74) rgb=[96,102,101]
      if(v>.38 && v<.407) rgb=[182,184,179]
      if(v>.61) rgb=[195,198,192]
      if(bay===1 && v>.18 && v<.38) rgb=[108,122,125]
    }
    if(u>.20 && u<.80 && v>=.65 && v<.68) rgb=[246,244,235]
    const i=(y*size+x)*4; px.set([...rgb,255],i)
  }
  return encodePng(size,size,px)
}
function inTriangle(x:number,y:number,a:XY,b:XY,c:XY) {
  const cross=(a:XY,b:XY)=>(b[0]-a[0])*(y-a[1])-(b[1]-a[1])*(x-a[0])
  const d=[cross(a,b),cross(b,c),cross(c,a)]
  return d.every(v=>v>=0)||d.every(v=>v<=0)
}
/** Four circular sunburst paintings in one 512px atlas, with fewer lights
 * toward the tip. The ribs and triangular glazing follow radii, not z rows. */
function crownTexture() {
  const size=512,px=new Uint8Array(size*size*4)
  for(let y=0;y<size;y++) for(let x=0;x<size;x++) {
    const cell=Math.floor(y/256)*2+Math.floor(x/256)
    const u=((x%256)-3)/250*2-1, v=1-((y%256)-3)/250*2
    const rho=Math.hypot(u,v),theta=Math.atan2(v,u)
    const rib=theta/Math.PI*48,phase=rib-Math.floor(rib)
    const gleam=Math.round(8*Math.cos(theta*6)+5*Math.sin(theta*19))
    let rgb=[234+gleam,239+gleam,240+gleam]
    if(phase<.10) rgb=[167,181,188]
    else if(phase<.22) rgb=[253,254,254]
    if(v>=0 && rho>.959 && rho<.997) rgb=[249,252,252]
    if(v>=0 && rho>.927 && rho<.958) rgb=[137,151,159]
    if(v<0) { const line=Math.abs(u*20-Math.round(u*20)); rgb=line<.09?[169,184,193]:[236,242,245] }
    // Punched windows in the narrow steel side bays below the masonry arch.
    if(cell===0 && v<0 && Math.abs(u)>.73 && Math.abs(u)<.82) {
      const floor=(-v*14.3/3.7)%1
      if(floor>.18 && floor<.72) rgb=floor>.43&&floor<.47?[183,197,201]:[62,78,89]
    }
    const count=[7,5,3,1][cell]
    for(let k=0;k<count;k++) {
      const a=(k+.5)*Math.PI/count
      const pt=(r:number,t:number):XY=>[r*Math.cos(t),r*Math.sin(t)]
      const spread=.87/count
      const base=.42,tip=.92,extension=[0,.16,.42,.9][cell]
      const basePt=(angle:number):XY=>{const v=pt(base,angle);return [v[0],v[1]-extension]}
      const A=basePt(a-spread),B=basePt(a+spread),C=pt(tip,a)
      if(inTriangle(u,v,A,B,C)) {
        rgb=[63,82,94]
        if(Math.abs(rho-.64)<.012 || Math.abs(rho-.75)<.012 || Math.abs(theta-a)<.012) rgb=[187,201,204]
      } else if(inTriangle(u,v,[A[0]-.025,A[1]-.025],[B[0]+.025,B[1]-.025],pt(tip+.035,a))) rgb=[253,255,255]
    }
    const i=(y*size+x)*4; px.set([...rgb.map(c=>Math.max(0,Math.min(255,c))),255],i)
  }
  return encodePng(size,size,px)
}

const parts: {part:Part,material:MaterialSpec}[] = [
  {part:masonry,material:{name:'white-grey brick / small punched windows',color:0xffffff,texture:{png:facadeTexture(false)}}},
  {part:banded,material:{name:'corner brick stripes / window courses',color:0xffffff,texture:{png:facadeTexture(true)}}},
  {part:crown,material:{name:'stainless sunbursts / radial ribs / triangular glazing',color:0xffffff,roughness:.25,texture:{png:crownTexture()}}},
  {part:stone,material:{name:'pale limestone copings',color:0xe4e2dc}},
  {part:roofs,material:{name:'setback roofs',color:0xb0b8ba}},
  {part:steel,material:{name:'bright folded stainless steel',color:0xf3f8fa,roughness:.2}},
  {part:shadow,material:{name:'eagle eye recesses',color:0x3c4d57}},
  {part:reflection,material:{name:'cool needle reflection',color:0xa9bdcb,roughness:.24}},
]
const triangles=parts.reduce((n,p)=>n+p.part.triangles,0)
if(triangles>14000) throw new Error(`Triangle budget exceeded: ${triangles}`)
const glb=writeGlb('Chrysler Building',parts,{
  frame:'Y up, -Z north, +X east; metres; ground anchor; bearing supplied by catalog',
  anchor:[40.75151,-73.9752851],bearing:29,
  crown:tiers.map(t=>({radius:t.r,spring:t.apex-t.r,apex:t.apex})),
  facade:{bayMetres:BAY,floorMetres:FLOOR},
})
if(glb.length>500*1024) throw new Error(`GLB budget exceeded: ${glb.length}`)
const out=process.argv[2]??new URL('../../landmarks/models/chrysler-building.glb',import.meta.url).pathname
await Bun.write(out,glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length/1024).toFixed(1)} KiB)`)
