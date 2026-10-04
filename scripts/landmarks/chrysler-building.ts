/**
 * Chrysler Building — a geometry-only, Apple Maps style landmark.
 * bun scripts/landmarks/chrysler-building.ts [out.glb]
 * Authoring frame: x=v, y=u, z=height, metres. Catalog bearing: 29 degrees.
 * The supplied OSM envelopes establish the footprint and setback elevations.
 */
import { Part, writeGlb, addGltfTriangles, type V3, type MaterialSpec } from './mesh'

type XY = [number, number]
const CX = -8.4, CY = 7
const stone = new Part(), glazing = new Part(), roof = new Part(), silver = new Part(), accent = new Part()
const BEVEL = .4, RECESS = .65, ARC_SEGMENTS = 10
const norm = (v: V3): V3 => { const l = Math.hypot(...v); return v.map(n => n / l) as V3 }
const add = (a: V3, b: V3): V3 => a.map((v, i) => v + b[i]) as V3
const rect = (x0: number, y0: number, x1: number, y1: number): XY[] => [[x0,y0],[x1,y0],[x1,y1],[x0,y1]]
function quad(p: Part, v: V3[], normals?: V3[]) {
  p.tri(v[0],v[1],v[2],undefined,undefined,undefined,normals?.slice(0,3))
  p.tri(v[0],v[2],v[3],undefined,undefined,undefined,normals && [normals[0],normals[2],normals[3]])
}
function cap(p: Part, xy: XY[], z: number) {
  for(let i=1;i<xy.length-1;i++) p.tri([xy[0][0],xy[0][1],z],[xy[i][0],xy[i][1],z],[xy[i+1][0],xy[i+1][1],z])
}

// A height field of the union removes all the buried walls of overlapping
// blocks. Only visible walls and terraces consume geometry or draw calls.
const masses = [
  [-40.5,-6.6,20.5,19,15],[-40.5,18.8,23.3,37.5,50],[-40.5,-25.3,12.6,-6.6,50],
  [-34.8,-19,12.6,-6.6,70],[-34.8,18.8,17.4,31.7,70],
  [-32,-16.5,14.1,-6.4,80],[-32,18.8,14.7,29,80],
  [-22.7,-13.4,5.8,-10.7,90],[-22.6,24.2,6,26,90],
  [-22.7,-10.7,6,24.2,185],
]
const upper: XY[] = [[-17.8,-10.7],[1,-10.7],[1,-2.9],[6,-2.9],[6,16.4],[.9,16.4],
  [.9,24.2],[-18.1,24.2],[-18.1,16.6],[-22.7,16.6],[-22.7,-2.5],[-17.8,-2.5]]
function inside(x: number, y: number, polygon: XY[]) {
  let hit = false
  for(let i=0,j=polygon.length-1;i<polygon.length;j=i++) {
    const a=polygon[i],b=polygon[j]
    if((a[1]>y)!==(b[1]>y) && x<(b[0]-a[0])*(y-a[1])/(b[1]-a[1])+a[0]) hit=!hit
  }
  return hit
}
const xs = [...new Set([...masses.flatMap(m=>[m[0],m[2]]),...upper.map(p=>p[0])])].sort((a,b)=>a-b)
const ys = [...new Set([...masses.flatMap(m=>[m[1],m[3]]),...upper.map(p=>p[1])])].sort((a,b)=>a-b)
const heights = ys.slice(1).map((_,j)=>xs.slice(1).map((_,i)=> {
  const x=(xs[i]+xs[i+1])/2,y=(ys[j]+ys[j+1])/2
  return Math.max(inside(x,y,upper)?200:0,...masses.map(m=>x>m[0]&&x<m[2]&&y>m[1]&&y<m[3]?m[4]:0))
}))
type Wall = { a: XY; b: XY; lo: number; hi: number; start: number; end: number }
const walls: Wall[]=[]
function collect(a: XY,b: XY,lo: number,hi: number) {
  if(hi<=lo) return
  // Merge collinear neighbouring cells, so windows don't restart at grid cuts.
  const old=walls.find(w=>w.lo===lo&&w.hi===hi&&w.b[0]===a[0]&&w.b[1]===a[1]&&
    (w.b[0]-w.a[0])*(b[1]-a[1])===(w.b[1]-w.a[1])*(b[0]-a[0]))
  if(old) old.b=b; else walls.push({a,b,lo,hi,start:0,end:0})
}
// Traverse in each face's winding order to make greedy merging deterministic.
for(let j=0;j<heights.length;j++) for(let i=0;i<heights[j].length;i++) collect([xs[i],ys[j]],[xs[i+1],ys[j]],heights[j-1]?.[i]??0,heights[j][i])
for(let j=heights.length-1;j>=0;j--) for(let i=heights[j].length-1;i>=0;i--) collect([xs[i+1],ys[j+1]],[xs[i],ys[j+1]],heights[j+1]?.[i]??0,heights[j][i])
for(let i=xs.length-2;i>=0;i--) for(let j=0;j<heights.length;j++) collect([xs[i+1],ys[j]],[xs[i+1],ys[j+1]],heights[j][i+1]??0,heights[j][i])
for(let i=0;i<xs.length-1;i++) for(let j=heights.length-1;j>=0;j--) collect([xs[i],ys[j+1]],[xs[i],ys[j]],heights[j][i-1]??0,heights[j][i])

// Convex corners with matching stage heights get a real smooth-shaded chamfer.
const corners: {at:XY; a:XY; b:XY; z:number}[]=[]
for(const a of walls) for(const b of walls) {
  if(a===b||a.lo!==b.lo||a.hi!==b.hi||a.b[0]!==b.a[0]||a.b[1]!==b.a[1]) continue
  const da=[a.b[0]-a.a[0],a.b[1]-a.a[1]],db=[b.b[0]-b.a[0],b.b[1]-b.a[1]]
  if(da[0]*db[1]-da[1]*db[0]<=0) continue
  const la=Math.hypot(...da),lb=Math.hypot(...db),w=Math.min(BEVEL,la*.2,lb*.2)
  a.end=w;b.start=w
  const p:XY=[a.b[0]-da[0]/la*w,a.b[1]-da[1]/la*w],q:XY=[b.a[0]+db[0]/lb*w,b.a[1]+db[1]/lb*w]
  const na:V3=[da[1]/la,-da[0]/la,0],nb:V3=[db[1]/lb,-db[0]/lb,0]
  quad(stone,[[...p,a.lo],[...q,a.lo],[...q,a.hi-BEVEL],[...p,a.hi-BEVEL]],[na,nb,nb,na])
  corners.push({at:a.b,a:p,b:q,z:a.hi})
}

/** A true recessed band. Outer and inner frames share no coplanar overlay;
 * the sloping white reveals are .4m wide and the grey back is .65m inset. */
function opening(at:(s:number,d:number,z:number)=>V3,normal:V3,l:number,r:number,lo:number,hi:number) {
  const bevel=Math.min(BEVEL,(r-l)*.15,(hi-lo)*.12)
  const outer=[[l,lo],[r,lo],[r,hi],[l,hi]]
  const inner=[[l+bevel,lo+bevel],[r-bevel,lo+bevel],[r-bevel,hi-bevel],[l+bevel,hi-bevel]]
  const o=outer.map(([s,z])=>at(s,0,z)),i=inner.map(([s,z])=>at(s,-RECESS,z))
  for(let k=0;k<4;k++) {
    const j=(k+1)%4
    // Smooth white bevel at the lip, with a sharper junction to the glass.
    const tangent:V3=[o[j][0]-o[k][0],o[j][1]-o[k][1],o[j][2]-o[k][2]]
    const inward=norm([normal[1]*tangent[2]-normal[2]*tangent[1],normal[2]*tangent[0]-normal[0]*tangent[2],normal[0]*tangent[1]-normal[1]*tangent[0]])
    const n=norm(add(normal,inward))
    quad(stone,[o[k],o[j],i[j],i[k]],[normal,normal,n,n])
  }
  quad(glazing,i)
}
for(const w of walls) {
  const length=Math.hypot(w.b[0]-w.a[0],w.b[1]-w.a[1]),dx=(w.b[0]-w.a[0])/length,dy=(w.b[1]-w.a[1])/length
  const normal:V3=[dy,-dx,0]
  const at=(s:number,d:number,z:number):V3=>[w.a[0]+s*dx+d*dy,w.a[1]+s*dy-d*dx,z]
  const l=w.start,r=length-w.end,top=w.hi-BEVEL
  const n=Math.max(1,Math.min(8,Math.round((r-l)/5.5))),pitch=(r-l)/n
  const bottom=w.lo+(w.lo===0?1.2:.7),windowTop=top-.9
  if(pitch<2.6||windowTop-bottom<3) {
    quad(stone,[at(l,0,w.lo),at(r,0,w.lo),at(r,0,top),at(l,0,top)])
  } else {
    quad(stone,[at(l,0,w.lo),at(r,0,w.lo),at(r,0,bottom),at(l,0,bottom)])
    quad(stone,[at(l,0,windowTop),at(r,0,windowTop),at(r,0,top),at(l,0,top)])
    let last=l
    for(let k=0;k<n;k++) {
      const a=l+k*pitch+pitch*.10,b=l+(k+1)*pitch-pitch*.10
      quad(stone,[at(last,0,bottom),at(a,0,bottom),at(a,0,windowTop),at(last,0,windowTop)])
      opening(at,normal,a,b,bottom,windowTop)
      last=b
    }
    quad(stone,[at(last,0,bottom),at(r,0,bottom),at(r,0,windowTop),at(last,0,windowTop)])
  }
  // A broad white roof edge, smoothly bevelled into a quiet rose terrace.
  const upperNormal=norm(add(normal,[0,0,1]))
  quad(stone,[at(l,0,top),at(r,0,top),at(r,-BEVEL,w.hi),at(l,-BEVEL,w.hi)],[normal,normal,upperNormal,upperNormal])
  quad(stone,[at(l,-BEVEL,w.hi),at(r,-BEVEL,w.hi),at(r,-.95,w.hi),at(l,-.95,w.hi)])
}

// Merge exposed roof cells without adding a parapet around each grid cell.
const used=new Set<string>()
for(let j=0;j<heights.length;j++) for(let i=0;i<heights[j].length;i++) {
  const h=heights[j][i];if(!h||used.has(`${i},${j}`))continue
  let end=i+1;while(end<heights[j].length&&heights[j][end]===h&&!used.has(`${end},${j}`))end++
  let row=j+1;while(row<heights.length&&Array.from({length:end-i},(_,k)=>i+k).every(k=>heights[row][k]===h&&!used.has(`${k},${row}`)))row++
  for(let yy=j;yy<row;yy++)for(let xx=i;xx<end;xx++)used.add(`${xx},${yy}`)
  let polygon=rect(xs[i],ys[j],xs[end],ys[row])
  for(const corner of corners.filter(c=>c.z===h&&polygon.some(p=>p[0]===c.at[0]&&p[1]===c.at[1]))) {
    const a=corner.a,b=corner.b,side=(p:XY)=>(b[0]-a[0])*(p[1]-a[1])-(b[1]-a[1])*(p[0]-a[0])
    const clipped:XY[]=[]
    for(let k=0;k<polygon.length;k++) {
      const p=polygon[k],q=polygon[(k+1)%polygon.length],sp=side(p),sq=side(q)
      if(sp>=0)clipped.push(p)
      if((sp>=0)!==(sq>=0)){const t=sp/(sp-sq);clipped.push([p[0]+t*(q[0]-p[0]),p[1]+t*(q[1]-p[1])])}
    }
    polygon=clipped
  }
  cap(roof,polygon,h-.06)
}

const depth=(r:number)=>r*(r<3?.39:.82)
const place=(face:number,s:number,d:number,z:number):V3=> {
  const a=face*Math.PI/2;return [CX+s*Math.cos(a)-d*Math.sin(a),CY+s*Math.sin(a)+d*Math.cos(a),z]
}
const faceNormal=(face:number):V3=>[-Math.sin(face*Math.PI/2),Math.cos(face*Math.PI/2),0]
const tiers=[{r:11.1,apex:235},{r:10.25,apex:242},{r:8.65,apex:249},{r:6.95,apex:256},{r:4.95,apex:262},{r:4.05,apex:267},{r:2.95,apex:272}]
const arc=(r:number,apex:number,s:number)=>apex-r+Math.sqrt(Math.max(0,r*r-s*s))

/** Circular vaults retain the established silhouette, with ten segments per
 * half-circle. Broad bevels take the place of separate trim/rib geometry. */
function vault(r:number,apex:number,base:number,previous?:{r:number,apex:number}) {
  const d=depth(r)
  for(let f=0;f<4;f++) for(let j=0;j<ARC_SEGMENTS;j++) {
    const a=j*Math.PI/ARC_SEGMENTS,b=(j+1)*Math.PI/ARC_SEGMENTS
    const s0=-r*Math.cos(a),s1=-r*Math.cos(b),z0=apex-r+r*Math.sin(a),z1=apex-r+r*Math.sin(b)
    const l0=previous?Math.min(z0,arc(previous.r,previous.apex,s0)):base,l1=previous?Math.min(z1,arc(previous.r,previous.apex,s1)):base
    const point=(s:number,dd:number,z:number):V3=> {
      const p=place(f,s,dd,z)
      if(!previous) {p[0]=Math.max(CX-13.1,Math.min(CX+13.1,p[0]));p[1]=Math.max(CY-13.1,Math.min(CY+13.1,p[1]))}
      return p
    }
    const at=(s:number,z:number)=>point(s,d,z)
    quad(silver,[at(s1,l1),at(s0,l0),at(s0,z0),at(s1,z1)])
    const n0=norm(add(faceNormal(f),[0,0,Math.sin(a)])),n1=norm(add(faceNormal(f),[0,0,Math.sin(b)]))
    quad(silver,[at(s0,z0),point(s0,0,z0),point(s1,0,z1),at(s1,z1)],[n0,[0,0,1],[0,0,1],n1])
  }
}
vault(14.3,229,200)

/** Upper shaft: three grey recessed columns, including a tall central round
 * arch, inside the large masonry arch on each of the four faces. */
for(let f=0;f<4;f++) {
  const d=f%2?14.3:14.9,r=8.2,spring=218.8,n=faceNormal(f)
  const at=(s:number,inset:number,z:number)=>place(f,s,d+inset,z)
  // Front wall is tiled between three aperture silhouettes, leaving raised piers.
  const openings=[{l:-6.6,r:-3.7,top:220,round:false},{l:-2.65,r:2.65,top:223,round:true},{l:3.7,r:6.6,top:220,round:false}]
  let last=-r
  const topAt=(s:number)=>spring+Math.sqrt(Math.max(0,r*r-s*s))
  function pier(l:number,rr:number) {
    const cuts=[l,...Array.from({length:ARC_SEGMENTS-1},(_,i)=>-r*Math.cos((i+1)*Math.PI/ARC_SEGMENTS)).filter(s=>s>l&&s<rr),rr]
    for(let k=0;k<cuts.length-1;k++)quad(stone,[at(cuts[k+1],0,200),at(cuts[k],0,200),at(cuts[k],0,topAt(cuts[k])),at(cuts[k+1],0,topAt(cuts[k+1]))])
  }
  for(const o of openings) {
    pier(last,o.l)
    const rad=(o.r-o.l)/2,c=(o.l+o.r)/2,spr=o.top-rad,segments=o.round?6:1
    // Lower sill; surrounding wall above the aperture follows the masonry arch.
    quad(stone,[at(o.r,0,200),at(o.l,0,200),at(o.l,0,201),at(o.r,0,201)])
    const contour:XY[]=[[o.r,201],[o.l,201]]
    for(let k=0;k<=segments;k++) {
      const s=o.round?c-rad*Math.cos(k*Math.PI/segments):k===0?o.l:o.r
      const z=o.round?spr+rad*Math.sin(k*Math.PI/segments):o.top
      contour.push([s,z])
    }
    // Recess perimeter: each grey panel is physically behind its white lip.
    const inn=contour.map(([s,z]):XY=>[c+(s-c)*.86,z===201?201.35:z-.35])
    for(let k=0;k<contour.length;k++) {
      const j=(k+1)%contour.length,p=contour[k],q=contour[j],ip=inn[k],iq=inn[j]
      quad(stone,[at(p[0],0,p[1]),at(q[0],0,q[1]),at(iq[0],-RECESS,iq[1]),at(ip[0],-RECESS,ip[1])],[n,n,n,n])
    }
    const center=at(c,-RECESS,(201+o.top)/2)
    for(let k=0;k<inn.length;k++){const j=(k+1)%inn.length;glazing.tri(center,at(inn[k][0],-RECESS,inn[k][1]),at(inn[j][0],-RECESS,inn[j][1]))}
    for(let k=2;k<contour.length-1;k++) {
      const p=contour[k],q=contour[k+1]
      quad(stone,[at(q[0],0,q[1]),at(p[0],0,p[1]),at(p[0],0,topAt(p[0])),at(q[0],0,topAt(q[0]))])
    }
    last=o.r
  }
  pier(last,r)
  // Close the two side returns of the masonry shoulder behind the glazing.
  for(const side of [-1,1]) {
    const points=[at(side*r,0,200),at(side*r,-2.4,200),at(side*r,-2.4,spring),at(side*r,0,spring)]
    quad(stone,side===1?points:points.reverse())
  }
  // A smooth .45m bevel around the large round shoulder.
  for(let j=0;j<ARC_SEGMENTS;j++) {
    const a=j*Math.PI/ARC_SEGMENTS,b=(j+1)*Math.PI/ARC_SEGMENTS
    const p=(t:number,rr:number,dd:number)=>place(f,-rr*Math.cos(t),dd,spring+rr*Math.sin(t))
    quad(stone,[p(a,r,d),p(b,r,d),p(b,r+.45,d-.4),p(a,r+.45,d-.4)],[n,n,norm(add(n,[0,0,1])),norm(add(n,[0,0,1]))])
  }
}

/** Apple's crown teeth: a few fat, rounded cones per tier per face, each
 * rooted on the tier's arch ledge and leaning out along the arch's radius,
 * rising into the face of the tier above. Few and full-bodied — thin flat
 * spikes scattered over every face read as fuzz at map scale. Kept off the
 * arch ends, where neighbouring faces' teeth would crowd the corners, and
 * pointing mostly up, so they stay inside the crown's outline. */
function teeth(r:number,apex:number,next?:{r:number,apex:number}) {
  const spring=apex-r,count=r>8?4:3
  const gap=next?Math.max(2.5,next.apex-apex+(r-next.r)):3
  const length=Math.min(gap*.95,r*.5),width=Math.min(1.35,r*.14)
  const cone=new Part()
  for(let f=0;f<4;f++) for(let k=0;k<count;k++) {
    const a=.85+(Math.PI-1.7)*k/(count-1)
    const radial:XY=[Math.cos(a),Math.sin(a)],tangent:XY=[-Math.sin(a),Math.cos(a)]
    // Lean out along the radius, but more upward than the arch alone would,
    // so the side teeth stand rather than lie flat.
    const dir:XY=(()=>{const x=radial[0]*.4,y=radial[1]*.4+.9,l=Math.hypot(x,y);return [x/l,y/l]})()
    const d=Math.max(depth(r),Math.sqrt(Math.max(0,r*r-(radial[1]*r)**2)))
    const base=(u:number,z:number,out:number):V3=>place(f,u,d+width+out,z)
    const bu=radial[0]*r*.9,bz=spring+radial[1]*r*.9
    const ring=Array.from({length:6},(_,j)=>{
      const t=j/6*Math.PI*2
      // Circle in the plane across the cone's axis: one axis along the face
      // (perpendicular to dir in elevation), the other out of the face.
      const across=Math.cos(t)*width,outward=Math.sin(t)*width
      return base(bu-dir[1]*across,bz+dir[0]*across,outward)
    })
    const tip=base(bu+dir[0]*length,bz+dir[1]*length,0)
    for(let j=0;j<6;j++){const n=(j+1)%6;cone.tri(ring[j],ring[n],tip)}
  }
  addGltfTriangles(accent,new Float32Array(cone.pos),Uint32Array.from({length:cone.pos.length/3},(_,i)=>i),{creaseDegrees:70})
}
for(let i=0;i<tiers.length;i++) {const t=tiers[i];vault(t.r,t.apex,200,i?tiers[i-1]:{r:14.3,apex:229});if(i<tiers.length-1)teeth(t.r,t.apex,tiers[i+1])}

// Bold faceted eagle/hood silhouettes; the small eyes, feathers and ribs go.
function eagle(x:number,y:number,dx:number,dy:number,z:number,scale=1) {
  const l=Math.hypot(dx,dy);dx/=l;dy/=l
  const at=(u:number,v:number,h:number):V3=>[x+scale*(u*dx-v*dy),y+scale*(u*dy+v*dx),z+scale*h]
  const a=at(-1,-1,-.6),b=at(-1,1,-.6),c=at(2.5,.45,.5),d=at(2.5,-.45,.5),tip=at(3.85,0,.45),ridge=at(2.55,0,1.35)
  glazing.tri(a,b,ridge);glazing.tri(b,c,ridge);glazing.tri(c,tip,ridge);glazing.tri(tip,d,ridge);glazing.tri(d,a,ridge)
  glazing.tri(a,d,c);glazing.tri(a,c,b);glazing.tri(c,d,tip)
}
for(const [x,y,dx,dy] of [[-22.7,-2.5,-1,-1],[-17.8,-10.7,-1,-1],[6,-2.9,1,-1],[1,-10.7,1,-1],[-22.7,16.6,-1,1],[-18.1,24.2,-1,1],[6,16.4,1,1],[.9,24.2,1,1]])eagle(x,y,dx,dy,199.5,1.035)
for(const [x0,y0,x1,y1,dx,dy] of [[-26.2,23.8,-20,29.9,-1,1],[3.7,23.4,9.8,29.7,1,1],[-26.5,-17.1,-20.4,-10.8,-1,-1],[3.3,-17.1,9.6,-11,1,-1]]) {
  const x=(x0+x1)/2,y=(y0+y1)/2,p=rect(x0,y0,x1,y1)
  const lo=p.map(([x,y]):V3=>[x,y,89]),hi=p.map(([xx,yy]):V3=>[x+(xx-x)*.55,y+(yy-y)*.55,91])
  silver.loft([lo,hi]);silver.cap(hi,true);eagle(x,y,dx,dy,90,.64)
}
// Same 319m tip and 2.3m needle base, with eight sides and a cool shaded edge.
const needle=[[271.8,1.15],[301,.36],[319,0]]
const nr=needle.map(([z,r])=>Array.from({length:8},(_,i):V3=>[CX+r*Math.cos(i*Math.PI/4),CY+r*Math.sin(i*Math.PI/4),z]))
for(let i=0;i<8;i++){const j=(i+1)%8,p=i<3?glazing:silver;p.quad(nr[0][i],nr[0][j],nr[1][j],nr[1][i]);p.tri(nr[1][i],nr[1][j],[CX,CY,319])}

const parts:{part:Part;material:MaterialSpec}[]=[
  {part:stone,material:{name:'warm off-white bevelled piers',color:0xe9e6df,roughness:.65}},
  {part:glazing,material:{name:'soft grey recessed bands and eagles',color:0x929ba0,roughness:.7}},
  {part:roof,material:{name:'muted rose terraces',color:0xc8968a,roughness:.85}},
  {part:silver,material:{name:'light silver crown and needle',color:0xd8dddf,roughness:.42}},
  {part:accent,material:{name:'grey crown teeth',color:0x77878f,roughness:.65,doubleSided:true}},
]
const triangles=parts.reduce((sum,p)=>sum+p.part.triangles,0)
const glb=writeGlb('Chrysler Building',parts,{frame:'Y up, -Z north, +X east; metres; ground anchor',anchor:[40.75151,-73.9752851],bearing:29,crown:tiers.map(t=>({radius:t.r,spring:t.apex-t.r,apex:t.apex})),style:'geometry-only, broad recessed bands and bevelled piers',recessMetres:RECESS,bevelMetres:BEVEL})
if(triangles>5000||glb.length>250000)throw new Error(`Budget exceeded: ${triangles} triangles / ${glb.length} bytes`)
const out=process.argv[2]??new URL('../../landmarks/models/chrysler-building.glb',import.meta.url).pathname
await Bun.write(out,glb)
console.log(`${out}: ${triangles} triangles, ${glb.length} bytes (${(glb.length/1024).toFixed(1)} KiB); ${walls.length} exposed facade patches`)
