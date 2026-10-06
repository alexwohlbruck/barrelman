/**
 * Preview a landmark GLB the way the map lights it, without a browser.
 *
 *   bun scripts/landmarks/preview.ts <model.glb> <out-dir> [photo.png|jpg.png ...]
 *
 * A small z-buffer rasteriser with Parchment's landmark lighting: faces turned
 * from the sun keep 72% of their colour, so contrast is as flat as on the map.
 * Writes a standard set of views — front (south), side (west), a high
 * three-quarter from the south-west (the usual phone view), its reverse, top,
 * and phone-size 200 px and 80 px versions — plus a contact sheet. Any PNG
 * photos given are put beside the three-quarter view in `compare-N.png`.
 *
 * A development tool for authoring models; nothing in the API runs it.
 */
import { inflateSync } from 'node:zlib'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { encodePng } from './mesh'
const [source,out,...photos]=process.argv.slice(2)
if(!source||!out){console.error('usage: preview.ts <model.glb> <out-dir> [photo.png ...]');process.exit(1)}
mkdirSync(out,{recursive:true})
const bytes=readFileSync(source),jlen=bytes.readUInt32LE(12)
const gltf=JSON.parse(bytes.subarray(20,20+jlen).toString()),bin=bytes.subarray(28+jlen)
const read=(idx:number)=> {
 const a=gltf.accessors[idx],v=gltf.bufferViews[a.bufferView],n=({SCALAR:1,VEC2:2,VEC3:3,VEC4:4} as any)[a.type]
 const Type=({5126:Float32Array,5123:Uint16Array,5125:Uint32Array} as any)[a.componentType]
 const b=bin.subarray((v.byteOffset||0)+(a.byteOffset||0),(v.byteOffset||0)+(a.byteOffset||0)+a.count*n*Type.BYTES_PER_ELEMENT)
 return new Type(Uint8Array.from(b).buffer)
}
type Bitmap={w:number,h:number,data:Uint8Array}
function decode(png:Uint8Array):Bitmap {
 const b=Buffer.from(png),w=b.readUInt32BE(16),h=b.readUInt32BE(20),chunks:Buffer[]=[]
 for(let at=8;at<b.length;){const n=b.readUInt32BE(at);if(b.toString('ascii',at+4,at+8)==='IDAT')chunks.push(b.subarray(at+8,at+8+n));at+=12+n}
 const raw=inflateSync(Buffer.concat(chunks)),data=new Uint8Array(w*h*4)
 for(let y=0;y<h;y++)for(let x=0;x<w*4;x++) {
  const f=raw[y*(w*4+1)],a=x>=4?data[y*w*4+x-4]:0,bb=y?data[(y-1)*w*4+x]:0,c=y&&x>=4?data[(y-1)*w*4+x-4]:0
  let pred=0
  if(f===1)pred=a;if(f===2)pred=bb;if(f===3)pred=Math.floor((a+bb)/2)
  if(f===4){const p=a+bb-c,pa=Math.abs(p-a),pb=Math.abs(p-bb),pc=Math.abs(p-c);pred=pa<=pb&&pa<=pc?a:pb<=pc?bb:c}
  data[y*w*4+x]=(raw[y*(w*4+1)+1+x]+pred)&255
 }
 return {w,h,data}
}
type Mip={w:number,h:number,data:Float32Array}
const textures:Mip[][]=(gltf.images||[]).map((im:any,i:number)=> {
 const v=gltf.bufferViews[im.bufferView],png=bin.subarray(v.byteOffset,v.byteOffset+v.byteLength),b=decode(png)
 const levels:Mip[]=[{w:b.w,h:b.h,data:Float32Array.from(b.data,(v,k)=>k%4===3?v/255:Math.pow(v/255,2.2))}]
 while(levels.at(-1)!.w>1) {
  const p=levels.at(-1)!,w=Math.max(1,p.w>>1),h=Math.max(1,p.h>>1),data=new Float32Array(w*h*4)
  for(let y=0;y<h;y++)for(let x=0;x<w;x++)for(let c=0;c<4;c++) for(let dy=0;dy<2;dy++)for(let dx=0;dx<2;dx++)data[(y*w+x)*4+c]+=p.data[((y*2+dy)*p.w+x*2+dx)*4+c]/4
  levels.push({w,h,data})
 }
 return levels
})
// Walk the scene, placing every node's mesh by its transform. A moving node is
// drawn as it stands at t = 0: its first keyframe, where it has one, else its
// own TRS.
const frame0=new Map<string,number[]>()
for(const ch of gltf.animations?.[0]?.channels||[]){
 const s=gltf.animations[0].samplers[ch.sampler],v=read(s.output),n=ch.target.path==='rotation'?4:3
 frame0.set(ch.target.node+'/'+ch.target.path,Array.from(v.subarray(0,n)))
}
const mul=(a:number[],b:number[])=>{const o=new Array(16).fill(0);for(let c=0;c<4;c++)for(let r=0;r<4;r++)for(let k=0;k<4;k++)o[c*4+r]+=a[k*4+r]*b[c*4+k];return o}
function local(i:number):number[] {
 const nd=gltf.nodes[i];if(nd.matrix)return nd.matrix
 const [x,y,z,w]=frame0.get(i+'/rotation')??nd.rotation??[0,0,0,1],[sx,sy,sz]=frame0.get(i+'/scale')??nd.scale??[1,1,1],[tx,ty,tz]=frame0.get(i+'/translation')??nd.translation??[0,0,0]
 return [(1-2*(y*y+z*z))*sx,2*(x*y+w*z)*sx,2*(x*z-w*y)*sx,0,2*(x*y-w*z)*sy,(1-2*(x*x+z*z))*sy,2*(y*z+w*x)*sy,0,2*(x*z+w*y)*sz,2*(y*z-w*x)*sz,(1-2*(x*x+y*y))*sz,0,tx,ty,tz,1]
}
const primitives:any[]=[]
function visit(i:number,parent:number[]) {
 const m=mul(parent,local(i)),nd=gltf.nodes[i]
 for(const p of nd.mesh!==undefined?gltf.meshes[nd.mesh].primitives:[]) {
  const P=read(p.attributes.POSITION),N=read(p.attributes.NORMAL),pp=new Float32Array(P.length),nn=new Float32Array(N.length)
  for(let k=0;k<P.length;k+=3)for(let r=0;r<3;r++){pp[k+r]=m[r]*P[k]+m[4+r]*P[k+1]+m[8+r]*P[k+2]+m[12+r];nn[k+r]=m[r]*N[k]+m[4+r]*N[k+1]+m[8+r]*N[k+2]}
  primitives.push({p:pp,n:nn,uv:p.attributes.TEXCOORD_0!==undefined?read(p.attributes.TEXCOORD_0):null,ix:read(p.indices),mat:gltf.materials[p.material]})
 }
 for(const c of nd.children||[])visit(c,m)
}
for(const r of gltf.scenes[gltf.scene??0].nodes)visit(r,[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1])
const norm=(v:number[])=>{const l=Math.hypot(...v);return v.map(x=>x/l)}
const dot=(a:number[],b:number[])=>a[0]*b[0]+a[1]*b[1]+a[2]*b[2]
const cross=(a:number[],b:number[])=>[a[1]*b[2]-a[2]*b[1],a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]]
const sun=norm([-1,-.7,1.5]),bg=[234,230,220]
function sample(m:Mip,u:number,v:number):number[] {
 const x=((u%1+1)%1)*m.w-.5,y=((v%1+1)%1)*m.h-.5,ix=Math.floor(x),iy=Math.floor(y),fx=x-ix,fy=y-iy,c=[0,0,0]
 for(let dy=0;dy<2;dy++)for(let dx=0;dx<2;dx++){
  const xx=(ix+dx+m.w)%m.w,yy=(iy+dy+m.h)%m.h,k=(yy*m.w+xx)*4,w=(dx?fx:1-fx)*(dy?fy:1-fy)
  for(let j=0;j<3;j++)c[j]+=m.data[k+j]*w
 }
 return c
}
const lo=[Infinity,Infinity,Infinity],hi=[-Infinity,-Infinity,-Infinity]
for(const pr of primitives)for(let i=0;i<pr.p.length/3;i++){const v=[pr.p[i*3],-pr.p[i*3+2],pr.p[i*3+1]];for(let k=0;k<3;k++){lo[k]=Math.min(lo[k],v[k]);hi[k]=Math.max(hi[k],v[k])}}
const centre=[(lo[0]+hi[0])/2,(lo[1]+hi[1])/2,(lo[2]+hi[2])/2],radius=Math.hypot(hi[0]-lo[0],hi[1]-lo[1],hi[2]-lo[2])/2
function render(name:string,w:number,h:number,az:number,el:number,ss=2) {
 const azr=az*Math.PI/180,elr=el*Math.PI/180
 const camera=[Math.cos(azr)*Math.cos(elr),Math.sin(azr)*Math.cos(elr),Math.sin(elr)]
 const right=norm(cross([0,0,1],camera)),up=cross(camera,right)
 const target=centre,scale=Math.min(w,h)*.46/radius,W=w*ss,H=h*ss
 const buffer=new Float32Array(W*H*3),depth=new Float32Array(W*H).fill(-Infinity)
 const bgl=bg.map(v=>Math.pow(v/255,2.2));for(let i=0;i<W*H;i++)buffer.set(bgl,i*3)
 const half=norm(sun.map((s,i)=>s+camera[i]))
 for(const prim of primitives) {
  const {p,n,uv,ix,mat}=prim,pbr=mat.pbrMetallicRoughness,tex=pbr.baseColorTexture?textures[gltf.textures[pbr.baseColorTexture.index].source]:null
  const pos:any[]=[],normal:any[]=[]
  for(let i=0;i<p.length/3;i++) {
   const v=[p[i*3]-target[0],-p[i*3+2]-target[1],p[i*3+1]-target[2]]
   pos.push([(w/2+dot(v,right)*scale)*ss,(h/2-dot(v,up)*scale)*ss,dot(v,camera)])
   normal.push([n[i*3],-n[i*3+2],n[i*3+1]])
  }
  for(let t=0;t<ix.length;t+=3) {
   const ids=[ix[t],ix[t+1],ix[t+2]],nn=normal[ids[0]]
   if(dot(nn,camera)<-.00001&&!mat.doubleSided)continue
   const [a,b,c]=ids.map(i=>pos[i]),det=(b[0]-a[0])*(c[1]-a[1])-(b[1]-a[1])*(c[0]-a[0])
   if(Math.abs(det)<1e-8)continue
   const xmin=Math.max(0,Math.floor(Math.min(a[0],b[0],c[0]))),xmax=Math.min(W-1,Math.ceil(Math.max(a[0],b[0],c[0])))
   const ymin=Math.max(0,Math.floor(Math.min(a[1],b[1],c[1]))),ymax=Math.min(H-1,Math.ceil(Math.max(a[1],b[1],c[1])))
   const dbdx=(c[1]-a[1])/det,dbdy=-(c[0]-a[0])/det,dcdx=-(b[1]-a[1])/det,dcdy=(b[0]-a[0])/det
   const uvv=uv?ids.map(i=>[uv[i*2],uv[i*2+1]]):[[0,0],[0,0],[0,0]]
   let lod=0,aniso=1,du=0,dv=0
   if(tex) {
    const deriv=(k:number,d1:number,d2:number)=>((uvv[1][k]-uvv[0][k])*d1+(uvv[2][k]-uvv[0][k])*d2)*ss
    const ux=deriv(0,dbdx,dcdx),vx=deriv(1,dbdx,dcdx),uy=deriv(0,dbdy,dcdy),vy=deriv(1,dbdy,dcdy)
    const lx=Math.hypot(ux*tex[0].w,vx*tex[0].h),ly=Math.hypot(uy*tex[0].w,vy*tex[0].h)
    const major=Math.max(lx,ly),minor=Math.max(.5,Math.min(lx,ly))
    aniso=Math.min(8,Math.max(1,Math.ceil(major/minor)));lod=Math.max(0,Math.min(tex.length-1,Math.log2(Math.max(minor,major/8))))
    du=lx>ly?ux:uy;dv=lx>ly?vx:vy
   }
   const level=Math.floor(lod),blend=lod-level
   // Deliberately shallow map lighting: fully turned faces retain 72%.
   const ns=ids.map(i=>normal[i])
   for(let y=ymin;y<=ymax;y++)for(let x=xmin;x<=xmax;x++) {
    const dx=x+.5-a[0],dy=y+.5-a[1],wb=dx*dbdx+dy*dbdy,wc=dx*dcdx+dy*dcdy,wa=1-wb-wc
    if(wa<-.00001||wb<-.00001||wc<-.00001)continue
    const z=wa*a[2]+wb*b[2]+wc*c[2],at=y*W+x;if(z<depth[at])continue;depth[at]=z
    let color=pbr.baseColorFactor.slice(0,3)
    if(tex) {
     const u=wa*uvv[0][0]+wb*uvv[1][0]+wc*uvv[2][0],v=wa*uvv[0][1]+wb*uvv[1][1]+wc*uvv[2][1],col=[0,0,0]
     for(let k=0;k<aniso;k++) {
      const o=(k+.5)/aniso-.5,lo=sample(tex[level],u+o*du,v+o*dv),hi=blend?sample(tex[Math.min(level+1,tex.length-1)],u+o*du,v+o*dv):lo
      for(let j=0;j<3;j++)col[j]+=(lo[j]*(1-blend)+hi[j]*blend)/aniso
     }
     color=color.map((v:number,i:number)=>v*col[i])
    }
    const smooth=norm(ns[0].map((v:number,i:number)=>wa*v+wb*ns[1][i]+wc*ns[2][i]))
    const light=.72+.28*(dot(smooth,sun)*.5+.5)
    const spec=pbr.roughnessFactor<.5?Math.pow(Math.max(0,dot(smooth,half)),35)*.055:0
    for(let j=0;j<3;j++)buffer[at*3+j]=Math.min(1,color[j]*Math.pow(light,2.2)+spec)
   }
  }
 }
 const data=new Uint8Array(w*h*4)
 for(let y=0;y<h;y++)for(let x=0;x<w;x++) {
  const col=[0,0,0];for(let dy=0;dy<ss;dy++)for(let dx=0;dx<ss;dx++)for(let c=0;c<3;c++)col[c]+=buffer[((y*ss+dy)*W+x*ss+dx)*3+c]/(ss*ss)
  data.set([...col.map(v=>Math.round(255*Math.pow(v,1/2.2))),255],(y*w+x)*4)
 }
 const bmp={w,h,data};writeFileSync(out+'/'+name+'.png',encodePng(w,h,data));return bmp
}
function resample(b:Bitmap,crop:number[],w:number,h:number):Bitmap {
 const data=new Uint8Array(w*h*4)
 for(let y=0;y<h;y++)for(let x=0;x<w;x++) {
  const xx=Math.max(0,Math.min(b.w-1,Math.floor(crop[0]+(x+.5)/w*crop[2]))),yy=Math.max(0,Math.min(b.h-1,Math.floor(crop[1]+(y+.5)/h*crop[3])))
  data.set(b.data.subarray((yy*b.w+xx)*4,(yy*b.w+xx)*4+4),(y*w+x)*4)
 }
 return {w,h,data}
}
function sheet(name:string,a:Bitmap,b:Bitmap) {
 const w=a.w+b.w+60,h=Math.max(a.h,b.h)+40,data=new Uint8Array(w*h*4)
 for(let i=0;i<w*h;i++)data.set([...bg,255],i*4)
 for(const [im,ox] of [[a,20],[b,a.w+40]] as const)for(let y=0;y<im.h;y++)data.set(im.data.subarray(y*im.w*4,(y+1)*im.w*4),((y+20)*w+ox)*4)
 writeFileSync(out+'/'+name+'.png',encodePng(w,h,data))
}
// Views. Azimuth is where the camera stands, degrees counter-clockwise from
// east: 270 is due south, 225 south-west.
const views=[
  render('front-south',600,600,270,4),
  render('side-west',600,600,180,4),
  render('three-quarter-sw',600,600,225,35),
  render('three-quarter-ne',600,600,45,35),
  render('top',600,600,270,89.5),
]
const phone200=render('phone-200',200,200,225,35,4)
render('phone-80',80,80,225,35,6)
// Contact sheet: the five views in a row of three over two, then the phone view.
{
  const cw=600,ch=600,cols=3,rows=2,w=cw*cols,h=ch*rows,data=new Uint8Array(w*h*4)
  for(let i=0;i<w*h;i++)data.set([...bg,255],i*4)
  ;[...views,phone200].forEach((im,i)=>{
    const ox=(i%cols)*cw+(cw-im.w)/2|0,oy=Math.floor(i/cols)*ch+(ch-im.h)/2|0
    for(let y=0;y<im.h;y++)data.set(im.data.subarray(y*im.w*4,(y+1)*im.w*4),((y+oy)*w+ox)*4)
  })
  writeFileSync(out+'/contact.png',encodePng(w,h,data))
}
photos.forEach((p,i)=>{
  const im=decode(new Uint8Array(readFileSync(p)))
  const scaled=resample(im,[0,0,im.w,im.h],Math.round(im.w*600/im.h),600)
  sheet(`compare-${i+1}`,scaled,views[2])
})
let triangles=0
for(const p of primitives)triangles+=p.ix.length/3
const summary={triangles,bytes:bytes.length,bounds:{min:lo,max:hi},materials:gltf.materials.map((m:any)=>m.name)}
writeFileSync(out+'/summary.json',JSON.stringify(summary,null,2))
console.log(JSON.stringify(summary))
