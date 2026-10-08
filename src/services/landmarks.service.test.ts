import { describe, test, expect } from 'bun:test'
import { createModelIndex, glbBounds, modelFileName, MODEL_FILE_RE } from './landmarks.service'

// The shipped models, and the catalog checks that went with them, now live in
// github.com/alexwohlbruck/landmarks. These tests build the few GLBs they
// need by hand, in glTF axes (x east, y up, z south).

type Box = { min: number[]; max: number[] }
type NodeSpec = { box?: Box; translation?: number[]; children?: number[] }
type Channel = { node: number; path: 'translation' | 'rotation'; values: number[][] }

/** A GLB of boxes (8 corner vertices each) on nodes, with an optional LINEAR clip. */
function glb(nodes: NodeSpec[], animation?: { times: number[]; channels: Channel[] }): Uint8Array {
  const floats: number[] = []
  const accessors: any[] = []
  const bufferViews: any[] = []
  const add = (values: number[], type: string, minmax = false) => {
    const per = { SCALAR: 1, VEC3: 3, VEC4: 4 }[type]!
    bufferViews.push({ buffer: 0, byteOffset: floats.length * 4, byteLength: values.length * 4 })
    const accessor: any = { bufferView: bufferViews.length - 1, componentType: 5126, count: values.length / per, type }
    if (minmax) {
      accessor.min = [0, 1, 2].slice(0, per).map((k) => Math.min(...values.filter((_, i) => i % per === k)))
      accessor.max = [0, 1, 2].slice(0, per).map((k) => Math.max(...values.filter((_, i) => i % per === k)))
    }
    floats.push(...values)
    accessors.push(accessor)
    return accessors.length - 1
  }
  const meshes: any[] = []
  const json: any = {
    asset: { version: '2.0' },
    scene: 0,
    scenes: [{ nodes: [0] }],
    nodes: nodes.map((n) => {
      const node: any = {}
      if (n.box) {
        const corners = [0, 1, 2, 3, 4, 5, 6, 7].flatMap((i) => [0, 1, 2].map((k) => (i & (1 << k) ? n.box!.max[k] : n.box!.min[k])))
        meshes.push({ primitives: [{ attributes: { POSITION: add(corners, 'VEC3', true) } }] })
        node.mesh = meshes.length - 1
      }
      if (n.translation) node.translation = n.translation
      if (n.children) node.children = n.children
      return node
    }),
    meshes,
    accessors,
    bufferViews,
  }
  if (animation) {
    const input = add(animation.times, 'SCALAR', true)
    json.animations = [{
      channels: animation.channels.map((c, i) => ({ sampler: i, target: { node: c.node, path: c.path } })),
      samplers: animation.channels.map((c) => ({ input, output: add(c.values.flat(), c.path === 'rotation' ? 'VEC4' : 'VEC3'), interpolation: 'LINEAR' })),
    }]
  }
  const bin = new Uint8Array(new Float32Array(floats).buffer)
  json.buffers = [{ byteLength: bin.length }]
  let text = JSON.stringify(json)
  while (text.length % 4) text += ' '
  const out = new Uint8Array(20 + text.length + 8 + bin.length)
  const view = new DataView(out.buffer)
  view.setUint32(0, 0x46546c67, true)
  view.setUint32(4, 2, true)
  view.setUint32(8, out.length, true)
  view.setUint32(12, text.length, true)
  view.setUint32(16, 0x4e4f534a, true)
  out.set(new TextEncoder().encode(text), 20)
  view.setUint32(20 + text.length, bin.length, true)
  view.setUint32(24 + text.length, 0x004e4942, true)
  out.set(bin, 28 + text.length)
  return out
}

const box = (min: number[], max: number[]): Box => ({ min, max })
/** A quarter turn about north (-z), as a quaternion. */
const turn = (i: number) => {
  const a = (i / 4) * Math.PI
  return [0, 0, -Math.sin(a), Math.cos(a)]
}

describe('glbBounds', () => {
  test('reads a static model from its accessor bounds', () => {
    const { height, radius } = glbBounds(glb([{ box: box([-62.5, 0, -62.5], [62.5, 330, 62.5]) }]))
    expect(height).toBeCloseTo(330, 5)
    // Corner of a 125 m square: half the diagonal.
    expect(radius).toBeCloseTo(62.5 * Math.SQRT2, 5)
  })

  test('carries a child node’s box through its translation', () => {
    // A 2 m cube 10 m east and 20 m up.
    const { height, radius } = glbBounds(glb([
      { box: box([-1, 0, -1], [1, 2, 1]), children: [1] },
      { box: box([-1, -1, -1], [1, 1, 1]), translation: [10, 20, 0] },
    ]))
    expect(height).toBeCloseTo(21, 5)
    expect(radius).toBeCloseTo(Math.hypot(11, 1), 5)
  })

  test('bounds an animated node by a sphere about its pivot', () => {
    // A 10 m arm on a pivot 20 m up, turning about north: at rest it is
    // level, but its tip passes 10 m over the pivot.
    const { height, radius } = glbBounds(glb(
      [
        { box: box([-1, 0, -1], [1, 2, 1]), children: [1] },
        { box: box([0, -0.5, -0.5], [10, 0.5, 0.5]), translation: [0, 20, 0] },
      ],
      { times: [0, 1, 2, 3, 4], channels: [{ node: 1, path: 'rotation', values: [0, 1, 2, 3, 4].map(turn) }] },
    ))
    const tip = Math.hypot(10, 0.5, 0.5)
    expect(height).toBeCloseTo(20 + tip, 5)
    expect(radius).toBeCloseTo(tip, 5)
  })

  test('keeps a lift’s reach to its car, not its travel', () => {
    // A 10 m car whose pivot rides 60 m up an axis on LINEAR keyframes: its
    // height is the top of the travel, but in plan it never leaves the axis.
    const { height, radius } = glbBounds(glb(
      [
        { box: box([-1, 0, -1], [1, 2, 1]), children: [1] },
        { box: box([-5, -0.5, -0.5], [5, 0.5, 0.5]), translation: [0, 60, 0] },
      ],
      { times: [0, 1, 2], channels: [{ node: 1, path: 'translation', values: [[0, 60, 0], [0, 2, 0], [0, 60, 0]] }] },
    ))
    const tip = Math.hypot(5, 0.5, 0.5)
    expect(height).toBeCloseTo(60 + tip, 5)
    expect(radius).toBeCloseTo(tip, 5)
  })

  test('refuses something that is not a GLB', () => {
    expect(() => glbBounds(new TextEncoder().encode('not a model at all'))).toThrow('not a GLB')
  })
})

describe('model file names', () => {
  test('are content-addressed and match the served pattern', () => {
    const name = modelFileName('eiffel-tower', 'ab'.repeat(32))
    expect(name).toBe('eiffel-tower.abababababab.glb')
    expect(MODEL_FILE_RE.test(name)).toBe(true)
  })

  test('the pattern admits no path', () => {
    expect(MODEL_FILE_RE.test('../eiffel-tower.abababababab.glb')).toBe(false)
    expect(MODEL_FILE_RE.test('eiffel-tower.abababababab.glb/x')).toBe(false)
  })
})

describe('model index', () => {
  const OLD = 'eiffel-tower.aaaaaaaaaaaa.glb'
  const NEW = 'big-ben.bbbbbbbbbbbb.glb'

  /** An index over a mutable "database", counting loads, on a fake clock. */
  function index() {
    const db = new Map([[OLD, '/models/old.glb']])
    const clock = { t: 0 }
    let loads = 0
    const idx = createModelIndex({
      load: async () => {
        loads++
        return [...db]
      },
      reloadMs: 10_000,
      now: () => clock.t,
    })
    return { idx, db, clock, loads: () => loads }
  }

  test('a model imported by another process is found on the next miss', async () => {
    const { idx, db, clock, loads } = index()
    await idx.refresh()
    // A separate import process writes a new model; this one's copy is stale.
    db.set(NEW, '/models/new.glb')
    clock.t = 10_000
    expect(await idx.path(NEW)).toBe('/models/new.glb')
    expect(loads()).toBe(2)
  })

  test('a hit never touches the database', async () => {
    const { idx, clock, loads } = index()
    await idx.refresh()
    clock.t = 60_000
    expect(await idx.path(OLD)).toBe('/models/old.glb')
    expect(loads()).toBe(1)
  })

  test('misses reload at most once per interval', async () => {
    const { idx, db, clock, loads } = index()
    await idx.refresh()
    clock.t = 10_000
    expect(await idx.path(NEW)).toBeNull()
    expect(loads()).toBe(2)
    // Published just after that reload: missed until the interval has passed.
    db.set(NEW, '/models/new.glb')
    clock.t = 15_000
    expect(await idx.path(NEW)).toBeNull()
    expect(loads()).toBe(2)
    clock.t = 20_000
    expect(await idx.path(NEW)).toBe('/models/new.glb')
    expect(loads()).toBe(3)
  })

  test('concurrent misses share one reload', async () => {
    const { idx, db, clock, loads } = index()
    await idx.refresh()
    db.set(NEW, '/models/new.glb')
    clock.t = 10_000
    const found = await Promise.all([idx.path(NEW), idx.path(NEW), idx.path('nope.cccccccccccc.glb')])
    expect(found).toEqual(['/models/new.glb', '/models/new.glb', null])
    expect(loads()).toBe(2)
  })

  test('the first miss loads an index nothing has filled yet', async () => {
    const { idx, loads } = index()
    expect(await idx.path(OLD)).toBe('/models/old.glb')
    expect(loads()).toBe(1)
  })

  test('a malformed name never reloads', async () => {
    const { idx, clock, loads } = index()
    clock.t = 10_000
    expect(await idx.path('../.env')).toBeNull()
    expect(loads()).toBe(0)
  })

  test('a failed reload is a miss, not an error', async () => {
    const idx = createModelIndex({ load: async () => { throw new Error('db down') }, now: () => 0 })
    const err = console.error
    console.error = () => {}
    try {
      expect(await idx.path(NEW)).toBeNull()
    } finally {
      console.error = err
    }
  })
})
