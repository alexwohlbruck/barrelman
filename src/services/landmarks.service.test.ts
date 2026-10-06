import { describe, test, expect } from 'bun:test'
import { join } from 'path'
import {
  validateCatalog,
  glbBounds,
  modelFileName,
  MODEL_FILE_RE,
  resolveLandmarksDir,
  type Catalog,
} from './landmarks.service'
import { Part, axisAngle, writeGlb, type V3 } from '../../scripts/landmarks/mesh'

const LANDMARKS = join(import.meta.dir, '../../landmarks')

describe('the shipped catalog', () => {
  test('is valid', async () => {
    const catalog = (await Bun.file(join(LANDMARKS, 'catalog.json')).json()) as Catalog
    expect(validateCatalog(catalog)).toEqual([])
  })

  test('every model it lists exists and follows the frame contract', async () => {
    const catalog = (await Bun.file(join(LANDMARKS, 'catalog.json')).json()) as Catalog
    for (const model of catalog.models) {
      const bytes = new Uint8Array(await Bun.file(join(LANDMARKS, model.file)).arrayBuffer())
      const { height, radius } = glbBounds(bytes)
      // Standing on its origin, not centred on it: a model whose bounds go
      // well below zero has its origin somewhere other than the ground.
      expect(height).toBeGreaterThan(0)
      expect(radius).toBeGreaterThan(0)
    }
  })
})

describe('glbBounds', () => {
  test('reads the Eiffel Tower as 330 m tall on a 125 m base', async () => {
    const bytes = new Uint8Array(await Bun.file(join(LANDMARKS, 'models/eiffel-tower.glb')).arrayBuffer())
    const { height, radius } = glbBounds(bytes)
    expect(height).toBeCloseTo(330, 0)
    // Corner of a 125 m square: half the diagonal.
    expect(radius).toBeCloseTo(62.5 * Math.SQRT2, 0)
  })

  /** A box in the map frame (x east, y north, z up) from corner a to corner b. */
  const box = (a: V3, b: V3) => {
    const part = new Part()
    const ring = (z: number): V3[] => [[a[0], a[1], z], [b[0], a[1], z], [b[0], b[1], z], [a[0], b[1], z]]
    part.loft([ring(a[2]), ring(b[2])])
    part.cap(ring(b[2]), true)
    part.cap(ring(a[2]), false)
    return part
  }
  const material = { name: 'stone', color: 0x808080 }

  test('carries a child node’s box through its translation', () => {
    const glb = writeGlb('test', [{ part: box([-1, -1, 0], [1, 1, 2]), material }], {}, {
      // 10 m east and 20 m up, a 2 m cube about its origin.
      nodes: [{ name: 'lamp', translation: [10, 0, 20], parts: [{ part: box([-1, -1, -1], [1, 1, 1]), material }] }],
    })
    const { height, radius } = glbBounds(glb)
    expect(height).toBeCloseTo(21, 5)
    expect(radius).toBeCloseTo(Math.hypot(11, 1), 5)
  })

  test('bounds an animated node by a sphere about its pivot', () => {
    // A 10 m arm on a pivot 20 m up, turning about north: at rest it is
    // level, but its tip passes 10 m over the pivot.
    const glb = writeGlb('test', [{ part: box([-1, -1, 0], [1, 1, 2]), material }], {}, {
      nodes: [{ name: 'arm', translation: [0, 0, 20], parts: [{ part: box([0, -0.5, -0.5], [10, 0.5, 0.5]), material }] }],
      animation: {
        name: 'turn',
        times: [0, 1, 2, 3, 4],
        channels: [{ node: 0, path: 'rotation', values: [0, 1, 2, 3, 4].map((i) => axisAngle([0, 1, 0], (i / 4) * Math.PI * 2)) }],
      },
    })
    const tip = Math.hypot(10, 0.5, 0.5)
    const { height, radius } = glbBounds(glb)
    expect(height).toBeCloseTo(20 + tip, 5)
    expect(radius).toBeCloseTo(tip, 5)
  })

  test('keeps the turning Wonder Wheel at its full height', async () => {
    const bytes = new Uint8Array(await Bun.file(join(LANDMARKS, 'models/wonder-wheel.glb')).arrayBuffer())
    const { height, radius } = glbBounds(bytes)
    // 46 m to the rim's top; the sphere about the axle may only overstate it.
    expect(height).toBeGreaterThanOrEqual(46)
    expect(height).toBeLessThan(50)
    expect(radius).toBeGreaterThanOrEqual(22.4)
    expect(radius).toBeLessThan(26)
  })

  test('refuses something that is not a GLB', () => {
    expect(() => glbBounds(new TextEncoder().encode('not a model at all'))).toThrow('not a GLB')
  })
})

describe('validateCatalog', () => {
  const ok: Catalog = {
    models: [{ id: 'tower', file: 'models/tower.glb', license: 'CC0-1.0', author: 'me' }],
    landmarks: [{ id: 'the-tower', name: 'Tower', model: 'tower', lng: 2.29, lat: 48.86, replaces: ['way/1'] }],
  }

  test('passes a well-formed catalog', () => {
    expect(validateCatalog(ok)).toEqual([])
  })

  test('reports every problem at once', () => {
    const problems = validateCatalog({
      models: [{ id: 'Tower', file: '../tower.obj', license: '', author: 'me' }],
      landmarks: [
        { id: 'a', name: 'A', model: 'missing', lng: 200, lat: 0, scale: 0, replaces: ['5013364'] },
      ],
    })
    expect(problems).toHaveLength(8)
  })

  test('refuses a CC-BY model with no credit to show', () => {
    const problems = validateCatalog({ ...ok, models: [{ ...ok.models[0], license: 'CC-BY-3.0' }] })
    expect(problems).toEqual(['model "tower": a CC-BY-3.0 model needs an attribution'])
  })

  test('requires OSM refs with their type, since a bare number is ambiguous', () => {
    const problems = validateCatalog({ ...ok, landmarks: [{ ...ok.landmarks[0], replaces: ['5013364'] }] })
    expect(problems[0]).toContain('not an OSM ref')
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

test('the landmarks dir defaults to the repo copy', () => {
  const saved = process.env.LANDMARKS_DIR
  process.env.LANDMARKS_DIR = ''
  try {
    expect(resolveLandmarksDir()).toEndWith('/landmarks')
  } finally {
    if (saved === undefined) delete process.env.LANDMARKS_DIR
    else process.env.LANDMARKS_DIR = saved
  }
})
