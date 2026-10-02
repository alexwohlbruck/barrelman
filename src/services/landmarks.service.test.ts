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
