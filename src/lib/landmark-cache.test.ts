import { afterEach, describe, expect, it } from 'bun:test'
import { mkdtemp, rm } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import { landmarkCacheAvailable } from './landmark-cache'

const directories: string[] = []
afterEach(async () => {
  await Promise.all(directories.splice(0).map(dir => rm(dir, { recursive: true, force: true })))
})

async function cache() {
  const dir = await mkdtemp(join(tmpdir(), 'landmark-cache-'))
  directories.push(dir)
  const model = { path: join(dir, 'abc.glb'), sha256: 'abc' }
  await Bun.write(model.path, 'model')
  return { dir, model }
}

describe('landmarkCacheAvailable', () => {
  it('keeps an intact release and an empty source unchanged', async () => {
    const { dir, model } = await cache()
    expect(await landmarkCacheAvailable([model], dir)).toBe(true)
    expect(await landmarkCacheAvailable([], dir)).toBe(true)
  })

  it('requires recovery when a container loses its model files', async () => {
    const { dir, model } = await cache()
    await rm(model.path)
    expect(await landmarkCacheAvailable([model], dir)).toBe(false)
  })

  it('requires every model, including detail models, to be present', async () => {
    const { dir, model } = await cache()
    expect(await landmarkCacheAvailable([model, { path: join(dir, 'detail.glb'), sha256: 'detail' }], dir)).toBe(false)
    expect(await landmarkCacheAvailable([{ path: null, sha256: 'abc' }], dir)).toBe(false)
  })

  it('reimports paths when the cache moves to a persistent volume', async () => {
    const { dir, model } = await cache()
    expect(await landmarkCacheAvailable([model], join(dir, 'persistent'))).toBe(false)
  })
})
