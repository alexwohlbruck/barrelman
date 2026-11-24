import { afterAll, describe, expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { spaceReader } from './area-spaces.service'

const dir = mkdtempSync(join(tmpdir(), 'area-spaces-'))

const lines = [
  { ways: [100, 104], rings: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
  { ways: [105, 105], rings: [[[5, 5], [6, 5], [6, 6], [5, 5]]] },
].map(l => JSON.stringify(l) + '\n')
const index: number[][] = []
let offset = 0
for (const line of lines) {
  const [first, last] = JSON.parse(line).ways
  index.push([first, last, offset, Buffer.byteLength(line)])
  offset += Buffer.byteLength(line)
}
writeFileSync(join(dir, 'area-spaces.ndjson'), lines.join(''))
writeFileSync(join(dir, 'area-spaces.index.json'), JSON.stringify(index))

const spacesFor = spaceReader(dir)

afterAll(() => rmSync(dir, { recursive: true, force: true }))

describe('spacesFor', () => {
  test('ways in one space share it, and each space reads its own line', async () => {
    const found = await spacesFor([101, 104, 105])
    expect(found.get(101)).toBe(found.get(104)!)
    expect(found.get(101)!.rings[0][1]).toEqual([1, 0])
    expect(found.get(105)!.rings[0][0]).toEqual([5, 5])
  })

  test('a way no space lists has none', async () => {
    expect((await spacesFor([99, 106])).size).toBe(0)
  })
})
