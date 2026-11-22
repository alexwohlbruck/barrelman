import { afterAll, describe, expect, it } from 'bun:test'
import JSZip from 'jszip'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { eachZipCsvRow, listZipEntries, splitCsvLine } from './zip-stream'

const dir = mkdtempSync(join(tmpdir(), 'zip-stream-'))
afterAll(() => rmSync(dir, { recursive: true, force: true }))

async function writeZip(files: Record<string, string>, compression: 'DEFLATE' | 'STORE'): Promise<string> {
  const zip = new JSZip()
  for (const [name, text] of Object.entries(files)) zip.file(name, text)
  const path = join(dir, `${Object.keys(files).join('-').replaceAll('/', '_')}-${compression}.zip`)
  await Bun.write(path, await zip.generateAsync({ type: 'uint8array', compression }))
  return path
}

async function readRows(path: string, name: string) {
  const out: Record<string, string>[] = []
  const found = await eachZipCsvRow(path, await listZipEntries(path), name, (cells, col) => {
    out.push({ stop: cells[col('stop_id')], name: cells[col('stop_name')] })
  })
  return { found, out }
}

describe('eachZipCsvRow', () => {
  const stops = '﻿stop_id,stop_name\r\n1,"Main St, Downtown"\r\n2,"Quoted ""Plaza"""\r\n3,"Two\nLines"\r\n'
  const expected = [
    { stop: '1', name: 'Main St, Downtown' },
    { stop: '2', name: 'Quoted "Plaza"' },
    { stop: '3', name: 'Two\nLines' },
  ]

  it('streams a deflated entry, with a BOM, CRLF and quoted cells', async () => {
    const path = await writeZip({ 'stops.txt': stops, 'other.txt': 'a\n1\n' }, 'DEFLATE')
    expect(await readRows(path, 'stops.txt')).toEqual({ found: true, out: expected })
  })

  it('reads stored entries and entries inside a folder', async () => {
    const path = await writeZip({ 'feed/stops.txt': stops }, 'STORE')
    expect(await readRows(path, 'stops.txt')).toEqual({ found: true, out: expected })
  })

  it('handles an entry larger than one stream chunk', async () => {
    const lines = Array.from({ length: 50_000 }, (_, i) => `${i},Stop ${i}`)
    const path = await writeZip({ 'stops.txt': `stop_id,stop_name\n${lines.join('\n')}` }, 'DEFLATE')
    const { out } = await readRows(path, 'stops.txt')
    expect(out.length).toBe(50_000)
    expect(out.at(-1)).toEqual({ stop: '49999', name: 'Stop 49999' })
  })

  it('reports a missing entry', async () => {
    const path = await writeZip({ 'agency.txt': 'agency_id\n1\n' }, 'DEFLATE')
    expect((await readRows(path, 'stops.txt')).found).toBe(false)
  })
})

describe('splitCsvLine', () => {
  it('splits plain and quoted cells', () => {
    expect(splitCsvLine('a,,c')).toEqual(['a', '', 'c'])
    expect(splitCsvLine('"a,b",c')).toEqual(['a,b', 'c'])
  })
})
