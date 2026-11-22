/**
 * Read one entry of a zip on disk as a stream of CSV rows, without loading the
 * archive. GTFS `stop_times.txt` for a New York bus borough inflates to
 * hundreds of megabytes, which JSZip would hold in memory whole.
 */

const EOCD_SIGNATURE = 0x06054b50
const CENTRAL_SIGNATURE = 0x02014b50
const LOCAL_SIGNATURE = 0x04034b50
const STORED = 0
const DEFLATE = 8

interface ZipEntry {
  name: string
  method: number
  compressedSize: number
  localHeaderOffset: number
}

async function readRange(path: string, start: number, end: number): Promise<DataView> {
  const buf = await Bun.file(path).slice(start, end).arrayBuffer()
  return new DataView(buf)
}

/** The archive's entries, read from its central directory. */
export async function listZipEntries(path: string): Promise<ZipEntry[]> {
  const size = Bun.file(path).size
  const tailStart = Math.max(0, size - 65_557)
  const tail = await readRange(path, tailStart, size)
  let eocd = -1
  for (let i = tail.byteLength - 22; i >= 0; i--) {
    if (tail.getUint32(i, true) === EOCD_SIGNATURE) { eocd = i; break }
  }
  if (eocd < 0) throw new Error(`${path} is not a zip`)
  const count = tail.getUint16(eocd + 10, true)
  const dirSize = tail.getUint32(eocd + 12, true)
  const dirOffset = tail.getUint32(eocd + 16, true)
  if (dirOffset === 0xffffffff || count === 0xffff) throw new Error(`${path} is a zip64 archive`)

  const dir = await readRange(path, dirOffset, dirOffset + dirSize)
  const decoder = new TextDecoder()
  const entries: ZipEntry[] = []
  let p = 0
  for (let i = 0; i < count && p + 46 <= dir.byteLength; i++) {
    if (dir.getUint32(p, true) !== CENTRAL_SIGNATURE) break
    const nameLength = dir.getUint16(p + 28, true)
    const extraLength = dir.getUint16(p + 30, true)
    const commentLength = dir.getUint16(p + 32, true)
    const name = decoder.decode(new Uint8Array(dir.buffer, dir.byteOffset + p + 46, nameLength))
    entries.push({
      name,
      method: dir.getUint16(p + 10, true),
      compressedSize: dir.getUint32(p + 20, true),
      localHeaderOffset: dir.getUint32(p + 42, true),
    })
    p += 46 + nameLength + extraLength + commentLength
  }
  return entries
}

/** Find an entry by file name, ignoring any folder the feed was zipped inside. */
export function findEntry(entries: ZipEntry[], fileName: string): ZipEntry | undefined {
  return entries.find(e => e.name === fileName) ?? entries.find(e => e.name.endsWith(`/${fileName}`))
}

async function entryStream(path: string, entry: ZipEntry): Promise<ReadableStream<Uint8Array>> {
  const local = await readRange(path, entry.localHeaderOffset, entry.localHeaderOffset + 30)
  if (local.getUint32(0, true) !== LOCAL_SIGNATURE) throw new Error(`bad local header for ${entry.name}`)
  const start = entry.localHeaderOffset + 30 + local.getUint16(26, true) + local.getUint16(28, true)
  const raw = Bun.file(path).slice(start, start + entry.compressedSize).stream()
  if (entry.method === STORED) return raw
  if (entry.method === DEFLATE) return raw.pipeThrough(new DecompressionStream('deflate-raw'))
  throw new Error(`unsupported compression ${entry.method} for ${entry.name}`)
}

/** Split one CSV line into cells; quoted cells may hold commas and doubled quotes. */
export function splitCsvLine(line: string): string[] {
  if (!line.includes('"')) return line.split(',')
  const cells: string[] = []
  let cell = ''
  let quoted = false
  for (let i = 0; i < line.length; i++) {
    const c = line[i]
    if (quoted) {
      if (c === '"') {
        if (line[i + 1] === '"') { cell += '"'; i++ } else quoted = false
      } else cell += c
    } else if (c === '"') quoted = true
    else if (c === ',') { cells.push(cell); cell = '' }
    else cell += c
  }
  cells.push(cell)
  return cells
}

const unevenQuotes = (s: string) => (s.match(/"/g)?.length ?? 0) % 2 === 1

/**
 * Call `onRow` for every row of a CSV entry, with a lookup from column name to
 * index. Yields to the event loop between chunks. Resolves false when the
 * entry is missing.
 */
export async function eachZipCsvRow(
  path: string,
  entries: ZipEntry[],
  fileName: string,
  onRow: (cells: string[], column: (name: string) => number) => void,
): Promise<boolean> {
  const entry = findEntry(entries, fileName)
  if (!entry) return false
  const reader = (await entryStream(path, entry)).pipeThrough(new TextDecoderStream() as unknown as ReadableWritablePair<string, Uint8Array>).getReader()
  let header: Map<string, number> | null = null
  const column = (name: string) => header?.get(name) ?? -1
  let rest = ''
  let pending = ''

  const handle = (line: string) => {
    if (line.endsWith('\r')) line = line.slice(0, -1)
    if (pending) line = `${pending}\n${line}`
    if (unevenQuotes(line)) { pending = line; return }
    pending = ''
    if (!header) {
      header = new Map(splitCsvLine(line.replace(/^﻿/, '')).map((name, i) => [name.trim(), i]))
      return
    }
    if (line) onRow(splitCsvLine(line), column)
  }

  for (;;) {
    const { value, done } = await reader.read()
    if (done) break
    const text = rest + value
    const lines = text.split('\n')
    rest = lines.pop() ?? ''
    for (const line of lines) handle(line)
  }
  if (rest) handle(rest)
  return true
}
