/**
 * Just enough of the MVT protobuf to add a string property to some features
 * of one layer. Everything else in the tile is copied through byte for byte.
 */

type Field = { field: number; wire: number; start: number; end: number; value: number; body?: Uint8Array }

function readVarint(bytes: Uint8Array, at: number): [number, number] {
  let value = 0
  let scale = 1
  for (;;) {
    if (at >= bytes.length) throw new Error('Truncated varint')
    const b = bytes[at++]
    value += (b & 0x7f) * scale
    if (b < 0x80) return [value, at]
    scale *= 128
  }
}

function* fields(bytes: Uint8Array): Generator<Field> {
  let at = 0
  while (at < bytes.length) {
    const start = at
    const [key, next] = readVarint(bytes, at)
    at = next
    const field = Math.floor(key / 8), wire = key & 7
    let value = 0
    let body: Uint8Array | undefined
    if (wire === 0) [value, at] = readVarint(bytes, at)
    else if (wire === 1) at += 8
    else if (wire === 5) at += 4
    else if (wire === 2) {
      const [length, from] = readVarint(bytes, at)
      at = from + length
      body = bytes.subarray(from, at)
    } else throw new Error(`Unsupported wire type ${wire}`)
    if (at > bytes.length) throw new Error('Truncated field')
    yield { field, wire, start, end: at, value, body }
  }
}

const packed = (body: Uint8Array) => {
  const out: number[] = []
  for (let at = 0; at < body.length;) {
    const [v, next] = readVarint(body, at)
    out.push(v)
    at = next
  }
  return out
}

class Writer {
  private chunks: Uint8Array[] = []
  private pending: number[] = []
  private size = 0
  varint(v: number) {
    while (v > 127) {
      this.pending.push((v % 128) | 128)
      v = Math.floor(v / 128)
    }
    this.pending.push(v)
    return this
  }
  bytes(field: number, body: Uint8Array) {
    return this.varint(field * 8 + 2).varint(body.length).raw(body)
  }
  raw(body: Uint8Array) {
    this.flush()
    this.chunks.push(body)
    this.size += body.length
    return this
  }
  private flush() {
    if (!this.pending.length) return
    this.chunks.push(new Uint8Array(this.pending))
    this.size += this.pending.length
    this.pending = []
  }
  done(): Uint8Array<ArrayBuffer> {
    this.flush()
    const out = new Uint8Array(this.size)
    let at = 0
    for (const c of this.chunks) {
      out.set(c, at)
      at += c.length
    }
    return out
  }
}

const zigzag = (n: number) => (n % 2 ? -(n + 1) / 2 : n / 2)

/** A feature's lines in tile units: one per MoveTo. */
function decodeLines(geometry: number[]): Array<Array<[number, number]>> {
  const lines: Array<Array<[number, number]>> = []
  let x = 0, y = 0
  for (let i = 0; i < geometry.length;) {
    const command = geometry[i] & 7, count = geometry[i] >> 3
    i++
    if (command === 7) continue
    for (let k = 0; k < count; k++) {
      x += zigzag(geometry[i++])
      y += zigzag(geometry[i++])
      if (command === 1) lines.push([[x, y]])
      else lines.at(-1)?.push([x, y])
    }
  }
  return lines.filter(line => line.length > 1)
}

/** One feature as read from a layer: its id, decoded properties and geometry in tile units. */
export type MvtFeature = { id?: number; type: number; properties: Record<string, string | number | boolean>; lines: Array<Array<[number, number]>> }

function readValue(body: Uint8Array): string | number | boolean {
  for (const f of fields(body)) {
    if (f.field === 1 && f.body) return new TextDecoder().decode(f.body)
    if (f.field === 7) return f.value !== 0
    if (f.field === 4) return f.value
    if (f.field === 5) return f.value
    if (f.field === 6) return zigzag(f.value)
    if (f.field === 2) return new DataView(body.buffer, body.byteOffset + f.start + 1, 4).getFloat32(0, true)
    if (f.field === 3) return new DataView(body.buffer, body.byteOffset + f.start + 1, 8).getFloat64(0, true)
  }
  return ''
}

/** The names of a tile's layers, in order. */
export function layerNames(tile: Uint8Array): string[] {
  const names: string[] = []
  for (const f of fields(tile)) if (f.field === 3 && f.body) names.push(nameOf(f.body))
  return names
}

function nameOf(layer: Uint8Array): string {
  for (const f of fields(layer)) if (f.field === 1 && f.body) return new TextDecoder().decode(f.body)
  return ''
}

/** A layer's features and extent, decoded. */
export function readLayer(tile: Uint8Array, name: string): { extent: number; features: MvtFeature[] } | null {
  for (const f of fields(tile)) if (f.field === 3 && f.body && nameOf(f.body) === name) return decodeLayer(f.body)
  return null
}

function decodeLayer(layer: Uint8Array) {
  const keys: string[] = [], values: Array<string | number | boolean> = [], raw: Uint8Array[] = []
  let extent = 4096
  for (const f of fields(layer)) {
    if (f.field === 3 && f.body) keys.push(new TextDecoder().decode(f.body))
    else if (f.field === 4 && f.body) values.push(readValue(f.body))
    else if (f.field === 2 && f.body) raw.push(f.body)
    else if (f.field === 5) extent = f.value
  }
  const features = raw.map(body => {
    const feature: MvtFeature = { type: 0, properties: {}, lines: [] }
    for (const f of fields(body)) {
      if (f.field === 1) feature.id = f.value
      else if (f.field === 3) feature.type = f.value
      else if (f.field === 2 && f.body) {
        const tags = packed(f.body)
        for (let i = 0; i + 1 < tags.length; i += 2) feature.properties[keys[tags[i]]] = values[tags[i + 1]]
      } else if (f.field === 4 && f.body) feature.lines = decodeLines(packed(f.body))
    }
    return feature
  })
  return { extent, keys, features }
}

/**
 * The tile with a string property `key` added to features of layer `name`:
 * `value` is asked for each feature in order, and null leaves it as it was.
 * Returns the tile itself when nothing changes, or the layer already has `key`.
 */
export function withProperty(
  tile: Uint8Array,
  name: string,
  key: string,
  value: (feature: MvtFeature, extent: number) => string | null,
): Uint8Array {
  const out = new Writer()
  let changed = false
  for (const f of fields(tile)) {
    if (f.field !== 3 || !f.body || changed || nameOf(f.body) !== name) {
      out.raw(tile.subarray(f.start, f.end))
      continue
    }
    const layer = rewriteLayer(f.body, key, value)
    if (!layer) {
      out.raw(tile.subarray(f.start, f.end))
      continue
    }
    changed = true
    out.bytes(3, layer)
  }
  return changed ? out.done() : tile
}

function rewriteLayer(layer: Uint8Array, key: string, value: (feature: MvtFeature, extent: number) => string | null): Uint8Array | null {
  const { extent, keys, features } = decodeLayer(layer)
  if (keys.includes(key)) return null
  const added = features.map(feature => value(feature, extent))
  if (added.every(v => v === null)) return null
  const keyIndex = keys.length
  let valueIndex = 0
  for (const f of fields(layer)) if (f.field === 4) valueIndex++
  const strings: string[] = []
  const out = new Writer()
  let feature = 0
  for (const f of fields(layer)) {
    if (f.field !== 2 || !f.body) {
      out.raw(layer.subarray(f.start, f.end))
      continue
    }
    const text = added[feature++]
    if (text === null) {
      out.raw(layer.subarray(f.start, f.end))
      continue
    }
    const tag = [keyIndex, valueIndex + strings.length]
    strings.push(text)
    out.bytes(2, retagged(f.body, tag))
  }
  out.bytes(3, new TextEncoder().encode(key))
  for (const s of strings) out.bytes(4, new Writer().bytes(1, new TextEncoder().encode(s)).done())
  return out.done()
}

function retagged(feature: Uint8Array, tag: number[]): Uint8Array {
  const out = new Writer()
  let tagged = false
  for (const f of fields(feature)) {
    if (f.field === 2 && f.body && !tagged) {
      const tags = new Writer().raw(f.body)
      for (const t of tag) tags.varint(t)
      out.bytes(2, tags.done())
      tagged = true
    } else out.raw(feature.subarray(f.start, f.end))
  }
  if (!tagged) {
    const tags = new Writer()
    for (const t of tag) tags.varint(t)
    out.bytes(2, tags.done())
  }
  return out.done()
}

/** A layer as an MVT protobuf, for tests and fixtures: lines in tile units, properties as strings, numbers or booleans. */
export function encodeLayer(name: string, features: Array<Omit<MvtFeature, 'type'> & { type?: number }>, extent = 4096): Uint8Array<ArrayBuffer> {
  const keys: string[] = [], values: Array<string | number | boolean> = []
  const index = <T>(list: T[], v: T) => (list.includes(v) ? list.indexOf(v) : list.push(v) - 1)
  const layer = new Writer().varint(15 * 8).varint(2).bytes(1, new TextEncoder().encode(name))
  for (const feature of features) {
    const body = new Writer()
    if (feature.id !== undefined) body.varint(8).varint(feature.id)
    const tags = new Writer()
    for (const [k, v] of Object.entries(feature.properties)) tags.varint(index(keys, k)).varint(index(values, v))
    body.bytes(2, tags.done()).varint(3 * 8).varint(feature.type ?? 2)
    const geometry = new Writer()
    let x = 0, y = 0
    const zz = (n: number) => (n < 0 ? -2 * n - 1 : 2 * n)
    for (const line of feature.lines) {
      line.forEach(([px, py], i) => {
        if (i === 0) geometry.varint((1 << 3) | 1)
        if (i === 1) geometry.varint(((line.length - 1) << 3) | 2)
        geometry.varint(zz(px - x)).varint(zz(py - y))
        x = px
        y = py
      })
    }
    body.bytes(4, geometry.done())
    layer.bytes(2, body.done())
  }
  for (const k of keys) layer.bytes(3, new TextEncoder().encode(k))
  for (const v of values) {
    const value = new Writer()
    if (typeof v === 'string') value.bytes(1, new TextEncoder().encode(v))
    else if (typeof v === 'boolean') value.varint(7 * 8).varint(v ? 1 : 0)
    else if (Number.isInteger(v) && v >= 0) value.varint(5 * 8).varint(v)
    else {
      const b = new Uint8Array(8)
      new DataView(b.buffer).setFloat64(0, v, true)
      value.varint(3 * 8 + 1).raw(b)
    }
    layer.bytes(4, value.done())
  }
  layer.varint(5 * 8).varint(extent)
  return new Writer().bytes(3, layer.done()).done()
}
