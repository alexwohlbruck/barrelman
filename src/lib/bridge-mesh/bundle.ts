/**
 * BRT1: every deck mesh a map tile needs, in one response, so a client loads a
 * tile's decks with the tile rather than one request per deck after it.
 *
 *   u32 magic 'BRT1', u32 count, then per mesh: 32-byte sha256 name, u32 length,
 *   the BRM bytes, padded to 4.
 *
 * Little-endian. A mesh's name is the hash its file is stored under, so a
 * client that already holds a mesh can skip it.
 */
export const BUNDLE_MAGIC = 0x31545242
/** The one zoom bundles are cut at: the lowest the bridge_decks tiles carry. */
export const BUNDLE_ZOOM = 14

const align = (n: number) => Math.ceil(n / 4) * 4

export function packBundle(meshes: Array<{ name: string; bytes: Uint8Array }>): Uint8Array {
  const size = 8 + meshes.reduce((n, m) => n + 36 + align(m.bytes.byteLength), 0)
  const out = new Uint8Array(size)
  const view = new DataView(out.buffer)
  view.setUint32(0, BUNDLE_MAGIC, true)
  view.setUint32(4, meshes.length, true)
  let at = 8
  for (const { name, bytes } of meshes) {
    out.set(Buffer.from(name.slice(0, 64), 'hex'), at)
    view.setUint32(at + 32, bytes.byteLength, true)
    out.set(bytes, at + 36)
    at += 36 + align(bytes.byteLength)
  }
  return out
}
