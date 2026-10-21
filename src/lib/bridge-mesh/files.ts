import { createHash } from 'node:crypto'
import { mkdir, writeFile, rename } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { envString } from '../../config/env'

export const BRIDGE_MESH_FILE = /^[0-9a-f]{64}\.brm$/
export const bridgeMeshDirectory = () => resolve(envString('BRIDGE_MESH_DIR', './data/bridge-meshes'))

export async function writeBridgeMesh(bytes: Uint8Array, directory = bridgeMeshDirectory()): Promise<string> {
  const name = `${createHash('sha256').update(bytes).digest('hex')}.brm`
  await mkdir(directory, { recursive: true })
  const temporary = join(directory, `${name}.${crypto.randomUUID()}.tmp`)
  await writeFile(temporary, bytes)
  await rename(temporary, join(directory, name))
  const compressed = `${temporary}.gz`
  await writeFile(compressed, Bun.gzipSync(bytes as Uint8Array<ArrayBuffer>, { level: 6 }))
  await rename(compressed, join(directory, `${name}.gz`))
  return name
}
