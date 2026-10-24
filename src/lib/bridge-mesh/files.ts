import { createHash } from 'node:crypto'
import { mkdir, writeFile, rename } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { envString } from '../../config/env'

export const BRIDGE_MESH_FILE = /^[0-9a-f]{64}\.brm$/
export const bridgeMeshDirectory = () => resolve(envString('BRIDGE_MESH_DIR', './data/bridge-meshes'))

/** Stored gzipped only: every client takes gzip, and the route inflates a copy for any that does not. */
export async function writeBridgeMesh(bytes: Uint8Array, directory = bridgeMeshDirectory()): Promise<string> {
  const name = `${createHash('sha256').update(bytes).digest('hex')}.brm`
  await mkdir(directory, { recursive: true })
  const temporary = join(directory, `${name}.${crypto.randomUUID()}.tmp`)
  await writeFile(temporary, Bun.gzipSync(bytes as Uint8Array<ArrayBuffer>, { level: 6 }))
  await rename(temporary, join(directory, `${name}.gz`))
  return name
}
