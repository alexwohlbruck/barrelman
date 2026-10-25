import { deckTileBuilder, type DeckTileBuilder } from './tile-pieces'

declare const self: Worker
let build: DeckTileBuilder | null = null

self.onmessage = async ({ data }: MessageEvent<{ directory?: string; id: number; z: number; x: number; y: number; files: string[] }>) => {
  if (data.directory !== undefined) {
    build = deckTileBuilder(data.directory)
    return
  }
  try {
    const { bytes, complete } = await build!(data.z, data.x, data.y, data.files)
    self.postMessage({ id: data.id, bytes, complete }, [bytes.buffer])
  } catch (error) {
    self.postMessage({ id: data.id, error: String(error) })
  }
}
