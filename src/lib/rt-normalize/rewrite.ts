import { existsSync, readFileSync, statSync } from 'fs'
import type { StaticIndex } from './static-index'
import { decodeFeed, encodeFeed, indexFromZip } from './feed-codec'
import { normalizeFeed, type NormalizeStats } from './normalize'
import { indexCalls, type LiveFeed } from './exact-times'
import { repairAlertSelectors } from './alert-selectors'

export interface NormalizedFeed {
  body: Uint8Array
  /** Null when the feed was passed through unrewritten. */
  stats: NormalizeStats | null
}

/** A view that owns its whole buffer, so the buffer can be transferred without detaching anyone else's bytes. */
export function owned(bytes: Uint8Array): Uint8Array {
  return bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength ? bytes : bytes.slice()
}

export interface RewriteJob {
  /** The static feed MOTIS loaded, which the realtime is matched against. */
  zipPath: string
  /** The realtime message as the upstream sent it. */
  raw: Uint8Array
  normalize: boolean
}

/** The CPU half of the relay: decode, match to the timetable, encode, and index the calls in a message. */
export interface Rewriter {
  rewrite(job: RewriteJob): Promise<NormalizedFeed>
  calls(body: Uint8Array): Promise<LiveFeed>
}

export function createRewriter(): Rewriter {
  // The zip on disk is the one MOTIS loaded at its last rebuild; reload when it changes.
  const indexes = new Map<string, { mtimeMs: number; index: Promise<StaticIndex> }>()

  function staticIndex(path: string): Promise<StaticIndex> {
    if (!existsSync(path)) return Promise.reject(new Error(`no static feed at ${path}`))
    const { mtimeMs } = statSync(path)
    const cached = indexes.get(path)
    if (cached?.mtimeMs === mtimeMs) return cached.index
    const index = indexFromZip(readFileSync(path))
    indexes.set(path, { mtimeMs, index })
    index.catch(() => indexes.delete(path))
    return index
  }

  return {
    async rewrite({ zipPath, raw, normalize }) {
      const { message } = repairAlertSelectors(decodeFeed(raw))
      if (!normalize) return { body: encodeFeed(message), stats: null }
      try {
        const { message: out, stats } = normalizeFeed(await staticIndex(zipPath), message)
        return { body: encodeFeed(out), stats }
      } catch (err) {
        // Unrewritten realtime still resolves what it always did.
        console.warn(`[rt-normalize] ${zipPath}: passing through unrewritten: ${err instanceof Error ? err.message : err}`)
        return { body: encodeFeed(message), stats: null }
      }
    },
    async calls(body) {
      return indexCalls(decodeFeed(body))
    },
  }
}
