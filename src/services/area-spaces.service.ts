/**
 * The open spaces the routing graph's area paths cross, read from the files
 * rebuild-graphhopper.sh keeps beside the graph (area-spaces.ndjson and its
 * index), so they always describe the graph being served.
 */
import { stat } from 'node:fs/promises'
import { join } from 'node:path'
import type { Space } from '../lib/area-paths/bends'

const RECHECK_MS = 60_000
const MAX_CACHED = 2_000

type Entry = [first: number, last: number, offset: number, length: number]

const GRAPH_DIR = '/data/graph-cache'

function entryFor(entries: Entry[], wayId: number): Entry | undefined {
  let lo = 0, hi = entries.length - 1
  while (lo <= hi) {
    const mid = (lo + hi) >> 1
    if (wayId < entries[mid][0]) hi = mid - 1
    else if (wayId > entries[mid][1]) lo = mid + 1
    else return entries[mid]
  }
  return undefined
}

/** Reads the spaces kept in `dir`, re-reading the index when the graph beside it is swapped. */
export function spaceReader(dir: string) {
  let loaded: { at: number; mtime: number; index: Entry[] } | null = null
  let loading: Promise<Entry[]> | null = null
  const cache = new Map<number, Space>()

  async function index(): Promise<Entry[]> {
    if (loaded && Date.now() - loaded.at < RECHECK_MS) return loaded.index
    loading ??= (async () => {
      try {
        const file = join(dir, 'area-spaces.index.json')
        const mtime = (await stat(file)).mtimeMs
        if (loaded?.mtime !== mtime) {
          cache.clear()
          loaded = { at: Date.now(), mtime, index: await Bun.file(file).json() as Entry[] }
        } else loaded.at = Date.now()
      } catch {
        cache.clear()
        loaded = { at: Date.now(), mtime: 0, index: [] }
      }
      return loaded.index
    })().finally(() => { loading = null })
    return loading
  }

  /** The space each of these area-path ways crosses; one object per space, so ways in one space share it. */
  return async function spacesFor(wayIds: number[]): Promise<Map<number, Space>> {
    const found = new Map<number, Space>()
    if (!wayIds.length) return found
    const entries = await index()
    const data = Bun.file(join(dir, 'area-spaces.ndjson'))
    const wanted = new Set(wayIds.map(id => entryFor(entries, id)).filter((e): e is Entry => !!e))
    await Promise.all([...wanted].map(async entry => {
      let space = cache.get(entry[0])
      if (!space) {
        space = { rings: JSON.parse(await data.slice(entry[2], entry[2] + entry[3]).text()).rings }
        if (cache.size >= MAX_CACHED) cache.delete(cache.keys().next().value!)
        cache.set(entry[0], space)
      }
      for (const id of wayIds) if (id >= entry[0] && id <= entry[1]) found.set(id, space)
    }))
    return found
  }
}

export const spacesFor = spaceReader(GRAPH_DIR)
