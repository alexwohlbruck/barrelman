export interface Arc {
  to: string
  cost: number
  /** Index into the graph's edge list, for rebuilding the path. */
  edge: number
  vertical: boolean
}

export type Adjacency = Map<string, Arc[]>

/** An adjacency flattened to integer ids, built once and shared by every search over it. */
interface Indexed {
  ids: string[]
  index: Map<string, number>
  start: Int32Array
  to: Int32Array
  cost: Float64Array
  edge: Int32Array
  vertical: Uint8Array
}

const indexed = new WeakMap<Adjacency, Indexed>()

function indexOf(adj: Adjacency): Indexed {
  const hit = indexed.get(adj)
  if (hit) return hit
  const index = new Map<string, number>()
  const id = (n: string) => {
    let i = index.get(n)
    if (i === undefined) { i = index.size; index.set(n, i) }
    return i
  }
  for (const [from, arcs] of adj) { id(from); for (const a of arcs) id(a.to) }
  const n = index.size
  const count = new Int32Array(n + 1)
  for (const [from, arcs] of adj) count[index.get(from)! + 1] += arcs.length
  for (let i = 0; i < n; i++) count[i + 1] += count[i]
  const m = count[n]
  const out: Indexed = {
    ids: [...index.keys()], index, start: count, to: new Int32Array(m), cost: new Float64Array(m),
    edge: new Int32Array(m), vertical: new Uint8Array(m),
  }
  const fill = count.slice()
  for (const [from, arcs] of adj) {
    for (const a of arcs) {
      const at = fill[index.get(from)!]++
      out.to[at] = index.get(a.to)!
      out.cost[at] = a.cost
      out.edge[at] = a.edge
      out.vertical[at] = a.vertical ? 1 : 0
    }
  }
  indexed.set(adj, out)
  return out
}

/** Union-find over a search graph, treating arcs as undirected. */
export function components(adj: Adjacency): (node: string) => string {
  const parent = new Map<string, string>()
  const find = (a: string): string => {
    let r = a
    while (parent.has(r)) r = parent.get(r)!
    while (a !== r) { const next = parent.get(a)!; parent.set(a, r); a = next }
    return r
  }
  for (const [a, arcs] of adj) {
    for (const arc of arcs) {
      const ra = find(a), rb = find(arc.to)
      if (ra !== rb) parent.set(ra, rb)
    }
  }
  return find
}

export interface Hop {
  node: string
  /** Edge used to arrive here; -1 at the source. */
  edge: number
}

export interface Tree {
  /** Cost to reach a node in a state that counts (climbed, when a climb is required). */
  cost(node: string): number | undefined
  /** Which source the cheapest path to node started from. */
  origin(node: string): string | undefined
  path(node: string): Hop[] | undefined
  /** Every node reached in a state that counts, with its cost. */
  reached(): Map<string, number>
}

/**
 * Plain Dijkstra from several sources at once. With `needClimb`, a node counts
 * as reached only after the path has used a vertical arc: an underground
 * platform has not reached the street until it climbs.
 */
export function shortestPaths(
  adj: Adjacency,
  sources: Iterable<string>,
  opts: { needClimb?: boolean; limit?: number } = {},
): Tree {
  const g = indexOf(adj)
  const limit = opts.limit ?? 1800
  const n = g.ids.length
  // State s = node * 2 + climbed.
  const dist = new Float64Array(2 * n).fill(Infinity)
  const prev = new Int32Array(2 * n).fill(-1)
  const via = new Int32Array(2 * n).fill(-1)
  const origin = new Int32Array(2 * n).fill(-1)
  const done = new Uint8Array(2 * n)
  const heapK: number[] = []
  const heapS: number[] = []
  const push = (k: number, s: number) => {
    let i = heapK.length
    heapK.push(k); heapS.push(s)
    while (i > 0) {
      const p = (i - 1) >> 1
      if (heapK[p] <= k) break
      heapK[i] = heapK[p]; heapS[i] = heapS[p]
      i = p
    }
    heapK[i] = k; heapS[i] = s
  }
  const pop = (): number => {
    const top = heapS[0]
    const k = heapK.pop()!, s = heapS.pop()!
    const len = heapK.length
    if (len) {
      let i = 0
      for (;;) {
        const l = 2 * i + 1, r = l + 1
        let m = i, mk = k
        if (l < len && heapK[l] < mk) { m = l; mk = heapK[l] }
        if (r < len && heapK[r] < mk) { m = r; mk = heapK[r] }
        if (m === i) break
        heapK[i] = heapK[m]; heapS[i] = heapS[m]
        i = m
      }
      heapK[i] = k; heapS[i] = s
    }
    return top
  }

  const climbed0 = opts.needClimb ? 0 : 1
  for (const src of sources) {
    const i = g.index.get(src)
    if (i === undefined) continue
    const s = i * 2 + climbed0
    if (dist[s] > 0) { dist[s] = 0; origin[s] = i; push(0, s) }
  }
  while (heapK.length) {
    const s = pop()
    if (done[s]) continue
    done[s] = 1
    const node = s >> 1, climbed = s & 1, d = dist[s]
    for (let a = g.start[node]; a < g.start[node + 1]; a++) {
      const nd = d + g.cost[a]
      if (nd > limit) continue
      const t = g.to[a] * 2 + (climbed | g.vertical[a])
      if (done[t] || nd >= dist[t]) continue
      dist[t] = nd; prev[t] = s; via[t] = g.edge[a]; origin[t] = origin[s]
      push(nd, t)
    }
  }

  const reached = (node: string) => {
    const i = g.index.get(node)
    return i !== undefined && done[i * 2 + 1] ? i * 2 + 1 : -1
  }
  return {
    cost: (node) => { const s = reached(node); return s < 0 ? undefined : dist[s] },
    origin: (node) => { const s = reached(node); return s < 0 ? undefined : g.ids[origin[s]] },
    reached() {
      const out = new Map<string, number>()
      for (let i = 0; i < n; i++) if (done[i * 2 + 1]) out.set(g.ids[i], dist[i * 2 + 1])
      return out
    },
    path(node) {
      let s = reached(node)
      if (s < 0) return undefined
      const hops: Hop[] = []
      while (s >= 0) { hops.push({ node: g.ids[s >> 1], edge: via[s] }); s = prev[s] }
      return hops.reverse()
    },
  }
}
