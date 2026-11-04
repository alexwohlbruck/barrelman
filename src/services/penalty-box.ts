/**
 * The penalty box's state, with enough bookkeeping to persist it.
 *
 * Checks stay in memory: nothing here touches the database. What it adds over
 * a plain Map is a record of which entries changed since the last flush, and a
 * way to fold in rows another replica (or this one, before a restart) wrote.
 * `penalty-sync.service.ts` moves the rows; this class decides what they mean.
 *
 * Rows are identified by a keyed hash of the penalty key, never the key itself.
 * Keys carry the caller's address (`user@ip`, `ip:ip`), and nothing else in the
 * accounts tables stores a raw IP — see `hashIp()`. The cost is that a row
 * loaded from the database cannot be matched to a key until a request presents
 * that key, so loaded rows wait in `remote` until a lookup hashes its way to
 * them. That hash is only computed while `remote` is non-empty.
 *
 * Only entries that have been boxed at least once are persisted. Strikes short
 * of the threshold rebuild in seconds against a caller that is actually
 * misbehaving, and persisting every stray 401 would turn a key-guesser into a
 * stream of writes.
 */

export interface Penalty {
  strikes: number
  /** Refused until this timestamp (ms). Zero if this run of strikes never reached the box. */
  until: number
  lastStrikeAt: number
}

/** A penalty as stored: `id` is the hashed penalty key. */
export interface PenaltyRow extends Penalty {
  id: string
}

export interface PenaltyBoxOptions {
  /** Rejections tolerated before a caller is boxed. */
  strikesBeforePenalty: number
  /** Strikes decay after this long without a new one. */
  decayMs: number
  maxPenaltyMs: number
  /** Turns a penalty key into the id it is stored under. */
  hash: (key: string) => string
}

/** Still worth keeping: boxed now, or holding strikes that have not decayed. */
export function isLivePenalty(p: Penalty, now: number, decayMs: number): boolean {
  return p.until > now || now - p.lastStrikeAt <= decayMs
}

/**
 * Combine what this process knows with a stored row for the same caller. The
 * stricter of the two wins on every field: two replicas boxing the same caller
 * should not have one of them shorten the other's penalty. A local entry whose
 * strikes have decayed is stale and simply replaced.
 */
export function mergePenalty(local: Penalty | undefined, stored: Penalty, now: number, decayMs: number): Penalty {
  if (!local || !isLivePenalty(local, now, decayMs)) {
    return { strikes: stored.strikes, until: stored.until, lastStrikeAt: stored.lastStrikeAt }
  }
  return {
    strikes: Math.max(local.strikes, stored.strikes),
    until: Math.max(local.until, stored.until),
    lastStrikeAt: Math.max(local.lastStrikeAt, stored.lastStrikeAt),
  }
}

export interface PenaltyBatch {
  upserts: PenaltyRow[]
  /** Ids whose rows should go: the caller succeeded after its box expired. */
  deletes: string[]
  /** The keys behind `upserts`, so a failed write can be queued again. */
  keys: string[]
}

export class PenaltyBox {
  /** By penalty key. Every check reads this. */
  private local = new Map<string, Penalty>()
  /** By id: loaded rows no request has matched to a key yet. */
  private remote = new Map<string, Penalty>()
  /** key → id, for every key with a local entry. */
  private ids = new Map<string, string>()
  private dirty = new Set<string>()
  private removed = new Set<string>()

  constructor(private readonly options: PenaltyBoxOptions) {}

  private idFor(key: string): string {
    let id = this.ids.get(key)
    if (!id) {
      id = this.options.hash(key)
      this.ids.set(key, id)
    }
    return id
  }

  /** Local entry, or a loaded row adopted under this key on first sight. */
  private lookup(key: string): Penalty | undefined {
    const found = this.local.get(key)
    if (found || this.remote.size === 0) return found

    // Not cached through idFor: a miss here is every clean request while some
    // other caller's row is loaded, and caching those would grow without bound.
    const id = this.ids.get(key) ?? this.options.hash(key)
    const stored = this.remote.get(id)
    if (!stored) return undefined

    this.remote.delete(id)
    this.local.set(key, stored)
    this.ids.set(key, id)
    return stored
  }

  recordRejection(key: string, now = Date.now()): void {
    const { strikesBeforePenalty, decayMs, maxPenaltyMs } = this.options
    // Every local entry gets its id now, not when it is first boxed: that is
    // what lets applyStored() match another replica's box to a caller this
    // replica holds only a few strikes for. A rejection is not the hot path,
    // so the hash costs nothing that matters.
    this.idFor(key)
    const existing = this.lookup(key)

    if (!existing || now - existing.lastStrikeAt > decayMs) {
      this.local.set(key, { strikes: 1, until: 0, lastStrikeAt: now })
      return
    }

    existing.strikes += 1
    existing.lastStrikeAt = now

    if (existing.strikes >= strikesBeforePenalty) {
      const over = existing.strikes - strikesBeforePenalty
      existing.until = now + Math.min(maxPenaltyMs, 5_000 * 2 ** Math.min(over, 12))
    }
    if (existing.until > 0) this.dirty.add(key)
  }

  recordSuccess(key: string, now = Date.now()): void {
    const existing = this.local.get(key)
    if (!existing || existing.until > now) return

    this.local.delete(key)
    this.dirty.delete(key)
    const id = this.ids.get(key)
    if (id) {
      this.removed.add(id)
      this.ids.delete(key)
    }
  }

  remaining(key: string, now = Date.now()): number {
    const existing = this.lookup(key)
    if (!existing || existing.until <= now) return 0
    return Math.ceil((existing.until - now) / 1000)
  }

  strikes(key: string, now = Date.now()): number {
    const existing = this.lookup(key)
    if (!existing || now - existing.lastStrikeAt > this.options.decayMs) return 0
    return existing.strikes
  }

  /** Drain what changed since the last call. */
  takeDirty(): PenaltyBatch {
    const keys: string[] = []
    const upserts: PenaltyRow[] = []
    for (const key of this.dirty) {
      const p = this.local.get(key)
      if (!p) continue
      keys.push(key)
      upserts.push({ id: this.idFor(key), strikes: p.strikes, until: p.until, lastStrikeAt: p.lastStrikeAt })
    }
    const deletes = [...this.removed]
    this.dirty.clear()
    this.removed.clear()
    return { upserts, deletes, keys }
  }

  /** Put a batch back after its write failed, so the next flush retries it. */
  requeue(batch: PenaltyBatch): void {
    for (const key of batch.keys) if (this.local.has(key)) this.dirty.add(key)
    for (const id of batch.deletes) this.removed.add(id)
  }

  /**
   * Fold in rows read from the database. Does not mark anything dirty: these
   * rows are already stored, and echoing them back would be a write per row
   * per replica per refresh.
   */
  applyStored(rows: PenaltyRow[], now = Date.now()): void {
    const { decayMs } = this.options
    const keyById = new Map<string, string>()
    for (const [key, id] of this.ids) keyById.set(id, key)

    for (const row of rows) {
      if (!isLivePenalty(row, now, decayMs)) continue
      const key = keyById.get(row.id)
      if (key) this.local.set(key, mergePenalty(this.local.get(key), row, now, decayMs))
      else this.remote.set(row.id, mergePenalty(this.remote.get(row.id), row, now, decayMs))
    }
  }

  prune(now = Date.now()): void {
    const { decayMs } = this.options
    for (const [key, p] of this.local) {
      if (isLivePenalty(p, now, decayMs)) continue
      this.local.delete(key)
      this.dirty.delete(key)
      this.ids.delete(key)
    }
    for (const [id, p] of this.remote) {
      if (!isLivePenalty(p, now, decayMs)) this.remote.delete(id)
    }
  }

  penalisedCount(now = Date.now()): number {
    let n = 0
    for (const p of this.local.values()) if (p.until > now) n += 1
    for (const p of this.remote.values()) if (p.until > now) n += 1
    return n
  }

  clear(): void {
    this.local.clear()
    this.remote.clear()
    this.ids.clear()
    this.dirty.clear()
    this.removed.clear()
  }
}
