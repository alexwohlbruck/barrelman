/**
 * Timeouts and outage memory for the embedding service.
 *
 * Its own module on purpose. `search.service.test.ts` module-mocks
 * `./embeddings`, and bun's `mock.module` is process-global — so anything
 * living in that file is either invisible to other suites or forces them to
 * import a mocked module, which is how a test-ordering failure gets into a
 * release build. Nothing mocks this file, so it can be tested directly.
 */

/**
 * How long a QUERY embedding is worth waiting for.
 *
 * Thirty seconds is the batch importer's timeout, and on the request path it
 * is not a timeout at all — it is an outage. An instance without Ollama
 * running answered every semantic search in exactly 30s: the search itself
 * finished in milliseconds and then sat waiting for a vector it was never
 * going to get, until the caller's own timeout fired and the request failed
 * outright. Two seconds is past the point where a semantic result would still
 * feel like part of the same search.
 */
export const QUERY_EMBED_TIMEOUT_MS = 2000

/** Batch embedding runs offline and can afford to wait. */
export const BATCH_EMBED_TIMEOUT_MS = 30000

/** How long a refusal is remembered, so a burst of searches does not each pay
 *  the timeout over again. One request per cooldown probes whether it is back. */
export const OUTAGE_COOLDOWN_MS = 60_000

let unreachableUntil = 0

/** Whether the embedding service is currently believed to be down. */
export const embeddingCircuit = {
  isOpen: () => Date.now() < unreachableUntil,
  trip: () => {
    unreachableUntil = Date.now() + OUTAGE_COOLDOWN_MS
  },
  reset: () => {
    unreachableUntil = 0
  },
}
