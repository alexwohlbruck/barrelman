import axios from 'axios'

const OLLAMA_HOST = process.env.OLLAMA_HOST || 'http://localhost:11434'
const MODEL = 'nomic-embed-text'

/**
 * How long a QUERY embedding is worth waiting for.
 *
 * Thirty seconds was the same timeout the batch importer uses, and on the
 * request path it is not a timeout at all — it is an outage. An instance
 * without Ollama running answered every semantic search in exactly 30s: the
 * search itself finished in milliseconds and then sat waiting for a vector it
 * was never going to get, until the caller's own timeout fired first and the
 * request failed outright. Two seconds is past the point where a semantic
 * result would still feel like part of the same search.
 */
export const QUERY_EMBED_TIMEOUT_MS = 2000

/** Batch embedding runs offline and can afford to wait. */
const BATCH_EMBED_TIMEOUT_MS = 30000

/**
 * When Ollama last refused, so a burst of searches does not each pay the
 * timeout over again. One request per cooldown probes whether it is back.
 */
const OUTAGE_COOLDOWN_MS = 60_000
let unreachableUntil = 0

/**
 * The "is it down" memory, as its own unit.
 *
 * Exported because `generateQueryEmbedding` cannot be observed from another
 * suite: `search.service.test.ts` module-mocks it, and bun's `mock.module` is
 * process-global, so any test of the real wrapper sees the mock instead.
 */
export const embeddingCircuit = {
  isOpen: () => Date.now() < unreachableUntil,
  trip: () => {
    unreachableUntil = Date.now() + OUTAGE_COOLDOWN_MS
  },
  reset: () => {
    unreachableUntil = 0
  },
}

export interface EmbeddingResult {
  embeddings: number[][]
}

/** The POST this module makes, as a seam. Injected rather than module-mocked:
 *  a `mock.module('axios')` would replace axios for every test file loaded
 *  afterwards, not just this one. */
export type EmbeddingPoster = (
  url: string,
  body: unknown,
  opts: { timeout?: number },
) => Promise<{ data: EmbeddingResult }>

const defaultPoster: EmbeddingPoster = (url, body, opts) =>
  axios.post<EmbeddingResult>(url, body, opts)

/**
 * Generate embeddings for one or more texts using Ollama's nomic-embed-text model.
 * Returns an array of 512-dim float vectors.
 */
export async function generateEmbeddings(
  texts: string[],
  timeout = BATCH_EMBED_TIMEOUT_MS,
  post: EmbeddingPoster = defaultPoster,
): Promise<number[][]> {
  const response = await post(
    `${OLLAMA_HOST}/api/embed`,
    {
      model: MODEL,
      input: texts,
    },
    { timeout },
  )

  return response.data.embeddings
}

/**
 * Generate a single embedding for a query string.
 */
export async function generateQueryEmbedding(
  query: string,
  post: EmbeddingPoster = defaultPoster,
): Promise<number[]> {
  if (embeddingCircuit.isOpen()) {
    throw new Error('Embedding service unreachable (cooling down)')
  }
  try {
    const results = await generateEmbeddings([query], QUERY_EMBED_TIMEOUT_MS, post)
    embeddingCircuit.reset()
    return results[0]
  } catch (err) {
    embeddingCircuit.trip()
    throw err
  }
}

/**
 * Build the embedding input string for a place.
 * Format: "name · categories · description · cuisine · operator · location context"
 *
 * Location context comes from parent_context (resolved admin boundary names +
 * address fields) with a fallback to addr:city/country for places that haven't
 * been through the parent context resolution step.
 */
export function buildEmbeddingInput(place: {
  name?: string | null
  categories?: string[] | null
  address?: { city?: string; country?: string } | null
  osmTags?: Record<string, string> | null // legacy alias
  tags?: Record<string, string> | null
  parentContext?: string | null
}): string {
  const t = place.tags || place.osmTags
  const locationContext = place.parentContext
    || [(place.address as any)?.city, (place.address as any)?.country].filter(Boolean).join(' ')
  const parts = [
    place.name,
    place.categories?.join(', '),
    t?.description,
    t?.cuisine,
    t?.operator,
    locationContext || null,
  ]
  return parts.filter(Boolean).join(' · ')
}
