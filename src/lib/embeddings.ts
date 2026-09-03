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
const QUERY_EMBED_TIMEOUT_MS = 2000

/** Batch embedding runs offline and can afford to wait. */
const BATCH_EMBED_TIMEOUT_MS = 30000

/**
 * When Ollama last refused, so a burst of searches does not each pay the
 * timeout over again. One request per cooldown probes whether it is back.
 */
const OUTAGE_COOLDOWN_MS = 60_000
let unreachableUntil = 0

/** Test seam: forget that Ollama was ever down. */
export function resetEmbeddingCircuit(): void {
  unreachableUntil = 0
}

export interface EmbeddingResult {
  embeddings: number[][]
}

/**
 * Generate embeddings for one or more texts using Ollama's nomic-embed-text model.
 * Returns an array of 512-dim float vectors.
 */
export async function generateEmbeddings(
  texts: string[],
  timeout = BATCH_EMBED_TIMEOUT_MS,
): Promise<number[][]> {
  const response = await axios.post<EmbeddingResult>(
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
export async function generateQueryEmbedding(query: string): Promise<number[]> {
  if (Date.now() < unreachableUntil) {
    throw new Error('Embedding service unreachable (cooling down)')
  }
  try {
    const results = await generateEmbeddings([query], QUERY_EMBED_TIMEOUT_MS)
    unreachableUntil = 0
    return results[0]
  } catch (err) {
    unreachableUntil = Date.now() + OUTAGE_COOLDOWN_MS
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
