import axios from 'axios'
import {
  QUERY_EMBED_TIMEOUT_MS,
  BATCH_EMBED_TIMEOUT_MS,
  embeddingCircuit,
} from './embedding-circuit'

const OLLAMA_HOST = process.env.OLLAMA_HOST || 'http://localhost:11434'
const MODEL = 'nomic-embed-text'

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
