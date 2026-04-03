import axios from 'axios'

const OLLAMA_HOST = process.env.OLLAMA_HOST || 'http://localhost:11434'
const MODEL = 'nomic-embed-text'

export interface EmbeddingResult {
  embeddings: number[][]
}

/**
 * Generate embeddings for one or more texts using Ollama's nomic-embed-text model.
 * Returns an array of 512-dim float vectors.
 */
export async function generateEmbeddings(texts: string[]): Promise<number[][]> {
  const response = await axios.post<EmbeddingResult>(
    `${OLLAMA_HOST}/api/embed`,
    {
      model: MODEL,
      input: texts,
    },
    { timeout: 30000 },
  )

  return response.data.embeddings
}

/**
 * Generate a single embedding for a query string.
 */
export async function generateQueryEmbedding(query: string): Promise<number[]> {
  const results = await generateEmbeddings([query])
  return results[0]
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
