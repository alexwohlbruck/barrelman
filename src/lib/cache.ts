import { LRUCache } from 'lru-cache'

// Spatial queries: 1h TTL
export const spatialCache = new LRUCache<string, any>({
  max: 5000,
  ttl: 60 * 60 * 1000,
})

// Search results: 5min TTL
export const searchCache = new LRUCache<string, any>({
  max: 2000,
  ttl: 5 * 60 * 1000,
})

// Query embeddings: 1h TTL
export const embeddingCache = new LRUCache<string, number[]>({
  max: 1000,
  ttl: 60 * 60 * 1000,
})
