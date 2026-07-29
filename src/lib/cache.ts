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

// GraphHopper isochrone polygons: 1h TTL. Keyed by profile + rounded point +
// duration + direction. Transit isochrones fan out to hundreds of per-stop
// walk polygons and keep asking for the same (stop, duration) pairs, so this
// is what keeps a repeat query cheap. The street graph only changes on
// re-import, hence the generous TTL.
export const isochroneCache = new LRUCache<string, any>({
  max: 20000,
  ttl: 60 * 60 * 1000,
})

// Query embeddings: 1h TTL
export const embeddingCache = new LRUCache<string, number[]>({
  max: 1000,
  ttl: 60 * 60 * 1000,
})
