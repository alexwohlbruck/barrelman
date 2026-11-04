import type { operations } from './schema'
import { BarrelmanError } from './errors'

export const DEFAULT_BASE_URL = 'https://api.barrelman.dev'

// ---------------------------------------------------------------------------
// Request types, derived from the generated schema
// ---------------------------------------------------------------------------

type Op = keyof operations

/**
 * Query parameters arrive at the API as strings, so the spec types numeric
 * ones as `string`. Accept numbers and booleans too and stringify them, so
 * callers can pass `lat: 35.2` rather than `lat: '35.2'`.
 */
type Loose<T> = { [K in keyof T]: T[K] extends string | undefined ? T[K] | number | boolean : T[K] }

type QueryOf<O extends Op> = operations[O]['parameters'] extends { query?: infer Q }
  ? Loose<NonNullable<Q>>
  : never

type BodyOf<O extends Op> = operations[O] extends { requestBody: { content: { 'application/json': infer B } } }
  ? B
  : operations[O] extends { requestBody?: { content: { 'application/json': infer B } } }
    ? B
    : never

export type SearchParams = BodyOf<'postSearch'>
export type AutocompleteParams = Omit<SearchParams, 'autocomplete' | 'query'> & { query: string }
export type ReverseGeocodeParams = QueryOf<'getGeocodeReverse'>
export type HierarchyParams = QueryOf<'getGeocode'>
export type MatchParams = BodyOf<'postMatch'>
export type IsochroneParams = BodyOf<'postIsochrone'>
export type TransitRouteParams = BodyOf<'postTransitRoute'>
export type TransitStopsParams = QueryOf<'getTransitStops'>
export type TransitDeparturesParams = QueryOf<'getTransitDepartures'>

/**
 * A GraphHopper /route request. The API forwards it to GraphHopper, so any
 * GraphHopper parameter is accepted; the common ones are typed here.
 */
export interface RouteParams {
  /** `[lng, lat]` pairs, start to finish. */
  points: [number, number][]
  profile?: string
  locale?: string
  instructions?: boolean
  points_encoded?: boolean
  [parameter: string]: unknown
}

export type OsmType = 'node' | 'way' | 'relation'
export type StyleName = 'basemap' | 'basemap-dark'

/** A TileJSON 3.0.0 document, as `GET /tiles/<source>.json` returns it. */
export interface TileJSON {
  tilejson: string
  tiles: string[]
  name?: string
  attribution?: string
  minzoom?: number
  maxzoom?: number
  bounds?: [number, number, number, number]
  center?: [number, number, number]
  vector_layers?: { id: string; fields?: Record<string, string>; minzoom?: number; maxzoom?: number }[]
  [field: string]: unknown
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface RetryOptions {
  /** Attempts after the first. */
  retries: number
  /** First backoff delay, doubled on each retry. Defaults to 500 ms. */
  minDelayMs?: number
  /**
   * Ceiling on the backoff delay. Defaults to 10 s. A `Retry-After` longer
   * than this is not waited out: the error is thrown for the caller to handle.
   */
  maxDelayMs?: number
}

export interface ClientOptions {
  /** An account API key (`brm_live_…`). Sent as `Authorization: Bearer`. */
  apiKey: string
  /** Defaults to https://api.barrelman.dev. Point it at a self-hosted instance. */
  baseUrl?: string
  /** A fetch implementation. Defaults to the global `fetch`. */
  fetch?: typeof fetch
  /** Headers added to every request. */
  headers?: Record<string, string>
  /**
   * Retry 429 and 5xx responses with exponential backoff. Off by default,
   * since a retried request is charged again if it reaches the endpoint.
   * Pass a number for that many retries with the default delays.
   */
  retry?: number | RetryOptions
}

export interface RequestOptions {
  signal?: AbortSignal
  headers?: Record<string, string>
}

type QueryValue = string | number | boolean | undefined | null

export interface RawRequest extends RequestOptions {
  method?: 'GET' | 'POST'
  query?: Record<string, QueryValue>
  body?: unknown
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/**
 * Create a client for the Barrelman API.
 *
 * Response bodies are typed `unknown` by default because the OpenAPI spec does
 * not describe them yet. Pass the shape you expect as a type argument:
 * `client.search<MyResult[]>({ query: 'coffee' })`.
 */
export function createClient(options: ClientOptions) {
  if (!options?.apiKey) throw new TypeError('createClient: apiKey is required')

  const baseUrl = (options.baseUrl ?? DEFAULT_BASE_URL).replace(/\/+$/, '')
  const fetchImpl = options.fetch ?? ((...args: Parameters<typeof fetch>) => globalThis.fetch(...args))
  const retry = normaliseRetry(options.retry)
  const auth = `Bearer ${options.apiKey}`

  function url(path: string, query?: Record<string, QueryValue>): string {
    const search = new URLSearchParams()
    for (const [key, value] of Object.entries(query ?? {})) {
      if (value !== undefined && value !== null) search.set(key, String(value))
    }
    const qs = search.toString()
    return `${baseUrl}${path}${qs ? `?${qs}` : ''}`
  }

  async function request<T = unknown>(path: string, init: RawRequest = {}): Promise<T> {
    const method = init.method ?? (init.body === undefined ? 'GET' : 'POST')
    // Headers merges case-insensitively, so a caller's `Authorization` is
    // replaced rather than sent beside ours.
    const headers = new Headers({ accept: 'application/json' })
    for (const extra of [options.headers, init.headers]) {
      for (const [name, value] of Object.entries(extra ?? {})) headers.set(name, value)
    }
    headers.set('authorization', auth)
    if (init.body !== undefined) headers.set('content-type', 'application/json')
    const target = url(path, init.query)
    const body = init.body === undefined ? undefined : JSON.stringify(init.body)

    for (let attempt = 0; ; attempt++) {
      const response = await fetchImpl(target, { method, headers, body, signal: init.signal })
      if (response.ok) return (await parseBody(response)) as T

      const error = await BarrelmanError.fromResponse(response)
      const delay = attempt < retry.retries && isRetryable(error) ? retryDelay(error, attempt, retry) : undefined
      if (delay === undefined) throw error
      await sleep(delay, init.signal)
    }
  }

  /** Exact origin and path match, so `api.barrelman.dev.example.com` never gets the key. */
  const isOwnUrl = (candidate: string) =>
    candidate === baseUrl || candidate.startsWith(`${baseUrl}/`) || candidate.startsWith(`${baseUrl}?`)

  /** The key as a query string, for URLs a map library fetches by itself. */
  const keyQuery = (withKey: boolean) => (withKey ? { api_key: options.apiKey } : undefined)

  return {
    baseUrl,

    /** Call any endpoint. Paths are relative to `baseUrl`, e.g. `/brands`. */
    request,

    /** `POST /search`: places by text, category or tag, near a point or along a route. */
    search: <T = unknown>(params: SearchParams, opts?: RequestOptions) =>
      request<T>('/search', { ...opts, method: 'POST', body: params }),

    /** `POST /search` in autocomplete mode, which skips the semantic layer for low-latency typeahead. */
    autocomplete: <T = unknown>(params: AutocompleteParams, opts?: RequestOptions) =>
      request<T>('/search', { ...opts, method: 'POST', body: { ...params, autocomplete: true } }),

    geocode: {
      /** Text to places. The API has one search endpoint, so this is `search` with a query. */
      forward: <T = unknown>(query: string, params: Omit<SearchParams, 'query'> = {}, opts?: RequestOptions) =>
        request<T>('/search', { ...opts, method: 'POST', body: { ...params, query } }),
      /** `GET /geocode/reverse`: addressable features at a coordinate. */
      reverse: <T = unknown>(params: ReverseGeocodeParams, opts?: RequestOptions) =>
        request<T>('/geocode/reverse', { ...opts, query: params }),
      /** `GET /geocode`: the administrative hierarchy (city, county, state, country) at a coordinate. */
      hierarchy: <T = unknown>(params: HierarchyParams, opts?: RequestOptions) =>
        request<T>('/geocode', { ...opts, query: params }),
      /** `GET /geocode/place`: a geocoder place by its gid. */
      place: <T = unknown>(id: string, opts?: RequestOptions) =>
        request<T>('/geocode/place', { ...opts, query: { id } }),
    },

    /** `GET /place/{osmType}/{osmId}`: one OSM element with its tags and geometry. */
    place: <T = unknown>(osmType: OsmType, osmId: number | string, opts?: RequestOptions) =>
      request<T>(`/place/${encodeURIComponent(osmType)}/${encodeURIComponent(String(osmId))}`, opts),

    /** `POST /route`: a GraphHopper route, enriched with surface, road class and elevation details. */
    route: <T = unknown>(params: RouteParams, opts?: RequestOptions) =>
      request<T>('/route', { ...opts, method: 'POST', body: params }),

    /** `POST /isochrone`: areas reachable within the given durations. */
    isochrone: <T = unknown>(params: IsochroneParams, opts?: RequestOptions) =>
      request<T>('/isochrone', { ...opts, method: 'POST', body: params }),

    /** `POST /match`: snap a GPS trace to the road network. */
    match: <T = unknown>(params: MatchParams, opts?: RequestOptions) =>
      request<T>('/match', { ...opts, method: 'POST', body: params }),

    transit: {
      /** `POST /transit/route`: transit itineraries between two points. */
      route: <T = unknown>(params: TransitRouteParams, opts?: RequestOptions) =>
        request<T>('/transit/route', { ...opts, method: 'POST', body: params }),
      /** `GET /transit/stops`: stops near a point. */
      stops: <T = unknown>(params: TransitStopsParams, opts?: RequestOptions) =>
        request<T>('/transit/stops', { ...opts, query: params }),
      /** `GET /transit/departures`: departure boards near a point or for a stop. */
      departures: <T = unknown>(params: TransitDeparturesParams, opts?: RequestOptions) =>
        request<T>('/transit/departures', { ...opts, query: params }),
    },

    tiles: {
      /**
       * URL of a MapLibre style. Carries the key as `?api_key=` by default,
       * because a map library fetches the style, TileJSON and tiles itself and
       * the API copies a URL key on to each of them.
       */
      styleUrl: (name: StyleName = 'basemap', { withKey = true }: { withKey?: boolean } = {}) =>
        url(`/styles/${name}.json`, keyQuery(withKey)),
      /** URL of the TileJSON for a source, bundle (`detail`) or comma-joined composite. */
      tileJsonUrl: (source: string, { withKey = true }: { withKey?: boolean } = {}) =>
        url(`/tiles/${source}.json`, keyQuery(withKey)),
      /** A `{z}/{x}/{y}` tile URL template for a source. */
      tileUrlTemplate: (source: string, { withKey = true }: { withKey?: boolean } = {}) =>
        url(`/tiles/${source}/{z}/{x}/{y}`, keyQuery(withKey)).replace(/%7B([xyz])%7D/g, '{$1}'),
      /** Fetch a source's TileJSON with the Bearer header. Its tile URLs then carry no key. */
      tileJson: (source: string, opts?: RequestOptions) => request<TileJSON>(`/tiles/${source}.json`, opts),
      /**
       * A MapLibre `transformRequest` that adds the Bearer header to requests
       * for this API, for maps built from key-less URLs (`withKey: false`).
       */
      transformRequest: (requestUrl: string) =>
        isOwnUrl(requestUrl) ? { url: requestUrl, headers: { Authorization: auth } } : { url: requestUrl },
    },
  }
}

export type BarrelmanClient = ReturnType<typeof createClient>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function parseBody(response: Response): Promise<unknown> {
  if (response.status === 204) return undefined
  const type = response.headers.get('content-type') ?? ''
  if (type.includes('json')) return response.json()
  if (type.startsWith('text/')) return response.text()
  return response.arrayBuffer()
}

function normaliseRetry(retry: ClientOptions['retry']): Required<RetryOptions> {
  const base = typeof retry === 'number' ? { retries: retry } : (retry ?? { retries: 0 })
  return { minDelayMs: 500, maxDelayMs: 10_000, ...base, retries: Math.max(0, Math.floor(base.retries)) }
}

/**
 * 429 and 5xx are worth another try. `too_many_failures` is a 429 earned by
 * requests that could never succeed, such as a wrong key, so retrying it only
 * extends the pause.
 */
export function isRetryable(error: BarrelmanError): boolean {
  if (error.status === 429) return error.code !== 'too_many_failures'
  return error.status >= 500
}

/** Milliseconds to wait before the next attempt, or undefined to give up. */
export function retryDelay(error: BarrelmanError, attempt: number, retry: Required<RetryOptions>): number | undefined {
  if (error.retryAfter !== undefined) {
    const wait = error.retryAfter * 1000
    return wait <= retry.maxDelayMs ? wait : undefined
  }
  // Full jitter, so clients that failed together do not retry together.
  const ceiling = Math.min(retry.maxDelayMs, retry.minDelayMs * 2 ** attempt)
  return Math.random() * ceiling
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason)
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = () => {
      clearTimeout(timer)
      reject(signal!.reason)
    }
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}
