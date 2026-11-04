import { createHash } from 'node:crypto'
import { sql } from 'drizzle-orm'
import { db } from '../db'
import { serverOrigin } from '../config/accounts.config'
import { queryableQid } from '../lib/brand-logos'
import { fetchLogoInfo, logoFileFromUrl, politeFetch } from '../lib/commons-logo'
import { etagOf } from '../lib/static-response'

/**
 * Brand logo images, served from barrelman rather than hotlinked from Commons.
 *
 * The first request for a logo downloads it from Commons and stores the bytes
 * in brand_logo_files; every later one is answered from there. Files pass
 * through unchanged: an SVG stays an SVG (unless Commons' original is too big,
 * when its PNG thumbnail is taken instead, see pickDownload).
 */

export interface LogoFile {
  contentType: string
  bytes: Uint8Array
  etag: string
}

/** Formats we pass on. Anything else Commons hands back is refused. */
const ALLOWED_TYPES = new Set(['image/svg+xml', 'image/png', 'image/jpeg', 'image/gif', 'image/webp'])
/** A logo larger than this is not a logo; refuse it rather than store it. */
const MAX_BYTES = 1024 * 1024

/** Short, stable hash of the file name, used as cache-buster and ETag seed. */
function fileVersion(file: string): string {
  return createHash('sha1').update(file).digest('hex').slice(0, 10)
}

/**
 * The URL a response gives for a brand's logo. Versioned by file name, so when
 * Wikidata points a brand at a new logo the URL changes and long-lived caches
 * fetch the new one.
 */
export function brandLogoUrl(wikidata: string | null, file: string | null, origin: string = serverOrigin): string | null {
  const qid = wikidata ? queryableQid(wikidata) : null
  if (!qid || !file) return null
  return `${origin}/brand-logos/${qid}?v=${fileVersion(file)}`
}

interface LogoRow {
  logo_file: string
  logo_download_url: string | null
}

type RawRow = { logo_file: string | null; logo_url: string | null; logo_download_url: string | null }

/** Downloads in progress, so a burst of requests for one logo fetches it once. */
const inflight = new Map<string, Promise<LogoFile | null>>()
/** Files Commons failed to give us, and when, so a missing logo is not re-fetched per request. */
const failedAt = new Map<string, number>()
const RETRY_FAILED_MS = 10 * 60_000

export async function getBrandLogoFile(qid: string): Promise<LogoFile | null> {
  if (queryableQid(qid) !== qid) return null
  // Rows are keyed by the raw brand:wikidata value, which can list several ids.
  const raw = Array.from(
    (await db.execute(sql`
      SELECT logo_file, logo_url, logo_download_url FROM brand_logos
      WHERE (wikidata = ${qid} OR wikidata LIKE ${qid + ';%'}) AND logo_url IS NOT NULL
      ORDER BY wikidata = ${qid} DESC
      LIMIT 1
    `)) as unknown as RawRow[],
  )[0]
  // Rows resolved before logo_file existed name their file only in logo_url.
  const file = raw ? raw.logo_file ?? logoFileFromUrl(raw.logo_url) : null
  if (!raw || !file) return null
  const row: LogoRow = { logo_file: file, logo_download_url: raw.logo_download_url }

  const stored = Array.from(
    (await db.execute(sql`
      SELECT content_type, bytes, etag FROM brand_logo_files WHERE file = ${row.logo_file}
    `)) as unknown as { content_type: string; bytes: Uint8Array; etag: string }[],
  )[0]
  if (stored) return { contentType: stored.content_type, bytes: stored.bytes, etag: stored.etag }

  const pending = inflight.get(row.logo_file)
  if (pending) return pending
  if (Date.now() - (failedAt.get(row.logo_file) ?? 0) < RETRY_FAILED_MS) return null
  const p = downloadAndStore(row)
    .catch(() => null)
    .then((f) => {
      if (f) failedAt.delete(row.logo_file)
      else failedAt.set(row.logo_file, Date.now())
      return f
    })
    .finally(() => inflight.delete(row.logo_file))
  inflight.set(row.logo_file, p)
  return p
}

async function downloadAndStore(row: LogoRow): Promise<LogoFile | null> {
  let url = row.logo_download_url
  if (!url) {
    // Resolved before the licence columns existed and not yet backfilled.
    url = (await fetchLogoInfo([row.logo_file])).get(row.logo_file)?.downloadUrl ?? null
  }
  if (!url) return null

  const res = await politeFetch(url).catch(() => null)
  if (!res?.ok) return null
  const contentType = (res.headers.get('content-type') ?? '').split(';')[0].trim().toLowerCase()
  if (!ALLOWED_TYPES.has(contentType)) return null
  const bytes = new Uint8Array(await res.arrayBuffer())
  if (bytes.byteLength === 0 || bytes.byteLength > MAX_BYTES) return null

  const etag = etagOf(bytes)
  await db.execute(sql`
    INSERT INTO brand_logo_files (file, content_type, bytes, etag)
    VALUES (${row.logo_file}, ${contentType}, ${Buffer.from(bytes)}, ${etag})
    ON CONFLICT (file) DO NOTHING
  `)
  return { contentType, bytes, etag }
}
