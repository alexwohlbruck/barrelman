import pkg from '../../package.json' with { type: 'json' }

/**
 * Wikimedia Commons metadata for brand logo files: the licence, the author and
 * where to download the file from.
 *
 * Wikidata's P154 names a Commons file. Commons answers `imageinfo` with the
 * licence read off the file's description page (`extmetadata`), which is what
 * a map showing the logo has to credit. Up to 50 titles go in one request.
 */

/**
 * Wikimedia's User-Agent policy asks for a client name, version and a way to
 * reach the operator. Requests without one are throttled or refused.
 * https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
 */
export const WIKIMEDIA_UA =
  `Barrelman/${pkg.version} (https://barrelman.dev; https://github.com/alexwohlbruck/barrelman/issues) brand-logos`

/** Titles per imageinfo request; the API's limit for ordinary clients. */
export const COMMONS_BATCH = 50
const MAX_ATTEMPTS = 4

/** Thumbnail width asked of Commons for raster files and oversized SVGs. */
const THUMB_WIDTH = 256
/** An SVG larger than this is fetched as Commons' PNG thumbnail instead. */
export const MAX_SVG_BYTES = 512 * 1024

export interface LogoLicense {
  /** Short licence name as Commons gives it: "Public domain", "CC BY-SA 4.0". */
  name: string | null
  url: string | null
  /** Plain-text author, from the file page's Artist field. */
  artist: string | null
  attributionRequired: boolean
  publicDomain: boolean
  /** Commons marks most logos "trademarked": free to copy, still a trademark. */
  trademarked: boolean
}

export interface LogoInfo extends LogoLicense {
  /** The Commons file page, which is where the full terms live. */
  pageUrl: string | null
  /** What to download: the original SVG, or Commons' PNG thumbnail. */
  downloadUrl: string | null
}

const ENTITIES: Record<string, string> = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ' }

/**
 * Plain text from an extmetadata value, which is HTML. Commons hides
 * duplicates in `display: none` spans ("Unknown source<span style="display:
 * none;">Unknown source</span>"), so those are dropped before the tags.
 */
export function stripHtml(html: string | null | undefined): string | null {
  if (!html) return null
  const text = html
    .replace(/<span[^>]*display:\s*none[^>]*>[\s\S]*?<\/span>/gi, '')
    .replace(/<[^>]+>/g, '')
    .replace(/&(#\d+|#x[0-9a-f]+|[a-z]+);/gi, (m, e: string) => {
      if (e[0] === '#') {
        const code = e[1] === 'x' || e[1] === 'X' ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10)
        return Number.isFinite(code) ? String.fromCodePoint(code) : m
      }
      return ENTITIES[e.toLowerCase()] ?? m
    })
    .replace(/\s+/g, ' ')
    .trim()
  return text || null
}

type ExtMetadata = Record<string, { value?: unknown } | undefined>

function field(meta: ExtMetadata, name: string): string | null {
  const v = meta[name]?.value
  return typeof v === 'string' && v.trim() ? v.trim() : null
}

/** The licence fields of one file's extmetadata. */
export function parseLogoLicense(meta: ExtMetadata | null | undefined): LogoLicense {
  const m = meta ?? {}
  const name = stripHtml(field(m, 'LicenseShortName'))
  const code = field(m, 'License')?.toLowerCase() ?? ''
  const publicDomain = code === 'pd' || code.startsWith('pd-') || code === 'cc0' || /public domain/i.test(name ?? '')
  return {
    name,
    url: field(m, 'LicenseUrl'),
    artist: stripHtml(field(m, 'Artist')),
    // Commons answers "true" or "false". When it says nothing, assume credit is
    // owed: crediting a public-domain file costs nothing, omitting a required
    // credit breaks its licence.
    attributionRequired: field(m, 'AttributionRequired') !== 'false',
    publicDomain,
    trademarked: /trademark/i.test(field(m, 'Restrictions') ?? ''),
  }
}

/** The Commons file name inside a stored Special:FilePath URL. */
export function logoFileFromUrl(url: string | null | undefined): string | null {
  if (!url) return null
  const m = /\/Special:FilePath\/([^?#]+)/.exec(url)
  if (!m) return null
  try {
    return decodeURIComponent(m[1]).replace(/_/g, ' ')
  } catch {
    return null
  }
}

/** Commons treats "a_b.svg" and "a b.svg" as one title, and the first letter as uppercase. */
export function normalizeTitle(file: string): string {
  const t = file.replace(/^File:/i, '').replace(/_/g, ' ').trim()
  return t.charAt(0).toUpperCase() + t.slice(1)
}

/**
 * The URL to download: an SVG's original when it is small enough, otherwise
 * the thumbnail Commons renders. Barrelman passes files through unchanged and
 * never rasterises one itself.
 */
export function pickDownload(ii: { mime?: string; size?: number; url?: string; thumburl?: string }): string | null {
  if (ii.mime === 'image/svg+xml' && ii.url && (ii.size ?? 0) <= MAX_SVG_BYTES) return ii.url
  return ii.thumburl ?? ii.url ?? null
}

export async function politeFetch(url: string, fetchFn: typeof fetch = fetch): Promise<Response | null> {
  let res: Response | null = null
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    res = await fetchFn(url, { headers: { 'User-Agent': WIKIMEDIA_UA } })
    if (res.ok || (res.status !== 429 && res.status < 500)) return res
    const retryAfter = Number(res.headers.get('retry-after'))
    const waitMs = retryAfter > 0 ? retryAfter * 1000 : 1000 * 2 ** attempt
    await new Promise((r) => setTimeout(r, Math.min(waitMs, 60_000)))
  }
  return res
}

/**
 * Licence and download URL for up to COMMONS_BATCH files, keyed by the name
 * each was asked for. A file Commons does not have maps to null; a file left
 * out of the map was not answered (the request failed) and should be asked
 * for again later.
 */
export async function fetchLogoInfo(files: string[], fetchFn: typeof fetch = fetch): Promise<Map<string, LogoInfo | null>> {
  const out = new Map<string, LogoInfo | null>()
  if (files.length === 0) return out
  const titles = files.map((f) => `File:${normalizeTitle(f)}`)
  const url =
    'https://commons.wikimedia.org/w/api.php?action=query&format=json&formatversion=2' +
    '&prop=imageinfo&iiprop=url|size|mime|extmetadata' +
    `&iiurlwidth=${THUMB_WIDTH}` +
    '&iiextmetadatafilter=LicenseShortName|LicenseUrl|License|Artist|AttributionRequired|Restrictions' +
    `&titles=${titles.map(encodeURIComponent).join('|')}`
  const res = await politeFetch(url, fetchFn).catch(() => null)
  if (!res?.ok) return out
  const data = (await res.json().catch(() => null)) as any
  const pages: any[] = data?.query?.pages ?? []
  const byTitle = new Map<string, any>(pages.map((p) => [p.title, p]))
  // Commons may still rewrite a title (Unicode normalisation, namespace case).
  const renamed = new Map<string, string>(
    (data?.query?.normalized ?? []).map((n: { from: string; to: string }) => [n.from, n.to]),
  )
  for (const [i, file] of files.entries()) {
    const page = byTitle.get(renamed.get(titles[i]) ?? titles[i])
    if (!page) continue
    const ii = page.imageinfo?.[0]
    if (page.missing || !ii) {
      out.set(file, null)
      continue
    }
    out.set(file, {
      ...parseLogoLicense(ii.extmetadata),
      pageUrl: ii.descriptionurl ?? null,
      downloadUrl: pickDownload(ii),
    })
  }
  return out
}
