/**
 * CIDR parsing and matching for IPv4 and IPv6, used by the admin IP allowlist.
 *
 * Addresses are compared as byte arrays. An IPv4-mapped IPv6 address
 * (`::ffff:203.0.113.7`, which is how a dual-stack socket reports an IPv4
 * peer) is folded to its IPv4 form first, so an IPv4 range matches it.
 */

export interface Cidr {
  bytes: Uint8Array
  prefix: number
}

function parseIpv4(text: string): Uint8Array | null {
  const parts = text.split('.')
  if (parts.length !== 4) return null
  const out = new Uint8Array(4)
  for (let i = 0; i < 4; i++) {
    if (!/^\d{1,3}$/.test(parts[i])) return null
    const n = Number(parts[i])
    if (n > 255) return null
    out[i] = n
  }
  return out
}

function parseIpv6(text: string): Uint8Array | null {
  // A zone id (`fe80::1%eth0`) names an interface, not part of the address.
  const address = text.split('%')[0]
  if (!/^[0-9a-fA-F:.]+$/.test(address)) return null

  const halves = address.split('::')
  if (halves.length > 2) return null

  const groups = (part: string): number[] | null => {
    if (part === '') return []
    const out: number[] = []
    const pieces = part.split(':')
    for (let i = 0; i < pieces.length; i++) {
      const piece = pieces[i]
      // An embedded IPv4 tail (`::ffff:1.2.3.4`) is two groups.
      if (i === pieces.length - 1 && piece.includes('.')) {
        const v4 = parseIpv4(piece)
        if (!v4) return null
        out.push((v4[0] << 8) | v4[1], (v4[2] << 8) | v4[3])
        continue
      }
      if (!/^[0-9a-fA-F]{1,4}$/.test(piece)) return null
      out.push(parseInt(piece, 16))
    }
    return out
  }

  const head = groups(halves[0])
  const tail = halves.length === 2 ? groups(halves[1]) : []
  if (!head || !tail) return null

  let all: number[]
  if (halves.length === 2) {
    const fill = 8 - head.length - tail.length
    if (fill < 1) return null
    all = [...head, ...new Array(fill).fill(0), ...tail]
  } else {
    all = head
  }
  if (all.length !== 8) return null

  const out = new Uint8Array(16)
  all.forEach((g, i) => {
    out[i * 2] = g >> 8
    out[i * 2 + 1] = g & 0xff
  })
  return out
}

/** `::ffff:a.b.c.d` as its four IPv4 bytes; any other address unchanged. */
function unmapIpv4(bytes: Uint8Array): Uint8Array {
  if (bytes.length !== 16) return bytes
  for (let i = 0; i < 10; i++) if (bytes[i] !== 0) return bytes
  if (bytes[10] !== 0xff || bytes[11] !== 0xff) return bytes
  return bytes.slice(12)
}

/** Parse an IPv4 or IPv6 address. Returns null for anything else. */
export function parseIp(text: string): Uint8Array | null {
  const trimmed = text.trim().replace(/^\[|\]$/g, '')
  if (trimmed === '') return null
  const bytes = trimmed.includes(':') ? parseIpv6(trimmed) : parseIpv4(trimmed)
  return bytes ? unmapIpv4(bytes) : null
}

/**
 * Parse `203.0.113.0/24`, `2001:db8::/32`, or a bare address (a single host).
 * Returns null when the text is not a valid range.
 */
export function parseCidr(text: string): Cidr | null {
  const [address, prefixText, extra] = text.trim().split('/')
  if (extra !== undefined) return null
  const raw = address.includes(':') ? parseIpv6(address.trim()) : parseIpv4(address.trim())
  if (!raw) return null

  const max = raw.length * 8
  let prefix = max
  if (prefixText !== undefined) {
    if (!/^\d{1,3}$/.test(prefixText.trim())) return null
    prefix = Number(prefixText.trim())
    if (prefix > max) return null
  }

  // A mapped range (`::ffff:10.0.0.0/104`) is an IPv4 range in disguise.
  const bytes = unmapIpv4(raw)
  if (bytes.length === 4 && raw.length === 16) prefix = Math.max(0, prefix - 96)
  return { bytes, prefix }
}

export function cidrContains(range: Cidr, address: Uint8Array): boolean {
  if (range.bytes.length !== address.length) return false
  let bits = range.prefix
  for (let i = 0; i < address.length && bits > 0; i++) {
    const take = Math.min(8, bits)
    const mask = (0xff << (8 - take)) & 0xff
    if ((range.bytes[i] & mask) !== (address[i] & mask)) return false
    bits -= take
  }
  return true
}

export interface ParsedCidrList {
  ranges: Cidr[]
  invalid: string[]
}

/** Split a comma-separated list, keeping the entries that failed to parse for the log. */
export function parseCidrList(text: string): ParsedCidrList {
  const ranges: Cidr[] = []
  const invalid: string[] = []
  for (const entry of text.split(',').map((e) => e.trim()).filter(Boolean)) {
    const range = parseCidr(entry)
    if (range) ranges.push(range)
    else invalid.push(entry)
  }
  return { ranges, invalid }
}

/** Whether `ip` falls in any of `ranges`. An unparseable address never does. */
export function ipAllowed(ip: string, ranges: readonly Cidr[]): boolean {
  const address = parseIp(ip)
  if (!address) return false
  return ranges.some((range) => cidrContains(range, address))
}
