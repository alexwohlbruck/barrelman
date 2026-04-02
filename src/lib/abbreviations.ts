const STOP_WORDS = new Set([
  'of', 'the', 'and', 'at', 'in', 'for', 'a', 'an',
  'de', 'la', 'le', 'les', 'du', 'des', 'et', 'au',
  'der', 'die', 'das', 'von', 'und', 'im', 'am',
  'del', 'los', 'las', 'el', 'dos', 'das', 'e',
  'di', 'del', 'della', 'dei', 'degli',
])

/**
 * Generate an abbreviation for Latin-script names.
 * e.g. "University of North Carolina" → "unc"
 *      "Technische Universität München" → "tum"
 *      "北京大学" → null (non-Latin, rely on OSM abbreviation tag)
 */
export function generateAbbrev(name: string): string | null {
  if (!/^[\p{Script=Latin}\s\d\-'\.&]+$/u.test(name)) return null

  const tokens = name
    .split(/\s+/)
    .filter((w) => w.length > 0 && !STOP_WORDS.has(w.toLowerCase()))

  if (tokens.length < 2) return null

  return tokens.map((w) => w[0].toLowerCase()).join('')
}

const CODE_TAGS = ['iata', 'icao', 'ref', 'short_name', 'abbreviation']

export function extractCodes(tags: Record<string, string>): string[] {
  const codes: string[] = []
  for (const key of CODE_TAGS) {
    const val = tags[key]
    if (val) {
      codes.push(...val.split(';').map(v => v.trim().toLowerCase()).filter(Boolean))
    }
  }
  if (tags.alt_name) {
    codes.push(...tags.alt_name.split(';').map(v => v.trim().toLowerCase()).filter(Boolean))
  }
  return [...new Set(codes)]
}
