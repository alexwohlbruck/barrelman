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
