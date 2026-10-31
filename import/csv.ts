/**
 * CSV helpers shared by the import-time zip surgery steps.
 *
 * GTFS text files are CSV, and every step that rewrites one has to survive
 * quoted fields containing commas, quotes and newlines. Splitting on `,` and
 * rejoining works on the feeds we happen to import today and corrupts the
 * first one that quotes a field, so the parse and the serialize live here
 * together and are always used as a pair.
 */

/**
 * Parse CSV into rows of raw cells, header included as row 0.
 *
 * Row-level rather than object-level, because a rewrite has to put a file back
 * with its columns in the original order and its blank rows where they were.
 * Fields are returned untrimmed; `stop_times.txt` runs to hundreds of
 * thousands of rows and per-cell trimming is a measurable cost for callers
 * that only touch one column.
 */
export function parseCsvRows(text: string): string[][] {
  const rows: string[][] = []
  let field = ''
  let row: string[] = []
  let inQuotes = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i]
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++ } else inQuotes = false
      } else field += c
    } else if (c === '"') inQuotes = true
    else if (c === ',') { row.push(field); field = '' }
    else if (c === '\n') {
      row.push(field); field = ''
      rows.push(row); row = []
    } else if (c !== '\r') field += c
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row) }
  return rows
}

/** Quote a field if it contains anything that would otherwise break the row. */
export const escapeCsvField = (v: string): string =>
  /[",\n\r]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v

/** Serialize rows produced by `parseCsvRows`. */
export function serializeCsvRows(rows: string[][]): string {
  return rows.map(r => r.map(escapeCsvField).join(',')).join('\n') + '\n'
}

/**
 * Rewrite one column of a CSV file a line at a time.
 *
 * For files too large to hold as rows: stop_times.txt for a New York bus
 * borough is 2.4 million rows, and parseCsvRows + serializeCsvRows on it
 * peaked at 6.5 GB. Lines whose value doesn't change are copied through
 * verbatim, so their quoting and line endings survive untouched.
 *
 * `next` maps a trimmed value to its replacement, or undefined to leave it.
 * Returns the text unchanged (same string) when the column is absent.
 */
export function rewriteCsvColumn(
  text: string,
  column: string,
  next: (value: string) => string | undefined,
): { text: string; changed: number } {
  let pos = 0
  // One record from pos, as [start, end) of the source; quoted fields may span lines.
  const nextRecord = (): [number, number] | null => {
    if (pos >= text.length) return null
    const start = pos
    let quoted = false
    while (pos < text.length) {
      const c = text.charCodeAt(pos++)
      if (c === 34) quoted = !quoted // a "" escape toggles twice
      else if (c === 10 && !quoted) return [start, pos - 1]
    }
    return [start, pos]
  }
  const cellsOf = (line: string) => (line.includes('"') ? parseCsvRows(line)[0] ?? [] : line.split(','))

  const head = nextRecord()
  if (!head) return { text, changed: 0 }
  const col = headerIndex(cellsOf(text.slice(head[0], head[1]).replace(/\r$/, ''))).get(column)
  if (col === undefined) return { text, changed: 0 }

  const parts: string[] = []
  let copiedTo = 0
  let changed = 0
  for (let rec = nextRecord(); rec; rec = nextRecord()) {
    let line = text.slice(rec[0], rec[1])
    const cr = line.endsWith('\r')
    if (cr) line = line.slice(0, -1)
    if (!line) continue
    const cells = cellsOf(line)
    const value = cells[col]
    if (value === undefined) continue
    const replacement = next(value.trim())
    if (replacement === undefined || replacement === value) continue
    cells[col] = replacement
    parts.push(text.slice(copiedTo, rec[0]), cells.map(escapeCsvField).join(',') + (cr ? '\r' : ''))
    copiedTo = rec[1]
    changed++
  }
  if (!changed) return { text, changed: 0 }
  parts.push(text.slice(copiedTo))
  return { text: parts.join(''), changed }
}

/** Column index lookup for a header row, tolerating whitespace and a BOM. */
export function headerIndex(header: string[]): Map<string, number> {
  const index = new Map<string, number>()
  header.forEach((h, i) => index.set(h.trim().replace(/^﻿/, ''), i))
  return index
}

/** Minimal RFC-4180-ish parser: quoted fields, embedded commas, CRLF. */
export function parseCsv(text: string): Record<string, string>[] {
  const rows = parseCsvRows(text)
  if (!rows.length) return []
  const header = rows[0]!.map(h => h.trim().replace(/^﻿/, ''))
  return rows.slice(1).filter(r => r.some(v => v !== '')).map(r => {
    const obj: Record<string, string> = {}
    header.forEach((h, i) => { obj[h] = (r[i] ?? '').trim() })
    return obj
  })
}

export function toCsv(header: string[], rows: string[][]): string {
  return [header.join(','), ...rows.map(r => r.map(escapeCsvField).join(','))].join('\n') + '\n'
}
