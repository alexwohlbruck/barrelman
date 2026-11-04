/**
 * Validation of the params an operator (or a schedule) passes to a manifest
 * script, against that param's declared type and format.
 *
 * Process jobs turn params into argv and env for a shell or bun script, so an
 * unchecked value is an injection point: `--feeds` was free text, a positional
 * was split on spaces into as many arguments as it had words, and a value like
 * `--output=/etc/x` became a flag of its own. Every value now has to match its
 * manifest entry, a leading `-` is refused unless the param allows it, and a
 * positional is one argv element (or one per item, for a declared list).
 *
 * Shared by the API (when a job or schedule is created) and the ops worker
 * (when a job runs) through `buildInvocation()`.
 */
import type { ParamFormat, ScriptDef, ScriptParam } from '../admin/scripts-manifest'

/** A param value the manifest doesn't allow. The message names the param. */
export class ParamValidationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'ParamValidationError'
  }
}

const IDENTIFIER = /^[A-Za-z0-9_.:@+~-]+$/
const REGION = /^[a-z0-9][a-z0-9-]*$/
const PATH = /^[A-Za-z0-9_./-]+$/
const DATE = /^\d{4}-\d{2}-\d{2}$/
const INTEGER = /^\d+$/
const SIGNED_INTEGER = /^-?\d+$/
const DECIMAL = /^(\d+(\.\d+)?|\.\d+)$/
const SIGNED_DECIMAL = /^-?(\d+(\.\d+)?|\.\d+)$/
const SINGLE_LINE = /^[^\x00-\x1f\x7f]+$/
const MAX_LENGTH = 500

export function isEmptyParam(val: unknown): boolean {
  return val === undefined || val === null || val === ''
}

/** Why `value` doesn't match `format`, or null when it does. */
export function formatError(format: ParamFormat, value: string): string | null {
  switch (format) {
    case 'identifier':
      return IDENTIFIER.test(value) ? null : 'may only contain letters, digits and _ . : @ + ~ -'
    case 'identifiers':
      return value.split(',').every((v) => IDENTIFIER.test(v.trim()) && !v.trim().startsWith('-'))
        ? null
        : 'must be a comma-separated list of names (letters, digits and _ . : @ + ~ -)'
    case 'region':
      return REGION.test(value) ? null : 'must be a region key (lowercase letters, digits and dashes)'
    case 'regions':
      return value.split(',').every((v) => REGION.test(v.trim()))
        ? null
        : 'must be comma-separated region keys (lowercase letters, digits and dashes)'
    case 'path':
      if (!PATH.test(value)) return 'may only contain letters, digits and _ . / -'
      if (value.startsWith('/')) return 'must be a relative path'
      if (value.split('/').includes('..')) return 'must not contain ".."'
      return null
    case 'bbox': {
      const parts = value.split(',').map((v) => v.trim())
      return parts.length === 4 && parts.every((v) => SIGNED_DECIMAL.test(v))
        ? null
        : 'must be four comma-separated numbers: west,south,east,north'
    }
    case 'date': {
      if (!DATE.test(value)) return 'must be a date as YYYY-MM-DD'
      // Round-trips only for a real calendar day, so 2026-02-30 fails.
      const parsed = new Date(`${value}T00:00:00Z`)
      return !Number.isNaN(parsed.getTime()) && parsed.toISOString().startsWith(value) ? null : 'is not a real date'
    }
    case 'decimal':
      return DECIMAL.test(value) ? null : 'must be a number'
    case 'text':
      // Spaces are harmless in a single argv element. Newlines and control
      // characters are not, once the value lands in a log line or an `echo`.
      return SINGLE_LINE.test(value) ? null : 'must be a single line of text'
  }
}

/**
 * Check one param value against its manifest entry and return the string(s)
 * to pass on: one for most params, one per item for a `multiple` positional.
 * Throws ParamValidationError otherwise.
 */
export function validateParam(p: ScriptParam, val: unknown): string[] {
  const fail = (why: string): never => {
    throw new ParamValidationError(`${p.label || p.name}: ${why}`)
  }

  if (p.type === 'boolean') {
    if (val === true || val === false || val === 'true' || val === 'false') return [String(val)]
    return fail('must be true or false')
  }

  if (typeof val !== 'string' && typeof val !== 'number') return fail('must be text or a number')
  const str = String(val).trim()
  if (str.length > MAX_LENGTH) return fail(`must be at most ${MAX_LENGTH} characters`)

  if (p.type === 'select') {
    const allowed = (p.options ?? []).map((o) => o.value)
    return allowed.includes(str) ? [str] : fail(`must be one of ${allowed.join(', ')}`)
  }

  // A leading dash reads as an option to nearly every argument parser. A
  // bbox's west longitude is legitimately negative; buildInvocation writes
  // such a flag value as `--flag=-109.06,...` so it can't be read as one.
  const dashAllowed = p.allowDash === true || p.format === 'bbox'
  const items = p.apply === 'positional' && p.multiple ? str.split(/\s+/) : [str]

  for (const item of items) {
    if (item.startsWith('-') && !dashAllowed) fail('must not start with "-"')
    if (p.type === 'number') {
      const pattern = p.decimal
        ? dashAllowed
          ? SIGNED_DECIMAL
          : DECIMAL
        : dashAllowed
          ? SIGNED_INTEGER
          : INTEGER
      if (!pattern.test(item)) fail(p.decimal ? 'must be a number' : 'must be a whole number')
    } else {
      const why = formatError(p.format ?? 'identifier', item)
      if (why) fail(why)
    }
  }
  return items
}

/**
 * Validate every param a script declares. Keys the script doesn't declare are
 * ignored, as they always were: nothing reads them. Throws on the first bad
 * value.
 */
export function validateParams(script: ScriptDef, params: Record<string, unknown>): void {
  for (const p of script.params ?? []) {
    const val = params[p.name]
    if (isEmptyParam(val)) {
      if (p.required) throw new ParamValidationError(`${p.label || p.name}: required`)
      continue
    }
    validateParam(p, val)
  }
}
