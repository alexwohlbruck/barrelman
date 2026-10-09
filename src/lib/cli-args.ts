/**
 * Command-line options for the import scripts, which take only a few and do
 * not need a parser.
 */

/**
 * A `--name` option's value, given either as `--name value` or
 * `--name=value`. The console sends the second when the value starts with "-"
 * (see services/job-invocation.ts), because a bbox west of Greenwich would
 * otherwise read as another option, so a script that only understood the
 * first silently ignored every such area.
 */
export function argValue(args: string[], name: string): string | undefined {
  const eq = args.find((a) => a.startsWith(`--${name}=`))
  if (eq) return eq.slice(name.length + 3)
  const idx = args.indexOf(`--${name}`)
  return idx >= 0 && idx + 1 < args.length ? args[idx + 1] : undefined
}
