import type { ScriptDef, ResolvedChainStep } from './types'

/**
 * Resolve a script's `postScripts` ids against the manifest so the UI can name
 * the follow-ups instead of showing raw ids.
 *
 * The manifest is sent to the browser whole, so this is a local lookup rather
 * than a second request. An id with no match is dropped: the server-side test
 * (src/admin/scripts-manifest.test.ts) is what catches that, and rendering a
 * blank row here would be worse than rendering one fewer.
 */
export function resolveChain(script: ScriptDef | null, all: ScriptDef[]): ResolvedChainStep[] {
  if (!script?.postScripts?.length) return []
  const byId = new Map(all.map((s) => [s.id, s]))
  return script.postScripts.flatMap((step) => {
    const target = byId.get(step.script)
    if (!target) return []
    return [{ id: target.id, name: target.name, danger: target.danger, when: step.when }]
  })
}
