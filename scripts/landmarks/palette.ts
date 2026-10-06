/**
 * The shared landmark material palette.
 *
 * Names and base colours follow the Open Landmarks material library
 * (materials.json, schema 1), so a model made here can be contributed there
 * unchanged and lights the same way at night: Parchment glows `window*` and
 * `entrance` by name. Use these names, not new ones, for anything they fit.
 *
 * `finish()` covers the colours that are part of a building's identity —
 * Charlotte's rose granite, a green copper dome, painted steel — as a named
 * landmark-specific material. Keep those muted: the same lightness range as
 * `stone` and `roof`, so a landmark still sits among the pale extrusions
 * around it. STYLE.md says when one is justified.
 */
export type Swatch = { name: string; color: number; roughness?: number }

export const PALETTE = {
  /** Masonry and render; the map's own light building colour. */
  stone: { name: 'stone', color: 0xefe4d3, roughness: 0.85 },
  /** Cornices, frames, mullion lines and lighter details. */
  trim: { name: 'trim', color: 0xfff0dd, roughness: 0.85 },
  /** Flat and pitched roofs, setback terraces, mechanical floors. */
  roof: { name: 'roof', color: 0x9da6ad, roughness: 0.85 },
  /** Structural and decorative metal: frames, fins, spires, railings. */
  metal: { name: 'metal', color: 0x99816b, roughness: 0.85 },
  /** Doors and lit entrance surfaces; glow at night. */
  entrance: { name: 'entrance', color: 0xfff0cd, roughness: 0.85 },
  /** Windows on walls: panels and bands; glow warm at night. */
  window: { name: 'window', color: 0x64798a, roughness: 0.35 },
  /** Structural glazing: domes, atria, skylights, glass roofs. Never glows. */
  glass: { name: 'glass', color: 0xa4c4d9, roughness: 0.24 },
  copper: { name: 'copper', color: 0x80a28d, roughness: 0.85 },
  patina: { name: 'patina', color: 0x6a947f, roughness: 0.85 },
  terracotta: { name: 'terracotta', color: 0xad6854, roughness: 0.9 },
} satisfies Record<string, Swatch>

/** A landmark-specific finish, e.g. `finish('boa-granite', 0xd9cbc1)`. */
export function finish(name: string, color: number, roughness = 0.85): Swatch {
  if (!/^[a-z0-9]+(-[a-z0-9]+)*$/.test(name)) throw new Error(`finish name must be a slug: ${name}`)
  return { name, color, roughness }
}

/** A second window colour on one model needs its own name: `window-2`, `window-3`. */
export function windowVariant(n: number, color: number): Swatch {
  return { name: `window-${n}`, color, roughness: 0.35 }
}
