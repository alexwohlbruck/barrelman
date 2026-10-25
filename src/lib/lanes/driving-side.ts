import type { DrivingSide } from './cross-section'

/** Countries that drive on the left, as ISO 3166-1 alpha-3 (GraphHopper's `country`). */
const LEFT_HAND = new Set([
  'AIA', 'ATG', 'AUS', 'BGD', 'BHS', 'BMU', 'BRB', 'BRN', 'BTN', 'BWA', 'CCK', 'COK', 'CXR', 'CYM', 'CYP',
  'DMA', 'FJI', 'FLK', 'GBR', 'GGY', 'GRD', 'GUY', 'HKG', 'IDN', 'IMN', 'IND', 'IRL', 'JAM', 'JEY', 'JPN',
  'KEN', 'KIR', 'KNA', 'LCA', 'LKA', 'LSO', 'MAC', 'MDV', 'MLT', 'MOZ', 'MSR', 'MUS', 'MWI', 'MYS', 'NAM',
  'NFK', 'NIU', 'NPL', 'NRU', 'NZL', 'PAK', 'PCN', 'PNG', 'SGP', 'SHN', 'SLB', 'SUR', 'SWZ', 'SYC', 'TCA',
  'THA', 'TKL', 'TLS', 'TON', 'TTO', 'TUV', 'TZA', 'UGA', 'VCT', 'VGB', 'VIR', 'WSM', 'ZAF', 'ZMB', 'ZWE',
])

export function drivingSideFor(country: string | null | undefined): DrivingSide {
  return country && LEFT_HAND.has(country.toUpperCase()) ? 'left' : 'right'
}
