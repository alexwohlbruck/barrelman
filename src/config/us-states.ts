/**
 * ISO 3166-2 (US-XX) → Census FIPS state code.
 *
 * Pelias's address-interpolation step downloads TIGER data per state, keyed by
 * the numeric FIPS code (`imports.interpolation.download.tiger.states`), while
 * Geofabrik tags its US extracts with ISO 3166-2 codes. This table is the join
 * between them, so resolving a boundary can fill in `pelias.tigerStates`
 * automatically instead of making an operator look the code up.
 *
 * Source of truth: https://www2.census.gov/geo/docs/reference/state.txt
 * (FIPS state codes are stable — this does not need periodic refreshing.)
 */

export interface UsState {
  /** Numeric FIPS state code, e.g. 8 for Colorado. */
  fips: number
  name: string
}

export const US_STATES_BY_ISO: Record<string, UsState> = {
  'US-AL': { fips: 1, name: 'Alabama' },
  'US-AK': { fips: 2, name: 'Alaska' },
  'US-AZ': { fips: 4, name: 'Arizona' },
  'US-AR': { fips: 5, name: 'Arkansas' },
  'US-CA': { fips: 6, name: 'California' },
  'US-CO': { fips: 8, name: 'Colorado' },
  'US-CT': { fips: 9, name: 'Connecticut' },
  'US-DE': { fips: 10, name: 'Delaware' },
  'US-DC': { fips: 11, name: 'District of Columbia' },
  'US-FL': { fips: 12, name: 'Florida' },
  'US-GA': { fips: 13, name: 'Georgia' },
  'US-HI': { fips: 15, name: 'Hawaii' },
  'US-ID': { fips: 16, name: 'Idaho' },
  'US-IL': { fips: 17, name: 'Illinois' },
  'US-IN': { fips: 18, name: 'Indiana' },
  'US-IA': { fips: 19, name: 'Iowa' },
  'US-KS': { fips: 20, name: 'Kansas' },
  'US-KY': { fips: 21, name: 'Kentucky' },
  'US-LA': { fips: 22, name: 'Louisiana' },
  'US-ME': { fips: 23, name: 'Maine' },
  'US-MD': { fips: 24, name: 'Maryland' },
  'US-MA': { fips: 25, name: 'Massachusetts' },
  'US-MI': { fips: 26, name: 'Michigan' },
  'US-MN': { fips: 27, name: 'Minnesota' },
  'US-MS': { fips: 28, name: 'Mississippi' },
  'US-MO': { fips: 29, name: 'Missouri' },
  'US-MT': { fips: 30, name: 'Montana' },
  'US-NE': { fips: 31, name: 'Nebraska' },
  'US-NV': { fips: 32, name: 'Nevada' },
  'US-NH': { fips: 33, name: 'New Hampshire' },
  'US-NJ': { fips: 34, name: 'New Jersey' },
  'US-NM': { fips: 35, name: 'New Mexico' },
  'US-NY': { fips: 36, name: 'New York' },
  'US-NC': { fips: 37, name: 'North Carolina' },
  'US-ND': { fips: 38, name: 'North Dakota' },
  'US-OH': { fips: 39, name: 'Ohio' },
  'US-OK': { fips: 40, name: 'Oklahoma' },
  'US-OR': { fips: 41, name: 'Oregon' },
  'US-PA': { fips: 42, name: 'Pennsylvania' },
  'US-RI': { fips: 44, name: 'Rhode Island' },
  'US-SC': { fips: 45, name: 'South Carolina' },
  'US-SD': { fips: 46, name: 'South Dakota' },
  'US-TN': { fips: 47, name: 'Tennessee' },
  'US-TX': { fips: 48, name: 'Texas' },
  'US-UT': { fips: 49, name: 'Utah' },
  'US-VT': { fips: 50, name: 'Vermont' },
  'US-VA': { fips: 51, name: 'Virginia' },
  'US-WA': { fips: 53, name: 'Washington' },
  'US-WV': { fips: 54, name: 'West Virginia' },
  'US-WI': { fips: 55, name: 'Wisconsin' },
  'US-WY': { fips: 56, name: 'Wyoming' },
  'US-AS': { fips: 60, name: 'American Samoa' },
  'US-GU': { fips: 66, name: 'Guam' },
  'US-MP': { fips: 69, name: 'Northern Mariana Islands' },
  'US-PR': { fips: 72, name: 'Puerto Rico' },
  'US-UM': { fips: 74, name: 'U.S. Minor Outlying Islands' },
  'US-VI': { fips: 78, name: 'U.S. Virgin Islands' },
}

/** Two-letter lowercase state code ("co") from an ISO 3166-2 code ("US-CO"). */
export function isoToStateSlug(iso: string): string | null {
  const m = /^US-([A-Z]{2})$/.exec(iso.toUpperCase())
  return m ? m[1].toLowerCase() : null
}
