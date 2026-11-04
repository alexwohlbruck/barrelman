/**
 * Licence terms published for GTFS and GBFS feeds, and the policy that decides
 * whether a feed may be served.
 *
 * Transitland records each feed's licence as a set of yes/no/unknown answers
 * (https://www.transit.land/documentation/rest-api/feeds). GBFS v2.3+ carries a
 * smaller set in `system_information`. Both are stored as published; this
 * module only reads them.
 */
import { envString } from '../config/env'

/** Transitland's tri-state licence answer. */
export type LicenseAnswer = 'yes' | 'no' | 'unknown'

export interface FeedLicense {
  spdxIdentifier: string | null
  url: string | null
  useWithoutAttribution: LicenseAnswer | null
  createDerivedProduct: LicenseAnswer | null
  redistributionAllowed: LicenseAnswer | null
  commercialUseAllowed: LicenseAnswer | null
  shareAlikeOptional: LicenseAnswer | null
  attributionText: string | null
  attributionInstructions: string | null
}

const text = (v: unknown): string | null => {
  if (typeof v !== 'string') return null
  const t = v.trim()
  return t === '' ? null : t
}

/**
 * Read one yes/no/unknown field. Transitland answers with strings, but a
 * boolean is accepted too so a hand-edited record means what it says.
 */
export function licenseAnswer(v: unknown): LicenseAnswer | null {
  if (v === true) return 'yes'
  if (v === false) return 'no'
  const t = text(v)?.toLowerCase()
  if (t === 'yes' || t === 'no' || t === 'unknown') return t
  return null
}

/**
 * The licence off a Transitland feed record, or null when the record carries
 * none at all. A record whose licence fields are all blank also returns null,
 * so "Transitland knows nothing" is stored the same way however it is spelled.
 */
export function parseTransitlandLicense(record: any): FeedLicense | null {
  const l = record?.license
  if (!l || typeof l !== 'object') return null
  const license: FeedLicense = {
    spdxIdentifier: text(l.spdx_identifier),
    url: text(l.url),
    useWithoutAttribution: licenseAnswer(l.use_without_attribution),
    createDerivedProduct: licenseAnswer(l.create_derived_product),
    redistributionAllowed: licenseAnswer(l.redistribution_allowed),
    commercialUseAllowed: licenseAnswer(l.commercial_use_allowed),
    shareAlikeOptional: licenseAnswer(l.share_alike_optional),
    attributionText: text(l.attribution_text),
    attributionInstructions: text(l.attribution_instructions),
  }
  return Object.values(license).some((v) => v !== null) ? license : null
}

export const LICENSE_POLICIES = ['off', 'commercial'] as const
export type LicensePolicy = (typeof LICENSE_POLICIES)[number]

/**
 * `GTFS_LICENCE_POLICY`: `off` records licences and serves every feed;
 * `commercial` stops serving feeds whose licence forbids commercial use or
 * redistribution.
 */
export function licensePolicy(): LicensePolicy {
  return envString('GTFS_LICENCE_POLICY', 'off', LICENSE_POLICIES)
}

/** The two answers the `commercial` policy reads. */
export type CommercialTerms = Pick<FeedLicense, 'commercialUseAllowed' | 'redistributionAllowed'>

/**
 * Whether a feed's licence says no to commercial use or to redistribution.
 *
 * Only an explicit `no` counts. A feed with no licence on record, or with
 * `unknown`, is served: most feeds in Transitland have never had their terms
 * filled in, and treating silence as refusal would drop most of the network.
 */
export function forbidsCommercialUse(terms: Partial<CommercialTerms> | null | undefined): boolean {
  return terms?.commercialUseAllowed === 'no' || terms?.redistributionAllowed === 'no'
}

/** Whether the policy in force keeps this feed out of what is served. */
export function isExcludedByPolicy(
  terms: Partial<CommercialTerms> | null | undefined,
  policy: LicensePolicy = licensePolicy(),
): boolean {
  return policy === 'commercial' && forbidsCommercialUse(terms)
}

/** One feed's credit line in a transit response. */
export interface FeedAttribution {
  feedId: string
  /** The feed's name as Transitland lists it, usually the agency. */
  agency: string | null
  /** The licence's own attribution text, when it publishes one. */
  text: string | null
  /** The licence URL, when it publishes one. */
  url: string | null
}

/**
 * GBFS licence and attribution fields from `system_information` (v2.3+).
 * All optional in the spec; most systems publish none.
 */
export interface GbfsLicense {
  licenseId: string | null
  licenseUrl: string | null
  attributionOrganizationName: string | null
  attributionUrl: string | null
}

/** Read the GBFS licence fields off a `system_information` payload's `data`. */
export function parseGbfsLicense(data: any): GbfsLicense | null {
  if (!data || typeof data !== 'object') return null
  // v3 localises text fields as [{ text, language }].
  const localized = (v: unknown): string | null =>
    Array.isArray(v) ? text(v[0]?.text) : text(v)
  const license: GbfsLicense = {
    licenseId: text(data.license_id),
    licenseUrl: text(data.license_url),
    attributionOrganizationName: localized(data.attribution_organization_name),
    attributionUrl: text(data.attribution_url),
  }
  return Object.values(license).some((v) => v !== null) ? license : null
}
