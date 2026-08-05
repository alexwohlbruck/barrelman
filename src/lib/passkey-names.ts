/**
 * Human-readable names for newly registered passkeys.
 *
 * Users should never have to invent a label for a credential, so one is derived
 * from the authenticator itself. Every authenticator reports an AAGUID
 * identifying its make — that is the good signal, and the table below covers
 * the handful that account for nearly all real registrations. Anything else
 * falls back to the browser and platform, which is vaguer but still tells the
 * user which device they are looking at in the list.
 */

/** AAGUIDs of the common consumer authenticators, from the FIDO MDS. */
const KNOWN_AAGUIDS: Record<string, string> = {
  'adce0002-35bc-c60a-648b-0b25f1f05503': 'Chrome on Mac',
  'fbfc3007-154e-4ecc-8c0b-6e020557d7bd': 'iCloud Keychain',
  '08987058-cadc-4b81-b6e1-30de50dcbe96': 'Windows Hello',
  '9ddd1817-af5a-4672-a2b9-3e3dd95000a9': 'Windows Hello',
  '6028b017-b1d4-4c02-b4b3-afcdafc96bb2': 'Windows Hello',
  'dd4ec289-e01d-41c9-bb89-70fa845d4bf2': 'iCloud Keychain',
  'bada5566-a7aa-401f-bd96-45619a55120d': '1Password',
  'b84e4048-15dc-4dd0-8640-f4f60813c8af': 'NordPass',
  '0ea242b4-43c4-4a1b-8b17-dd6d0b6baec6': 'Keeper',
  '891494da-2c90-4d31-a9cd-4eab0aed1309': 'Dashlane',
  'd548826e-79b4-db40-a3d8-11116f7e8349': 'Bitwarden',
  'cb69481e-8ff7-4039-93ec-0a2729a154a8': 'YubiKey 5',
  'ee882879-721c-4913-9775-3dfcce97072a': 'YubiKey 5',
  'fa2b99dc-9e39-4257-8f92-4a30d23c4118': 'YubiKey 5 NFC',
  '2fc0579f-8113-47ea-b116-bb5a8db9202a': 'YubiKey 5 NFC',
  '73bb0cd4-e502-49b8-9c6f-b59445bf720b': 'YubiKey 5 FIPS',
  'b93fd961-f2e6-462f-b122-82002247de78': 'Android',
}

/** Rough platform/browser read of a UA string — enough to tell devices apart. */
function describeUserAgent(userAgent: string): string {
  const platform = /iPhone|iPad|iPod/i.test(userAgent)
    ? 'iOS'
    : /Android/i.test(userAgent)
      ? 'Android'
      : /Macintosh|Mac OS X/i.test(userAgent)
        ? 'macOS'
        : /Windows/i.test(userAgent)
          ? 'Windows'
          : /Linux/i.test(userAgent)
            ? 'Linux'
            : null

  // Order matters: Edge and Chrome both claim "Chrome", Chrome claims "Safari".
  const browser = /Edg\//i.test(userAgent)
    ? 'Edge'
    : /OPR\//i.test(userAgent)
      ? 'Opera'
      : /Firefox\//i.test(userAgent)
        ? 'Firefox'
        : /Chrome\//i.test(userAgent)
          ? 'Chrome'
          : /Safari\//i.test(userAgent)
            ? 'Safari'
            : null

  if (platform && browser) return `${browser} on ${platform}`
  return platform ?? browser ?? 'Passkey'
}

export function passkeyNameFor(aaguid: string | undefined, userAgent?: string | null): string {
  // An all-zero AAGUID means "declined to identify itself" — common for
  // privacy-preserving platform authenticators — so treat it as unknown.
  if (aaguid && aaguid !== '00000000-0000-0000-0000-000000000000') {
    const known = KNOWN_AAGUIDS[aaguid.toLowerCase()]
    if (known) return known
  }
  return userAgent ? describeUserAgent(userAgent) : 'Passkey'
}
