/**
 * The reconciliation sweep is seeded as a schedule on every instance, so it
 * has to be harmless where billing is off: no Polar client, no queries.
 */
import { describe, test, expect } from 'bun:test'
import { reconcileBilling } from './billing-reconcile.service'
import { filterMatchesEvent } from './billing.service'
import { billing } from '../config/billing.config'

describe('reconcileBilling', () => {
  test('does nothing while billing is disabled', async () => {
    expect(billing.enabled).toBe(false)
    const lines: string[] = []

    const result = await reconcileBilling((line) => lines.push(line))

    expect(result).toEqual({ accounts: 0, synced: 0, failed: 0, drift: 0 })
    expect(lines).toEqual(['Billing is not enabled on this instance; nothing to reconcile.'])
  })
})

describe('filterMatchesEvent', () => {
  test('finds the usage event name in a meter filter, nested or not', () => {
    const flat = { conjunction: 'and', clauses: [{ property: 'name', operator: 'eq', value: 'barrelman_credits' }] }
    const nested = {
      conjunction: 'and',
      clauses: [{ conjunction: 'or', clauses: [{ property: 'name', operator: 'eq', value: 'barrelman_credits' }] }],
    }
    const other = { conjunction: 'and', clauses: [{ property: 'name', operator: 'eq', value: 'something_else' }] }

    expect(filterMatchesEvent(flat, 'barrelman_credits')).toBe(true)
    expect(filterMatchesEvent(nested, 'barrelman_credits')).toBe(true)
    expect(filterMatchesEvent(other, 'barrelman_credits')).toBe(false)
  })
})
