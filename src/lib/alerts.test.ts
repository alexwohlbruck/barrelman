import { describe, test, expect, mock } from 'bun:test'
import { createAlerter, formatAlert, type Alert } from './alerts'

const down: Alert = { key: 'dependency:motis', severity: 'critical', title: 'MOTIS is down', message: 'Probe failed.' }
const up: Alert = { ...down, severity: 'resolved', title: 'MOTIS recovered', message: 'Back.' }

function harness(format?: 'ntfy' | 'slack' | 'json', status = 200) {
  let clock = 1_000_000
  const fetchFn = mock(async (_url: string | URL | Request, _init?: RequestInit) => new Response('', { status }))
  const alerter = createAlerter({
    url: 'https://ntfy.example/barrelman',
    format,
    fetchFn: fetchFn as unknown as typeof fetch,
    now: () => clock,
    dedupeMs: 60_000,
  })
  return { alerter, fetchFn, advance: (ms: number) => (clock += ms) }
}

describe('formatAlert', () => {
  test('ntfy sends a text body with Title, Priority and Tags headers', () => {
    const { headers, body } = formatAlert(down, 'ntfy', 0)
    expect(body).toBe('Probe failed.')
    expect(headers.Title).toBe('MOTIS is down')
    expect(headers.Priority).toBe('urgent')
    expect(headers.Tags).toBe('rotating_light')
  })

  test('ntfy encodes a non-ASCII title instead of producing an invalid header', () => {
    const { headers } = formatAlert({ ...down, title: 'Zürich feed failed' }, 'ntfy', 0)
    expect(headers.Title).toStartWith('=?UTF-8?B?')
    expect(() => new Headers(headers)).not.toThrow()
  })

  test('slack sends { text } with a bold title', () => {
    expect(JSON.parse(formatAlert(down, 'slack', 0).body)).toEqual({ text: '*MOTIS is down*\nProbe failed.' })
  })

  test('json carries key, severity and an ISO timestamp', () => {
    const payload = JSON.parse(formatAlert(up, 'json', Date.UTC(2026, 0, 1)).body)
    expect(payload).toMatchObject({ source: 'barrelman', key: 'dependency:motis', severity: 'resolved' })
    expect(payload.timestamp).toBe('2026-01-01T00:00:00.000Z')
  })
})

describe('createAlerter', () => {
  test('POSTs to the webhook URL', async () => {
    const { alerter, fetchFn } = harness('ntfy')
    expect(await alerter.send(down)).toBe(true)
    const [url, init] = fetchFn.mock.calls[0]
    expect(url).toBe('https://ntfy.example/barrelman')
    expect(init?.method).toBe('POST')
    expect(init?.body).toBe('Probe failed.')
  })

  test('drops a repeat of the same alert inside the window, and sends it again after', async () => {
    const { alerter, fetchFn, advance } = harness()
    await alerter.send(down)
    expect(await alerter.send(down)).toBe(false)
    advance(60_000)
    expect(await alerter.send(down)).toBe(true)
    expect(fetchFn).toHaveBeenCalledTimes(2)
  })

  test('a per-call window overrides the default', async () => {
    const { alerter, fetchFn, advance } = harness()
    await alerter.send(down, { dedupeMs: 6 * 60 * 60_000 })
    advance(60 * 60_000)
    await alerter.send(down, { dedupeMs: 6 * 60 * 60_000 })
    expect(fetchFn).toHaveBeenCalledTimes(1)
  })

  test('down, up, down inside the window sends all three', async () => {
    const { alerter, fetchFn } = harness()
    await alerter.send(down)
    await alerter.send(up)
    await alerter.send(down)
    expect(fetchFn).toHaveBeenCalledTimes(3)
  })

  test('different keys are rate-limited independently', async () => {
    const { alerter, fetchFn } = harness()
    await alerter.send(down)
    await alerter.send({ ...down, key: 'dependency:martin' })
    expect(fetchFn).toHaveBeenCalledTimes(2)
  })

  test('never throws when the webhook is unreachable or refuses', async () => {
    const failing = createAlerter({
      url: 'https://x',
      fetchFn: (async () => {
        throw new Error('ECONNREFUSED')
      }) as unknown as typeof fetch,
    })
    expect(await failing.send(down)).toBe(false)

    const { alerter } = harness('json', 500)
    expect(await alerter.send(down)).toBe(false)
  })
})
