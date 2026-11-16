import { beforeEach, describe, expect, mock, test } from 'bun:test'

// Module mocks must stay in a subprocess so the shared test registry is untouched.
if (process.env.BARRELMAN_MAILER_TEST === '1') {

  const sendMail = mock(async (_options: any) => ({ messageId: 'test' }))
  mock.module('nodemailer', () => ({
    default: { createTransport: () => ({ sendMail }) },
  }))
  mock.module('../config/accounts.config', () => ({
    appName: 'Barrelman',
    otpTtlMinutes: 15,
    serverOrigin: 'https://api.barrelman.test',
    consoleOrigin: 'https://console.barrelman.test:443/console',
  }))

  const previousHost = process.env.SMTP_HOST
  process.env.SMTP_HOST = 'smtp.test'
  const { sendVerificationCode, sendAccountDeletionCode, sendLowCreditsWarning } = await import('./mailer.service')
  if (previousHost === undefined) delete process.env.SMTP_HOST
  else process.env.SMTP_HOST = previousHost

  beforeEach(() => sendMail.mockClear())

  describe('OTP email compatibility', () => {
    test('sends readable code and metadata bound to the console origin', async () => {
      expect(await sendVerificationCode('user@example.com', '00123456')).toBe(true)
      expect(sendMail.mock.calls[0][0]).toMatchObject({
        subject: 'Your Barrelman verification code',
        text: expect.stringContaining('00123456'),
        html: expect.stringContaining('00123456'),
        headers: { 'OTP-Token': '"00123456"; origin="https://console.barrelman.test"' },
      })
    })

    test('includes metadata for account deletion codes', async () => {
      await sendAccountDeletionCode('user@example.com', '12345678')
      expect(sendMail.mock.calls[0][0].headers).toEqual({
        'OTP-Token': '"12345678"; origin="https://console.barrelman.test"',
      })
    })

    test('does not attach OTP metadata to billing emails', async () => {
      await sendLowCreditsWarning('user@example.com', 100, 90)
      expect(sendMail.mock.calls[0][0].headers).toBeUndefined()
    })
  })
} else {
  test('OTP email delivery metadata', async () => {
    const child = Bun.spawn([process.execPath, 'test', import.meta.path], {
      env: { ...process.env, BARRELMAN_MAILER_TEST: '1' },
      stdout: 'pipe',
      stderr: 'pipe',
    })
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    expect(exitCode, stdout + stderr).toBe(0)
  })
}
