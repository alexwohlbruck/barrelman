/**
 * Outbound email. Only transactional sign-in mail today.
 *
 * SMTP is optional: with no `SMTP_HOST` configured the transport is never
 * created and codes are written to the log instead. That keeps `./start.sh dev`
 * working with no mail server, and it means a misconfigured production instance
 * degrades to "sign-in codes appear in the logs" rather than crashing on boot —
 * which is why `isEmailConfigured` is exported for the health surface to warn on.
 */
import nodemailer from 'nodemailer'
import { appName, serverOrigin } from '../config/accounts.config'

const host = process.env.SMTP_HOST
const port = Number(process.env.SMTP_PORT || 465)
const secure = process.env.SMTP_SECURE !== 'false'
const user = process.env.SMTP_USER
const pass = process.env.SMTP_PASS

export const isEmailConfigured = Boolean(host)

export const smtpFrom = process.env.SMTP_FROM || `${appName} <noreply@barrelman.dev>`

const transporter = isEmailConfigured
  ? nodemailer.createTransport({
      host,
      port,
      secure,
      auth: user ? { user, pass } : undefined,
    })
  : null

function layout(heading: string, bodyHtml: string): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:32px 16px;background:#f5f5f4;font-family:ui-sans-serif,system-ui,-apple-system,'Segoe UI',sans-serif;color:#1c1917">
  <table role="presentation" cellpadding="0" cellspacing="0" style="max-width:480px;margin:0 auto;background:#fff;border-radius:12px;border:1px solid #e7e5e4">
    <tr><td style="padding:32px">
      <p style="margin:0 0 24px;font-size:13px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:#78716c">${appName}</p>
      <h1 style="margin:0 0 16px;font-size:20px;font-weight:600">${heading}</h1>
      ${bodyHtml}
    </td></tr>
  </table>
  <p style="max-width:480px;margin:16px auto 0;font-size:12px;color:#a8a29e;text-align:center">
    Sent by ${appName} · <a href="${serverOrigin}" style="color:#a8a29e">${serverOrigin}</a>
  </p>
</body></html>`
}

async function send(to: string, subject: string, html: string, text: string): Promise<boolean> {
  if (!transporter) {
    console.warn(`[mailer] SMTP not configured — "${subject}" to ${to} was not sent`)
    return false
  }
  try {
    await transporter.sendMail({ from: smtpFrom, to, subject, html, text })
    return true
  } catch (err) {
    console.error('[mailer] send failed', err)
    return false
  }
}

/**
 * Deliver a sign-in code. Returns whether it reached an inbox — callers still
 * report success to the client either way, because whether an address is
 * deliverable is not something an unauthenticated caller should learn.
 */
export async function sendVerificationCode(email: string, code: string): Promise<boolean> {
  // Logged ONLY when there is no mail transport to deliver it, because then
  // the log is the sole way to obtain the code. Keying this on NODE_ENV (as an
  // earlier version did) meant every deployment that forgot to set it — which
  // is every docker-compose deployment, since no compose file sets NODE_ENV —
  // wrote live sign-in codes to the log, where anyone with `docker logs` could
  // read one inside its 15-minute window and sign in as that user.
  if (!isEmailConfigured) {
    console.log(`[auth] sign-in code for ${email}: ${code} (SMTP not configured)`)
  }

  const html = layout(
    'Your sign-in code',
    `<p style="margin:0 0 24px;font-size:15px;line-height:1.6;color:#57534e">Enter this code to sign in to your ${appName} account.</p>
     <p style="margin:0 0 24px;padding:16px;background:#fafaf9;border:1px solid #e7e5e4;border-radius:8px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:28px;font-weight:600;letter-spacing:.2em;text-align:center">${code}</p>
     <p style="margin:0;font-size:13px;line-height:1.6;color:#78716c">The code expires in 15 minutes and can only be used once. If you didn't request it, you can ignore this email.</p>`,
  )
  const text = `Your ${appName} sign-in code is ${code}. It expires in 15 minutes and can only be used once.`

  return send(email, `Your ${appName} sign-in code`, html, text)
}

/** Warn an account that its remaining credit balance is nearly spent. */
export async function sendLowCreditsWarning(
  email: string,
  remaining: number,
  percentUsed: number,
): Promise<boolean> {
  const html = layout(
    'You are running low on API credits',
    `<p style="margin:0 0 24px;font-size:15px;line-height:1.6;color:#57534e">
       You have used ${percentUsed}% of your ${appName} API credits for this billing period —
       <strong>${remaining.toLocaleString()}</strong> credits remain.</p>
     <p style="margin:0 0 24px;font-size:15px;line-height:1.6;color:#57534e">
       When the balance reaches zero, API requests are refused with a <code>402</code> until the
       period resets or you add credits.</p>
     <p style="margin:0"><a href="${serverOrigin}/console/billing" style="display:inline-block;padding:10px 18px;background:#1c1917;color:#fff;border-radius:8px;text-decoration:none;font-size:14px;font-weight:500">Manage billing</a></p>`,
  )
  const text = `You have used ${percentUsed}% of your ${appName} API credits — ${remaining} remain. Manage billing at ${serverOrigin}/console/billing`

  return send(email, `${percentUsed}% of your ${appName} API credits used`, html, text)
}
