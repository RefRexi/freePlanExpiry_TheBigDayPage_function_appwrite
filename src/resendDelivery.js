/**
 * The one way a mail is handed to Resend. Copied byte-identical into thebigdaypage, tbdp-partner,
 * tbdp-admin, tbdp-guestUploadNotifier, tbdp-freeplanexpiry and tbdp-newsletterdoubleoptin; a test
 * in each repo pins the hash. Change one copy, change them all.
 *
 * Why it exists: `resend.emails.send()` does not throw. Every API error, network failure and
 * abort RESOLVES as `{ data: null, error }`, so `await send()` inside a try/catch reports success
 * for a mail nobody accepted.
 *
 * What the result means:
 *   accepted   Resend took the mail and returned an id. This is what every `*SentAt` marker in
 *              the platform records. It is NOT delivery: a later bounce is only visible in the
 *              Resend dashboard, because nothing here consumes Resend's delivery webhooks.
 *   failed     Resend did not confirm it. Leave the marker unset so the next run sends again.
 *              `retryable`: the same mail can succeed later without being changed.
 *              `ambiguous`: Resend may have taken it anyway, so repeat it only under the same
 *              idempotency key. Resend keeps a key for 24 hours and does not store failed requests.
 */
import { createHash } from 'crypto';

export const DEFAULT_ATTEMPT_TIMEOUT_MS = 8_000;

const BACKOFF_MS = [1_000, 2_000, 4_000];
// A longer rate-limit wait is left to the next run instead of holding a request or cron open.
const LONGEST_WAIT_MS = 5_000;

const defaultSleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function retryAfterMs(response) {
  const seconds = Number(response?.headers?.['retry-after']);
  return Number.isFinite(seconds) && seconds >= 0 ? seconds * 1000 : null;
}

function classify(response, timedOut) {
  const error = response?.error ?? null;
  const providerCode = error?.name ?? null;
  const statusCode = typeof error?.statusCode === 'number' ? error.statusCode : null;
  const facts = { providerCode, statusCode, message: error?.message ?? null };

  // The SDK reports an abort like any other network failure, so only the signal tells them apart.
  if (timedOut) return { ...facts, reason: 'timeout', retryable: true, ambiguous: true, repeatNow: true };
  if (!error) {
    return { ...facts, message: 'response without an email id', reason: 'invalid_response', retryable: true, ambiguous: true, repeatNow: true };
  }

  if (providerCode === 'rate_limit_exceeded') {
    return { ...facts, reason: 'rate_limited', retryable: true, ambiguous: false, repeatNow: true, retryAfterMs: retryAfterMs(response) };
  }
  if (providerCode === 'daily_quota_exceeded' || providerCode === 'monthly_quota_exceeded') {
    return { ...facts, reason: 'quota_exceeded', retryable: true, ambiguous: false, repeatNow: false };
  }
  if (providerCode === 'concurrent_idempotent_requests') {
    return { ...facts, reason: 'concurrent', retryable: true, ambiguous: true, repeatNow: true };
  }
  if (providerCode === 'invalid_idempotent_request') {
    // An earlier mail under this key was accepted with a different body. Repeating cannot help.
    return { ...facts, reason: 'idempotency_conflict', retryable: false, ambiguous: false, repeatNow: false };
  }

  const isUnspecifiedFailure = providerCode === 'application_error' || providerCode === 'internal_server_error';
  if (statusCode === null && isUnspecifiedFailure) {
    return { ...facts, reason: 'network', retryable: true, ambiguous: true, repeatNow: true };
  }
  if (statusCode !== null && statusCode >= 500) {
    return { ...facts, reason: 'provider_error', retryable: true, ambiguous: true, repeatNow: true };
  }
  return { ...facts, reason: 'rejected', retryable: false, ambiguous: false, repeatNow: false };
}

/**
 * @param {{ emails: { send: Function } }} resend
 * @param {object} payload  what `resend.emails.send` takes
 * @param {{ idempotencyKey?: string, maxAttempts?: number, timeoutMs?: number, sleep?: (ms: number) => Promise<void> }} [options]
 * @returns {Promise<
 *   { status: 'accepted', id: string, attempts: number, replayed: boolean } |
 *   { status: 'failed', reason: string, retryable: boolean, ambiguous: boolean, attempts: number,
 *     providerCode: string|null, statusCode: number|null, message: string|null, retryAfterMs?: number|null }
 * >}
 */
export async function deliverViaResend(resend, payload, options = {}) {
  const { idempotencyKey, maxAttempts = 1, timeoutMs = DEFAULT_ATTEMPT_TIMEOUT_MS, sleep = defaultSleep } = options;

  for (let attempts = 1; ; attempts += 1) {
    const signal = AbortSignal.timeout(timeoutMs);
    let response;
    try {
      response = await resend.emails.send(payload, { ...(idempotencyKey ? { idempotencyKey } : {}), signal });
    } catch (error) {
      response = { data: null, error: { name: 'application_error', statusCode: null, message: error?.message ?? String(error) } };
    }

    const id = response?.data?.id;
    if (!response?.error && typeof id === 'string' && id) {
      return { status: 'accepted', id, attempts, replayed: response?.headers?.['idempotent-replayed'] === 'true' };
    }

    const { repeatNow, ...failure } = classify(response, signal.aborted);
    const wait = failure.retryAfterMs ?? BACKOFF_MS[attempts - 1] ?? BACKOFF_MS[BACKOFF_MS.length - 1];
    const safeToRepeat = !failure.ambiguous || Boolean(idempotencyKey);
    if (attempts >= maxAttempts || !repeatNow || !safeToRepeat || wait > LONGEST_WAIT_MS) {
      return { status: 'failed', attempts, ...failure };
    }
    await sleep(wait);
  }
}

/**
 * An idempotency key for a mail with no natural id, such as a contact-form message: the same
 * content gives the same key. The parts are hashed, so the key carries none of the content.
 */
export function contentIdempotencyKey(prefix, ...parts) {
  const hash = createHash('sha256').update(JSON.stringify(parts)).digest('hex');
  return `${prefix}/${hash}`;
}

/** One line for a log or an activity entry, built from the provider's answer. */
export function describeDeliveryFailure(result) {
  const provider = [result.providerCode, result.statusCode].filter((value) => value !== null && value !== undefined).join(' ');
  return `${result.reason}${provider ? ` (${provider})` : ''}${result.message ? `: ${result.message}` : ''}`;
}
