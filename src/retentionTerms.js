/**
 * Terms v2 gate for the free-plan mails (account lifecycle design §7.3). MIRRORS
 * thebigdaypage src/lib/accountRetention/eligibility.js isNewTerms and
 * tbdp-cleanupExpiredMedia src/retentionGate.js — change all three together.
 *
 * Accounts on the v2 Terms get the `-v2` mail rows, which say that the whole account
 * and page are deleted at the end of the grace period (not just the media).
 */

/** Only an unset/empty value is "not configured"; a set but unparseable date throws. */
export function readRetentionTerms(env) {
  const read = (name) => {
    const value = env[name];
    if (!value) return null;
    const millis = Date.parse(value);
    if (Number.isNaN(millis)) throw new Error(`invalid ${name}`);
    return new Date(millis);
  };
  return {
    publishedAt: read("TERMS_V2_PUBLISHED_AT"),
    legacyEffectiveAt: read("TERMS_V2_LEGACY_EFFECTIVE_AT"),
  };
}

/** termsAcceptedAt at/after publishedAt, or now at/after legacyEffectiveAt. */
export function isNewTerms(userDoc, now, terms) {
  const { publishedAt, legacyEffectiveAt } = terms;
  const acceptedMs = new Date(userDoc?.termsAcceptedAt ?? NaN).getTime();
  if (publishedAt && Number.isFinite(acceptedMs) && acceptedMs >= publishedAt.getTime()) return true;
  if (legacyEffectiveAt && now.getTime() >= legacyEffectiveAt.getTime()) return true;
  return false;
}

/** `free-plan-expired` → `free-plan-expired-v2` for new-terms accounts. */
export function templateNameFor(base, userDoc, now, terms) {
  return isNewTerms(userDoc, now, terms) ? `${base}-v2` : base;
}

/** Resend `replyTo` from a template row (empty → omitted). */
export function replyToOf(template) {
  const value = typeof template?.replyTo === "string" ? template.replyTo.trim() : "";
  return value ? { replyTo: value } : {};
}
