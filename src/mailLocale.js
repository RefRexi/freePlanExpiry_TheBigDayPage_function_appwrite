/**
 * Mail-language helpers (language directive Phase 7b) — pure, no Appwrite.
 *
 * Mails go out in the six market locales; anything else falls back to English,
 * mirroring thebigdaypage/src/lib/mailLocale.ts. Template rows are looked up per
 * (name, language) with the English row as fallback.
 */
export const MAIL_LOCALES = ["en", "de", "sq", "mk", "tr", "ar"];

/** Clamp userData.language (any BCP-47-ish value) to a supported mail locale. */
export function resolveMailLocale(raw) {
  if (typeof raw !== "string") return "en";
  const primary = raw.trim().toLowerCase().split(/[-_]/)[0];
  return MAIL_LOCALES.includes(primary) ? primary : "en";
}

/** Long date ("August 28, 2026" / "28. August 2026"); en is byte-identical to the old en-US output. */
export function formatMailDate(date, locale) {
  try {
    return new Intl.DateTimeFormat(locale, { year: "numeric", month: "long", day: "numeric" }).format(date);
  } catch {
    return date.toISOString().slice(0, 10);
  }
}

/** next-intl `localePrefix: 'as-needed'` semantics: en unprefixed, others /<locale>/… */
export function localizedPath(locale, path) {
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return locale === "en" ? normalized : `/${locale}${normalized}`;
}

/** Salutation when the account has no name ("Hi there," in English). */
const GREETING_FALLBACK = { en: "there", de: "ihr zwei", sq: "të dashur", mk: "драги", tr: "sevgili çift", ar: "أعزاءنا" };
export function greetingFallback(locale) {
  return GREETING_FALLBACK[locale] || GREETING_FALLBACK.en;
}

export function applyPlaceholders(text, values) {
  return String(text || "").replace(/\{\{(\w+)\}\}/g, (m, k) => (k in values ? values[k] : m));
}

/**
 * Wrap a `(name, language) => row|null` fetcher: falls back to the English row
 * and caches results per (name, language) for the lifetime of one run.
 */
export function createTemplateLoader(fetchByLanguage) {
  const cache = new Map();
  return async (name, language) => {
    const key = `${name}:${language}`;
    if (!cache.has(key)) {
      let template = await fetchByLanguage(name, language);
      if (!template && language !== "en") template = await fetchByLanguage(name, "en");
      cache.set(key, template || null);
    }
    return cache.get(key);
  };
}
