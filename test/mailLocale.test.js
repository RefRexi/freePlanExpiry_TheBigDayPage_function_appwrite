import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  MAIL_LOCALES, applyPlaceholders, createTemplateLoader, formatMailDate, greetingFallback, localizedPath, resolveMailLocale,
} from "../src/mailLocale.js";

describe("resolveMailLocale", () => {
  it("passes market locales and clamps everything else to en", () => {
    for (const l of MAIL_LOCALES) assert.equal(resolveMailLocale(l), l);
    for (const l of [undefined, null, "", 3, "fr", "he", "xx"]) assert.equal(resolveMailLocale(l), "en");
    assert.equal(resolveMailLocale("de-AT"), "de");
    assert.equal(resolveMailLocale(" AR_sa "), "ar");
  });
});

describe("formatMailDate / localizedPath / greetingFallback", () => {
  const date = new Date(Date.UTC(2026, 7, 28, 12));
  it("en is byte-identical to the previous en-US toLocaleDateString output", () => {
    assert.equal(formatMailDate(date, "en"), date.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" }));
    assert.equal(formatMailDate(date, "de"), "28. August 2026");
  });
  it("prefixes every non-English path", () => {
    assert.equal(localizedPath("en", "/plans"), "/plans");
    assert.equal(localizedPath("tr", "/plans"), "/tr/plans");
    assert.equal(localizedPath("sq", "plans"), "/sq/plans");
  });
  it("has a salutation fallback for every locale", () => {
    for (const l of MAIL_LOCALES) assert.ok(greetingFallback(l));
    assert.equal(greetingFallback("xx"), "there");
  });
});

describe("applyPlaceholders", () => {
  it("replaces known keys and leaves unknown tokens", () => {
    assert.equal(applyPlaceholders("Hi {{name}} {{x}}", { name: "A" }), "Hi A {{x}}");
    assert.equal(applyPlaceholders(null, {}), "");
  });
});

describe("createTemplateLoader", () => {
  it("falls back to the English row and caches per (name, language)", async () => {
    const calls = [];
    const rows = { "free-plan-expiring:en": { subject: "en" }, "free-plan-expiring:de": { subject: "de" } };
    const load = createTemplateLoader(async (name, lang) => { calls.push(`${name}:${lang}`); return rows[`${name}:${lang}`] || null; });
    assert.equal((await load("free-plan-expiring", "de")).subject, "de");
    assert.equal((await load("free-plan-expiring", "tr")).subject, "en"); // fallback
    assert.equal(await load("free-plan-expired", "tr"), null);            // nothing at all
    await load("free-plan-expiring", "tr");                                // cached
    assert.deepEqual(calls, ["free-plan-expiring:de", "free-plan-expiring:tr", "free-plan-expiring:en", "free-plan-expired:tr", "free-plan-expired:en"]);
  });
});
