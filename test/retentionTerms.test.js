import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { isNewTerms, readRetentionTerms, replyToOf, templateNameFor } from "../src/retentionTerms.js";

const NOW = new Date("2027-03-01T10:00:00Z");
const PUB = { publishedAt: new Date("2026-11-01T00:00:00Z"), legacyEffectiveAt: null };
const LEGACY = { publishedAt: null, legacyEffectiveAt: null };

describe("readRetentionTerms", () => {
  it("unset -> null, valid -> Date, garbage -> throws", () => {
    assert.deepEqual(readRetentionTerms({}), LEGACY);
    assert.equal(readRetentionTerms({ TERMS_V2_PUBLISHED_AT: "2026-11-01T00:00:00Z" }).publishedAt.toISOString(), "2026-11-01T00:00:00.000Z");
    assert.throws(() => readRetentionTerms({ TERMS_V2_PUBLISHED_AT: "soon" }), /invalid TERMS_V2_PUBLISHED_AT/);
  });
});

describe("isNewTerms / templateNameFor", () => {
  it("acceptance at/after publication, or legacy date passed; missing acceptance stays legacy", () => {
    assert.equal(isNewTerms({ termsAcceptedAt: "2026-11-02T00:00:00Z" }, NOW, PUB), true);
    assert.equal(isNewTerms({ termsAcceptedAt: "2026-10-01T00:00:00Z" }, NOW, PUB), false);
    assert.equal(isNewTerms({ termsAcceptedAt: null }, NOW, PUB), false);
    assert.equal(isNewTerms({}, NOW, { publishedAt: null, legacyEffectiveAt: new Date("2027-01-01T00:00:00Z") }), true);
    assert.equal(isNewTerms({ termsAcceptedAt: "2026-11-02T00:00:00Z" }, NOW, LEGACY), false);
  });
  it("picks the -v2 row only for new-terms accounts", () => {
    assert.equal(templateNameFor("free-plan-expired", { termsAcceptedAt: "2026-11-02T00:00:00Z" }, NOW, PUB), "free-plan-expired-v2");
    assert.equal(templateNameFor("free-plan-expired", { termsAcceptedAt: "2026-10-01T00:00:00Z" }, NOW, PUB), "free-plan-expired");
  });
});

describe("replyToOf", () => {
  it("passes a set replyTo, omits empty/missing", () => {
    assert.deepEqual(replyToOf({ replyTo: "support@thebigdaypage.com" }), { replyTo: "support@thebigdaypage.com" });
    assert.deepEqual(replyToOf({ replyTo: "" }), {});
    assert.deepEqual(replyToOf({ replyTo: null }), {});
    assert.deepEqual(replyToOf(null), {});
  });
});
