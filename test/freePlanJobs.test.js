import { beforeEach, describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

process.env.APPWRITE_DATABASE_ID = "db";
process.env.APPWRITE_USER_COLLECTION_ID = "userData";
process.env.SYSTEM_DB_ID = "system";
process.env.MAIL_TEMPLATES_COLLECTION_ID = "templates";
process.env.APPWRITE_FUNCTION_LOGS_COLLECTION_ID = "functionLogs";
delete process.env.TERMS_V2_PUBLISHED_AT;
delete process.env.TERMS_V2_LEGACY_EFFECTIVE_AT;

const { runFreePlanExpiry } = await import("../src/index.js");

const NOW = new Date("2027-03-01T10:00:00Z");
const DAY = 86_400_000;
const daysAgo = (n) => new Date(NOW.getTime() - n * DAY).toISOString();

const ACCEPTED = { data: { id: "mail_1" }, error: null };
const REJECTED = { data: null, error: { name: "validation_error", statusCode: 422, message: "Invalid `to` field." } };
const DOWN = { data: null, error: { name: "internal_server_error", statusCode: 500, message: "boom" } };

let rows;
let answers;
let sends;
let logs;
let order;
let userLookup;
let templateLookup;

const parse = (queries) => queries.map((q) => JSON.parse(q));

const databases = {
  async listDocuments(db, collection, queries) {
    const parsed = parse(queries);
    if (collection === "templates") {
      const name = parsed.find((q) => q.attribute === "name").values[0];
      const language = parsed.find((q) => q.attribute === "language").values[0];
      return { documents: templateLookup(name, language) };
    }
    // Like Appwrite: the filters run on the rows as they are now, and a cursor is a position in
    // the table, so it stays valid when an earlier row no longer matches the filters.
    const cursor = parsed.find((q) => q.method === "cursorAfter")?.values[0];
    const start = cursor ? rows.findIndex((row) => row.$id === cursor) + 1 : 0;
    const offset = parsed.find((q) => q.method === "offset")?.values[0] ?? 0;
    const limit = parsed.find((q) => q.method === "limit")?.values[0] ?? 25;
    const matches = (row) => parsed.every((q) => {
      if (q.method === "equal") return q.values.includes(row[q.attribute]);
      if (q.method === "notEqual") return row[q.attribute] != null && !q.values.includes(row[q.attribute]);
      if (q.method === "isNull") return row[q.attribute] == null;
      if (q.method === "lessThanEqual") return row[q.attribute] != null && new Date(row[q.attribute]) <= new Date(q.values[0]);
      return true;
    });
    const documents = rows.slice(start).filter(matches).slice(offset, offset + limit);
    return { documents: documents.map((row) => ({ ...row })) };
  },
  async updateDocument(db, collection, id, fields) {
    order.push("update");
    Object.assign(rows.find((row) => row.$id === id), fields);
    return { $id: id };
  },
  async createDocument(db, collection, id, entry) {
    logs.push(entry);
    return entry;
  },
};

const users = { get: async (id) => userLookup(id) };

const resend = {
  emails: {
    async send(payload, options) {
      order.push("mail");
      sends.push({ payload, options });
      return answers.length > 1 ? answers.shift() : answers[0];
    },
  },
};

const context = {
  log: () => {},
  error: () => {},
  res: { json: (body) => body },
};

const runJobs = () => runFreePlanExpiry(context, { databases, users, resend, now: NOW, sleep: async () => {} });

const warningDue = (over = {}) => ({ $id: "row1", userId: "u1", plan: "free", planStarted: daysAgo(175), subscriptionStatus: "active", language: "de", freeExpiryWarnedAt: null, ...over });
const expiryDue = (over = {}) => ({ ...warningDue(), planStarted: daysAgo(190), freeExpiryWarnedAt: daysAgo(10), ...over });
const errorLogs = () => logs.filter((log) => log.action === "error");

beforeEach(() => {
  rows = [];
  answers = [ACCEPTED];
  sends = [];
  logs = [];
  order = [];
  userLookup = () => ({ name: "A & B", email: "couple@example.test" });
  templateLookup = (name, language) => [{ name, language, subject: `${name} ${language}`, bodyHtml: "<p>{{name}}</p>", bodyPlain: "{{name}}" }];
});

describe("14-day warning", () => {
  it("records the warning once the provider accepted it", async () => {
    rows = [warningDue()];
    const summary = await runJobs();
    assert.equal(summary.warned, 1);
    assert.equal(rows[0].freeExpiryWarnedAt, NOW.toISOString());
  });

  it("uses the stored template in the user's language", async () => {
    rows = [warningDue()];
    await runJobs();
    assert.equal(sends[0].payload.subject, "free-plan-expiring de");
    assert.deepEqual(sends[0].payload.to, ["couple@example.test"]);
  });

  it("sets no marker when the provider answered with { error }", async () => {
    rows = [warningDue()];
    answers = [REJECTED];
    const summary = await runJobs();
    assert.equal(summary.warned, 0);
    assert.equal(summary.errors, 1);
    assert.equal(rows[0].freeExpiryWarnedAt, null);
    assert.equal(errorLogs().length, 1);
  });

  it("sends again on the next run after an outage, under the same key", async () => {
    rows = [warningDue()];
    answers = [DOWN, DOWN, ACCEPTED];
    await runJobs();
    assert.equal(rows[0].freeExpiryWarnedAt, null);

    const second = await runJobs();
    assert.equal(second.warned, 1);
    assert.equal(rows[0].freeExpiryWarnedAt, NOW.toISOString());
    assert.equal(new Set(sends.map((s) => s.options.idempotencyKey)).size, 1);
    assert.match(sends[0].options.idempotencyKey, /^free-plan-expiring\/u1\//);
  });
});

describe("plan expiry", () => {
  it("expires the plan after the provider accepted the mail", async () => {
    rows = [expiryDue()];
    const summary = await runJobs();
    assert.equal(summary.expired, 1);
    assert.equal(rows[0].subscriptionStatus, "free_expired");
    assert.deepEqual(order, ["mail", "update"]);
    assert.ok(logs.some((log) => log.action === "plan_expired"));
  });

  it("sends the stored template in the user's language", async () => {
    rows = [expiryDue()];
    await runJobs();
    assert.equal(sends[0].payload.subject, "free-plan-expired de");
  });

  it("keeps the row due while the provider is down, so the next run can send: the status update would end every retry", async () => {
    rows = [expiryDue()];
    answers = [DOWN, DOWN, ACCEPTED];

    const first = await runJobs();
    assert.equal(first.expired, 0);
    assert.equal(first.errors, 1);
    assert.equal(rows[0].subscriptionStatus, "active");
    assert.equal(rows[0].deleteMedia, undefined);

    const second = await runJobs();
    assert.equal(second.expired, 1);
    assert.equal(rows[0].subscriptionStatus, "free_expired");
    assert.equal(new Set(sends.map((s) => s.options.idempotencyKey)).size, 1);
    assert.match(sends[0].options.idempotencyKey, /^free-plan-expired\/u1\//);
  });

  it("a mail the provider rejects does not hold the plan back: repeating cannot help", async () => {
    rows = [expiryDue()];
    answers = [REJECTED];
    const summary = await runJobs();
    assert.equal(rows[0].subscriptionStatus, "free_expired");
    assert.equal(summary.expired, 1);
    assert.equal(summary.errors, 1);
    assert.equal(errorLogs().length, 1);
  });

  it("a user who cannot be read right now stays due", async () => {
    rows = [expiryDue()];
    userLookup = () => { throw Object.assign(new Error("appwrite down"), { code: 503 }); };
    const summary = await runJobs();
    assert.equal(rows[0].subscriptionStatus, "active");
    assert.equal(summary.errors, 1);
  });

  it("a user who no longer exists expires without a mail", async () => {
    rows = [expiryDue()];
    userLookup = () => { throw Object.assign(new Error("not found"), { code: 404 }); };
    await runJobs();
    assert.equal(rows[0].subscriptionStatus, "free_expired");
    assert.equal(sends.length, 0);
  });

  it("a template the database cannot deliver right now keeps the row due", async () => {
    rows = [expiryDue()];
    templateLookup = () => { throw new Error("database down"); };
    const summary = await runJobs();
    assert.equal(rows[0].subscriptionStatus, "active");
    assert.equal(summary.errors, 1);
  });

  it("a template that does not exist does not hold the plan back", async () => {
    rows = [expiryDue()];
    templateLookup = () => [];
    await runJobs();
    assert.equal(rows[0].subscriptionStatus, "free_expired");
    assert.equal(sends.length, 0);
  });
});

describe("shared module", () => {
  it("is the agreed copy of thebigdaypage/src/lib/resendDelivery.js", () => {
    const source = readFileSync(new URL("../src/resendDelivery.js", import.meta.url));
    assert.equal(createHash("sha256").update(source).digest("hex"), "621f0377d6a16220ba9871ed16af3025cb6539384f8a0714bd6f41ec99a88a0a");
  });
});

const daysAhead = (n) => new Date(NOW.getTime() + n * DAY).toISOString();

describe("an account that already has a deletion date", () => {
  // planStarted is the start of the paid plan: it is older than the free period as soon as a
  // plan bought half a year ago ends, and the account is on plan "free" from that moment on.
  const scheduled = (over) => expiryDue({ plan: "free", planStarted: daysAgo(400), freeExpiryWarnedAt: null, ...over });

  it("keeps the 30 days of a full refund instead of moving them to 183", async () => {
    const date = daysAhead(20);
    rows = [scheduled({ subscriptionStatus: "refunded", deleteMedia: date, deleteMediaReason: "refunded" })];
    await runJobs();
    assert.equal(rows[0].deleteMedia, date);
    assert.equal(rows[0].subscriptionStatus, "refunded");
    assert.equal(rows[0].deleteMediaReason, "refunded");
    assert.equal(sends.length, 0);
  });

  it("keeps the 30 days of a lost dispute", async () => {
    const date = daysAhead(25);
    rows = [scheduled({ subscriptionStatus: "disputed_lost", deleteMedia: date, deleteMediaReason: "disputed_lost" })];
    await runJobs();
    assert.equal(rows[0].deleteMedia, date);
    assert.equal(rows[0].subscriptionStatus, "disputed_lost");
    assert.equal(sends.length, 0);
  });

  it("does not move the date of a cancelled subscription forward", async () => {
    const date = daysAhead(400);
    rows = [scheduled({ subscriptionStatus: "subscription_nonrenewed", deleteMedia: date, deleteMediaReason: "subscription_nonrenewed" })];
    await runJobs();
    assert.equal(rows[0].deleteMedia, date);
    assert.equal(rows[0].subscriptionStatus, "subscription_nonrenewed");
    assert.equal(sends.length, 0);
  });

  it("gets no warning that a free plan is about to expire", async () => {
    rows = [scheduled({ planStarted: daysAgo(175), subscriptionStatus: "refunded", deleteMedia: daysAhead(20) })];
    const result = await runJobs();
    assert.equal(sends.length, 0);
    assert.equal(rows[0].freeExpiryWarnedAt, null);
    assert.equal(result.warned, 0);
  });

  it("an account without a date still expires, also after a subscription ended unnoticed", async () => {
    rows = [expiryDue({ subscriptionStatus: "canceled", deleteMedia: null })];
    await runJobs();
    assert.equal(rows[0].subscriptionStatus, "free_expired");
    assert.ok(rows[0].deleteMedia);
  });
});

describe("a run with more rows than one page", () => {
  // Expiring and warning take a row out of the query. Counting pages by offset then skips as
  // many rows as were just handled.
  const many = (make) => Array.from({ length: 250 }, (_, i) => make({ $id: `row${i}`, userId: `u${i}` }));

  it("expires every due row", async () => {
    rows = many(expiryDue);
    const result = await runJobs();
    assert.equal(result.expired, 250);
    assert.equal(rows.filter((row) => row.subscriptionStatus === "free_expired").length, 250);
  });

  it("warns every due row", async () => {
    rows = many(warningDue);
    const result = await runJobs();
    assert.equal(result.warned, 250);
    assert.equal(rows.filter((row) => row.freeExpiryWarnedAt).length, 250);
  });
});
