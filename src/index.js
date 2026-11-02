import { Client, Databases, Users, Query, ID } from "node-appwrite";
import { Resend } from "resend";
import {
  resolveMailLocale,
  formatMailDate,
  localizedPath,
  greetingFallback,
  applyPlaceholders,
  createTemplateLoader,
} from "./mailLocale.js";
import { deliverViaResend, describeDeliveryFailure } from "./resendDelivery.js";
import { readRetentionTerms, replyToOf, templateNameFor } from "./retentionTerms.js";

const FREE_PLAN_DURATION_DAYS = 183; // ~6 months
const WARNING_DAYS_BEFORE = 14;
const WARNING_CUTOFF_DAYS = FREE_PLAN_DURATION_DAYS - WARNING_DAYS_BEFORE; // 169 days
// Must match docs/media-deletion-policy.md §Grace Period Matrix exactly.
const DELETION_REASON_FREE_EXPIRED = 'free_expired';   // reason value
const FREE_PLAN_MEDIA_GRACE_DAYS   = 183;              // grace days for free_expired
const BATCH_SIZE = 100;

export default async function main(context) {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  return runFreePlanExpiry(context, {
    databases: new Databases(client),
    users: new Users(client),
    resend: new Resend(process.env.RESEND_API_KEY),
  });
}

export async function runFreePlanExpiry(context, { databases, users, resend, now = new Date(), sleep }) {
  const DB_ID = process.env.APPWRITE_DATABASE_ID;
  const USER_COLLECTION_ID = process.env.APPWRITE_USER_COLLECTION_ID;
  const SYSTEM_DB_ID = process.env.SYSTEM_DB_ID;
  const MAIL_TEMPLATES_COLLECTION_ID = process.env.MAIL_TEMPLATES_COLLECTION_ID;
  const LOGS_COLLECTION_ID = process.env.APPWRITE_FUNCTION_LOGS_COLLECTION_ID;
  const SITE_URL = process.env.NEXT_PUBLIC_SITE_URL || "https://thebigdaypage.com";

  const writeLog = async (entry) => {
    if (!LOGS_COLLECTION_ID || !DB_ID) return;
    try {
      await databases.createDocument(DB_ID, LOGS_COLLECTION_ID, ID.unique(), entry);
    } catch (err) {
      context.error(`Failed to write log entry: ${err.message}`);
    }
  };

  // Phase 7b: mail templates are resolved per user language (userData.language
  // clamped to the market locales) with the English row as fallback; cached per
  // (name, language) for this run.
  const loadTemplate = createTemplateLoader(async (name, language) => {
    const res = await databases.listDocuments(SYSTEM_DB_ID, MAIL_TEMPLATES_COLLECTION_ID, [
      Query.equal("name", name),
      Query.equal("language", language),
    ]);
    return res.documents[0] || null;
  });

  // Terms v2 gate: accounts on the v2 Terms get the -v2 mails (whole account deleted at the
  // end of the grace period). An unparseable TERMS_V2_* value stops the run: better no mail
  // than one that misstates what will be deleted.
  let retentionTerms;
  try {
    retentionTerms = readRetentionTerms(process.env);
  } catch (err) {
    context.error(`Retention terms config invalid: ${err.message}`);
    await writeLog({
      functionName: "tbdp-freeplanexpiry",
      action: "error",
      userId: null,
      details: `Retention terms config invalid (${err.message}); no mails sent, no plans expired.`,
    });
    return context.res.json({ success: false, error: `retention terms config invalid: ${err.message}` });
  }

  let warnedCount = 0;
  let expiredCount = 0;
  let errorCount = 0;

  // One key per user and free period: the next run repeats a failed send, and a marker
  // write that fails after an accepted send cannot mail twice within Resend's 24 hours.
  const mailKey = (mail, userDoc) => `${mail}/${userDoc.userId}/${String(userDoc.planStarted).slice(0, 10)}`;

  const logExpiryMailFailure = async (userDoc, details) => {
    context.error(`Expiry mail for user ${userDoc.userId}: ${details}`);
    await writeLog({
      functionName: "tbdp-freeplanexpiry",
      action: "error",
      userId: userDoc.userId,
      details: `Expiry mail: ${details}`,
    });
  };

  /**
   * Sends the expiry notification in the user's language (English row as fallback).
   * "retry_next_run": this can still work, keep the row due. "done": nothing more to try.
   */
  const sendExpiryMail = async (userDoc) => {
    const language = resolveMailLocale(userDoc.language);
    let template;
    try {
      template = await loadTemplate(templateNameFor("free-plan-expired", userDoc, now, retentionTerms), language);
    } catch (err) {
      await logExpiryMailFailure(userDoc, `template unavailable (${language}): ${err.message}`);
      return "retry_next_run";
    }
    if (!template) return "done";

    let userAccount;
    try {
      userAccount = await users.get(userDoc.userId);
    } catch (err) {
      if (err?.code === 404) return "done";
      await logExpiryMailFailure(userDoc, `user account unavailable: ${err.message}`);
      return "retry_next_run";
    }
    if (!userAccount.email) return "done";

    const values = {
      name: userAccount.name || greetingFallback(language),
      upgradeUrl: `${SITE_URL}${localizedPath(language, "/plans")}`,
    };
    const textBody = template.bodyPlain ? applyPlaceholders(template.bodyPlain, values) : undefined;
    const result = await deliverViaResend(
      resend,
      {
        from: "TheBigDayPage <noreply@thebigdaypage.com>",
        to: [userAccount.email],
        subject: applyPlaceholders(template.subject || "Your free plan has expired", values),
        html: applyPlaceholders(template.bodyHtml || "", values),
        ...(textBody ? { text: textBody } : {}),
        ...replyToOf(template),
      },
      { idempotencyKey: mailKey("free-plan-expired", userDoc), maxAttempts: 2, sleep }
    );
    if (result.status === "accepted") {
      context.log(`Expiry email accepted for user ${userDoc.userId}: ${result.id}`);
      return "done";
    }

    await logExpiryMailFailure(userDoc, `not accepted: ${describeDeliveryFailure(result)}`);
    if (result.retryable) return "retry_next_run";
    errorCount++;
    return "done";
  };

  // Both jobs leave an account alone once it has a deletion date. A paid plan that ended is on
  // plan "free" with the planStarted of that plan, usually older than the free period, but it
  // has its own date (refund and lost dispute 30 days, cancellation from the period end).
  // Pages go by cursor: handling a row takes it out of the query, an offset would skip rows.

  // ─── Job 1: Send 14-day warning emails ───
  try {
    const warningCutoff = new Date(now);
    warningCutoff.setDate(warningCutoff.getDate() - WARNING_CUTOFF_DAYS);
    const warningCutoffISO = warningCutoff.toISOString();

    context.log(`[Warning] Checking users with planStarted <= ${warningCutoffISO}`);

    // The warning template is resolved per user (language) inside the loop.

    let cursor = null;
    let hasMore = true;

    while (hasMore) {
      const batch = await databases.listDocuments(DB_ID, USER_COLLECTION_ID, [
        Query.equal("plan", "free"),
        Query.lessThanEqual("planStarted", warningCutoffISO),
        Query.isNull("freeExpiryWarnedAt"),
        Query.isNull("deleteMedia"),
        Query.limit(BATCH_SIZE),
        ...(cursor ? [Query.cursorAfter(cursor)] : []),
      ]);

      for (const userDoc of batch.documents) {
        try {
          // Calculate expiry date for this user
          const planStarted = new Date(userDoc.planStarted);
          const expiryDate = new Date(planStarted);
          expiryDate.setDate(expiryDate.getDate() + FREE_PLAN_DURATION_DAYS);

          // Skip if already expired (Job 2 will handle)
          if (expiryDate <= now) {
            continue;
          }

          // Fetch user email from Appwrite Users API
          let userAccount;
          try {
            userAccount = await users.get(userDoc.userId);
          } catch (err) {
            context.error(
              `Failed to fetch user ${userDoc.userId}: ${err.message}`
            );
            await writeLog({
              functionName: "tbdp-freeplanexpiry",
              action: "error",
              userId: userDoc.userId,
              details: `Failed to fetch user account for warning job: ${err.message}`,
            });
            errorCount++;
            continue;
          }

          const language = resolveMailLocale(userDoc.language);
          const userName = userAccount.name || greetingFallback(language);
          const userEmail = userAccount.email;

          if (!userEmail) {
            context.log(
              `User ${userDoc.userId} has no email, skipping warning`
            );
            continue;
          }

          // Send warning email in the user's language (English row as fallback)
          let warningTemplate = null;
          try {
            warningTemplate = await loadTemplate(templateNameFor("free-plan-expiring", userDoc, now, retentionTerms), language);
          } catch (err) {
            context.error(`Failed to fetch warning email template (${language}): ${err.message}`);
            await writeLog({
              functionName: "tbdp-freeplanexpiry",
              action: "error",
              userId: userDoc.userId,
              details: `Failed to fetch warning email template (${language}): ${err.message}`,
            });
          }
          if (warningTemplate) {
            const values = {
              name: userName,
              expiryDate: formatMailDate(expiryDate, language),
              upgradeUrl: `${SITE_URL}${localizedPath(language, "/plans")}`,
            };
            const subject = applyPlaceholders(warningTemplate.subject || "Your free plan expires in 14 days", values);
            const htmlBody = applyPlaceholders(warningTemplate.bodyHtml || "", values);
            const textBody = warningTemplate.bodyPlain ? applyPlaceholders(warningTemplate.bodyPlain, values) : undefined;

            const result = await deliverViaResend(
              resend,
              {
                from: "TheBigDayPage <noreply@thebigdaypage.com>",
                to: [userEmail],
                subject,
                html: htmlBody,
                ...(textBody ? { text: textBody } : {}),
                ...replyToOf(warningTemplate),
              },
              { idempotencyKey: mailKey("free-plan-expiring", userDoc), maxAttempts: 2, sleep }
            );
            if (result.status !== "accepted") {
              const detail = describeDeliveryFailure(result);
              context.error(`Warning email not accepted for user ${userDoc.userId}: ${detail}`);
              await writeLog({
                functionName: "tbdp-freeplanexpiry",
                action: "error",
                userId: userDoc.userId,
                details: `Warning email not accepted, freeExpiryWarnedAt not set: ${detail}`,
              });
              errorCount++;
              continue;
            }
            context.log(`Warning email accepted for user ${userDoc.userId}: ${result.id}`);
          } else {
            context.log(
              `No warning template found, skipping warning for user ${userDoc.userId}`
            );
            await writeLog({
              functionName: "tbdp-freeplanexpiry",
              action: "error",
              userId: userDoc.userId,
              details: `Warning email skipped: no '${templateNameFor("free-plan-expiring", userDoc, now, retentionTerms)}' template found. freeExpiryWarnedAt not set.`,
            });
            errorCount++;
            continue;
          }

          // Mark as warned
          await databases.updateDocument(
            DB_ID,
            USER_COLLECTION_ID,
            userDoc.$id,
            { freeExpiryWarnedAt: now.toISOString() }
          );

          await writeLog({
            functionName: "tbdp-freeplanexpiry",
            action: "warning_sent",
            userId: userDoc.userId,
            details: `14-day expiry warning email sent. Plan expires ${expiryDate.toISOString()}.`,
          });

          warnedCount++;
        } catch (err) {
          context.error(
            `Error processing warning for user ${userDoc.userId}: ${err.message}`
          );
          await writeLog({
            functionName: "tbdp-freeplanexpiry",
            action: "error",
            userId: userDoc.userId,
            details: `Unhandled error in warning job: ${err.message}`,
          });
          errorCount++;
        }
      }

      hasMore = batch.documents.length === BATCH_SIZE;
      cursor = batch.documents.at(-1)?.$id ?? null;
    }
  } catch (err) {
    context.error(`Warning job failed: ${err.message}`);
    await writeLog({
      functionName: "tbdp-freeplanexpiry",
      action: "error",
      userId: null,
      details: `Warning job failed (outer catch): ${err.message}`,
    });
  }

  // ─── Job 2: Expire free plans ───
  try {
    const expiryCutoff = new Date(now);
    expiryCutoff.setDate(expiryCutoff.getDate() - FREE_PLAN_DURATION_DAYS);
    const expiryCutoffISO = expiryCutoff.toISOString();

    context.log(`[Expiry] Checking users with planStarted <= ${expiryCutoffISO}`);

    // The expiry template is resolved per user (language) inside the loop.

    let cursor = null;
    let hasMore = true;

    while (hasMore) {
      const batch = await databases.listDocuments(DB_ID, USER_COLLECTION_ID, [
        Query.equal("plan", "free"),
        Query.lessThanEqual("planStarted", expiryCutoffISO),
        Query.notEqual("subscriptionStatus", "free_expired"),
        Query.notEqual("subscriptionStatus", "archived"),
        Query.isNull("deleteMedia"),
        Query.limit(BATCH_SIZE),
        ...(cursor ? [Query.cursorAfter(cursor)] : []),
      ]);

      for (const userDoc of batch.documents) {
        try {
          // The mail comes first: expiring the plan takes the row out of this query, which
          // would end every retry. While the mail can still get through, the row stays due.
          if ((await sendExpiryMail(userDoc)) === "retry_next_run") {
            errorCount++;
            continue;
          }

          // Calculate media deletion date
          const deleteAt = new Date(now);
          deleteAt.setDate(deleteAt.getDate() + FREE_PLAN_MEDIA_GRACE_DAYS);

          // Update user status with full scheduling metadata
          await databases.updateDocument(
            DB_ID,
            USER_COLLECTION_ID,
            userDoc.$id,
            {
              subscriptionStatus:     'free_expired',
              deleteMedia:            deleteAt.toISOString(),
              deleteMediaReason:      DELETION_REASON_FREE_EXPIRED,
              deleteMediaScheduledAt: now.toISOString(),
              deleteMediaGraceDays:   FREE_PLAN_MEDIA_GRACE_DAYS,
            }
          );

          context.log(`Expired free plan for user ${userDoc.userId}`);

          await writeLog({
            functionName: "tbdp-freeplanexpiry",
            action: "plan_expired",
            userId: userDoc.userId,
            details: `Free plan expired. Reason: ${DELETION_REASON_FREE_EXPIRED}. Media scheduled for deletion on ${deleteAt.toISOString()} (grace: ${FREE_PLAN_MEDIA_GRACE_DAYS} days).`,
          });

          expiredCount++;
        } catch (err) {
          context.error(
            `Error expiring user ${userDoc.userId}: ${err.message}`
          );
          await writeLog({
            functionName: "tbdp-freeplanexpiry",
            action: "error",
            userId: userDoc.userId,
            details: `Unhandled error in expiry job: ${err.message}`,
          });
          errorCount++;
        }
      }

      hasMore = batch.documents.length === BATCH_SIZE;
      cursor = batch.documents.at(-1)?.$id ?? null;
    }
  } catch (err) {
    context.error(`Expiry job failed: ${err.message}`);
    await writeLog({
      functionName: "tbdp-freeplanexpiry",
      action: "error",
      userId: null,
      details: `Expiry job failed (outer catch): ${err.message}`,
    });
  }

  const summary = {
    success: true,
    warned: warnedCount,
    expired: expiredCount,
    errors: errorCount,
    timestamp: now.toISOString(),
  };

  context.log(`Free plan expiry job complete: ${JSON.stringify(summary)}`);
  return context.res.json(summary);
}
