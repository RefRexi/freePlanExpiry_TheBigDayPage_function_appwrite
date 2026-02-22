import { Client, Databases, Users, Query, ID } from "node-appwrite";
import { Resend } from "resend";

const FREE_PLAN_DURATION_DAYS = 183; // ~6 months
const WARNING_DAYS_BEFORE = 14;
const WARNING_CUTOFF_DAYS = FREE_PLAN_DURATION_DAYS - WARNING_DAYS_BEFORE; // 169 days
const MEDIA_GRACE_PERIOD_DAYS = 183; // 6 months grace before media deletion
const BATCH_SIZE = 100;

export default async function main(context) {
  const client = new Client()
    .setEndpoint(process.env.APPWRITE_ENDPOINT)
    .setProject(process.env.APPWRITE_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY);

  const databases = new Databases(client);
  const users = new Users(client);
  const resend = new Resend(process.env.RESEND_API_KEY);

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

  const now = new Date();

  let warnedCount = 0;
  let expiredCount = 0;
  let errorCount = 0;

  // ─── Job 1: Send 14-day warning emails ───
  try {
    const warningCutoff = new Date(now);
    warningCutoff.setDate(warningCutoff.getDate() - WARNING_CUTOFF_DAYS);
    const warningCutoffISO = warningCutoff.toISOString();

    context.log(`[Warning] Checking users with planStarted <= ${warningCutoffISO}`);

    // Fetch the warning email template
    let warningTemplate = null;
    try {
      const templateRes = await databases.listDocuments(
        SYSTEM_DB_ID,
        MAIL_TEMPLATES_COLLECTION_ID,
        [
          Query.equal("name", "free-plan-expiring"),
          Query.equal("language", "en"),
        ]
      );
      if (templateRes.documents.length > 0) {
        warningTemplate = templateRes.documents[0];
      }
    } catch (err) {
      context.error(`Failed to fetch warning email template: ${err.message}`);
    }

    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const batch = await databases.listDocuments(DB_ID, USER_COLLECTION_ID, [
        Query.equal("plan", "free"),
        Query.lessThanEqual("planStarted", warningCutoffISO),
        Query.isNull("freeExpiryWarnedAt"),
        Query.limit(BATCH_SIZE),
        Query.offset(offset),
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
            errorCount++;
            continue;
          }

          const userName = userAccount.name || "there";
          const userEmail = userAccount.email;

          if (!userEmail) {
            context.log(
              `User ${userDoc.userId} has no email, skipping warning`
            );
            continue;
          }

          // Send warning email
          if (warningTemplate) {
            const subject = (warningTemplate.subject || "Your free plan expires in 14 days")
              .replace(/\{\{name\}\}/g, userName)
              .replace(/\{\{expiryDate\}\}/g, expiryDate.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" }))
              .replace(/\{\{upgradeUrl\}\}/g, `${SITE_URL}/plans`);

            const htmlBody = (warningTemplate.bodyHtml || "")
              .replace(/\{\{name\}\}/g, userName)
              .replace(/\{\{expiryDate\}\}/g, expiryDate.toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" }))
              .replace(/\{\{upgradeUrl\}\}/g, `${SITE_URL}/plans`);

            try {
              await resend.emails.send({
                from: "TheBigDayPage <noreply@thebigdaypage.com>",
                to: [userEmail],
                subject,
                html: htmlBody,
              });
              context.log(`Warning email sent to ${userEmail}`);
            } catch (emailErr) {
              context.error(
                `Failed to send warning email to ${userEmail}: ${emailErr.message}`
              );
              errorCount++;
              continue;
            }
          } else {
            context.log(
              `No warning template found, skipping email for ${userEmail}`
            );
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
          errorCount++;
        }
      }

      hasMore = batch.documents.length === BATCH_SIZE;
      offset += BATCH_SIZE;
    }
  } catch (err) {
    context.error(`Warning job failed: ${err.message}`);
  }

  // ─── Job 2: Expire free plans ───
  try {
    const expiryCutoff = new Date(now);
    expiryCutoff.setDate(expiryCutoff.getDate() - FREE_PLAN_DURATION_DAYS);
    const expiryCutoffISO = expiryCutoff.toISOString();

    context.log(`[Expiry] Checking users with planStarted <= ${expiryCutoffISO}`);

    // Fetch the expiry email template
    let expiryTemplate = null;
    try {
      const templateRes = await databases.listDocuments(
        SYSTEM_DB_ID,
        MAIL_TEMPLATES_COLLECTION_ID,
        [
          Query.equal("name", "free-plan-expired"),
          Query.equal("language", "en"),
        ]
      );
      if (templateRes.documents.length > 0) {
        expiryTemplate = templateRes.documents[0];
      }
    } catch (err) {
      context.error(`Failed to fetch expiry email template: ${err.message}`);
    }

    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const batch = await databases.listDocuments(DB_ID, USER_COLLECTION_ID, [
        Query.equal("plan", "free"),
        Query.lessThanEqual("planStarted", expiryCutoffISO),
        Query.notEqual("subscriptionStatus", "free_expired"),
        Query.notEqual("subscriptionStatus", "archived"),
        Query.limit(BATCH_SIZE),
        Query.offset(offset),
      ]);

      for (const userDoc of batch.documents) {
        try {
          // Calculate media deletion date (6 months grace)
          const deleteMediaDate = new Date(now);
          deleteMediaDate.setDate(
            deleteMediaDate.getDate() + MEDIA_GRACE_PERIOD_DAYS
          );

          // Update user status
          await databases.updateDocument(
            DB_ID,
            USER_COLLECTION_ID,
            userDoc.$id,
            {
              subscriptionStatus: "free_expired",
              deleteMedia: deleteMediaDate.toISOString(),
            }
          );

          context.log(`Expired free plan for user ${userDoc.userId}`);

          await writeLog({
            functionName: "tbdp-freeplanexpiry",
            action: "plan_expired",
            userId: userDoc.userId,
            details: `Free plan expired. Media scheduled for deletion on ${deleteMediaDate.toISOString()}.`,
          });

          // Send expiry notification email
          if (expiryTemplate) {
            let userAccount;
            try {
              userAccount = await users.get(userDoc.userId);
            } catch (err) {
              context.error(
                `Failed to fetch user ${userDoc.userId} for expiry email: ${err.message}`
              );
              expiredCount++;
              continue;
            }

            const userName = userAccount.name || "there";
            const userEmail = userAccount.email;

            if (userEmail) {
              const subject = (expiryTemplate.subject || "Your free plan has expired")
                .replace(/\{\{name\}\}/g, userName)
                .replace(/\{\{upgradeUrl\}\}/g, `${SITE_URL}/plans`);

              const htmlBody = (expiryTemplate.bodyHtml || "")
                .replace(/\{\{name\}\}/g, userName)
                .replace(/\{\{upgradeUrl\}\}/g, `${SITE_URL}/plans`);

              try {
                await resend.emails.send({
                  from: "TheBigDayPage <noreply@thebigdaypage.com>",
                  to: [userEmail],
                  subject,
                  html: htmlBody,
                });
                context.log(`Expiry email sent to ${userEmail}`);
              } catch (emailErr) {
                context.error(
                  `Failed to send expiry email to ${userEmail}: ${emailErr.message}`
                );
              }
            }
          }

          expiredCount++;
        } catch (err) {
          context.error(
            `Error expiring user ${userDoc.userId}: ${err.message}`
          );
          errorCount++;
        }
      }

      hasMore = batch.documents.length === BATCH_SIZE;
      offset += BATCH_SIZE;
    }
  } catch (err) {
    context.error(`Expiry job failed: ${err.message}`);
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
