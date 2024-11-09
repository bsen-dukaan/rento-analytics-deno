import { Hono } from "https://deno.land/x/hono@v4.3.11/mod.ts";
import { cors } from "https://deno.land/x/hono@v4.3.11/middleware.ts";
import * as postgres from "https://deno.land/x/postgres@v0.17.0/mod.ts";
import { stringify as csvStringify } from "https://deno.land/std@0.177.0/encoding/csv.ts";

const app = new Hono();

app.use(
  "/*",
  cors({
    origin: "*",
    allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowHeaders: ["Content-Type", "Authorization"],
    credentials: true,
  })
);

const POSTMARK_TOKEN = "66fafdf6-510f-4993-b2c2-f684c344bf39";
const POSTGRES_URL =
  "postgresql://bot9:4vj9s0ef5xz958n4@139.84.154.202:5432/bot9";

const pool = new postgres.Pool(POSTGRES_URL, 3, true);

interface Conversation {
  id: string;
  chatbotId: string;
  createdAt: Date;
  updatedAt: Date;
  endUserId: string;
  AgentId?: string;
  notes?: string;
  Source: string;
  ChatTags?: Array<{ name: string }>;
  tagNames?: string[];
}

interface Message {
  ConversationId: string;
  meta: any;
  chatUser: string;
  createdAt: Date;
  chatText: string;
}

interface Review {
  entityId: string;
  value: number;
  createdAt: Date;
}

interface TagAttribute {
  entityId: string;
  value: string;
}

interface User {
  id: string;
  email: string;
}

// Database connection helper
async function getConnection() {
  console.log("Getting database connection...");
  const connection = await pool.connect();
  console.log("Database connection established");
  return connection;
}

// Data Fetching Functions
async function fetchRawChatData(
  chatbotId: string,
  startDateTime: Date,
  endDateTime: Date
) {
  const client = await getConnection();

  try {
    const conversationsResult = await client.queryObject`
      SELECT 
        c.id,
        c."createdAt",
        c."Source",
        c."endUserId"
      FROM "Conversations" c
      WHERE c."chatbotId" = ${chatbotId}
      AND c."createdAt" BETWEEN ${startDateTime} AND ${endDateTime}
    `;

    if (!conversationsResult.rows.length) {
      return {
        conversations: [],
        messages: [],
        reviews: [],
        conversationTags: [],
        tagAttributes: [],
      };
    }

    const conversationIds = conversationsResult.rows.map((c) => c.id);
    const endUserIds = conversationsResult.rows.map((c) => c.endUserId);

    const [messagesResult, reviewsResult, tagsResult, userTagsResult] =
      await Promise.all([
        client.queryObject`
          SELECT 
            "ConversationId",
            meta,
            "createdAt"
          FROM "Messages"
          WHERE "ConversationId" = ANY(${conversationIds})
          ORDER BY "createdAt" ASC
        `,
        client.queryObject`
          SELECT "entityId", value
          FROM "Reviews"
          WHERE "entityId" = ANY(${conversationIds})
        `,
        client.queryObject`
          SELECT 
            ct.name,
            cct."conversationId" as "ConversationId"
          FROM "ChatTags" ct
          JOIN "ConversationChatTags" cct ON ct.id = cct."chatTagId"
          WHERE cct."conversationId" = ANY(${conversationIds})
        `,
        client.queryObject`
          SELECT "entityId", value
          FROM "TagAttributes"
          WHERE "entityId" = ANY(${endUserIds})
          AND "tagId" = '0f788ec5-2041-4e26-b44e-f04cf4d06bf6'
        `,
      ]);

    return {
      conversations: conversationsResult.rows,
      messages: messagesResult.rows,
      reviews: reviewsResult.rows,
      conversationTags: tagsResult.rows,
      tagAttributes: userTagsResult.rows,
    };
  } catch (error) {
    console.error("Database error:", error);
    throw new Error("Failed to fetch chat data");
  } finally {
    client.release();
  }
}

async function fetchRawAgentData(
  chatbotId: string,
  startDateTime: Date,
  endDateTime: Date
) {
  const client = await getConnection();

  try {
    const conversationsResult = await client.queryObject<Conversation>`
      SELECT 
        c.id, 
        c."createdAt", 
        c."updatedAt", 
        c."endUserId",
        c."AgentId", 
        c.notes, 
        c."Source",
        array_agg(ct.name) as "tagNames"
      FROM "Conversations" c
      LEFT JOIN "ConversationChatTags" cct ON c.id = cct."conversationId"
      LEFT JOIN "ChatTags" ct ON cct."chatTagId" = ct.id
      WHERE c."chatbotId" = ${chatbotId}
      AND c."createdAt" BETWEEN ${startDateTime} AND ${endDateTime}
      GROUP BY c.id, c."createdAt", c."updatedAt", c."endUserId", c."AgentId", c.notes, c."Source"
    `;

    const conversationIds = conversationsResult.rows.map((c) => c.id);
    const endUserIds = conversationsResult.rows.map((c) => c.endUserId);
    const agentIds = conversationsResult.rows
      .map((c) => c.AgentId)
      .filter(Boolean);

    const [
      messagesResult,
      reviewsResult,
      userTagsResult,
      cityTagsResult,
      usersResult,
    ] = await Promise.all([
      client.queryObject<Message>`
        SELECT 
          "ConversationId",
          meta,
          "chatUser",
          "createdAt",
          "chatText"
        FROM "Messages"
        WHERE "ConversationId" = ANY(${conversationIds})
        AND "createdAt" BETWEEN ${startDateTime} AND ${endDateTime}
        ORDER BY "createdAt" ASC
      `,
      client.queryObject<Review>`
        SELECT 
          "entityId",
          value,
          "createdAt"
        FROM "Reviews"
        WHERE "entityId" = ANY(${conversationIds})
      `,
      client.queryObject<TagAttribute>`
        SELECT 
          "entityId",
          value
        FROM "TagAttributes"
        WHERE "entityId" = ANY(${endUserIds})
        AND "tagId" = '0f788ec5-2041-4e26-b44e-f04cf4d06bf6'
      `,
      client.queryObject<TagAttribute>`
        SELECT 
          "entityId",
          value
        FROM "TagAttributes"
        WHERE "entityId" = ANY(${endUserIds})
        AND "tagId" = '5a225216-82bc-4761-9215-4dd10520cfca'
      `,
      client.queryObject<User>`
        SELECT id, email
        FROM "Users"
        WHERE id = ANY(${agentIds})
      `,
    ]);

    const agentEmails = usersResult.rows.map((u) => u.email);
    const invitationsResult =
      agentEmails.length > 0
        ? await client.queryObject`
        SELECT email, name
        FROM "Invitations"
        WHERE email = ANY(${agentEmails})
      `
        : { rows: [] };

    return {
      conversations: conversationsResult.rows.map((conv) => ({
        ...conv,
        ChatTags: conv.tagNames
          ? conv.tagNames.filter(Boolean).map((name) => ({ name }))
          : [],
      })),
      messages: messagesResult.rows,
      reviews: reviewsResult.rows,
      userIdTagAttributes: userTagsResult.rows,
      cityTagAttributes: cityTagsResult.rows,
      users: usersResult.rows,
      invitations: invitationsResult.rows,
    };
  } catch (error) {
    console.error("Database error:", error);
    throw new Error("Failed to fetch agent data");
  } finally {
    client.release();
  }
}

function base64Encode(str: string): string {
  const bytes = new TextEncoder().encode(str);
  const binString = Array.from(bytes, (x) => String.fromCodePoint(x)).join("");
  return btoa(binString);
}

function processChatData(rawData: any) {
  const { conversations, messages, reviews, conversationTags, tagAttributes } =
    rawData;

  if (!conversations || !Array.isArray(conversations)) {
    console.error("No conversations data found");
    return [];
  }

  const csatScores = new Map(
    (reviews || []).map((review: any) => [
      review.entityId,
      review.value?.toString() || "N/A",
    ])
  );

  const conversationTagsMap = new Map();
  (conversationTags || []).forEach((tag: any) => {
    if (!conversationTagsMap.has(tag.ConversationId)) {
      conversationTagsMap.set(tag.ConversationId, []);
    }
    conversationTagsMap
      .get(tag.ConversationId)
      .push(tag.name?.toString() || "N/A");
  });

  const userIdsMap = new Map(
    (tagAttributes || []).map((attr: any) => [
      attr.entityId,
      attr.value?.toString() || "N/A",
    ])
  );

  const conversationStatus = new Map();
  (messages || []).forEach((message: any) => {
    if (message.meta?.functionCall) {
      const convoId = message.ConversationId;
      if (!conversationStatus.has(convoId)) {
        conversationStatus.set(convoId, "Bot");
      }

      message.meta.functionCall.forEach((call: any) => {
        if (
          call.name === "AssignChatToAgent" ||
          call.name === "AutoAssignChats"
        ) {
          conversationStatus.set(convoId, "Assigned to agent");
        } else if (
          call.name === "escalateduringworkinghours" &&
          conversationStatus.get(convoId) !== "Assigned to agent"
        ) {
          conversationStatus.set(convoId, "Escalated during working hours");
        } else if (
          call.name === "escalateduringoutsideworkinghours" &&
          conversationStatus.get(convoId) !== "Assigned to agent"
        ) {
          conversationStatus.set(
            convoId,
            "Escalated during outside working hours"
          );
        }
      });
    }
  });

  const latestMessageTimes = new Map();
  (messages || []).forEach((message: any) => {
    const convoId = message.ConversationId;
    const messageTime = new Date(message.createdAt).getTime();
    if (
      !latestMessageTimes.has(convoId) ||
      messageTime > latestMessageTimes.get(convoId)
    ) {
      latestMessageTimes.set(convoId, messageTime);
    }
  });

  return conversations.map((convo: any) => ({
    "Chat Start Time": toISTString(new Date(convo.createdAt)),
    "Chat End Time": toISTString(
      new Date(latestMessageTimes.get(convo.id) || convo.createdAt)
    ),
    UserId: (
      userIdsMap.get(convo.endUserId) ||
      convo.endUserId ||
      "N/A"
    ).toString(),
    Tag: conversationTagsMap.has(convo.id)
      ? conversationTagsMap.get(convo.id).join(", ")
      : "N/A",
    ChatLink: `https://app.bot9.ai/inbox/${convo.id}?status=bot&search=`,
    CSAT: (csatScores.get(convo.id) || "N/A").toString(),
    "Handled by": (conversationStatus.get(convo.id) || "Bot").toString(),
    Source: (convo.Source || "N/A").toString(),
  }));
}

function toISTString(date: Date): string {
  const istDate = new Date(date.getTime() + 5.5 * 60 * 60 * 1000);
  const day = String(istDate.getUTCDate()).padStart(2, "0");
  const month = String(istDate.getUTCMonth() + 1).padStart(2, "0");
  const year = istDate.getUTCFullYear();
  let hours = istDate.getUTCHours();
  const minutes = String(istDate.getUTCMinutes()).padStart(2, "0");
  const seconds = String(istDate.getUTCSeconds()).padStart(2, "0");
  const ampm = hours >= 12 ? "PM" : "AM";
  hours = hours % 12;
  hours = hours ? hours : 12;
  return `${day}-${month}-${year}- ${hours}:${minutes}:${seconds} ${ampm}`;
}

function formatTime(seconds: number): string {
  if (seconds === null || seconds === undefined || isNaN(seconds)) return "N/A";

  seconds = Math.round(seconds);

  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = Math.floor(seconds % 60);

  if (seconds === 0) {
    return "00:00:01";
  }

  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(
    2,
    "0"
  )}:${String(remainingSeconds).padStart(2, "0")}`;
}

function processAgentData(rawData: any) {
  const {
    conversations,
    messages,
    reviews,
    userIdTagAttributes,
    cityTagAttributes,
    users,
    invitations,
  } = rawData;

  const messagesByConversation = new Map();
  messages.forEach((message) => {
    if (!messagesByConversation.has(message.ConversationId)) {
      messagesByConversation.set(message.ConversationId, []);
    }
    messagesByConversation.get(message.ConversationId).push(message);
  });

  const csatScores = new Map(
    reviews.map((review) => [
      review.entityId,
      {
        value: review.value,
        createdAt: review.createdAt
          ? new Date(review.createdAt).getTime()
          : null,
      },
    ])
  );

  const userIdsMap = new Map(
    userIdTagAttributes.map((attr) => [attr.entityId, attr.value || "N/A"])
  );

  const cityMap = new Map(
    cityTagAttributes.map((attr) => [attr.entityId, attr.value || "N/A"])
  );

  const agentNamesMap = new Map(
    invitations
      .map((invitation) => {
        const user = users.find((u) => u.email === invitation.email);
        return user ? [user.id, invitation.name] : null;
      })
      .filter(Boolean)
  );

  return conversations.flatMap((convo) => {
    const feedback = csatScores.get(convo.id);
    const city = cityMap.get(convo.endUserId) || "N/A";
    const channel = convo.Source || "N/A";
    const convoMessages = messagesByConversation.get(convo.id) || [];

    convoMessages.sort((a, b) => {
      const timeA = new Date(a.createdAt).getTime();
      const timeB = new Date(b.createdAt).getTime();
      return timeA - timeB;
    });

    let segments = [];
    let currentSegment = null;
    let currentHandler = null;
    let queueStartTime = null;
    let lastUserMessageTime = null;
    let currentAgentName = null;

    for (const message of convoMessages) {
      if (!message.createdAt) continue;

      const chatUser = message.chatUser;
      const createdAt = new Date(message.createdAt);

      if (chatUser === "system") {
        try {
          const chatText = JSON.parse(message.chatText);
          if (chatText.message === "An Agent will be Assigned Soon") {
            queueStartTime = createdAt;
          } else if (chatText.message?.includes("has joined the chat")) {
            const agentName = chatText.message.replace(
              " has joined the chat",
              ""
            );

            if (currentSegment) {
              currentSegment.endTime = createdAt;
              segments.push(currentSegment);
            }

            currentHandler = "Agent";
            currentAgentName = agentName;
            currentSegment = {
              handler: "Agent",
              agentName: agentName,
              startTime: createdAt,
              endTime: null,
              userMessages: 0,
              handlerMessages: 0,
              firstResponseTime: null,
              agentJoinTime: createdAt,
              totalResponseTime: 0,
              responseCount: 0,
              queueTime: queueStartTime
                ? createdAt.getTime() - queueStartTime.getTime()
                : 0,
            };
            queueStartTime = null;
          } else if (chatText.conversationTransfer === "closed") {
            if (currentSegment) {
              currentSegment.endTime = createdAt;
              segments.push(currentSegment);
              currentSegment = null;
              currentHandler = null;
              currentAgentName = null;
            }
          }
        } catch (e) {}
        continue;
      }

      let handlerType = currentHandler;

      if (chatUser === "bot") {
        handlerType = "Bot";
      } else if (chatUser === "humanagent") {
        handlerType = "Agent";
        if (
          currentSegment &&
          currentSegment.agentJoinTime &&
          !currentSegment.firstResponseTime
        ) {
          currentSegment.firstResponseTime =
            (createdAt.getTime() - currentSegment.agentJoinTime.getTime()) /
            1000;
        }
      } else if (chatUser === "user") {
        handlerType = currentHandler || "Bot";
        lastUserMessageTime = createdAt;
      }

      if (!currentSegment || (handlerType === "Agent" && !currentAgentName)) {
        if (currentSegment) {
          currentSegment.endTime = createdAt;
          segments.push(currentSegment);
        }
        currentSegment = {
          handler: handlerType,
          agentName: currentAgentName,
          startTime: createdAt,
          endTime: null,
          userMessages: 0,
          handlerMessages: 0,
          firstResponseTime: null,
          agentJoinTime: null,
          totalResponseTime: 0,
          responseCount: 0,
          queueTime: 0,
        };
        currentHandler = handlerType;
      }

      if (currentSegment) {
        if (chatUser === "user") {
          currentSegment.userMessages += 1;
        } else if (chatUser === "bot" || chatUser === "humanagent") {
          currentSegment.handlerMessages += 1;
          if (lastUserMessageTime) {
            const responseTime =
              (createdAt.getTime() - lastUserMessageTime.getTime()) / 1000;
            currentSegment.totalResponseTime += responseTime;
            currentSegment.responseCount += 1;
            lastUserMessageTime = null;
          }
        }
      }
    }

    if (currentSegment && convoMessages.length > 0) {
      currentSegment.endTime =
        currentSegment.endTime ||
        new Date(convoMessages[convoMessages.length - 1].createdAt);
      segments.push(currentSegment);
    }

    segments = segments.filter((segment) => {
      if (!segment.startTime || !segment.endTime) return false;
      if (segment.startTime.getTime() === segment.endTime.getTime())
        return false;
      if (segment.userMessages === 0 && segment.handlerMessages === 0)
        return false;
      return true;
    });

    return segments.map((segment) => ({
      "Chat Start Time": segment.startTime
        ? toISTString(segment.startTime)
        : "N/A",
      "Chat End Time": segment.endTime ? toISTString(segment.endTime) : "N/A",
      Channel: channel,
      UserId: userIdsMap.get(convo.endUserId) || "N/A",
      Tag: convo.ChatTags?.map((tag) => tag.name).join(", ") || "N/A",
      ChatLink: `https://app.bot9.ai/inbox/${convo.id}?status=bot&search=`,
      CSAT: feedback?.value?.toString() || "N/A",
      "Feedback for":
        feedback?.createdAt &&
        segment.startTime &&
        segment.endTime &&
        feedback.createdAt >= segment.startTime.getTime() &&
        feedback.createdAt <= segment.endTime.getTime()
          ? segment.handler
          : "N/A",
      City: city,
      "Handled by": segment.handler || "N/A",
      "Agent Name":
        segment.handler === "Bot"
          ? "Bot9"
          : segment.agentName || agentNamesMap.get(convo.AgentId) || "N/A",
      "Queue Time": segment.queueTime
        ? formatTime(Math.round(segment.queueTime / 1000))
        : "N/A",
      "Handling Time / Resolution Time":
        segment.startTime && segment.endTime
          ? formatTime(
              (segment.endTime.getTime() - segment.startTime.getTime()) / 1000
            )
          : "N/A",
      "First Response Time": segment.firstResponseTime
        ? formatTime(segment.firstResponseTime)
        : "N/A",
      "Average Response time":
        segment.responseCount > 0
          ? formatTime(segment.totalResponseTime / segment.responseCount)
          : "N/A",
      "Messages sent by user": segment.userMessages || 0,
      "Messages sent by handler": segment.handlerMessages || 0,
      Notes: convo.notes || "N/A",
    }));
  });
}

app.get("/", async (c) => {
  return c.json({ msg: "Rento analytics server is live" });
});

async function sendErrorEmail(
  email: string,
  errorDetails: string,
  requestType: "CSAT" | "Agent Metrics",
  requestParams: {
    bot9ID: string;
    startDate: string;
    startTime: string;
    endDate: string;
    endTime: string;
  }
) {
  try {
    const emailResponse = await fetch("https://api.postmarkapp.com/email", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "X-Postmark-Server-Token": POSTMARK_TOKEN,
      },
      body: JSON.stringify({
        From: "no-reply@bot9.ai",
        To: email,
        Cc: "biswarup.sen@rankz.io",
        Subject: `BOT9 - ${requestType} Export Failed`,
        TextBody:
          `Hello,\n\n` +
          `We encountered an error while processing your ${requestType} data export request.\n\n` +
          `Request Details:\n` +
          `- Bot ID: ${requestParams.bot9ID}\n` +
          `- Date Range: ${requestParams.startDate} ${requestParams.startTime} to ${requestParams.endDate} ${requestParams.endTime}\n\n` +
          `Error Details:\n${errorDetails}\n\n` +
          `Please try again or contact support if the issue persists.\n\n` +
          `Best regards,\nBOT9 Team`,
      }),
    });

    if (!emailResponse.ok) {
      console.error("Failed to send error notification email");
    }
  } catch (error) {
    console.error("Error sending error notification:", error);
  }
}

function splitDateRange(startDate: Date, endDate: Date, chunkDays: number = 2) {
  const chunks: { start: Date; end: Date }[] = [];
  let currentStart = new Date(startDate);

  while (currentStart < endDate) {
    let currentEnd = new Date(currentStart);
    currentEnd.setDate(currentEnd.getDate() + chunkDays);

    if (currentEnd > endDate) {
      currentEnd = new Date(endDate);
    }

    chunks.push({
      start: new Date(currentStart),
      end: new Date(currentEnd),
    });

    currentStart = new Date(currentEnd);
  }

  return chunks;
}

async function processDataInChunks(params: {
  bot9ID: string;
  email: string;
  startDateTime: Date;
  endDateTime: Date;
  fetchData: (bot9ID: string, start: Date, end: Date) => Promise<any>;
  processData: (rawData: any) => any[];
  columns: string[];
  reportType: "CSAT" | "Agent Metrics";
}) {
  const {
    bot9ID,
    email,
    startDateTime,
    endDateTime,
    fetchData,
    processData,
    columns,
    reportType,
  } = params;

  console.log(`Starting ${reportType} data processing for date range:`, {
    startDateTime,
    endDateTime,
    bot9ID,
  });

  const dateChunks = splitDateRange(startDateTime, endDateTime);
  console.log(`Split processing into ${dateChunks.length} chunks`);

  let totalRecordsProcessed = 0;
  let currentChunk = 1;

  for (const chunk of dateChunks) {
    console.log(`Processing chunk ${currentChunk}/${dateChunks.length}:`, {
      start: chunk.start,
      end: chunk.end,
    });

    try {
      const rawData = await fetchData(bot9ID, chunk.start, chunk.end);
      console.log(`Fetched raw data for chunk ${currentChunk}. Processing...`);

      const processedChunkData = processData(rawData);
      console.log(
        `Completed processing chunk ${currentChunk}. Records processed:`,
        processedChunkData.length
      );

      // const startFormatted = chunk.start.toLocaleString("en-US", {
      //   day: "2-digit",
      //   month: "short",
      //   year: "numeric",
      //   hour: "2-digit",
      //   minute: "2-digit",
      //   hour12: true,
      // });
      // const endFormatted = chunk.end.toLocaleString("en-US", {
      //   day: "2-digit",
      //   month: "short",
      //   year: "numeric",
      //   hour: "2-digit",
      //   minute: "2-digit",
      //   hour12: true,
      // });

      const csvData = await csvStringify(processedChunkData, {
        columns: columns,
        headers: true,
      });

      const csvBase64 = base64Encode(csvData);
      const filename = `${reportType.toLowerCase()}_data_${bot9ID}_${
        chunk.start.toISOString().split("T")[0]
      }_${chunk.end.toISOString().split("T")[0]}.csv`;

      console.log(`Sending email for chunk ${currentChunk}...`);

      const emailResponse = await fetch("https://api.postmarkapp.com/email", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Postmark-Server-Token": POSTMARK_TOKEN,
        },
        body: JSON.stringify({
          From: "no-reply@bot9.ai",
          To: email,
          Cc: "biswarup.sen@rankz.io",
          Subject: `BOT9 - ${reportType} Data Export`,
          TextBody:
            `Hello,\n\n` +
            `Please find attached your ${reportType} data export\n` +
            // `From: ${startFormatted}\n` +
            // `To: ${endFormatted}\n\n` +
            `Records in this file: ${processedChunkData.length}\n` +
            `Part ${currentChunk} of ${dateChunks.length}\n\n` +
            `Note: Due to the date range of your request, the data is being sent in ${dateChunks.length} separate parts.`,
          Attachments: [
            {
              Name: filename,
              Content: csvBase64,
              ContentType: "text/csv",
            },
          ],
        }),
      });

      if (!emailResponse.ok) {
        throw new Error(`Failed to send email for chunk ${currentChunk}`);
      }

      console.log(`Successfully sent email for chunk ${currentChunk}`);
      totalRecordsProcessed += processedChunkData.length;
      currentChunk++;

      processedChunkData.length = 0;
    } catch (error) {
      console.error(`Error processing chunk ${currentChunk}:`, error);
      throw error;
    }
  }

  console.log(
    `All chunks processed. Total records processed:`,
    totalRecordsProcessed
  );
  return totalRecordsProcessed;
}

app.post("/:bot9ID/csat", async (c) => {
  try {
    const { bot9ID } = c.req.param();
    const { startDate, startTime, endDate, endTime } = c.req.query();
    const { email } = await c.req.json();

    if (!email) {
      return c.json({ error: "Missing email" }, 400);
    }

    if (!bot9ID || !startDate || !startTime || !endDate || !endTime) {
      return c.json({ error: "Missing required parameters" }, 400);
    }

    const startDateTime = new Date(`${startDate}T${startTime}+05:30`);

    const endDateTime = new Date(`${endDate}T${endTime}+05:30`);
    endDateTime.setSeconds(endDateTime.getSeconds() + 60);

    Promise.resolve().then(async () => {
      try {
        const csatColumns = [
          "Chat Start Time",
          "Chat End Time",
          "UserId",
          "Tag",
          "ChatLink",
          "CSAT",
          "Handled by",
          "Source",
        ];

        await processDataInChunks({
          bot9ID,
          email,
          startDateTime,
          endDateTime,
          fetchData: fetchRawChatData,
          processData: processChatData,
          columns: csatColumns,
          reportType: "CSAT",
        });
      } catch (error) {
        console.error("Error in background CSAT processing:", error);
        await sendErrorEmail(
          email,
          error.message || "Unknown error occurred during processing",
          "CSAT",
          { bot9ID, startDate, startTime, endDate, endTime }
        );
      }
    });

    return c.json({
      message:
        "Your request is being processed. You will receive the CSAT data in your email shortly.",
      success: true,
    });
  } catch (error) {
    console.error("Error initiating request:", error);
    return c.json(
      {
        error: "Failed to initiate request",
        details: error.message,
      },
      500
    );
  }
});

app.post("/:bot9ID/agent-dump", async (c) => {
  try {
    const { bot9ID } = c.req.param();
    const { startDate, startTime, endDate, endTime } = c.req.query();
    const { email } = await c.req.json();

    if (!email) {
      return c.json({ error: "Missing email" }, 400);
    }

    if (!bot9ID || !startDate || !startTime || !endDate || !endTime) {
      return c.json({ error: "Missing required parameters" }, 400);
    }

    const startDateTime = new Date(`${startDate}T${startTime}+05:30`);

    const endDateTime = new Date(`${endDate}T${endTime}+05:30`);
    endDateTime.setSeconds(endDateTime.getSeconds() + 60);

    Promise.resolve().then(async () => {
      try {
        const agentColumns = [
          "Chat Start Time",
          "Chat End Time",
          "Channel",
          "UserId",
          "Tag",
          "ChatLink",
          "CSAT",
          "Feedback for",
          "City",
          "Handled by",
          "Agent Name",
          "Queue Time",
          "Handling Time / Resolution Time",
          "First Response Time",
          "Average Response time",
          "Messages sent by user",
          "Messages sent by handler",
          "Notes",
        ];

        await processDataInChunks({
          bot9ID,
          email,
          startDateTime,
          endDateTime,
          fetchData: fetchRawAgentData,
          processData: processAgentData,
          columns: agentColumns,
          reportType: "Agent Metrics",
        });
      } catch (error) {
        console.error("Error in background agent metrics processing:", error);
        await sendErrorEmail(
          email,
          error.message || "Unknown error occurred during processing",
          "Agent Metrics",
          { bot9ID, startDate, startTime, endDate, endTime }
        );
      }
    });

    return c.json({
      message:
        "Your request is being processed. You will receive the Agent Metrics data in your email shortly.",
      success: true,
    });
  } catch (error) {
    console.error("Error initiating request:", error);
    return c.json(
      {
        error: "Failed to initiate request",
        details: error.message,
      },
      500
    );
  }
});

export default app;
