import { Hono } from "https://deno.land/x/hono@v4.3.11/mod.ts";
import * as postgres from "https://deno.land/x/postgres@v0.17.0/mod.ts";
import { stringify as csvStringify } from "https://deno.land/std@0.177.0/encoding/csv.ts";

const app = new Hono();

const POSTMARK_TOKEN = "66fafdf6-510f-4993-b2c2-f684c344bf39";
const POSTGRES_URL =
  "postgresql://bot9:4vj9s0ef5xz958n4@139.84.208.184:5432/bot9";

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
    // Fetch conversations
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

    // Fetch all related data in parallel
    const [messagesResult, reviewsResult, tagsResult, userTagsResult] =
      await Promise.all([
        // Messages
        client.queryObject`
        SELECT 
          "ConversationId",
          meta,
          "createdAt"
        FROM "Messages"
        WHERE "ConversationId" = ANY(${conversationIds})
        ORDER BY "createdAt" ASC
      `,
        // Reviews
        client.queryObject`
        SELECT "entityId", value
        FROM "Reviews"
        WHERE "entityId" = ANY(${conversationIds})
      `,
        // Chat tags
        client.queryObject`
        SELECT 
          ct.name,
          cct."conversationId" as "ConversationId"
        FROM "ChatTags" ct
        JOIN "ConversationChatTags" cct ON ct.id = cct."chatTagId"
        WHERE cct."conversationId" = ANY(${conversationIds})
      `,
        // User tag attributes
        client.queryObject`
        SELECT "entityId", value
        FROM "TagAttributes"
        WHERE "entityId" = ANY(${endUserIds})
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
    // Fetch conversations with ChatTags and AgentIds
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

    // Get agent names if available
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

  // Create lookups
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

  // Track conversation statuses
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

  // Get latest message times
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

  // Process conversations
  return conversations.map((convo: any) => ({
    "Chat Start Time": toISTString(new Date(convo.createdAt)),
    "Chat End Time": toISTString(
      new Date(latestMessageTimes.get(convo.id) || convo.createdAt)
    ),
    UserId: (userIdsMap.get(convo.endUserId) || "N/A").toString(),
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

  // Round the seconds to remove fractional parts
  seconds = Math.round(seconds);

  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = Math.floor(seconds % 60);

  // If time is less than a second, return "00:00:01" instead of "00:00:00"
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

  // Create lookup maps
  const csatScores = new Map(
    reviews.map((review) => [
      review.entityId,
      {
        value: review.value,
        createdAt: new Date(review.createdAt).getTime(),
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

    convoMessages.sort((a, b) => new Date(a.createdAt) - new Date(b.createdAt));

    let segments = [];
    let currentSegment = null;
    let currentHandler = null;
    let queueStartTime = null;
    let agentJoinTime = null;
    let lastUserMessageTime = null;

    for (const message of convoMessages) {
      const chatUser = message.chatUser;
      const createdAt = new Date(message.createdAt);

      if (chatUser === "system") {
        try {
          const chatText = JSON.parse(message.chatText);
          if (chatText.message === "An Agent will be Assigned Soon") {
            queueStartTime = createdAt;
          } else if (chatText.message?.includes("has joined the chat")) {
            agentJoinTime = createdAt;
            if (currentSegment) {
              currentSegment.agentJoinTime = createdAt;
            }
            if (queueStartTime) {
              const queueTimeMs =
                agentJoinTime.getTime() - queueStartTime.getTime();
              if (currentSegment) {
                currentSegment.queueTime = queueTimeMs;
              }
            }
          }
        } catch (e) {}
      }

      let handlerType = currentHandler;

      if (chatUser === "bot") {
        handlerType = "Bot";
      } else if (chatUser === "humanagent") {
        handlerType = "Agent";
        // Calculate first response time if agent hasn't sent a message yet
        if (
          currentSegment &&
          currentSegment.agentJoinTime &&
          !currentSegment.firstResponseTime
        ) {
          currentSegment.firstResponseTime =
            (createdAt.getTime() - currentSegment.agentJoinTime.getTime()) /
            1000;
        }
      } else if (chatUser === "system") {
        let chatText = {};
        try {
          chatText = message.chatText ? JSON.parse(message.chatText) : {};
        } catch (e) {}

        if (chatText.conversationTransfer === "needs_review") {
          handlerType = "Agent";
        } else if (chatText.conversationTransfer === "closed") {
          if (currentSegment) {
            currentSegment.endTime = createdAt;
            segments.push(currentSegment);
            currentSegment = null;
            currentHandler = null;
          }
          continue;
        }
      } else if (chatUser === "user") {
        handlerType = currentHandler || "Bot";
        lastUserMessageTime = createdAt;
      }

      if (handlerType !== currentHandler) {
        if (currentSegment) {
          currentSegment.endTime = createdAt;
          segments.push(currentSegment);
        }
        currentSegment = {
          handler: handlerType,
          startTime: createdAt,
          endTime: null,
          userMessages: 0,
          handlerMessages: 0,
          firstResponseTime: null,
          agentJoinTime: agentJoinTime, // Track when agent joined
          totalResponseTime: 0,
          responseCount: 0,
          queueTime:
            queueStartTime && agentJoinTime
              ? agentJoinTime.getTime() - queueStartTime.getTime()
              : 0,
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

    if (currentSegment) {
      currentSegment.endTime =
        currentSegment.endTime || currentSegment.startTime;
      segments.push(currentSegment);
    }

    segments = segments.filter((segment) => {
      if (segment.startTime.getTime() === segment.endTime.getTime())
        return false;
      if (segment.userMessages === 0 && segment.handlerMessages === 0)
        return false;
      if (
        segment.handler === "Bot" &&
        segment.handlerMessages === 1 &&
        segment.userMessages === 0 &&
        segments.some(
          (s) =>
            s.handler === "Agent" &&
            s.startTime.getTime() === segment.endTime.getTime()
        )
      )
        return false;
      return true;
    });

    return segments.map((segment) => ({
      "Chat Start Time": toISTString(segment.startTime),
      "Chat End Time": toISTString(segment.endTime),
      Channel: channel,
      UserId: userIdsMap.get(convo.endUserId) || "N/A",
      Tag: convo.ChatTags?.map((tag) => tag.name).join(", ") || "N/A",
      ChatLink: `https://app.bot9.ai/inbox/${convo.id}?status=bot&search=`,
      CSAT: feedback?.value?.toString() || "N/A",
      "Feedback for":
        feedback?.createdAt >= segment.startTime.getTime() &&
        feedback?.createdAt <= segment.endTime.getTime()
          ? segment.handler
          : "N/A",
      City: city,
      "Handled by": segment.handler || "N/A",
      "Agent Name":
        segment.handler === "Bot"
          ? "Bot9"
          : agentNamesMap.get(convo.AgentId) || "N/A",
      "Queue Time": segment.queueTime
        ? formatTime(Math.round(segment.queueTime / 1000))
        : "N/A",
      "Handling Time / Resolution Time": formatTime(
        (segment.endTime.getTime() - segment.startTime.getTime()) / 1000
      ),
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

app.post("/:bot9ID/csat/rentomojo", async (c) => {
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

    const startDateTime = new Date(`${startDate}T${startTime}Z`);
    const endDateTime = new Date(`${endDate}T${endTime}Z`);

    const rawData = await fetchRawChatData(bot9ID, startDateTime, endDateTime);
    const processedData = processChatData(rawData);

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

    const csvData = await csvStringify(processedData, {
      columns: csatColumns,
      headers: true,
    });

    const csvBase64 = base64Encode(csvData);
    const filename = `csat_data_${bot9ID}_${startDate}_${endDate}.csv`;

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
        Subject: "BOT9 - CSAT Data Export",
        TextBody: `Hello,\n\nPlease find attached your CSAT data for your bot from ${startDate} ${startTime} to ${endDate} ${endTime}.`,
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
      throw new Error("Failed to send email");
    }

    return c.json({
      message: "CSAT data has been sent to your email",
      success: true,
    });
  } catch (error) {
    console.error("Error processing request:", error);
    return c.json(
      {
        error: "Failed to process request",
        details: error.message,
      },
      500
    );
  }
});

app.post("/:bot9ID/agent-dump/rentomojo", async (c) => {
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

    const startDateTime = new Date(`${startDate}T${startTime}Z`);
    const endDateTime = new Date(`${endDate}T${endTime}Z`);

    const rawData = await fetchRawAgentData(bot9ID, startDateTime, endDateTime);
    const processedData = processAgentData(rawData);

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

    const csvData = await csvStringify(processedData, {
      columns: agentColumns,
      headers: true,
    });

    const csvBase64 = base64Encode(csvData);
    const filename = `agent_metrics_${bot9ID}_${startDate}_${endDate}.csv`;

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
        Subject: "BOT9 - Agent Metrics Data Export",
        TextBody: `Hello,\n\nPlease find attached your Agent Metrics data for your bot from ${startDate} ${startTime} to ${endDate} ${endTime}.`,
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
      throw new Error("Failed to send email");
    }

    return c.json({
      message: "Agent metrics data has been sent to your email",
      success: true,
    });
  } catch (error) {
    console.error("Error processing request:", error);
    return c.json(
      {
        error: "Failed to process request",
        details: error.message,
      },
      500
    );
  }
});

export default app;
