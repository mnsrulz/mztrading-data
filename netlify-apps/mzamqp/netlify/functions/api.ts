import type { Context } from "@netlify/functions";
import { AmqpClient, type RequestPayload } from "../../amqpClient.js";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...CORS_HEADERS,
    },
  });
}

export default async (req: Request, context: Context): Promise<Response> => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  if (req.method !== "POST") {
    return jsonResponse({ error: "Method not allowed" }, 405);
  }

  let body: RequestPayload;
  try {
    body = await req.json();
  } catch {
    return jsonResponse({ error: "Invalid JSON body" }, 400);
  }

  if (!body?.requestType || !body?.symbol || !body?.requestId) {
    return jsonResponse(
      { error: "Missing required fields: requestType, symbol, requestId" },
      400
    );
  }

  const client = new AmqpClient();

  try {
    await client.connect();
    await client.declareQueue();

    const correlationId = body.requestId;
    await client.publishRequest(body.requestType, body, correlationId);

    const result = await client.waitForReply(correlationId, 8000);
    return jsonResponse(result, 200);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";

    if (message.includes("Reply timeout")) {
      return jsonResponse({ error: "Worker reply timeout" }, 504);
    }

    console.error("AMQP error:", message);
    return jsonResponse({ error: "Internal server error" }, 500);
  } finally {
    await client.close();
  }
};

export const config = {
  path: "/api/requests",
};
