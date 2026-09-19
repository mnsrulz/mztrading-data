import type { Context } from "@netlify/functions";
import { Hono } from "hono";
import { cors } from "hono/cors";
import { AmqpClient } from "../../amqpClient.js";

const app = new Hono();
app.use("*", cors());

app.get("/api/health", (c) => c.json({ status: "ok" }));

app.post("/api/requests", async (c) => {
  let body: Record<string, unknown>;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: "Invalid JSON body" }, 400);
  }

  if (!body?.requestType || !body?.symbol || !body?.requestId) {
    return c.json(
      { error: "Missing required fields: requestType, symbol, requestId" },
      400
    );
  }

  const client = new AmqpClient();

  try {
    await client.connect();
    await client.declareQueue();

    const correlationId = body.requestId as string;
    await client.publishRequest(
      body.requestType as string,
      body as never,
      correlationId
    );

    const result = await client.waitForReply(correlationId, 8000);
    return c.json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";

    if (message.includes("Reply timeout")) {
      return c.json({ error: "Worker reply timeout" }, 504);
    }

    console.error("AMQP error:", message);
    return c.json({ error: "Internal server error" }, 500);
  } finally {
    await client.close();
  }
});

export default async (req: Request, context: Context) => {
  return app.fetch(req);
};

export const config = {
  path: "/api/*",
};
