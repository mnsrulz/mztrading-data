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

  if (!body?.requestType || !body?.symbol) {
    return c.json(
      { error: "Missing required fields: requestType, symbol" },
      400
    );
  }

  const requestId = crypto.randomUUID();
  const client = new AmqpClient();

  try {
    await client.connect();
    await client.declareQueue();

    const result = await client.rpc(
      body.requestType as string,
      { ...body, requestId } as never,
      requestId,
      8000
    );
    return c.json({ requestId, ...result as object });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";

    if (message.includes("Reply timeout")) {
      return c.json({ error: "Worker reply timeout" }, 504);
    }

    console.error("AMQP error:", message);
    return c.json({ error: message }, 500);
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
