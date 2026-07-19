import { Hono } from "https://esm.sh/hono@4.12.27";
import { getStore } from "https://esm.sh/@netlify/blobs@10.7.9";
import { cors } from "https://esm.sh/hono@4.12.27/cors";
import { handle, getConnInfo } from "https://esm.sh/hono@4.12.27/netlify";
import { HTTPException } from "https://esm.sh/hono@4.12.27/http-exception";
import Pusher from 'https://esm.sh/pusher@5.3.3';
import { Redis } from 'https://esm.sh/@upstash/redis@1.38.0'
import delay from "https://esm.sh/delay@7.0.0";
const instanceId = crypto.randomUUID();

const pusherConfig = {
    pusherUri: Deno.env.get("PUSHER_URI"),
    channelName: Deno.env.get("PUSHER_CHANNEL_NAME") || "mztrading-channel",
}

const redis = Redis.fromEnv();

if (!pusherConfig.pusherUri) {
    throw new Error("PUSHER_URI is not set. Please set it in your environment variables to enable pusher functionality.");
};

const pusher = Pusher.forURL(pusherConfig.pusherUri!);

const app = new Hono();
app.use('/api/*', cors());

app.use("*", async (c, next) => {
    c.header("x-server-instance-id", instanceId);
    await next();
});

app.post('/api/requests', async c => {
    const args = await c.req.json<{ symbol: string; requestId: string, requestType: string }>().catch(() => null);
    if (!args) {
        throw new HTTPException(400, { message: "Search request is empty!" });
    }

    //Register a 10sec timer, if we don't receive the response we will fire a timeout event through pusher.
    const tmr = setTimeout(async () => {
        await pusher.trigger(pusherConfig.channelName, 'query-timeout', { instanceId, ...args });
    }, 10000);

    const info = getConnInfo(c)
    const clientId = info.remote.address;
    await redis.publish("worker-request", JSON.stringify({ channel: `channel-${instanceId}`, ...args, clientId }));
    const data = await waitForResult(args.requestId);
    clearTimeout(tmr);
    return c.json(data);
});

app.put('/api/requests/:id/result', async (c) => {
    const id = c.req.param('id');
    const body = await c.req.json().catch(() => null);
    if (!body) throw new HTTPException(400, { message: "Invalid payload or empty data." });

    try {
        const resultStore = getStore({
            name: "request-results",
            consistency: "strong" // Ensures immediate read availability
        });

        await resultStore.setJSON(id, body);
        return c.json({ message: "Result safely stored in Netlify Blobs" });
    } catch (error) {
        console.error(`Failed to store blob for ${id}:`, error);
        throw new HTTPException(500, { message: "Failed to persist data to blob storage" });
    }
});

/**
 * Polls the Netlify Blob store for a specific ID until it exists or times out.
 * @param id The query/request ID to check
 * @param timeoutMs Maximum time to wait in milliseconds (defaults to 10000ms)
 * @param pollIntervalMs Time to wait between checks (defaults to 500ms)
 */
async function waitForResult(id: string, timeoutMs = 10000, pollIntervalMs = 500) {
    const startTime = Date.now();
    const resultStore = getStore({
        name: "request-results",
        consistency: "strong" // Ensures immediate read availability
    });

    while (Date.now() - startTime < timeoutMs) {

        // Attempt to fetch the data from Netlify Blobs
        const data = await resultStore.get(id, {
            type: "json"
        })

        if (data) return data;
        // Wait a short duration before trying again to prevent rate-limiting
        await delay(pollIntervalMs);
    }

    // If the loop finishes without finding data, it timed out
    throw new Error(`Polling timed out after ${timeoutMs / 1000} seconds for ID: ${id}`);
}

app.onError((err, c) => {
    console.error("Global error handler caught:", err); // Log the error if it's not known

    // For other errors, return a generic 500 response
    return c.json(
        {
            success: false,
            errors: [{ code: 7000, message: "Internal Server Error" }],
        },
        500,
    );
});


console.info(`App started with instance: ${instanceId}`)

export const config = { path: "/api/*" };
export default handle(app);