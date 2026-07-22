import { Hono } from "https://esm.sh/hono@4.12.27";
import { cors } from "https://esm.sh/hono@4.12.27/cors";
import { handle, getConnInfo } from "https://esm.sh/hono@4.12.27/netlify";
import { HTTPException } from "https://esm.sh/hono@4.12.27/http-exception";
import Pusher from 'https://esm.sh/pusher@5.3.3';
import { Redis } from 'https://esm.sh/@upstash/redis@1.38.0'
import delay from "https://esm.sh/delay@7.0.0";
import { storeResult, getResult } from './tursoStore.ts';
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
    const start = performance.now();
    const req = c.req.raw;
    try {
        c.header("x-server-instance-id", instanceId);
        await next();
    } finally {
        const end = performance.now();
        console.log(
            `${req.method} ${new URL(req.url).pathname} ${req.headers.get("x-forwarded-for") || req.headers.get("x-real-ip") || ""} ${(end - start).toFixed(2)} ms`,
        );
    }
});

app.post('/api/requests', async c => {
    const args = await c.req.json<{ symbol: string; requestId: string, requestType: string }>().catch(() => null);
    if (!args) {
        throw new HTTPException(400, { message: "Search request is empty!" });
    }

    args.requestId = crypto.randomUUID();   //let's overwrite the id

    //Register a 10sec timer, if we don't receive the response we will fire a timeout event through pusher.
    const tmr = setTimeout(async () => {
        await pusher.trigger(pusherConfig.channelName, 'query-timeout', { instanceId, ...args });
    }, 10000);

    const info = getConnInfo(c)
    const clientId = info.remote.address;
    await redis.publish("worker-request", JSON.stringify({ channel: `channel-${instanceId}`, ...args, clientId }));
    // for instrumentation only now.
    waitForResultFromTurso(args.requestId).catch(err => console.error(`timeout retreving from turso store..`, err));
    const data = await waitForResult(args.requestId);
    clearTimeout(tmr);
    return c.json(data);
});

app.put('/api/requests/:id/result', async (c) => {
    const id = c.req.param('id');
    const body = await c.req.json().catch(() => null);
    if (!body) throw new HTTPException(400, { message: "Invalid payload or empty data." });

    try {
        await storeResult(id, body);
        return c.json({ message: "Result stored" });
    } catch (error) {
        console.error(`Failed to store result for ${id}:`, error);
        return c.json({ message: "Failed to persist result" }, 500);
    }
});

async function waitForResult(id: string, timeoutMs = 10000, pollIntervalMs = 200) {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
        const raw = await redis.get(id);
        if (raw) {
            //redis.del(id).catch(() => {});
            return typeof raw === 'string' ? JSON.parse(raw) : raw;
        }
        await delay(pollIntervalMs);
    }
    throw new Error(`Polling timed out after ${timeoutMs / 1000} seconds for ID: ${id}`);
}

async function waitForResultFromTurso(id: string, timeoutMs = 10000, pollIntervalMs = 200) {
    const startTime = Date.now();
    while (Date.now() - startTime < timeoutMs) {
        const raw = await getResult(id);
        if (raw) {
            //redis.del(id).catch(() => {});
            console.log(`Result retrieved from turso store for this id: ${id}`);
            return;
        }
        await delay(pollIntervalMs);
    }
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
