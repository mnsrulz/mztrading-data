import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";
//import { DuckDBInstance } from 'npm:@duckdb/node-bindings';
// @deno-types="https://esm.sh/v135/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME, DuckDBBindings } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
const instanceId = crypto.randomUUID();

// Start a Hono app
const app = new Hono();

app.use("*", async (c, next) => {
  await next();
  c.header("x-server-instance-id", instanceId);
});

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

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();

const execute = async ()=> {
    const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
    await db.instantiate(() => { });
    
    const connection = db.connect();
    const result = await connection.send(`SELECT 1 AS c1`);
    return result.readAll().flatMap(k => k.toArray().map((row) => row.toJSON()));
}

app.get('/api/duckdb', async c=> {
    const result = await execute();
    return c.json(result);
})

export const config = { path: "/api/*" };
export default handle(app);