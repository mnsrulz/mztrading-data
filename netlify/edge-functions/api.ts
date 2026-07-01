// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';
import { Hono } from "hono";
import { handle } from "hono/netlify";

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await db.instantiate(() => { });

const app = new Hono();


app.get("/api/hello", async (c) => {
  const connection = await db.connect();

  //const connection = await duckDbInstance.connect();
  const result = await connection.query(`SELECT 1 AS value`);
  
  const rows = result.toArray();
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows });
});

export const config = { path: "/api/*" };
export default handle(app);