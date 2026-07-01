// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-blocking.mjs/+esm';
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";

console.log("Starting up DuckDB on Netlify Edge Functions...");

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await db.instantiate(() => { });

console.log("Duckdb Booting completed...");
const app = new Hono();

app.get("/api/hello", async (c) => {
  console.log("Start of func call!");
  const connection = db.connect();
  
  const url = new URL("/options_data.parquet", c.req.url);
  const res = await fetch(url);
  
  const buffer = await res.arrayBuffer();
  
  //const connection = await duckDbInstance.connect();
  const result = connection.query(`SELECT version() AS version`);
  
  const rows = result.toArray();
  console.log("End of func call!");
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows, d: buffer.byteLength });
});

app.get("/api/query", (c) => {
  const connection = db.connect();

  //const connection = await duckDbInstance.connect();
  const result = connection.query(`SELECT * from 'temp/options_data.parquet' LIMIT 100`);

  const rows = result.toArray();
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows });
});

export const config = { path: "/api/*" };
export default handle(app);