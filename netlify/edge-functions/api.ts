// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-blocking.mjs/+esm';
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";
import data from "../../public/options_data.parquet" with {
    type: "bytes",
};

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();
const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
await db.instantiate(() => { });

const app = new Hono();

app.get("/api/hello", (c) => {
  const connection = db.connect();
  const t = Deno.readDirSync("public");

  //const connection = await duckDbInstance.connect();
  const result = connection.query(`SELECT version() AS version`);

  const rows = result.toArray();
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows, files: t.map(k => k.name), d: data.length });
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