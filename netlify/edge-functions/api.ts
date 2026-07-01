import { Hono } from "hono";
import { handle } from "hono/netlify";
import * as duckdb from "https://esm.sh/@duckdb/duckdb-wasm";

const app = new Hono();


app.get("/api/hello", async (c) => {

  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);

  const worker = new Worker(bundle.mainWorker!);

  const logger = new duckdb.ConsoleLogger();

  const db = new duckdb.AsyncDuckDB(logger, worker);

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  const connection = await db.connect();

  //const connection = await duckDbInstance.connect();
  const result = await connection.query(`SELECT 1 AS value`);
  
  const rows = result.toArray();
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows });
});

export const config = { path: "/api/*" };
export default handle(app);