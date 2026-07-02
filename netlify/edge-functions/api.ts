// @deno-types="https://esm.sh/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME, DuckDBConnection } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-blocking.mjs/+esm';
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";
//import { getHistoricalSnapshotDatesFromParquet } from "../../lib/historicalOptions.ts";
let connection: DuckDBConnection;

async function init() {
  if (!connection) {
    console.log("Starting up DuckDB on Netlify Edge Functions...");
    const logger = new ConsoleLogger();
    const JSDELIVR_BUNDLES = getJsDelivrBundles();
    const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
    await db.instantiate(() => { });
    connection = db.connect();
    console.log("Duckdb Booting completed...");
  }
  return connection;
}
const app = new Hono();

app.get("/api/hello", async (c) => {
  const connection = await init();

  //const historicalDates = await getHistoricalSnapshotDatesFromParquet("AAPL");

  //const connection = await duckDbInstance.connect();
  const result = connection.query(`SELECT version() AS version`);

  const rows = result.toArray();
  console.log("End of func call!");
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows, historicalDates: [] });
});

app.get("/api/query", async (c) => {
  const connection = await init();

  //const connection = await duckDbInstance.connect();
  const result = connection.query(`SELECT * from 'temp/options_data.parquet' LIMIT 100`);

  const rows = result.toArray();
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows });
});

app.get("/api/memory", (c) => {
  const memoryInfo = Deno.systemMemoryInfo();
  const loadAvg = Deno.loadavg();
  const memoryUsage = Deno.memoryUsage();

  return c.json({
    message: "System Memory Info",
    totalMemoryMB: memoryInfo.total / 1024 / 1024,
    freeMemoryMB: memoryInfo.free / 1024 / 1024,
    availableMemoryMB: memoryInfo.available / 1024 / 1024,
    buffersMB: memoryInfo.buffers / 1024 / 1024,
    cachedMB: memoryInfo.cached / 1024 / 1024,
    swapTotalMB: memoryInfo.swapTotal / 1024 / 1024,
    swapFreeMB: memoryInfo.swapFree / 1024 / 1024,
    loadAverage1Min: loadAvg[0],
    loadAverage5Min: loadAvg[1],
    loadAverage15Min: loadAvg[2],
    externalMemoryUsageMB: memoryUsage.external / 1024 / 1024,
    rssMemoryUsageMB: memoryUsage.rss / 1024 / 1024,
    heapTotalMemoryUsageMB: memoryUsage.heapTotal / 1024 / 1024,
    heapUsedMemoryUsageMB: memoryUsage.heapUsed / 1024 / 1024
  })
});

export const config = { path: "/api/*" };
export default handle(app);