import { Hono } from "hono";
import { handle } from "hono/netlify";
import { DuckDBInstance } from "@duckdb/node-api";

const app = new Hono();
const duckDbInstance = await DuckDBInstance.create(":memory:");

app.get("/api/hello", async (c) => {
  const connection = await duckDbInstance.connect();
  const result = await connection.runAndReadAll(`SELECT 1 AS value`)
  const rows = { rows: result.getRowsJson(), columns: result.columnNamesAndTypesJson() };
  return c.json({ message: "Hello from Deno on Netlify Edge!", rows });
});

export const config = { path: "/api/*" };
export default handle(app);