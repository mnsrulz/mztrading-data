import { createClient } from "https://esm.sh/@libsql/client@0.17.4/web";

const url = Deno.env.get("TURSO_URL");
const authToken = Deno.env.get("TURSO_TOKEN");

if (!url || !authToken) {
  console.warn("TURSO_URL or TURSO_TOKEN not set — Turso store disabled");
}

const client = url && authToken ? createClient({ url, authToken }) : null;

if (client) {
  client.execute("CREATE TABLE IF NOT EXISTS request_results (id TEXT PRIMARY KEY, payload TEXT NOT NULL, created_at INTEGER)")
    .then(() => console.log("Turso: table ready"))
    .catch(e => console.error("Turso: table creation failed:", e));
} else {
  console.warn(`Client not initialized!`)
}

export async function storeResult(id: string, payload: unknown) {
  if (!client) return;
  const raw = JSON.stringify(payload);
  await client.execute({
    sql: "INSERT OR REPLACE INTO request_results (id, payload, created_at) VALUES (?, ?, ?)",
    args: [id, raw, Date.now()],
  });
}

export async function getResult(id: string) {
  if (!client) return null;
  const rs = await client.execute({
    sql: "SELECT payload FROM request_results WHERE id = ? LIMIT 1",
    args: [id],
  });
  if (rs.rows.length === 0) return null;
  return JSON.parse(rs.rows[0].payload as string);
}

export async function deleteResult(id: string) {
  if (!client) return;
  await client.execute({
    sql: "DELETE FROM request_results WHERE id = ?",
    args: [id],
  });
}
