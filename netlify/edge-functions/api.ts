import { Hono } from "hono";
import { handle } from "hono/netlify";

const app = new Hono();

app.get("/api/hello", (c) => {
  return c.json({ message: "Hello from Deno on Netlify Edge!" });
});

export const config = { path: "/api/*" };
export default handle(app);