import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";

import { DuckDBInstance } from "npm:@duckdb/node-api@1.5.2-r.1";
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


const duckDbInstance = await DuckDBInstance.create(":memory:");

const execute = async ()=> {
    const connection = await duckDbInstance.connect();
    const result = await connection.runAndReadAll(`SELECT 1 AS c1`);
    return result;
}

app.get('/', async c=> {
    const result = await execute();
    c.json(result);
})

export const config = { path: "/api/*" };
export default handle(app);