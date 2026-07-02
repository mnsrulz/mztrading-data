import { OptionsExpirationStrikes } from "../../lib/data.ts"
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";

const app = new Hono();
app.get("/api2/hello", (c) => {
    const symbol = c.req.query("symbol") || "AAPL";
    const expiry = c.req.query("expiry") || "2026-01-16";
    return c.json({
        symbol,
        expiry,
        data: OptionsExpirationStrikes[symbol][expiry]
    });
});

export const config = { path: "/api2/*" };
export default handle(app);