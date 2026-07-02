import { allTickerSymbols } from "../../lib/data.ts"
import { Hono } from "hono";
import { handle } from "hono/netlify";

import p from "./../../public/options_data.parquet" with {
    type: "bytes",
};

const app = new Hono();
app.get("/api3/hello", (c) => {
    return c.json({
        symbols: allTickerSymbols,
        l: p.length
    });
});

export const config = { path: "/api3/*" };
export default handle(app);