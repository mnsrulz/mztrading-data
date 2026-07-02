import { allTickerSymbols } from "../../lib/data.ts"
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";

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