import { allTickerSymbols } from "../../lib/data.ts"
import { Hono } from "https://esm.sh/hono@4.12";
import { handle } from "https://esm.sh/hono@4.12/netlify";

import optionsData from "./../../public/options_data.json" with {
    type: "json",
};

const app = new Hono();
app.get("/api3/hello", (c) => {
    return c.json({
        symbols: allTickerSymbols,
        l: optionsData.filename
    });
});

export const config = { path: "/api3/*" };
export default handle(app);