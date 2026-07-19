import { Hono } from "https://esm.sh/hono@4.12";
import { HTTPException } from "https://esm.sh/hono@4.12/http-exception";
import { handle } from "https://esm.sh/hono@4.12/netlify";
import { sortBy } from "https://deno.land/std@0.224.0/collections/sort_by.ts";
import { stringify } from "jsr:@std/csv@1.0.6";

import {
    AvailableSnapshotDates,
    searchTicker,
    getSnapshotsAvailableForDate,
    getSnapshotsAvailableForSymbol,
    allTickerSymbols,
} from "../lib/data.ts";
import {
    calculateExpsoure,
    type ExposureDataRequest,
    getExposureData,
    getHistoricalGreeksSummaryDataFromParquet,
    getHistoricalExposureWallsFromParquet,
    getHistoricalSnapshotDatesFromParquet,
    getLiveCboeOptionsPricingData,
    getHistoricalSnapshotDates,
    getHistoricalGreeksSummaryDataBySymbolFromParquet,
    getHistoricalGreeksAvailableExpirationsBySymbolFromParquet,
    getOIAnomalyDataFromParquet,
    getHistoricalDataForOptionContractFromParquet,
    getHistoricalOIDataBySymbolFromParquet,
    getSymbolExpirations
} from "../lib/historicalOptions.ts";
import { getIndicatorValues } from "../lib/ta.ts";
import {
    type OIAnomalyFacetSearchRequestType,
    type OIAnomalySearchRequest,
    queryOIAnomalyFacetSearch,
    queryOIAnomalySearch,
} from "../lib/oianomalySearchClient.ts";

const app = new Hono();

app.use("*", async (c, next) => {
    const start = performance.now();
    const req = c.req.raw;
    try {
        c.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        c.header("Access-Control-Allow-Origin", "*");
        c.header("Access-Control-Max-Age", "86400");
        c.header("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        await next();
    } finally {
        const end = performance.now();
        console.log(
            `${req.method} ${new URL(req.url).pathname} ${req.headers.get("x-forwarded-for") || req.headers.get("x-real-ip") || ""} ${(end - start).toFixed(2)} ms`,
        );
    }
});

app.onError((err, c) => {
    const status = err instanceof HTTPException ? err.status : 500;
    const message = err instanceof Error ? err.message : "Internal Server Error";
    console.error("Global error handler caught:", err);
    return c.json({ error: message }, status);
});

app.options("*", (c) => {
    c.status(204);
    return c.body(null);
});

app.get("/", (c) => c.text("hello"));

app.get("/symbols", (c) => {
    const q = c.req.query("q") ?? "";
    return c.json(searchTicker(q));
});

app.get("/api/symbols", (c) => {
    const q = c.req.query("q") ?? "";
    return c.json(searchTicker(q));
});

app.get("/api/symbols/all", (c) => c.json(allTickerSymbols));

app.get("/api/stocks/:symbol/indicators", async (c) => {
    const symbol = c.req.param("symbol");
    const q = c.req.query("q") ?? "";
    const indicators = q.split(",").map((item) => item.trim()).filter(Boolean);
    const data = await getIndicatorValues(symbol, indicators);
    return c.json(data);
});

app.post("/api/options/exposure/calculate", async (c) => {
    const body = await c.req.json<ExposureDataRequest>().catch(() => null);
    if (!body) {
        throw new HTTPException(415, { message: "Request body is missing" });
    }

    return c.json(calculateExpsoure(body.spotPrice, body.data, body.spotDate, new Date()));
});

app.get("/api/options/exposures/dates", async (c) => {
    const data = await getHistoricalSnapshotDates();
    return c.json(data);
});

app.get("/api/options/exposures/snapshot-dates", (c) => c.json(AvailableSnapshotDates));

app.get("/api/options/exposures/snapshots", (c) => {
    const dt = c.req.query("dt");
    if (!dt) {
        throw new HTTPException(400, { message: "dt parameter is missing!" });
    }

    const results = getSnapshotsAvailableForDate(dt);
    return c.json(sortBy(results, (it) => it.symbol));
});

app.get("/api/options/report/greeks", async (c) => {
    const dt = c.req.query("dt");
    const dte = c.req.query("dte");
    if (!dt) {
        throw new HTTPException(400, { message: "dt parameter is missing!" });
    }

    const data = await getHistoricalGreeksSummaryDataFromParquet(dt, Number(dte));
    return c.json(data);
});

app.get("/api/options/report/exposure-walls", async (c) => {
    const dt = c.req.query("dt");
    const dte = c.req.query("dte");
    const symbol = c.req.query("symbol");
    const data = await getHistoricalExposureWallsFromParquet(dt, Number(dte), symbol);
    return c.json(data);
});

app.get("/api/options/report/oi-anomaly", async (c) => {
    const dt = c.req.query("dt") ?? "";
    const dteFrom = c.req.query("dteFrom") ?? "";
    const dteTo = c.req.query("dteTo") ?? "";
    const symbols = c.req.query("symbols") ?? "";

    const symbolList = symbols.split(",").map((k) => k.trim()).filter(Boolean);
    const dtList = dt.split(",").map((k) => k.trim()).filter(Boolean);

    const data = await getOIAnomalyDataFromParquet(dtList, Number(dteFrom), Number(dteTo), symbolList);
    return c.json(data);
});

app.post("/api/search/oi-anomaly", async (c) => {
    const searchRequest = await c.req.json<OIAnomalySearchRequest[]>().catch(() => null);
    if (!searchRequest || searchRequest.length === 0) {
        throw new HTTPException(400, { message: "Search request is empty!" });
    }

    const data = await queryOIAnomalySearch(searchRequest);
    return c.json(data);
});

app.post("/api/search/oi-anomaly/facet", async (c) => {
    const searchRequest = await c.req.json<OIAnomalyFacetSearchRequestType>().catch(() => null);
    if (!searchRequest) {
        throw new HTTPException(400, { message: "Facet search request is empty!" });
    }

    const data = await queryOIAnomalyFacetSearch(searchRequest);
    return c.json(data);
});

app.get("/api/options/report/greeks.txt", async (c) => {
    const dt = c.req.query("dt");
    const dte = c.req.query("dte");
    const result = await getHistoricalGreeksSummaryDataFromParquet(dt, Number(dte));
    if (result.length === 0) {
        throw new HTTPException(404, { message: "No data found for the given date range" });
    }

    return c.text(stringify(result, { columns: Object.keys(result.at(0) || {}) }));
});

app.get("/api/options/:symbol/exposure", async (c) => {
    const symbol = c.req.param("symbol");
    const data = await getExposureData(symbol, "LIVE");
    return c.json(data);
});

app.get("/api/options/:symbol/exposure/historical-dates", async (c) => {
    const symbol = c.req.param("symbol");
    const dates = await getHistoricalSnapshotDatesFromParquet(symbol);
    return c.json(dates);
});

app.get("/api/options/:symbol/exposure/historical", async (c) => {
    const symbol = c.req.param("symbol");
    const dt = c.req.query("dt");
    if (!dt) {
        throw new HTTPException(400, { message: "dt parameter is missing!" });
    }

    const data = await getExposureData(symbol, dt);
    return c.json(data);
});

app.get("/api/options/:symbol/pricing", async (c) => {
    const symbol = c.req.param("symbol");
    const data = await getLiveCboeOptionsPricingData(symbol);
    return c.json(data);
});

app.get("/api/options/:symbol/expirations", async (c) => {
    const symbol = c.req.param("symbol");
    const data = await getSymbolExpirations(symbol);
    return c.json(data);
});

app.get("/api/options/:symbol/exposures/snapshots", (c) => {
    const symbol = c.req.param("symbol");
    const results = getSnapshotsAvailableForSymbol(symbol);
    return c.json(sortBy(results, (it) => it.date, { order: "desc" }));
});

app.get("/api/options/:symbol/report/greeks", async (c) => {
    const symbol = c.req.param("symbol");
    const data = await getHistoricalGreeksSummaryDataBySymbolFromParquet(symbol);
    return c.json(data);
});

app.get("/api/options/:symbol/report/oi", async (c) => {
    const symbol = c.req.param("symbol");
    const expirationDates = c.req.query("expirationDates") ?? "";
    const expirations = expirationDates.split(",").map((k) => k.trim()).filter(Boolean);
    const data = await getHistoricalOIDataBySymbolFromParquet(symbol, expirations);
    return c.json(data);
});

app.get("/api/options/contracts/:contractId/historical-data", async (c) => {
    const contractId = c.req.param("contractId");
    const data = await getHistoricalDataForOptionContractFromParquet(contractId);
    return c.json(data);
});

app.get("/api/options/:symbol/report/greeks/expirations", async (c) => {
    const symbol = c.req.param("symbol");
    const data = await getHistoricalGreeksAvailableExpirationsBySymbolFromParquet(symbol);
    return c.json(data);
});

export default handle(app);

export {
    app
};