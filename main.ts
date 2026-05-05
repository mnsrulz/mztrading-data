import {
    Application,
    isHttpError,
    Router,
} from "https://deno.land/x/oak@v12.6.1/mod.ts";
import pino from "https://esm.sh/pino@10.1.0";
import pretty from "https://esm.sh/pino-pretty@10.3.0";

import { sortBy } from "https://deno.land/std@0.224.0/collections/sort_by.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
// import ky from "https://esm.sh/ky@1.8.2";
import { stringify } from "jsr:@std/csv@1.0.6";
import {
    AvailableSnapshotDates,
    // CboeOptionsRawSummary,
    // getOptionsDataSummary,
    // mapDataToLegacy,
    // OptionsSnapshotSummary,
    //OptionsSnapshotSummaryLegacy,
    searchTicker,
    getSnapshotsAvailableForDate,
    getSnapshotsAvailableForSymbol,
    getSymbolExpirations,
    allTickerSymbols
} from "./lib/data.ts";
const stream = pretty({
  singleLine: true,
  colorize: true,
  include: "msg",
  messageFormat: (log, messageKey) => { return `${log[messageKey]}` },
});

const logger = pino({
  //level: "info" 
}, stream);

// import { getPriceAtDate } from './lib/historicalPrice.ts'
import {
    calculateExpsoure, ExposureDataRequest, getExposureData, getHistoricalGreeksSummaryDataFromParquet,
    getHistoricalExposureWallsFromParquet,
    // getHistoricalOptionDataFromParquet, 
    getHistoricalSnapshotDatesFromParquet,
    // lastHistoricalOptionDataFromParquet, 
    getLiveCboeOptionsPricingData, getHistoricalSnapshotDates, getHistoricalGreeksSummaryDataBySymbolFromParquet, getHistoricalGreeksAvailableExpirationsBySymbolFromParquet,
    getOIAnomalyDataFromParquet, getHistoricalDataForOptionContractFromParquet,
    getHistoricalOIDataBySymbolFromParquet,
} from "./lib/historicalOptions.ts";
// import { getOptionsAnalytics, getOptionsChain } from "./lib/cboe.ts";
import { getIndicatorValues } from "./lib/ta.ts";
import { OIAnomalyFacetSearchRequestType, OIAnomalySearchRequest, queryOIAnomalyFacetSearch, queryOIAnomalySearch } from "./lib/oianomalySearchClient.ts";
import { error } from "node:console";

// const token = Deno.env.get("ghtoken") || '';
const router = new Router();

router.get("/", (context) => {
    context.response.body = "hello";
})    
    .get("/symbols", (context) => {
        //api/symbols/search?q=t
        const { q } = getQuery(context);
        const items = searchTicker(q);
        context.response.body = items;
    })

    // .get("/symbols/:symbol/historical/snapshots", (context) => {
    //     const { symbol } = context.params;
    //     const result = Object.keys(OptionsSnapshotSummaryLegacy)
    //         .filter((j) =>
    //             Object.keys(OptionsSnapshotSummaryLegacy[j].symbols).includes(
    //                 symbol,
    //             )
    //         )
    //         .map((k) => ({ date: k, data: OptionsSnapshotSummaryLegacy[k].symbols[symbol] }))
    //         .map(({ data, date }) => ({
    //             date: date,
    //             dex: {
    //                 hdAssetUrl: data.dex.hdAssetUrl,
    //                 sdAssetUrl: data.dex.sdAssetUrl
    //             },
    //             gex: {
    //                 hdAssetUrl: data.gex.hdAssetUrl,
    //                 sdAssetUrl: data.gex.sdAssetUrl
    //             },
    //         }));
    //     context.response.body = { items: result };
    //     context.response.type = "application/json";
    // })    
    .get("/api/symbols", (context) => {
        //api/symbols/search?q=t
        const { q } = getQuery(context);
        const items = searchTicker(q);
        context.response.body = items;
    })
    .get("/api/symbols/all", (context) => {
        //api/symbols/all
        const items = allTickerSymbols;
        context.response.body = items;
    })
    .get("/api/stocks/:symbol/indicators", async (context) => {
        const { symbol } = context.params;
        const { q } = getQuery(context);
        const indicators = q.split(',');

        context.response.body = await getIndicatorValues(symbol, indicators)
        context.response.type = "application/json";
    })
    .post("/api/options/exposure/calculate", async (context) => {
        if (!context.request.hasBody) {
            context.throw(415);
        }
        const { data, spotPrice, spotDate } = await context.request.body().value as ExposureDataRequest;
        context.response.body = calculateExpsoure(spotPrice, data, spotDate, new Date());
        context.response.type = "application/json";
    })
    .get("/api/options/exposures/dates", async (context) => {
        context.response.body = await getHistoricalSnapshotDates();
        context.response.type = "application/json";
    })
    .get("/api/options/exposures/snapshot-dates", (context) => {
        context.response.body = AvailableSnapshotDates;
        context.response.type = "application/json";
    })
    .get("/api/options/exposures/snapshots", (context) => {
        const { dt } = getQuery(context);
        if (!dt) throw new Error("dt parameter is missing!");
        const results = getSnapshotsAvailableForDate(dt);
        context.response.body = sortBy(results, (it) => it.symbol);
        context.response.type = "application/json";
    })
    .get("/api/options/report/greeks", async (context) => {
        const { dt, dte } = getQuery(context);
        if (!dt) throw new Error("dt parameter is missing!");
        context.response.body = await getHistoricalGreeksSummaryDataFromParquet(dt, dte);
        context.response.type = "application/json";
    })
    .get("/api/options/report/exposure-walls", async (context) => {
        const { dt, dte, symbol } = getQuery(context);
        // if (!dt) throw new Error("dt parameter is missing!");
        context.response.body = await getHistoricalExposureWallsFromParquet(dt, dte, symbol);
        context.response.type = "application/json";
    })
    .get("/api/options/report/oi-anomaly", async (context) => {
        const { dt, dteFrom, dteTo, symbols } = getQuery(context);
        const symbolList = (symbols || '').split(',').map(k => k.trim()).filter(k => k);
        const dtList = (dt || '').split(',').map(k => k.trim()).filter(k => k);
        context.response.body = await getOIAnomalyDataFromParquet(dtList, dteFrom, dteTo, symbolList);
        context.response.type = "application/json";
    })
    .post("/api/search/oi-anomaly", async (context) => {
        if (!context.request.hasBody) {
            context.throw(415);
        }
        try {
            const searchRequest = await context.request.body().value as OIAnomalySearchRequest[];
            if (!searchRequest || searchRequest.length == 0) {
                throw new Error("Search request is empty!");
            }
            context.response.body = await queryOIAnomalySearch(searchRequest);
        } catch (error) {
            console.error(error);
            context.response.body = error;
            context.response.status = 500;
        }
        context.response.type = "application/json";
    })
    .post("/api/search/oi-anomaly/facet", async (context) => {
        if (!context.request.hasBody) {
            context.throw(415);
        }
        try {
            const searchRequest = await context.request.body().value as OIAnomalyFacetSearchRequestType;
            if (!searchRequest) {
                throw new Error("Facet search request is empty!");
            }
            context.response.body = await queryOIAnomalyFacetSearch(searchRequest);
        } catch (error) {
            console.error(error);
            context.response.body = error;
            context.response.status = 500;
        }
        context.response.type = "application/json";
    })
    .get("/api/options/report/greeks.txt", async (context) => {
        const { dt, dte } = getQuery(context);
        const result = await getHistoricalGreeksSummaryDataFromParquet(dt, dte);
        if (result.length == 0) throw new Error("No data found for the given date range");
        context.response.body = stringify(result, { columns: Object.keys(result.at(0) || {}) });
        context.response.type = "text/plain";
    })
    .get("/api/options/:symbol/exposure", async (context) => {
        const { symbol } = context.params;
        context.response.body = await getExposureData(symbol, 'LIVE');
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/exposure/historical-dates", async (context) => {
        const { symbol } = context.params;
        context.response.body = await getHistoricalSnapshotDatesFromParquet(symbol);
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/exposure/historical", async (context) => {
        const { symbol } = context.params;
        const { dt } = getQuery(context);
        if (!dt) throw new Error("dt parameter is missing!");
        context.response.body = await getExposureData(symbol, dt);
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/pricing", async (context) => {
        const { symbol } = context.params;
        context.response.body = await getLiveCboeOptionsPricingData(symbol);
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/expirations", async (context) => {
        const { symbol } = context.params;
        context.response.body = getSymbolExpirations(symbol);
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/exposures/snapshots", (context) => {
        const { symbol } = context.params;
        const results = getSnapshotsAvailableForSymbol(symbol);
        context.response.body = sortBy(results, (it) => it.date, { order: "desc" });
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/report/greeks", async (context) => {
        const { symbol } = context.params;
        context.response.body = await getHistoricalGreeksSummaryDataBySymbolFromParquet(symbol);
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/report/oi", async (context) => {
        const { symbol } = context.params;
        const { expirationDates } = getQuery(context);
        const expirations = expirationDates ? expirationDates.split(',').map(k => k.trim()).filter(k => k) : [];
        context.response.body = await getHistoricalOIDataBySymbolFromParquet(symbol, expirations);
        context.response.type = "application/json";
    })
    .get("/options/contracts/:contractId/historical-data", async (context) => {
        const { contractId } = context.params;
        context.response.body = await getHistoricalDataForOptionContractFromParquet(contractId);
        context.response.type = "application/json";
    })
    .get("/api/options/:symbol/report/greeks/expirations", async (context) => {
        const { symbol } = context.params;
        context.response.body = await getHistoricalGreeksAvailableExpirationsBySymbolFromParquet(symbol);
        context.response.type = "application/json";
    });

const app = new Application();

app.use(async (context, next) => {
    const start = performance.now();
    const req = context.request;
    let hasError = false;
    try {
        context.response.headers.set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        context.response.headers.set("Access-Control-Allow-Origin", "*");
        context.response.headers.set("Access-Control-Max-Age", "86400");
        context.response.headers.set("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
        await next();
    } catch (err) {
        if (isHttpError(err)) {
            context.response.status = err.status;
        } else {
            context.response.status = 500;
        }
        context.response.body = { error: err.message };
        context.response.type = "json";
        logger.error(`Error occurred: ${err.message}`);
        hasError = true;
    } finally {
        const end = performance.now();
        logger.info(`${ hasError ? 'ERR':'OK' } ${req.method} ${req.url.pathname} ${req.headers.get('X-Forwarded-For') || req.headers.get('x-real-ip') || ''} ${(end - start).toFixed(2)} ms`)
    }
});

app.use(router.routes());
app.use(router.allowedMethods());
await app.listen({ port: 8000 });
