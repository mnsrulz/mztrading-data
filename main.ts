import {
    Application,
    isHttpError,
    Router,
} from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { sortBy } from "https://deno.land/std@0.224.0/collections/sort_by.ts";
import { getQuery } from "https://deno.land/x/oak@v12.6.1/helpers.ts";
import ky from "https://esm.sh/ky@1.2.3";
import { stringify } from "jsr:@std/csv";
import {
    AvailableSnapshotDates,
    CboeOptionsRawSummary,
    getOptionsDataSummary,
    mapDataToLegacy,
    OptionsSnapshotSummary,
    OptionsSnapshotSummaryLegacy,
    searchTicker,
    getSnapshotsAvailableForDate,
    getSnapshotsAvailableForSymbol
} from "./lib/data.ts";

import { getPriceAtDate } from './lib/historicalPrice.ts'
import { calculateExpsoure, ExposureDataRequest, getExposureData, getHistoricalGreeksSummaryDataFromParquet, getHistoricalOptionDataFromParquet, getHistoricalSnapshotDatesFromParquet, lastHistoricalOptionDataFromParquet, getLiveCboeOptionsPricingData, getHistoricalSnapshotDates, getHistoricalGreeksSummaryDataBySymbolFromParquet } from "./lib/historicalOptions.ts";
import { getOptionsAnalytics, getOptionsChain } from "./lib/cboe.ts";
import { getIndicatorValues } from "./lib/ta.ts";

const token = Deno.env.get("ghtoken") || '';
const router = new Router();

const getHistoricalOptionsData = async (s: string, dt: string) => {
    const memData = getOptionsDataSummary();
    const assetUrl = Object.values(memData).find((j) => j.displayName == dt)
        ?.symbols[s.toUpperCase()].assetUrl;
    if (assetUrl) {
        return await ky(assetUrl).json();
    }

    const data = await ky(
        `https://raw.githubusercontent.com/mnsrulz/mytradingview-data/main/data/dt=${dt}/symbol=${s.toUpperCase()}/data.json`,
        {
            headers: {
                Authorization: `Bearer ${token}`,
            },
        },
    ).json();
    return data;
};

router.get("/", (context) => {
    context.response.body = "hello";
})
    .get("/releases", async (context) => {
        const newReleases = Object.values(OptionsSnapshotSummary).map((j) => ({
            name: j.displayName,
        })).reverse();
        const releases = await ky(
            `https://api.github.com/repos/mnsrulz/mytradingview-data/tags`,
            {
                headers: {
                    Authorization: `Bearer ${token}`,
                },
            },
        ).json<{ name: string }[]>();

        const finalResponse = [...newReleases, ...releases].map((j) => ({
            name: j.name,
        }));
        context.response.body = finalResponse;
        context.response.type = "application/json";
    })
    .get("/beta/optionsdatasummary", (context) => {
        const data = getOptionsDataSummary();
        context.response.body = Object.keys(data).map((j) => ({
            name: j,
            displayName: data[j].displayName,
        }));
    })
    .get("/beta/optionsdata", async (context) => {
        const { s, r } = getQuery(context);
        const data = getOptionsDataSummary();
        const { assetUrl } = data[r].symbols[s];
        console.log(`making http call to access: ${assetUrl}`);
        const assetData = await ky(assetUrl).json<{}>();
        context.response.body = assetData;
        context.response.type = "application/json";
    })
    .get("/summary", async (context) => {
        const { s } = getQuery(context);
        const data = await ky(
            `https://raw.githubusercontent.com/mnsrulz/mytradingview-data/main/summary/data.json`,
            {
                headers: {
                    Authorization: `Bearer ${token}`,
                },
            },
        ).json<{ symbol: string; dt: string }[]>();
        const newReleaseData = mapDataToLegacy();

        const mergedDataset = [...data, ...newReleaseData];

        const filteredData = s
            ? mergedDataset.filter((j) =>
                j.symbol.toUpperCase() == s.toUpperCase()
            )
            : mergedDataset;

        const sortedByDates = sortBy(filteredData, (it) => it.dt, {
            order: "desc",
        });

        context.response.body = sortedByDates;
        context.response.type = "application/json";
    })
    .get("/data", async (context) => {
        const { dt, s } = getQuery(context);
        if (!dt || !s) {
            throw new Error(
                `empty query provided. Use with ?dt=YOUR_QUERY&s=aapl`,
            );
        }
        // const cu = await yf.historical(s, {
        //     period1: dayjs(dt.substr(0, 10)).toDate(),
        //     interval: '1d',
        //     period2: dayjs(dt.substr(0, 10)).add(1, 'days').toDate()
        // })
        // const currentPrice = cu.at(0)?.open;

        const data = await getHistoricalOptionsData(s, dt);
        const priceAtDate = await getPriceAtDate(s, dt)
        context.response.body = { data, price: priceAtDate };
        context.response.type = "application/json";
    })
    .get("/symbols", (context) => {
        //api/symbols/search?q=t
        const { q } = getQuery(context);
        const items = searchTicker(q);
        context.response.body = items;
    })
    
    .get("/symbols/:symbol/historical/snapshots", (context) => {
        const { symbol } = context.params;
        const result = Object.keys(OptionsSnapshotSummaryLegacy)
            .filter((j) =>
                Object.keys(OptionsSnapshotSummaryLegacy[j].symbols).includes(
                    symbol,
                )
            )
            .map((k) => ({ date: k, data: OptionsSnapshotSummaryLegacy[k].symbols[symbol] }))
            .map(({ data, date }) => ({
                date: date,
                dex: {
                    hdAssetUrl: data.dex.hdAssetUrl,
                    sdAssetUrl: data.dex.sdAssetUrl
                },
                gex: {
                    hdAssetUrl: data.gex.hdAssetUrl,
                    sdAssetUrl: data.gex.sdAssetUrl
                },
            }));
        context.response.body = { items: result };
        context.response.type = "application/json";
    })
    .get("/beta/historical/cboesummary", (context) => {        //make the resource name more appropriate
        context.response.body = CboeOptionsRawSummary;
        context.response.type = "application/json";
    })
    .get("/beta/symbols/:symbol/historical/snapshots", async (context) => {        //make the resource name more appropriate
        const { symbol } = context.params;
        context.response.body = await getHistoricalSnapshotDatesFromParquet(symbol);
        context.response.type = "application/json";
    })
    .get("/beta/symbols/:symbol/historical/snapshots/:dt", async (context) => {        //make the resource name more appropriate
        const { symbol, dt } = context.params;
        context.response.body = await getHistoricalOptionDataFromParquet(symbol, dt);
        context.response.type = "application/json";
    })
    .get("/beta/misc/last30rolling.parquet", (context) => {        //make the resource name more appropriate
        const { assetUrl } = lastHistoricalOptionDataFromParquet();
        context.response.redirect(assetUrl);
    })
    .get("/beta/symbols/:symbol/cboeoptionchain", async (context) => {        //make the resource name more appropriate
        const { symbol } = context.params;
        const data = await getOptionsChain(symbol);
        context.response.body = data;
        context.response.type = "application/json";
    })
    .get("/beta/cboeoptionchainanalytics", async (context) => {        //make the resource name more appropriate
        const data = await getOptionsAnalytics();
        context.response.body = data;
        context.response.type = "application/json";
    })
    .get("/beta/symbols/:symbol/historical/snapshots/:dt/exposure", async (context) => {
        const { symbol, dt } = context.params;
        context.response.body = await getExposureData(symbol, dt);
        context.response.type = "application/json";
    })
    .get("/beta/symbols/:symbol/exposure", async (context) => {             //remove it
        const { symbol } = context.params;
        context.response.body = await getExposureData(symbol, 'LIVE');
        context.response.type = "application/json";
    })
    .get("/beta/symbols/:symbol/optionspricing", async (context) => {   //remove it
        const { symbol } = context.params;
        context.response.body = await getLiveCboeOptionsPricingData(symbol);
        context.response.type = "application/json";
    })
    .post("/beta/tools/exposure", async (context) => {                   //remove it
        if (!context.request.hasBody) {
            context.throw(415);
        }
        const { data, spotPrice, spotDate } = await context.request.body().value as ExposureDataRequest;
        context.response.body = calculateExpsoure(spotPrice, data, spotDate);
        context.response.type = "application/json";
    })
    .get("/api/symbols", (context) => {
        //api/symbols/search?q=t
        const { q } = getQuery(context);
        const items = searchTicker(q);
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
        context.response.body = calculateExpsoure(spotPrice, data, spotDate);
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
    .get("/api/options/report/greeks.txt", async (context) => {
        const { dt, dte } = getQuery(context);        
        const result = await getHistoricalGreeksSummaryDataFromParquet(dt, dte);
        if(result.length == 0) throw new Error("No data found for the given date range");
        context.response.body = stringify(result, {columns: Object.keys(result.at(0) || {})});
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
    });

const app = new Application();

app.use(async (context, next) => {
    try {
        context.response.headers.set("Access-Control-Allow-Origin", "*");
        await next();
    } catch (err) {
        if (isHttpError(err)) {
            context.response.status = err.status;
        } else {
            context.response.status = 500;
        }
        context.response.body = { error: err.message };
        context.response.type = "json";
    }
});

app.use(router.routes());
app.use(router.allowedMethods());
await app.listen({ port: 8000 });
