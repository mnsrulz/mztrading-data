import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";
import puppeteer, { Page } from "https://deno.land/x/puppeteer@16.2.0/mod.ts";
import pretry from "https://esm.sh/p-retry@6.2.1";
import pMap from "https://esm.sh/p-map@7.0.3";
import { chunk } from "jsr:@std/collections";

const maxBatches = 10;

import { getOptionsSnapshotSummary, ghRepoBaseUrl, cleanSymbol, getCboeLatestDateAndSymbols } from "../lib/data.ts";
const dataFolder = `temp/options-snapshots`;
await ensureDir(dataFolder);
const data = getOptionsSnapshotSummary();

const timeoutInMS = 3000;
const releaseName = Deno.env.get("RELEASE_NAME") ||
    `DEX_GEX_SNAPSHOT_${format(new Date(), "yyyy-MM-dd")}`;
const forceDayId = Deno.env.get("FORCE_DAY_ID")

forceDayId && console.log(`Force day id for this release: ${forceDayId}`);

console.log(`Generating options snapshot for release: ${releaseName}`);

data[releaseName] = {
    displayName: forceDayId || format(new Date(), "yyyy-MM-dd"),
    created: new Date(),
    zipAssetUrl: `${ghRepoBaseUrl}/${releaseName}.zip`,
    symbols: {},
};
const currentRelease = data[releaseName];

const latestDateAndSymbols = getCboeLatestDateAndSymbols(forceDayId);
let totalSymbols = 0;
let processingCounter = 0;
if (latestDateAndSymbols && latestDateAndSymbols.latestDate) {
    console.log(`Latest date: ${latestDateAndSymbols.latestDate}`);
    const allSymbols = latestDateAndSymbols.symbols;//.slice(0, 30); //for testing work only with 3 items
    console.log(`Found ${allSymbols.length} tickers...`);
    totalSymbols = allSymbols.length;

    const batches = chunk(allSymbols, maxBatches);
    await pMap(batches, processBatch, {
        concurrency: maxBatches
    });

    console.log(`Finished generating snapshot files!`);

    Deno.writeTextFileSync(
        "./data/options-snapshot.summary.json",
        JSON.stringify(data, null, 2),
    );

    console.log(`Summary file generated successfully!`);
} else {
    throw new Error(`Unable to find any latest date in the data summary file!`);    
}

async function processBatch(batchSymbols: string[]) {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.goto(
        `https://mztrading.netlify.app/tools/snapshot?dgextab=DEX&print=true&mode=HISTORICAL&historical=${encodeURIComponent(latestDateAndSymbols.latestDate)}`,
        {
            waitUntil: "networkidle2",
        },
    ); // replace


    for (const symbol of batchSymbols) {
        await pretry(async (n: number) => {
            if (n > 1) console.log(`Main retry attempt: ${n}`)
            await processSymbol(page, batchSymbols, symbol);
        }, {
            retries: 3
        })
    }
    await browser.close();
}

async function processSymbol(page: Page, allSymbols: string[], symbol: string) {
    const cleanedSymbol = cleanSymbol(symbol)
    
    console.log(`(${++processingCounter}/${totalSymbols}) Fetching dex/gex page for ${symbol}`);

    currentRelease.symbols[symbol] = {
        dex: {
            sdFileName: `${cleanedSymbol}_DEX_620.png`,
            hdFileName: `${cleanedSymbol}_DEX_1240.png`,            
        },
        gex: {
            sdFileName: `${cleanedSymbol}_GEX_620.png`,
            hdFileName: `${cleanedSymbol}_GEX_1240.png`           
        }
    }

    const currentSymbol = currentRelease.symbols[symbol];

    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    let varurlname = `urldex${allSymbols.indexOf(symbol)}${new Date().getTime()}`
    const scriptToRun = `
            let ${varurlname} = new URL(location.href);
            ${varurlname}.searchParams.set("dgextab", "DEX");
            ${varurlname}.searchParams.set("symbol", "${symbol}");
            history.replaceState(null, "", ${varurlname});
        `;
    // console.log(`Script: ${scriptToRun}`);
    await page.evaluate(scriptToRun);

    console.log(`Generating high definition DEX snapshot page for ${symbol}`);

    await page.waitForNetworkIdle({
        timeout: timeoutInMS
    });
    await captureScreenshot(page, `${dataFolder}/${currentSymbol.dex.hdFileName}`); // take a screenshot and save it to a file

    console.log(`Generating standard definition DEX snapshot page for ${symbol}`);
    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 1 }); // set the viewport size

    await captureScreenshot(page, `${dataFolder}/${currentSymbol.dex.sdFileName}`); // take a screenshot and save it to a file

    varurlname = `urlgex${allSymbols.indexOf(symbol)}${new Date().getTime()}`
    const scriptToRunGex = `
        let ${varurlname} = new URL(location.href);
        ${varurlname}.searchParams.set("dgextab", "GEX");
        history.replaceState(null, "", ${varurlname});
        `;
    await page.evaluate(scriptToRunGex);
    console.log(`Generating standard definition GEX snapshot page for ${symbol}`);

    await captureScreenshot(page, `${dataFolder}/${currentSymbol.gex.sdFileName}`); // take a screenshot and save it to a file

    await page.setViewport({ width: 620, height: 620, deviceScaleFactor: 2 }); // set the viewport size
    console.log(`Generating high definition GEX snapshot page for ${symbol}`);

    await captureScreenshot(page, `${dataFolder}/${currentSymbol.gex.hdFileName}`); // take a screenshot and save it to a file      
}

async function captureScreenshot(page: Page, path: string) {
    await pretry(async (n: number) => {
        if (n > 1) console.log(`Retry attempt: ${n}`)
        await page.waitForNetworkIdle({
            timeout: timeoutInMS
        });
        await page.screenshot({
            path: path
        }); // take a screenshot and save it to a file
    }, {
        retries: 3
    })
}