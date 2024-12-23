
// @deno-types="https://esm.sh/v135/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.d.ts"
import { createDuckDB, getJsDelivrBundles, ConsoleLogger, DEFAULT_RUNTIME } from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/dist/duckdb-browser-blocking.mjs/+esm';

import optionsRollingSummary from "./../data/cboe-options-rolling.json" with {
    type: "json",
};

const logger = new ConsoleLogger();
const JSDELIVR_BUNDLES = getJsDelivrBundles();

const initialize = async ()=>{
    const { assetUrl, name } = optionsRollingSummary;
    // const ds = 'https://github.com/mnsrulz/mztrading-data/releases/download/archives/output_test_all.parquet';
    
    //HTTP paths are not supported due to xhr not available in deno.
    //db.registerFileURL('db.parquet', assetUrl, DuckDBDataProtocol.HTTP, false);
    
    console.log(`initializing duckdb with ${assetUrl} and name: ${name}`);
    const db = await createDuckDB(JSDELIVR_BUNDLES, logger, DEFAULT_RUNTIME);
    await db.instantiate(() => { });
    const arrayBuffer = await fetch(assetUrl)    //let's initialize the data set in memory
        .then(r => r.arrayBuffer());
    db.registerFileBuffer('db.parquet', new Uint8Array(arrayBuffer));
    return db;
}

const dbPromise = initialize();

export const getConnection = async ()=> {
    const dbPromiseVal = await dbPromise;
    return dbPromiseVal.connect();
}

//find a way to parametrize the query
export const getHistoricalSnapshotDatesFromParquet = async (symbol: string) => { 
    const conn = await getConnection();
    const arrowResult = await conn.send("SELECT DISTINCT CAST(dt as STRING) as dt FROM 'db.parquet' WHERE option_symbol = '" + symbol + "'");
    return arrowResult.readAll()[0].toArray().map((row) => row.toJSON());
}

export const getHistoricalOptionDataFromParquet = async (symbol: string, dt: string) => { 
    const conn = await getConnection();
    const arrowResult = await conn.send("SELECT cast(expiration as string) as expiration, delta, option_type, gamma, strike, open_interest, volume FROM 'db.parquet' WHERE option_symbol = '" + symbol + "' AND dt = '" + dt + "'");
    return arrowResult.readAll()[0].toArray().map((row) => row.toJSON());
}

export const lastHistoricalOptionDataFromParquet = ()=>{
    return optionsRollingSummary;
}