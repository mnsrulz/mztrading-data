import { z } from "https://esm.sh/zod@4.3.6";
import pino from "https://esm.sh/pino@10.1.0";
import pretty from "https://esm.sh/pino-pretty@10.3.0";
import prettyBytes from "https://esm.sh/pretty-bytes@7.1.0";
import PusherJS from 'https://esm.sh/pusher-js@8.4.0';
import Pusher from 'https://esm.sh/pusher@5.3.2';
import lz from "https://esm.sh/lz-string@1.5.0";
import pako from 'https://esm.sh/pako@2.1.0';

import { createClient } from "npm:redis@^4.5";
import { DuckDBInstance } from "npm:@duckdb/node-api@1.4.3-r.2";
import { Buffer } from "node:buffer";

const pusher = Pusher.forURL(Deno.env.get('PUSHER_URI') || '');
const channelName = Deno.env.get('PUSHER_CHANNEL_NAME') || 'mztrading-channel';
const pusherClient = new PusherJS(Deno.env.get('PUSHER_APP_KEY') || '', {
    cluster: Deno.env.get('PUSHER_APP_CLUSTER') || 'us2',
});

const channel = pusherClient.subscribe(channelName);
const DATA_DIR = Deno.env.get("DATA_DIR") || 'temp/w2-output';
const OHLC_DATA_DIR = Deno.env.get("OHLC_DIR") || 'temp/ohlc';
const LOG_LEVEL = Deno.env.get("LOG_LEVEL") || 'info';

const REDIS_URI = Deno.env.get('REDIS_URI');

const redisClient = createClient({
    url: REDIS_URI,
});

try {
    await redisClient.connect();
    console.log("Connected to Redis");    
} catch (error) {
    console.error("Failed to connect to Redis:", error);
    Deno.exit(1);
}

const stream = pretty({
    singleLine: true,
    colorize: true,
    include: "time,msg",
    messageFormat: (log, messageKey) => { return `${log[messageKey]}` },
});

const logger = pino({
    level: LOG_LEVEL
}, stream);

const WorkerRequestSchema = z.object({
    requestType: z.enum(["volatility-query", "options-stat-query"]),
    requestId: z.uuid(),
    data: z.any()
});

const OptionsStatsSchema = z.object({
    symbol: z.string()
        .nonempty()
        .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),
    lookbackDays: z.number().int().positive(),
    requestId: z.uuid()
});

const BaseSchema = {
    symbol: z.string()
        .nonempty()
        .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),

    lookbackDays: z.number().int().positive(),

    requestId: z.uuid(),
};

const FixedExpirySchema = z.object({
    expiryMode: z.literal("fixed").optional(),
    expiration: z.string()
        .regex(/^\d{4}-\d{2}-\d{2}$/, "Expiration must be YYYY-MM-DD"),
    dte: z.undefined().optional()
});

const RollingExpirySchema = z.object({
    expiryMode: z.literal("rolling"),
    dte: z.number().int().positive(),
    expiration: z.undefined().optional()
});

const ExpirySchema = z.discriminatedUnion("expiryMode", [
    FixedExpirySchema,
    RollingExpirySchema
]);

// delta mode
const DeltaModeSchema = z.object({
    ...BaseSchema,
    mode: z.literal("delta"),
    delta: z.number().max(100).nonnegative(),
    strike: z.null().optional(),
});

// strike mode
const StrikeModeSchema = z.object({
    ...BaseSchema,
    mode: z.literal("strike"),
    strike: z.number().positive(),
    delta: z.null().optional(),
});

// atm mode
const AtmModeSchema = z.object({
    ...BaseSchema,
    mode: z.literal("atm"),
    strike: z.null().optional(),
    delta: z.null().optional(),
});

export const OptionsVolRequestSchema = z.intersection(
    ExpirySchema, 
    z.discriminatedUnion("mode", [
        DeltaModeSchema,
        StrikeModeSchema,
        AtmModeSchema
    ])
);

type OptionsVolRequest = z.infer<typeof OptionsVolRequestSchema>;
type OptionsStatsRequest = z.infer<typeof OptionsStatsSchema>;

/*
 {"symbol":"AAPL","lookbackDays":180,"delta":25,"expiration":"2025-11-07","mode":"delta","requestId":"977c5c7b-7e96-45d1-991b-db70992d0846"}       
*/
const handleVolatilityMessage = async (args: OptionsVolRequest) => {
    try {
        const { symbol, lookbackDays, delta, expiration, mode, strike, requestId, dte, expiryMode } = OptionsVolRequestSchema.parse(args);

        logger.info(`Worker volatility request received: ${JSON.stringify(args)}`);
        using stack = new DisposableStack();
        const instance = await DuckDBInstance.create(":memory:");
        stack.defer(() => instance.closeSync());
        const connection = await instance.connect();
        stack.defer(() => connection.closeSync());
        const strikeFilter = mode == 'strike' ? ` AND strike = ${strike}` : '';
        let partitionOrderColumn = mode == 'atm' ? 'price_strike_diff' : 'delta_diff';
        let rows = [];
        let hasError = false;
        const useRollingExpiry = expiryMode === 'rolling';
        if (useRollingExpiry) {
            partitionOrderColumn = `dte ASC, ${partitionOrderColumn}`
        }
        try {

            const queryToExecute = `
            SELECT to_json(t)    
            FROM (
                WITH OHLC AS (
                  SELECT DISTINCT dt, iv30/100  AS iv30, if(close > 0, close, LAG(close) OVER (PARTITION BY symbol ORDER BY dt)) AS close,
                  PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY iv30) AS iv_percentile
                  FROM '${OHLC_DATA_DIR}/*.parquet' WHERE replace(symbol, '^', '') = '${symbol}'
                ), I AS (
                    SELECT DISTINCT opdata.dt, iv, option_type, option_symbol, expiration, strike, (bid + ask)/2 AS  mid_price, 
                    OHLC.close, OHLC.iv30, OHLC.iv_percentile,
                    abs(delta) AS abs_delta,
                    abs(strike - OHLC.close) AS price_strike_diff,
                    abs(abs(delta) - ${(delta || 0) / 100}) AS delta_diff,
                    DATE_DIFF('day', opdata.dt, expiration) AS dte
                    --FROM '${DATA_DIR}/${symbol}_*.parquet' opdata
                    FROM '${DATA_DIR}/symbol=${symbol}/*.parquet' opdata
                    JOIN OHLC ON OHLC.dt = opdata.dt
                    WHERE 1 = 1 
                        AND (open_interest > 0 OR bid > 0 OR ask > 0 OR iv > 0)           --JUST TO MAKE SURE NEW CONTRACTS WON'T APPEAR IN THE DATASET WHICH LIKELY REPRESENTED BY 0 OI, bid/ask/iv
                        AND OHLC.dt >= current_date - ${lookbackDays} ${strikeFilter} 
                ), J AS (
                    SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY dt, option_type ORDER BY dte ASC) AS dte_rn
                    FROM I
                    WHERE ${useRollingExpiry ? `dte >= ${dte}` : `expiration = '${expiration}'`}
                ), M AS (
                    SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY dt, option_type ORDER BY ${partitionOrderColumn} ASC) AS rn
                    FROM I
                    WHERE ${useRollingExpiry ? `dte >= ${dte}` : `expiration = '${expiration}'`}
                    --${useRollingExpiry ? 'WHERE dte_rn = 1' : ''}
                )
                SELECT 
                    array_agg(DISTINCT dt ORDER BY dt) AS dt,
                    --array_agg(expiration ORDER BY dt) FILTER (WHERE option_type='C') AS expiration,
                    array_agg(CAST(close AS DECIMAL(10, 2)) ORDER BY dt) FILTER (WHERE option_type='C') AS close,
                    array_agg(CAST(iv30 AS DECIMAL(10, 4)) ORDER BY dt) FILTER (WHERE option_type='C') AS iv30,
                    array_agg(CAST(iv_percentile AS DECIMAL(10, 4)) ORDER BY dt) FILTER (WHERE option_type='C') AS iv_percentile,
                    array_agg(CAST(iv AS DECIMAL(10, 4)) ORDER BY dt) FILTER (WHERE option_type='C') AS cv,
                    array_agg(CAST(iv AS DECIMAL(10, 4)) ORDER BY dt) FILTER (WHERE option_type='P') AS pv,
                    array_agg(CAST(mid_price AS DECIMAL(10, 2)) ORDER BY dt) FILTER (WHERE option_type='C') AS cp,
                    array_agg(CAST(mid_price AS DECIMAL(10, 2)) ORDER BY dt) FILTER (WHERE option_type='P') AS pp
                FROM M
                WHERE rn = 1
                --${mode == 'strike' ? '' : 'WHERE rn = 1'}
            ) t`;

            //log the time it took to complete it
            const start = performance.now();
            const result = await connection.runAndReadAll(queryToExecute)
            const end = performance.now();
            logger.info(`✅ Query completed in ${(end - start).toFixed(2)} ms`);
            rows = result.getRows().map(r => JSON.parse(r[0]))[0];  //takes first row and first column
        }
        catch (err) {
            logger.error(`error occurred while processing request: ${err}`);
            hasError = true;
        }

        await publish(requestId, hasError, rows);
        logger.debug(`Worker volatility request completed! ${JSON.stringify(args)}`,);

    } catch (error) {
        console.error(error);
        logger.error(`Error processing worker-volatility-request: ${JSON.stringify(error)}`);
    }
};

const handleOptionsStatsMessage = async (args: OptionsStatsRequest) => {
    try {
        const { symbol, lookbackDays, requestId } = OptionsStatsSchema.parse(args);

        logger.info(`Worker options stats request received: ${JSON.stringify(args)}`);
        using stack = new DisposableStack();
        const instance = await DuckDBInstance.create(":memory:");
        stack.defer(() => instance.closeSync());
        const connection = await instance.connect();
        stack.defer(() => connection.closeSync());
        let rows = [];
        let hasError = false;
        try {

            const queryToExecute = `
            SELECT to_json(t)    
            FROM (
                WITH T AS (
                    SELECT DISTINCT dt, close, symbol
                    FROM '${OHLC_DATA_DIR}/*.parquet' WHERE replace(symbol, '^', '') = '${symbol}'
                    AND close > 0
                ), T2 AS (
                    SELECT dt, option_type, SUM(open_interest) AS total_oi,
                    SUM(open_interest * theo)*100 AS total_price,
                    SUM(open_interest * abs(delta)) AS total_delta,
                    COUNT(DISTINCT option) AS options_count
                    FROM '${DATA_DIR}/symbol=${symbol}/*.parquet'
                    GROUP BY dt, option_type
                ), M AS (
                    SELECT T2.*, T.close
                    FROM T
                    JOIN T2 ON T.dt = T2.dt
                    WHERE T.dt >= current_date - ${lookbackDays}
                )
                
                SELECT 
                    array_agg(DISTINCT dt ORDER BY dt) AS dt,
                    array_agg(close ORDER BY dt) FILTER (WHERE option_type='C') AS close,
                    array_agg(options_count ORDER BY dt) FILTER (WHERE option_type='C') AS options_count,
                    array_agg(total_oi ORDER BY dt) FILTER (WHERE option_type='C') AS co,
                    array_agg(total_oi ORDER BY dt) FILTER (WHERE option_type='P') AS po,
                    array_agg(CAST(total_price AS BIGINT) ORDER BY dt) FILTER (WHERE option_type='C') AS cp,
                    array_agg(CAST(total_price AS BIGINT) ORDER BY dt) FILTER (WHERE option_type='P') AS pp,
                    array_agg(CAST(total_delta AS BIGINT) ORDER BY dt) FILTER (WHERE option_type='C') AS cd,
                    array_agg(CAST(total_delta AS BIGINT) ORDER BY dt) FILTER (WHERE option_type='P') AS pd
                FROM M
            ) t`;

            //log the time it took to complete it
            const start = performance.now();
            const result = await connection.runAndReadAll(queryToExecute)
            const end = performance.now();
            logger.info(`✅ Query completed in ${(end - start).toFixed(2)} ms`);
            rows = result.getRows().map(r => JSON.parse(r[0]))[0];  //takes first row and first column
        }
        catch (err) {
            logger.error(`error occurred while processing request: ${err}`);
            hasError = true;
        }
        await publish(requestId, hasError, rows);
        logger.debug(`Worker volatility request completed! ${JSON.stringify(args)}`,);

    } catch (error) {
        logger.error(`Error processing worker-volatility-request: ${JSON.stringify(error)}`);
    }
};

async function publish(requestId: string, hasError: boolean, rows: any) {
    const payload = {
        requestId: requestId,
        hasError,
        value: rows
    };
    //const packed = lz.compressToBase64(JSON.stringify(payload));
    const packed = Buffer.from(pako.deflate(JSON.stringify(payload))).toString("base64");
    const packedData = {
        d: packed,
    };

    logger.info(`Publishing response for requestId ${requestId}. Size ${prettyBytes(JSON.stringify(packedData).length)}`);

    await pusher.trigger(channelName, `worker-response-${requestId}`, packedData);

    await redisClient.publish(`worker-response-${requestId}`, JSON.stringify(payload));
}

function shutdown() {
    logger.info(`shutting down...`);
    channel.disconnect();
    setTimeout(() => {
        Deno.exit(0);
    }, 100);

    logger.info("will shut down in 100ms")
}

channel.bind('volatility-query', handleVolatilityMessage);
channel.bind('options-stat-query', handleOptionsStatsMessage);

logger.info(`Worker is listening for messages on channel ${channelName}...`);

Deno.addSignalListener("SIGTERM", shutdown);
Deno.addSignalListener("SIGINT", shutdown);

// For debugging in local env.
// handleVolatilityMessage({
//     symbol: "COIN",
//     lookbackDays: 90,
//     delta: 25,
//     expiration: "2026-04-17",
//     // expiryMode: "fixed",
//     // expiryMode: "rolling",
//     // dte : 7,
//     mode: "delta",
//     //expiryMode: "rolling",
//     requestId: crypto.randomUUID()
// })