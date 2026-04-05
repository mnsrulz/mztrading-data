import { z } from "https://esm.sh/zod@4.2.1";
import pino from "https://esm.sh/pino@10.1.0";
import pretty from "https://esm.sh/pino-pretty@10.3.0";
import PusherJS from 'https://esm.sh/pusher-js@8.4.0';
import Pusher from 'https://esm.sh/pusher@5.3.2';
const pusher = Pusher.forURL(Deno.env.get('PUSHER_URI') || '');
const channelName = Deno.env.get('PUSHER_CHANNEL_NAME') || 'mztrading-channel';
const pusherClient = new PusherJS(Deno.env.get('PUSHER_APP_KEY') || '', {
    cluster: Deno.env.get('PUSHER_APP_CLUSTER') || 'us2',
});

const channel = pusherClient.subscribe(channelName);
const DATA_DIR = Deno.env.get("DATA_DIR") || '/data/w2-output-flat';
const OHLC_DATA_DIR = Deno.env.get("OHLC_DIR") || '/data/ohlc';
const LOG_LEVEL = Deno.env.get("LOG_LEVEL") || 'info';

import { DuckDBInstance } from "npm:@duckdb/node-api@1.4.3-r.2";

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

    expiration: z.string()
        .regex(/^\d{4}-\d{2}-\d{2}$/, "Expiration must be YYYY-MM-DD format"),

    requestId: z.uuid(),
};

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

export const OptionsVolRequestSchema = z.discriminatedUnion("mode", [
    DeltaModeSchema,
    StrikeModeSchema,
    AtmModeSchema
]);

type OptionsVolRequest = z.infer<typeof OptionsVolRequestSchema>;
type OptionsStatsRequest = z.infer<typeof OptionsStatsSchema>;

/*
 {"symbol":"AAPL","lookbackDays":180,"delta":25,"expiration":"2025-11-07","mode":"delta","requestId":"977c5c7b-7e96-45d1-991b-db70992d0846"}       
*/
const handleVolatilityMessage = async (args: OptionsVolRequest) => {
    try {
        const { symbol, lookbackDays, delta, expiration, mode, strike, requestId } = OptionsVolRequestSchema.parse(args);

        logger.info(`Worker volatility request received: ${JSON.stringify(args)}`);
        using stack = new DisposableStack();
        const instance = await DuckDBInstance.create(":memory:");
        stack.defer(() => instance.closeSync());
        const connection = await instance.connect();
        stack.defer(() => connection.closeSync());
        const strikeFilter = mode == 'strike' ? ` AND strike = ${strike}` : '';
        const partitionOrderColumn = mode == 'atm' ? 'price_strike_diff' : 'delta_diff';
        let rows = [];
        let hasError = false;
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
                    abs(abs(delta) - ${(delta || 0) / 100}) AS delta_diff
                    --FROM '${DATA_DIR}/${symbol}_*.parquet' opdata
                    FROM '${DATA_DIR}/symbol=${symbol}/*.parquet' opdata
                    JOIN OHLC ON OHLC.dt = opdata.dt
                    WHERE expiration = '${expiration}' 
                            AND (open_interest > 0 OR bid > 0 OR ask > 0 OR iv > 0)           --JUST TO MAKE SURE NEW CONTRACTS WON'T APPEAR IN THE DATASET WHICH LIKELY REPRESENTED BY 0 OI, bid/ask/iv
                            AND OHLC.dt >= current_date - ${lookbackDays} ${strikeFilter} 
                ), M AS (
                    SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY dt, option_type ORDER BY ${partitionOrderColumn} ASC) AS rn
                    FROM I
                )
                SELECT 
                    array_agg(DISTINCT dt ORDER BY dt) AS dt,
                    array_agg(close AS DECIMAL(10, 2) ORDER BY dt) FILTER (WHERE option_type='C') AS close,
                    array_agg(iv30 AS DECIMAL(10, 4) ORDER BY dt) FILTER (WHERE option_type='C') AS iv30,
                    array_agg(iv_percentile AS DECIMAL(10, 4) ORDER BY dt) FILTER (WHERE option_type='C') AS iv_percentile,
                    array_agg(iv AS DECIMAL(10, 4) ORDER BY dt) FILTER (WHERE option_type='C') AS cv,
                    array_agg(iv AS DECIMAL(10, 4) ORDER BY dt) FILTER (WHERE option_type='P') AS pv,
                    array_agg(mid_price AS DECIMAL(10, 2) ORDER BY dt) FILTER (WHERE option_type='C') AS cp,
                    array_agg(mid_price AS DECIMAL(10, 2) ORDER BY dt) FILTER (WHERE option_type='P') AS pp
                FROM M
                ${ mode == 'strike' ? '' : 'WHERE rn = 1'}
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

        await pusher.trigger(channelName, `worker-response-${requestId}`, {
            requestId: requestId,
            hasError,
            value: rows
        });

        logger.debug(`Worker volatility request completed! ${JSON.stringify(args)}`, );

    } catch (error) {
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
                    array_agg(total_price ORDER BY dt) FILTER (WHERE option_type='C') AS cp,
                    array_agg(total_price ORDER BY dt) FILTER (WHERE option_type='P') AS pp,
                    array_agg(total_delta ORDER BY dt) FILTER (WHERE option_type='C') AS cd,
                    array_agg(total_delta ORDER BY dt) FILTER (WHERE option_type='P') AS pd
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

        await pusher.trigger(channelName, `worker-response-${requestId}`, {
            requestId: requestId,
            hasError,
            value: rows
        });

        logger.debug(`Worker volatility request completed! ${JSON.stringify(args)}`, );

    } catch (error) {
        logger.error(`Error processing worker-volatility-request: ${JSON.stringify(error)}`);
    }
};



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


Deno.addSignalListener("SIGTERM", shutdown);
Deno.addSignalListener("SIGINT", shutdown);