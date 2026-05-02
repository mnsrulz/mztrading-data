import { z } from "https://esm.sh/zod@4.3.6";
import pino from "https://esm.sh/pino@10.1.0";
import pretty from "https://esm.sh/pino-pretty@10.3.0";

import { createClient } from "npm:redis@^4.5";
import { DuckDBInstance } from "npm:@duckdb/node-api@1.4.3-r.2";
import Emittery from 'https://esm.sh/emittery@2.0.0';

const emitter = new Emittery();

const DATA_DIR = Deno.env.get("DATA_DIR") || 'temp/w2-output';
const OHLC_DATA_DIR = Deno.env.get("OHLC_DIR") || 'temp/ohlc';
const LOG_LEVEL = Deno.env.get("LOG_LEVEL") || 'info';

const REDIS_URI = Deno.env.get('REDIS_URI');
const DEBUG_MODE = Deno.env.get('DEBUG_MODE') == '1';

const redisSubscriber = createClient({
    url: REDIS_URI,
    socket: {
        keepAlive: 30000,
        reconnectStrategy: retries => {
            logger.warn(`Redis connection lost. Attempting to reconnect... (attempt ${retries + 1})`);
            return Math.min(retries * 1000, 30000); // Exponential backoff up to 30 seconds 
        }
    }
});
const duckDbInstance = await DuckDBInstance.create(":memory:");

const stream = pretty({
    singleLine: true,
    colorize: true,
    include: "time,msg,err",
    messageFormat: (log, messageKey) => { return `${log[messageKey]}` },
});

const logger = pino({
    level: LOG_LEVEL
}, stream);

BigInt.prototype.toJSON = function () {
    return Number(this.toString());
};


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
    requestId: z.uuid(),
    channel: z.string()
});

const ExpectedMoveRequestSchema = z.object({
    symbol: z.string()
        .nonempty()
        .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),
    lookbackDays: z.number().int().positive(),
    requestId: z.uuid(),
    channel: z.string(),
    expiryMode: z.enum(["weekly", "monthly"])
});

const OhlcSchema = z.object({
    symbol: z.string()
        .nonempty()
        .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),
    lookbackDays: z.number().int().positive(),
    requestId: z.uuid(),
    channel: z.string()
});

const DynamicSqlSchema = z.object({
    symbol: z.string()
        .nonempty()
        .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),
    sql: z.string().nonempty(),
    requestId: z.uuid(),
    channel: z.string()
});

const BaseSchema = {
    symbol: z.string()
        .nonempty()
        .regex(/^[a-zA-Z0-9]+$/, "Symbol must be alphanumeric"),

    lookbackDays: z.number().int().positive(),

    requestId: z.uuid(),
    channel: z.string()
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
type ExpectedMoveRequest = z.infer<typeof ExpectedMoveRequestSchema>;
type DynamicSqlRequest = z.infer<typeof DynamicSqlSchema>;
type OhlcRequest = z.infer<typeof OhlcSchema>;

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
            logger.error({ err }, `error occurred while processing request`);
            hasError = true;
        }

        await publish(requestId, hasError, rows, args.channel);
        logger.debug(`Worker volatility request completed! ${JSON.stringify(args)}`,);

    } catch (error) {
        console.error(error);
        logger.error({ err: error }, `Error processing worker-volatility-request`);
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
        await publish(requestId, hasError, rows, args.channel);
        logger.debug(`Worker volatility request completed! ${JSON.stringify(args)}`);

    } catch (error) {
        logger.error({ err: error }, `Error processing worker-volatility-request`);
    }
};

const handleExpectedMoveMessage = async (args: ExpectedMoveRequest) => {
    try {
        const { symbol, lookbackDays, requestId, expiryMode: mode } = ExpectedMoveRequestSchema.parse(args);
        logger.info(`Expected Move request received: ${JSON.stringify(args)}`);

        const query = `
            SELECT quote_date as dt, underlying_close_price as last_close, straddle_price
            --, high, low, weekly_close as close, strike_price, expected_move_percent, low, high,
            --ROUND(CASE WHEN ABS(high/underlying_close_price-1) > ABS(low/underlying_close_price-1) THEN high/underlying_close_price-1
            --ELSE low/underlying_close_price-1 END * 100, 2) AS actual_max_move_percent,
            --ROUND((weekly_close/underlying_close_price-1) * 100, 2) AS actual_move_percent 
            FROM (
                SELECT quote_date, expiration_date, dte, strike_price, underlying_close_price,
                round(SUM(mid_price),2) AS straddle_price, 
                round((straddle_price/underlying_close_price)*100,2) as expected_move_percent
                FROM dataset
                WHERE moneyness = 'ATM'
                AND quote_dow = 1
                AND quote_date >= current_date - ${lookbackDays}
                AND ${mode == 'weekly' ? 'dte <=7' : 'dte BETWEEN 24 AND 28'}
                AND ${mode == 'weekly' ? 'is_weekly_expiration' : 'is_monthly_expiration'} = 1
                GROUP BY quote_date, dte, strike_price, expiration_date, underlying_close_price
            ) 
            --LEFT JOIN (
            --    SELECT CAST(date_trunc(${mode == 'weekly' ? "'week'" : "'month'"}, CAST(quote_date as date) - 1) AS STRING) AS start, 
            --    MAX(underlying_high_price) high, 
            --    MIN(underlying_low_price) low,
            --    arg_max(underlying_close_price, quote_date) AS weekly_close
            --    FROM dataset
            --    GROUP BY start
            --) WT ON WT.start = CAST(date_trunc(${mode == 'weekly' ? "'week'" : "'month'"}, CAST(quote_date as date)) AS STRING)
            ORDER BY quote_date
        `

        let rows;
        let hasError = false;
        try {

            const result = await executeReaderInternal(symbol, query, 99999);
            const [dt, last_close, straddle_price] = result.getColumnsJson();

            rows = {
                dt,
                last_close,
                straddle_price
            };
        }
        catch (err) {
            logger.error({ err }, `error occurred while processing request`);
            hasError = true;
        }
        await publish(requestId, hasError, rows, args.channel);
        logger.debug(`Expected Move request finished${hasError ? ' with error.' : '!'}`);
    } catch (error) {
        logger.error({ err: error }, `Error processing Expected move request`);
    }
};

const handleOhlcMessage = async (args: OhlcRequest) => {
    try {
        const { symbol, lookbackDays, requestId } = OhlcSchema.parse(args);
        logger.info(`Ohlc request received: ${JSON.stringify(args)}`);

        //const defaultOhlcData = { dt: [] as string[], open: [] as number[], high: [] as number[], low: [] as number[], close: [] as number[], iv30: [] as number[] };
        /*
        This method will return the actual ohlc for given range. We can also get it from yfinace apis.
        */
        const query = `
            SELECT DISTINCT strftime(CASE 
                WHEN dayofweek(CAST(dt AS DATE)) = 1 THEN CAST(dt AS DATE) - INTERVAL 3 DAY 
                ELSE CAST(dt AS DATE) - INTERVAL 1 DAY 
            END, '%Y-%m-%d') dt, underlying_open_price as open, underlying_high_price as high, 
            underlying_low_price as low, underlying_close_price as close, underlying_iv30 as iv30
            FROM T
            WHERE T.dt >= current_date - ${lookbackDays} AND dayofweek(dt) <> 6 --EXCLUDE SATURDAYS LEGACY DATA 
            ORDER BY dt
        `

        let rows;
        let hasError = false;
        try {

            const result = await executeReaderInternal(symbol, query, 99999);
            const [dt, open, high, low, close, iv30 ] = result.getColumnsJson();

            rows = {
                dt,
                open,
                high,
                low,
                close,
                iv30
            };
        }
        catch (err) {
            logger.error({ err }, `error occurred while processing request`);
            hasError = true;
        }
        await publish(requestId, hasError, rows, args.channel);
        logger.debug(`Ohlc request finished${hasError ? ' with error.' : '!'}`);
    } catch (error) {
        logger.error({ err: error }, `Error processing Ohlc request`);
    }
};

const handleDynamicSqlMessage = async (args: DynamicSqlRequest) => {
    try {
        const { symbol, sql, requestId } = DynamicSqlSchema.parse(args);
        let rows: { rows: any[], columns: any } = { rows: [], columns: null };
        let hasError = false;
        try {
            const result = await executeReaderInternal(symbol, sql);
            rows = { rows: result.getRowsJson(), columns: result.columnNamesAndTypesJson() };
        }
        catch (err) {
            logger.error({ err }, `error occurred while processing request.`);
            hasError = true;
        }
        await publish(requestId, hasError, rows, args.channel);
        logger.debug(`Dynamic SQL request completed! ${JSON.stringify(args)}`);

    } catch (error) {
        logger.error({ err: error }, `Error processing worker-dynamic-sql-request`);
    }
};

async function publish(requestId: string, hasError: boolean, rows: any, channel: string) {
    if (DEBUG_MODE) return;
    const payload = {
        requestId: requestId,
        hasError,
        value: rows
    };

    //this is for publishing the message so any subscriber can listen to.
    const redisPublisher = redisSubscriber.duplicate();
    await redisPublisher.connect();

    const count = await redisPublisher.publish(`worker-response-${channel}`, JSON.stringify(payload));
    redisPublisher.quit();
    logger.info(`Published response for requestId ${requestId} to ${count} subscriber${count > 1 ? 's' : ''}`);
    //await redisClient.publish(`worker-response-${requestId}`, JSON.stringify(payload));
}

async function executeReaderInternal(symbol: string, sql: string, limit = 1000) {
    using stack = new DisposableStack();
    const connection = await duckDbInstance.connect();
    stack.defer(() => connection.closeSync());
    let rows = [];
    const limitClause = limit > 0 ? `LIMIT ${limit}` : '';

    const baseQueryCte = `
                WITH T AS (
                    SELECT DISTINCT dt, open as underlying_open_price, high as underlying_high_price, low as underlying_low_price, 
                    close as underlying_close_price, volume as underlying_volume, 
                    -- iv30 as underlying_iv30, 
                    -- NULLIF treats 0 as NULL so IGNORE NULLS can skip it
                    last_value(nullif(iv30, 0) IGNORE NULLS) OVER (
                        ORDER BY dt 
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as underlying_iv30,
                    symbol as underlying_symbol
                    FROM '${OHLC_DATA_DIR}/*.parquet' WHERE replace(symbol, '^', '') = '${symbol}'
                    AND underlying_close_price > 0
                ), T2 AS (
                    SELECT *,
                    CAST(DATE_DIFF('day', dt, CAST(strftime(expiration, '%Y-%m-%d') as date)) AS INT) AS dte
                    FROM '${DATA_DIR}/symbol=${symbol}/*.parquet'
                    -- this is to make sure we remove the first quote for each option contract when they appear for the very first time in the dataset, which likely represented by 0 OI, bid, ask, iv. we want to remove those data points because they can be very misleading for the analysis, especially for the newly listed contracts which usually have a lot of zero-OI quotes at the beginning.
                    QUALIFY dt > MIN(dt) OVER (PARTITION BY expiration, strike, option_type)    
                ), dataset AS (
                    SELECT T.dt AS quote_date,
                    strftime(expiration, '%Y-%m-%d') AS expiration_date,
                    dayofweek(CAST(expiration as date)) AS expiration_dow,
                    CASE WHEN expiration_date = MAX(expiration_date) OVER (PARTITION BY date_trunc('week', CAST(expiration_date AS DATE))) THEN 1 ELSE 0 END AS is_weekly_expiration,
                    CASE WHEN is_weekly_expiration = 1 AND day(CAST(expiration_date AS DATE)) BETWEEN 15 AND 21 THEN 1 ELSE 0 END AS is_monthly_expiration,
                    dayofweek(CAST(T.dt as date)) AS quote_dow,

                    dte,
                    option_symbol AS option_ticker,
                    option_type,
                    strike AS strike_price,

                    open_interest,
                    volume AS option_volume,

                    delta,
                    gamma,
                    vega,
                    theta,
                    rho,

                    theo AS theoretical_price,
                    iv AS implied_volatility,

                    open AS option_open_price,
                    high AS option_high_price,
                    bid AS bid_price,
                    ask AS ask_price,
                    round((bid + ask) / 2, 2) AS mid_price,
                    CASE
                        WHEN open_interest > 1000 AND volume > 100 THEN 'HIGH'
                        WHEN open_interest > 100 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END AS liquidity_tier,
                    volume / NULLIF(open_interest, 0) AS volume_oi_ratio,
                    underlying_symbol,
                    underlying_open_price, underlying_high_price, underlying_low_price, underlying_close_price,
                    underlying_iv30, underlying_volume,
                    CASE
                        WHEN ROW_NUMBER() OVER ( 
                            PARTITION BY T2.dt, expiration, option_type 
                            ORDER BY ABS(strike - underlying_close_price), strike 
                        ) = 1 THEN 'ATM'
                        WHEN (
                            (option_type = 'call' AND strike < underlying_close_price) OR
                            (option_type = 'put'  AND strike > underlying_close_price)
                        ) THEN 'ITM'
                        ELSE 'OTM'
                    END AS moneyness,
                    CASE
                        WHEN (option_type = 'C' AND strike < underlying_close_price) OR
                            (option_type = 'P'  AND strike > underlying_close_price)
                            THEN -ABS(strike - underlying_close_price) / underlying_close_price * 100
                        ELSE ABS(strike - underlying_close_price) / underlying_close_price * 100
                    END AS moneyness_percent,
                    CASE
                        WHEN dte <= 7 THEN '0-7D'
                        WHEN dte <= 30 THEN '7-30D'
                        WHEN dte <= 90 THEN '30-90D'
                        ELSE '90D+'
                    END AS expiry_bucket
                    FROM T2
                    JOIN T ON T.dt = T2.dt
                )`;

    //log the time it took to complete it
    const start = performance.now();
    const result = await connection.runAndReadAll(`${baseQueryCte}
            --SELECT json_group_array(t) FROM (    
            SELECT * FROM (
                SELECT * FROM (    
                    ${sql}
                    ) LIMITED_CTE ${limitClause}
            ) t`);
    const end = performance.now();
    logger.info(`✅ Query completed in ${(end - start).toFixed(2)} ms`);

    // rows = JSON.parse(result.getRows().at(0)?.at(0) as string); //takes first row and first column
    //rows = result.getRows();
    return result;
}

if (DEBUG_MODE) {
    logger.info(`Running in debug mode. Executing test query...`);

    await handleOhlcMessage({
        channel: 'test',
        lookbackDays: 45,
        requestId: crypto.randomUUID(),
        symbol: 'COIN'
    })
    //await executeReaderInternal("COIN", `SELECT * FROM dataset LIMIT 10`, 5);
} else {
    const shutdown = () => {
        logger.info(`shutting down...`);
        redisSubscriber.quit();
        setTimeout(() => {
            Deno.exit(0);
        }, 100);

        logger.info("will shut down in 100ms")
    }
    emitter.on('volatility-query', ({ data }) => handleVolatilityMessage(data as OptionsVolRequest));
    emitter.on('options-stat-query', ({ data }) => handleOptionsStatsMessage(data as OptionsStatsRequest));
    emitter.on('dynamic-sql-query', ({ data }) => handleDynamicSqlMessage(data as DynamicSqlRequest));
    emitter.on('expected-move-query', ({ data }) => handleExpectedMoveMessage(data as ExpectedMoveRequest));
    emitter.on('ohlc-query', ({ data }) => handleOhlcMessage(data as OhlcRequest));

    await redisSubscriber.connect();

    await redisSubscriber.subscribe('worker-request', (message) => {
        logger.info(`Received message from Redis on worker-request channel`);
        const data = JSON.parse(message) as { requestType: string };
        emitter.emit(data.requestType, data);
    });

    Deno.addSignalListener("SIGTERM", shutdown);
    Deno.addSignalListener("SIGINT", shutdown);
}

logger.info(`Worker is running...`);

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

