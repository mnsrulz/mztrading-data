import { Hono } from "https://esm.sh/hono@4.9.8";
import { z } from "https://esm.sh/zod@4.3.6";
import pino from "https://esm.sh/pino@10.1.0";
import pretty from "https://esm.sh/pino-pretty@10.3.0";
import pinologtail from "https://esm.sh/@logtail/pino@0.5.8";

import { createClient } from "npm:redis@^4.5";
import { DuckDBInstance } from "npm:@duckdb/node-api@1.5.2-r.1";
import PusherJS from 'https://esm.sh/pusher-js@8.4.0';
import pRetry from 'https://esm.sh/p-retry@8.0.0';
import ky from 'https://esm.sh/ky@1.8.2';


const app = new Hono();


const DATA_DIR = Deno.env.get("DATA_DIR") || 'temp/w2-output';
const OHLC_DATA_DIR = Deno.env.get("OHLC_DIR") || 'temp/ohlc';
const LOG_LEVEL = Deno.env.get("LOG_LEVEL") || 'info';

const PUSHER_APP_KEY = Deno.env.get('PUSHER_APP_KEY');
const REDIS_URI = Deno.env.get('REDIS_URI');
const VALKEY_URI = Deno.env.get('VALKEY_URI');
const LOGTAIL_TOKEN = Deno.env.get('LOGTAIL_TOKEN');
const DEBUG_MODE = Deno.env.get('DEBUG_MODE') == '1';

const mzingestClient = ky.create({
    prefixUrl: Deno.env.get('MZINGEST_BASE_URI') || 'https://mzingest.netlify.app/api'
});
// 1. Initialize and connect your Redis client to your Valkey container
const valkey = createClient({
    url: VALKEY_URI
});

let redisSubscriber = createClient({
    url: REDIS_URI,
    socket: {
        keepAlive: 30000,
        reconnectStrategy: retries => {
            logger.warn(`Redis connection lost. Attempting to reconnect... (attempt ${retries + 1})`);
            return Math.min(retries * 1000, 30000); // Exponential backoff up to 30 seconds 
        }
    }
});

let consecutiveRedisFailures = 0;

const channelName = Deno.env.get('PUSHER_CHANNEL_NAME') || 'mztrading-channel';
const pusherClient = new PusherJS(PUSHER_APP_KEY || '', {
    cluster: Deno.env.get('PUSHER_APP_CLUSTER') || 'us2',
});

const channel = pusherClient.subscribe(channelName);

const duckDbInstance = await DuckDBInstance.create(":memory:");

const stream = pretty({
    singleLine: true,
    colorize: true,
    include: "time,msg,err",
    messageFormat: (log, messageKey) => { return `${log[messageKey]}` },
});

const pinoStreams = [stream];

if (LOGTAIL_TOKEN) {
    const logTailStream = await pinologtail({
        options: {


        },
        sourceToken: LOGTAIL_TOKEN
    });
    pinoStreams.push(logTailStream);
}

const logger = pino({
    level: LOG_LEVEL
}, pino.multistream(pinoStreams));

BigInt.prototype.toJSON = function () {
    return Number(this.toString());
};

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
        .regex(/^\d{4}-\d{2}-\d{2}$/, "Expiration must be YYYY-MM-DD")
});

const RollingExpirySchema = z.object({
    expiryMode: z.literal("rolling"),
    dte: z.number().int().positive()
});

const ExpirySchema = z.discriminatedUnion("expiryMode", [
    FixedExpirySchema,
    RollingExpirySchema
]);

// delta mode
const DeltaModeSchema = z.object({
    ...BaseSchema,
    mode: z.literal("delta"),
    delta: z.number().max(100).nonnegative()
});

// strike mode
const StrikeModeSchema = z.object({
    ...BaseSchema,
    mode: z.literal("strike"),
    strike: z.number().positive()
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

const handleVolatilityMessageV2 = async (args: OptionsVolRequest) => {
    const { symbol, lookbackDays } = OptionsVolRequestSchema.parse(args);
    const delta = args.mode == 'delta' ? args.delta : 0;

    let qualifyClause = '', whereClause = ` WHERE quote_date >= (current_date - ${lookbackDays})`;
    let useQualifyClause = false;

    if (args.expiryMode == 'rolling') {
        useQualifyClause = true;
        whereClause = `${whereClause} AND dte >= '${args.dte}'`;
    } else if (args.expiryMode == 'fixed') {
        whereClause = `${whereClause} AND expiration_date = '${args.expiration}'`;
    }

    let partitionOrderColumn = '';

    if (args.mode == 'strike') {
        whereClause = `${whereClause} AND strike_price = ${args.strike}`;
    } else {
        useQualifyClause = true;
        partitionOrderColumn = args.mode == 'atm' ? ', price_strike_diff' : ', delta_diff';
    }

    if (useQualifyClause) {
        qualifyClause = ` 
        QUALIFY ROW_NUMBER() OVER (
                PARTITION BY quote_date, option_type 
                ORDER BY dte ASC ${partitionOrderColumn}
        ) = 1`;
    }

    const sql = `
    PIVOT (
    SELECT quote_date as dt, option_type, underlying_close_price as close, 
    expiration_date as expiry, strike_price, round((bid_price + ask_price) / 2, 2) as mid_price, 
    implied_volatility as iv, underlying_iv30 as iv30,
    underlying_iv30_percentile as iv30_percentile
    FROM (
        SELECT *, abs(delta) AS abs_delta,
                abs(strike_price - underlying_close_price) AS price_strike_diff,
                abs(abs(delta) - ${(delta || 0) / 100}) AS delta_diff 
        FROM
        base
    )
    ${whereClause}
    ${qualifyClause}
    ORDER BY dt
    )
    ON option_type
    USING FIRST(strike_price) AS strike, FIRST(mid_price) AS mid, FIRST(iv) AS iv
    GROUP BY dt, close, iv30, iv30_percentile, expiry
    ORDER BY dt
    `;

    const result = await executeReaderInternal(symbol, sql, 99999);
    const [dt, close, iv30, iv_percentile, expiry, cs, cp, cv, ps, pp, pv] = result.getColumnsJson();

    return { dt, close, iv30, iv_percentile, cv, pv, cs, ps, cp, pp, expiry };
};

const handleOptionsStatsMessageV2 = async (args: OptionsStatsRequest) => {
    const { symbol, lookbackDays } = OptionsStatsSchema.parse(args);
    logger.info(`Worker options stats V2 request received: ${JSON.stringify(args)}`);

    const query = `
        PIVOT (
            SELECT T2.dt, T2.option_type, T.underlying_close_price AS close,
                SUM(T2.open_interest) AS total_oi,
                ROUND(SUM(T2.open_interest * T2.theo) * 100) AS total_price,
                ROUND(SUM(T2.open_interest * abs(T2.delta))) AS total_delta,
                CAST(COUNT(DISTINCT T2.option) AS INTEGER) AS options_count
            FROM T2
            JOIN T ON T.dt = T2.dt
            WHERE T2.dt >= current_date - ${lookbackDays}
            GROUP BY T2.dt, T2.option_type, T.underlying_close_price
        )
        ON option_type
        USING FIRST(total_oi) AS oi, FIRST(total_price) AS price, FIRST(total_delta) AS delta, FIRST(options_count) AS options
        GROUP BY dt, close
        ORDER BY dt
    `

    const result = await executeReaderInternal(symbol, query, 99999);
    const [dt, close, co, cp, cd, options_count, po, pp, pd] = result.getColumnsJson();
    logger.debug(`Worker statistics V2 request completed! ${JSON.stringify(args)}`);
    return { dt, close, options_count, co, po, cp, pp, cd, pd };
};

const handleExpectedMoveMessage = async (args: ExpectedMoveRequest) => {
    const { symbol, lookbackDays, expiryMode: mode } = ExpectedMoveRequestSchema.parse(args);
    logger.info(`Expected Move request received: ${JSON.stringify(args)}`);

    const query = `
        SELECT quote_date as dt, underlying_close_price as last_close, straddle_price, expiration_date AS expiry
        FROM (
            SELECT quote_date, expiration_date, dte, strike_price, underlying_close_price,
            round(SUM(mid_price),2) AS straddle_price, 
            round((straddle_price/underlying_close_price)*100,2) as expected_move_percent
            FROM dataset
            WHERE moneyness = 'ATM'
            AND quote_dow = 1
            AND quote_date >= current_date - ${lookbackDays}
            AND ${mode == 'weekly' ? 'dte <=7' : 'dte BETWEEN 25 AND 31'}
            AND ${mode == 'weekly' ? 'is_weekly_expiration' : 'is_monthly_expiration'} = 1
            GROUP BY quote_date, dte, strike_price, expiration_date, underlying_close_price
        ) 
        ORDER BY quote_date
    `

    const result = await executeReaderInternal(symbol, query, 99999);
    const [dt, last_close, straddle_price, expiry] = result.getColumnsJson();
    logger.debug(`Expected Move request finished!`);
    return { dt, last_close, straddle_price, expiry };
};

const handleOhlcMessage = async (args: OhlcRequest) => {
    const { symbol, lookbackDays } = OhlcSchema.parse(args);
    logger.info(`Ohlc request received: ${JSON.stringify(args)}`);

    const query = `
        SELECT DISTINCT strftime(CASE 
            WHEN dayofweek(CAST(dt AS DATE)) = 1 THEN CAST(dt AS DATE) - INTERVAL 3 DAY 
            ELSE CAST(dt AS DATE) - INTERVAL 1 DAY 
        END, '%Y-%m-%d') dt, underlying_open_price as open, underlying_high_price as high, 
        underlying_low_price as low, underlying_close_price as close, underlying_iv30 as iv30
        FROM T
        WHERE T.dt >= current_date - ${lookbackDays} AND dayofweek(dt) <> 6
        ORDER BY dt
    `

    const result = await executeReaderInternal(symbol, query, 99999);
    const [dt, open, high, low, close, iv30] = result.getColumnsJson();
    logger.debug(`Ohlc request finished!`);
    return { dt, open, high, low, close, iv30 };
};

const handleDynamicSqlMessage = async (args: DynamicSqlRequest) => {
    const { symbol, sql } = DynamicSqlSchema.parse(args);
    const result = await executeReaderInternal(symbol, sql);
    logger.debug(`Dynamic SQL request completed! ${JSON.stringify(args)}`);
    return { rows: result.getRowsJson(), columns: result.columnNamesAndTypesJson() };
};

async function publish(requestId: string, hasError: boolean, rows: any) {
    if (DEBUG_MODE) {
        logger.info(`[DEBUG] publish called for ${requestId}: hasError=${hasError}, rows=${JSON.stringify(rows)}`);
        return;
    }
    const payload = {
        requestId: requestId,
        hasError,
        value: rows
    };

    const publishToMzIngest = async () => {
        await mzingestClient.put(`requests/${requestId}/result`, {
            json: payload
        }).json().catch(d => logger.error(`Invalid response received while persisting the result for request: ${requestId}, ${JSON.stringify(d)}`))
    }

    const publishToRedis = async () => {
        const redisWriter = redisSubscriber.duplicate();
        await redisWriter.connect();
        await redisWriter.set(requestId, JSON.stringify(payload), { EX: 60 });
        await redisWriter.quit();
        logger.info(`Stored result for requestId ${requestId} in Redis`);
    }

    await Promise.all([
        pRetry(publishToRedis, { retries: 3 }),
        pRetry(publishToMzIngest, { retries: 3 })
    ])
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
                    symbol as underlying_symbol,
                    CAST(100 *PERCENT_RANK() OVER (PARTITION BY symbol ORDER BY iv30) AS DECIMAL(10, 2)) AS underlying_iv30_percentile
                    FROM '${OHLC_DATA_DIR}/*.parquet' WHERE replace(symbol, '^', '') = '${symbol}'
                    AND underlying_close_price > 0
                ), T2 AS (
                    SELECT *,
                    --CAST(DATE_DIFF('day', dt, CAST(strftime(expiration, '%Y-%m-%d') as date)) AS INT) AS dte
                    DATE_DIFF('day', dt, expiration) AS dte
                    FROM '${DATA_DIR}/symbol=${symbol}/*.parquet'
                    -- this is to make sure we remove the first quote for each option contract when they appear for the very first time in the dataset, which likely represented by 0 OI, bid, ask, iv. we want to remove those data points because they can be very misleading for the analysis, especially for the newly listed contracts which usually have a lot of zero-OI quotes at the beginning.
                    --QUALIFY dt > MIN(dt) OVER (PARTITION BY expiration, strike, option_type)    
                    WHERE open_interest > 0 OR bid > 0 OR ask > 0 OR volume > 0
                ), weekly_expiries as (
                    select DISTINCT max(expiration) OVER (
                                            PARTITION BY date_trunc('week', expiration)
                                        ) AS expiration
                    from (SELECT DISTINCT expiration FROM T2)
                ), base AS (
                    SELECT T.dt AS quote_date,
                    expiration AS expiration_date,
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
                    iv * 100 AS implied_volatility,

                    open AS option_open_price,
                    high AS option_high_price,
                    bid AS bid_price,
                    ask AS ask_price,
                    underlying_symbol,
                    underlying_open_price, underlying_high_price, underlying_low_price, underlying_close_price,
                    underlying_iv30, underlying_volume,
                    underlying_iv30_percentile
                    FROM T2
                    JOIN T ON T.dt = T2.dt
                ), calc AS (
                    SELECT *,
                    dayofweek(CAST(quote_date as date)) AS quote_dow,
                    dayofweek(expiration_date) AS expiration_dow,
                    round((bid_price + ask_price) / 2, 2) AS mid_price,
                    abs(strike_price - underlying_close_price) AS strike_distance,
                    abs(strike_price - underlying_close_price) / underlying_close_price * 100 AS strike_distance_pct,
                    CASE WHEN weekly_expiries.expiration IS NOT NULL THEN 1 ELSE 0 END AS is_weekly_expiration,
                    CASE WHEN is_weekly_expiration = 1 AND day(expiration_date) BETWEEN 15 AND 21 THEN 1 ELSE 0 END AS is_monthly_expiration
                    FROM base
                    LEFT JOIN weekly_expiries ON base.expiration_date = weekly_expiries.expiration
                ), ranked AS (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY quote_date, expiration_date, option_type
                            ORDER BY strike_distance, strike_price
                        ) AS atm_rank
                    FROM calc
                ), dataset AS (
                    SELECT quote_date,
                    expiration_date,
                    expiration_dow,
                    quote_dow,
                    is_weekly_expiration,
                    is_monthly_expiration,
                    dte,
                    option_ticker,
                    option_type,
                    strike_price,
                    open_interest,
                    option_volume,

                    delta,
                    gamma,
                    vega,
                    theta,
                    rho,

                    theoretical_price,
                    implied_volatility,

                    option_open_price,
                    option_high_price,
                    bid_price,
                    ask_price,
                    mid_price,  
                    CASE
                        WHEN open_interest > 1000 AND option_volume > 100 THEN 'HIGH'
                        WHEN open_interest > 100 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END AS liquidity_tier,
                    option_volume / NULLIF(open_interest, 0) AS volume_oi_ratio,
                    underlying_symbol,
                    underlying_open_price, underlying_high_price, underlying_low_price, underlying_close_price,
                    underlying_iv30, underlying_volume,
                    underlying_iv30_percentile,
                    
                    CASE
                        WHEN atm_rank = 1 THEN 'ATM'
                        WHEN (
                            (option_type = 'call' AND strike_price < underlying_close_price) OR
                            (option_type = 'put'  AND strike_price > underlying_close_price)
                        ) THEN 'ITM'
                        ELSE 'OTM'
                    END AS moneyness,
                    CASE
                        WHEN (option_type = 'C' AND strike_price < underlying_close_price) OR
                            (option_type = 'P'  AND strike_price > underlying_close_price)
                            THEN -(strike_distance_pct)
                        ELSE strike_distance_pct
                    END AS moneyness_percent,
                    CASE
                        WHEN dte <= 7 THEN '0-7D'
                        WHEN dte <= 30 THEN '7-30D'
                        WHEN dte <= 90 THEN '30-90D'
                        ELSE '90D+'
                    END AS expiry_bucket
                    FROM ranked
                )`;

    //log the time it took to complete it
    const start = performance.now();
    const finalQuery = `
            -----BEGIN QUERY----
            ${baseQueryCte}
            --SELECT json_group_array(t) FROM (    
            SELECT * FROM (
                SELECT * FROM (    
                    ${sql}
                    ) LIMITED_CTE ${limitClause}
            )`
    //logger.info(`Executing query: ${finalQuery}`)
    const result = await connection.runAndReadAll(finalQuery);
    const end = performance.now();
    logger.info(`✅ Query completed in ${(end - start).toFixed(2)} ms`);

    // rows = JSON.parse(result.getRows().at(0)?.at(0) as string); //takes first row and first column
    //rows = result.getRows();
    return result;
}

function trackClient(clientId: string) {
    const currentMinute = new Date().toISOString().slice(0, 16);
    const valkeyKey = `track:${clientId}:${currentMinute}`;
    valkey.incr(valkeyKey).catch(k => logger.error(k));
}

const handlers: Record<string, (args: any) => Promise<any>> = {
    'volatility-query': (args) => handleVolatilityMessageV2(args as OptionsVolRequest),
    'options-stat-query': (args) => handleOptionsStatsMessageV2(args as OptionsStatsRequest),
    'dynamic-sql-query': (args) => handleDynamicSqlMessage(args as DynamicSqlRequest),
    'expected-move-query': (args) => handleExpectedMoveMessage(args as ExpectedMoveRequest),
    'ohlc-query': (args) => handleOhlcMessage(args as OhlcRequest),
};

async function initRedisSubscription() {
    try {
        logger.info(`Initializing Redis subscription on worker-request channel`);

        redisSubscriber = redisSubscriber.duplicate();
        await redisSubscriber.connect();

        await redisSubscriber.subscribe('worker-request', (message) => {
            try {
                consecutiveRedisFailures = 0;
                const { requestType, clientId, data: args } = JSON.parse(message);
                trackClient(clientId);

                logger.info(`Received message from Redis: ${JSON.stringify({ requestType, clientId })}`);

                const requestId = args?.requestId || 'unknown';
                const handler = handlers[requestType];

                if (!handler) {
                    logger.error(`Unknown request type: ${requestType}`);
                    publish(requestId, true, {});
                    return;
                }

                handler(args)
                    .then(rows => publish(requestId, false, rows))
                    .catch(error => {
                        logger.error({ err: error }, `Error processing ${requestType}`);
                        return publish(requestId, true, {});
                    });
            } catch (error) {
                logger.error({ err: error }, `Error processing message: ${message}`);
            }
        });

        logger.info(`Subscribed to Redis on worker-request channel`);
    } catch (error) {
        logger.error({ err: error }, `error occurred while initializing the redis subscription.`);
        Deno.exit(1);
    }
}

if (DEBUG_MODE) {
    logger.info(`Running in debug mode. Executing test query...`);

    const result = await handleOptionsStatsMessageV2({
        channel: 'test',
        lookbackDays: 180,
        requestId: crypto.randomUUID(),
        symbol: 'COIN'
    });

    logger.info(`Result: ${JSON.stringify(result)}`);
} else {
    const shutdown = () => {
        logger.info(`shutting down...`);
        redisSubscriber.quit();
        setTimeout(() => {
            Deno.exit(0);
        }, 100);

        pusherClient.disconnect();
        logger.info("will shut down in 100ms")
    }

    await initRedisSubscription();

    channel.bind('query-timeout', async (args: any) => {
        consecutiveRedisFailures++;
        logger.warn(`query timeout (${consecutiveRedisFailures}/5) received for event: ${JSON.stringify(args)}`);

        if (consecutiveRedisFailures >= 5) {
            logger.fatal(`Too many consecutive Redis failures. Restarting process...`);
            Deno.exit(1);
        }

        try {
            await redisSubscriber.unsubscribe('worker-request');
            await redisSubscriber.quit();
        } catch (error) {
            logger.error({ err: error }, `error occurred while disconnecting the redis subscription. Will continue new subscription`);
        }

        await initRedisSubscription();
    });

    await valkey.connect();

    Deno.addSignalListener("SIGTERM", shutdown);
    Deno.addSignalListener("SIGINT", shutdown);

    logger.info(`Worker is running...`);

    app.get('/',
        c => c.json({ "message": "hello" })
    ).get('/stats', async c => {
        const stats: Record<string, number> = {};

        for await (const key of valkey.scanIterator({ MATCH: 'track:*' })) {
            const count = await valkey.get(key);

            if (count !== null) {
                stats[key] = parseInt(count, 10);
            }
        }

        return c.json({
            description: "Active client request counts inside active windows",
            data: stats
        });
    });

    Deno.serve(app.fetch);
}