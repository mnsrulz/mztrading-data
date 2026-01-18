import { io } from "https://esm.sh/socket.io-client@4.8.1";
import { z } from "https://esm.sh/zod@4.2.1";
import delay from "https://esm.sh/delay@7.0.0";
import pino from "https://esm.sh/pino@10.1.0";
import pretty from "https://esm.sh/pino-pretty@10.3.0";
const socketUrl = `https://mztrading-socket.deno.dev`
const DATA_DIR = Deno.env.get("DATA_DIR") || '/data/w2-output-flat';
const OHLC_DATA_DIR = Deno.env.get("OHLC_DIR") || '/data/ohlc';

import { DuckDBInstance } from "npm:@duckdb/node-api@1.4.3-r.2";

const stream = pretty({
    singleLine: true,
    colorize: true,
    include: "time,msg",
    messageFormat: (log, messageKey) => { return `${log[messageKey]}` },
});

const logger = pino({
    //level: "info" 
}, stream);


const socket = io(socketUrl, {
    reconnectionDelayMax: 10000,
    transports: ['websocket']
});

socket.on("connect", () => {
    logger.info(`Connected to the server! Socket ID: ${socket.id}`);

    socket.emit("register-worker", {});
    startWorker();
    logger.info("Client started, waiting for requests...");

});

socket.on("disconnect", () => {
    logger.info("disconnection");
});

socket.on("hello", (args) => {
    logger.info(`hello response: ${JSON.stringify(args)}`);
});

const WorkerRequestSchema = z.object({
    requestType: z.string(),
    requestId: z.uuid(),
    data: z.any()
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
                  SELECT DISTINCT dt, iv30/100  AS iv30, if(close > 0, close, LAG(close) OVER (PARTITION BY symbol ORDER BY dt)) AS close
                  FROM '${OHLC_DATA_DIR}/*.parquet' WHERE replace(symbol, '^', '') = '${symbol}'
                ), I AS (
                    SELECT DISTINCT opdata.dt, iv, option_type, option_symbol, expiration, strike, (bid + ask)/2 AS  mid_price, OHLC.close, OHLC.iv30,
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
                    array_agg(close ORDER BY dt) FILTER (WHERE option_type='C') AS close,
                    array_agg(iv30 ORDER BY dt) FILTER (WHERE option_type='C') AS iv30,
                    array_agg(iv ORDER BY dt) FILTER (WHERE option_type='C') AS cv,
                    array_agg(iv ORDER BY dt) FILTER (WHERE option_type='P') AS pv,
                    array_agg(mid_price ORDER BY dt) FILTER (WHERE option_type='C') AS cp,
                    array_agg(mid_price ORDER BY dt) FILTER (WHERE option_type='P') AS pp
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
            logger.info(`error occurred while processing request: ${err}`);
            hasError = true;
        }
        socket.emit(`worker-response`, {
            requestId: requestId,
            hasError,
            value: rows
        });

        logger.info(`Worker volatility request completed! ${JSON.stringify(args)}`, );

    } catch (error) {
        logger.info(`Error processing worker-volatility-request: ${JSON.stringify(error)}`);
    }
};

let abortController = new AbortController();
socket.on("worker-notification", () => {
    logger.info("Worker notification received.");
    abortController.abort();
});

socket.on("register-worker-success", a => { logger.info(`worker registration succeeded, : ${JSON.stringify(a)}`) })

socket.on("reconnect_attempt", (attempt) => {
    logger.info(`Reconnection attempt #${attempt}`);
});

socket.on("reconnect", () => {
    logger.info(`Reconnected successfully! Socket ID: ${socket.id}`);
    socket.emit("register-worker", {});
});

let isWorkerStarted = false;
async function startWorker() {
    if (isWorkerStarted) return;
    isWorkerStarted = true;
    while (true) {
        try {
            logger.info("Requesting work item from server...");
            const item = await socket.timeout(3000).emitWithAck("receive-message");
            if (item) {
                const parsed = WorkerRequestSchema.parse(item); //will add more handlers here later
                if (parsed.requestType === "volatility-query") {
                    await handleVolatilityMessage({ ...parsed.data, requestId: parsed.requestId });
                    continue;   //let's ask another item right away
                } else {
                    logger.info(`Unknown request type: ${parsed.requestType}`);
                }
            } else {
                logger.info("No work items available, waiting for notification...");
            }
        } catch (error) {
            console.error("Error handling worker request:", error);
        }

        //may be we will explore async events later, but for now let's use abort controller signal.
        await delay(30000, { signal: abortController.signal }).catch(() => { logger.info('signal must have aborted this') });
        abortController = new AbortController();
    }
}

function shutdown() {
    logger.info(`shutting down...`);

    socket.removeAllListeners();
    socket.disconnect();

    // Give socket.io time to close cleanly
    setTimeout(() => {
        Deno.exit(0);
    }, 100);

    logger.info("will shut down in 100ms")
}

Deno.addSignalListener("SIGTERM", shutdown);
Deno.addSignalListener("SIGINT", shutdown);