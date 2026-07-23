# mztrading-data

Deno + Python repo for options market data — CBOE options chains, Greeks, GEX/DEX snapshots, OI anomaly detection.

## Runtime

- **Deno v1.x / v2.x** — main API, workers, and TS jobs
- **Python 3.9–3.14** — data processing jobs (pandas, pyarrow, duckdb, requests)

No npm/node_modules in prod. All imports are URL-based ESM.

## Entrypoints

| File | Purpose | Deployed To |
|---|---|---|
| `netlify-apps/mztradingdata/api.ts` | Main REST API (24 endpoints) | Netlify Edge Function |
| `netlify-apps/mzingest/api.ts` | Request ingress + response polling (Pusher + Upstash Redis + Netlify Blobs) | Netlify Edge Function |
| `api/snapshot-cdn.ts` | Snapshot image CDN with caching (deprecated) | — |
| `api/worker.ts` | Background worker (Redis + DuckDB Node-API + Pusher) | Standalone server |

## Local dev

```
deno --allow-all --unstable-kv netlify-apps/mztradingdata/api.ts
```

No test, lint, or typecheck commands exist.

## CI pipeline (GitHub Actions)

Workflows form a directed chain via `workflow_run` triggers:

```
tickers (daily CBOE symbol sync) — independent, no downstream trigger
  (no link — manual only)
cboe-options (download daily parquet from CBOE) — invoked via workflow_dispatch by a third-party scheduler at 6AM EST every weekday (GitHub Actions cron was unreliable)
  → cboe-options-consolidated (30-day rolling parquet consolidation)
    → cboe-options-oi-anomaly (OI anomaly detection)
    → options-snapshot-v2 (GEX/DEX PNG snapshot generation)
    → parquet-files-cf (parquet file listing SSG + Cloudflare deploy)
```

Netlify deploys happen automatically via Git integration (connected to `netlify-apps/` subdirectories). All pipeline steps can be manually triggered via `workflow_dispatch`.

## Data architecture

- Parquet files are hosted as **GitHub Releases** with tag pattern `DATA_YYYY-MM-DD`
- `data/cboe-options-rolling.json` is the single source of truth for current parquet URLs — update this if storage changes
- The API uses **DuckDB-WASM** (`@duckdb/duckdb-wasm`) to query parquet files at the edge
- Static JSON summaries in `data/` are committed to the repo (CI-managed)

## Key lib files

| File | Role |
|---|---|
| `lib/historicalOptions.ts` | DuckDB-WASM queries for Greeks, exposure, OI anomaly |
| `lib/cboe.ts` | Live CBOE options chain fetcher (Deno KV cache for index symbols) |
| `lib/data.ts` | Static data loader, Fuse.js search, snapshot URL generation |
| `lib/oianomalySearchClient.ts` | Algolia-compatible faceted search over DuckDB |
| `lib/historicalPrice.ts` | Stock price fetcher from Cloudflare Worker |
| `api/worker.ts` | Redis pub/sub background worker using Node-API DuckDB bindings |

## Netlify apps structure

Each app under `netlify-apps/` has its own `netlify.toml`:
- `netlify-apps/mztradingdata/` — imports `api/api.ts`, builds via `deno bundle`
- `netlify-apps/mzingest/` — standalone Hono app with Pusher + Redis + Netlify Blobs

## Worker (`api/worker.ts`)

Background worker that listens for Redis pub/sub messages, processes options market data via DuckDB (Node-API bindings), and publishes results back through Redis + mzingest HTTP API.

### Request types (Redis channel `worker-request`)

| Event | Handler | Schema |
|---|---|---|
| `volatility-query` | `handleVolatilityMessageV2` | symbol + expiry mode (fixed/rolling/dte) + mode (delta/strike/atm) + lookback |
| `options-stat-query` | `handleOptionsStatsMessage` | symbol + lookback → OI, delta, premium by option type |
| `expected-move-query` | `handleExpectedMoveMessage` | symbol + expiryMode (weekly/monthly) + lookback → ATM straddle price |
| `ohlc-query` | `handleOhlcMessage` | symbol + lookback → OHLC + iv30 |
| `dynamic-sql-query` | `handleDynamicSqlMessage` | symbol + raw SQL → arbitrary query results |

### Response flow

Results are written simultaneously via two paths:
1. **Redis** — keyed by `requestId` with 60s TTL (`publishToRedis`)
2. **HTTP PUT** — to `mzingest` at `requests/{requestId}/result` (`publishToMzIngest`)

Both use `p-retry` (3 retries). In `DEBUG_MODE=1` mode, publishes are skipped.

### Architecture

```
Redis (worker-request channel)
  └─ initRedisSubscription() → emitter.emit()
       ├─ volatility-query  → handleVolatilityMessageV2
       ├─ options-stat-query → handleOptionsStatsMessage
       ├─ expected-move-query → handleExpectedMoveMessage
       ├─ ohlc-query        → handleOhlcMessage
       └─ dynamic-sql-query → handleDynamicSqlMessage
              └─ executeReaderInternal(symbol, sql)
                   └─ DuckDB (in-memory, Node-API)
                        ├─ OHLC parquet: ${OHLC_DATA_DIR}/*.parquet
                        └─ Options parquet: ${DATA_DIR}/symbol=${symbol}/*.parquet
```

### Shared CTE (`baseQueryCte` in `executeReaderInternal`)

Builds a `dataset` view with these computed columns on every query:

| Column | Derivation |
|---|---|
| `mid_price` | `(bid + ask) / 2` |
| `dte` | `DATE_DIFF('day', quote_date, expiration)` |
| `strike_distance` / `strike_distance_pct` | `abs(strike - underlying_close)` |
| `moneyness` | ATM/ITM/OTM based on strike vs close and atm_rank |
| `moneyness_percent` | Signed % distance from spot |
| `liquidity_tier` | HIGH (>1k OI + >100 vol), MEDIUM (>100 OI), LOW |
| `volume_oi_ratio` | `volume / NULLIF(open_interest, 0)` |
| `expiry_bucket` | 0-7D / 7-30D / 30-90D / 90D+ |
| `is_weekly_expiration` | 1 if expiration is last of its week |
| `is_monthly_expiration` | 1 if weekly expiry lands on 15th–21st |
| `underlying_iv30` | Last non-zero iv30 carried forward (`IGNORE NULLS`) |
| `underlying_iv30_percentile` | `PERCENT_RANK()` over iv30 × 100 |

### Resilience

- **Redis reconnect**: exponential backoff up to 30s
- **Consecutive failure counter**: tracked via `consecutiveRedisFailures`; after 5 `query-timeout` Pusher events, process exits (`Deno.exit(1)`) → supervisor restarts
- **Query timeouts**: Pusher `query-timeout` event triggers Redis resubscription cycle
- **Graceful shutdown**: SIGTERM/SIGINT handlers quit Redis and exit after 100ms
- **`p-retry`**: 3 retries on publish failures

### Valkey rate tracking

Tracks per-client request counts via `track:{clientId}:{YYYY-MM-DDTHH:mm}` keys, exposed at HTTP `GET /stats`.

### Known issues

- **SQL injection**: `symbol`, `sql`, and date values are string-interpolated into queries (same as `lib/historicalOptions.ts`)
- **Unused imports**: `PusherJS` imported but only used for `query-timeout` binding on the Pusher channel (edge case)
- **`valkey` client**: no reconnect strategy — if initial `.connect()` at line 877 fails, process crashes
- **`BigInt.prototype.toJSON`**: global monkey-patch
- **`DisposableStack` risk**: `connection.closeSync()` via `stack.defer()` may throw if connection is already closed
- **Dead code**: old `handleVolatilityMessage` commented out (lines 197–285), old `publishInternal` Redis pub path commented
- **`DATA_DIR` constant references flat file pattern** but the CTE uses Hive-partitioned paths (`symbol=${symbol}`); verify if legacy code paths still rely on flat parquet layout

### Environemnt variables

| Var | Default | Purpose |
|---|---|---|
| `DATA_DIR` | `temp/w2-output` | Options parquet directory |
| `OHLC_DIR` | `temp/ohlc` | OHLC parquet directory |
| `REDIS_URI` | — | Redis connection for pub/sub |
| `VALKEY_URI` | — | Valkey connection for rate tracking |
| `PUSHER_APP_KEY` | — | Pusher for `query-timeout` events |
| `PUSHER_APP_CLUSTER` | `us2` | Pusher cluster |
| `PUSHER_CHANNEL_NAME` | `mztrading-channel` | Pusher channel name |
| `MZINGEST_BASE_URI` | `https://mzingest.netlify.app/api` | mzingest API base |
| `LOG_LEVEL` | `info` | Pino log level |
| `LOGTAIL_TOKEN` | — | Better Stack log tailing |
| `DEBUG_MODE` | `0` | Set to `1` to run a test query and skip Redis/Pusher init |

### Local dev

```bash
DEBUG_MODE=1 deno --allow-all api/worker.ts
```

In `DEBUG_MODE=1`, you must manually construct the function call to test a specific handler. The current code calls `handleOptionsStatsMessage` by default — edit `api/worker.ts` to uncomment or swap the call to test a different handler (e.g., `handleVolatilityMessageV2`, `handleOhlcMessage`, `executeReaderInternal`).

## Conventions

- All application code lives in `lib/`, `api/`, or `netlify-apps/`
- Do not edit files in `data/` directly — they are CI-managed and overwritten
- `data/cboe-options-rolling.json` is the live config — change URLs there if storage moves
- When adding new parquet-based endpoints, follow the pattern in `historicalOptions.ts` (`registerFileBuffer` + `conn.send`)
- Job scripts in `jobs/` use Python for data processing, Deno for orchestration
- **Always ask the user before committing** — never commit without explicit confirmation
