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
| `netlify-apps/mztradingparquetfiles/api.ts` | Parquet file explorer (Preact SSG) | Netlify Edge Function |
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
- `netlify-apps/mztradingparquetfiles/` — imports `api/parquet-files.tsx`, SSG build
- `netlify-apps/mzingest/` — standalone Hono app with Pusher + Redis + Netlify Blobs

## SQL injection warning

`lib/historicalOptions.ts` builds SQL via string interpolation on user-supplied symbols and dates. Not parameterized. Review before adding new query functions.

## Conventions

- All application code lives in `lib/`, `api/`, or `netlify-apps/`
- Do not edit files in `data/` directly — they are CI-managed and overwritten
- `data/cboe-options-rolling.json` is the live config — change URLs there if storage moves
- When adding new parquet-based endpoints, follow the pattern in `historicalOptions.ts` (`registerFileBuffer` + `conn.send`)
- Job scripts in `jobs/` use Python for data processing, Deno for orchestration
