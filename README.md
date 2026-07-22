![GitHub repo size](https://img.shields.io/github/repo-size/mnsrulz/mztrading-data)
![GitHub last commit](https://img.shields.io/github/last-commit/mnsrulz/mztrading-data)

# mztrading-data

Options market data backend — CBOE options chains, Greeks, GEX/DEX snapshots, OI anomaly detection.

## Architecture

**Two Netlify Edge Functions** (each under `netlify-apps/`) + **Cloudflare Workers** for static assets:

| App | Role |
|---|---|
| `mztradingdata` | Main REST API — options data, Greeks, exposure, pricing, OI anomaly (Hono, DuckDB-WASM) |
| `mzingest` | Request ingress — accepts queries, returns results via WebSocket polling (Pusher + Redis + Netlify Blobs) |

**Two standalone services:**

| Service | Role |
|---|---|
| `api/snapshot-cdn.ts` | Snapshot image CDN with caching (deprecated) |
| `api/worker.ts` | Background worker — processes queued analytics via DuckDB (Redis pub/sub + Pusher) |

**Data pipeline** — orchestrated via GitHub Actions `workflow_run` triggers:

```
tickers (daily symbol sync) — standalone, no downstream
cboe-options (download parquet) — triggered by third-party scheduler at 6AM EST weekdays via workflow_dispatch (GitHub Actions cron was unreliable)
  → cboe-options-consolidated (30-day rolling)
    → cboe-options-oi-anomaly (OI anomaly)
    → options-snapshot-v2 (GEX/DEX PNG)
    → parquet-files-cf (parquet file listing SSG + Cloudflare deploy)
```

Parquet files are stored as GitHub Releases. The API uses DuckDB-WASM at the edge.

## Directory structure

| Path | Purpose |
|---|---|
| `api/` | Endpoint handlers and application logic |
| `lib/` | Core libraries: DuckDB queries, CBOE fetcher, search, pricing, TA |
| `netlify-apps/` | Netlify app shells — each has its own `netlify.toml` |
| `data/` | CI-managed JSON summaries (do not edit directly) |
| `jobs/` | Python scripts for data processing, Deno scripts for orchestration |
| `.github/workflows/` | CI pipeline workflow definitions |

## Local dev

```
deno --allow-all --unstable-kv netlify-apps/mztradingdata/api.ts
```

No test, lint, or typecheck commands exist.

## Tech stack

- **Runtime:** Deno (URL-based ESM imports, no npm/node_modules)
- **Edge framework:** Hono
- **Data query:** DuckDB-WASM + DuckDB Node-API
- **Data format:** Parquet files (GitHub Releases)
- **Infra:** Netlify Edge Functions, Cloudflare Workers (static assets)
- **Messaging:** Redis, Pusher, Netlify Blobs
