## Why

The current request-reply pattern relies on Redis pub/sub for communication between mzingest and the worker. Redis pub/sub is fire-and-forget — if a worker is down or slow, messages are lost. For competing-consumer scenarios (multiple workers processing from a single queue with guaranteed delivery), we need a message broker with durable queues and manual acknowledgment.

CloudAMQP (managed RabbitMQ) provides this. This change introduces a new standalone Node.js Netlify Functions app that publishes requests to RabbitMQ and receives correlated replies using the Direct Reply-To pattern, while keeping the existing mzingest app intact.

## What Changes

- New Netlify Functions app `netlify-apps/mzamqp/` (Node.js + TypeScript)
- `POST /api/requests` — publishes request to RabbitMQ queue, waits for correlated reply via Direct Reply-To, returns result
- Single work queue (`mztrading.requests`) with competing consumers — only one worker processes each message
- Each function invocation creates a temporary AMQP connection, publishes, consumes the reply, and disconnects (fits within Netlify's 10s function timeout)
- Worker implementation is NOT part of this change — workers are separate processes that consume from the queue
- No changes to existing mzingest or worker code

## Capabilities

### New Capabilities

- `amqp-ingress`: HTTP endpoint that publishes requests to RabbitMQ and returns correlated replies via Direct Reply-To
- `amqp-client`: TypeScript wrapper around `amqplib` for connection management, queue declaration, and RPC call pattern

### Modified Capabilities

(None — no existing specs)

## Impact

- **New code**: `netlify-apps/mzamqp/` (api.ts, amqpClient.ts, netlify.toml, package.json, tsconfig.json)
- **New dependency**: `amqplib` (via npm)
- **Infrastructure**: CloudAMQP instance with `AMQP_URI` environment variable
- **Runtime**: Node.js 20+ (Netlify Functions, not Edge Functions)
- **No breaking changes**: Existing mzingest and worker remain untouched
- **Timeout constraint**: 10s Netlify function limit — workers must respond within this window
