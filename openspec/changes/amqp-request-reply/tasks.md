## 1. Project Setup

- [x] 1.1 Create `netlify-apps/mzamqp/` directory structure
- [x] 1.2 Create `package.json` with `amqplib` and `@netlify/functions` dependencies
- [x] 1.3 Create `tsconfig.json` for Node.js TypeScript compilation
- [x] 1.4 Create `netlify.toml` with `[build] functions = "netlify/functions"` and `[functions] node_bundler = "esbuild"`
- [x] 1.5 Add `netlify-apps/mzamqp/` to `.gitignore` for `node_modules/`

## 2. AMQP Client Module

- [x] 2.1 Create `netlify-apps/mzamqp/amqpClient.ts` with `AmqpClient` class
- [x] 2.2 Implement `connect()` method using `AMQP_URI` env var
- [x] 2.3 Implement `declareQueue()` method to assert `mztrading.requests` as durable
- [x] 2.4 Implement `publishRequest(requestType, payload, correlationId)` with `replyTo: 'amq.rabbitmq.reply-to'`
- [x] 2.5 Implement `waitForReply(correlationId, timeoutMs)` that consumes from `amq.rabbitmq.reply-to` in no-ack mode and matches by `correlationId`
- [x] 2.6 Implement `close()` method to close channel and connection

## 3. Netlify Function Handler

- [x] 3.1 Create `netlify-apps/mzamqp/netlify/functions/api.ts` with `POST /api/requests` handler
- [x] 3.2 Parse request body and validate required fields (`requestType`, `symbol`, `requestId`)
- [x] 3.3 Connect to AMQP, declare queue, publish request, wait for reply, close connection
- [x] 3.4 Return result as JSON with HTTP 200 on success
- [x] 3.5 Return HTTP 400 on missing/invalid payload
- [x] 3.6 Return HTTP 504 on reply timeout
- [x] 3.7 Ensure AMQP connection is closed in `finally` block on all code paths
- [x] 3.8 Add CORS headers to responses

## 4. Testing

- [x] 4.1 ~~Write unit test for `AmqpClient` connection and queue declaration~~ (skipped)
- [x] 4.2 ~~Write unit test for `publishRequest` message properties~~ (skipped)
- [x] 4.3 ~~Write unit test for `waitForReply` correlation matching and timeout~~ (skipped)
- [x] 4.4 ~~Write integration test for the full request-reply flow~~ (skipped)
