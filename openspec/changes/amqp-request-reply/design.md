## Context

The mztrading-data platform uses a request-reply pattern between the HTTP ingress (mzingest) and background workers (api/worker.ts). Currently this uses Redis pub/sub + Pusher for timeouts. The system needs a competing-consumer pattern where multiple workers share a single work queue with guaranteed delivery.

This design introduces a new Node.js Netlify Functions app (`mzamqp`) that acts as an AMQP RPC client — publishing requests to RabbitMQ and receiving correlated replies. Workers (separate processes) will consume from the queue. This app does NOT implement the worker side.

**Constraints:**
- Netlify Functions have a 10-second execution timeout
- CloudAMQP provides managed RabbitMQ (AMQP 0-9-1)
- Must not modify existing mzingest or worker code
- Node.js runtime (Deno lacks mature AMQP client support)

## Goals / Non-Goals

**Goals:**
- Provide an HTTP endpoint (`POST /api/requests`) that publishes requests to a RabbitMQ work queue and returns correlated replies
- Use RabbitMQ Direct Reply-To for efficient RPC without per-request reply queues
- Support competing consumers — multiple workers can process from the same queue, each message processed exactly once
- Fit within Netlify's 10-second function timeout
- Reuse existing request/response payload shapes from mzingest

**Non-Goals:**
- Worker implementation (consumers are separate processes)
- Replacing or modifying the existing mzingest app
- Multi-type request routing (single queue, no exchange routing)
- Message persistence or dead-letter handling (can be added later)
- Pusher-based timeout notifications (replaced by AMQP message TTL)

## Decisions

### Decision 1: Direct Reply-To over dedicated reply queues

**Choice:** Use RabbitMQ's `amq.rabbitmq.reply-to` pseudo-queue for replies.

**Alternatives considered:**
- *Exclusive reply queue per request:* Creates and deletes a queue per invocation. High overhead, wasteful.
- *Shared named reply queue with correlation:* Requires a long-lived consumer to demultiplex replies. Doesn't fit serverless model.
- *External store (Redis/Blobs) for replies:* Adds another dependency and polling latency.

**Rationale:** Direct Reply-To avoids queue declaration overhead. The function consumes from `amq.rabbitmq.reply-to` in no-ack mode, publishes the request with `replyTo: 'amq.rabbitmq.reply-to'`, and matches the reply by `correlationId`. No extra queues to manage.

### Decision 2: Single work queue over exchange routing

**Choice:** Publish directly to a named queue (`mztrading.requests`) via the default exchange.

**Alternatives considered:**
- *Topic exchange with routing keys:* Adds complexity for fan-out by request type. Not needed — workers compete for all messages.
- *Fanout exchange:* All workers get all messages. No competing consumer semantics.

**Rationale:** A single queue with competing consumers is the simplest model for "exactly one worker processes each message." RabbitMQ round-robins dispatch to consumers. Workers set `prefetch: 1` for fair distribution.

### Decision 3: Per-invocation AMQP connection

**Choice:** Each function invocation creates a new AMQP connection and channel, publishes, waits for reply, then closes.

**Alternatives considered:**
- *Persistent connection across invocations:* Not possible — Netlify serverless functions are stateless; each invocation may land on a different container.
- *Connection pooling:* Overhead of maintaining pool state across stateless invocations. Not worth it for 10s lifecycle.

**Rationale:** AMQP connection setup is ~100-200ms on CloudAMQP. Fits within the 10s budget. The connection is closed in a `finally` block to prevent resource leaks.

### Decision 4: Node.js runtime (not Deno Edge Functions)

**Choice:** Deploy as Netlify Functions (Node.js 20+) in `netlify/functions/`.

**Alternatives considered:**
- *Deno Edge Functions:* `amqplib` uses native TCP sockets, which Deno Edge Functions don't support (V8 isolates, no `net` module).
- *Deno Deploy:* Same constraint — no raw TCP for AMQP.

**Rationale:** `amqplib` requires Node.js TCP socket access. Netlify Functions run on AWS Lambda with full Node.js runtime. Cold starts (~200ms-1s) are acceptable for this use case.

### Decision 5: 10s timeout with fast-fail

**Choice:** Set `socket_timeout` and `connection_timeout` on the AMQP client to 5s. The function returns a 504 if no reply is received within 8s (leaving 2s buffer for connection setup/teardown).

**Rationale:** Workers must respond within the window. A 504 with clear error message is better than a silent Netlify timeout. Workers that can't respond in time should be scaled horizontally or optimized.

## Risks / Trade-offs

- **[Cold start latency]** → Netlify Functions cold start (200ms-1s) adds to request latency. Mitigation: CloudAMQP connection reuse is impossible in serverless, but connection setup is fast. Warm invocations benefit from Lambda container reuse.
- **[10s timeout]** → Workers that take >8s to respond will cause 504 errors. Mitigation: Monitor function logs for timeouts; optimize worker processing or increase prefetch.
- **[Connection churn]** → Each invocation opens/closes an AMQP connection. Mitigation: CloudAMQP handles connection bursts well. Connection limits per plan should be monitored.
- **[No message persistence]** → If the function crashes after publishing but before consuming the reply, the request is lost. Mitigation: Client can retry. Adding persistent messages and dead-letter queues is a future enhancement.
- **[Direct Reply-To limitations]** → Replies are not fault-tolerant — if the client disconnects, the reply is dropped. Mitigation: Client retries on timeout. Acceptable for this use case.
