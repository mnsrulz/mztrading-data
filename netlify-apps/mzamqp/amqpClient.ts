import { AMQPSession, type AMQPMessage } from "@cloudamqp/amqp-client";

const QUEUE_NAME = "mztrading.requests";

export interface RequestPayload {
  requestType: string;
  symbol: string;
  requestId: string;
  [key: string]: unknown;
}

export interface AmqpClientOptions {
  uri?: string;
  queueName?: string;
}

export class AmqpClient {
  private session: AMQPSession | null = null;
  private rpcClient: Awaited<ReturnType<AMQPSession["rpcClient"]>> | null = null;
  private readonly uri: string;
  private readonly queueName: string;

  constructor(options: AmqpClientOptions = {}) {
    this.uri = options.uri || process.env.AMQP_URI || "";
    this.queueName = options.queueName || QUEUE_NAME;
  }

  async connect(): Promise<void> {
    if (!this.uri) {
      throw new Error("AMQP_URI environment variable is not set");
    }

    this.session = await AMQPSession.connect(this.uri);
    this.rpcClient = await this.session.rpcClient();
  }

  async declareQueue(): Promise<void> {
    if (!this.session) {
      throw new Error("Session not initialized. Call connect() first.");
    }

    await this.session.queue(this.queueName, { durable: true });
  }

  async rpc(
    requestType: string,
    payload: RequestPayload,
    correlationId: string,
    timeoutMs: number = 8000
  ): Promise<unknown> {
    if (!this.rpcClient) {
      throw new Error("RPC client not initialized. Call connect() first.");
    }

    const message = JSON.stringify(payload);
    const reply: AMQPMessage = await this.rpcClient.call(this.queueName, message, {
      timeout: timeoutMs,
      correlationId,
      contentType: "application/json",
      deliveryMode: 2,
    });

    const body = reply.bodyToString();
    if (!body) {
      throw new Error("Empty reply body");
    }

    return JSON.parse(body);
  }

  async close(): Promise<void> {
    try {
      if (this.rpcClient) {
        await this.rpcClient.close();
        this.rpcClient = null;
      }
    } catch {
      // may already be closed
    }

    try {
      if (this.session && !this.session.closed) {
        await this.session.stop();
        this.session = null;
      }
    } catch {
      // may already be closed
    }
  }
}
