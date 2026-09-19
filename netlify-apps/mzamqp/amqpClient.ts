import { AMQPClient as AmqpSocketClient, type AMQPMessage } from "@cloudamqp/amqp-client";

const QUEUE_NAME = "mztrading.requests";
const REPLY_QUEUE = "amq.rabbitmq.reply-to";

export interface RequestPayload {
  requestType: string;
  symbol: string;
  requestId: string;
  [key: string]: unknown;
}

export interface AmqpClientOptions {
  uri?: string;
  queueName?: string;
  socketTimeout?: number;
}

export class AmqpClient {
  private client: AmqpSocketClient | null = null;
  private channel: Awaited<ReturnType<AmqpSocketClient["channel"]>> | null = null;
  private readonly uri: string;
  private readonly queueName: string;
  private readonly socketTimeout: number;

  constructor(options: AmqpClientOptions = {}) {
    this.uri = options.uri || process.env.AMQP_URI || "";
    this.queueName = options.queueName || QUEUE_NAME;
    this.socketTimeout = options.socketTimeout || 5000;
  }

  async connect(): Promise<void> {
    if (!this.uri) {
      throw new Error("AMQP_URI environment variable is not set");
    }

    this.client = new AmqpSocketClient(this.uri);
    await this.client.connect();

    this.channel = await this.client.channel();
    await this.channel.basicQos(1);
  }

  async declareQueue(): Promise<void> {
    if (!this.channel) {
      throw new Error("Channel not initialized. Call connect() first.");
    }

    await this.channel.queueDeclare(this.queueName, { durable: true });
  }

  async publishRequest(
    requestType: string,
    payload: RequestPayload,
    correlationId: string
  ): Promise<void> {
    if (!this.channel) {
      throw new Error("Channel not initialized. Call connect() first.");
    }

    const message = JSON.stringify(payload);

    await this.channel.basicPublish("", this.queueName, message, {
      correlationId,
      replyTo: REPLY_QUEUE,
      contentType: "application/json",
      deliveryMode: 2,
    });
  }

  waitForReply(correlationId: string, timeoutMs: number = 8000): Promise<unknown> {
    if (!this.channel) {
      return Promise.reject(new Error("Channel not initialized. Call connect() first."));
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`Reply timeout after ${timeoutMs}ms for correlationId: ${correlationId}`));
      }, timeoutMs);

      const onMessage = async (msg: AMQPMessage) => {
        if (msg.properties?.correlationId === correlationId) {
          clearTimeout(timeout);
          try {
            await this.channel?.basicCancel(msg.consumerTag ?? "");
          } catch {
            // consumer may already be cancelled
          }

          try {
            const body = msg.bodyToString();
            if (!body) {
              reject(new Error("Empty reply body"));
              return;
            }
            const reply = JSON.parse(body);
            resolve(reply);
          } catch {
            reject(new Error("Failed to parse reply message as JSON"));
          }
        }
      };

      this.channel!.basicConsume(REPLY_QUEUE, { noAck: true }, onMessage).catch((err) => {
          clearTimeout(timeout);
          reject(err);
        });
    });
  }

  async close(): Promise<void> {
    try {
      if (this.channel && !this.channel.closed) {
        await this.channel.close();
        this.channel = null;
      }
    } catch {
      // Channel may already be closed
    }

    try {
      if (this.client && !this.client.closed) {
        await this.client.close();
        this.client = null;
      }
    } catch {
      // Connection may already be closed
    }
  }
}
