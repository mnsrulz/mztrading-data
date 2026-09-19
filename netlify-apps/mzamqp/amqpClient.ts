import amqplib from "amqplib";

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
  connectionTimeout?: number;
}

export class AmqpClient {
  private connection: amqplib.Connection | null = null;
  private channel: amqplib.Channel | null = null;
  private readonly uri: string;
  private readonly queueName: string;
  private readonly socketTimeout: number;
  private readonly connectionTimeout: number;

  constructor(options: AmqpClientOptions = {}) {
    this.uri = options.uri || process.env.AMQP_URI || "";
    this.queueName = options.queueName || QUEUE_NAME;
    this.socketTimeout = options.socketTimeout || 5000;
    this.connectionTimeout = options.connectionTimeout || 5000;
  }

  async connect(): Promise<void> {
    if (!this.uri) {
      throw new Error("AMQP_URI environment variable is not set");
    }

    this.connection = await amqplib.connect(this.uri, {
      socket_options: {
        timeout: this.socketTimeout,
      },
      connection_timeout: this.connectionTimeout,
    });

    this.channel = await this.connection.createChannel();
    await this.channel.prefetch(1);
  }

  async declareQueue(): Promise<void> {
    if (!this.channel) {
      throw new Error("Channel not initialized. Call connect() first.");
    }

    await this.channel.assertQueue(this.queueName, { durable: true });
  }

  async publishRequest(
    requestType: string,
    payload: RequestPayload,
    correlationId: string
  ): Promise<void> {
    if (!this.channel) {
      throw new Error("Channel not initialized. Call connect() first.");
    }

    const message = Buffer.from(JSON.stringify(payload));

    this.channel.sendToQueue(this.queueName, message, {
      correlationId,
      replyTo: REPLY_QUEUE,
      contentType: "application/json",
      persistent: true,
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

      const onMessage = (msg: amqplib.ConsumeMessage | null) => {
        if (!msg) {
          clearTimeout(timeout);
          this.channel?.cancel(onMessage as unknown as string);
          reject(new Error("Consumer cancelled by server"));
          return;
        }

        if (msg.properties.correlationId === correlationId) {
          clearTimeout(timeout);
          this.channel?.cancel(onMessage as unknown as string);

          try {
            const reply = JSON.parse(msg.content.toString());
            resolve(reply);
          } catch {
            reject(new Error("Failed to parse reply message as JSON"));
          }
        }
      };

      this.channel!.consume(REPLY_QUEUE, onMessage, { noAck: true }).catch((err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  async close(): Promise<void> {
    try {
      if (this.channel) {
        await this.channel.close();
        this.channel = null;
      }
    } catch {
      // Channel may already be closed
    }

    try {
      if (this.connection) {
        await this.connection.close();
        this.connection = null;
      }
    } catch {
      // Connection may already be closed
    }
  }
}
