## ADDED Requirements

### Requirement: Connect to CloudAMQP using AMQP_URI environment variable
The system SHALL establish an AMQP connection using the `AMQP_URI` environment variable.

#### Scenario: AMQP_URI is set
- **WHEN** the `AMQP_URI` environment variable is present
- **THEN** the system connects to RabbitMQ at the specified URI

#### Scenario: AMQP_URI is missing
- **WHEN** the `AMQP_URI` environment variable is not set
- **THEN** the system throws an error on connection attempt

### Requirement: Declare the work queue
The system SHALL declare the `mztrading.requests` queue as durable.

#### Scenario: Queue declaration
- **WHEN** a channel is created
- **THEN** the system asserts the `mztrading.requests` queue with `durable: true`

### Requirement: Publish request with RPC properties
The system SHALL publish a message to the work queue with `correlationId` and `replyTo` properties set.

#### Scenario: Publish with correlation
- **WHEN** `publishRequest(requestType, payload, correlationId)` is called
- **THEN** the system publishes a JSON message to the `mztrading.requests` queue with `correlationId` set and `replyTo` set to `amq.rabbitmq.reply-to`

### Requirement: Consume reply with correlation matching
The system SHALL consume from `amq.rabbitmq.reply-to` in no-ack mode and resolve a promise when a message with matching `correlationId` arrives.

#### Scenario: Matching reply received
- **WHEN** `waitForReply(correlationId, timeoutMs)` is called and a reply with matching `correlationId` arrives before timeout
- **THEN** the system resolves the promise with the parsed JSON reply payload

#### Scenario: Timeout exceeded
- **WHEN** `waitForReply(correlationId, timeoutMs)` is called and no matching reply arrives within `timeoutMs`
- **THEN** the system rejects the promise with a timeout error

### Requirement: Connection and channel are closable
The system SHALL provide a `close()` method that closes both the channel and connection.

#### Scenario: Close after use
- **WHEN** `close()` is called
- **THEN** the AMQP channel and connection are closed
