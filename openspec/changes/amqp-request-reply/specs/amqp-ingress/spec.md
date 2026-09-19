## ADDED Requirements

### Requirement: HTTP endpoint accepts request payloads
The system SHALL expose a `POST /api/requests` endpoint that accepts JSON request payloads.

#### Scenario: Valid request payload
- **WHEN** a client sends a POST to `/api/requests` with a JSON body containing `requestType`, `symbol`, and `requestId`
- **THEN** the system accepts the request and returns a JSON response with the result

#### Scenario: Missing or invalid payload
- **WHEN** a client sends a POST to `/api/requests` with an empty or malformed JSON body
- **THEN** the system returns HTTP 400 with an error message

### Requirement: Request is published to RabbitMQ work queue
The system SHALL publish the incoming request to the `mztrading.requests` queue via the default exchange using the request's `requestType` as the routing key.

#### Scenario: Successful publish
- **WHEN** a valid request is received
- **THEN** the system publishes a message to the `mztrading.requests` queue with `correlationId` set to the request's `requestId` and `replyTo` set to `amq.rabbitmq.reply-to`

#### Scenario: Queue does not exist yet
- **WHEN** a request is published and the `mztrading.requests` queue has not been declared
- **THEN** the system declares the queue with `durable: true` before publishing

### Requirement: Reply is consumed via Direct Reply-To
The system SHALL consume from the `amq.rabbitmq.reply-to` pseudo-queue in no-ack mode and match replies by `correlationId`.

#### Scenario: Reply received within timeout
- **WHEN** a request is published and a reply with matching `correlationId` is received within 8 seconds
- **THEN** the system returns the reply payload as JSON with HTTP 200

#### Scenario: Reply not received within timeout
- **WHEN** a request is published but no reply is received within 8 seconds
- **THEN** the system returns HTTP 504 with a timeout error message

### Requirement: AMQP connection is cleaned up after each invocation
The system SHALL close the AMQP connection and channel in a `finally` block after each function invocation, regardless of success or failure.

#### Scenario: Successful invocation
- **WHEN** a request completes successfully
- **THEN** the AMQP connection and channel are closed before the response is returned

#### Scenario: Invocation fails
- **WHEN** a request fails with an error
- **THEN** the AMQP connection and channel are closed before the error response is returned

### Requirement: CORS headers are set
The system SHALL respond to OPTIONS requests with appropriate CORS headers and allow cross-origin requests to `POST /api/requests`.

#### Scenario: Preflight request
- **WHEN** a client sends an OPTIONS request to `/api/requests`
- **THEN** the system responds with CORS headers allowing the request

#### Scenario: Cross-origin POST
- **WHEN** a client sends a cross-origin POST to `/api/requests`
- **THEN** the system processes the request normally
