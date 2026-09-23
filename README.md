# tracking-logging

Standardized, MDC-based tracking-ID logging for Spring Boot WebFlux
services across the organization. One resolution rule, one MDC key, one
propagation mechanism -- every service that adopts it produces log lines
that line up the same way in Splunk.

## Resolution rule

```
trackingId = runNodeId   if present and non-blank
           = correlationId  else if present and non-blank
           = generated UUID  otherwise
```

Implemented once in `TrackingIdResolver`, used everywhere else in the
library. No service reimplements this logic.

## Design goals this meets

- **Automatic tracking identifier** -- `TrackingIdWebFilter` resolves it
  from headers (`X-Run-Node-Id`, `X-Correlation-Id`) for every inbound
  HTTP request with zero service code.
- **MDC-based context propagation, Spring Boot compatible, thread-safe** --
  `ReactorMdcContextLifter` copies the resolved ID from Reactor `Context`
  into SLF4J `MDC` around every signal in a reactive chain, so it is
  correct regardless of which pooled thread (Netty event loop,
  `boundedElastic`, etc.) Reactor uses. MDC itself is thread-local; the
  lifter saves/restores the prior value per signal so nothing leaks onto
  unrelated work sharing the same thread.
- **Minimal code changes in services** -- for header-driven tracking
  (the common case), *no* code changes at all: the autoconfiguration wires
  the filter and hook automatically once the dependency is added. For the
  case where `runNodeId` only becomes known after the request body is
  parsed (it's a field on the DTO, not a header), the change is one line
  at the existing return statement.

## Install

```bash
mvn install
```

```xml
<dependency>
    <groupId>com.novaflow</groupId>
    <artifactId>tracking-logging</artifactId>
    <version>1.0.0</version>
</dependency>
```

Copy `src/main/resources/logback-spring-sample.xml` to each service's
`src/main/resources/logback-spring.xml`, filling in the `application` name
in the JSON appender's `customFields`.

## Usage

### Case 1: tracking ID arrives as a header (most services, zero code)

Nothing to do. `TrackingIdWebFilter` reads `X-Run-Node-Id` /
`X-Correlation-Id`, resolves per the rule above, and every log line in the
request -- on any thread -- carries `trackingId` automatically.

### Case 2: runNodeId is a field on the request body

One line, at the point the service already returns its `Mono`/`Flux`:

```java
// Before
public Mono<WorkflowResult> execute(WorkflowRequest request) {
    String correlationId = StringUtils.hasText(request.getCorrelationId())
            ? request.getCorrelationId()
            : UUID.randomUUID().toString();
    log.info("Starting workflow execution: {} [{}] correlation: {}",
            workflow.getName(), workflow.getId(), correlationId);
    return doWork(request);
}

// After
public Mono<WorkflowResult> execute(WorkflowRequest request) {
    log.info("Starting workflow execution: {} [{}]", workflow.getName(), workflow.getId());
    return TrackingContext.attach(doWork(request), request.getRunNodeId(), request.getCorrelationId());
}
```

All manual MDC handling, UUID generation, and correlation-ID plumbing that
used to live in the service is deleted; `TrackingContext.attach` is the
only addition.

### Case 3: synchronous code (schedulers, Kafka listeners, batch jobs)

```java
TrackingIdMdcUtil.resolveAndRun(message.getRunNodeId(), message.getCorrelationId(), () -> {
    log.info("Processing message {}", message.getId());
    // ... existing method body, unchanged
});
```

## Logback pattern

```
%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] [%X{trackingId}] %-5level %logger{36} - %msg%n
```

or the JSON appender in `logback-spring-sample.xml`, which indexes
`trackingId` as a real Splunk field.

## Verifying in your own environment

Scaffolded and hand-reviewed here, but not compiled or test-run in this
sandbox: outbound access to Maven Central is blocked by the network
allow-list this ran under. Run `mvn test` in your own environment before
relying on it in production. `ReactorMdcContextLifterTest` checks the
precedence rule end-to-end across a real `subscribeOn` thread hop and
confirms no leakage between unrelated subscriptions sharing a thread;
`TrackingIdResolverTest` checks the precedence rule in isolation.
