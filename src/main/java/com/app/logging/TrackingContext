package com.novaflow.logging;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Reactive counterpart to {@link TrackingIdMdcUtil}. This is the intended
 * integration point for reactive services: wrap the existing return
 * statement in {@link #attach} rather than adding manual MDC calls
 * anywhere in the chain. Every operator downstream -- on whatever thread
 * Reactor schedules it on -- will see the resolved tracking ID in MDC via
 * {@link ReactorMdcContextLifter}, with no further code changes needed.
 *
 * <p>Before:</p>
 * <pre>{@code
 * public Mono<WorkflowResult> execute(WorkflowRequest request) {
 *     String correlationId = StringUtils.hasText(request.getCorrelationId())
 *             ? request.getCorrelationId()
 *             : UUID.randomUUID().toString();
 *     log.info("Starting workflow execution: {} [{}] correlation: {}",
 *             workflow.getName(), workflow.getId(), correlationId);
 *     return doWork(request);
 * }
 * }</pre>
 *
 * <p>After (one line changed at the return statement, tracking ID now
 * consistent across every downstream log line, on every thread):</p>
 * <pre>{@code
 * public Mono<WorkflowResult> execute(WorkflowRequest request) {
 *     log.info("Starting workflow execution: {} [{}]", workflow.getName(), workflow.getId());
 *     return TrackingContext.attach(doWork(request), request.getRunNodeId(), request.getCorrelationId());
 * }
 * }</pre>
 *
 * <p>Note the first {@code log.info} call above still needs the ID set
 * synchronously if it runs before {@code attach} is applied -- prefer
 * moving such logging inside the {@code Mono} (e.g. {@code Mono.defer} or
 * {@code doFirst}) so it too benefits from the propagated context, or call
 * {@link TrackingIdMdcUtil#resolveAndRun} for that one line instead.</p>
 */
public final class TrackingContext {

    private TrackingContext() {
    }

    /** Builds a Reactor {@link Context} carrying the resolved tracking ID, for manual {@code contextWrite} use. */
    public static Context of(String runNodeId, String correlationId) {
        return Context.of(TrackingIdKeys.MDC_KEY, TrackingIdResolver.resolve(runNodeId, correlationId));
    }

    /** Builds a Reactor {@link Context} carrying an already-resolved tracking ID. */
    public static Context ofResolved(String trackingId) {
        return Context.of(TrackingIdKeys.MDC_KEY, trackingId);
    }

    /** Wraps a {@link Mono} so every downstream operator sees the resolved tracking ID in MDC. */
    public static <T> Mono<T> attach(Mono<T> mono, String runNodeId, String correlationId) {
        return mono.contextWrite(of(runNodeId, correlationId));
    }

    /** Wraps a {@link Flux} so every downstream operator sees the resolved tracking ID in MDC. */
    public static <T> Flux<T> attach(Flux<T> flux, String runNodeId, String correlationId) {
        return flux.contextWrite(of(runNodeId, correlationId));
    }

    /** Reads the tracking ID out of a Reactor {@link Context}, or null if not present. */
    public static String readFrom(Context context) {
        return context.getOrDefault(TrackingIdKeys.MDC_KEY, null);
    }
}
