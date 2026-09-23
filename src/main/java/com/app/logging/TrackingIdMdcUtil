package com.novaflow.logging;

import org.slf4j.MDC;

/**
 * Plain thread-local MDC helpers for synchronous code paths -- servlet
 * filters, scheduled jobs, Kafka listener threads, batch steps.
 *
 * <p>In reactive (WebFlux/Reactor) pipelines, use {@link TrackingContext}
 * instead: MDC is thread-local and will not survive a thread hop caused by
 * {@code subscribeOn}/{@code publishOn}, whereas Reactor {@code Context}
 * does, and {@link ReactorMdcContextLifter} bridges it into MDC per signal
 * automatically.</p>
 */
public final class TrackingIdMdcUtil {

    private TrackingIdMdcUtil() {
    }

    /** Reads the resolved tracking ID from MDC for the current thread, or null if unset. */
    public static String get() {
        return MDC.get(TrackingIdKeys.MDC_KEY);
    }

    /** Puts a tracking ID into MDC for the current thread directly. Prefer {@link #resolveAndRun} where possible. */
    public static void set(String trackingId) {
        MDC.put(TrackingIdKeys.MDC_KEY, trackingId);
    }

    /** Removes the tracking ID from MDC for the current thread. */
    public static void clear() {
        MDC.remove(TrackingIdKeys.MDC_KEY);
    }

    /**
     * The one line most synchronous services need: resolves the tracking ID
     * (runNodeId &gt; correlationId &gt; generated), runs {@code action}
     * with it set in MDC, and always restores the prior MDC state
     * afterwards -- safe on pooled threads.
     *
     * <pre>{@code
     * TrackingIdMdcUtil.resolveAndRun(request.getRunNodeId(), request.getCorrelationId(), () -> {
     *     log.info("Starting workflow execution: {}", workflow.getName());
     *     // ... rest of the existing method body, unchanged
     * });
     * }</pre>
     */
    public static void resolveAndRun(String runNodeId, String correlationId, Runnable action) {
        String trackingId = TrackingIdResolver.resolve(runNodeId, correlationId);
        String previous = get();
        set(trackingId);
        try {
            action.run();
        } finally {
            if (previous != null) {
                set(previous);
            } else {
                clear();
            }
        }
    }
}
