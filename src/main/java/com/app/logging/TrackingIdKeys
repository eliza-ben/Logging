package com.novaflow.logging;

/**
 * Well-known keys used across the tracking-logging library.
 *
 * <p>Resolution rule, applied everywhere a tracking ID is derived
 * (see {@link TrackingIdResolver}): prefer {@code runNodeId} when present,
 * fall back to {@code correlationId}, otherwise generate a new one. The
 * resolved value is what actually goes into MDC / Reactor Context and
 * therefore into every log line.</p>
 */
public final class TrackingIdKeys {

    /** MDC / Reactor Context key holding the single resolved tracking ID. */
    public static final String MDC_KEY = "trackingId";

    /** Inbound header carrying an upstream-assigned run/node identifier (preferred source). */
    public static final String RUN_NODE_ID_HEADER = "X-Run-Node-Id";

    /** Inbound header carrying a plain correlation ID (fallback source). */
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    /** Outbound header the resolved tracking ID is echoed on, so callers can log/propagate it too. */
    public static final String TRACKING_ID_RESPONSE_HEADER = "X-Tracking-Id";

    private TrackingIdKeys() {
    }
}
