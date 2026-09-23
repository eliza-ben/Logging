package com.novaflow.logging;

import org.springframework.util.StringUtils;

import java.util.UUID;

/**
 * Single source of truth for turning "whatever tracking identifiers this
 * request/message happened to arrive with" into one tracking ID, applied
 * identically by every service that pulls in this library.
 *
 * <p>Rule: {@code runNodeId} wins if present and non-blank; otherwise fall
 * back to {@code correlationId} if present and non-blank; otherwise
 * generate a new random ID. This keeps a single, consistent identifier in
 * every log line regardless of which upstream system originated the
 * request or what it happened to populate.</p>
 */
public final class TrackingIdResolver {

    private TrackingIdResolver() {
    }

    /**
     * Resolves the tracking ID per the standard precedence rule:
     * runNodeId &gt; correlationId &gt; generated UUID.
     */
    public static String resolve(String runNodeId, String correlationId) {
        if (StringUtils.hasText(runNodeId)) {
            return runNodeId;
        }
        if (StringUtils.hasText(correlationId)) {
            return correlationId;
        }
        return generate();
    }

    /** Generates a new random tracking ID. Exposed for callers that need to mint one explicitly. */
    public static String generate() {
        return UUID.randomUUID().toString();
    }
}
