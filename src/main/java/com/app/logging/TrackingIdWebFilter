package com.novaflow.logging;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * WebFlux filter that resolves a tracking ID for every request using the
 * standard precedence rule (see {@link TrackingIdResolver}) from the
 * {@code X-Run-Node-Id} and {@code X-Correlation-Id} headers, echoes the
 * resolved value on the response, and seeds it into the Reactor
 * {@link reactor.util.context.Context} so {@link ReactorMdcContextLifter}
 * can surface it in MDC for every log line in the request.
 *
 * <p>This covers services entirely automatically when the tracking
 * identifiers arrive as headers -- no code changes needed in the service
 * at all. When a runNodeId only becomes known after the request body is
 * deserialized (e.g. it's a field on the request DTO, not a header), use
 * {@link TrackingContext#attach} at the point the service already returns
 * its {@code Mono}/{@code Flux} instead; that one-line change takes
 * precedence in that request (see {@link TrackingContext}).</p>
 *
 * <p>Runs first ({@link Ordered#HIGHEST_PRECEDENCE}) so every subsequent
 * filter and handler sees the ID.</p>
 */
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TrackingIdWebFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();

        String runNodeId = request.getHeaders().getFirst(TrackingIdKeys.RUN_NODE_ID_HEADER);
        String correlationId = request.getHeaders().getFirst(TrackingIdKeys.CORRELATION_ID_HEADER);

        String trackingId = TrackingIdResolver.resolve(runNodeId, correlationId);

        exchange.getResponse()
                .getHeaders()
                .set(TrackingIdKeys.TRACKING_ID_RESPONSE_HEADER, trackingId);

        return chain.filter(exchange)
                .contextWrite(TrackingContext.ofResolved(trackingId));
    }
}
