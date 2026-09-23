package com.novaflow.logging;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

/**
 * Autoconfiguration that makes this library "minimal code changes" in
 * practice: adding the dependency is enough for a standard WebFlux service
 * to get automatic tracking-ID resolution and thread-safe MDC propagation,
 * with no {@code @Bean} definitions or manual hook registration required.
 *
 * <p>On the classpath, this:</p>
 * <ul>
 *   <li>registers the {@link ReactorMdcContextLifter} hook at startup, so
 *       MDC propagation works across every reactive chain in the
 *       application, not just HTTP-triggered ones; and</li>
 *   <li>registers {@link TrackingIdWebFilter} for reactive web
 *       applications, so every inbound HTTP request gets a resolved
 *       tracking ID automatically from its headers.</li>
 * </ul>
 *
 * <p>Both can be overridden or disabled by defining your own bean of the
 * same type, or excluded via
 * {@code @SpringBootApplication(exclude = TrackingLoggingAutoConfiguration.class)}.</p>
 */
@Configuration
@ConditionalOnClass(Mono.class)
public class TrackingLoggingAutoConfiguration {

    @PostConstruct
    public void installReactorHook() {
        ReactorMdcContextLifter.install();
    }

    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    @ConditionalOnMissingBean
    public TrackingIdWebFilter trackingIdWebFilter() {
        return new TrackingIdWebFilter();
    }
}
