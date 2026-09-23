package com.novaflow.logging;

import org.reactivestreams.Subscription;
import org.slf4j.MDC;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

/**
 * Bridges the tracking ID stored in Reactor's {@link Context} into SLF4J's
 * thread-local {@link MDC} for the duration of each signal ({@code onNext},
 * {@code onError}, {@code onComplete}), so every log statement anywhere in
 * a {@code Mono}/{@code Flux} chain -- regardless of which pooled worker
 * thread Reactor happens to schedule that signal on -- sees the correct
 * tracking ID.
 *
 * <p>Registered automatically by {@link TrackingLoggingAutoConfiguration}
 * in Spring Boot applications. Call {@link #install()} yourself only in
 * non-Spring or manual-wiring setups.</p>
 */
public final class ReactorMdcContextLifter {

    private static final String HOOK_KEY = ReactorMdcContextLifter.class.getName();

    private ReactorMdcContextLifter() {
    }

    /** Registers the hook globally. Idempotent -- safe to call more than once. */
    public static void install() {
        Hooks.onEachOperator(HOOK_KEY, Operators.lift((scannable, subscriber) -> new MdcContextSubscriber<>(subscriber)));
    }

    /** Removes the hook. Mainly useful in tests. */
    public static void uninstall() {
        Hooks.resetOnEachOperator(HOOK_KEY);
    }

    /**
     * Subscriber decorator that copies the tracking ID out of the Reactor
     * Context into MDC immediately before delegating each signal, and
     * restores whatever was in MDC before the signal ran afterwards, so a
     * value never leaks onto unrelated log statements sharing the same
     * pooled thread. Thread-safe: MDC itself is thread-local, and this
     * class holds no shared mutable state.
     */
    private static final class MdcContextSubscriber<T> implements CoreSubscriber<T> {

        private final CoreSubscriber<T> delegate;

        MdcContextSubscriber(CoreSubscriber<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public Context currentContext() {
            return delegate.currentContext();
        }

        @Override
        public void onSubscribe(Subscription s) {
            withMdc(() -> delegate.onSubscribe(s));
        }

        @Override
        public void onNext(T t) {
            withMdc(() -> delegate.onNext(t));
        }

        @Override
        public void onError(Throwable t) {
            withMdc(() -> delegate.onError(t));
        }

        @Override
        public void onComplete() {
            withMdc(delegate::onComplete);
        }

        private void withMdc(Runnable signal) {
            String trackingId = currentContext().getOrDefault(TrackingIdKeys.MDC_KEY, null);
            String previous = MDC.get(TrackingIdKeys.MDC_KEY);
            if (trackingId != null) {
                MDC.put(TrackingIdKeys.MDC_KEY, trackingId);
            }
            try {
                signal.run();
            } finally {
                if (previous != null) {
                    MDC.put(TrackingIdKeys.MDC_KEY, previous);
                } else {
                    MDC.remove(TrackingIdKeys.MDC_KEY);
                }
            }
        }
    }
}
