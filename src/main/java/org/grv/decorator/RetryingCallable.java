package org.grv.decorator;

import org.grv.config.TransientException;
import org.grv.matrics.Matrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class RetryingCallable<T> implements Callable<T> {

    private static final Logger log = LoggerFactory.getLogger(RetryingCallable.class);

    private final Callable<T> delegate;
    private final int maxAttempts;
    private final long baseBackoffMs;
    private final Matrics matrics;

    public RetryingCallable(Callable<T> delegate, int maxAttempts, long baseBackoffMs, Matrics matrics) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.delegate = delegate;
        this.maxAttempts = maxAttempts;
        this.baseBackoffMs = baseBackoffMs;
        this.matrics = matrics;
    }

    @Override
    public T call() throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                return delegate.call();
            } catch (TransientException e) {
                if (attempt == maxAttempts) {
                    throw e;
                }
                long backoff = backoffMs(attempt);
                matrics.recordRetried();
                log.info("retry {}/{} in {}ms: {}", attempt, maxAttempts - 1, backoff, e.getMessage());
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
    }

    private long backoffMs(int attempt) {
        long exponential = baseBackoffMs << (attempt - 1);
        long jitter = ThreadLocalRandom.current().nextLong(baseBackoffMs + 1);
        return exponential + jitter;
    }
}
