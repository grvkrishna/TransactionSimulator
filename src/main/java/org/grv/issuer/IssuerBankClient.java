package org.grv.issuer;

import org.grv.model.Decision;
import org.grv.model.DeclineReason;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IssuerBankClient {


    private static final int MAX_CONCURRENT_CALLS = 5;
    private static final long PERMIT_TIMEOUT_MS = 500;
    private static final long BANK_CALL_MS = 100;

    private final Semaphore permits = new Semaphore(MAX_CONCURRENT_CALLS, true);
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();


    public Decision issuerAuthorizations(Decision decision) {
        boolean acquired;
        try {
            acquired = permits.tryAcquire(PERMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return decline(decision);
        }

        if (!acquired) {
            return decline(decision);
        }

        try {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);

            Thread.sleep(BANK_CALL_MS);
            return decision;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return decline(decision);
        } finally {
            inFlight.decrementAndGet();
            permits.release();
        }
    }

    private static Decision decline(Decision decision) {
        return Decision.declined(decision.txnrcd(), DeclineReason.ISSUER_UNAVAILABLE, decision.latencyMs());
    }

    public int inUse()        { return MAX_CONCURRENT_CALLS - permits.availablePermits(); }
    public int waiting()      { return permits.getQueueLength(); }
    public int maxInFlight()  { return maxInFlight.get(); }
    public int capacity()     { return MAX_CONCURRENT_CALLS; }
}
