package org.grv.service.authpipeline;

import org.grv.model.*;
import org.grv.service.CallableToSupplierAdaptor;

import java.util.concurrent.*;


public class AuthorizationsPipeline {

    private final AccountLedger balanceStore;
    private final DailySpendTracker spendTracker;
    private final ExecutorService cpuPool;
    private final ExecutorService ioPool;

    private static final long FRAUD_TIMEOUT_MS = 300;

    public AuthorizationsPipeline(AccountLedger balanceStore, DailySpendTracker spendTracker, ExecutorService cpuPool, ExecutorService ioPool) {
        this.balanceStore = balanceStore;
        this.spendTracker = spendTracker;
        this.cpuPool = cpuPool;
        this.ioPool = ioPool;
    }

    public  CompletableFuture<Decision> authorize(TransactionRecord txnRecord) {

        long start = System.nanoTime();

        CompletableFuture<CheckResult> fraudFuture = CompletableFuture.supplyAsync(CallableToSupplierAdaptor.adapt(new FraudCheck(txnRecord))
                , cpuPool).orTimeout(FRAUD_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .exceptionally(ex -> {
                    if (unwrap(ex) instanceof TimeoutException) {
                        return new CheckResult("FraudCheck", false, DeclineReason.FRAUD_TIMEOUT);
                    }
                    throw new CompletionException(unwrap(ex));
                });

        CompletableFuture<CheckResult> balanceFuture = CompletableFuture.supplyAsync(CallableToSupplierAdaptor.adapt(new BalanceCheck(txnRecord,balanceStore)), ioPool);

        CompletableFuture<CheckResult> limitFuture = CompletableFuture.supplyAsync(CallableToSupplierAdaptor.adapt(new LimitCheck(txnRecord,spendTracker)),ioPool);

        return CompletableFuture.allOf(fraudFuture, balanceFuture, limitFuture)
                .thenApply(v -> decide(txnRecord, start, fraudFuture.join(), balanceFuture.join(), limitFuture.join()))
                .exceptionally(ex -> { //We are throwing any failure (e.g. TransientException from BalanceCheck) becomes a decline. No transaction is ever lost.
                    System.out.printf("txn=%d check failed: %s%n", txnRecord.id(), unwrap(ex));
                    return Decision.declined(txnRecord, DeclineReason.CHECK_ERROR, elapsedMs(start));
                });
    }

    private static Decision decide(TransactionRecord txn, long start, CheckResult... results) {
        long latencyMs = elapsedMs(start);
        for (CheckResult result : results) {
            if (!result.passed()) {
                return Decision.declined(txn, result.declineReason(), latencyMs);
            }
        }
        return Decision.approved(txn, latencyMs);
    }

    private static Throwable unwrap(Throwable ex) {
        while ((ex instanceof CompletionException || ex instanceof ExecutionException) && ex.getCause() != null) {
            ex = ex.getCause();
        }
        return ex;
    }

    private static long elapsedMs(long start) {
        return (System.nanoTime() - start) / 1_000_000;
    }
}
