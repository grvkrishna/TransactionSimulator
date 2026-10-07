package org.grv.service.authpipeline;


import org.grv.audit.AuditLogger;
import org.grv.matrics.Matrics;
import org.grv.model.*;
import org.grv.service.CallableToSupplierAdaptor;

import java.util.concurrent.*;


public class AuthorizationsPipeline {

    private final AccountLedger balanceStore;
    private final DailySpendTracker spendTracker;
    private final ExecutorService cpuPool;
    private final ExecutorService ioPool;
    private final AuditLogger auditLogger;
    private final Matrics matrics;
    private static final long FRAUD_TIMEOUT_MS = 300;

    public AuthorizationsPipeline(AccountLedger balanceStore, DailySpendTracker spendTracker, ExecutorService cpuPool, ExecutorService ioPool, AuditLogger auditLogger, Matrics matrics) {
        this.balanceStore = balanceStore;
        this.spendTracker = spendTracker;
        this.cpuPool = cpuPool;
        this.ioPool = ioPool;
        this.auditLogger = auditLogger;
        this.matrics = matrics;
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
                }).thenApply(this::updateAccount);
    }

    private Decision updateAccount(Decision decision){
        TransactionRecord txn = decision.txnrcd();
        if (decision.isApproved()){
            if (balanceStore.debit(txn.accountId(),txn.amount())){
                spendTracker.record(txn.cardId(),txn.amount());
            }else {
                System.out.printf("txn=%d approved but debit failed: balance changed since BalanceCheck%n", txn.id());
                decision = Decision.declined(txn, DeclineReason.INSUFFICIENT_BALANCE, decision.latencyMs());
            }
        }
        audit(decision);
        recordMatrices(decision);
        return decision;
    }


    private  void recordMatrices(Decision decision){
        if (decision.isApproved()){
            matrics.recordApproved();
        }else{
            matrics.recordDeclineReason(decision.declineReason());
            matrics.recordRejected();
        }
    }

    private void audit(Decision decision) {
        TransactionRecord txn = decision.txnrcd();
        String auditLog = String.format("AUDIT txn=%d account=%d amount=%.2f %s%s",
                txn.id(), txn.accountId(), txn.amount(), decision.status(),
                decision.isApproved() ? "" : " reason=" + decision.declineReason());
        auditLogger.log(auditLog);
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
