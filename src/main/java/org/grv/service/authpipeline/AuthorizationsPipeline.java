package org.grv.service.authpipeline;


import org.grv.audit.AuditLogger;
import org.grv.decorator.RetryingCallable;
import org.grv.issuer.IssuerBankClient;
import org.grv.matrics.Matrics;
import org.grv.model.*;
import org.grv.service.CallableToSupplierAdaptor;
import org.grv.service.settlement.SettlementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;


public class AuthorizationsPipeline {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationsPipeline.class);

    private final AccountLedger balanceStore;
    private final DailySpendTracker spendTracker;
    private final Executor cpuPool;
    private final Executor ioPool;
    private final AuditLogger auditLogger;
    private final Matrics matrics;
    private static final long FRAUD_TIMEOUT_MS = 300;
    private static final long ISSUER_THRESHOLD = 10000;
    private final IssuerBankClient issuerBankClient;

    private final SettlementService settlementService;

    public AuthorizationsPipeline(AccountLedger balanceStore, DailySpendTracker spendTracker, Executor cpuPool, Executor ioPool, AuditLogger auditLogger, Matrics matrics, IssuerBankClient issuerBankClient, SettlementService settlementService) {
        this.balanceStore = balanceStore;
        this.spendTracker = spendTracker;
        this.cpuPool = cpuPool;
        this.ioPool = ioPool;
        this.auditLogger = auditLogger;
        this.matrics = matrics;
        this.issuerBankClient = issuerBankClient;
        this.settlementService = settlementService;
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

        CompletableFuture<CheckResult> balanceFuture = CompletableFuture.supplyAsync(CallableToSupplierAdaptor.adapt(new RetryingCallable<>(new BalanceCheck(txnRecord,balanceStore),3,10,matrics) ), ioPool);

        CompletableFuture<CheckResult> limitFuture = CompletableFuture.supplyAsync(CallableToSupplierAdaptor.adapt(new LimitCheck(txnRecord,spendTracker)),ioPool);

        return CompletableFuture.allOf(fraudFuture, balanceFuture, limitFuture)
                .thenApply(v -> decide(txnRecord, start, fraudFuture.join(), balanceFuture.join(), limitFuture.join()))
                .thenCompose( v -> {
                    if (v.txnrcd().amount() > ISSUER_THRESHOLD && v.isApproved()) {
                        return   CompletableFuture.supplyAsync(() -> issuerBankClient.issuerAuthorizations(v),ioPool);
                    }
                    return   CompletableFuture.completedFuture(v);
                })
                .exceptionally(ex -> { //We are throwing any failure (e.g. TransientException from BalanceCheck) becomes a decline. No transaction is ever lost.
                    log.warn("txn={} check failed: {}", txnRecord.id(), unwrap(ex).toString());
                    return Decision.declined(txnRecord, DeclineReason.CHECK_ERROR, elapsedMs(start));
                }).thenApply(this::updateAccount);
    }

    private Decision updateAccount(Decision decision){
        TransactionRecord txn = decision.txnrcd();
        if (decision.isApproved()){
            if (balanceStore.debit(txn.accountId(),txn.amount())){
                spendTracker.record(txn.cardId(),txn.amount());
                settlementService.record(txn);
            }else {
                log.warn("txn={} approved but debit failed: balance changed since BalanceCheck", txn.id());
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
