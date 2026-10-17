package org.grv.service;


import org.grv.matrics.Matrics;
import org.grv.model.Decision;
import org.grv.model.TransactionRecord;
import org.grv.service.authpipeline.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.CompletionException;
public class TransactionProcessor implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(TransactionProcessor.class);

    private final TransactionRecord txnRecord;
    private final AuthorizationsPipeline pipeline;
    private final Matrics matrics;
    public TransactionProcessor(TransactionRecord txnRecord, AuthorizationsPipeline pipeline, Matrics matrics) {
        this.txnRecord = txnRecord;
        this.pipeline = pipeline;
        this.matrics = matrics;
    }

    @Override
    public void run() {
//            Future<CheckResult> fraudFuture = ThreadPoolConfig.executorServiceAuthPipeline.submit(new FraudCheck(txnRecord));
//            Future<CheckResult> limitFuture = ThreadPoolConfig.executorServiceAuthPipeline.submit(new LimitCheck(txnRecord,dailySpendTracker));
//            Future<CheckResult> balanceFuture = ThreadPoolConfig.executorServiceAuthPipeline.submit(new BalanceCheck(txnRecord,accountBalanceStore));

//            CheckResult fraudResult = fraudFuture.get();
//            CheckResult limitCheckResult = limitFuture.get();
//            CheckResult balanceCheckResult = balanceFuture.get();
//            boolean declined = false;
//            if (!balanceCheckResult.passed()) {
//                System.out.println("Transaction :" +txnRecord.id()+" : DECLINED" + " Cause: BalanceCheckFailed") ;
//                declined=true;
//            }
//            if (!fraudResult.passed()) {
//                System.out.println("Transaction :" +txnRecord.id()+" : DECLINED" + " Cause: FraudCheckFailed") ;
//                declined=true;
//            }
//            if (!limitCheckResult.passed()) {
//                System.out.println("Transaction :" + txnRecord.id() + " : DECLINED" + " Cause: LimitCheckFailed");
//                declined = true;
//            }
//            if (!declined){
//                System.out.println("Transaction :" + txnRecord.id() + " : APPROVED");
//                dailySpendTracker.record(txnRecord.cardId(),txnRecord.amount());
//            }
//        String thread = Thread.currentThread().getName();
        MDC.put("txnId", String.valueOf(txnRecord.id()));
        try {
            Decision decision = pipeline.authorize(txnRecord).join();
            log.info("txn={} {}{} latency={}ms", txnRecord.id(), decision.status(),
                    decision.isApproved() ? "" : " reason=" + decision.declineReason(), decision.latencyMs());
        }  catch (CompletionException e) {
            log.warn("txn={} failed cause={}", txnRecord.id(), e.getCause().toString());
        } finally {
            matrics.recordProcessed();
            MDC.remove("txnId");
        }
    }
}
