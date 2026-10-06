package org.grv.service;


import org.grv.service.authpipeline.AccountLedger;
import org.grv.model.Decision;
import org.grv.model.TransactionRecord;
import org.grv.service.authpipeline.*;

import java.util.concurrent.CompletionException;
public class TransactionProcessor implements Runnable{

    private final TransactionRecord txnRecord;
    private final AuthorizationsPipeline pipeline;

    public TransactionProcessor(TransactionRecord txnRecord, AuthorizationsPipeline pipeline) {
        this.txnRecord = txnRecord;
        this.pipeline = pipeline;
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
        String thread = Thread.currentThread().getName();
        try {
            Decision decision = pipeline.authorize(txnRecord).join();
            System.out.printf("%s txn=%d %s%s latency=%dms%n", thread, txnRecord.id(), decision.status(),
                    decision.isApproved() ? "" : " reason=" + decision.declineReason(), decision.latencyMs());
        } catch (CompletionException e) {
            System.out.printf("%s txn=%d ERROR cause=%s%n", thread, txnRecord.id(), e.getCause());
        }
    }
}
