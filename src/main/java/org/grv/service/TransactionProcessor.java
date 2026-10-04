package org.grv.service;

import org.grv.config.ThreadPoolConfig;
import org.grv.config.TransientException;
import org.grv.model.AccountBalanceStore;
import org.grv.model.CheckResult;
import org.grv.model.TransactionRecord;
import org.grv.service.authpipeline.BalanceCheck;
import org.grv.service.authpipeline.DailySpendTracker;
import org.grv.service.authpipeline.FraudCheck;
import org.grv.service.authpipeline.LimitCheck;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TransactionProcessor implements Runnable{

    private final TransactionRecord txnRecord;
    private final DailySpendTracker dailySpendTracker;
    private final AccountBalanceStore accountBalanceStore;

    public TransactionProcessor(TransactionRecord txnRecord, DailySpendTracker dailySpendTracker, AccountBalanceStore accountBalanceStore) {
        this.txnRecord = txnRecord;
        this.dailySpendTracker = dailySpendTracker;
        this.accountBalanceStore = accountBalanceStore;
    }

    @Override
    public void run() {
        try {
            long start = System.nanoTime();
            System.out.println(Thread.currentThread().getName()+" - Transaction record: " + txnRecord.toString());
            Future<CheckResult> fraudFuture = ThreadPoolConfig.executorServiceAuthPipeline.submit(new FraudCheck(txnRecord));
            Future<CheckResult> limitFuture = ThreadPoolConfig.executorServiceAuthPipeline.submit(new LimitCheck(txnRecord,dailySpendTracker));
            Future<CheckResult> balanceFuture = ThreadPoolConfig.executorServiceAuthPipeline.submit(new BalanceCheck(txnRecord,accountBalanceStore));

            CheckResult fraudResult = fraudFuture.get();
            CheckResult limitCheckResult = limitFuture.get();
            CheckResult balanceCheckResult = balanceFuture.get();
            boolean declined = false;
            if (!balanceCheckResult.passed()) {
                System.out.println("Transaction :" +txnRecord.id()+" : DECLINED" + " Cause: BalanceCheckFailed") ;
                declined=true;
            }
            if (!fraudResult.passed()) {
                System.out.println("Transaction :" +txnRecord.id()+" : DECLINED" + " Cause: FraudCheckFailed") ;
                declined=true;
            }
            if (!limitCheckResult.passed()) {
                System.out.println("Transaction :" + txnRecord.id() + " : DECLINED" + " Cause: LimitCheckFailed");
                declined = true;
            }
            if (!declined){
                System.out.println("Transaction :" + txnRecord.id() + " : APPROVED");
                dailySpendTracker.record(txnRecord.cardId(),txnRecord.amount());
            }
            long end = System.nanoTime();
            System.out.println(" total latency to run transaction :" +txnRecord.id()+" :" +(end - start)/1_000_000);

        } catch (InterruptedException  e) {
            Thread.currentThread().interrupt();
        }catch (ExecutionException e){
            System.out.println("Transaction :" +txnRecord.id()+" : DECLINED" + " Cause: "+e.getCause()) ;
        }
    }
}
