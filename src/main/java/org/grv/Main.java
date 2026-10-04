package org.grv;


import org.grv.config.ThreadPoolConfig;
import org.grv.ingest.TransactionSimulator;
import org.grv.service.authpipeline.AccountLedger;
import org.grv.service.authpipeline.AuthorizationsPipeline;
import org.grv.service.authpipeline.DailySpendTracker;

public class Main {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome!");
        AccountLedger balanceStore = new AccountLedger(20, 50_000);
        DailySpendTracker spendTracker = new DailySpendTracker();
        AuthorizationsPipeline pipeline = new AuthorizationsPipeline(balanceStore, spendTracker,
                ThreadPoolConfig.cpuExecutorService, ThreadPoolConfig.ioExecutorService);
        TransactionSimulator tf = new TransactionSimulator(spendTracker,pipeline);
       tf.transactionPubSub();
    }
}