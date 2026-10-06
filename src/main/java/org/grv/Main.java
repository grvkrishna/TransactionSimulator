package org.grv;


import org.grv.audit.AuditLogger;
import org.grv.config.ThreadPoolConfig;
import org.grv.ingest.TransactionSimulator;
import org.grv.service.authpipeline.AccountLedger;
import org.grv.service.authpipeline.AuthorizationsPipeline;
import org.grv.service.authpipeline.DailySpendTracker;

import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome!");
        AuditLogger auditLogger = new AuditLogger(Path.of("audit.log"));
        auditLogger.start();
        AccountLedger balanceStore = new AccountLedger(20, 50_000);
        DailySpendTracker spendTracker = new DailySpendTracker();
        AuthorizationsPipeline pipeline = new AuthorizationsPipeline(balanceStore, spendTracker,
                ThreadPoolConfig.cpuExecutorService, ThreadPoolConfig.ioExecutorService,auditLogger);
        TransactionSimulator tf = new TransactionSimulator(pipeline);
       tf.transactionPubSub();
    }
}