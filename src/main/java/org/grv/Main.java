package org.grv;


import org.grv.audit.AuditLogger;
import org.grv.config.ThreadPoolConfig;
import org.grv.ingest.TransactionSimulator;
import org.grv.matrics.Matrics;
import org.grv.matrics.MatrixReporter;
import org.grv.service.authpipeline.AccountLedger;
import org.grv.service.authpipeline.AuthorizationsPipeline;
import org.grv.service.authpipeline.DailySpendTracker;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome!");
        AuditLogger auditLogger = new AuditLogger(Path.of("audit.log"));
        auditLogger.start();

        Matrics matrics = new Matrics();

        AccountLedger balanceStore = new AccountLedger(20, 50_000);
        DailySpendTracker spendTracker = new DailySpendTracker();
        AuthorizationsPipeline pipeline = new AuthorizationsPipeline(balanceStore, spendTracker,
                ThreadPoolConfig.cpuExecutorService, ThreadPoolConfig.ioExecutorService,auditLogger,matrics);
        TransactionSimulator tf = new TransactionSimulator(pipeline,matrics);

        MatrixReporter reporter = new MatrixReporter(matrics, tf.getBuffer(),
                ThreadPoolConfig.executorService, ThreadPoolConfig.cpuExecutorService, ThreadPoolConfig.ioExecutorService);

        ScheduledExecutorService metricsScheduler =
                Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "metrics-reporter"));
        metricsScheduler.scheduleAtFixedRate(reporter, 2, 2, TimeUnit.SECONDS);

        tf.transactionPubSub();
    }
}