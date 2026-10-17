package org.grv;


import org.grv.audit.AuditLogger;
import org.grv.config.ThreadPoolConfig;
import org.grv.decorator.TimingExecutor;
import org.grv.decorator.TracingExecutor;
import org.grv.ingest.TransactionSimulator;
import org.grv.issuer.IssuerBankClient;
import org.grv.matrics.Matrics;
import org.grv.matrics.MatrixReporter;
import org.grv.service.authpipeline.AccountLedger;
import org.grv.service.authpipeline.AuthorizationsPipeline;
import org.grv.service.authpipeline.DailySpendTracker;
import org.grv.service.settlement.SettlementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.*;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final int RUN_SECONDS = 30;
    private static final long POOL_TIMEOUT_SEC = 10;

    public static void main(String[] args) throws InterruptedException {
        System.out.printf("Hello and welcome!");
        AuditLogger auditLogger = new AuditLogger(Path.of("audit.log"));
        auditLogger.start();

        Matrics matrics = new Matrics();

        AccountLedger balanceStore = new AccountLedger(20, 50_000);
        DailySpendTracker spendTracker = new DailySpendTracker();
        IssuerBankClient issuerBankClient = new IssuerBankClient();
        TimingExecutor cpuTimed = new TimingExecutor(ThreadPoolConfig.cpuExecutorService, "cpu");
        TimingExecutor ioTimed  = new TimingExecutor(ThreadPoolConfig.ioExecutorService, "io");
        Executor cpuExec = new TracingExecutor(cpuTimed);
        Executor ioExec  = new TracingExecutor(ioTimed);
        SettlementService settlementService = new SettlementService(auditLogger, matrics);
        settlementService.start();

        AuthorizationsPipeline pipeline = new AuthorizationsPipeline(balanceStore, spendTracker,
                cpuExec, ioExec,auditLogger,matrics,issuerBankClient,settlementService);
        TransactionSimulator tf = new TransactionSimulator(pipeline,matrics);

        MatrixReporter reporter = new MatrixReporter(matrics, tf.getBuffer(),
                ThreadPoolConfig.executorService, ThreadPoolConfig.cpuExecutorService, ThreadPoolConfig.ioExecutorService,issuerBankClient,cpuTimed,ioTimed);

        ScheduledExecutorService metricsScheduler =
                Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "metrics-reporter"));
        metricsScheduler.scheduleAtFixedRate(reporter, 2, 2, TimeUnit.SECONDS);

        tf.transactionPubSub();
        log.info("running for {}s", RUN_SECONDS);
        Thread.sleep(RUN_SECONDS * 1000L);


        log.info("shutdown: started");

        tf.stop();
        log.info("shutdown: producer stopped (produced={}), dispatcher drained buffer", tf.produced());

        shutdownAndWait("consumer pool", ThreadPoolConfig.executorService);
        shutdownAndWait("cpu pool", ThreadPoolConfig.cpuExecutorService);
        shutdownAndWait("io pool", ThreadPoolConfig.ioExecutorService);

        settlementService.stop();
        log.info("shutdown: settlement stopped");

        shutdownAndWait("metrics reporter", metricsScheduler);
        reporter.run();

        auditLogger.stop();
        log.info("shutdown: audit logger stopped");

        // ---- 8: prove nothing was lost ----
        long approved = matrics.getApproved();
        long declined = matrics.getTotalDeclined();
        boolean ok = true;
        ok &= check("produced == received", tf.produced() == matrics.getReceived());
        ok &= check("received == approved + declined", matrics.getReceived() == approved + declined);
        ok &= check("settled == approved", matrics.getSettled() == approved);
        ok &= check("buffer and settlement queue empty",
                tf.getBuffer().size() == 0 && settlementService.pendingCount() == 0);

        log.info("shutdown: complete, all checks {}", ok ? "PASSED" : "FAILED");
        if (!ok) {
            System.exit(1);
        }
    }

    private static void shutdownAndWait(String name, ExecutorService pool) throws InterruptedException {
        pool.shutdown();
        if (pool.awaitTermination(POOL_TIMEOUT_SEC, TimeUnit.SECONDS)) {
            log.info("shutdown: {} terminated", name);
        } else {
            log.warn("shutdown: {} did not finish in {}s, forcing", name, POOL_TIMEOUT_SEC);
            pool.shutdownNow();
        }
    }

    private static boolean check(String name, boolean passed) {
        log.info("CHECK {} ... {}", name, passed ? "PASS" : "FAIL");
        return passed;
    }
}