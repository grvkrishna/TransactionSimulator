package org.grv.service.settlement;

import org.grv.audit.AuditLogger;
import org.grv.matrics.Matrics;
import org.grv.model.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SettlementService {

    private static final Logger log = LoggerFactory.getLogger(SettlementService.class);

    private static final long BATCH_INTERVAL_SEC = 10;
    private static final long BATCH_TIMEOUT_SEC = 5;
    private static final int WORKERS = 4;

    private record MerchantTotal(int count, double amount) {}

    private final BlockingQueue<TransactionRecord> pending = new LinkedBlockingQueue<>();
    private final AuditLogger auditLogger;
    private final Matrics matrics;
    private final AtomicInteger batchNo = new AtomicInteger();
    private final AtomicInteger workerNo = new AtomicInteger(1);

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "settlement-scheduler"));
    private final ExecutorService workers =
            Executors.newFixedThreadPool(WORKERS, r -> new Thread(r, "settlement-" + workerNo.getAndIncrement()));

    public SettlementService(AuditLogger auditLogger, Matrics matrics) {
        this.auditLogger = auditLogger;
        this.matrics = matrics;
    }

    public void record(TransactionRecord txn) {
        pending.offer(txn);
    }

    public void start() {
        scheduler.scheduleWithFixedDelay(this::safeRunBatch, BATCH_INTERVAL_SEC, BATCH_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    public int pendingCount() {
        return pending.size();
    }

    private void safeRunBatch() {
        try {
            runBatch();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.error("settlement batch failed", e);
        }
    }

    private void runBatch() throws InterruptedException {
        List<TransactionRecord> batch = new ArrayList<>();
        pending.drainTo(batch);
        if (batch.isEmpty()) {
            return;
        }
        int id = batchNo.incrementAndGet();
        long start = System.nanoTime();

        Map<String, List<TransactionRecord>> byMerchant =
                batch.stream().collect(Collectors.groupingBy(TransactionRecord::merchant));

        Map<String, MerchantTotal> results = new ConcurrentHashMap<>();
        CountDownLatch latch = new CountDownLatch(byMerchant.size());

        for (Map.Entry<String, List<TransactionRecord>> group : byMerchant.entrySet()) {
            workers.execute(() -> {
                try {
                    results.put(group.getKey(), settleMerchant(group.getValue()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }


        boolean complete = latch.await(BATCH_TIMEOUT_SEC, TimeUnit.SECONDS);


        int settledCount = 0;
        double settledAmount = 0;
        for (MerchantTotal t : results.values()) {
            settledCount += t.count();
            settledAmount += t.amount();
        }
        matrics.recordSettled(settledCount);
        long tookMs = (System.nanoTime() - start) / 1_000_000;

        String summary = String.format("SETTLEMENT batch=%d txns=%d merchants=%d/%d total=%.2f took=%dms%s",
                id, settledCount, results.size(), byMerchant.size(), settledAmount, tookMs,
                complete ? "" : " INCOMPLETE unsettled=" + (batch.size() - settledCount));
        auditLogger.log(summary);
        if (complete) {
            log.info(summary);
        } else {
            log.warn(summary);
        }
    }

    private MerchantTotal settleMerchant(List<TransactionRecord> txns) throws InterruptedException {
        double sum = 0;
        for (TransactionRecord t : txns) {
            sum += t.amount();
        }
        Thread.sleep(ThreadLocalRandom.current().nextInt(50, 151));
        return new MerchantTotal(txns.size(), sum);
    }

    public void stop() throws InterruptedException {
        scheduler.shutdown();
        if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }

        runBatch();

        workers.shutdown();
        if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
            workers.shutdownNow();
        }
    }

}
