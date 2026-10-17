package org.grv.matrics;


import org.grv.decorator.TimingExecutor;
import org.grv.ingest.buffer.Buffer;
import org.grv.issuer.IssuerBankClient;
import org.grv.model.DeclineReason;

import java.util.StringJoiner;
import java.util.concurrent.ThreadPoolExecutor;

public class MatrixReporter implements Runnable{


    private final Matrics matrics;
    private final Buffer<?> buffer;
    private final ThreadPoolExecutor consumerPool;
    private final ThreadPoolExecutor cpuPool;
    private final ThreadPoolExecutor ioPool;
    private long lastReceived = 0;
    private long lastProcessed = 0;
    private long lastNanos = System.nanoTime();
    private final IssuerBankClient issuerBankClient;
    private final TimingExecutor cpuTimed;
    private final TimingExecutor ioTimed;

    public MatrixReporter(Matrics matrics, Buffer<?> buffer, ThreadPoolExecutor consumerPool, ThreadPoolExecutor cpuPool, ThreadPoolExecutor ioPool, IssuerBankClient issuerBankClient, TimingExecutor cpuTimed, TimingExecutor ioTimed) {
        this.matrics = matrics;
        this.buffer = buffer;
        this.consumerPool = consumerPool;
        this.cpuPool = cpuPool;
        this.ioPool = ioPool;
        this.issuerBankClient = issuerBankClient;
        this.cpuTimed = cpuTimed;
        this.ioTimed = ioTimed;
    }


    private void printMatrices(){

        long now = System.nanoTime();
        long received = matrics.getReceived();
        long processed = matrics.getProcessed();
        long approved = matrics.getApproved();
        long declined = matrics.getTotalDeclined();
        long rejected = matrics.getRejected();
        long inFlight = received - approved - declined;

        double seconds = (now - lastNanos)/1_000_000_000.0;
        long tps = seconds > 0 ? Math.round((received - lastReceived) / seconds) : 0;
        long tpsProcessed = seconds > 0 ? Math.round((processed - lastProcessed) / seconds) : 0;
        lastReceived = received;
        lastProcessed = processed;
        lastNanos = now;

        System.out.printf("[metrics] received=%d approved=%d declined=%d %s inFlight=%d rejected=%d retried=%d tps=%d processedTps=%d settled=%d%n",
                received, approved, declined, declineBreakdown(), inFlight, rejected, matrics.getRetried(), tps, tpsProcessed,matrics.getSettled());


        System.out.printf("[threads] buffer=%d/%d | %s | %s | %s | issuer inUse=%d/%d waiting=%d max=%d%n",
                buffer.size(), buffer.capacity(),
                poolStats("consumer", consumerPool),
                poolStats("cpu", cpuPool),
                poolStats("io", ioPool),
                issuerBankClient.inUse(), issuerBankClient.capacity(), issuerBankClient.waiting(), issuerBankClient.maxInFlight());

        System.out.printf("[latency] %s | %s%n", timing(cpuTimed), timing(ioTimed));

    }

    private static String timing(TimingExecutor t) {
        return String.format("%s wait=%.1fms run=%.1fms", t.name(), t.avgQueueMs(), t.avgRunMs());
    }

    private String declineBreakdown() {
        StringJoiner joiner = new StringJoiner(", ", "{", "}");
        for (DeclineReason reason : DeclineReason.values()) {
            long count = matrics.getDeclinedReasonCount(reason);
            if (count > 0) {
                joiner.add(reason + "=" + count);
            }
        }
        return joiner.toString();
    }


    private static String poolStats(String name, ThreadPoolExecutor pool) {
        return String.format("%sPool active=%d/%d queue=%d",
                name, pool.getActiveCount(), pool.getPoolSize(), pool.getQueue().size());
    }

    @Override
    public void run() {
        try {
            printMatrices();
        } catch (RuntimeException e) {
            System.err.println("metrics reporter failed: " + e);
        }
    }
}
