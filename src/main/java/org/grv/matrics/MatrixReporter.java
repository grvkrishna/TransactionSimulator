package org.grv.matrics;

import org.grv.config.ThreadPoolConfig;
import org.grv.ingest.TransactionSimulator;
import org.grv.ingest.buffer.Buffer;
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

    public MatrixReporter(Matrics matrics, Buffer<?> buffer, ThreadPoolExecutor consumerPool, ThreadPoolExecutor cpuPool, ThreadPoolExecutor ioPool) {
        this.matrics = matrics;
        this.buffer = buffer;
        this.consumerPool = consumerPool;
        this.cpuPool = cpuPool;
        this.ioPool = ioPool;
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

        System.out.printf("[metrics] received=%d approved=%d declined=%d %s inFlight=%d rejected=%d retried=%d tps=%d  processedTps=%d%n" ,
                received, approved, declined, declineBreakdown(), inFlight, rejected, matrics.getRetried(), tps,tpsProcessed);


        System.out.printf("[threads] buffer=%d/%d | %s | %s | %s%n",
                buffer.size(), buffer.capacity(),
                poolStats("consumer", consumerPool),
                poolStats("cpu", cpuPool),
                poolStats("io", ioPool));
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
        printMatrices();
    }
}
