package org.grv.decorator;


import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

public class TimingExecutor implements Executor {

    private final Executor delegate;
    private final String name;
    private final LongAdder tasks = new LongAdder();
    private final LongAdder totalQueueNanos = new LongAdder();
    private final LongAdder totalRunNanos = new LongAdder();

    public TimingExecutor(Executor delegate, String name) {
        this.delegate = delegate;
        this.name = name;
    }


    @Override
    public void execute(Runnable command) {
        long submitted = System.nanoTime();

        delegate.execute(() -> {
            long started = System.nanoTime();
            totalQueueNanos.add(started - submitted);
            try {
                command.run();
            } finally {
                totalRunNanos.add(System.nanoTime() - started);
                tasks.increment();
            }
        });
    }

    public String name() { return name; }

    public double avgQueueMs() { return avgMs(totalQueueNanos); }

    public double avgRunMs() { return avgMs(totalRunNanos); }

    private double avgMs(LongAdder totalNanos) {
        long n = tasks.sum();
        return n == 0 ? 0.0 : totalNanos.sum() / (double) n / 1_000_000.0;
    }


}
