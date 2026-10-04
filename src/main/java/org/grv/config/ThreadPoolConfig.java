package org.grv.config;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolConfig {

    static LinkedBlockingQueue<Runnable> processTask = new LinkedBlockingQueue<>(10);

    private static final AtomicInteger threadCount = new AtomicInteger(1);

    private static final ThreadFactory threadFactory = r -> new Thread(r, "Consumer-pool-"+threadCount.getAndIncrement());
    public static ExecutorService executorService = new ThreadPoolExecutor(3,
            6,
            10,
            TimeUnit.SECONDS,
            processTask,threadFactory,new  ThreadPoolExecutor.CallerRunsPolicy());

    public static ExecutorService executorServiceAuthPipeline = Executors.newFixedThreadPool(6);
}
