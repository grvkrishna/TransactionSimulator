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

    static LinkedBlockingQueue<Runnable> processCpu = new LinkedBlockingQueue<>(100);

    private static final AtomicInteger cpuThreadCount = new AtomicInteger(1);
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final ThreadFactory cpuThreadFactory = r -> new Thread(r, "cpu-pool-"+threadCount.getAndIncrement());
    public static ExecutorService cpuExecutorService = new ThreadPoolExecutor(CORES,
            CORES,
            0,
            TimeUnit.SECONDS,
            processCpu,cpuThreadFactory,new  ThreadPoolExecutor.CallerRunsPolicy());

    static LinkedBlockingQueue<Runnable> processIO = new LinkedBlockingQueue<>(10);

    private static final AtomicInteger ioThreadCount = new AtomicInteger(1);

    private static final ThreadFactory ioThreadFactory = r -> new Thread(r, "IO-pool-"+threadCount.getAndIncrement());
    public static ExecutorService ioExecutorService = new ThreadPoolExecutor(8,
            18,
            10,
            TimeUnit.SECONDS,
            processIO,ioThreadFactory,new  ThreadPoolExecutor.CallerRunsPolicy());
}
