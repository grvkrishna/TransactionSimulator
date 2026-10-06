package org.grv.ingest;

import org.grv.config.ThreadPoolConfig;
import org.grv.model.TransactionRecord;
import org.grv.service.TransactionProcessor;
import org.grv.ingest.buffer.BoundedBuffer;
import org.grv.service.authpipeline.AuthorizationsPipeline;


import java.util.concurrent.LinkedBlockingQueue;

public class TransactionSimulator {

    BoundedBuffer<TransactionRecord> queue = new BoundedBuffer<>(10);

    private final AuthorizationsPipeline pipeline;
    public TransactionSimulator( AuthorizationsPipeline pipeline) {
        this.pipeline = pipeline;
    }

    public void transactionPubSub() {
        TransactionProducer producer = new TransactionProducer(queue, 1);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        Thread dispatcher = new Thread(() ->dispatcherLoop(),"dispatcher");
        dispatcher.start();
    }

    private void dispatcherLoop(){
        while (true){
            try {
                TransactionRecord txnRecord = queue.take();
                ThreadPoolConfig.executorService.execute(new TransactionProcessor(txnRecord,pipeline));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
