package org.grv.ingest;

import org.grv.config.ThreadPoolConfig;
import org.grv.ingest.buffer.Buffer;
import org.grv.matrics.Matrics;
import org.grv.model.TransactionRecord;
import org.grv.service.TransactionProcessor;
import org.grv.ingest.buffer.BoundedBuffer;
import org.grv.service.authpipeline.AuthorizationsPipeline;


import java.util.concurrent.LinkedBlockingQueue;

public class TransactionSimulator {

    private final  BoundedBuffer<TransactionRecord> queue = new BoundedBuffer<>(100);
    private final Matrics matrics;


    private final AuthorizationsPipeline pipeline;
    public TransactionSimulator( AuthorizationsPipeline pipeline,Matrics matrics) {
        this.matrics = matrics;
        this.pipeline = pipeline;
    }

    public void transactionPubSub() {
        TransactionProducer producer = new TransactionProducer(queue, 50);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        Thread dispatcher = new Thread(() ->dispatcherLoop(),"dispatcher");
        dispatcher.start();
    }

    private void dispatcherLoop(){
        while (true){
            try {
                TransactionRecord txnRecord = queue.take();
                matrics.recordReceived();
                ThreadPoolConfig.executorService.execute(new TransactionProcessor(txnRecord,pipeline,matrics));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }


    public Buffer<TransactionRecord> getBuffer() {
        return queue;
    }
}
