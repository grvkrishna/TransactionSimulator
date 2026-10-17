package org.grv.ingest;

import org.grv.config.ThreadPoolConfig;
import org.grv.ingest.buffer.Buffer;
import org.grv.matrics.Matrics;
import org.grv.model.TransactionRecord;
import org.grv.service.TransactionProcessor;
import org.grv.ingest.buffer.BoundedBuffer;
import org.grv.service.authpipeline.AuthorizationsPipeline;

public class TransactionSimulator {

    private static final TransactionRecord POISON =
            new TransactionRecord(-1, -1, -1, 0.0, "POISON", null, null);
    private final  BoundedBuffer<TransactionRecord> queue = new BoundedBuffer<>(100);
    private final Matrics matrics;

    private TransactionProducer producer;
    private Thread producerThread;
    private Thread dispatcher;
    private final AuthorizationsPipeline pipeline;
    public TransactionSimulator( AuthorizationsPipeline pipeline,Matrics matrics) {
        this.matrics = matrics;
        this.pipeline = pipeline;
    }

    public void transactionPubSub() {
        producer = new TransactionProducer(queue, 50);
        producerThread = new Thread(producer, "producer");
        producerThread.start();

        dispatcher = new Thread(() ->dispatcherLoop(),"dispatcher");
        dispatcher.start();
    }

    private void dispatcherLoop(){
        while (true){
            try {
                TransactionRecord txnRecord = queue.take();
                if (txnRecord == POISON) {break;}
                matrics.recordReceived();
                ThreadPoolConfig.executorService.execute(new TransactionProcessor(txnRecord,pipeline,matrics));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void stop() throws InterruptedException {
        producerThread.interrupt();
        producerThread.join();

        queue.put(POISON);
        dispatcher.join();
    }
    public int produced() {
        return producer.produced();
    }

    public Buffer<TransactionRecord> getBuffer() {
        return queue;
    }
}
