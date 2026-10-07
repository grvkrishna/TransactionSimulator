package org.grv.ingest;

import org.grv.matrics.Matrics;
import org.grv.service.TransactionRecordFactory;
import org.grv.ingest.buffer.BoundedBuffer;
import org.grv.model.TransactionRecord;

public class TransactionProducer implements Runnable{

    BoundedBuffer<TransactionRecord> transactionsBlockingQueue = null;
    int productionRate = 50;
    private int i =1;

    public TransactionProducer(BoundedBuffer<TransactionRecord> queue, int productionRate){
        this.transactionsBlockingQueue = queue;
        this.productionRate = productionRate;
    }

    public TransactionProducer(BoundedBuffer<TransactionRecord> queue){
        this.transactionsBlockingQueue = queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                TransactionRecord txn = TransactionRecordFactory.createRandom(i);
                transactionsBlockingQueue.put(txn);
                i++;
                sleep(1000/productionRate);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }


    private void sleep(Integer ms) throws InterruptedException {
        Thread.sleep(ms);
    }
}
