package org.grv.service;

import org.grv.ingest.buffer.BoundedBuffer;
import org.grv.model.TransactionRecord;

public class TransactionConsumer implements Runnable{
    BoundedBuffer<TransactionRecord> transactionsBlockingQueue = null;

    public TransactionConsumer(BoundedBuffer<TransactionRecord> queue){
        this.transactionsBlockingQueue = queue;
    }


    @Override
    public void run() {
        while (true) {
            try {
                TransactionRecord txnRecord = transactionsBlockingQueue.take();
                System.out.println("Transaction record consumer: " + txnRecord.toString());
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
