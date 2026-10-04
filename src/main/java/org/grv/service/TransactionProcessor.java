package org.grv.service;

import org.grv.model.TransactionRecord;

public class TransactionProcessor implements Runnable{

    private final TransactionRecord txnRecord;

    public TransactionProcessor(TransactionRecord txnRecord) {
        this.txnRecord = txnRecord;
    }

    @Override
    public void run() {
        try {
            System.out.println(Thread.currentThread().getName()+" - Transaction record: " + txnRecord.toString());
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
