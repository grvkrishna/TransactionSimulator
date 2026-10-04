package org.grv;


import org.grv.ingest.TransactionSimulator;
import org.grv.model.AccountBalanceStore;
import org.grv.service.authpipeline.DailySpendTracker;

public class Main {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome!");

       TransactionSimulator tf = new TransactionSimulator(new DailySpendTracker(), new AccountBalanceStore(20, 50000));
       tf.transactionPubSub();
    }
}