package org.grv;


import org.grv.ingest.TransactionSimulator;

public class Main {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome!");

       TransactionSimulator tf = new TransactionSimulator();
       tf.transactionPubSub();
    }
}