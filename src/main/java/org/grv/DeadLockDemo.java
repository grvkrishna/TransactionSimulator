package org.grv;

import org.grv.service.authpipeline.AccountLedger;

public class DeadLockDemo {
    public static void main(String[] args) {
        System.out.printf("Hello and welcome DeaLocal!");
        AccountLedger accountLedger = new AccountLedger(2,1000);

        Thread A = new Thread(() ->  transferLoop(accountLedger, 1, 2), "transfer-A");
        Thread B = new Thread(() -> transferLoop(accountLedger, 2, 1), "transfer-B");

        A.start();
        B.start();


    }

    private static void transferLoop(AccountLedger ledger, int from, int to) {
        String name = Thread.currentThread().getName();
        long iteration = 0;
        while (!Thread.currentThread().isInterrupted()) {
            boolean val = ledger.transferWithTryLockRetry(from, to, 1);
            iteration++;
            System.out.printf("%s transferred %d -> %d (iteration %d)%n  return %n", name, from, to, iteration,val);
        }
    }
}
