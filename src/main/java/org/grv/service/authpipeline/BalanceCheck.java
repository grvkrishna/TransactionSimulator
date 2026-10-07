package org.grv.service.authpipeline;

import org.grv.config.TransientException;
import org.grv.model.CheckResult;
import org.grv.model.DeclineReason;
import org.grv.model.TransactionRecord;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class BalanceCheck implements Callable<CheckResult> {

    private final TransactionRecord txnRecord;
    private final AccountLedger balanceStore;

    public BalanceCheck(TransactionRecord txnRecord, AccountLedger balanceStore) {
        this.txnRecord = txnRecord;
        this.balanceStore = balanceStore;
    }

    @Override
    public CheckResult call() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Thread.sleep(rnd.nextInt(50, 201)); //Db call simulation
        if (rnd.nextDouble()  < 0.05) {
            throw new TransientException("Balance DB unavailable for txn " + txnRecord.id());
        }
        double balance = balanceStore.getBalance(txnRecord.accountId());
        if (balance < txnRecord.amount()) {
            return new CheckResult("BalanceCheck", false, DeclineReason.INSUFFICIENT_BALANCE);
        }
        return new CheckResult("BalanceCheck", true, null);
    }
}
