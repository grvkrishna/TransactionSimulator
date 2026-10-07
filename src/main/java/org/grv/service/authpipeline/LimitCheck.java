package org.grv.service.authpipeline;

import org.grv.model.CheckResult;
import org.grv.model.DeclineReason;
import org.grv.model.TransactionRecord;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class LimitCheck implements Callable<CheckResult> {

    private final TransactionRecord txnRecord;
    private final DailySpendTracker spendTracker;

    public LimitCheck(TransactionRecord txnRecord, DailySpendTracker spendTracker) {
        this.txnRecord = txnRecord;
        this.spendTracker = spendTracker;
    }


    @Override
    public CheckResult call() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Thread.sleep(rnd.nextInt(10, 31)); //Cache call simulation
        double spent = spendTracker.spentToday(txnRecord.cardId());

        if (spent + txnRecord.amount() > DailySpendTracker.CARD_DAILY_LIMIT) {
            return new CheckResult("LimitCheck", false, DeclineReason.LIMIT_DECLINE);
        }
        return new CheckResult("LimitCheck", true, null);
    }
}
