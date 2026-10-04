package org.grv.service.authpipeline;

import org.grv.model.CheckResult;
import org.grv.model.DeclineReason;
import org.grv.model.TransactionRecord;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public class FraudCheck implements Callable<CheckResult> {

    private final TransactionRecord txnRecord;

    public FraudCheck(TransactionRecord txnRecord) {
        this.txnRecord = txnRecord;
    }

    @Override
    public CheckResult call() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        cpuTask(rnd.nextInt(5, 21));
        int score =  txnRecord.amount() > 10_000 ? rnd.nextInt(40, 101) : rnd.nextInt(0, 91);
        if (score > 80){
           return new CheckResult("FraudCheck",false, DeclineReason.SCORE_DECLINE);
        }
        return new CheckResult("FraudCheck",true,null);
    }

    private static void cpuTask(int ms){
        long t = System.nanoTime() + ms * 1_000_000;
        double x =0;
        while (System.nanoTime()<t){
            x +=Math.sqrt(x+1);
        }
        if(x < 0) System.out.println(x);
    }
}
