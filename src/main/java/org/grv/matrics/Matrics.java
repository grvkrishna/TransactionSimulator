package org.grv.matrics;

import org.grv.model.DeclineReason;

import java.util.EnumMap;
import java.util.concurrent.atomic.LongAdder;

public class Matrics {

    private final LongAdder received = new
            LongAdder();
    private final LongAdder approved = new
            LongAdder();
    private final LongAdder rejected= new
            LongAdder();
    private final LongAdder fraudTimeout= new
            LongAdder();
    private final LongAdder retried= new
            LongAdder();

    private final LongAdder processed= new
            LongAdder();

    private final EnumMap<DeclineReason, LongAdder> declineReasonCounter =  new EnumMap<>(DeclineReason.class);


    public Matrics(){
        for (DeclineReason value : DeclineReason.values()) {
            declineReasonCounter.put(value,new LongAdder());
        }
    }

    public long getProcessed(){
        return this.processed.longValue();
    }

    public void recordProcessed(){
        this.processed.increment();
    }

    public long getReceived() {
        return received.longValue();
    }

    public void recordReceived() {
        this.received.increment();
    }

    public long getApproved() {
        return approved.longValue();
    }

    public void recordApproved() {
        this.approved.increment();
    }

    public long getRejected() {
        return rejected.longValue();
    }

    public void recordRejected() {
        this.rejected.increment();
    }

    public long getFraudTimeout() {
        return fraudTimeout.longValue();
    }

    public void recordFraudTimeout() {
        this.fraudTimeout.increment();
    }

    public long getRetried() {
        return retried.longValue();
    }

    public void recordRetried() {
        this.retried.increment();
    }

    public void recordDeclineReason(DeclineReason declineReason){
        declineReasonCounter.get(declineReason).increment();
    }

    public long getDeclinedReasonCount(DeclineReason declineReason){
        return declineReasonCounter.get(declineReason).longValue();
    }

    public long getTotalDeclined(){
        long total =0;
        for (LongAdder value : declineReasonCounter.values()) {
            total +=value.longValue();
        }

        return total;
    }
}
