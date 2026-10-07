package org.grv.model;

public record Decision(TransactionRecord txnrcd, Status status, DeclineReason declineReason, long latencyMs) {

    public static Decision approved(TransactionRecord txn, long latencyMs) {
        return new Decision(txn, Status.APPROVED, null, latencyMs);
    }

    public static Decision declined(TransactionRecord txn, DeclineReason reason, long latencyMs) {
        return new Decision(txn, Status.DECLINED, reason, latencyMs);
    }

    public boolean isApproved() {
        return status == Status.APPROVED;
    }
}
