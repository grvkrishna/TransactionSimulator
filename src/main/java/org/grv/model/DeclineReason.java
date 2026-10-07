package org.grv.model;

public enum DeclineReason {

    SCORE_DECLINE("Fraud score above 80"),
    INSUFFICIENT_BALANCE("Insufficient balance"),
    LIMIT_DECLINE("Daily card limit exceeded"),
    FRAUD_TIMEOUT("Fraud check took longer than 300 ms"),
    CHECK_ERROR("A check failed with an error");

    private final String description;

    DeclineReason(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}
