package org.grv.model;

public enum DeclineReason {

    SCORE_DECLINE("Score above 80"),
    BALANCE_CHEK_DECLINE("Random Failure"),
    LIMIT_DECLINE("daily limit exceeds"),
    INSUFFICIENT_BALANCE("Insufficient Balance")
    ;

    DeclineReason(String s) {

    }
}
