package org.grv.model;

import java.time.LocalDateTime;

public record TransactionRecord(Integer id, Integer cardId, Integer accountId, Double amount, String merchant,
                                LocalDateTime timeStamp,String location) {

}
