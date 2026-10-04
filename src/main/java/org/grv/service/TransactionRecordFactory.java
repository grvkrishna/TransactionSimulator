package org.grv.service;

import org.grv.model.TransactionRecord;

import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

public final class TransactionRecordFactory {

    private static final String[] MERCHANTS = {
            "Amazon", "Walmart", "Target", "BestBuy", "Starbucks", "Shell", "Uber"
    };

    private static final String[] LOCATIONS = {
            "New York", "San Francisco", "Chicago", "Austin", "Seattle"
    };

    private static final int NUM_TEST_ACCOUNTS = 20;

    public static TransactionRecord createRandom(int id) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        int accountId = rnd.nextInt(1, NUM_TEST_ACCOUNTS + 1);
        int cardId = 1000 + accountId;

        double amount = rnd.nextDouble(0, 1) < 0.05
                ? rnd.nextDouble(10_000, 20_000)   // ~5% large txns, exercise issuer bank path
                : rnd.nextDouble(1, 500);
        String merchant = MERCHANTS[rnd.nextInt(MERCHANTS.length)];
        String location = LOCATIONS[rnd.nextInt(LOCATIONS.length)];

        return new TransactionRecord(id, cardId, accountId, amount, merchant,
                LocalDateTime.now(), location);
    }
}