package org.grv.service.authpipeline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DailySpendTracker {
    public static final double CARD_DAILY_LIMIT = 25_000;

    private final Map<Integer, Double> spendByCard = new ConcurrentHashMap<>();

    public double spentToday(int cardId) {
        return spendByCard.getOrDefault(cardId, 0.0);
    }

    public void record(int cardId, double amount) {
        spendByCard.merge(cardId, amount, Double::sum);
    }
}
