package org.grv.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AccountBalanceStore {

    private final Map<Integer, Double> balances = new ConcurrentHashMap<>();

    public AccountBalanceStore(int numAccounts, double openingBalance) {
        for (int id = 1; id <= numAccounts; id++) {
            balances.put(id, openingBalance);
        }
    }

    public double getBalance(int accountId) {
        return balances.getOrDefault(accountId, 0.0);
    }
}
