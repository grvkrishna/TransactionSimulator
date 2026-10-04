package org.grv.service.authpipeline;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AccountLedger {

    private final Map<Integer, Double> balances = new ConcurrentHashMap<>();
    private final Map<Integer, ReentrantLock> locks = new ConcurrentHashMap<>();

    public AccountLedger(int numAccounts, double openingBalance) {
        for (int id = 1; id <= numAccounts; id++) {
            balances.put(id, openingBalance);
            locks.put(id, new ReentrantLock());
        }
    }

    public double getBalance(int accountId) {
        ReentrantLock lock = locks.get(accountId);
        Objects.requireNonNull(lock,"unknown account " + accountId);
        lock.lock();
        try{
            return balances.getOrDefault(accountId, 0.0);
        }finally {
            lock.unlock();
        }
    }

    public boolean debit(int accountId, double amount){
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
        ReentrantLock lock = locks.get(accountId);
        Objects.requireNonNull(lock,"unknown account " + accountId);
        lock.lock();
        try{
            double balance = balances.get(accountId) ;
            if (balance>= amount) {
                balances.put(accountId, balance - amount);
                return true;
            }
            return false;
        }finally {
            lock.unlock();
        }
    }

    public boolean transfer(int from, int to, double amount){
        if (from == to) {
            throw new IllegalArgumentException("cannot transfer to the same account: " + from);
        }
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
        ReentrantLock lock = locks.get(from);
        Objects.requireNonNull(lock,"unknown account " + from);
        ReentrantLock lockTo = locks.get(to);
        Objects.requireNonNull(lockTo,"unknown account " + to);

        lock.lock();
        try {
            Thread.sleep(50);
            lockTo.lock();
            try{
                double fromBal = balances.get(from);
                if (fromBal >= amount){
                    balances.put(from, fromBal - amount);
                    balances.put(to, balances.get(to) + amount);
                    return true;
                }
                return false;
            }finally {
                lockTo.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            lock.unlock();
        }

    }


    public boolean transferNaive(int from, int to, double amount) {
        validateTransfer(from, to, amount);
        ReentrantLock fromLock = lockFor(from);
        ReentrantLock toLock = lockFor(to);

        fromLock.lock();
        try {
            Thread.sleep(50); // widen the race window: hold 'from' while the other thread grabs 'to'
            toLock.lock();
            try {
                return moveMoney(from, to, amount);
            } finally {
                toLock.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // keep the stop signal for the caller
            return false;
        } finally {
            fromLock.unlock();
        }
    }

    private boolean moveMoney(int from, int to, double amount) {
        double fromBal = balances.get(from);
        if (fromBal < amount) {
            return false;
        }
        balances.put(from, fromBal - amount);
        balances.put(to, balances.get(to) + amount);
        return true;
    }

    private void validateTransfer(int from, int to, double amount) {
        if (from == to) {
            throw new IllegalArgumentException("cannot transfer to the same account: " + from);
        }
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
    }

    private ReentrantLock lockFor(int accountId) {
        return Objects.requireNonNull(locks.get(accountId), "unknown account " + accountId);
    }
}
