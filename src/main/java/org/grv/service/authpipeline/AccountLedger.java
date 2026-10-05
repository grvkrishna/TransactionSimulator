package org.grv.service.authpipeline;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
        ReentrantLock lock = lockFor(accountId);
        lock.lock();
        try {
            return balances.getOrDefault(accountId, 0.0);
        } finally {
            lock.unlock();
        }
    }

    public boolean debit(int accountId, double amount) {
        if (amount <= 0) {
            throw new IllegalArgumentException("amount must be positive: " + amount);
        }
        ReentrantLock lock = lockFor(accountId);
        lock.lock();
        try {
            double balance = balances.get(accountId);
            if (balance >= amount) {
                balances.put(accountId, balance - amount);
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    public boolean transfer(int from, int to, double amount) {
        validateTransfer(from, to, amount);
        ReentrantLock firstLock = lockFor(Math.min(from, to));
        ReentrantLock secondLock = lockFor(Math.max(from, to));
        firstLock.lock();
        try {
            secondLock.lock();
            try {
                return moveMoney(from, to, amount);
            } finally {
                secondLock.unlock();
            }
        } finally {
            firstLock.unlock();
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
            Thread.currentThread().interrupt();
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

    public boolean transferWithTryLock(int from, int to, double amount) { // this is livelock like every thread waiting for same time got timeout and then come again waiting for same time timeout so this livelock always return false.
        validateTransfer(from, to, amount);
        ReentrantLock fromLock = lockFor(from);
        ReentrantLock toLock = lockFor(to);

        fromLock.lock();
        try {
            Thread.sleep(50); // widen the race window: hold 'from' while the other thread grabs 'to'
            if (toLock.tryLock(51, TimeUnit.MILLISECONDS)) {
                try {
                    return moveMoney(from, to, amount);
                } finally {
                    toLock.unlock();
                }
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally {
            fromLock.unlock();
        }
    }


    public boolean transferWithTryLockRetry(int from, int to, double amount) { //tryLock + retry + random backoff: avoids deadlock and livelock
        validateTransfer(from, to, amount);
        ReentrantLock fromLock = lockFor(from);
        ReentrantLock toLock = lockFor(to);
        int attampt =0;
        int maxTry = 5;

        try {
            while (attampt < maxTry) {
                fromLock.lock();

                try {
                    Thread.sleep(50); // widen the race window: hold 'from' while the other thread grabs 'to'

                    if (toLock.tryLock(51, TimeUnit.MILLISECONDS)) {
                        try {
                            return moveMoney(from, to, amount);
                        } finally {
                            toLock.unlock();
                        }
                    } else {
                        attampt++;
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    fromLock.unlock();

                }
                Thread.sleep(ThreadLocalRandom.current().nextLong(1, 11));
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

    }
}
