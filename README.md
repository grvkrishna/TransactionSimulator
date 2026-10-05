# TransactionSimulator
Multithreading understanding



**Code:** `AccountLedger` (per-account `ReentrantLock`), `DeadLockDemo` (two threads transferring in opposite directions: `transfer-A` 1 → 2, `transfer-B` 2 → 1).                                       
**Thread dump:** [`docs/deadlock-dump.txt`](docs/deadlock-dump.txt)

### Why per-account locks
One global lock would serialize every debit in the system. A lock per account lets debits on                                                                                                             
different accounts run in parallel; only operations on the *same* account wait for each other.                                                                                                           
`debit` checks the balance and subtracts **under the lock**, so check + subtract is atomic and                                                                                                           
an account can never go negative, even if `BalanceCheck` saw an older balance.

### The four transfer variants

| Method | Locking strategy | What I observed in `DeadLockDemo` |                                                                                                                                        
  |---|---|---|                                                                                                                                                                                            
| `transferNaive` | lock `from`, then `to` | Froze on the first iteration. No output, CPU idle, process never exits. **Deadlock.** |                                                                     
| `transferWithTryLock` | lock `from`, `tryLock(to, 51 ms)`, give up on timeout | Never hangs, but every call returned `false`. **Livelock.** |                                                          
| `transferWithTryLockRetry` | per attempt: lock `from`, `tryLock(to)`, release everything; random 1–10 ms backoff; max 5 attempts | Mostly `true`; some iterations slower because an attempt failed and 
  retried. |
| `transfer` | lock ordering: always lock the **smaller account id first** | Always `true`, fastest, never stuck. **Used by the app.** |                                                                 

(The demo variants add a 50 ms sleep while holding the first lock to make the race window wide                                                                                                           
and the problem reproducible on every run.)

### Why the naive version deadlocks
| Time | transfer-A (1 → 2) | transfer-B (2 → 1) |                                                                                                                                                       
  |---|---|---|                                                                                                                                                                                            
| 0 ms | locks account 1 | locks account 2 |                                                                                                                                                             
| 50 ms | waits for account 2 (held by B) | waits for account 1 (held by A) |                                                                                                                            
| forever | waiting | waiting |                                                                                                                                                                          

The thread dump confirms it:                                                                                                                                                                             
Found one Java-level deadlock:                                                                                                                                                                           
"transfer-A": waiting for ownable synchronizer ..., which is held by "transfer-B"                                                                                                                        
"transfer-B": waiting for ownable synchronizer ..., which is held by "transfer-A"                                                                                                                        
Both threads are `WAITING (parking)` inside `ReentrantLock.lock()`, called from `transferNaive`.  

A deadlock needs all four **Coffman conditions**:
1. **Mutual exclusion:** a lock has one owner at a time.
2. **Hold and wait:** a thread holds one lock while waiting for another.
3. **No preemption:** a lock can't be taken away from its owner.
4. **Circular wait:** A waits for B, and B waits for A.

Each fix breaks one of them.

### Fix 1: lock ordering (breaks *circular wait*)
Both threads lock `min(from, to)` first, so both go for account 1 first. Whichever gets it                                                                                                               
continues; the other waits **while holding nothing**, so no cycle can form. The money still                                                                                                              
moves `from → to`; only the *lock acquisition order* is sorted.

- ✅ Simple, can't faail, no retries, deadlock is impossible.
- ⚠️ You must know all the locks up front and have a global order for them (here: account id).

### Fix 2: `tryLock` + timeout + retry with random backoff (breaks *hold and wait*)
`tryLock(timeout)` gives up instead of waiting forever, and releasing the first lock on                                                                                                                  
failure means a thread never holds one lock while waiting on another. But **timeout alone                                                                                                                
isn't enough**: with symmetric timing both threads time out together, release, and retry in                                                                                                              
lockstep, so every call fails (livelock). A **random** backoff between attempts, taken while                                                                                                             
holding *no* locks, breaks the symmetry: one thread retries first, gets both locks and finishes.

- ✅ Works even when there's no natural lock order (locks discovered dynamically).
- ⚠️ More code, a transfer can fail after N attempts, and retries add latency.
- `synchronized` can't do this at all: it has no `tryLock` and no timeout. That is the main reason to use `ReentrantLock` here.

### Lessons
- Always `lock(); try { … } finally { unlock(); }`. Call `lock()` *before* `try`, so a failed acquire never unlocks a lock you don't hold.
- Only unlock what you actually locked: check the `boolean` from `tryLock`.
- Never `return` from `finally`. It overrides the method's result and silently swallows exceptions.
- `lock()` ignores interrupts while waiting, so an interrupt can't break a deadlock; `tryLock(timeout)` and `lockInterruptibly()` can be interrupted.
- To diagnose a frozen JVM: `jps -l` to find the PID, then `jstack <pid>`, and look for "Found one Java-level deadlock".

Two things to check before committing it:
- The "What I observed" column: replace my expected wording with what you actually saw. For example, if transferWithTryLock produced the occasional true because of scheduling jitter, say so. That's an
  honest and interesting detail.
- The dump excerpt: I shortened it. Paste the real lines from your docs/deadlock-dump.txt, with the addresses, if you prefer it verbatim.      