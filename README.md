# Card Transaction Simulator: Java Concurrency, Hands-On

A card-authorization system simulated in plain Java 21 (no Spring), built step by step to learn
multithreading. Every concurrency tool here exists because the simulator needs it, so each
section below answers three questions: **what is it, why is it here, and where is the code.**

Use this README as an interview revision sheet: read a concept, then open the linked class to see
the real implementation.

**Run:** start `org.grv.Main` from the IDE. It runs for 30 s, shuts down gracefully, prints
PASS/FAIL checks, and exits on its own. `org.grv.DeadLockDemo` reproduces the R6 deadlock.

**Stack:** Java 21, Gradle, SLF4J + Logback, JUnit 5.

---

## Contents
1. [Architecture and thread map](#1-architecture-and-thread-map)
2. [Requirement → concept → code index](#2-requirement--concept--code-index)
3. [Threads, Runnable and interrupts](#3-threads-runnable-and-interrupts)
4. [Bounded buffer with wait/notifyAll (backpressure)](#4-bounded-buffer-with-waitnotifyall-backpressure)
5. [Thread pools: ThreadPoolExecutor](#5-thread-pools-threadpoolexecutor)
6. [Callable, Future and the Callable→Supplier adaptor](#6-callable-future-and-the-callablesupplier-adaptor)
7. [CompletableFuture pipeline](#7-completablefuture-pipeline)
8. [Own executors vs the common pool](#8-own-executors-vs-the-common-pool)
9. [Locks, deadlock and the fixes](#9-locks-deadlock-and-the-fixes)
10. [Semaphore: issuer bank bulkhead](#10-semaphore-issuer-bank-bulkhead)
11. [BlockingQueue and poison pill: audit logger](#11-blockingqueue-and-poison-pill-audit-logger)
12. [Atomics, LongAdder and concurrent collections: metrics](#12-atomics-longadder-and-concurrent-collections-metrics)
13. [Decorators: retry, timing, tracing (hand-written AOP)](#13-decorators-retry-timing-tracing-hand-written-aop)
14. [Logging: SLF4J, Logback and MDC](#14-logging-slf4j-logback-and-mdc)
15. [Scheduling and CountDownLatch: settlement](#15-scheduling-and-countdownlatch-settlement)
16. [Graceful shutdown](#16-graceful-shutdown)
17. [Java Memory Model: visibility and happens-before](#17-java-memory-model-visibility-and-happens-before)
18. [Reading the numbers: a real run](#18-reading-the-numbers-a-real-run)
19. [Interview quick-fire Q&A](#19-interview-quick-fire-qa)
20. [Known limitations and next steps](#20-known-limitations-and-next-steps)

---

## 1. Architecture and thread map

```
 producer ──put──► BoundedBuffer(100) ──take──► dispatcher ──execute──► Consumer pool (3..6)
 (50 tps)          wait/notifyAll                                         TransactionProcessor
                                                                          │ authorize(txn).join()
                                                                          ▼
                                          AuthorizationsPipeline (CompletableFuture)
                     ┌──────────────── supplyAsync ────────────────┐
                     ▼                      ▼                      ▼
              FraudCheck (cpu)      BalanceCheck (io)       LimitCheck (io)
              orTimeout 300 ms      RetryingCallable x3
                     └──────────────── allOf → decide ──────────────┘
                                          │ thenCompose (approved && > 10,000)
                                          ▼
                                IssuerBankClient (Semaphore 5, io)
                                          │ exceptionally → CHECK_ERROR
                                          ▼
                     updateAccount: ledger debit (ReentrantLock) → spend tracker
                                    → SettlementService.record → audit → metrics

 Side threads:  audit-logger (BlockingQueue + poison pill)
                metrics-reporter (scheduleAtFixedRate 2 s)
                settlement-scheduler + settlement-1..4 (scheduleWithFixedDelay 10 s, CountDownLatch)
```

| Thread name | Created in | Role |
|---|---|---|
| `producer` | [`TransactionSimulator`](src/main/java/org/grv/ingest/TransactionSimulator.java) | Generates 50 txns/s into the buffer |
| `dispatcher` | `TransactionSimulator` | Takes from the buffer, submits to the consumer pool |
| `Consumer-pool-N` | [`ThreadPoolConfig`](src/main/java/org/grv/config/ThreadPoolConfig.java) | Runs `TransactionProcessor`, waits for the pipeline |
| `cpu-pool-N` | `ThreadPoolConfig` | CPU-bound FraudCheck |
| `IO-pool-N` | `ThreadPoolConfig` | Blocking checks + issuer call |
| `CompletableFutureDelayScheduler` | JDK | Fires `orTimeout` (daemon, not ours) |
| `audit-logger` | [`AuditLogger`](src/main/java/org/grv/audit/AuditLogger.java) | Single writer to `audit.log` |
| `metrics-reporter` | [`Main`](src/main/java/org/grv/Main.java) | Prints metrics every 2 s |
| `settlement-scheduler`, `settlement-N` | [`SettlementService`](src/main/java/org/grv/service/settlement/SettlementService.java) | Batch coordinator + workers |
| `transfer-A`, `transfer-B` | [`DeadLockDemo`](src/main/java/org/grv/DeadLockDemo.java) | Deadlock reproduction |

---

## 2. Requirement → concept → code index

| Req | Feature | Concurrency concept | Code |
|---|---|---|---|
| R1 | Transaction generator | `Thread`, `Runnable`, interrupt | [`TransactionProducer`](src/main/java/org/grv/ingest/TransactionProducer.java) |
| R2 | Bounded buffer | `synchronized`, `wait`/`notifyAll`, backpressure | [`BoundedBuffer`](src/main/java/org/grv/ingest/buffer/BoundedBuffer.java) |
| R3 | Dispatcher + consumers | `ThreadPoolExecutor`, `ThreadFactory`, `CallerRunsPolicy` | [`TransactionSimulator`](src/main/java/org/grv/ingest/TransactionSimulator.java), [`ThreadPoolConfig`](src/main/java/org/grv/config/ThreadPoolConfig.java) |
| R4 | Parallel checks | `Callable`, `Future`, `ExecutionException` | [`FraudCheck`](src/main/java/org/grv/service/authpipeline/FraudCheck.java), [`BalanceCheck`](src/main/java/org/grv/service/authpipeline/BalanceCheck.java), [`LimitCheck`](src/main/java/org/grv/service/authpipeline/LimitCheck.java) |
| R5 | Async pipeline | `CompletableFuture`: `allOf`, `orTimeout`, `exceptionally`, `thenCompose` | [`AuthorizationsPipeline`](src/main/java/org/grv/service/authpipeline/AuthorizationsPipeline.java), [`CallableToSupplierAdaptor`](src/main/java/org/grv/service/CallableToSupplierAdaptor.java) |
| R6 | Account ledger | `ReentrantLock`, deadlock, lock ordering, `tryLock` | [`AccountLedger`](src/main/java/org/grv/service/authpipeline/AccountLedger.java), [`DeadLockDemo`](src/main/java/org/grv/DeadLockDemo.java), [`docs/deadlock-dump.txt`](docs/deadlock-dump.txt) |
| R7 | Audit log | `BlockingQueue`, single writer, poison pill | [`AuditLogger`](src/main/java/org/grv/audit/AuditLogger.java) |
| R8 | Issuer bank limit | `Semaphore`, `tryAcquire(timeout)`, bulkhead | [`IssuerBankClient`](src/main/java/org/grv/issuer/IssuerBankClient.java) |
| R9 | Settlement | `ScheduledExecutorService`, `CountDownLatch`, `drainTo` | [`SettlementService`](src/main/java/org/grv/service/settlement/SettlementService.java) |
| R10 | Metrics | `LongAdder`, `EnumMap`, `scheduleAtFixedRate`, Little's law | [`Matrics`](src/main/java/org/grv/matrics/Matrics.java), [`MatrixReporter`](src/main/java/org/grv/matrics/MatrixReporter.java) |
| R11 | Cross-cutting concerns | Decorator pattern, MDC / `ThreadLocal` propagation | [`RetryingCallable`](src/main/java/org/grv/decorator/RetryingCallable.java), [`TimingExecutor`](src/main/java/org/grv/decorator/TimingExecutor.java), [`TracingExecutor`](src/main/java/org/grv/decorator/TracingExecutor.java) |
| R12 | Graceful shutdown | `shutdown`/`awaitTermination`, ordering, `join` | [`Main`](src/main/java/org/grv/Main.java), `TransactionSimulator.stop()`, `SettlementService.stop()` |

---

## 3. Threads, Runnable and interrupts

**Code:** [`TransactionProducer`](src/main/java/org/grv/ingest/TransactionProducer.java), `TransactionSimulator.transactionPubSub()`

- A `Runnable` is the work; a `Thread` runs it. Call `start()` (new thread), never `run()` (runs on the current thread).
- **Name every thread** (`new Thread(r, "producer")`). Names show up in logs and `jstack`, and unnamed `Thread-7` is useless when debugging.
- **Daemon vs non-daemon:** the JVM exits only when all *non-daemon* threads finish. Pool threads are non-daemon by default, which is why R12 must stop them, or the app never exits.

### Interrupts: the cooperative stop signal
Java can't kill a thread. `thread.interrupt()` only **sets a flag** and wakes it up if it is blocked
in `sleep`, `wait`, `join`, `BlockingQueue.take`, `Semaphore.tryAcquire`, `CountDownLatch.await`…
which then throw `InterruptedException`.

**Throwing `InterruptedException` clears the flag.** So the rule is: **restore it *and* act on it.**
```java
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();   // 1. restore the flag for code further up the stack
    break;                                // 2. actually stop (or return / rethrow)
}
```
- Restoring without acting is a bug: a loop like `while(true)` keeps spinning, each blocking call throws instantly, so the thread burns CPU forever.
- Swallowing it (empty catch) is a bug: the stop request is lost and shutdown hangs.
- Where this pattern is used: producer, dispatcher, `BalanceCheck` sleep, retry backoff, issuer `tryAcquire`, settlement `latch.await`, `AuditLogger.writeLoop`. It's what makes `shutdownNow()` in R12 work.
- `ReentrantLock.lock()` and `synchronized` **ignore** interrupts while waiting; `lockInterruptibly()` and `tryLock(timeout)` respond to them.

---

## 4. Bounded buffer with wait/notifyAll (backpressure)

**Code:** [`BoundedBuffer`](src/main/java/org/grv/ingest/buffer/BoundedBuffer.java)

```java
synchronized (lock) {
    while (capacity == queue.size()) lock.wait();   // full → producer waits
    queue.offer(item);
    lock.notifyAll();
}
```
- **Backpressure:** when consumers are slow, the buffer fills up and `put()` blocks the producer. A bounded buffer protects memory; an unbounded one hides overload until you run out of memory.
- **`while`, not `if`:** a thread can wake up **spuriously**, or another thread may have grabbed the slot first. Always re-check the condition after waking.
- **`notifyAll`, not `notify`:** producers and consumers wait on the **same** monitor for different conditions (not full / not empty). `notify` might wake a thread of the wrong kind, which goes back to sleep, and the right one never wakes (a lost wakeup). `notifyAll` wakes everyone, and each re-checks its own `while`.
- `wait()`/`notify()` must be called while **holding** that object's monitor, otherwise `IllegalMonitorStateException`.
- `wait()` **releases** the monitor while waiting; `sleep()` does **not** release anything.
- A private `lock` object (instead of `this`) stops outside code from synchronizing on our monitor.
- Production equivalent: `ArrayBlockingQueue` / `LinkedBlockingQueue` (uses `ReentrantLock` + two `Condition`s, so it can signal only the right side).

---

## 5. Thread pools: ThreadPoolExecutor

**Code:** [`ThreadPoolConfig`](src/main/java/org/grv/config/ThreadPoolConfig.java), `TransactionSimulator.dispatcherLoop()`

| Pool | core / max | Queue | Work |
|---|---|---|---|
| Consumer | 3 / 6 | 10 | `TransactionProcessor` (waits on the pipeline) |
| cpu | cores / cores | 100 | FraudCheck (busy loop) |
| io | 8 / 18 | 10 | Balance, Limit, Issuer (sleeping = waiting on "I/O") |

### How a task is placed (interview favourite)
1. Fewer than **core** threads running → start a new thread.
2. Otherwise → put it in the **queue**.
3. Queue full → start threads up to **max**.
4. At max and queue full → **rejection policy**.

So with an unbounded queue, `max` is never used. That's why our queues are bounded.

### Rejection policies
| Policy | Behaviour |
|---|---|
| `AbortPolicy` (default) | throws `RejectedExecutionException` |
| `CallerRunsPolicy` (**used here**) | the submitting thread runs the task itself, which slows the submitter: natural backpressure |
| `DiscardPolicy` / `DiscardOldestPolicy` | silently drops the new / oldest task |

⚠️ **`CallerRunsPolicy` after `shutdown()` silently discards the task.** That's the root cause behind the R12 shutdown ordering rules (section 16).

### Sizing
- CPU-bound: about **number of cores**. More threads only add context switching.
- I/O-bound: `threads ≈ cores × (1 + wait time / compute time)`.
- **Little's law:** `L = λ × W`, i.e. items in the system = arrival rate × time each spends there. Example: 100 io calls/s × 0.08 s ≈ **8 busy io threads**. It also explains why the pools looked idle at 1 tps and busy at 50 tps.

### Named `ThreadFactory`
`r -> new Thread(r, "IO-pool-" + counter.getAndIncrement())` gives readable thread names. Use a
**separate counter per pool**, or numbering is shared across pools.

---

## 6. Callable, Future and the Callable→Supplier adaptor

**Code:** the three checks, [`CallableToSupplierAdaptor`](src/main/java/org/grv/service/CallableToSupplierAdaptor.java)

| | `Runnable` | `Callable<T>` |
|---|---|---|
| Returns | nothing | a `T` |
| Checked exceptions | not allowed | `throws Exception` |

- `executor.submit(callable)` returns a `Future<T>`. `future.get()` **blocks** until done.
- `get()` wraps the task's exception in **`ExecutionException`**; the real one is `e.getCause()`.
- Phase 4 version: submit 3 checks, then `get()` each one in turn. It's parallel, but the calling thread blocks.
- `CompletableFuture.supplyAsync` takes a `Supplier`, which can't throw checked exceptions. The adaptor
  bridges them: `RuntimeException`s pass through; `InterruptedException` → restore flag + wrap in
  `CompletionException`; other checked exceptions → wrap in `CompletionException`.

---

## 7. CompletableFuture pipeline

**Code:** `AuthorizationsPipeline.authorize()`

```java
fraud   = supplyAsync(fraudCheck, cpuPool).orTimeout(300 ms).exceptionally(timeout → FRAUD_TIMEOUT)
balance = supplyAsync(retrying(balanceCheck), ioPool)
limit   = supplyAsync(limitCheck, ioPool)

allOf(fraud, balance, limit)
   .thenApply(v -> decide(fraud.join(), balance.join(), limit.join()))   // join() is safe: all are done
   .thenCompose(d -> needsIssuer(d) ? supplyAsync(issuer, ioPool) : completedFuture(d))
   .exceptionally(ex -> declined(CHECK_ERROR))                             // nothing is ever lost
   .thenApply(this::updateAccount);                                        // debit, settle, audit, metrics
```

| Method | Input → output | Use when |
|---|---|---|
| `thenApply(fn)` | `T → U` | transform a result (like `map`) |
| `thenAccept(fn)` | `T → void` | consume at the end, no result |
| `thenCompose(fn)` | `T → CompletableFuture<U>` | the next step is itself async (like `flatMap`); avoids `CF<CF<U>>` |
| `exceptionally(fn)` | `Throwable → T` | recover from failure with a fallback value |
| `handle(fn)` | `(T, Throwable) → U` | see both success and failure |
| `whenComplete(fn)` | `(T, Throwable) → void` | side effect, keeps the original result |
| `allOf(...)` | `CF<Void>` | wait for all; then `join()` each for the values |

### Things that bite
- **Which thread runs a stage?** Non-async stages (`thenApply`) run on the thread that **completed the previous stage**, or on the caller if it was already complete. The `...Async(fn)` variants **without an executor** use the common pool. So a blocking call like the issuer goes in `supplyAsync(..., ioPool)` inside `thenCompose`, never in `thenApply`, where it could park a cpu thread.
- **`orTimeout` doesn't cancel the task.** The future completes with `TimeoutException`, but the FraudCheck keeps running and keeps its cpu thread (visible in `[latency]` cpu run time).
- **`join()` throws `CompletionException`**, while `get()` throws `ExecutionException`. Unwrap with `getCause()` (see `unwrap()` in the pipeline).
- **`exceptionally` position matters:** placed *after* `thenCompose`, so issuer failures also become `CHECK_ERROR` instead of skipping `updateAccount` (losing the txn).
- **Don't block inside the pipeline:** compose with `thenApply`/`thenCompose` instead of calling `.join()` in the middle. The only blocking `join()` is at the very end, in `TransactionProcessor`.
- **Debit after decision, under a lock:** `BalanceCheck` read the balance earlier; another txn may have spent it since. `debit()` re-checks under the account lock, and a failure turns approval into `INSUFFICIENT_BALANCE`. This is the check-then-act fix.

---

## 8. Own executors vs the common pool

Without an executor argument, `supplyAsync` runs on `ForkJoinPool.commonPool()`. This project always passes its own pool:

1. **Too small:** the common pool has `cores − 1` threads. Our io load needs about 8 or more (Little's law), so tasks queue up and `FRAUD_TIMEOUT` fires because of **waiting**, not slow work (`orTimeout` counts from the call, not from task start).
2. **Built for CPU work:** a sleeping task holds a thread doing nothing. Hence separate **cpu** and **io** pools, each sized for its kind of work.
3. **Shared by the whole JVM:** parallel streams, other libraries and every executor-less `...Async` all use it. One slow dependency starves everything. Separate pools act as **bulkheads**.
4. **Not configurable or observable:** no bounded queue or rejection policy, generic thread names, no `getActiveCount()` for our metrics.
5. **Can't be shut down:** `commonPool().shutdown()` does nothing, and its threads are **daemon** threads, so in-flight work is killed silently at JVM exit.

---

## 9. Locks, deadlock and the fixes

**Code:** [`AccountLedger`](src/main/java/org/grv/service/authpipeline/AccountLedger.java), [`DeadLockDemo`](src/main/java/org/grv/DeadLockDemo.java) (`transfer-A` 1→2, `transfer-B` 2→1), thread dump: [`docs/deadlock-dump.txt`](docs/deadlock-dump.txt)

### Why per-account locks
One global lock would serialize every debit in the system. A lock per account lets debits on
different accounts run in parallel; only operations on the *same* account wait. `debit` checks and
subtracts **under the lock**, so check + subtract is atomic and a balance can never go negative.

### The lock idiom
```java
lock.lock();          // BEFORE try: if acquiring fails, finally must not unlock
try { ... }
finally { lock.unlock(); }
```
`ReentrantLock` vs `synchronized`: it adds `tryLock(timeout)`, `lockInterruptibly()`, fairness and
multiple `Condition`s. In exchange, you must unlock manually in `finally`.

### The four transfer variants

| Method | Locking strategy | What I observed in `DeadLockDemo` |
|---|---|---|
| `transferNaive` | lock `from`, then `to` | Froze on the first iteration: no output, CPU idle, never exits. **Deadlock.** |
| `transferWithTryLock` | lock `from`, `tryLock(to, 51 ms)`, give up on timeout | Never hangs, but every call returned `false`. **Livelock.** |
| `transferWithTryLockRetry` | per attempt: lock `from`, `tryLock(to)`, release all; random 1–10 ms backoff; max 5 attempts | Mostly `true`; some calls slower because an attempt failed and retried. |
| `transfer` | lock ordering: always lock the **smaller account id first** | Always `true`, fastest, never stuck. **Used by the app.** |

(The demo variants sleep 50 ms while holding the first lock so the race happens every run.)

### Why the naive version deadlocks
| Time | transfer-A (1 → 2) | transfer-B (2 → 1) |
|---|---|---|
| 0 ms | locks account 1 | locks account 2 |
| 50 ms | waits for 2 (held by B) | waits for 1 (held by A) |
| forever | waiting | waiting |

The thread dump confirms it: `Found one Java-level deadlock`, with both threads `WAITING (parking)` in `ReentrantLock.lock()` called from `transferNaive`.

**Coffman conditions:** a deadlock needs all four, and each fix breaks one:
1. **Mutual exclusion:** one owner per lock.
2. **Hold and wait:** a thread holds one lock while waiting for another. *Broken by `tryLock` + release.*
3. **No preemption:** a lock can't be taken away from its owner.
4. **Circular wait:** A waits for B, B waits for A. *Broken by lock ordering.*

### Fix 1: lock ordering (breaks circular wait)
Both threads lock `min(from, to)` first, so no cycle can form. Money still moves `from → to`; only the *acquisition order* is sorted.
✅ Simple, deadlock impossible. ⚠️ You must know all the locks up front and have a global order for them.

### Fix 2: `tryLock` + timeout + random backoff (breaks hold-and-wait)
A timeout alone gives a **livelock**: both threads time out, release and retry in lockstep. A
**random** backoff, taken while holding *no* locks, breaks the symmetry.
✅ Works without a natural order. ⚠️ More code, can fail after N attempts, adds latency.
`synchronized` can't do this at all: no `tryLock`, no timeout.

### Lessons
- Only unlock what you actually locked: check `tryLock`'s boolean.
- **Never `return` from `finally`:** it overrides the result and swallows exceptions.
- Validate `from != to`. A self-transfer with the "read both balances, write both" approach loses money.
- Diagnose a frozen JVM: `jps -l` to find the PID, then `jstack <pid>` (or `kill -3 <pid>`), and look for "Found one Java-level deadlock". An IDE-wide dump shows the IDE's threads, not your app's.

---

## 10. Semaphore: issuer bank bulkhead

**Code:** [`IssuerBankClient`](src/main/java/org/grv/issuer/IssuerBankClient.java)

Approved transactions above 10,000 call the issuer bank (mocked as a 100 ms call), which allows **at most 5 concurrent calls**.

```java
if (!permits.tryAcquire(500, MILLISECONDS)) return decline(ISSUER_UNAVAILABLE);  // no permit → no release
try { sleep(100); return decision; }
finally { permits.release(); }                                                   // only after a successful acquire
```
- **Semaphore = N permits**; a lock is effectively 1 permit with an owner.
- **No ownership:** any thread can `release()`, even one that never acquired. An extra `release()` silently grows the limit to 6, 7… So `release()` must be reachable **only** after a successful acquire.
- **`tryAcquire(timeout)` vs `acquire()`:** blocking forever lets a slow bank pile up io threads and slow *every* transaction. With a timeout, the damage stays with large transactions only: the **bulkhead pattern**.
- **Fair** (`new Semaphore(5, true)`): waiters get permits in FIFO order, so none starves.
- **Proof:** an `AtomicInteger inFlight` + `maxInFlight.accumulateAndGet(now, Math::max)` shows the limit is never exceeded.
- Wired with `thenCompose` + `supplyAsync(..., ioPool)` so the blocking call never parks a cpu thread, and only for `isApproved() && amount > 10,000`, so declined transactions keep their real reason.

---

## 11. BlockingQueue and poison pill: audit logger

**Code:** [`AuditLogger`](src/main/java/org/grv/audit/AuditLogger.java)

- **Single-writer pattern:** many pipeline threads call `log(line)`, which does `queue.offer(line)`. **One** `audit-logger` thread does `take()` and writes the file. No file locking, no interleaved lines, and callers never wait on disk I/O.
- **Unbounded `LinkedBlockingQueue` + `offer`:** logging never blocks the business path. (The trade-off: unbounded memory under extreme load.)
- **Poison pill shutdown:** `stop()` enqueues a special sentinel and `join()`s the writer. Everything queued *before* the pill is written first, so no line is lost.
- **The pill is compared by identity (`==`)** and created with `new String(...)`, so no real line that happens to have the same text can stop the logger.
- The same pill technique stops the **dispatcher** in R12 (`TransactionSimulator.POISON`).
- `start()` is `synchronized` and guarded, so the writer thread can't be started twice.

---

## 12. Atomics, LongAdder and concurrent collections: metrics

**Code:** [`Matrics`](src/main/java/org/grv/matrics/Matrics.java), [`MatrixReporter`](src/main/java/org/grv/matrics/MatrixReporter.java)

| Tool | Use | Where |
|---|---|---|
| `LongAdder` | counters written by many threads, read rarely | all metrics counters |
| `AtomicInteger`/`AtomicLong` | exact value needed at once, CAS operations | issuer `inFlight`, batch numbers |
| `accumulateAndGet(x, Math::max)` | atomic "update the max" (avoids a check-then-act race) | issuer `maxInFlight` |
| `ConcurrentHashMap` (+ `merge`) | shared map, atomic per-key update | ledger, `DailySpendTracker`, settlement results |
| `EnumMap` pre-filled in the constructor | one `LongAdder` per decline reason; the map itself is never modified afterwards, so concurrent reads are safe | decline breakdown |

- **`LongAdder` vs `AtomicLong`:** under contention, `AtomicLong` makes every thread CAS-retry on one memory location. `LongAdder` spreads increments over cells and adds them up on `sum()`, making writes much faster. The trade-off: `sum()` is not an atomic snapshot.
- **Reporter via `scheduleAtFixedRate(2 s)`:**
  - TPS = Δcount ÷ elapsed seconds, computed as a `double` from `nanoTime`, so the actual interval is used rather than the nominal 2 s.
  - An exception escaping `run()` **silently cancels all future runs**, so `run()` has a try/catch.
- **`%d` with a `double` or a `LongAdder` throws `IllegalFormatConversionException`.** Use `%.1f` / `.sum()`.
- **Counters read "together" can be skewed:** they're updated at different moments without a common lock (e.g. `settled` vs `approved`). Only after shutdown do they have to match exactly.
- **Design choice:** metrics are passed in by constructor (**dependency injection**), not via a singleton. That makes it testable and explicit. If you do write a singleton: double-checked locking needs `volatile`; prefer `static final` or the holder idiom.

---

## 13. Decorators: retry, timing, tracing (hand-written AOP)

**Code:** [`decorator/`](src/main/java/org/grv/decorator)

A **decorator** implements the same interface as the object it wraps, delegates to it, and adds
behaviour before/after. The pipeline only sees `Executor`/`Callable`, so it never knows wrappers exist:
```
supplyAsync(task, cpuExec)
  cpuExec = TracingExecutor → TimingExecutor → ThreadPoolExecutor
  task    = adapt(RetryingCallable(BalanceCheck))
```
This is what Spring AOP does with proxies (`@Retryable`, `@Timed`): cross-cutting concerns kept out of business code. An AOP proxy runs **on the caller's thread**, while `@Async` hands off to an executor.

**Rule: depend on the interface.** Pipeline fields are `Executor`, not `ExecutorService` or
`TimingExecutor`. That's what allows stacking wrappers in any order. Only `Executor` (one method) is
implemented, because `ExecutorService` would need about 12 delegating methods.

### `RetryingCallable<T> implements Callable<T>`
- Retries **only `TransientException`**. Real bugs and `InterruptedException` propagate immediately.
- **Exponential backoff + jitter:** `base × 2^(attempt−1) + random(0..base)`. Jitter prevents retry storms in lockstep (the same idea as the R6 livelock fix).
- Rethrows the **original** exception on the last attempt. It never returns `null` (an early bug: a null became an NPE in `decide()` and hid the real cause).
- An interrupt during backoff → restore the flag and throw; never keep retrying.
- **Retry only idempotent operations.** `BalanceCheck` only reads; a blind retry of `debit` could charge twice.
- Result: about 5% of balance checks fail once, but `CHECK_ERROR` drops to about 0.05³ ≈ 1 in 8,000.

### `TimingExecutor implements Executor`
```java
long submitted = nanoTime();                     // caller thread, now
delegate.execute(() -> {
    long started = nanoTime();                   // worker thread, later
    queueWait += started - submitted;
    try { command.run(); } finally { runTime += nanoTime() - started; }
});
```
- Splits latency into **queue wait** (pool too small → add threads) and **run time** (work is slow → fix the dependency). End-to-end latency alone can't tell those apart.
- `nanoTime` for intervals; `currentTimeMillis` can jump (NTP).
- Shows that `orTimeout` doesn't cancel: cpu run time includes the 400 ms FraudChecks that already "timed out".

### `TracingExecutor implements Executor`: MDC propagation
- MDC is a **`ThreadLocal` map**. `txnId` is put on the consumer thread, but the checks run on cpu/io threads, where it would be empty.
- Capture `MDC.getCopyOfContextMap()` in `execute()` (the **submitting** thread); on the worker: save previous → set captured → run → **restore previous** in `finally`.
- **Restore, don't just clear:** with `CallerRunsPolicy` the wrapper may run on the *caller's* thread, and clearing would wipe its own `txnId`.
- **Why `finally` matters:** pool threads are reused. A leftover value means the next task logs the wrong `txnId` (the classic ThreadLocal leak).
- **Why not `InheritableThreadLocal`:** it copies only when a thread is *created*. Pool threads are created once and reused.
- **Limit:** the `orTimeout` timeout fires on the JDK's `CompletableFutureDelayScheduler` thread, so stages run there have no MDC. Executor-based propagation only covers threads you submit to.

---

## 14. Logging: SLF4J, Logback and MDC

**Code:** [`logback.xml`](src/main/resources/logback.xml), loggers in `TransactionProcessor`, `AuthorizationsPipeline`, decorators, settlement

- **SLF4J is the API (facade)**, Logback the implementation. Code imports only `org.slf4j.*`, so the backend can be swapped.
- Pattern: `%d{HH:mm:ss.SSS} [%thread] [txn=%X{txnId}] %-5level %logger{20} - %msg%n`. `%thread` shows which thread ran each stage; `%X{txnId}` reads the MDC.
- **Placeholders, not `String.format`:** `log.info("txn={} {}", id, status)`. The string is only built if the level is enabled.
- ⚠️ **Trailing-`Throwable` rule:** if the last argument is a `Throwable`, Logback treats it as *the exception* (prints a stack trace) and doesn't use it for a `{}`. Use `e.toString()` for a one-liner, or leave no placeholder to get the trace.
- **Levels:** `info` = normal flow, `warn` = handled problem (check failed, debit failed), `error` = unexpected failure.
- `AuditLogger` is separate on purpose: it's business data (a file of decisions), not diagnostics.

---

## 15. Scheduling and CountDownLatch: settlement

**Code:** [`SettlementService`](src/main/java/org/grv/service/settlement/SettlementService.java)

Every 10 s, settle all approved transactions since the last batch, grouped by merchant, in parallel.

```
record(txn)            → pending.offer(txn)              // many threads, LinkedBlockingQueue
batch (scheduler):
  pending.drainTo(batch)                                 // atomic hand-off: this batch or the next, never both
  group by merchant                                      // one thread only → no locks (thread confinement)
  latch = new CountDownLatch(groups)
  per group → workers.execute(settle; finally latch.countDown())
  latch.await(5 s)                                       // wait for all, but never forever
  summary → audit log + metrics
```

| | `scheduleAtFixedRate` (metrics) | `scheduleWithFixedDelay` (settlement) |
|---|---|---|
| Next run | period measured from the **start** of the previous run | delay measured from the **end** of the previous run |
| Slow run | following runs start late and back-to-back (never in parallel) | always a full gap |
| Good for | steady clocks: metrics | variable-length jobs: batches |

- **Exceptions in scheduled tasks** cancel all future runs silently, so wrap the job body (`safeRunBatch`).
- **CountDownLatch:** one thread waits until N tasks call `countDown()`.
  - `countDown()` goes in **`finally`**, or a failing task makes `await()` wait forever.
  - **One-shot:** it can't be reset, so create a new one per batch. (`CyclicBarrier` is the reusable, all-wait-for-each-other variant.)
  - **Alternatives:** `invokeAll` (also returns results and exceptions), `CompletableFuture.allOf`.
- **Timeout ≠ cancellation:** on `await` timeout, late merchants are reported as unsettled, but their worker may still finish (same lesson as `orTimeout`).
- **Only debited money is settled:** `record()` is called right after a successful `debit()`.

---

## 16. Graceful shutdown

**Code:** end of [`Main`](src/main/java/org/grv/Main.java), `TransactionSimulator.stop()`, `SettlementService.stop()`, `AuditLogger.stop()`

**Rule: stop upstream before downstream.** A stage stops only after everything feeding it has stopped.

| # | Step | Why here |
|---|---|---|
| 1 | Interrupt + `join` the producer | no new transactions |
| 2 | Put `POISON` into the buffer, `join` the dispatcher | everything already buffered is dispatched first; the pill is guaranteed last |
| 3 | `shutdown` + `awaitTermination` the consumer pool | each `TransactionProcessor` `join()`s its pipeline, so when this pool terminates, every pipeline is finished |
| 4 | Then the cpu and io pools | earlier would be fatal: `CallerRunsPolicy` silently **discards** tasks after shutdown → a `supplyAsync` never completes → the consumer's `join()` hangs forever |
| 5 | Settlement `stop()`: stop the scheduler, run a **final batch**, stop the workers | pipelines feed `record()`, so this must come after them |
| 6 | Stop the metrics scheduler, print one final report | shows the end state |
| 7 | `AuditLogger.stop()` (poison pill + join) | **last**, because everyone writes to it |
| 8 | Checks: `produced == received`, `received == approved + declined`, `settled == approved`, queues empty | proof that nothing was lost |

```java
pool.shutdown();                                   // no new tasks; finish queued + running ones
if (!pool.awaitTermination(10, SECONDS)) {
    pool.shutdownNow();                            // interrupts running tasks → interrupt handling pays off
}
```
- `shutdown()` = polite (drain); `shutdownNow()` = interrupt + return the tasks that never started.
- **No `System.exit()` on success:** the JVM exiting by itself proves no non-daemon thread was left running. If it hangs, `jstack` shows the forgotten thread.
- Optional extension: a `Runtime.addShutdownHook` for Ctrl+C, guarded with an `AtomicBoolean` so the shutdown runs only once.
- **A bug I hit:** declaring `Thread producerThread = ...` inside a method **shadowed** the field, which stayed `null`, so `stop()` threw an NPE. Assign the field without re-declaring the type.

---

## 17. Java Memory Model: visibility and happens-before

Without synchronization, a thread may **never see** another thread's writes (CPU caches,
reordering). A **happens-before** edge guarantees visibility. This project relies on:

| Edge | Example here |
|---|---|
| Unlocking a lock / leaving `synchronized` → the next lock of the same monitor | `AccountLedger`, `BoundedBuffer` |
| Putting into a concurrent collection → a later read from it | `BlockingQueue`s, `ConcurrentHashMap` |
| `Thread.start()` → everything the new thread does | fields set before `start()` are visible |
| Everything a thread did → `join()` returning | `tf.produced()` reads the producer's counter after `join`, no `volatile` needed |
| Task completion → `Future.get()` / `CompletableFuture.join()` | check results read in `decide()` |
| `volatile` write → later read | needed for double-checked locking |
| `final` fields after construction | immutable `record`s (`TransactionRecord`, `Decision`) are safe to share |

**Immutability is the cheapest thread safety:** records with `final` fields can be passed between threads freely.

---

## 18. Reading the numbers: a real run

```
[metrics] received=1814 approved=946 declined=855 {SCORE_DECLINE=228, LIMIT_DECLINE=596,
          FRAUD_TIMEOUT=30, CHECK_ERROR=1} inFlight=13 retried=77 tps=51 processedTps=50 settled=884
```
| Number | Meaning |
|---|---|
| `tps=51 processedTps=50` | steady state: keeping up with arrivals |
| `inFlight=13` | Little's law: 50 tps × ~0.25 s ≈ 12.5 ✅ |
| `1814 = 946 + 855 + 13` | nothing lost |
| `retried=77`, `CHECK_ERROR=1` | retry decorator absorbs about 5% transient failures |
| `FRAUD_TIMEOUT=30` (≈1.7%) | matches the 2% forced-slow FraudChecks, not queueing |
| `settled=884` vs `approved=946` | about 60 waiting for the next batch (< 1 interval) |
| no `ISSUER_UNAVAILABLE` | about 2.5 large txns/s × 0.1 s ≈ 0.25 permits used: far from 5 |
| `LIMIT_DECLINE` ≈ ⅓ | simulation effect: 20 cards × ~2,500/s spend hit the 25,000 daily limit in about 10 s, and the "day" never resets |

`[threads]` shows pool activity, buffer fill and issuer permits; `[latency]` shows per-pool queue wait vs run time.

---

## 19. Interview quick-fire Q&A

**Threads & basics**
- *`start()` vs `run()`?* `start()` creates a new thread; `run()` just calls the method on the current thread.
- *`sleep` vs `wait`?* `sleep` keeps all locks; `wait` releases the monitor and needs to be called while holding it.
- *Why `wait()` in a `while` loop?* Spurious wakeups, and another thread may change the condition first.
- *`notify` vs `notifyAll`?* With several conditions on one monitor, `notify` can wake the wrong waiter (lost wakeup).
- *How do you stop a thread?* Interrupt it, and have its code check the flag / handle `InterruptedException`. Never `Thread.stop()`.
- *Why restore the interrupt flag?* Catching `InterruptedException` clears it; callers further up need to see it.

**Executors**
- *Core vs max vs queue order?* core threads → queue → grow to max → reject.
- *Why bounded queues?* An unbounded queue never triggers `max` or the rejection policy, and hides overload until OOM.
- *What does `CallerRunsPolicy` do?* The submitter runs the task: backpressure. After shutdown it **discards** tasks.
- *How to size pools?* CPU ≈ cores; I/O ≈ cores × (1 + wait/compute); check with Little's law.
- *`shutdown` vs `shutdownNow`?* Drain vs interrupt; always follow with `awaitTermination`.
- *Why not `Executors.newFixedThreadPool` everywhere?* Its queue is unbounded; prefer an explicit `ThreadPoolExecutor`.

**Futures**
- *`Future.get` vs `CompletableFuture.join`?* Checked `ExecutionException` vs unchecked `CompletionException`.
- *`thenApply` vs `thenCompose`?* map vs flatMap: use `thenCompose` when the function returns a future.
- *Which thread runs `thenApply`?* The one that completed the previous stage (or the caller if already done).
- *Does `orTimeout` cancel the task?* No. It only completes the future exceptionally.
- *Why pass your own executor to `supplyAsync`?* The common pool is small, shared, CPU-oriented, unobservable and can't be shut down.

**Locks & synchronizers**
- *`synchronized` vs `ReentrantLock`?* The lock adds tryLock/timeout, interruptible locking, fairness and multiple Conditions; you must unlock in `finally`.
- *Four deadlock conditions? How to prevent?* Mutual exclusion, hold-and-wait, no preemption, circular wait. Use lock ordering or tryLock + backoff.
- *Deadlock vs livelock vs starvation?* Blocked forever / busy but making no progress / never getting a turn.
- *How to find a deadlock?* `jstack <pid>` → "Found one Java-level deadlock".
- *Semaphore vs lock?* N permits, no owner, any thread can release.
- *CountDownLatch vs CyclicBarrier?* Latch: one-shot, waiters wait for N events. Barrier: reusable, N threads wait for each other.
- *Check-then-act race example?* Check the balance, then debit later. Fix: re-check and act under one lock.

**Concurrent data**
- *`LongAdder` vs `AtomicLong`?* Striped cells make it faster under write contention; `sum()` isn't a snapshot.
- *Is `ConcurrentHashMap.get` then `put` atomic?* No. Use `merge`/`compute`/`putIfAbsent`.
- *What is a poison pill?* A sentinel in a queue telling the consumer to stop after draining; compare by identity.
- *What is happens-before?* The JMM rule that guarantees visibility: locks, volatile, start/join, concurrent collections, futures.
- *Why does double-checked locking need `volatile`?* Without it, another thread can see a non-null reference to a not-yet-constructed object.

**Design**
- *What is the decorator pattern here?* Retry/timing/tracing wrappers around `Callable`/`Executor`: hand-written AOP.
- *Why doesn't MDC show up in pool threads?* It's a ThreadLocal; propagate it by capturing at submit and restoring in the worker.
- *Why retry only idempotent operations?* Retrying a non-idempotent call (debit) can apply it twice.
- *What is a bulkhead?* A limit on how much of the system one slow dependency can occupy (separate pools, semaphores).
- *What is graceful shutdown?* Stop intake, drain in upstream→downstream order, bounded waits, verify, let the JVM exit by itself.

---

## 20. Known limitations and next steps

- **Daily limit never resets:** add a scheduled "midnight" reset of `DailySpendTracker` for realistic long runs.
- **Settlement timeout:** late merchants are reported as unsettled; a real system would retry them in the next batch idempotently.
- **`orTimeout` stages lose MDC:** they run on the JDK delay thread. Pass `txnId` explicitly in those log lines, or use `...Async(fn, executor)`.
- **Retry backoff sleeps on an io thread:** fine at these sizes; at scale use a scheduler (`delayedExecutor`) instead of sleeping.
- **Stretch:** benchmark `BoundedBuffer` vs `ArrayBlockingQueue`; try virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) for the io pool.
- **After the project:** a JDK dynamic `Proxy` timing wrapper, to see how Spring-style interface proxies work.
