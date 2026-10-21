# Concurrency Bug Repros

Concurrency bugs depend on scheduling — the same code produces different outcomes depending on thread interleaving. This makes them hard to trigger reliably and easy to shrink into a different bug.

## Principles

1. **Do not use `Thread.sleep()` as synchronization.** Sleep-based tests are flaky: they fail on slow hardware and pass on fast hardware, or vice versa. They also make tests slow.

2. **Make the schedule explicit.** Use one of these approaches:
   - Barriers, latches, and condition variables to force a specific interleaving
   - Stress harnesses that run many iterations and report failure rate
   - Controlled concurrency testing (CCT) tools that systematically explore interleavings
   - Deterministic simulation that controls the scheduler

3. **Report failure rate.** If the bug is probabilistic, run N iterations and report how many failed.

4. **Print and record seeds.** Any randomized element (thread scheduling, input generation) must print its seed so failures can be replayed.

## Approach 1: Event-based schedule control

Force a specific interleaving using barriers or latches:

```
// Pseudocode — the general pattern
barrier = new CyclicBarrier(2)

thread1 {
    step1()        // T1 does step 1
    barrier.await() // sync point: both threads reach here
    step3()        // T1 does step 3 (after T2 did step 2)
}

thread2 {
    barrier.await() // wait for T1 to finish step 1
    step2()        // T2 does step 2
}

// ORACLE: check final state
```

This makes the test deterministic — it always produces the same interleaving. Use this when you know exactly which schedule triggers the bug.

## Approach 2: Stress harness (N-iteration)

Run the trigger many times and check the invariant after each:

```
// Pseudocode
failures = 0
for i in 1..N:
    result = run_concurrent_scenario()
    if not invariant(result):
        failures++
        record_failure(i, result)
        break  // or continue to measure rate

assert failures > 0, "bug did not reproduce in N iterations"
// Or for showing the bug is fixed:
assert failures == 0, f"failed {failures}/{N} times"
```

Guidelines:
- N=1000 is a reasonable starting point
- Report the failure rate (e.g., "fails 23/1000")
- Record the first failure details for diagnosis
- Use a timeout per iteration to catch hangs

## Approach 3: jcstress (Java)

jcstress is the gold standard for Java Memory Model bugs. It runs millions of iterations automatically and reports observed outcome frequencies.

```java
@JCStressTest
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Sequentially consistent")
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Both early reads")
@Outcome(id = "0, 1", expect = Expect.FORBIDDEN, desc = "Broken invariant")
@State
public class ReproRace {
    int x; int flag;

    @Actor
    public void writer() {
        x = 42;
        flag = 1;
    }

    @Actor
    public void reader(II_Result r) {
        r.r1 = flag;
        r.r2 = x;
    }
}
```

Each `@Actor` method runs in its own thread. jcstress explores many interleavings and reports which `@Outcome` patterns were observed.

## Approach 4: Controlled concurrency testing (CCT)

Systematic tools that explore thread interleavings:

- **CHESS**: iterative preemption bounding — explores all interleavings with up to K preemptions
- **PCT/PPCT**: probabilistic concurrency testing — assigns random priorities to threads, mathematically guaranteed coverage
- **QL**: Q-learning-based exploration — learns which interleavings are interesting
- **Period**: periodical scheduling — imposes periodic preemption patterns

These are research tools; in practice, most teams use stress harnesses or jcstress.

## Approach 5: Linearizability checking

For concurrent data structures or distributed systems, check that the observed history is linearizable:

1. Record a history of operations with start/end timestamps and return values
2. Feed it to a linearizability checker
3. The checker returns true (history is linearizable) or false (violation found)

Tools:
- **Porcupine** (Go): fast, handles concurrent maps/registers/queues. 1000x-10,000x faster than Knossos.
- **Knossos** (Clojure): Jepsen's checker, handles arbitrary models
- **Elle** (Clojure): specifically for database transaction isolation — detects G0, G1a/b/c, G2 anomalies

History format (Porcupine):
```go
history := []porcupine.Event{
    {Kind: porcupine.CallEvent, Value: WriteInput{Key: "k", Value: 1}, Id: 0},
    {Kind: porcupine.ReturnEvent, Value: WriteOutput{}, Id: 0},
    {Kind: porcupine.CallEvent, Value: ReadInput{Key: "k"}, Id: 1},
    {Kind: porcupine.ReturnEvent, Value: ReadOutput{Value: 0}, Id: 1}, // stale read!
}
ok := porcupine.CheckEvents(model, history)
// ORACLE: ok should be true; if false, linearizability violated
```

## Common interleaving patterns that cause bugs

1. **Check-then-act**: `if (x != null) x.method()` — x can become null between check and use
2. **Read-modify-write**: `counter = counter + 1` — lost update without atomicity
3. **Publication without barrier**: object constructed but reference published without happens-before
4. **Double-checked locking**: broken without volatile/atomic in many memory models
5. **Iterator invalidation**: modifying a collection while iterating over it concurrently
6. **Close-during-use**: resource closed by one thread while another is using it

## What to include in a concurrency repro

1. The exact threads and their operations
2. The specific interleaving (or schedule constraint) that triggers the bug
3. Whether the bug requires specific hardware (# cores, NUMA, weak memory model)
4. Failure rate if the test is probabilistic
5. Seeds if any element is randomized
6. Thread dump or outcome log showing the violation
