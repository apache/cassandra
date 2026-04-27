# Minimization Guide

A minimal reproducer has exactly the code needed to trigger the bug — no more. Minimization makes the root cause obvious, removes false leads, and makes the test fast to run.

## The subtraction method

Minimization is a **binary search over code removal**:

1. Start with the first version that triggers the bug
2. Identify a candidate to remove (a config, a step, a dependency, a data item)
3. Remove it
4. Run the test
5. **Bug still fails** → keep the removal, continue
6. **Bug no longer fails** → undo the removal (that element is essential)
7. Repeat until nothing more can be removed

Work through these dimensions in order (cheapest reductions first):

### Dimension 1: Infrastructure scale

| What to try | How |
|---|---|
| Reduce service/instance count | Run with 1 instance instead of N |
| Reduce data partitions/shards | Use 1 partition/shard instead of many |
| Reduce replication/redundancy | Use minimal replication factor |
| Remove extra resources | Delete all resources (tables, queues, topics, buckets) except the one involved in the trigger |

If the bug disappears when you reduce instance count, that's an important signal — the bug involves distributed coordination. Keep the minimum count that reproduces it.

### Dimension 2: Data volume

| What to try | How |
|---|---|
| Reduce item count | Process 1 item instead of 100/1000 |
| Reduce item size | Use minimal payloads instead of large ones |
| Reduce batch size | Remove batching tuning, let defaults apply |

### Dimension 3: Configuration

Remove each non-default config one at a time. Start with configs that seem unrelated to the bug:

```
# Before: many custom settings
config.set("timeout", 5000)
config.set("retries", 3)
config.set("batch_size", 16384)
config.set("compression", "snappy")
config.set("buffer_size", 33554432)
config.set("max_connections", 10)
config.set("cache_ttl", 60)
config.set("log_level", "debug")

# Try removing each — if bug persists without it, delete it
```

### Dimension 4: Setup steps

Remove preparatory operations one at a time:
- Initial "warmup" operations before the trigger
- Extra resource creation steps
- Pre-population of data that may not be needed
- Initialization sequences that precede the trigger

Ask: "Does the bug appear on a fresh state, or only after some prior state is established?" If prior state is needed, keep only the minimum state setup.

### Dimension 5: Timing

Replace real sleeps with deterministic time control:

```
# BEFORE (fragile, slow)
service.send(request)
sleep(5000)  # wait for async processing
assert_expected()

# AFTER (fast, deterministic)
# Option A: Use a fake/mock clock and advance it exactly
fake_clock.advance(exact_threshold + 1)
assert_expected()

# Option B: Poll for the condition with a timeout
wait_until(lambda: condition_is_met(), timeout=30_000, message="condition not reached")
```

If `sleep()` is in the test because the code needs time to do something asynchronous, use a polling/retry helper with a timeout instead.

### Dimension 6: Trigger sequence

Sometimes the trigger sequence can be simplified:
- Can the bug be triggered with a single API call instead of a sequence?
- Can the trigger be a direct call to an internal function instead of going through the public API?
- Can the ordering be simplified (A then B, instead of A, B, C, B, A, C)?

---

## What "minimal" looks like in practice

**Before minimization** (real-world repro attempt):
```
3 service instances, 6 partitions, replication=3, 100 data items,
5 custom configs, warmup operations, 10s sleep, then trigger
```

**After minimization**:
```
1 instance, 1 partition, no replication, 1 data item, default configs,
direct trigger call
```

If the bug requires 2 instances (distributed coordination), keep 2. If it requires 3 partitions (hash-based routing), keep 3. The goal is **necessary minimum**, not arbitrary minimum.

---

## Verification checklist

Before declaring the repro final:

-  Test fails with the expected exception or wrong value (not a test setup error)
-  Test passes after the bug fix is applied (confirms it's a true repro, not a pre-existing failure)
-  Removing any single setup step makes the test pass (confirms minimality)
-  Test name/description references the issue tracker ticket
-  Documentation states: expected behavior, actual (buggy) behavior
-  No `sleep()` unless truly necessary (prefer fake clocks or polling helpers)
-  No hardcoded ports, paths, or machine-specific values

---

## Common traps

**Trap: Test passes on first run, fails intermittently**
The bug is a race condition. Use a fake clock to control ordering, or use synchronization primitives (barriers, latches, controlled scheduling) to inject deterministic interleavings. A flaky repro is not a repro.

**Trap: The bug only shows up after many iterations**
Reduce the iteration count to the minimum. If the bug requires N iterations, find the minimum N and document why. Consider using a loop with an early-exit assertion.

**Trap: The test fails but for the wrong reason**
The setup may be broken (wrong resource, wrong config, wrong state). Always read the failure message. A `NullPointerException` in the setup is not the same as the `NullPointerException` caused by the bug.

**Trap: Minimization accidentally tests the wrong thing**
After each removal, re-read the assertion. If you stripped away too much, the test might still fail but now for a different reason. The failure message should stay the same throughout minimization.
