<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Invariants of the Accord execution queues

Reference for `tla/AccordExec.tla`. Each invariant names the code that establishes it and the code
that relies on it. §1–2 define the objects; §3 is local and checked by `validate()`; §4 is global
and is where the acyclicity argument lives.

## 1. Objects

| | |
|---|---|
| `E` | cache entries; `kind(e) ∈ {CMD, CFK}`, `status(e) ∈ {LOADING, LOADED, …}` |
| `T` | tasks (`SafeTask`) |
| `region(t)` | `FIFO ≺ ORDERED ≺ BAG` |
| `prefix(e)` | the runnable prefix: fifo head, else sorted head, else the whole bag (`runnablePrefix()`, Q4) |
| `lock(e)`, `mode(e)` | the lock holder and `UNLOCKED / RELEASE_QUEUE / HOLD_QUEUE / UNQUEUED` |
| `fifoAt(t)` | stamped when `t` first becomes a fifo claim; inherited by an ATOMIC consequence |

## 2. The wait relation

```
W == Wpos ∪ Wlock

Wpos  == { (a,b) : ∃e. pos(a,e) ≠ ⊥ ∧ pos(a,e) ∉ prefix(e) ∧ b ∈ prefix(e) ∧ status(e) ≠ LOADING }
Wlock == { (a,b) : ∃e. mode(e) = HOLD_QUEUE ∧ lock(e) = b ∧ pos(a,e) ≠ ⊥ ∧ a ≠ b
                       ∧ status(e) ≠ LOADING }
```

The `status(e) ≠ LOADING` conjunct on `Wlock` is vacuous — locking requires leading a
loaded entry, and `loading` only ever shrinks — but `AccordExec.tla`'s `WaitsFor` applies it
to both disjuncts, so it is stated here too rather than leaving the two to differ.

Deadlock ⟺ a cycle in `W`. Three assumptions, each discharged separately:

- **A1** a `LOADING` entry carries no edge — nothing is runnable on it and the load completes
  independently of any task (`AccordCacheEntry.add` passes a null owner while `!isLoaded()`).
- **A2** a run never waits for a task — `RELEASE_QUEUE`/`UNQUEUED` are returned within the run that
  takes them, and a consequence is queued on `Task.next` until its submitter completes. Hence only
  `HOLD_QUEUE` yields a lock edge.
- **A3** one task per store is prepared, run and completed at a time (`ExclusiveExecutor` holds a
  single `task` and polls the next only in `completeTask()`). This is what makes it safe for a whole
  bag to be in the runnable prefix at once: its members do not wait for each other and are each
  told they may run, and they take the entry's lock one at a time. Without A3 the second bag member
  to prepare would trip `lockExclusive`'s `require(!isLocked())`. `Inv_AtMostOneLock` is the
  model-side statement that no two tasks hold `HOLD_QUEUE` on one entry, which `Prefix`
  would otherwise hide by choosing arbitrarily among holders. `CanRun` *assumes* that guard
  (`NoForeignLock`) rather than deriving it, so the invariant is falsifiable only under the
  `ctl-double-lock` control (`PAllowDoubleLock`) — which also turns off the O7 upgrade,
  because while the holder is pinned at the fifo head no second task can lead the entry and
  reach the lock at all. Without that control the row is a tautology, which is the vacuity
  the control exists to rule out.

## 3. Representation (per entry, local — `AccordCacheEntryQueue#validate`)

| | |
|---|---|
| **R1** | `[priorityHead, priorityTail+unsequencedSize)` and `(fifoTail, fifoHead]` are non-null; every other slot is null |
| **R2** | every task occupies *exactly one* queue slot (`validateMembership`). The lock slot is not a position; a `HOLD_QUEUE` holder occupies the lock slot **and** the fifo head. A duplicate is fatal — the task waits for itself |
| **R3** | the sorted region is sorted by `compare` (Q1) |
| **R4** | every bag member sorts after every sorted member (Q2), maintained by `extendPriorityRegion`/`ensureSorted`; this is what makes an O(1) bag insertion sound |
| **R5** | `isLocked() ⟺ the lock holder is recorded`, and `isLockedHoldingQueue() ⇒ hasFifo() ∧ tasks[fifoHead] = lock(e)` |
| **R6** | no *failed* task holds a position (`requireNotFailed`) — with one exception, added with partial failure: on an entry that is `isInconsistent()`, a failed task that is a continuation or has started incrementally may keep its position, because it is the task that poisoned the entry and nothing is meant to run there again |
| **R7** | every non-prefix member is in a state meaning "waiting on caches" (`isWaitingOnCaches`) |
| **R8** | `queue` is `null \| SafeTask \| AccordCacheEntryMiniQueue \| AccordCacheEntryQueue`; `ensureQueue`/`maybeUnwrap` preserve the abstract queue, with `isLiveQueue` guarding re-entrant replacement. The mini queue is the two-claim case and exists only while locked (`miniQueue()` requires `isLocked()`); its head is the holder if `HOLD_QUEUE`, else the other claim |

R1/R3/R4/R8 are array (and union) algebra and are not modelled; R2, R5, R6 are.

## 4. Ordering (global — the acyclicity argument)

**O1 pair-determined order.** `compare(a,b)` = `(position, executionKind, createdAt)` with
`createdAt` unique per store, so it is a strict total order and *the same on every entry a pair
shares*. Stated in the `AccordCacheEntryQueue` javadoc: *"the order imposed on any two tasks is a
function of the pair alone … this is what makes the scheme deadlock free"*.

**O2 region stratification.** `FIFO ≺ ORDERED ≺ BAG`, and bag members never wait for one another
(Q3/Q4). So an `UNSEQUENCED` task cannot run while anything sequenced is queued on an entry it
declared — including into the middle of an ATOMIC unit. That is *at least* what the semantics asks
for: `ExecutionSequence.ATOMIC` promises to appear atomic "with respect to other tasks", full stop
(earlier revisions of these documents quoted a narrower promise, "from the point of view of other
SEQUENCED tasks" — no such text exists in the tree, and `UNSEQUENCED`'s own javadoc claims only
freedom from ordering, not freedom to interleave into someone else's unit). It is nevertheless
stronger than an unsequenced task's *own* declared requirement, and no weaker formulation is known
that stays acyclic: BAG is the top layer of the rank, so a bag member in the runnable prefix gives
every sequenced task behind it an edge that cannot decrease `⟨layer, key⟩`, while `Wlock` still gives
the bag member an outgoing edge to any `HOLD_QUEUE` holder of an entry it declared.
`AccordExec.tla`'s `PBagInterleaves` demonstrates both, and which topologies close a real cycle; see
README, "What an interleaving bag would cost".

**O3 uniform region.** A task's region is a function of the task alone. Established by
`setSequencedExclusive(context.executionSequence())`, now applied to top-level tasks as well as to
consequences, plus `requireSequencedIfHoldsLocksBetweenRuns` (O11): with an UNSEQUENCED task barred
from declaring a txnId when it is INCR, an unsequenced task holds no CMD entry it would be ordered
on, so `isUnsequenced(entry)`'s `entry.isCommandsForKey() || !isIncremental()` qualifier is vacuous —
and `isUnsequenced(entry)` now **asserts** exactly that, so the uniformity `RankOK` needs is checked
at runtime rather than argued. (Before the sequence was applied at top level, a top-level INCR task
with a txnId was ORDERED on its command entries and BAGGED on its keys, which is a per-entry region
and admits a two-task cycle; that is what the assertion now excludes.) Note that this is *not* a
model invariant: `AccordExec.tla`'s `Region` is a function of the task by construction, so the model
cannot express its violation — `ctl-unseq-incr-txn` removes the guard that makes it true instead.

**O4 single-pass claim of everything.** `waitOnTxnsExclusive` takes the txnId positions and then
calls `queueOnKeysExclusive` in the same turn, and a task keeps every position until it is done.
Losing a txnId therefore revokes nothing: `incrementWaitingTxns` only counts the wait, leaving the
positions and the key waits counted for them in place, so `waitOnKeysExclusive` has nothing to
re-place when the txnId comes back. There is no state in which a task holds txnIds and not keys.
(The former two-phase claim, its `completeChangeOfRunnableStatus` revocation and the
`DeferredChangeOfRunnableStatus` that made the revocation safe inside a notification loop were all
removed with `QUEUE_ON_KEYS_AT_ONCE`; nothing models them.)

**O5 indivisible setup.** An ATOMIC task takes its fifo position on every entry it declares in one
uninterrupted setup pass; enforced negatively by `addCachedKeyExclusive`'s
`require(!isCacheQueuedFifo())`. That guard protects *isolation*, not acyclicity: a fifo claim that
adopts an entry after its setup pass is placed by its stamp, so an older-stamped task that adopts late
is entitled to run before a younger unit's consequence, and lands between that consequence and its
submitter. `ctl-fifo-adopt` is the witness — every ordering property stays green and `Inv_Isolation`
breaks — which settles the "likely" in the code comment there.
**O6 → Q5 fifo order.** The fifo region is ordered by `fifoAt`, stamped when a task first becomes a
fifo claim, ties broken by `createdAt`. `fifoAt` is inherited by an ATOMIC consequence, so ties are
real; the tie-break is pair-determined, and orders a consequence after its submitter because the
submitter's run is what created it. *Not* arrival order — `addFifo` inserts by stamp, so position is
independent of when the claim arrives.

**O7 upgrade-on-start.** In `prepareExclusiveMayThrow`, an INCR task that will hold a lock across
runs — or that is ATOMIC and so owes an isolation guarantee — is stamped and moved to fifo on every
entry it holds, **then** takes `HOLD_QUEUE`, **then** sets the started bit
(`isIncremental() && (holdsLocksBetweenRuns() || isAtomic()) && !isCacheQueuedFifo()`). So any task that can
retain a lock is a fifo claim everywhere. Load-bearing twice: it also licenses the O11 relaxation.
An ATOMIC consequence is already a fifo claim from setup, so the upgrade reaches only a task that is
not one: an INCR lock holder, or a top-level ATOMIC INCR task. The notification side of the move is
modelled by `AccordNotify`'s `Upgrade` — `moveToFifo` removes with a null owner (silently) and re-adds
with the real one, and the mover's own status comes back as a return value that `onKeyMovedToFifo`
folds in rather than a notification.

**O8 lock requires leading, and keeps the head.** `lockExclusive` requires the locker to lead:
`RELEASE_QUEUE` passes `REQUIRE_RUNNABLE`, `HOLD_QUEUE` requires the fifo head. Since Q5 makes
insertion independent of arrival, a claim with a lower stamp arriving later could displace a locker,
which it must not: the locker cannot yield the prefix, and an edge into a position in front of it
runs against the stamp order that makes `W` acyclic. `addFifo` therefore keeps a `HOLD_QUEUE` holder
at the head — but that placement is only *safe*, not *ordered*, so reaching it means a stamp was
issued out of order and the rank certificate no longer applies. It is expected to be unreachable:
`Invariants.expect` reports it, and `Inv_LockLeads` is the model-side statement of the same claim
(the holder is the least-stamped fifo claim on its entry, not merely the pinned one).

**O9 cross-run locks are CMD-only.** `holdsLocksBetweenRuns() = isIncremental() && primaryTxnId != null`,
applied only in `prepareTxnsExclusive`. Keys are always `RELEASE_QUEUE` — *except on the failure
path*: `NonSyncState.postRunExclusive` on a failed ATOMIC round calls `AccordCacheEntry.reclaimFifoHead`
for each key it locked, which sets `LOCKED_HOLDING_QUEUE` on that **key** entry and keeps the fifo
claim. Nothing releases it (`OptionalState.retry` is populated and never consumed), so the entry is
deliberately stalled for the life of the process. The model does not cover that path at all — see
`AccordExec.tla`'s ASSUMES — so O9 as modelled (`HoldsLock` only on `CmdEntries`) is the failure-free
statement.

**O10 ATOMIC subset restriction.** An ATOMIC consequence declares a subset of its submitter's
txnIds — asserted in the sharper form *its submitter holds the lock on each txnId it declares*, so
no foreign task can be queued between them — and a subset of its keys unless non-sync (then
`alwaysReady`); ATOMIC SYNC non-subset is rejected. Note the asymmetry: the txnId restriction is
asserted, the key subset is not. The key-subset test compares against the submitter's *current
batch* when the submitter is INCR, so `alwaysReady` applies at least as often as the declared key
sets suggest.

**O11 unsequenced ⇒ no retained lock.** `requireSequencedIfHoldsLocksBetweenRuns` forbids an
UNSEQUENCED INCR task from declaring a txnId; use BY_PRIORITY. It is applied wherever an INCR task
is set up — `preSetup` for a consequence, `submitExclusiveMayThrow` for a top-level task — so it
covers every task, which is what O3 needs. The general rule is *a task may be unsequenced on any
entry it will not hold across runs* — a bag imposes no order, so two prospective `HOLD_QUEUE`
lockers would both sit in the runnable prefix. `lockExclusive`'s `require(!task.isUnsequenced(this))`
asserts it directly.

**O12 load quiescence.** Nothing is notified while `LOADING`; `drainWaitingToLoad` plus re-add in
`compareForNotify` order rebuilds the regions, fifo claims first (`compareForNotify` orders every
fifo claim ahead of every other task, since a task that is not a fifo claim has no stamp to compare).
Fifo members are returned by the drain without being removed and re-placement is `contains`-guarded,
preserving R2.

**O13 submit before release.** An ATOMIC consequence claims its positions before its submitter
releases. Production does this for *every* consequence, not just an ATOMIC one:
`Task.completeExclusiveNoExcept` calls `submitConsequencesExclusive(prepareConsequencesExclusive())`
and only then `completeExclusiveMayThrow`, and `AccordExecutorSignalLoop.pushExecuted` states the
same thing in as many words (*"every consequence is submitted before the parent completes"*).
What isolation needs is the ATOMIC case, and **no ordering substitutes for it**: once a
foreign task has run on a shared entry, no arrangement of the queue undoes it. The constraint is
implemented in three places — the submission order in `Task.completeExclusiveNoExcept`, the
prefix/suffix split in `AccordExecutorSignalLoop.pushExecuted`, and the single pending queue whose
drain order that loop relies on ("draining NEW work first is safe but draining completions first is
NOT") — and **none of them asserts it**. That is why `ctl-defer-submit` is a control over *code*
rather than over an assertion, and it is the one rule here with no runtime guard: asserting in
`pushExecuted` / `completeExclusiveNoExcept` that a consequence is registered/queued before the
parent's `completeExclusiveMayThrow` is the obvious strengthening.

**O14 the atomic unit.** The unit boundary is set by which consequences **inherit the stamp**, not
by when they are submitted (submission is unconditional — O13). `SafeTask.preSetup`'s
`isAtomic()` branch is the test (it read `isSequencedByPriorityAtomic()` when these specs were
written; the predicate was renamed, not changed): only there does a consequence take
`parent.fifoAt` (or a fresh stamp if the submitter has none) and call
`setCacheQueuedFifoExclusive()`, so the isolation guarantee travels along a submission link exactly
when the child is ATOMIC. The unit it protects is therefore the chain of ATOMIC submissions, not
every descendant of an ATOMIC task: a `BY_PRIORITY` consequence of an ATOMIC submitter is submitted
at the same moment as an ATOMIC one but takes its place by `compare()`, whose placement is
arrival-independent, and an UNSEQUENCED one is bagged, so both may be preceded on a shared entry by
a foreign task and neither is inside the unit.
`AccordExec.tla`'s `UnitOf` is that closure and `Inv_Isolation` is stated over it; the `deep-chain`
topology is the witness that the wider reading — every descendant of an ATOMIC task — is false of the
implementation. The `ExecutionSequence.ATOMIC` javadoc is consistent with this rule and with a
top-level ATOMIC task — *"Appears to be processed 'atomically' both itself and with the task that
submits it, with respect to other tasks. Meaningful only when submitted by an already running task,
**or against an incremental task**"* — the second clause being the top-level INCR case that
`prepareExclusiveMayThrow` stamps on first run. (Earlier revisions of this document quoted two other
phrases from that javadoc, *"the combined unit of work must also have this property"* and *"May only
be submitted in follow-up to a SYNC or ASYNC task that is itself Sequenced"*; neither exists in the
tree — cf. §7's note on `submitConsequenceBeforeParentCompletes`.) The javadoc's third clause,
*"if the execution partially succeeds, any failing keys are blocked from further work to avoid
witnessing a partial update"*, is the failure path R6/O9 describe, and is not modelled.

## 5. The theorem

Rank each task `⟨layer, key⟩`, layer 0/1/2 for fifo/ordered/bag, key = `fifoAt` (O6/Q5) or the
`compare` key (O1) or 0. Every edge in `W` strictly decreases it:

- `Wpos`, `b` fifo and `a` not: `layer(b) < layer(a)` (O2, O3)
- `Wpos`, both fifo: `fifoAt(b) < fifoAt(a)` (Q5, O8 — including the pin, which O8 requires never to
  differ from the stamp order)
- `Wpos`, both ordered: `compare(b,a) < 0` (R3, O1)
- `Wpos`, `a` bag: `layer(a) = 2` and `b` is fifo or ordered; a bag-bag edge cannot exist, since both
  would be in the prefix (Q3/Q4)
- `Wlock`: `b = lock(e)` is the fifo head (O8, R5), so `layer(b) = 0`; if `a` is also fifo it is
  behind `b`

`<_lex` is a strict partial order and `W ⊆ <_lex`, so `W` is acyclic. Machine-checked in
`lean/AccordAcyclic.lean` (`wait_acyclic`), whose hypotheses are exactly the five cases above — and
`waitEdges_iff_rankOK` there proves that those hypotheses are *equivalent* to `RankOK`, so the single
invariant TLC checks discharges all of them, and `wait_acyclic_of_rankOK` consumes it directly.

The same rank gives progress directly, and at any size: the rank-minimal live task has no outgoing
edge, so it leads every entry it holds and no other task holds a lock it needs — which is `CanRun`.
That is `exists_runnable` in the same file, and it is the size-independent form of `NoStuck`; its extra
hypothesis is stated as the conformance requirement it is — *a live task that cannot run is waiting for
some task* — i.e. that `Wpos` and `Wlock` between them cover every reason a task cannot run. (It is
required of live tasks only: a finished task holds no position and has no outgoing edge, so
quantifying over every task would force "runnable" to be read as "can run **or** has finished".) Thresholds are
not modelled there and are not meant to be: leading every entry it holds satisfies any threshold,
since a threshold never exceeds the number of keys held.

Isolation has the same treatment, from O5, O6, O13 and Q4 rather than from the rank alone: `isolated`
in the same file proves that no task from outside an atomic unit runs on an entry between two members
of it, for any number of tasks. Unlike acyclicity, though, the model does **not** check those six
rules — it checks `Inv_Isolation`, the conclusion — and two of them (`handover`, `fifo_claim_order`)
cannot be stated in `AccordExec.tla` at all, since it records run order (`plog`) but not claim times.
So the two arguments for isolation are independent rather than complementary. §6 of that file
tabulates every hypothesis of all three theorems against the invariant it encodes and the control
profile or assertion that would catch its violation, marking the argued rows as such; §5 shows the
hypothesis bundles are satisfiable, without which `isolated` — which concludes `False` — would prove
nothing.

## 6. Counting and liveness

Acyclicity is necessary but not sufficient — a lost wakeup hangs an acyclic graph. Discharged by
`tla/AccordNotify.tla` as G1–G4:

- **C1** `waitingFor` packs two counters: `waitingForTxnCount()`, the txnIds the task does not lead,
  and `waitingForKeyCount()`, which counts keys only for a SYNC task — for a non-sync task key
  readiness is `blocking ∪ notBlocking` against the batch threshold
  (`min(keys - (processed + failed), alwaysReady ? 1 : MIN_BATCH)`, or
  `blocking.size() >= NONSYNC_BLOCKED_LIMIT`). Both survive a lost txnId
  (O4), so nothing has to be recounted when it returns
- **C2** `blocking` and `notBlocking` are disjoint and contain only keys the task holds, and
  — at quiescence — only keys it still *leads*. That last part is new and load-bearing:
  `NonSyncState.prepareExclusive` no longer re-checks the captured batch (the
  `statusIfPresent(owner) == NOT_RUNNABLE ? continue` skip went with the deferred revocation,
  since a handler that takes no position — L3 — cannot revoke a key captured earlier in the
  same pass), so it locks *everything* it captured with `RELEASE_QUEUE` and
  `REQUIRE_RUNNABLE` asserts it leads each one. The sets may still be transiently wrong
  *inside* a notification cascade, which is why `AccordNotify` states it as `G1_BatchLed` at
  quiescence and reaches the transient with `Probe_StaleBatch`. Disjointness is likewise
  only asserted, not maintained: `onNewHead`/`onNewBlockingHead` add to one set and
  `Invariants.paranoid` that the other does not hold the key, so `G3_Disjoint` is what
  reports a double file
- **C3** `WAITING_TO_RUN ⇒ both counters 0`, asserted directly and unconditionally
  (`Invariants.require(waitingFor == 0)` in `waitToRunExclusive`). The stronger reading —
  *and therefore leads every held entry* — is checked under paranoia in the same method, but
  per entry and only where it is required: for every reference it asserts the *position*
  (`Invariants.require(entry.contains(this))`), and it asserts the runnable status
  (`NEWLY_RUNNABLE` / `NEWLY_BLOCKING_RUNNABLE`) only `if (isSync() ||
  !entry.isCommandsForKey())`. So a non-sync task's *key* entries are exempt here — it need
  not lead a key it is not batching, and it holds positions on keys left for later rounds —
  but by C2 it must lead every key it has *captured*, and `prepareExclusive` asserts that at
  the lock. `AccordNotify`'s `G1_BatchLed` is the precise statement, and `G1_Strong` (which
  used to be a mere regression check, back when the re-check absorbed a stale capture)
  follows from it
- **L1** whenever a task enters `prefix(e)` it is eventually notified
- **L2** a started INCR task releases `HOLD_QUEUE` in finitely many rounds. Each round locks every
  key it captured and `processed` advances by that many, so — unlike before the re-check was
  removed — progress is per round again, with one exception on the failure path: a round whose
  captured set is *empty* can only arise from keys dropped by `onFailingKeyExclusive`, and
  `prepareExclusive` ends the task there (`PARTIALLY_FAILED`) rather than looping.
  `AccordNotify`'s `Probe_EmptyBatch` still reaches the mid-cascade state in which every
  captured key is stale, but that state can no longer survive to a prepare (C2)
- **L3** notification handlers no longer mutate queue positions, so delivery does not cascade: the
  depth bound G4 is met at depth 1, and is retained as the check that this remains true.
  `AccordNotify` states the rule itself as an action property,
  `L3_HandlerTakesNoPosition == [][Deliver => UNCHANGED <<holds, region>>]_vars` — no state
  invariant can say it — and it holds in every cell `notify.py` runs. It is true by
  construction of the handlers there (they return `holds` unchanged and never touch
  `region`), so it is a regression check on the model, not independent evidence about the
  code; the runtime re-entrancy guard is what checks the code.
  `G4_Bounded` is `Depth <= MaxDepth` with `Deliver` guarded one level above it, so it can
  actually fail — at `MaxDepth = 0` it does — rather than being true by construction; and
  `G4_Drains` (`[]<>Quiescent`) covers a cascade that never ends rather than one that is too
  deep. Both are checked by `tla/notify.py`.

## 7. Consistency with the code

The claims above cite:

- `SafeTask`: `submitExclusiveMayThrow`, `preSetup`, `waitOnTxnsExclusive`, `queueOnKeysExclusive`,
  `isUnsequenced`, `requireSequencedIfHoldsLocksBetweenRuns`, `prepareExclusiveMayThrow`,
  `prepareTxnsExclusive`, `NonSyncState.prepareExclusive`, `incrementWaitingTxns`,
  `addCachedKeyExclusive` (was `adoptCachedKeyExclusive`), `waitToRunExclusive`,
  `onFailingKeyExclusive`
- `AccordCacheEntry`: `add`, `remove`, `lockExclusive`, `moveToFifo`, `drainWaitingToLoad`,
  `ensureQueue`, `isLiveQueue`, `setInconsistent`, `reclaimFifoHead`
- `AccordCacheEntryQueue`: `addFifo` (Q5 and the pin), `addPrioritised`, `addUnsequenced`,
  `runnablePrefix`, `validate`, `validateMembership`, `requireNotFailed`, `onInconsistent`,
  `compare`, `compareFifo`, `compareForNotify`
- `Task`: `completeExclusiveNoExcept`, `prepareConsequencesExclusive`,
  `submitConsequencesExclusive`, `inherit`
- `ExclusiveExecutor`: `runTask`, `completeTask` (A3)

One correction worth recording, since the citation had propagated: earlier revisions of this
document (and of `README.md` and `AccordExec.tla`) cited a
`Task.submitConsequenceBeforeParentCompletes()` predicate for O13/O14. No such method exists
anywhere in the tree. The submission is unconditional (O13) and the ATOMIC test that actually
bounds the unit is `preSetup`'s `isAtomic()` branch (O14).

Three more of the same kind, found re-checking these documents against the tree at the
partial-failure commit, and fixed above rather than left to propagate:

- `adoptCachedKeyExclusive` is now `SafeTask.addCachedKeyExclusive`, and
  `isSequencedByPriorityAtomic()` is now `Task.isAtomic()` — renames, no behaviour change.
- `requireNotTerminal` is now `AccordCacheEntryQueue.requireNotFailed`, and it no longer says
  what R6 said: a failed continuation or started-incremental task may keep a position on an
  `isInconsistent()` entry.
- the two `ExecutionSequence` javadoc phrases these documents quoted — atomicity "from the point
  of view of other SEQUENCED tasks", and "May only be submitted in follow-up to a SYNC or ASYNC
  task that is itself Sequenced" — do not appear anywhere in the tree. The enum reads
  "with respect to other tasks" and "Meaningful only when submitted by an already running task,
  or against an incremental task". The first of those is why README's bag-interleaving section
  no longer claims the implementation is stronger than the declared semantics (O2), and the
  second is what licenses a top-level ATOMIC task (O14).

And one substantive divergence rather than a citation: `NonSyncState.prepareExclusive`'s per-key
re-check is gone, so C2/C3/L2 above and `AccordNotify`'s `Run` had to change with it — the batch
is now locked whole, under `REQUIRE_RUNNABLE`, which is what `G1_BatchLed` states.
