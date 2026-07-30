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
| **R6** | no task in `TERMINAL_FAILURE` holds a position (`requireNotTerminal`) |
| **R7** | every non-prefix member is in a state meaning "waiting on caches" (`isWaitingOnCaches`) |
| **R8** | `queue` is `null \| SafeTask \| AccordCacheEntryMiniQueue \| AccordCacheEntryQueue`; `ensureQueue`/`maybeUnwrap` preserve the abstract queue, with `isLiveQueue` guarding re-entrant replacement. The mini queue is the two-claim case and exists only while locked (`miniQueue()` requires `isLocked()`); its head is the holder if `HOLD_QUEUE`, else the other claim |

R1/R3/R4/R8 are array (and union) algebra and are not modelled; R2, R5, R6 are.

## 4. Ordering (global — the acyclicity argument)

**O1 pair-determined order.** `compare(a,b)` = `(position, executionKind, createdAt)` with
`createdAt` unique per store, so it is a strict total order and *the same on every entry a pair
shares*. Stated in the `AccordCacheEntryQueue` javadoc: *"the order imposed on any two tasks is a
function of the pair alone … this is what makes the scheme deadlock free"*.

**O2 region stratification.** `FIFO ≺ ORDERED ≺ BAG`, and bag members never wait for one another
(Q3/Q4). Note that this is *stronger than the semantics asks for*: `UNSEQUENCED` was intended to
interleave freely, including into the middle of an ATOMIC unit, which the promise permits because it
is made only "from the point of view of other SEQUENCED tasks". Q4 does not allow it — the bag runs
only once nothing sequenced is queued — and no weaker formulation is known that stays acyclic: BAG is
the top layer of the rank, so a bag member in the runnable prefix gives every sequenced task behind it
an edge that cannot decrease `⟨layer, key⟩`, while `Wlock` still gives the bag member an outgoing edge
to any `HOLD_QUEUE` holder of an entry it declared. `AccordExec.tla`'s `PBagInterleaves` demonstrates
both, and which topologies close a real cycle; see README, "UNSEQUENCED was meant to interleave".

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
uninterrupted setup pass; enforced negatively by `adoptCachedKeyExclusive`'s
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
entry it holds, **then** takes `HOLD_QUEUE`, **then** sets the started bit. So any task that can
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
applied only in `prepareTxnsExclusive`. Keys are always `RELEASE_QUEUE`.

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
`isSequencedByPriorityAtomic()` branch is the test: only there does a consequence take
`parent.fifoAt` (or a fresh stamp if the submitter has none) and call
`setCacheQueuedFifoExclusive()`, so the isolation guarantee travels along a submission link exactly
when the child is ATOMIC. The unit it protects is therefore the chain of ATOMIC submissions, not
every descendant of an ATOMIC task: a `BY_PRIORITY` consequence of an ATOMIC submitter is submitted
at the same moment as an ATOMIC one but takes its place by `compare()`, whose placement is
arrival-independent, and an UNSEQUENCED one is bagged, so both may be preceded on a shared entry by
a foreign task and neither is inside the unit.
`AccordExec.tla`'s `UnitOf` is that closure and `Inv_Isolation` is stated over it; the `deep-chain`
topology is the witness that the wider reading — every descendant of an ATOMIC task — is false of the
implementation. The `ExecutionSequence` javadoc is ambiguous here: *"if the task is submitted by some
other task … the combined unit of work must also have this property"* describes the link towards the
submitter, which is this rule, while *"May only be submitted in follow-up to a SYNC or ASYNC task that
is itself Sequenced"* is stale now that `submitExclusiveMayThrow` applies the sequence to top-level
tasks. Conversely the unit is protected from *more* than the promise names: an UNSEQUENCED task cannot
interleave into it either, for the reason given in O2.

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
  readiness is `blocking ∪ notBlocking` against the batch threshold. Both survive a lost txnId
  (O4), so nothing has to be recounted when it returns
- **C2** `blocking` and `notBlocking` are disjoint and contain only keys the task holds. They may
  contain keys it no longer *leads*: `NonSyncState.prepareExclusive` re-checks at lock time, because
  taking one lock notifies a new head and that can revoke a key captured earlier in the same pass. A
  SYNC task has no such re-check, so for SYNC "believed runnable" must imply "leads everything" or
  `REQUIRE_RUNNABLE` trips
- **C3** `WAITING_TO_RUN ⇒ both counters 0`, asserted directly and unconditionally
  (`Invariants.require(waitingFor == 0)` in `waitToRunExclusive`). The stronger reading —
  *and therefore leads every held entry* — is checked under paranoia in the same method, but
  per entry and only where it is required: for every reference it asserts the *position*
  (`Invariants.require(entry.contains(this))`), and it asserts the runnable status
  (`NEWLY_RUNNABLE` / `NEWLY_BLOCKING_RUNNABLE`) only `if (isSync() ||
  !entry.isCommandsForKey())`. So for a non-sync task's *key* entries leading is not merely
  unasserted, it is false by design — C2 above says the captured keys may no longer be led —
  which is why `AccordNotify` keeps `G1_Strong` as a regression check rather than as a
  requirement
- **L1** whenever a task enters `prefix(e)` it is eventually notified
- **L2** a started INCR task releases `HOLD_QUEUE` in finitely many rounds. Each round processes the
  keys it locked, and `processed` advances by that many — but a round whose every captured key was
  revoked by C2's re-check locks nothing and advances `processed` by 0, so progress is per *lock*
  taken, not per round (`AccordNotify`'s `Probe_EmptyBatch` reaches that round)
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
  `adoptCachedKeyExclusive`, `waitToRunExclusive`
- `AccordCacheEntry`: `add`, `remove`, `lockExclusive`, `moveToFifo`, `drainWaitingToLoad`,
  `ensureQueue`, `isLiveQueue`
- `AccordCacheEntryQueue`: `addFifo` (Q5 and the pin), `addPrioritised`, `addUnsequenced`,
  `runnablePrefix`, `validate`, `validateMembership`, `compare`, `compareForNotify`
- `Task`: `completeExclusiveNoExcept`, `prepareConsequencesExclusive`,
  `submitConsequencesExclusive`, `inherit`
- `ExclusiveExecutor`: `runTask`, `completeTask` (A3)

One correction worth recording, since the citation had propagated: earlier revisions of this
document (and of `README.md` and `AccordExec.tla`) cited a
`Task.submitConsequenceBeforeParentCompletes()` predicate for O13/O14. No such method exists
anywhere in the tree. The submission is unconditional (O13) and the ATOMIC test that actually
bounds the unit is `preSetup`'s `isSequencedByPriorityAtomic()` branch (O14).
