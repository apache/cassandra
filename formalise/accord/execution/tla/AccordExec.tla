(* Licensed to the Apache Software Foundation (ASF) under one
(* or more contributor license agreements.  See the NOTICE file
(* distributed with this work for additional information
(* regarding copyright ownership.  The ASF licenses this file
(* to you under the Apache License, Version 2.0 (the
(* "License"); you may not use this file except in compliance
(* with the License.  You may obtain a copy of the License at
(*
(*     http://www.apache.org/licenses/LICENSE-2.0
(*
(* Unless required by applicable law or agreed to in writing, software
(* distributed under the License is distributed on an "AS IS" BASIS,
(* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
(* See the License for the specific language governing permissions and
(* limitations under the License.

--------------------------- MODULE AccordExec ---------------------------
(***************************************************************************)
(* The wait relation induced by the Accord per-CommandStore execution       *)
(* queues: AccordCacheEntry, AccordCacheEntryQueue, SafeTask.               *)
(*                                                                         *)
(* PROVES  no set of live tasks can be mutually blocked (NoStuck), that the *)
(* relation is acyclic (NoCycle), and that a lexicographic rank witnesses   *)
(* the acyclicity (RankOK).  RankOK is the size-independent certificate:    *)
(* AccordAcyclic.lean derives acyclicity from it for any number of tasks    *)
(* and entries, so TLC's job is only to show the implementation maintains   *)
(* the hypotheses.                                                         *)
(*                                                                         *)
(* MODELS the queue as its three regions, with the fifo order derived from  *)
(* fifoAt (Q5) rather than from the array that maintains it; the array      *)
(* algebra (Q1-Q4, indices, compaction) is AccordCacheEntryQueueTest's.     *)
(* Readiness is derived from the queues; that abstraction is discharged by  *)
(* AccordNotify, which models the notification bookkeeping it stands for.   *)
(*                                                                         *)
(* CLAIMS ARE TAKEN IN ONE PASS.  waitOnTxnsExclusive takes the txnId       *)
(* positions and calls queueOnKeysExclusive in the same turn, and a task    *)
(* keeps every position until it is done: losing a txnId stops it running   *)
(* but revokes nothing (incrementWaitingTxns).  So there is no state in     *)
(* which a task holds txnIds and not keys, and nothing for a deferred       *)
(* revocation to undo - the two-phase claim, completeChangeOfRunnableStatus *)
(* and DeferredChangeOfRunnableStatus were all removed with                 *)
(* QUEUE_ON_KEYS_AT_ONCE, and are not modelled here.                       *)
(*                                                                         *)
(* ASSUMES  a LOADING entry carries no wait edge (nothing is runnable on it *)
(* and the load completes independently), and a run never waits for a task: *)
(* RELEASE_QUEUE locks are returned within the run that takes them, one     *)
(* task per store is prepared/run/completed at a time (ExclusiveExecutor),  *)
(* and a consequence is queued on Task.next until its submitter completes.  *)
(* Hence only HOLD_QUEUE yields a lock edge.                               *)
(*                                                                         *)
(* ASSUMES ALSO a FAILURE-FREE execution, which is a real restriction since *)
(* partial failure landed.  A round that fails an ATOMIC task marks each of *)
(* its keys inconsistent and calls AccordCacheEntry.reclaimFifoHead, which  *)
(* converts that round's RELEASE_QUEUE lock into a HOLD_QUEUE claim on a KEY *)
(* entry and never releases it (nothing consumes OptionalState.retry yet),  *)
(* so on that path HoldsLock's CmdEntries restriction (O9) is false and the  *)
(* stall is deliberate rather than a violation of NoStuck.  A key dropped by *)
(* onFailingKeyExclusive also lowers the threshold - isWaitReady tests       *)
(* keys - (processed + failed) - and AccordCacheEntryQueue.requireNotFailed  *)
(* permits a failed in-progress task to keep a position on an inconsistent   *)
(* entry (R6).  None of that is modelled here; see README, "Deliberately not *)
(* modelled".                                                              *)
(*                                                                         *)
(* The P* constants fall into two groups, and only one of them is a set of   *)
(* controls.  PAlwaysReady, PModelLoading, PAllowAdoption and PPartialRounds *)
(* are MODELLING switches: they select which production behaviour is in      *)
(* scope, and must NOT break anything (profiles baseline-*/ar-* are green).  *)
(* PPartialRounds is the one over-approximation among them: a round locks    *)
(* everything it captured, and captures every key it leads up to             *)
(* NONSYNC_MAX_BATCH_SIZE, which no model instance reaches - so a round      *)
(* locking a strict subset of what it leads is a behaviour production has    *)
(* only at 64 keys, admitted here because it costs nothing to allow.         *)
(* The ctl-* group each disables one RULE, and each must break a property    *)
(* (see matrix.py's control profiles): an assertion where the implementation *)
(* has one - PAllowUnseqIncrWithTxn (requireSequencedIfHoldsLocksBetweenRuns)*)
(* , PAllowFifoAdoption (addCachedKeyExclusive's !isCacheQueuedFifo) and    *)
(* PAllowDoubleLock (lockExclusive's require(!isLocked())) - and otherwise   *)
(* the code that establishes it: PUpgradeOnStart removes                    *)
(* prepareExclusiveMayThrow's moveToFifo block, and PSubmitBeforeRelease the *)
(* submit-before-complete ordering of Task.completeExclusiveNoExcept, which  *)
(* nothing in the implementation asserts (see INVARIANTS O13).              *)
(*                                                                         *)
(* Two names below are the implementation's older ones: the guard            *)
(* PAllowFifoAdoption relaxes now lives in addCachedKeyExclusive (it was     *)
(* adoptCachedKeyExclusive), and the ATOMIC-unit test in preSetup is now     *)
(* Task.isAtomic() (it was isSequencedByPriorityAtomic()).  Both are renames,*)
(* not behaviour changes, and are cited by their current names throughout.   *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    CmdEntries,              \* command (TxnId) cache entries
    KeyEntries,              \* commands-for-key cache entries
    NumTasks,                \* tasks are 1..NumTasks, in compare() order
    TaskTxns,                \* [1..NumTasks -> SUBSET CmdEntries]  context.txnIds()
    TaskKeys,                \* [1..NumTasks -> SUBSET KeyEntries]  context.keys()
    TaskParent,              \* [1..NumTasks -> 0..NumTasks]  submitter, 0 = top level
    MinBatch,                \* NONSYNC_MIN_BATCH_SIZE
    BlockedLimit,            \* NONSYNC_BLOCKED_LIMIT; 0 disables the escape
    PAlwaysReady,            \* when NonSyncState.alwaysReady applies; see AlwaysReady
    PModelLoading,           \* entries may start LOADING and drain later
    PAllowAdoption,          \* addCachedKeyExclusive
    PPartialRounds,          \* a round may lock fewer keys than it captured

    \* ---- controls: each disables one implementation assertion ------------
    PUpgradeOnStart,         \* O7, prepareExclusiveMayThrow's moveToFifo
    PSubmitBeforeRelease,    \* consequences claim before their submitter releases
    PAllowUnseqIncrWithTxn,  \* requireSequencedIfHoldsLocksBetweenRuns
    PAllowFifoAdoption,      \* addCachedKeyExclusive's !isCacheQueuedFifo
    \* A3, lockExclusive's require(!isLocked()).  CanRun ASSUMES that guard rather than
    \* deriving it, so without this constant Inv_AtMostOneLock cannot fail in any profile
    \* and is a vacuous row; this control is what makes it falsifiable.
    PAllowDoubleLock,

    \* ---- semantics probe, not a control: see Prefix and README -----------
    PBagInterleaves          \* Q4 relaxed so the bag is runnable behind a sequenced region

Tasks   == 1..NumTasks
Entries == CmdEntries \cup KeyEntries

ASSUME CmdEntries \cap KeyEntries = {}
ASSUME MinBatch \in Nat /\ MinBatch >= 1
\* a typo would otherwise fall silently into AlwaysReady's OTHER branch, which is the
\* production setting, so a mis-spelled probe would look like it had been exercised
ASSUME PAlwaysReady \in {"NEVER", "ALWAYS", "FIRST_RUN", "ON_NON_SUBSET"}
\* a task declares at most context.primaryTxnId() and context.additionalTxnId(), and
\* incrementWaitingTxns asserts waitingForTxnCount() < 2, so a topology declaring more
\* would model a task the implementation cannot build
ASSUME \A t \in Tasks : Cardinality(TaskTxns[t]) <= 2
\* a consequence is created by its submitter's run, so it has the larger createdAt;
\* position is inherited (Task.inherit) and createdAt is compare()'s last key, so a
\* consequence is later in compare() order than its submitter
ASSUME \A t \in Tasks : TaskParent[t] = 0 \/ TaskParent[t] < t

VARIABLES
    cfg,        \* [Tasks -> ConfigDomain], chosen in Init then constant
    phase,      \* [Tasks -> Phases]
    pending,    \* [Tasks -> SUBSET KeyEntries]  declared, unprocessed, position held
    adopted,    \* [Tasks -> SUBSET KeyEntries]  taken via addCachedKeyExclusive
    fifoAt,     \* [Tasks -> Nat]  stamped when the task becomes a fifo claim, 0 = never
    plog,       \* [Entries -> Seq(Tasks)]  the order entries were actually processed in
    loading,    \* SUBSET Entries
    clock

vars == << cfg, phase, pending, adopted, fifoAt, plog, loading, clock >>

Kinds  == {"ATOMIC", "PRIORITY", "UNSEQ"}   \* ExecutionContext.ExecutionSequence
Loads  == {"SYNC", "ASYNC", "INCR"}         \* LoadKeys
\* Claimed covers WAITING_ON_TXN, WAITING_ON_KEY and WAITING_TO_RUN: the positions
\* are the same in all three, and which of them a task is in is AccordNotify's
\* subject, not this module's.
Phases == {"New", "Claimed", "Started", "Done"}
ConfigDomain == [kind: Kinds, load: Loads]

IsChild(t) == TaskParent[t] # 0

\* holdsLocksBetweenRuns(): only these take HOLD_QUEUE, and only on txnIds
HoldsAcrossRuns(t) == cfg[t].load = "INCR" /\ TaskTxns[t] # {}

LegalConfigFor(t, c) ==
    \* requireSequencedIfHoldsLocksBetweenRuns, applied to every INCR task since the
    \* sequence is now set for top level tasks too: an UNSEQUENCED INCR task may not
    \* declare a txnId, so it retains no lock and is uniformly bagged
    /\ (c.load = "INCR" /\ c.kind = "UNSEQ" /\ TaskTxns[t] # {})
           => PAllowUnseqIncrWithTxn
    \* an ATOMIC CONSEQUENCE declares a subset of its submitter's txnIds - asserted in
    \* the sharper form "its submitter holds the lock on each of them", so no foreign
    \* task can be queued between them - and a subset of its keys unless non-sync (then
    \* alwaysReady); ATOMIC SYNC non-subset is rejected.  None of this applies to a top
    \* level ATOMIC task: those checks live in preSetup, which only a consequence runs.
    /\ (c.kind = "ATOMIC" /\ IsChild(t)) =>
           /\ TaskTxns[t] \subseteq TaskTxns[TaskParent[t]]
           /\ (c.load = "SYNC") => TaskKeys[t] \subseteq TaskKeys[TaskParent[t]]

(***************************************************************************)
(* REGIONS.  fifo runs ahead of sorted runs ahead of bag (Q4).  A task is a  *)
(* fifo claim exactly when it has been stamped.  The region is a function of *)
(* the task alone - isUnsequenced(entry) now asserts its own                 *)
(* isCommandsForKey() qualifier is vacuous, which is what makes that true -  *)
(* and RankOK requires it, since a rank must be a function of the task.      *)
(***************************************************************************)
IsFifo(t) == fifoAt[t] > 0
Region(t) == IF IsFifo(t) THEN "FIFO"
             ELSE IF cfg[t].kind = "UNSEQ" THEN "BAG" ELSE "ORD"

\* Q5: the fifo region is ordered by fifoAt, ties broken by createdAt.  fifoAt is
\* inherited by an ATOMIC consequence, so ties are real; the tie-break is
\* pair-determined, and orders a consequence after its submitter (see the ASSUME).
FifoKey(t) == fifoAt[t] * (NumTasks + 1) + t

HoldsPos(t, e) ==
    IF e \in CmdEntries
    THEN /\ e \in TaskTxns[t]
         /\ \/ phase[t] = "Claimed"
            \/ (phase[t] = "Started" /\ HoldsAcrossRuns(t))
    ELSE e \in pending[t] /\ phase[t] \in {"Claimed", "Started"}

\* HOLD_QUEUE, the only lock that survives a run (outside the failure path: see the header,
\* where reclaimFifoHead retains one on a key entry)
HoldsLock(t, e) ==
    e \in CmdEntries /\ e \in TaskTxns[t] /\ phase[t] = "Started" /\ HoldsAcrossRuns(t)

\* X are tasks treated as finished; A are tasks hypothetically placed now.
\* L ignores LOADING, for the reachability analysis: a load completes unaided.
Occupies(t, e, X, A) ==
    /\ t \notin X
    /\ \/ HoldsPos(t, e)
       \/ /\ t \in A
          /\ (IF e \in CmdEntries THEN e \in TaskTxns[t] ELSE e \in pending[t])

\* the fifo member with the least key, i.e. the head Q5 alone would give
LeastStamped(f) == CHOOSE t \in f : \A u \in f : FifoKey(t) <= FifoKey(u)

Prefix(e, X, A, L) ==
    IF (~L) /\ PModelLoading /\ e \in loading
    THEN {}
    ELSE LET occ == {t \in Tasks : Occupies(t, e, X, A)}
             f == {t \in occ : Region(t) = "FIFO"}
             o == {t \in occ : Region(t) = "ORD"}
             bag == {t \in occ : Region(t) = "BAG"}
             \* addFifo keeps a HOLD_QUEUE holder at the head whatever the stamps say:
             \* it cannot yield the prefix, since it holds the lock across its runs.
             \* Inv_LockLeads asserts this never differs from LeastStamped(f), which is
             \* what addFifo's Invariants.expect reports if it ever does.
             lk == {t \in f : HoldsLock(t, e)}
             \* Q4 makes the bag the last region, so an unsequenced task runs only once
             \* nothing sequenced is queued - it cannot interleave into an ATOMIC unit.
             \* Setting this admits the interleaving reading (which the ATOMIC javadoc does
             \* NOT ask for: it promises atomicity "with respect to other tasks") and shows
             \* what it would cost - see README, "What an interleaving bag would cost".
             extra == IF PBagInterleaves THEN bag ELSE {}
         IN IF lk # {} THEN {CHOOSE t \in lk : TRUE} \cup extra
            ELSE IF f # {} THEN {LeastStamped(f)} \cup extra
            ELSE IF o # {} THEN {CHOOSE t \in o : \A u \in o : t <= u} \cup extra
            ELSE bag

Leads(t, e) == t \in Prefix(e, {}, {}, FALSE)
\* would lead once X are finished, with t placed and loads resolved
WLeads(t, e, X) == t \in Prefix(e, X, {t}, TRUE)

(***************************************************************************)
(* EXECUTION THRESHOLDS (NonSyncState.isWaitReady).  A SYNC task needs every *)
(* key it holds; a non-sync task min(remaining, MIN_BATCH), or one if        *)
(* alwaysReady, and is also released by the BLOCKED_LIMIT escape - which is  *)
(* NonSyncState.isWaitReady's alone, hence the load # "SYNC" conjunct below: *)
(* a SYNC task reaches WAITING_TO_RUN only through waitToRunExclusive's      *)
(* require(waitingFor == 0), and lockExclusive's REQUIRE_RUNNABLE would trip *)
(* if it ran without leading everything.  BlockedLimit = 1 models            *)
(* NONSYNC_BLOCKED_LIMIT = 8 at model scale.  This is                       *)
(* why a task may lead some entries but not enough to run, so the relation   *)
(* is a threshold relation and not a conjunction.                           *)
(***************************************************************************)
Min2(a, b) == IF a < b THEN a ELSE b

\* preSetup sets alwaysReady for an ATOMIC non-sync CONSEQUENCE whose keys are not a
\* subset of its submitter's.  FIRST_RUN relaxes only while first taking locks.
\* (Production compares against the submitter's current batch when the submitter is
\* INCR, so it applies at least as often as this; that only matters if MinBatch > 1.)
NeedsAlwaysReady(t) ==
    /\ cfg[t].kind = "ATOMIC" /\ cfg[t].load # "SYNC" /\ IsChild(t)
    /\ ~(TaskKeys[t] \subseteq TaskKeys[TaskParent[t]])

AlwaysReady(t) ==
    CASE PAlwaysReady = "NEVER"     -> FALSE
      [] PAlwaysReady = "ALWAYS"    -> cfg[t].load # "SYNC"
      [] PAlwaysReady = "FIRST_RUN" -> NeedsAlwaysReady(t) /\ phase[t] # "Started"
      [] OTHER                      -> NeedsAlwaysReady(t)

Threshold(t) ==
    IF cfg[t].load = "SYNC" THEN Cardinality(pending[t])
    ELSE Min2(Cardinality(pending[t]), IF AlwaysReady(t) THEN 1 ELSE MinBatch)

LedKeys(t, X) == {e \in pending[t] : WLeads(t, e, X)}
BlockedKeys(t, X) ==
    {e \in LedKeys(t, X) : \E u \in Tasks \ (X \cup {t}) : HoldsPos(u, e)}

KeyReady(t, X) ==
    \/ Cardinality(LedKeys(t, X)) >= Threshold(t)
    \/ (cfg[t].load # "SYNC" /\ BlockedLimit > 0
            /\ Cardinality(BlockedKeys(t, X)) >= BlockedLimit)

TxnReady(t, X) ==
    \A e \in TaskTxns[t] :
        WLeads(t, e, X) /\ ~\E u \in Tasks \ (X \cup {t}) : HoldsLock(u, e)

\* the same, respecting LOADING, for the enabling conditions
LeadsAllTxns(t) == \A e \in TaskTxns[t] : Leads(t, e)
\* lockExclusive opens with require(!isLocked()): an entry held across another task's
\* runs cannot be locked, whoever leads.  Implied by LeadsAllTxns under O8 only.
NoForeignLock(t) ==
    \A e \in TaskTxns[t] : ~\E u \in Tasks \ {t} : HoldsLock(u, e)

CanRun(t) ==
    /\ phase[t] \in {"Claimed", "Started"}
    /\ LeadsAllTxns(t) /\ (PAllowDoubleLock \/ NoForeignLock(t))
    /\ LET led == {e \in pending[t] : Leads(t, e)}
           blk == {e \in led : \E u \in Tasks \ {t} : HoldsPos(u, e)}
       IN \/ Cardinality(led) >= Threshold(t)
          \/ (cfg[t].load # "SYNC" /\ BlockedLimit > 0
                  /\ Cardinality(blk) >= BlockedLimit)

(***************************************************************************)
(* ACTIONS                                                                  *)
(***************************************************************************)

\* the fifoAt an ATOMIC consequence takes: its submitter's if that is a fifo claim,
\* else a fresh stamp
InheritedStamp(t, at) == IF fifoAt[TaskParent[t]] > 0 THEN fifoAt[TaskParent[t]] ELSE at

\* submitExclusive: register, take the txnId positions, then the key positions, in one
\* turn.  An ATOMIC CONSEQUENCE is stamped here (preSetup's setCacheQueuedFifoExclusive)
\* and so claims everything it declares as a fifo claim, in one indivisible pass (O5).
\* A top level ATOMIC task is not: nothing stamps it until its first run, and then only
\* if it is INCR, so it starts in the sorted region like any other sequenced task.
Setup(t) ==
    /\ phase[t] = "New"
    \* A consequence appears once its submitter has run.  Production submits EVERY
    \* consequence before its submitter completes - Task.completeExclusiveNoExcept calls
    \* submitConsequencesExclusive(prepareConsequencesExclusive()) ahead of
    \* completeExclusiveMayThrow, unconditionally - but what puts a consequence inside its
    \* submitter's unit is preSetup's isAtomic() branch: only an ATOMIC
    \* consequence inherits parent.fifoAt and calls setCacheQueuedFifoExclusive, so only
    \* its claim is ordered by the submitter's stamp.  Run models that claim (newKids).  A
    \* BY_PRIORITY consequence joins the sorted region by compare(), whose placement is
    \* arrival-independent, so claiming it here - after the release - is indistinguishable;
    \* an UNSEQUENCED one is bagged.  PSubmitBeforeRelease = FALSE moves the ATOMIC claim
    \* here too, which is ctl-defer-submit: it removes the ordering O13 rests on.
    /\ IsChild(t) => /\ phase[TaskParent[t]] \in {"Started", "Done"}
                     /\ (cfg[t].kind = "ATOMIC") => ~PSubmitBeforeRelease
    /\ fifoAt' = IF cfg[t].kind = "ATOMIC" /\ IsChild(t)
                 THEN [fifoAt EXCEPT ![t] = InheritedStamp(t, clock)]
                 ELSE fifoAt
    /\ clock' = clock + 1
    /\ phase' = [phase EXCEPT ![t] = "Claimed"]
    /\ UNCHANGED << cfg, pending, adopted, plog, loading >>

\* prepareExclusiveMayThrow, one round of runMayThrow, and completeExclusive.
\* Order within the action mirrors production: upgrade to fifo, take the HOLD_QUEUE
\* lock, submit ATOMIC consequences, then release the batch.
Run(t) ==
    /\ CanRun(t)
    /\ LET firstRun == phase[t] # "Started"
           \* O7: isIncremental() && (holdsLocksBetweenRuns() || isAtomic())
           \* && !isCacheQueuedFifo().  So an INCR task becomes a fifo claim on its first
           \* run if it will hold a lock across runs, or if it owes an ATOMIC guarantee
           \* and was not already stamped - which is only a top level ATOMIC task, since
           \* a consequence was stamped by Setup.
           upgrade == /\ firstRun /\ PUpgradeOnStart /\ fifoAt[t] = 0
                      /\ cfg[t].load = "INCR"
                      /\ (TaskTxns[t] # {} \/ cfg[t].kind = "ATOMIC")
           led == {e \in pending[t] : Leads(t, e)}
           \* only an INCR task has rounds: NonSyncState.prepareExclusive's re-check may
           \* leave part of a captured batch for a later one.  A SYNC/ASYNC task locks and
           \* releases everything inside its single run, so a partial batch there would
           \* only under-record plog and weaken Inv_Isolation's evidence.
           batches == IF pending[t] = {} THEN {{}}
                      ELSE IF PPartialRounds /\ cfg[t].load = "INCR"
                      THEN (SUBSET led) \ {{}}
                      ELSE {led}
       IN \E batch \in batches :
            LET rest == pending[t] \ batch
                fin == rest = {} \/ cfg[t].load # "INCR"
                \* submitConsequencesExclusive: ATOMIC consequences claim their
                \* positions here, BEFORE the batch below is released
                newKids == IF PSubmitBeforeRelease
                           THEN {c \in Tasks : TaskParent[c] = t /\ phase[c] = "New"
                                               /\ cfg[c].kind = "ATOMIC"}
                           ELSE {}
                \* upgrade already implies fifoAt[t] = 0
                fa1 == [u \in Tasks |->
                          IF u = t /\ upgrade THEN clock
                          ELSE IF u \in newKids
                          THEN (IF fifoAt[t] > 0 THEN fifoAt[t] ELSE clock)
                          ELSE fifoAt[u]]
            IN /\ pending' = [pending EXCEPT ![t] = IF fin THEN {} ELSE rest]
               /\ fifoAt' = fa1
               /\ clock' = clock + 1
               /\ plog' = [e \in Entries |->
                             IF e \in batch \/ (e \in TaskTxns[t] /\ firstRun)
                             THEN Append(plog[e], t) ELSE plog[e]]
               /\ phase' = [u \in Tasks |->
                              IF u = t THEN (IF fin THEN "Done" ELSE "Started")
                              ELSE IF u \in newKids THEN "Claimed" ELSE phase[u]]
    /\ UNCHANGED << cfg, adopted, loading >>

\* onLoadedExclusive: the drain re-places every waiter, fifo claims first
\* (compareForNotify), so the regions are rebuilt
LoadCompletes(e) ==
    /\ PModelLoading
    /\ e \in loading
    /\ loading' = loading \ {e}
    /\ UNCHANGED << cfg, phase, pending, adopted, fifoAt, plog, clock >>

\* addCachedKeyExclusive, the only way a reference set grows after setup.  It queues the
\* adopted key only when the task isState(WAITING); that is every phase in which a task can
\* adopt here, because Run is atomic, so "Started" means between rounds - which is WAITING -
\* and there is no mid-run phase to exclude.
Adopt(t, e) ==
    /\ PAllowAdoption
    /\ e \in KeyEntries /\ e \notin TaskKeys[t] /\ e \notin adopted[t]
    \* require(entry.isLoaded()), and require(!entry.isInconsistent()) - the latter vacuous
    \* here, since failure is not modelled
    /\ ~(PModelLoading /\ e \in loading)
    /\ phase[t] \in {"Claimed", "Started"}
    /\ PAllowFifoAdoption \/ ~IsFifo(t)
    /\ adopted' = [adopted EXCEPT ![t] = @ \cup {e}]
    /\ pending' = [pending EXCEPT ![t] = @ \cup {e}]
    /\ UNCHANGED << cfg, phase, fifoAt, plog, loading, clock >>

TaskNext(t) == Setup(t) \/ Run(t) \/ (\E e \in KeyEntries : Adopt(t, e))

Next == (\E t \in Tasks : TaskNext(t)) \/ (\E e \in Entries : LoadCompletes(e))

Init ==
    /\ cfg \in [Tasks -> ConfigDomain]
    /\ \A t \in Tasks : LegalConfigFor(t, cfg[t])
    /\ phase = [t \in Tasks |-> "New"]
    /\ pending = [t \in Tasks |-> TaskKeys[t]]
    /\ adopted = [t \in Tasks |-> {}]
    /\ fifoAt = [t \in Tasks |-> 0]
    /\ plog = [e \in Entries |-> << >>]
    /\ loading \in (IF PModelLoading THEN SUBSET Entries ELSE {{}})
    /\ clock = 1

Fairness == /\ \A t \in Tasks : WF_vars(TaskNext(t))
            /\ \A e \in Entries : WF_vars(LoadCompletes(e))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* THE WAIT RELATION                                                        *)
(***************************************************************************)
WaitsFor(a, b) ==
    /\ a # b
    /\ \E e \in Entries :
         /\ ~(PModelLoading /\ e \in loading)
         /\ HoldsPos(a, e)
         /\ \/ (a \notin Prefix(e,{},{},FALSE) /\ b \in Prefix(e,{},{},FALSE))
            \/ HoldsLock(b, e)

Live == {t \in Tasks : phase[t] # "Done"}

(***************************************************************************)
(* NoStuck: the real property.  Least fixpoint of "can eventually run" - a  *)
(* cross-check of AccordAcyclic.lean's exists_runnable, which proves the same *)
(* thing from RankOK at any size: the rank-minimal live task can run.        *)
(* task joins once the tasks already in it have finished and released.  This *)
(* accounts for thresholds, so it is weaker than acyclicity: a task needing  *)
(* only min(remaining, MIN_BATCH) keys is not blocked by failing to lead one.*)
(* It presumes each started task finishes its rounds.                       *)
(***************************************************************************)
RECURSIVE EventuallyRunnable(_)
EventuallyRunnable(S) ==
    LET nxt == S \cup {t \in Live \ S : TxnReady(t, S) /\ KeyReady(t, S)}
    IN IF nxt = S THEN S ELSE EventuallyRunnable(nxt)

NoStuck == Live \subseteq EventuallyRunnable({})

(***************************************************************************)
(* RankOK: the size-independent certificate, and the hypothesis set of       *)
(* AccordAcyclic.lean (wait_acyclic, exists_runnable).  Rank is <<layer, key>> *)
(* FIFO 0 < ORD 1 < BAG 2; every wait edge must strictly decrease it, which  *)
(* makes the relation a subset of a strict partial order.  Bag members share  *)
(* key 0, so a bag-to-bag edge - which Q3 forbids - shows up as a violation  *)
(* rather than passing silently.                                            *)
(*                                                                          *)
(* This single invariant is EXACTLY the four WaitEdges hypotheses of          *)
(* AccordAcyclic.lean: waitEdges_iff_rankOK there proves the equivalence, so  *)
(* checking RankOK here discharges h_layer, h_fifo, h_ord and h_no_bag       *)
(* together rather than one at a time.                                      *)
(*                                                                          *)
(* Region uniformity (O3) is not a checkable invariant here: Region is a     *)
(* function of the task by construction, so the model cannot express its     *)
(* violation.  It is enforced in the implementation by isUnsequenced(entry)'s *)
(* assertion, and ctl-unseq-incr-txn is the profile that removes the guard    *)
(* making it true.                                                          *)
(***************************************************************************)
Layer(t) == IF IsFifo(t) THEN 0 ELSE IF cfg[t].kind = "UNSEQ" THEN 2 ELSE 1
RankKey(t) == IF IsFifo(t) THEN FifoKey(t) ELSE IF Layer(t) = 2 THEN 0 ELSE t
RankLt(x, y) == \/ Layer(x) < Layer(y)
                \/ (Layer(x) = Layer(y) /\ RankKey(x) < RankKey(y))

RankOK == \A a, b \in Tasks : WaitsFor(a, b) => RankLt(b, a)

(***************************************************************************)
(* NoCycle: what RankOK certifies, checked independently so a wrong ranking  *)
(* is distinguishable from a real stall.                                    *)
(***************************************************************************)
RECURSIVE TransClose(_)
TransClose(R) ==
    LET nxt == R \cup {<<a,c>> \in Tasks \X Tasks :
                          \E b \in Tasks : <<a,b>> \in R /\ <<b,c>> \in R}
    IN IF nxt = R THEN R ELSE TransClose(nxt)

NoCycle == \A t \in Tasks :
               <<t,t>> \notin TransClose({<<a,b>> \in Tasks \X Tasks : WaitsFor(a,b)})

(***************************************************************************)
(* STRUCTURAL INVARIANTS, so a failure names the property that broke.       *)
(***************************************************************************)
\* every variable, so that an arithmetic or Append bug in Run/Adopt is named here rather
\* than surfacing as a confusing Inv_Isolation failure.  plog is typed element-wise:
\* Seq(Tasks) is infinite, so membership in it is not something to ask TLC for.
TypeOK ==
    /\ cfg \in [Tasks -> ConfigDomain]
    /\ phase \in [Tasks -> Phases]
    /\ pending \in [Tasks -> SUBSET KeyEntries]
    /\ adopted \in [Tasks -> SUBSET KeyEntries]
    /\ fifoAt \in [Tasks -> Nat]
    /\ DOMAIN plog = Entries
    /\ \A e \in Entries : \A i \in 1..Len(plog[e]) : plog[e][i] \in Tasks
    /\ clock \in Nat
    /\ loading \subseteq Entries

\* O7: a task that may hold a lock across runs is a fifo claim.  Unconditional, so
\* that ctl-no-upgrade (PUpgradeOnStart = FALSE) breaks this too rather than hiding
\* behind its own control: the invariant states the property, the flag removes the code
\* that establishes it.
Inv_LockerIsFifo ==
    \A t \in Tasks : (phase[t] = "Started" /\ HoldsAcrossRuns(t)) => IsFifo(t)

\* O8: the HOLD_QUEUE holder is the LEAST STAMPED fifo claim on its entry, so it leads
\* by Q5 and not merely because addFifo pins it there.  Sorting the fifo region by
\* fifoAt makes insertion independent of arrival, so a late claim with a lower stamp
\* would otherwise displace the holder; the pin keeps that safe but breaks the rank
\* certificate, so the implementation reports it (addFifo's Invariants.expect) and this
\* is the model-side statement of the same claim.
Inv_LockLeads ==
    \A t \in Tasks : \A e \in Entries :
        HoldsLock(t, e) =>
            \/ (PModelLoading /\ e \in loading)
            \/ LET f == {u \in Tasks : Occupies(u, e, {}, {}) /\ Region(u) = "FIFO"}
               IN t \in f /\ LeastStamped(f) = t

\* at most one prospective HOLD_QUEUE locker is ever in an entry's runnable prefix
Inv_OneProspectiveLocker ==
    \A e \in CmdEntries :
        ~\E t, u \in Tasks :
            /\ t # u
            /\ HoldsAcrossRuns(t) /\ e \in TaskTxns[t]
            /\ HoldsAcrossRuns(u) /\ e \in TaskTxns[u]
            /\ t \in Prefix(e,{},{},FALSE) /\ u \in Prefix(e,{},{},FALSE)

\* lockExclusive opens with require(!isLocked()), so at most one task holds HOLD_QUEUE on
\* an entry.  Prefix's lk branch chooses arbitrarily among holders, so without this a
\* double lock would be silently masked instead of naming the invariant that broke.
\* CanRun assumes the guard (NoForeignLock) rather than deriving it, so this invariant is
\* unfalsifiable unless the assumption is removed: ctl-double-lock (PAllowDoubleLock) is
\* that control, and is what makes this row evidence rather than a tautology.
Inv_AtMostOneLock ==
    \A e \in Entries : \A t, u \in Tasks :
        (HoldsLock(t, e) /\ HoldsLock(u, e)) => t = u

(***************************************************************************)
(* ISOLATION.  Proved size-independently by AccordAcyclic.lean's `isolated`  *)
(* from O5, O6, O13 and Q4; what is checked here is that those rules hold.   *)
(*                                                                          *)
(* ExecutionSequence.ATOMIC promises that a task and the consequences        *)
(* it submits ATOMICALLY appear to happen atomically, so on the processing   *)
(* order of any entry nothing outside that unit may appear between two of    *)
(* its members.                                                             *)
(*                                                                          *)
(* The unit is the chain of ATOMIC SUBMISSIONS, not every descendant, and the *)
(* boundary is set by which consequences INHERIT THE STAMP, not by when they  *)
(* are submitted: preSetup's isAtomic() branch gives an                       *)
(* ATOMIC consequence its submitter's fifoAt and calls                        *)
(* setCacheQueuedFifoExclusive, so its claim is ordered by the submitter's     *)
(* stamp, and O13 has it in place before the submitter releases.  Production  *)
(* submits every consequence before the submitter completes                   *)
(* (Task.completeExclusiveNoExcept), so submission time does not distinguish   *)
(* them: a BY_PRIORITY consequence of an ATOMIC task takes its place by        *)
(* compare() like any other sequenced task and an UNSEQUENCED one is bagged,   *)
(* so neither is - or claims to be - part of the unit.  Following every parent *)
(* link instead would claim isolation the implementation does not attempt: see *)
(* the deep-chain topology, where task 4 is a BY_PRIORITY consequence of an    *)
(* ATOMIC task and a foreign task legitimately runs between them.             *)
(*                                                                          *)
(* Two distinct members of one unit implies the later is ATOMIC (only an     *)
(* ATOMIC link joins the unit), so no separate "one of them is ATOMIC" test  *)
(* is needed.  A single BY_PRIORITY INCR task interleaving its own rounds is *)
(* permitted, since a # b requires two distinct tasks.                       *)
(***************************************************************************)
RECURSIVE UnitOf(_)
UnitOf(t) == IF IsChild(t) /\ cfg[t].kind = "ATOMIC" THEN UnitOf(TaskParent[t]) ELSE t

Inv_Isolation ==
    \A e \in Entries :
        ~\E i, j, k \in 1..Len(plog[e]) :
            /\ i < j /\ j < k
            /\ LET a == plog[e][i] x == plog[e][j] b == plog[e][k]
               IN /\ a # b
                  /\ UnitOf(a) = UnitOf(b)
                  /\ UnitOf(x) # UnitOf(a)

\* Convenience conjunction for ad-hoc single runs; matrix.py checks the invariants
\* individually instead, so that a failure names the one that broke.
Safety ==
    /\ TypeOK
    /\ Inv_LockerIsFifo /\ Inv_LockLeads
    /\ Inv_OneProspectiveLocker /\ Inv_AtMostOneLock /\ Inv_Isolation
    /\ RankOK /\ NoCycle /\ NoStuck

Termination == <>[](\A t \in Tasks : phase[t] = "Done")

(***************************************************************************)
(* COVERAGE PROBES.  Negated reachability: TLC reporting one violated means  *)
(* the situation is reached.  A green table over a model that never holds a  *)
(* lock or waits on a threshold establishes nothing.                        *)
(***************************************************************************)
Probe_Contention == ~\E t, u \in Tasks : \E e \in Entries :
                        t # u /\ HoldsPos(t,e) /\ HoldsPos(u,e)
Probe_AnyWait == ~\E a, b \in Tasks : WaitsFor(a,b)
Probe_LockHeld == ~\E t \in Tasks : \E e \in Entries : HoldsLock(t,e)
Probe_LockHasWaiter == ~\E t, u \in Tasks : \E e \in Entries :
                           t # u /\ HoldsLock(t,e) /\ HoldsPos(u,e)
\* a FIFO claim queued behind a HOLD_QUEUE holder, so addFifo's pin was evaluated
\* against a real competitor rather than an empty region
Probe_LockHasFifoWaiter == ~\E t, u \in Tasks : \E e \in Entries :
                               /\ t # u /\ HoldsLock(t,e) /\ HoldsPos(u,e)
                               /\ IsFifo(u)
Probe_BetweenRounds == ~\E t \in Tasks : phase[t] = "Started" /\ pending[t] # {}
Probe_ThresholdWait ==
    ~\E t \in Tasks :
        /\ phase[t] = "Claimed" /\ LeadsAllTxns(t)
        /\ LET led == {e \in pending[t] : Leads(t,e)}
           IN Cardinality(led) > 0 /\ Cardinality(led) < Threshold(t)
\* a task holding key positions it does not lead while blocked on a txnId: the state
\* that used to be revoked, and is now simply kept
Probe_BlockedOnTxnHoldingKeys ==
    ~\E t \in Tasks : /\ phase[t] = "Claimed" /\ ~LeadsAllTxns(t)
                      /\ \E e \in pending[t] : ~Leads(t, e)
Probe_Consequence == ~\E t \in Tasks : IsChild(t) /\ phase[t] # "New"
\* a top level ATOMIC task: sequenced from setup, but stamped only on its first run,
\* and only if INCR
Probe_TopLevelAtomic == ~\E t \in Tasks : cfg[t].kind = "ATOMIC" /\ ~IsChild(t)
                                          /\ phase[t] # "New"
\* two members of one atomic unit both processed an entry, so isolation had something
\* to protect there
Probe_UnitRevisits ==
    ~\E e \in Entries : \E i, j \in 1..Len(plog[e]) :
        /\ i < j /\ UnitOf(plog[e][i]) = UnitOf(plog[e][j])
        /\ plog[e][i] # plog[e][j]

=============================================================================
