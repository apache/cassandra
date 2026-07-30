------------------------------- MODULE AccordNotify -------------------------------
(***************************************************************************)
(* The notification layer, modelled separately so AccordExec may assume its *)
(* guarantees rather than model them.                                       *)
(*                                                                         *)
(* AccordExec derives readiness from the queues.  The implementation instead *)
(* maintains SafeTask.waitingFor - two counters packed in one int, one for   *)
(* txnIds and one for keys - and NonSyncState.blocking/notBlocking, updated  *)
(* by onChangeRunnableStatus, which is called from inside queue mutations    *)
(* and whose handlers mutate further, so delivery nests.                    *)
(*                                                                         *)
(* GUARANTEES                                                              *)
(*   G1 soundness      a task believed runnable leads what it needs         *)
(*   G2 completeness   a task that leads what it needs is believed runnable *)
(*                     - the lost wakeup an acyclic graph does not rule out *)
(*   G3 set discipline blocking and notBlocking are disjoint, and hold only *)
(*                     keys the task has a position on                     *)
(*   G4 termination    the re-entrant cascade drains                       *)
(*                                                                         *)
(* G1 splits, and the split is forced by the implementation.  A SYNC task's *)
(* prepareExclusiveMayThrow locks every reference with RELEASE_QUEUE, whose  *)
(* REQUIRE_RUNNABLE asserts the locker leads, so for SYNC believed-runnable  *)
(* MUST imply leads-everything.  A non-sync task's                          *)
(* NonSyncState.prepareExclusive re-checks each captured key and skips any   *)
(* it no longer leads, so only the positions are guaranteed there.  With the *)
(* revocation removed G1_Strong nevertheless HOLDS at quiescence in every    *)
(* configuration checked, and is retained as a regression check rather than  *)
(* as a known-failing property - see README, "The G1_Strong status".         *)
(*                                                                         *)
(* Notifications are generated as the delta in runnable status, which is the *)
(* specification the scattered call sites implement.  What is modelled       *)
(* concretely is where a notification is deliberately SUPPRESSED, since that *)
(* is where a wakeup can be lost: a mutation on a LOADING entry passes a     *)
(* null owner and the drain must make it up, and moveToFifo removes with a   *)
(* null owner too - so the removal notifies nobody - and re-adds with the     *)
(* real one, so only addFifo's delta is delivered and the mover's own status  *)
(* is folded in inline (onKeyMovedToFifo) instead of delivered.  Upgrade(t)   *)
(* is that step, and the only one that changes a task's region.              *)
(*                                                                         *)
(* NO REVOCATION.  A task takes its key positions in the same turn as its    *)
(* txnId positions and keeps them for its life, so losing a txnId leaves     *)
(* both the positions and the key waits counted for them in place            *)
(* (incrementWaitingTxns).  The deferral this module used to model -         *)
(* DeferredChangeOfRunnableStatus, postponing the handler that dropped every *)
(* key position until the current loop finished - was removed with           *)
(* QUEUE_ON_KEYS_AT_ONCE and is gone.  Re-entrancy remains, so G4 does too.  *)
(*                                                                         *)
(* Not modelled: the array representation.  "A re-entrant notification must  *)
(* not reorder the range being iterated" is a property of a sorted array     *)
(* under an index loop; here the sorted region is a set ordered by compare,  *)
(* so it cannot be reordered.  Covered by the paranoia assertion in          *)
(* AccordCacheEntryQueue.onChangeRunnableStatus.                            *)
(*                                                                         *)
(* Nor is the LOCK modelled: there is no HOLD_QUEUE and no lockExclusive     *)
(* here, and both queue regions are ordered by task index rather than by     *)
(* fifoAt / compare().  This module assumes AccordExec's region assignment    *)
(* and ordering and checks only the bookkeeping over them.                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
    CmdEntries, KeyEntries,
    NumTasks,        \* tasks are 1..NumTasks; numeric order is compare()
    TaskTxns,        \* [1..NumTasks -> SUBSET CmdEntries]
    TaskKeys,        \* [1..NumTasks -> SUBSET KeyEntries]
    TaskRegion,      \* [1..NumTasks -> {"FIFO","ORD","BAG"}]  the INITIAL region; the
                     \* variable `region` starts here and Upgrade(t) moves an ORD task
                     \* to FIFO (O7, prepareExclusiveMayThrow's moveToFifo)
    TaskSync,        \* [1..NumTasks -> BOOLEAN]  TRUE = SYNC, needs every key
    MinBatch,        \* NONSYNC_MIN_BATCH_SIZE
    MaxDepth,        \* bound on re-entrant nesting; exceeding it is a G4 failure.
                     \* Deliver is guarded one level ABOVE it, so G4_Bounded is violated
                     \* (and reported) before the guard truncates the search.
    PDrainNotifies,  \* load completion re-notifies everyone (negative control: FALSE)
    PModelLoading

Tasks   == 1..NumTasks
Entries == CmdEntries \cup KeyEntries

ASSUME CmdEntries \cap KeyEntries = {}

VARIABLES
    holds,      \* [Tasks -> SUBSET Entries]  positions actually held
    region,     \* [Tasks -> {"FIFO","ORD","BAG"}]  moved only by Upgrade
    state,      \* [Tasks -> {"New","Loading","WaitTxn","WaitKey","Runnable","Done"}]
    wtxn,       \* [Tasks -> Nat]  waitingForTxnCount()
    wkey,       \* [Tasks -> Nat]  waitingForKeyCount(); SYNC only, as non-sync
                \* counts keys through blocking/notBlocking instead
    blocking,   \* [Tasks -> SUBSET KeyEntries]
    notBlk,     \* [Tasks -> SUBSET KeyEntries]
    loading,    \* SUBSET Entries
    waitLoad,   \* [Tasks -> SUBSET Entries]  loads we must see complete before we
                \* evaluate readiness at all (Task.State.LOADING_REQUIRED)
    frames      \* Seq of Seq(note); a STACK, not a queue: onChangeRunnableStatus is a
                \* direct call, so a mutation performed by a handler runs to
                \* completion inside it

vars == << holds, region, state, wtxn, wkey, blocking, notBlk, loading, waitLoad,
           frames >>

IsFifo(t) == region[t] = "FIFO"
Quiescent == frames = << >>
Depth == Len(frames)

(***************************************************************************)
(* THE QUEUE.  fifo before sorted before bag; the runnable prefix is the    *)
(* first non-empty region.  Fifo order is task order here: this module is   *)
(* about the bookkeeping, and AccordExec owns the claim-ordering argument.  *)
(* The prefix is taken over a (holds, region) PAIR rather than over holds    *)
(* alone, because Upgrade changes a prefix by moving a region, not a        *)
(* position.                                                               *)
(***************************************************************************)
OccupantsOf(h, e) == {t \in Tasks : e \in h[t]}

PrefixOf(h, rg, e) ==
    IF PModelLoading /\ e \in loading THEN {}
    ELSE LET occ == OccupantsOf(h, e)
             f == {t \in occ : rg[t] = "FIFO"}
             o == {t \in occ : rg[t] = "ORD"}
         IN IF f # {} THEN {CHOOSE t \in f : \A u \in f : t <= u}
            ELSE IF o # {} THEN {CHOOSE t \in o : \A u \in o : t <= u}
            ELSE {t \in occ : rg[t] = "BAG"}

Prefix(e) == PrefixOf(holds, region, e)
Leads(t, e) == t \in Prefix(e)

(***************************************************************************)
(* THE TRUTH that the bookkeeping is supposed to track                      *)
(***************************************************************************)
Min2(a,b) == IF a < b THEN a ELSE b
HeldKeys(t) == TaskKeys[t] \cap holds[t]
LedKeys(t) == {e \in HeldKeys(t) : Leads(t, e)}
LeadsAllTxns(t) == \A e \in TaskTxns[t] : Leads(t, e)
Threshold(t) == IF TaskSync[t] THEN Cardinality(HeldKeys(t))
                ELSE Min2(Cardinality(HeldKeys(t)), MinBatch)
TrulyReady(t) == LeadsAllTxns(t) /\ Cardinality(LedKeys(t)) >= Threshold(t)

(***************************************************************************)
(* NOTIFICATION GENERATION                                                  *)
(*                                                                          *)
(* Seq(set) linearises a set of tasks in compare() order, which is the      *)
(* order compareForNotify delivers in.                                      *)
(***************************************************************************)
Note(t, e, st) == [task |-> t, entry |-> e, status |-> st]

SeqOf(S) == [i \in 1..Cardinality(S) |->
                CHOOSE t \in S : Cardinality({u \in S : u <= t}) = i]

\* The status change on entry e caused by moving from (h0, rg0) to (h1, rg1).
\* `self` - the task performing the mutation - is excluded: addPrioritised and
\* friends RETURN its status to it, they do not notify it, and it folds the return
\* value into its own bookkeeping inline.  Notifying it as well double-counts.
DeltaOn(h0, rg0, h1, rg1, e, self) ==
    LET p0 == PrefixOf(h0, rg0, e)
        p1 == PrefixOf(h1, rg1, e)
        lost == {t \in (p0 \ p1) \ {self} : e \in h1[t]}  \* still queued, no longer leading
        gained == (p1 \ p0) \ {self}
        more == Cardinality(OccupantsOf(h1,e)) > Cardinality(p1)
        gs == IF more THEN "NEWLY_BLOCKING_RUNNABLE" ELSE "NEWLY_RUNNABLE"
    IN [i \in 1..(Cardinality(lost) + Cardinality(gained)) |->
          IF i <= Cardinality(lost)
          THEN Note(SeqOf(lost)[i], e, "NOT_RUNNABLE")
          ELSE Note(SeqOf(gained)[i - Cardinality(lost)], e, gs)]

\* all notifications arising from a change of positions or of regions, over the entries in
\* `on`.  Entries outside `on` are SUPPRESSED: that is the null-owner argument passed by
\* AccordCacheEntry.add while loading, and by moveToFifo when it removes.
\* Entries are not ordered, and the order notifications are delivered ACROSS
\* entries is not specified by the implementation either, so CHOOSE fixes one.
RECURSIVE NotesFor(_, _, _, _, _, _)
NotesFor(h0, rg0, h1, rg1, on, self) ==
    IF on = {} THEN << >>
    ELSE LET e == CHOOSE x \in on : TRUE
         IN DeltaOn(h0, rg0, h1, rg1, e, self)
              \o NotesFor(h0, rg0, h1, rg1, on \ {e}, self)

NotifiableEntries ==
    IF PModelLoading THEN {e \in Entries : e \notin loading} ELSE Entries

(***************************************************************************)
(* FRAMES.  A mutation pushes a frame; delivering one of its notifications  *)
(* may mutate and push another, which drains first.                         *)
(***************************************************************************)
PushOn(fs, notes) == IF notes = << >> THEN fs ELSE Append(fs, notes)

TopNotes == frames[Len(frames)]
PopTop   == SubSeq(frames, 1, Len(frames) - 1)
\* drop the delivered notification, then push whatever it caused
AdvanceTop(notes) == PushOn([frames EXCEPT ![Len(frames)] = Tail(TopNotes)], notes)

(***************************************************************************)
(* READINESS as the task BELIEVES it, from the bookkeeping alone            *)
(***************************************************************************)
BelievedKeyReady(t) ==
    IF TaskSync[t] THEN wkey[t] = 0
    ELSE Cardinality(blocking[t] \cup notBlk[t])
             >= Min2(Cardinality(HeldKeys(t)), MinBatch)

(***************************************************************************)
(* queueOnKeysExclusive - take the key positions and record what came back:   *)
(*     RunnableStatus status = ensureCacheQueued(entry);                     *)
(*     if (optional) addQueuedOptionalKey(entry, status);                    *)
(*     else if (status == NOT_RUNNABLE) ++waitingForKeyCount;                *)
(***************************************************************************)
AfterClaim(t, h2) ==
    LET led == {e \in TaskKeys[t] : t \in PrefixOf(h2, region, e)}
        nb  == {e \in led : Cardinality(OccupantsOf(h2,e)) = 1}
        bl  == led \ nb
        w   == Cardinality(TaskKeys[t] \ led)
        rdy == IF TaskSync[t] THEN w = 0
               ELSE Cardinality(led) >= Min2(Cardinality(TaskKeys[t]), MinBatch)
    IN [ ready |-> rdy,
         wkey |-> IF TaskSync[t] THEN w ELSE 0,
         blk |-> IF TaskSync[t] THEN {} ELSE bl,
         nblk |-> IF TaskSync[t] THEN {} ELSE nb ]

(***************************************************************************)
(* HANDLERS  (SafeTask.onChangeTxnRunnableStatus / onChangeKeyRunnableStatus)*)
(*                                                                          *)
(* Each returns the new bookkeeping for the notified task and the new holds. *)
(* Positions are never given back, so `holds` only ever grows here - which   *)
(* is why there is nothing to defer.                                        *)
(***************************************************************************)

\* Handlers do not take, give up or move a position: `holds` and `region` are returned
\* unchanged, which is what L3_HandlerTakesNoPosition checks as a property of the model.
\* onChangeTxnRunnableStatus
HandleTxn(t, st) ==
    IF state[t] \in {"New", "Loading", "Done"}
    THEN \* not yet (or no longer) party to the queue protocol: waitOnTxnsExclusive
         \* has not run, so we hold no txnId queue position to have a status on
         [ state |-> state[t], wtxn |-> wtxn[t], wkey |-> wkey[t],
           blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]
    ELSE IF st = "NOT_RUNNABLE"
    THEN \* incrementWaitingTxns: we keep every position and every key wait we have
         \* counted, so coming back only has to put us where we were
         [ state |-> "WaitTxn", wtxn |-> wtxn[t] + 1, wkey |-> wkey[t],
           blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]
    ELSE IF st = "STILL_RUNNABLE_NEWLY_BLOCKING"
    THEN [ state |-> state[t], wtxn |-> wtxn[t], wkey |-> wkey[t],
           blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]
    ELSE \* newly runnable on a txnId
         LET w == IF wtxn[t] > 0 THEN wtxn[t] - 1 ELSE 0
         IN IF w = 0 /\ state[t] = "WaitTxn"
            THEN \* waitOnKeysExclusive: nothing to re-place, just re-evaluate
                 [ state |-> IF BelievedKeyReady(t) THEN "Runnable" ELSE "WaitKey",
                   wtxn |-> 0, wkey |-> wkey[t],
                   blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]
            ELSE [ state |-> state[t], wtxn |-> w, wkey |-> wkey[t],
                   blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]

\* onChangeKeyRunnableStatus.  Below WAITING_ON_TXN the key claims have not been
\* placed, so only a task that took a fifo position at setup can be notified; at
\* WAITING_ON_TXN and above the counters are maintained, but only a task that is
\* WAITING_ON_KEY may be promoted to run by one.
HandleKey(t, e, st) ==
    IF state[t] \in {"New", "Loading", "Done"}
    THEN [ state |-> state[t], wtxn |-> wtxn[t], wkey |-> wkey[t],
           blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]
    ELSE IF TaskSync[t]
    THEN LET w == IF st = "NOT_RUNNABLE" THEN wkey[t] + 1
                  ELSE IF st = "STILL_RUNNABLE_NEWLY_BLOCKING" THEN wkey[t]
                  ELSE IF wkey[t] > 0 THEN wkey[t] - 1 ELSE 0
         IN [ state |-> IF state[t] = "WaitTxn" THEN "WaitTxn"
                        ELSE IF w = 0 THEN "Runnable" ELSE "WaitKey",
              wtxn |-> wtxn[t], wkey |-> w,
              blk |-> blocking[t], nblk |-> notBlk[t], holds |-> holds ]
    ELSE LET nb == CASE st = "NOT_RUNNABLE" -> notBlk[t] \ {e}
                     [] st = "NEWLY_RUNNABLE" -> notBlk[t] \cup {e}
                     [] st = "NEWLY_BLOCKING_RUNNABLE" -> notBlk[t] \ {e}
                     [] st = "STILL_RUNNABLE_NEWLY_BLOCKING" -> notBlk[t] \ {e}
                     [] OTHER -> notBlk[t]
             bl == CASE st = "NOT_RUNNABLE" -> blocking[t] \ {e}
                     [] st = "NEWLY_RUNNABLE" -> blocking[t] \ {e}
                     [] st = "NEWLY_BLOCKING_RUNNABLE" -> blocking[t] \cup {e}
                     [] st = "STILL_RUNNABLE_NEWLY_BLOCKING" ->
                            IF e \in notBlk[t] THEN blocking[t] \cup {e} ELSE blocking[t]
                     [] OTHER -> blocking[t]
             ready == Cardinality(bl \cup nb) >= Min2(Cardinality(HeldKeys(t)), MinBatch)
         IN [ state |-> IF state[t] = "WaitTxn" THEN "WaitTxn"
                        ELSE IF ready THEN "Runnable" ELSE "WaitKey",
              wtxn |-> wtxn[t], wkey |-> wkey[t], blk |-> bl, nblk |-> nb,
              holds |-> holds ]

Handle(n) == IF n.entry \in CmdEntries THEN HandleTxn(n.task, n.status)
                                       ELSE HandleKey(n.task, n.entry, n.status)

(***************************************************************************)
(* ACTIONS                                                                  *)
(***************************************************************************)

\* deliver the next notification in the innermost frame.
\* The guard is MaxDepth + 1, one level ABOVE G4_Bounded, so that exceeding the bound is
\* reported as an invariant violation rather than silently truncating the state space: a
\* guard at MaxDepth would make G4_Bounded unfalsifiable, and on reaching the bound would
\* instead disable every action (the others all require Quiescent), which is a deadlock
\* that CHECK_DEADLOCK FALSE then hides.
Deliver ==
    /\ frames # << >>
    /\ TopNotes # << >>
    /\ Depth <= MaxDepth + 1
    /\ LET n == Head(TopNotes)
           r == Handle(n)
           notes == NotesFor(holds, region, r.holds, region, NotifiableEntries, n.task)
       IN /\ state'    = [state EXCEPT ![n.task] = r.state]
          /\ wtxn'     = [wtxn EXCEPT ![n.task] = r.wtxn]
          /\ wkey'     = [wkey EXCEPT ![n.task] = r.wkey]
          /\ blocking' = [blocking EXCEPT ![n.task] = r.blk]
          /\ notBlk'   = [notBlk EXCEPT ![n.task] = r.nblk]
          /\ holds'    = r.holds
          /\ frames'   = AdvanceTop(notes)
    /\ UNCHANGED << region, loading, waitLoad >>

PopFrame ==
    /\ frames # << >>
    /\ TopNotes = << >>
    /\ frames' = PopTop
    /\ UNCHANGED << holds, region, state, wtxn, wkey, blocking, notBlk, loading, waitLoad >>

\* ---- top-level (non-re-entrant) events ----------------------------------

\* submitExclusive: register, take the txnId positions, then - in the same turn, and
\* whether or not we lead them - the key positions, and evaluate
MustAwaitLoad(t) ==
    IF ~PModelLoading THEN {}
    ELSE loading \cap (IF TaskSync[t] THEN TaskTxns[t] \cup TaskKeys[t] ELSE TaskTxns[t])

Setup(t) ==
    /\ Quiescent
    /\ state[t] = "New"
    /\ LET awaits == MustAwaitLoad(t)
           h1 == [holds EXCEPT ![t] = @ \cup TaskTxns[t]]
           leadsTxns == \A e \in TaskTxns[t] : t \in PrefixOf(h1, region, e)
           h2 == [h1 EXCEPT ![t] = @ \cup TaskKeys[t]]
           nTxn == Cardinality({e \in TaskTxns[t] : t \notin PrefixOf(h1, region, e)})
           a == AfterClaim(t, h2)
           held == IF awaits # {}
                   THEN [holds EXCEPT ![t] = @ \cup ((TaskTxns[t] \cup TaskKeys[t])
                                                     \cap loading)]
                   ELSE h2
       IN /\ holds' = held
          /\ waitLoad' = [waitLoad EXCEPT ![t] = awaits]
          /\ state' = [state EXCEPT ![t] =
                         IF awaits # {} THEN "Loading"
                         ELSE IF ~leadsTxns THEN "WaitTxn"
                         ELSE IF a.ready THEN "Runnable" ELSE "WaitKey"]
          /\ wtxn'  = [wtxn EXCEPT ![t] = IF awaits # {} THEN 0 ELSE nTxn]
          /\ wkey'  = [wkey EXCEPT ![t] = IF awaits # {} THEN 0 ELSE a.wkey]
          /\ blocking' = [blocking EXCEPT ![t] = IF awaits = {} THEN a.blk ELSE {}]
          /\ notBlk' = [notBlk EXCEPT ![t] = IF awaits = {} THEN a.nblk ELSE {}]
          /\ frames' = PushOn(frames, NotesFor(holds, region, held, region,
                                              NotifiableEntries, t))
    /\ UNCHANGED << loading, region >>

\* prepareExclusive: capture the batch (which DRAINS blocking/notBlocking), then
\* re-check each key and lock only those we still lead, keeping our position on the
\* rest for a later batch.  A SYNC task does not re-check - it locks everything with
\* RELEASE_QUEUE, whose REQUIRE_RUNNABLE asserts it leads - which is why the strong
\* soundness guarantee is required for SYNC and only the weak one for non-sync.
Run(t) ==
    /\ Quiescent
    /\ state[t] = "Runnable"
    /\ LET captured == IF TaskSync[t] THEN HeldKeys(t) ELSE blocking[t] \cup notBlk[t]
           locked   == IF TaskSync[t] THEN captured
                                      ELSE {e \in captured : Leads(t, e)}
           rest     == HeldKeys(t) \ locked
           done     == rest = {} \/ TaskSync[t]
           h1       == [holds EXCEPT ![t] = IF done THEN {} ELSE @ \ locked]
       IN /\ holds' = h1
          /\ state' = [state EXCEPT ![t] = IF done THEN "Done" ELSE "WaitKey"]
          /\ wtxn' = [wtxn EXCEPT ![t] = 0]
          /\ wkey' = [wkey EXCEPT ![t] = 0]
          \* populate() drains both sets whether or not the key survives the re-check
          /\ blocking' = [blocking EXCEPT ![t] = {}]
          /\ notBlk' = [notBlk EXCEPT ![t] = {}]
          /\ frames' = PushOn(frames, NotesFor(holds, region, h1, region,
                                              NotifiableEntries, t))
    /\ UNCHANGED << loading, waitLoad, region >>

(***************************************************************************)
(* THE O7 UPGRADE.  prepareExclusiveMayThrow moves an INCR task that will     *)
(* hold locks across runs - or that owes an ATOMIC isolation guarantee - to   *)
(* the fifo region on every entry it holds, before it locks anything.  This   *)
(* is the only step that moves a position between regions, and it is where a  *)
(* wakeup is most easily lost: moveToFifo REMOVES with a null owner (so the   *)
(* removal notifies nobody), re-adds with the real one (so only addFifo's     *)
(* delta is delivered), and hands the mover its own status back as a return   *)
(* value that onKeyMovedToFifo folds in inline.                              *)
(*                                                                          *)
(* Abstraction: a top-level step, so the notifications it raises drain before *)
(* the run that follows captures its batch.  In production the two are in one *)
(* exclusive turn, so treating them as separate steps admits interleavings    *)
(* production does not have - an over-approximation.  Only a non-sync ORD     *)
(* task upgrades: isIncremental() implies non-sync, and an UNSEQUENCED (bag)  *)
(* task is barred from holding a lock across runs by O11 and is never ATOMIC. *)
(***************************************************************************)
Upgrade(t) ==
    /\ Quiescent
    /\ ~TaskSync[t]
    /\ region[t] = "ORD"          \* !isCacheQueuedFifo()
    /\ state[t] = "Runnable"      \* the upgrade is the first thing prepareExclusive does
    /\ LET rg == [region EXCEPT ![t] = "FIFO"]
           \* onKeyMovedToFifo, folded in for the mover rather than delivered to it: the
           \* entries it did not lead before and leads now.  A command entry cannot appear
           \* here - a Runnable task leads every txnId it holds (wtxn = 0) - which is what
           \* makes onKeyMovedToFifo's key-only bookkeeping sound; the model restricts the
           \* fold to keys for that reason.  isLoaded() gates the fold, hence
           \* NotifiableEntries.
           gained == {e \in HeldKeys(t) :
                        /\ e \in NotifiableEntries
                        /\ t \notin PrefixOf(holds, region, e)
                        /\ t \in PrefixOf(holds, rg, e)}
           nb == {e \in gained : Cardinality(OccupantsOf(holds, e)) = 1}
       IN /\ region' = rg
          \* onNewHead / onNewBlockingHead add to one set; unlike the delivered handler
          \* they do not remove from the other, so a stale batch member that becomes led
          \* with a competitor present would end up in both.  G3_Disjoint is the check.
          /\ notBlk' = [notBlk EXCEPT ![t] = @ \cup nb]
          /\ blocking' = [blocking EXCEPT ![t] = @ \cup (gained \ nb)]
          /\ frames' = PushOn(frames, NotesFor(holds, region, holds, rg,
                                              NotifiableEntries, t))
    /\ UNCHANGED << holds, state, wtxn, wkey, loading, waitLoad >>

\* onLoadedExclusive: the drain.  Nothing on a loading entry was ever notified, so
\* the drain has to make up the whole delta - PDrainNotifies=FALSE is the negative
\* control showing what is lost if it does not.
LoadCompletes(e) ==
    /\ PModelLoading
    /\ Quiescent
    /\ e \in loading
    /\ loading' = loading \ {e}
    /\ waitLoad' = [t \in Tasks |-> waitLoad[t] \ {e}]
    /\ frames' =
         IF ~PDrainNotifies THEN frames
         ELSE LET occ == OccupantsOf(holds, e)
                  p == IF occ = {} THEN {}
                       ELSE LET f == {t \in occ : region[t] = "FIFO"}
                                o == {t \in occ : region[t] = "ORD"}
                            IN IF f # {} THEN {CHOOSE t \in f : \A u \in f : t <= u}
                               ELSE IF o # {} THEN {CHOOSE t \in o : \A u \in o : t <= u}
                               ELSE {t \in occ : region[t] = "BAG"}
                  more == Cardinality(occ) > Cardinality(p)
                  gs == IF more THEN "NEWLY_BLOCKING_RUNNABLE" ELSE "NEWLY_RUNNABLE"
              IN PushOn(frames, [i \in 1..Cardinality(p) |->
                                    Note(SeqOf(p)[i], e, gs)])
    /\ UNCHANGED << holds, region, state, wtxn, wkey, blocking, notBlk >>

\* onLoadedRequiredExclusive: this task's last required load has landed, so it now
\* runs waitOnTxnsExclusive - taking its txnId and then its key positions.  Per task,
\* as onLoadOne is.
OnLoaded(t) ==
    /\ Quiescent
    /\ state[t] = "Loading"
    /\ waitLoad[t] = {}
    /\ LET h1 == [holds EXCEPT ![t] = @ \cup TaskTxns[t]]
           leadsTxns == \A x \in TaskTxns[t] : t \in PrefixOf(h1, region, x)
           h2 == [h1 EXCEPT ![t] = @ \cup TaskKeys[t]]
           a == AfterClaim(t, h2)
       IN /\ holds' = h2
          /\ state' = [state EXCEPT ![t] =
                         IF ~leadsTxns THEN "WaitTxn"
                         ELSE IF a.ready THEN "Runnable" ELSE "WaitKey"]
          /\ wtxn' = [wtxn EXCEPT ![t] =
                        Cardinality({x \in TaskTxns[t] : t \notin PrefixOf(h1, region, x)})]
          /\ wkey' = [wkey EXCEPT ![t] = a.wkey]
          /\ blocking' = [blocking EXCEPT ![t] = a.blk]
          /\ notBlk' = [notBlk EXCEPT ![t] = a.nblk]
          /\ frames' = PushOn(frames, NotesFor(holds, region, h2, region,
                                              NotifiableEntries, t))
    /\ UNCHANGED << loading, waitLoad, region >>

Next ==
    \/ Deliver \/ PopFrame
    \/ \E t \in Tasks : Setup(t) \/ Run(t) \/ OnLoaded(t) \/ Upgrade(t)
    \/ \E e \in Entries : LoadCompletes(e)

Init ==
    /\ holds = [t \in Tasks |-> {}]
    /\ region = TaskRegion
    /\ state = [t \in Tasks |-> "New"]
    /\ wtxn = [t \in Tasks |-> 0]
    /\ wkey = [t \in Tasks |-> 0]
    /\ blocking = [t \in Tasks |-> {}]
    /\ notBlk = [t \in Tasks |-> {}]
    /\ loading \in (IF PModelLoading THEN SUBSET Entries ELSE {{}})
    /\ waitLoad = [t \in Tasks |-> {}]
    /\ frames = << >>

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

(***************************************************************************)
(* THE GUARANTEES                                                           *)
(*                                                                          *)
(* Checked only at quiescence: mid-cascade the bookkeeping is legitimately  *)
(* inconsistent, since a notification loop updates one task at a time.      *)
(***************************************************************************)
TypeOK ==
    /\ state \in [Tasks -> {"New","Loading","WaitTxn","WaitKey","Runnable","Done"}]
    /\ region \in [Tasks -> {"FIFO","ORD","BAG"}]
    /\ wtxn \in [Tasks -> Nat] /\ wkey \in [Tasks -> Nat]
    /\ \A t \in Tasks : blocking[t] \subseteq KeyEntries /\ notBlk[t] \subseteq KeyEntries

(*-------------------------------------------------------------------------*)
(* G1 SPLITS BY SYNC-NESS, and the split is forced by the implementation.    *)
(*                                                                           *)
(* A SYNC task's prepareExclusiveMayThrow locks every reference it holds with*)
(* RELEASE_QUEUE, and lockExclusive passes REQUIRE_RUNNABLE, which asserts   *)
(* the locker is the head.  So for SYNC tasks "believed runnable" MUST imply *)
(* "leads everything" - a violation is an assertion failure, not a stall.    *)
(*                                                                           *)
(* A non-sync task's NonSyncState.prepareExclusive instead RE-CHECKS each    *)
(* captured key and skips any it no longer leads.  So the strong property is *)
(* deliberately NOT maintained there; only the positions are guaranteed.     *)
(*-------------------------------------------------------------------------*)

\* required: a SYNC task that believes it can run really does lead everything
G1_SyncSound ==
    Quiescent =>
        \A t \in Tasks : (TaskSync[t] /\ state[t] = "Runnable") => TrulyReady(t)

\* required: whatever is in the batch sets, we still hold a position there, so the
\* re-check has something to keep for a later batch (addQueuedOptionalKey asserts this)
G1_PositionsHeld ==
    \A t \in Tasks : (blocking[t] \cup notBlk[t]) \subseteq HeldKeys(t)

\* Holds in every configuration checked, despite the non-sync re-check making it
\* unnecessary; checked separately from Guarantees so that a failure names it, since it
\* would mean a wakeup was lost or double counted rather than that the re-check is working.
G1_Strong == Quiescent => \A t \in Tasks : state[t] = "Runnable" => TrulyReady(t)

\* G2: completeness.  If we can run, we know it.  This is the lost-wakeup check.
G2_NoLostWakeup ==
    Quiescent =>
        \A t \in Tasks :
            \* a task that leads every txnId must have moved on to its keys
            /\ state[t] = "WaitTxn" => ~LeadsAllTxns(t)
            \* ...and one that leads enough keys must have been told it can run
            /\ (state[t] \in {"WaitKey","Runnable"} /\ TrulyReady(t))
                   => state[t] = "Runnable"

\* G3: the batch sets are disjoint (a key counted twice inflates readyCount and
\* would let a task run below its threshold)
G3_Disjoint == \A t \in Tasks : blocking[t] \cap notBlk[t] = {}

\* G4: the cascade drains.  G4_Bounded is the strict bound - Deliver is guarded one level
\* above it, so this can actually fail - and G4_Drains is the temporal statement, which
\* needs PROPERTY rather than INVARIANT.
G4_Bounded == Depth <= MaxDepth
G4_Drains == []<>(Quiescent)

\* L3: a notification handler never inserts into (or removes from, or moves within) a
\* queue.  The runtime re-entrancy guard states the same thing; here it is an ACTION
\* property, so it says what no state invariant can: the Deliver step itself leaves every
\* position and every region alone.  It is why the cascade is depth 1, which is what
\* Probe_Nested measures.  NOTE: in this module it is true BY CONSTRUCTION -
\* HandleTxn/HandleKey return `holds` unchanged and no handler touches `region` - so it is
\* a regression check on the model rather than independent evidence about the code: if a
\* handler is ever given a mutation, this fails and Probe_Nested starts being reached.
L3_HandlerTakesNoPosition == [][Deliver => UNCHANGED << holds, region >>]_vars

\* end-to-end: nothing is stranded
Termination == <>[](\A t \in Tasks : state[t] = "Done")

Guarantees ==
    /\ TypeOK /\ G4_Bounded
    /\ G1_SyncSound /\ G1_PositionsHeld /\ G3_Disjoint
    /\ G2_NoLostWakeup

(***************************************************************************)
(* COVERAGE PROBES - negated reachability, must be reported violated,       *)
(* except Probe_Nested: see below.                                         *)
(***************************************************************************)
\* EXPECTED UNREACHABLE.  A handler no longer mutates any queue position - the only
\* one that did was the revoked-keys drop - so a notification cannot generate another
\* and delivery is depth 1.  Retained as the check that this remains true: if this is
\* ever reported violated, handler re-entrancy is back and with it the question the
\* deferral used to answer.
Probe_Nested    == ~(Depth > 1)
Probe_Contention == ~(\E e \in Entries : Cardinality(OccupantsOf(holds,e)) > 1)
Probe_Runnable  == ~(\E t \in Tasks : state[t] = "Runnable")
\* blocked on a txnId while holding key positions: the state that used to be revoked,
\* and is now simply kept, so the counters have to survive it
Probe_KeptKeys  == ~(\E t \in Tasks : state[t] = "WaitTxn" /\ HeldKeys(t) # {})
\* ...and a key notification delivered to a task in that state
Probe_KeyNoteWhileWaitTxn ==
    ~(\E i \in 1..Len(frames) : \E j \in 1..Len(frames[i]) :
        /\ frames[i][j].entry \in KeyEntries
        /\ state[frames[i][j].task] = "WaitTxn")
\* the re-check actually discards a captured key: the race prepareExclusive guards
Probe_StaleBatch == ~(\E t \in Tasks :
                        /\ ~TaskSync[t]
                        /\ \E e \in blocking[t] \cup notBlk[t] : ~Leads(t, e))
\* ...and discards ALL of them, so the round locks nothing (the empty batch: processed
\* does not advance, so L2's per-round progress does not hold for that round)
Probe_EmptyBatch == ~(\E t \in Tasks :
                        /\ ~TaskSync[t] /\ state[t] = "Runnable"
                        /\ blocking[t] \cup notBlk[t] # {}
                        /\ \A e \in blocking[t] \cup notBlk[t] : ~Leads(t, e))
\* the O7 upgrade happened: a task moved from the sorted region to fifo, so G2 is checked
\* across the one step that changes a region rather than only over static shapes
Probe_Upgraded == ~(\E t \in Tasks : region[t] # TaskRegion[t])
\* ...and it took the prefix from a competitor, so the suppressed removal and addFifo's
\* delta were evaluated against a real queue rather than an empty one
Probe_UpgradeDisplaces ==
    ~(\E t, u \in Tasks : \E e \in Entries :
        /\ t # u /\ region[t] # TaskRegion[t] /\ IsFifo(t)
        /\ Leads(t, e) /\ e \in holds[u] /\ ~Leads(u, e))
\* EXPECTED UNREACHABLE, and it is a GAP rather than a design property, so it is stated
\* rather than left implicit.  onKeyMovedToFifo folds the mover's own status in with
\* onNewHead/onNewBlockingHead, which ADD to one batch set without removing from the other
\* (unlike the delivered handler, which moves the key).  So a task that is Runnable with a
\* key in notBlocking it no longer leads, and that would take the prefix back by upgrading
\* while a competitor is still queued there, would file that key into blocking as well -
\* two sets, one key, which G3_Disjoint reports.  Reaching this probe therefore says
\* "G3_Disjoint is now checking the fold"; not reaching it says the fold's dangerous case is
\* not built at these shapes, even though the stale state itself is (Probe_StaleBatch).
Probe_UpgradeWouldDoubleFile ==
    ~(\E t \in Tasks : \E e \in notBlk[t] :
        /\ Quiescent          \* Upgrade's own guard: mid-cascade it is not enabled
        /\ ~TaskSync[t] /\ region[t] = "ORD" /\ state[t] = "Runnable"
        /\ ~Leads(t, e)
        /\ t \in PrefixOf(holds, [region EXCEPT ![t] = "FIFO"], e)
        /\ Cardinality(OccupantsOf(holds, e)) > 1)

=============================================================================
