# Accord execution queues: formal model

Formal model of the wait relation induced by the per-CommandStore execution queues —
`AccordCacheEntry`, `AccordCacheEntryQueue`, `SafeTask` — and of the invariants that make it
deadlock free.

```
INVARIANTS.md            the invariants, each tied to the code that establishes it
tla/AccordExec.tla       the ordering model: acyclicity, thresholds, isolation
tla/AccordNotify.tla     the notification layer, checked separately
tla/matrix.py            driver: runs TLC over (topology x profile) and tabulates
tla/notify.py            driver: runs TLC over (shape x profile) for AccordNotify
lean/AccordAcyclic.lean  rank-decreasing => acyclic, for any number of tasks
```

## Running it

```bash
cd tla
./matrix.py --list
./matrix.py --profiles baseline --topologies hold-vs-consequence
./matrix.py                                  # the full matrix; ~55 min
./matrix.py --liveness --profiles baseline    # adds Termination (temporal)
./notify.py                                  # the notification layer; ~2 minutes
```

Both drivers **exit non-zero** on any deviation from the expected matrix, so they can be
run as a regression check. That matters most for the controls: a `ctl-*` profile that
stops breaking its property has silently become vacuous, which is the failure mode the
whole scheme guards against, so the expectation is recorded as data (`EXPECT_FAIL`) and a
*missing* failure is an error just as much as an unexpected one.

Both check temporal properties in a pass of their own, after the invariant loop. That is not
cosmetic: TLC checks liveness *during* the search and stops at the first violation, so a
property checked alongside the invariants can abort the safety search — and then the remaining
invariants would print as `ok` when they were never finished, which is exactly what the `?`
column exists to prevent.

Needs `tlc` on the `PATH` (TLA+ tools 1.8+, Java 11+). `tla/examples` is where to run a
single cell while iterating (see that directory's README). The Lean file has no
dependencies:

```bash
cd lean && lean AccordAcyclic.lean           # silence == proved
```

Rough costs: most `AccordExec` cells are seconds, and the full 15 x 8 matrix with probes is
about 55 minutes at `--jobs 5` on 5 cores. The one outlier is
`baseline-full/non-leading-waiter`, ~13 minutes on a dedicated core. Because a timeout is
now an error rather than a shrug, the default `--timeout` is deliberately generous; if you
oversubscribe (TLC runs 2 workers per cell, so `--jobs` above the core count does) raise it
rather than lowering it. `notify.py` is about two minutes for everything.

## Structure

Two layers and a lemma, each assuming only the one below it:

| layer | module | assumes | establishes |
|---|---|---|---|
| ordering | `AccordExec.tla` | readiness is signalled correctly (G1/G2) | the certificate and the scheduling rules hold in every reachable state |
| notification | `AccordNotify.tla` | `AccordExec`'s region assignment and queue order (it has no lock, and orders regions by task index) | G1 soundness, G2 no lost wakeup, G3 set discipline, G4 bounded delivery, L3 handlers take no position |
| lemma | `AccordAcyclic.lean` | the rank certificate, plus six scheduling rules for isolation | acyclicity, **no stuck set** and **isolation**, at any size |

### "the certificate holds in every reachable state"

The two halves of the argument quantify over different things, and it is worth being explicit about
which is which.

*Certificate* is `RankOK`: the rank `⟨layer, key⟩` together with the claim that **every wait edge
strictly decreases it**. It is a property of a *single state* — take the state, compute the wait
relation from the positions and locks in it, check each edge — and it is a witness for the properties
we actually want, in the sense that a rank exhibits acyclicity the way a certificate exhibits
primality: cheap to check, and it implies the global property without anyone having to search for a
cycle.

*Reachable state* is the model-checking sense: a valuation of the variables (`phase`, `pending`,
`fifoAt`, `plog`, …) that the model can actually get into — `Init`, then any finite number of `Next`
steps. `INVARIANT RankOK` in a `.cfg` asks TLC to enumerate every such state of that finite instance
and evaluate `RankOK` on each; the *N states generated, M distinct* line is the size of that set.

So the obligation splits:

| | quantifies over | who does it | size-independent? |
|---|---|---|---|
| certificate ⇒ acyclicity, and ⇒ some live task can run | one state, any number of tasks | `AccordAcyclic.lean` | yes |
| certificate holds in every reachable state | all states of a run, at 2–4 tasks | TLC | no |

For acyclicity that split is *exact*, and it is now proved rather than asserted:
`waitEdges_iff_rankOK` in the Lean file shows the four `WaitEdges` hypotheses are
equivalent to the single `RankOK` invariant TLC checks. So "TLC's job is to show the
implementation maintains the hypotheses" is literally true — it discharges all four at
once, and `wait_acyclic_of_rankOK` consumes what TLC checks directly.

**Isolation does not split the same way**, and the earlier text here claimed it did.
`isolated` derives isolation from six local rules about the schedule, but TLC checks
`Inv_Isolation` — the *conclusion* — and none of those six rules has a counterpart
invariant in `AccordExec.tla`. Two of them cannot even be stated there: `handover` and
`fifo_claim_order` quantify over claim times, and the operational model records only run
order (`plog`), not when a position was taken. So for isolation the Lean theorem and the
model check are two *independent* arguments for the same property, not two halves of one:
the size-independent version rests on rules whose correspondence to the code is argued in
the Lean file's §6 table, and the model-checked version rests on 2–4 tasks. Closing the
gap needs a `claimedAt` variable in the model; the rows that are argued rather than checked
are marked as such in that table.

The hypotheses of all three theorems, and what catches each one's violation, are tabulated
in `lean/AccordAcyclic.lean` §6 — that table is the whole review surface, since nothing in
the Lean file mentions the implementation. §5 there shows the hypothesis bundles are
satisfiable and non-degenerate, without which `isolated` — which concludes `False` — could
be true merely because its hypotheses cannot all hold.

The reachability half is the one still bounded by scale, and it is bounded because TLC establishes it by
*reachability* (enumerate and evaluate) rather than by *induction* (`Init ⇒ Inv`, and
`Inv ∧ Next ⇒ Inv'`). An inductive proof would be size-independent, but only if the invariant is
inductive, which `RankOK` alone is not: it needs the structural invariants as conjuncts —
`Inv_LockerIsFifo`, `Inv_LockLeads`, `Inv_OneProspectiveLocker` are exactly such auxiliary facts —
and probably more. Finding that strengthening is the expensive part, and Apalache's inductive mode is a
cheaper way to attempt it than a proof assistant.

`AccordExec` derives readiness from the queues rather than modelling `waitingFor` and
`blocking`/`notBlocking`. That abstraction is an obligation, not an assumption: `AccordNotify`
discharges it by modelling the real bookkeeping, including the two `waitingFor` counters, the
notifications a queue mutation emits while it is still in progress, and the O7 upgrade
(`Upgrade`, i.e. `moveToFifo`) — which is the one step that moves a position between regions,
and the one where a notification is deliberately suppressed on the way (`remove` with a null
owner, `addFifo` with the real one, and the mover's own status folded in inline by
`onKeyMovedToFifo`). Regions are therefore a *variable* there, not a constant, so G2 is checked
across the upgrade rather than only over static-region shapes.

## Properties

| property | meaning |
|---|---|
| `NoStuck` | **the real property.** No set of live tasks can be mutually blocked. A least fixpoint of "can eventually run", so it accounts for execution thresholds: a task needing only `min(remaining, MIN_BATCH)` keys is not blocked by failing to lead one of them. Also proved size-independently in Lean (`exists_runnable`): the rank-minimal live task has no outgoing wait edge, so it leads every entry it holds and can run. TLC's job here is only to cross-check the fixpoint against the rank at small scale. |
| `NoCycle` | the wait relation is acyclic. Sufficient but not necessary — with thresholds a cycle can be survivable. Checked independently of `RankOK` so a wrong ranking is distinguishable from a real stall. |
| `RankOK` | every wait edge strictly decreases `⟨layer, key⟩`, layer `FIFO 0 < ORDERED 1 < BAG 2`. This is the **size-independent** certificate: `AccordAcyclic.lean` derives acyclicity from it for any number of tasks and entries, and `waitEdges_iff_rankOK` there proves this single invariant is exactly that file's four hypotheses, so TLC discharges all of them at once. |
| `Inv_Isolation` | `BY_PRIORITY_ATOMIC` promises a task and the consequences it submits *atomically* appear atomic, so on the processing order of any entry nothing outside that unit appears between two of its members. The unit is the chain of ATOMIC submissions (O14), not every descendant. Checked against the processing history, not the queue arrangement — a task that has already run cannot be reordered. Also proved size-independently in Lean (`isolated`), but from six scheduling rules that TLC does **not** check — see "Structure" above. |

Plus `Inv_LockerIsFifo` (O7), `Inv_LockLeads` (O8), `Inv_OneProspectiveLocker` (O11) and
`Inv_AtMostOneLock`, so a failure names the invariant that broke rather than only
"deadlock". The last of those is there because `Prefix` picks arbitrarily among lock
holders, so a double lock would otherwise be masked rather than named — and note that it can
only *fail* under `ctl-double-lock`, since `CanRun` conjoins `NoForeignLock`: see Controls.

`Inv_LockLeads` is worth spelling out because it is stronger than "the `HOLD_QUEUE` holder leads its
entry", which `addFifo` now guarantees by construction: it asserts the holder is the *least stamped*
fifo claim there, i.e. that it leads by Q5 and not merely because it is pinned. The pin keeps a
displacement safe, but a displaced holder's waiter has a wait edge that runs against the stamp order,
so `RankOK` and the Lean lemma would no longer apply. The implementation reports that case
(`Invariants.expect` in `addFifo`) rather than relying on the pin silently, and this invariant is the
model-side statement of the same claim.

### Coverage probes

Each cell also reports which situations were actually reached — a held lock, a lock with a waiter, a
lock with a *fifo* waiter (so the pin was evaluated against a real competitor), a task between
rounds, a threshold wait, keys held while blocked on a txnId, a consequence, a unit revisiting an
entry. They are negated reachability claims, so TLC reporting one "violated" means the situation *is*
reached. **A green table over a model that never holds a lock establishes nothing**; treat an
unreached probe as an unchecked row. Under `baseline`, `consequence-non-subset` and `two-txns` each
reach all of them, and the other topologies drop a few apiece.

`notify.py` is stricter, because its probe expectations are derivable: every probe there
must be reached except `Probe_Nested`, which is *expected* unreachable (no handler mutates a
queue position, so delivery cannot nest), `Probe_StaleBatch`/`Probe_EmptyBatch`, which
test `~TaskSync[t]` and so cannot fire in an all-SYNC shape, `Probe_Upgraded`/
`Probe_UpgradeDisplaces`, which need a non-sync sorted-region task to upgrade at all, and
`Probe_UpgradeWouldDoubleFile`, which is a documented **gap** rather than a design property
(see "Scale and abstraction boundaries"). The driver asserts all of that
in both directions and fails if a probe changes status.

It also checks `L3_HandlerTakesNoPosition`, `[][Deliver => UNCHANGED <<holds, region>>]_vars`:
a notification handler never inserts into, removes from, or moves within a queue. It is an
*action* property, which is why no invariant states it, and it is the model-side statement of
the runtime re-entrancy guard. It **holds** in all 16 cells — but by construction, since no
handler in the module mutates `holds` or `region`, so it is a regression check on the model
rather than independent evidence about the code: if a handler is ever given a mutation it fails,
and `Probe_Nested` starts being reached.

## Controls

The `P*` constants are two groups, and only one of them is a set of controls.
`PAlwaysReady`, `PModelLoading`, `PAllowAdoption` and `PPartialRounds` are *modelling*
switches — they choose which production behaviour is in scope, and must **not** break
anything, which is why the `baseline-*` and `ar-*` profiles are required green. Each `ctl-*`
profile disables one *rule* and must break a property: an assertion where the implementation
has one — `PAllowUnseqIncrWithTxn` → `requireSequencedIfHoldsLocksBetweenRuns`,
`PAllowFifoAdoption` → `adoptCachedKeyExclusive`'s `require(!isCacheQueuedFifo())`,
`PAllowDoubleLock` → `lockExclusive`'s `require(!isLocked())` — and otherwise the *code* that
establishes it: `PUpgradeOnStart` removes `prepareExclusiveMayThrow`'s `moveToFifo` block, and
`PSubmitBeforeRelease` removes the submit-before-complete ordering of
`Task.completeExclusiveNoExcept`, which **nothing in the implementation asserts** (INVARIANTS
O13 — that missing assertion is a work item for the code, not a modelling choice). Controls
are topology-sensitive — a control cannot fail on a topology that cannot build the situation
it enables — so the witnesses are named:

| profile | disables | breaks | witness topology |
|---|---|---|---|
| `ctl-no-upgrade` | O7, `prepareExclusiveMayThrow`'s `moveToFifo` | `Inv_LockerIsFifo`, `Inv_LockLeads` on **all but `deep-chain`** (which declares no txnId, so `HoldsAcrossRuns` is false for every task and both invariants have no instances); `RankOK`, `NoCycle`, `NoStuck` on the five below | `RankOK`/`NoCycle`/`NoStuck` witnesses: `two-lockers`, `hold-vs-consequence`, `non-leading-waiter`, `consequence-non-subset`, `two-txns`; O7/O8 additionally on `disjoint-txns` and `keys-only` |
| `ctl-defer-submit` | O13, consequences claiming before their submitter releases | `Inv_Isolation` | `consequence-non-subset`, `deep-chain`, `two-txns` |
| `ctl-unseq-incr-txn` | O11, `requireSequencedIfHoldsLocksBetweenRuns` | `Inv_OneProspectiveLocker` (two bagged prospective lockers both in the runnable prefix) | `two-lockers`, `non-leading-waiter`, `two-txns` |
| `ctl-fifo-adopt` | O5, `adoptCachedKeyExclusive`'s `!isCacheQueuedFifo()` | `Inv_Isolation` | `consequence-non-subset` |
| `ctl-double-lock` | A3, `lockExclusive`'s `require(!isLocked())` — **and** O7 with it, see below | `Inv_AtMostOneLock`; plus everything `ctl-no-upgrade` breaks, on the same topologies | `Inv_AtMostOneLock`: `two-lockers`, `non-leading-waiter`, `consequence-non-subset`, `two-txns` |

The same table is encoded in `matrix.py`'s `EXPECT_FAIL`, and the driver fails if a cell
deviates from it in either direction.

`ctl-double-lock` exists because `Inv_AtMostOneLock` was otherwise **unfalsifiable**: `CanRun`
conjoins `NoForeignLock`, i.e. the model *assumes* `require(!isLocked())` rather than deriving
it, so no profile could reach two `HOLD_QUEUE` holders and the row was a tautology dressed as
a check — precisely the "green over a model that never reaches the situation" failure mode this
scheme is built to expose. Dropping the guard alone is still not enough, and that is
informative in itself: while the holder is a fifo claim (O7) `addFifo` keeps it at the head, so
no other task can lead the entry and reach the lock at all. The situation needs the *pin* gone
too, which is why the profile also sets `PUpgradeOnStart = FALSE` and therefore repeats
`ctl-no-upgrade`'s failures. Read the row as: O7 + O8 are what make the guard's precondition
unreachable, and the guard is the backstop that names it if they ever fail. The witness needs
two *independent* tasks sharing a txnId — a consequence inherits its submitter's stamp and
sorts after it — which is why `hold-vs-consequence` does not break it.

`ctl-fifo-adopt` used to be filed as "not a control", on the grounds that a claim taken outside a
task's acquisition pass is placed by its key rather than by arrival, so relaxing the guard is sound on
*ordering* grounds. That much is confirmed — every ordering and liveness property stays green — but it
breaks isolation, which is what the guard is actually for: a fifo task with an *older* stamp that
adopts a key late is entitled by the stamp order to go first, and so lands between an ATOMIC
submitter and its consequence (`plog[k1] = <<1, 3, 2>>` on `consequence-non-subset`). The code comment
at `adoptCachedKeyExclusive` guessed as much — *"it likely impacts the atomicity guarantee"* — and this
is the witness.

`two-lockers` exists for the `ctl-unseq-incr-txn` control: two *independent* tasks must share a txnId
for two prospective lockers to meet, and in a parent/child topology the submitter has already upgraded
to fifo by the time the consequence exists, so the control cannot fire there.

The threshold profiles are two regimes, not a sweep: `MinBatch = 1` (what `alwaysReady`
produces) and `MinBatch = 9`, which at model scale exceeds every key set and so makes
thresholds inert, i.e. every non-sync task behaves as SYNC. `BlockedLimit = 1`
(`baseline-blocked`, `baseline-full`) models `NONSYNC_BLOCKED_LIMIT = 8` at model scale: the
escape that lets a non-sync task run below its batch threshold when enough of the keys it leads
are contended. It applies to non-sync tasks only — `NonSyncState.isWaitReady` is the only place
it lives — and `KeyReady`/`CanRun` say so with an explicit `load # "SYNC"` conjunct: a SYNC task
reaches `WAITING_TO_RUN` only through `waitToRunExclusive`'s `require(waitingFor == 0)`, and
`lockExclusive`'s `REQUIRE_RUNNABLE` would trip if it ran without leading everything.

One operational note: `baseline-full` and `ar-never` are the heavy profiles and want a core each.
With `--jobs` above the core count a cell may exceed `--timeout`, which is reported `?` and exits
non-zero — correct (absence of a violation in an incomplete run proves nothing) but it wastes the run;
drop `--jobs` to the core count, or raise `--timeout`, for those.

`two-txns` is the only topology in which a task declares both `context.primaryTxnId()` and
`context.additionalTxnId()`. Without it `LeadsAllTxns`/`TxnReady` are never a real
conjunction, and O10's asserted txnId-subset restriction on an ATOMIC consequence is only
ever `{} ⊆ X` or `X ⊆ X`; there task 2 declares a proper non-trivial subset. It also gives
each task two keys, so an INCR task can take a partial batch and hold both txnId locks
between rounds — without that it would finish in one round and never reach `Started`, which
is what `ctl-no-upgrade` needs in order to fire.

## Scale and abstraction boundaries

The model runs at 2–4 tasks and 2–4 entries. That is not enough on its own, which is what the Lean
file is for: it lifts both headline properties — acyclicity and no-stuck — from the rank certificate to
any size, leaving TLC to show only that the certificate holds in every reachable state. What is not
covered at any size is everything else: isolation, the counting arguments, and the representation.

Deliberately not modelled:

- **the array and union representation** of the queue (Q1–Q4, indices, compaction, and the
  `null | SafeTask | MiniQueue | Queue` cases of R8). The fifo order is derived from `fifoAt` (Q5)
  rather than from the array maintaining it; that algebra belongs to `validate()` and
  `AccordCacheEntryQueueTest`. The mini queue is the two-claim case of the same abstract queue.
- **eviction, shrink and save/load state transitions**. `LOADING` is modelled only as "carries no
  wait edge, and drains".
- **the drain loop's re-entrancy.** `onLoadedExclusive` re-places one drained task at a time, and
  each re-placement notifies; `AccordNotify` models the re-placement as a top-level step instead.
  Covered by `AccordCacheEntryCycleTest`'s `KEY_LOAD_COMPLETES` scenarios and
  `AccordCacheEntryReentrancyTest`.
- **`refs` iteration and cache listener re-entrancy**. `adoptCachedKeyExclusive` is modelled as a
  late claim, not as a mutation during the upgrade's iteration. Its state gate *is* faithful:
  it queues the adopted key only when the task `isState(WAITING)`, and because `Run` is one
  atomic step there is no mid-run phase in the model to exclude — `Started` means between
  rounds, which is `WAITING`.
- **the window inside `NonSyncState.prepareExclusive`.** `Run` is one atomic step, so the model
  cannot show a lock taken early in the pass revoking a key captured for the same batch. That window
  is what the re-check exists for; `AccordNotify`'s `Probe_StaleBatch` and `Probe_EmptyBatch` show
  the stale state and the empty round exist, but only mid-cascade.
- **an open workload.** The model is closed, so starvation questions — whether a task can be
  repeatedly displaced under sustained arrivals — cannot be settled either way. Only unbounded delay
  is at stake; a stuck state would appear as `NoStuck`. Note L2: a round whose captured keys were all
  revoked locks nothing, so per-round progress is not guaranteed, only per-lock progress.
- **the lock, in `AccordNotify`.** That module has no `HOLD_QUEUE` and no `lockExclusive`, and it
  orders both queue regions by task index rather than by `fifoAt`/`compare()`. So the "assumes:
  nothing" cell in the Structure table above is about the *bookkeeping*: the notification layer
  does assume `AccordExec`'s region assignment and its ordering, which is where those are
  established.
- **the mover's own status fold, in the dangerous case.** `AccordNotify`'s `Upgrade` models
  `onKeyMovedToFifo` as production writes it: `onNewHead`/`onNewBlockingHead` *add* to one batch
  set without removing from the other, unlike the delivered handler, which moves the key. A task
  that is Runnable with a stale `notBlocking` key and takes the prefix back by upgrading, with a
  competitor still queued there, would therefore hold that key in both sets — `G3_Disjoint` reports
  it. `Probe_UpgradeWouldDoubleFile` is exactly that precondition and is **unreached** at every
  shape (the stale state itself *is* reached — `Probe_StaleBatch`), so `G3_Disjoint`'s green is not
  evidence about that path. It is a probe, not a claim.

`Inv_Isolation` is a history property over the entries a task processes; the queue arrangement is
only a proxy for it, so it is checked on the history.

### What isolation covers, and what it does not

`Inv_Isolation` was originally stated over `OriginOf` — the top of the whole submission chain — with
the side condition that one of the pair be ATOMIC. That is falsified by the implementation as soon as
a chain is more than one submission deep, which no topology used to be. The `deep-chain` topology is
the witness: a foreign task 1 that sorts first by `compare()`, a submitter 2, an ATOMIC consequence 3
of 2, and a `BY_PRIORITY` consequence 4 of 3, all on one key:

```
Setup(2), Run(2), Run(3), Setup(1), Run(1), Setup(4), Run(4)
  => plog[k1] = <<2, 3, 1, 4>>
```

Nothing here is out of order. Task 3 is ATOMIC, so it inherited task 2's `fifoAt` and claimed its
position before task 2 released (O13), and ran immediately after it. Task 4 is `BY_PRIORITY`: it is
submitted before task 3 completes, like every consequence (`Task.completeExclusiveNoExcept` submits
unconditionally), but it inherits no stamp — only `preSetup`'s `isSequencedByPriorityAtomic()` branch
does that — so it joins the sorted region by `compare()`, behind task 1, which has a lower
`position`. Placement there is arrival-independent, which is why submitting it earlier does not put
it inside the unit. An UNSEQUENCED consequence would be weaker still: bagged, so it sorts after
everything sequenced.

So the guarantee is a property of *ATOMIC submissions*, not of descent: `UnitOf` walks up only while
the child is ATOMIC (O14), and `Inv_Isolation` is stated over that. Under this reading the model needs
no top-level-ATOMIC boundary either — `submitExclusiveMayThrow` applies `context.executionSequence()`
to top-level tasks, so `LegalConfigFor` now permits an ATOMIC task with no submitter, stamped only on
its first run and only if INCR, exactly as `prepareExclusiveMayThrow` does it. With the unit-based
statement, `baseline` is green over all **eight** topologies including `deep-chain`, and `ctl-defer-submit`
still breaks `Inv_Isolation` — on `consequence-non-subset` *and* `deep-chain` — so the weaker
statement has lost none of its teeth.

Two things this deliberately does **not** claim:

- a consequence that is not itself ATOMIC gets no isolation from its ATOMIC submitter. That is the
  intended reading (O14): only an ATOMIC sub-task declares a relation to its submitter, so the
  transitive closure of those links is the unit.
- a single `BY_PRIORITY` INCR task may interleave its own rounds with other tasks, since `a # b`
  requires two distinct tasks.

And one thing it claims *more* than the semantics intend — see below.

### UNSEQUENCED was meant to interleave, and does not

`BY_PRIORITY_ATOMIC` promises atomicity *"from the point of view of other SEQUENCED tasks"*, so an
UNSEQUENCED task was intended to be free to run in the middle of a unit. The implementation does not
permit that, and `Inv_Isolation` follows the implementation: Q4 makes the bag the *last* region, so
while anything sequenced is queued the bag is not runnable at all, and O13 keeps a unit's successor
claim in place before its predecessor releases, so the window never opens. Both acquisition paths
agree, for different reasons: the optimistic `UNQUEUED` path (`acquireIfLoadedAndPermitted`) is
refused while `hasFifoOrLocked()`, and a unit's members are fifo claims (O5/O7) — though note it *is*
permitted past a merely `BY_PRIORITY` claim, since it skips queue accounting entirely.

The strengthening is deliberate, and `probe-bag-interleaves` (`PBagInterleaves = TRUE`: the bag is
runnable behind a sequenced region, i.e. the intended semantics) measures what it buys:

| topology | breaks |
|---|---|
| `keys-only` | `RankOK` |
| `non-leading-waiter` | `RankOK`, `NoCycle` |
| `consequence-non-subset` | `Inv_Isolation`, `RankOK`, `NoCycle` |
| `deep-chain` | `Inv_Isolation` |
| `two-txns` | `Inv_Isolation`, `RankOK` |
| `two-lockers`, `hold-vs-consequence`, `disjoint-txns` | nothing (no unsequenced interleaver is
  possible there) |

The rank dies before anything else, and by construction rather than by accident: BAG is layer 2, the
top of `⟨layer, key⟩`, so putting a bag member in the prefix gives every sequenced task queued behind
it a wait edge *into* layer 2, which cannot decrease the rank. Nor is a bag member edge-free in the
other direction — `Wlock` gives it an edge to the `HOLD_QUEUE` holder of any entry it declared — and
those two directions close a real cycle on `non-leading-waiter` and `consequence-non-subset`.
`NoStuck` survives at this scale, because thresholds and other tasks make those cycles escapable,
which is precisely why the certificate rather than the fixpoint is the thing to preserve.

So the open problem is a rank for an interleaving bag member: it must be pair-determined (O1) and it
must sit *below* every sequenced task that may wait for it, while "unsequenced" is defined as having
no place in that order. Until there is such a formulation, work that must interleave has to avoid
being waited on at all — hold no position — which is what `UNQUEUED` referencing does, within the
limit noted above.

### The `G1_Strong` status

`AccordNotify` keeps `G1_Strong` ("every task believed runnable leads what it needs") because the
non-sync re-check exists precisely because it need not hold. With the revocation removed it now
*holds at quiescence* in every configuration checked, at every `MinBatch` and every shape: nothing
takes a position away any more, so the bookkeeping is only transiently wrong, inside a notification
loop (`Probe_StaleBatch`) or inside `prepareExclusive`'s own pass (not modelled — see the boundary
above). It is retained as a regression check, not as a known-failing property; a failure at
quiescence would mean a wakeup was lost or double counted, not that the re-check is doing its job.
It is checked — by `notify.py` and by `examples/Notify.cfg` — separately from `Guarantees`, so that a
failure names it.

### What `AccordNotify`'s G4 actually checks

`G4_Bounded` is `Depth <= MaxDepth`, and `Deliver` is guarded one level *above* it, at
`MaxDepth + 1`. That ordering is deliberate and load-bearing: with the guard at `MaxDepth`
the invariant would be true by construction and could never fail, and reaching the bound
would instead disable every action (all the others require quiescence) — a deadlock, which
`CHECK_DEADLOCK FALSE` then hides, leaving a *smaller* state space reporting all-green. As
written, `MaxDepth = 0` reports `G4_Bounded is violated`; `MaxDepth >= 1` explores the whole
space and passes. `G4_Drains` (`[]<>Quiescent`) is the temporal companion and needs
`PROPERTY`, not `INVARIANT`.
