/-
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-/

/-!
# Acyclicity of the Accord execution wait relation

The size-independent half of the argument.  `AccordExec.tla` model-checks the
*hypotheses* below at small scale; this file proves that those hypotheses imply
acyclicity for **any** number of tasks and entries.

The division of labour is deliberate:

* whether the implementation maintains the hypotheses is a reachability question, and
  TLC answers it with counterexample traces;
* whether they suffice is an order-theoretic question, and it is answered here
  once and for all.

For acyclicity that split is exact, and §3's `waitEdges_iff_rankOK` proves it: the four
`WaitEdges` hypotheses are *equivalent* to the single `RankOK` invariant TLC checks.  For
isolation it is not - TLC checks the conclusion, `Inv_Isolation`, and the six `Discipline`
rules are argued against the code rather than model-checked.  §6 says which is which, row
by row, and §5 shows the hypothesis bundles are satisfiable at all.

No mathlib dependency: `lean AccordAcyclic.lean` is enough.
-/

namespace AccordAcyclic

/-! ## 1. The generic lemma -/

/-- Transitive closure.  `TC R a b` is "a reaches b by one or more R steps". -/
inductive TC {α : Type u} (R : α → α → Prop) : α → α → Prop where
  | base {a b} : R a b → TC R a b
  | step {a b c} : R a b → TC R b c → TC R a c

/-- A relation with no cycles: nothing reaches itself. -/
def Acyclic {α : Type u} (R : α → α → Prop) : Prop := ∀ a, ¬ TC R a a

/--
If every edge strictly decreases a rank into a transitive, irreflexive order,
the relation is acyclic.  This is the whole proof obligation that the layered
ranking discharges.
-/
theorem acyclic_of_rank_decreasing
    {α : Type u} {β : Type v}
    (R : α → α → Prop) (rank : α → β) (lt : β → β → Prop)
    (trans : ∀ {x y z}, lt x y → lt y z → lt x z)
    (irrefl : ∀ x, ¬ lt x x)
    (dec : ∀ a b, R a b → lt (rank b) (rank a)) :
    Acyclic R := by
  have key : ∀ {a b}, TC R a b → lt (rank b) (rank a) := by
    intro a b h
    induction h with
    | base hab => exact dec _ _ hab
    | step hab _ ih => exact trans ih (dec _ _ hab)
  intro a hcyc
  exact irrefl _ (key hcyc)

/-! ## 2. The order the ranking lands in: lexicographic on `Nat × Nat` -/

/-- `<<layer, key>>` compared lexicographically. -/
def Lex2 (x y : Nat × Nat) : Prop :=
  x.1 < y.1 ∨ (x.1 = y.1 ∧ x.2 < y.2)

theorem lex2_trans {x y z : Nat × Nat} : Lex2 x y → Lex2 y z → Lex2 x z := by
  intro hxy hyz
  rcases hxy with h1 | ⟨he1, h1⟩ <;> rcases hyz with h2 | ⟨he2, h2⟩
  · exact Or.inl (Nat.lt_trans h1 h2)
  · exact Or.inl (he2 ▸ h1)
  · exact Or.inl (he1 ▸ h2)
  · exact Or.inr ⟨he1.trans he2, Nat.lt_trans h1 h2⟩

theorem lex2_irrefl (x : Nat × Nat) : ¬ Lex2 x x := by
  intro h
  rcases h with h | ⟨_, h⟩ <;> exact absurd h (Nat.lt_irrefl _)

theorem lex2_total (x y : Nat × Nat) : Lex2 x y ∨ x = y ∨ Lex2 y x := by
  rcases Nat.lt_trichotomy x.1 y.1 with h | h | h
  · exact Or.inl (Or.inl h)
  · rcases Nat.lt_trichotomy x.2 y.2 with h2 | h2 | h2
    · exact Or.inl (Or.inr ⟨h, h2⟩)
    · refine Or.inr (Or.inl ?_)
      cases x; cases y; simp_all
    · exact Or.inr (Or.inr (Or.inr ⟨h.symm, h2⟩))
  · exact Or.inr (Or.inr (Or.inl h))

/-! ## 3. The Accord instantiation

`Region` is the queue region a task occupies.  The queue orders
`FIFO ≺ ORDERED ≺ BAG` in wait order (Q4 of `AccordCacheEntryQueue`).
-/

inductive Region where
  | fifo | ordered | bag
  deriving DecidableEq, Repr

/-- Wait-order layer: lower runs first. -/
def Region.layer : Region → Nat
  | .fifo => 0
  | .ordered => 1
  | .bag => 2

/--
The abstract model.  A `Task` has

* `region`  - the region it occupies.  **This is a function of the task alone**,
              which is exactly `O3`; the implementation establishes it by applying
              `context.executionSequence()` to every task and barring an UNSEQUENCED
              INCR task from declaring a txnId, and `isUnsequenced(entry)` asserts
              the resulting entry-independence directly.
* `fifoAt`  - the stamp a task takes when it first becomes a fifo claim (Q5),
              inherited by an ATOMIC consequence from its submitter, with ties
              broken by `createdAt`.
* `ordAt`   - its `compare()` key (O1: `(position, executionKind, createdAt)`,
              a strict total order, and a function of the pair alone, hence the
              same on every entry two tasks share).
-/
structure Model (Task : Type u) where
  region : Task → Region
  fifoAt : Task → Nat
  ordAt : Task → Nat

variable {Task : Type u} (M : Model Task)

/-- `<<layer, key>>`.  Bag members all share key 0, so a bag-to-bag edge can
never decrease the rank - which is how Q3 ("bag members are mutually
unordered, so none waits for another") is forced to be discharged rather than
silently assumed. -/
def rank (t : Task) : Nat × Nat :=
  (M.region t |>.layer,
   match M.region t with
   | .fifo => M.fifoAt t
   | .ordered => M.ordAt t
   | .bag => 0)

/--
The hypotheses, one per shape of wait edge.  Each is checked by TLC as an
invariant of the operational model (`AccordExec.tla`):

* `h_layer`   - Q4/O2: you only ever wait for the runnable prefix, and the
                prefix is drawn from the earliest non-empty region.
* `h_fifo`    - Q5 + O8: within the fifo region, order is by `fifoAt`; a
                `HOLD_QUEUE` holder is the fifo head, so a fifo waiter is behind it.
                Note that `addFifo` *pins* a `HOLD_QUEUE` holder at the head, which
                would satisfy "is the head" without satisfying this hypothesis; O8
                closes that gap by requiring the holder to be the least stamped claim
                on its entry, and the implementation reports it if it is not.
* `h_ord`     - Q1/O1: within the sorted region, order is `compare()`.
* `h_no_bag`  - Q3: two bag members never wait for one another (they are both
                in the runnable prefix).
-/
structure WaitEdges (R : Task → Task → Prop) : Prop where
  h_layer : ∀ a b, R a b → (M.region b).layer ≤ (M.region a).layer
  h_fifo : ∀ a b, R a b → M.region a = .fifo → M.region b = .fifo →
             M.fifoAt b < M.fifoAt a
  h_ord : ∀ a b, R a b → M.region a = .ordered → M.region b = .ordered →
            M.ordAt b < M.ordAt a
  h_no_bag : ∀ a b, R a b → M.region a = .bag → M.region b ≠ .bag

/-- Equal layers force equal regions, since `layer` is injective. -/
private theorem region_of_layer_eq {a b : Task}
    (h : (M.region a).layer = (M.region b).layer) : M.region a = M.region b := by
  cases hA : M.region a <;> cases hB : M.region b <;>
    simp [hA, hB, Region.layer] at h ⊢

/-- Every wait edge strictly decreases the rank: the case analysis both theorems below
rest on, one case per shape of edge. -/
theorem rank_lt_of_edge (R : Task → Task → Prop) (H : WaitEdges M R)
    {a b : Task} (hab : R a b) : Lex2 (rank M b) (rank M a) := by
  rcases Nat.lt_or_ge (M.region b).layer (M.region a).layer with hlt | hge
  · exact Or.inl hlt
  · -- h_layer gives ≤, so this branch is equality
    have heq : (M.region b).layer = (M.region a).layer :=
      Nat.le_antisymm (H.h_layer a b hab) hge
    have hr : M.region b = M.region a := region_of_layer_eq M heq
    refine Or.inr ⟨heq, ?_⟩
    cases hA : M.region a with
    | fifo =>
        have hB : M.region b = .fifo := by rw [hr, hA]
        simp [rank, hA, hB, Region.layer]
        exact H.h_fifo a b hab hA hB
    | ordered =>
        have hB : M.region b = .ordered := by rw [hr, hA]
        simp [rank, hA, hB, Region.layer]
        exact H.h_ord a b hab hA hB
    | bag =>
        -- both are bag, which h_no_bag forbids
        have hB : M.region b = .bag := by rw [hr, hA]
        exact absurd hB (H.h_no_bag a b hab hA)

/--
**The bridge to TLC.**  `AccordExec.tla` does not check the four hypotheses one at a time;
it checks a single invariant, `RankOK == \A a,b : WaitsFor(a,b) => RankLt(b,a)`, which is
literally the right-hand side below.  This equivalence is what makes "TLC shows the
implementation maintains the hypotheses" a true statement rather than an approximate one:
`RankOK` is not merely implied by `WaitEdges`, it is the same thing.

The reverse direction is where the encoding earns its keep.  `h_no_bag` falls out because
bag members share key `0`, so `Lex2` on two bag members reduces to `0 < 0`; and `h_layer`
falls out as `≤` because `Lex2` allows the layers to be equal.
-/
theorem waitEdges_iff_rankOK (R : Task → Task → Prop) :
    WaitEdges M R ↔ ∀ a b, R a b → Lex2 (rank M b) (rank M a) := by
  constructor
  · intro H a b hab; exact rank_lt_of_edge M R H hab
  · intro h
    refine ⟨?_, ?_, ?_, ?_⟩
    · intro a b hab
      rcases h a b hab with hl | ⟨he, _⟩
      · exact Nat.le_of_lt hl
      · exact Nat.le_of_eq he
    · intro a b hab ha hb
      rcases h a b hab with hl | ⟨_, hk⟩
      · simp [rank, ha, hb, Region.layer] at hl
      · simpa [rank, ha, hb] using hk
    · intro a b hab ha hb
      rcases h a b hab with hl | ⟨_, hk⟩
      · simp [rank, ha, hb, Region.layer] at hl
      · simpa [rank, ha, hb] using hk
    · intro a b hab ha hb
      rcases h a b hab with hl | ⟨_, hk⟩
      · simp [rank, ha, hb, Region.layer] at hl
      · simp [rank, ha, hb] at hk

/-- **Main theorem.**  Under the four hypotheses the wait relation is acyclic,
for any set of tasks and entries whatsoever. -/
theorem wait_acyclic (R : Task → Task → Prop) (H : WaitEdges M R) : Acyclic R :=
  acyclic_of_rank_decreasing R (rank M) Lex2 lex2_trans lex2_irrefl
    (fun _ _ hab => rank_lt_of_edge M R H hab)

/-- The same conclusion straight from what TLC checks, with no `WaitEdges` in sight. -/
theorem wait_acyclic_of_rankOK (R : Task → Task → Prop)
    (H : ∀ a b, R a b → Lex2 (rank M b) (rank M a)) : Acyclic R :=
  wait_acyclic M R ((waitEdges_iff_rankOK M R).mpr H)

/-!
### No stuck set

Acyclicity is necessary for progress but not obviously sufficient; the property that
matters is that some live task can always run.  It follows from the same hypotheses by
taking the rank-minimal live task, and the proof is short enough that there is no reason
to leave it to model checking at 2-4 tasks (`NoStuck` in `AccordExec.tla`).

The extra obligation is `h_blocked`, stated as the conformance requirement it is: **a live
task that cannot run is waiting for some task**, i.e. `R` covers every reason not to run.  In
the implementation those reasons are exactly two - a position outside an entry's runnable
prefix, and a `HOLD_QUEUE` lock held by someone else - and a task subject to neither leads
every entry it holds, which satisfies any execution threshold, since a threshold never
exceeds the number of keys held (`min(remaining, MIN_BATCH)`).

It is required of the tasks in `live` only, and that restriction matters: a task that has
finished holds no position and has no outgoing edge, so a version quantified over *every*
task would force `Runnable` to be read as "`CanRun` **or** already finished", which is not
what `CanRun` means.  Restricting it to `live` is exactly what the proof uses.

Note what this does *not* do: it does not model thresholds, and it is not meant to.  A
threshold that could exceed the keys a task holds, or a wait on something that is not a
position or a lock (an external resource, say), would falsify `h_blocked` and this theorem
would cease to apply - silently, since nothing here mentions thresholds.  That is a
conformance obligation on `CanRun`, checked where conformance is checked: by TLC against
the operational model, and by `waitToRunExclusive`'s assertion at runtime.
-/

/-- The rank-minimal element of a non-empty list. -/
theorem exists_rank_min {α : Type v} (key : α → Nat × Nat) :
    ∀ (l : List α), l ≠ [] → ∃ m, m ∈ l ∧ ∀ x ∈ l, ¬ Lex2 (key x) (key m)
  | [], h => absurd rfl h
  | a :: t, _ => by
      cases t with
      | nil =>
          refine ⟨a, List.Mem.head _, ?_⟩
          intro x hx
          cases hx with
          | head => exact lex2_irrefl _
          | tail _ h => cases h
      | cons b t' =>
          obtain ⟨m, hm, hmin⟩ := exists_rank_min key (b :: t') (by intro h; cases h)
          by_cases hab : Lex2 (key a) (key m)
          · refine ⟨a, List.Mem.head _, ?_⟩
            intro x hx
            cases hx with
            | head => exact lex2_irrefl _
            | tail _ hx' => intro hxa; exact hmin x hx' (lex2_trans hxa hab)
          · refine ⟨m, List.Mem.tail _ hm, ?_⟩
            intro x hx
            cases hx with
            | head => exact hab
            | tail _ hx' => exact hmin x hx'

/--
**No stuck set.**  Given a non-empty collection of live tasks whose wait edges satisfy
the same four hypotheses, some live task can run.  So no set of live tasks can be
mutually blocked, for any number of tasks and entries.
-/
theorem exists_runnable (R : Task → Task → Prop) (H : WaitEdges M R)
    (Runnable : Task → Prop)
    (live : List Task) (hne : live ≠ [])
    -- R covers every reason a LIVE task cannot run; see the note above
    (h_blocked : ∀ t ∈ live, ¬ Runnable t → ∃ u, R t u)
    -- a wait edge from a live task points at a live task: a task that has finished holds
    -- no position, so it is neither the source nor the target of an edge
    (h_closed : ∀ a ∈ live, ∀ b, R a b → b ∈ live) :
    ∃ m, m ∈ live ∧ Runnable m := by
  obtain ⟨m, hm, hmin⟩ := exists_rank_min (rank M) live hne
  refine ⟨m, hm, ?_⟩
  rcases Classical.em (Runnable m) with h | h
  · exact h
  · obtain ⟨u, hmu⟩ := h_blocked m hm h
    exact absurd (rank_lt_of_edge M R H hmu) (hmin u (h_closed m hm u hmu))

/-! ## 4. Isolation, at any size

`Inv_Isolation` says that on the processing order of any one cache entry, no task from
outside an atomic unit appears between two members of it - where a *unit* is a task
together with the consequences it submits ATOMICALLY, transitively (`UnitOf` in
`AccordExec.tla`, O14).  It is the property with the weakest model-checking evidence,
since exhibiting a violation needs three tasks and a submission chain, so it is worth
having at any size.

Unlike acyclicity this is a property of a *run*, not of a state.  It does not need the
dynamics, though: it needs six local rules about the schedule, each of which is an
invariant the spec already names.  The rules are stated over the same `rank` as the
acyclicity argument, so the fifo / ordered / bag structure is shared rather than
re-encoded.

Everything is per entry; `Task` here means "a task that holds or held a position on the
entry in question".  A task processes a given entry at most once, since a key belongs to
exactly one batch of one round.
-/

/-- One entry's history: who ran on it, when they took their position, when they ran, and
which atomic unit each belongs to.  Steps are abstract times, so `<` is "before". -/
structure History (Task : Type u) where
  /-- this task processed the entry -/
  ran : Task → Prop
  /-- the step at which it did -/
  ranAt : Task → Nat
  /-- the step at which it took its queue position on this entry -/
  claimed : Task → Nat
  /-- which atomic unit it belongs to (`UnitOf`) -/
  unit : Task → Nat

/--
The scheduling discipline the implementation maintains.  Each field is an invariant of
`AccordExec.tla`, and each has a control profile that breaks it, so a reader can check the
correspondence field by field rather than trusting the proof:

* `runs_lead` - Q4: a task processes an entry only while it leads it, so nothing queued
  and not yet run ranks before it.  `probe-bag-interleaves` breaks it.
* `handover` - O13: a unit's next member has claimed its position before its predecessor
  releases.  `ctl-defer-submit` breaks it.
* `later_is_fifo` - O5: within a unit, the member that runs later is a descendant of the
  one that ran first, hence ATOMIC, hence a fifo claim from setup.
* `unit_interval` - O6: a unit's members occupy a contiguous interval of the rank order,
  because they share an inherited `fifoAt` (ties by `createdAt`), and a foreign stamp
  cannot be drawn between two of theirs - stamps are drawn either by a consequence of the
  running task or by a task starting its own run, and only one task runs at a time (A3).
* `fifo_claim_order` - O5: a fifo claim takes every position it will hold at the moment it
  is stamped, so across units the rank order and the claim order agree.
  `ctl-fifo-adopt` breaks it.
* `fifo_rank_inj` - O1/O6: `⟨fifoAt, createdAt⟩` is a strict total order, so distinct fifo
  claims have distinct ranks.  (Bag members deliberately share a rank, which is why this is
  restricted to the fifo region.)
-/
structure Discipline (E : History Task) : Prop where
  runs_lead : ∀ t y, E.ran t → E.ran y → E.claimed y ≤ E.ranAt t → E.ranAt t < E.ranAt y →
                ¬ Lex2 (rank M y) (rank M t)
  handover : ∀ a b, E.unit a = E.unit b → E.ran a → E.ran b → E.ranAt a < E.ranAt b →
                E.claimed b < E.ranAt a
  later_is_fifo : ∀ a b, E.unit a = E.unit b → E.ran a → E.ran b → E.ranAt a < E.ranAt b →
                M.region b = .fifo
  unit_interval : ∀ a b y, E.unit a = E.unit b →
                Lex2 (rank M a) (rank M y) → Lex2 (rank M y) (rank M b) → E.unit y = E.unit a
  fifo_claim_order : ∀ x y, M.region x = .fifo → M.region y = .fifo → E.unit x ≠ E.unit y →
                Lex2 (rank M x) (rank M y) → E.claimed x < E.claimed y
  fifo_rank_inj : ∀ x y, M.region x = .fifo → M.region y = .fifo →
                rank M x = rank M y → x = y

/-- A fifo claim ranks before anything that is not one. -/
private theorem fifo_lt_of_ne {x y : Task} (hx : M.region x = .fifo)
    (hy : M.region y ≠ .fifo) : Lex2 (rank M x) (rank M y) := by
  refine Or.inl ?_
  cases hR : M.region y with
  | fifo => exact absurd hR hy
  | ordered => simp [rank, hx, hR, Region.layer]
  | bag => simp [rank, hx, hR, Region.layer]

/--
**Isolation.**  No task from outside an atomic unit runs on an entry between two distinct
members of that unit, for any number of tasks and entries.
-/
theorem isolated (E : History Task) (D : Discipline M E)
    (a x b : Task) (ha : E.ran a) (hx : E.ran x) (hb : E.ran b)
    (h_ax : E.ranAt a < E.ranAt x) (h_xb : E.ranAt x < E.ranAt b)
    (h_unit : E.unit a = E.unit b) (h_foreign : E.unit x ≠ E.unit a) : False := by
  have h_ab : E.ranAt a < E.ranAt b := Nat.lt_trans h_ax h_xb
  have h_foreign_b : E.unit x ≠ E.unit b := fun h => h_foreign (h.trans h_unit.symm)
  -- O13: b holds its position before a runs, hence before x runs
  have hcb : E.claimed b < E.ranAt a := D.handover a b h_unit ha hb h_ab
  have hbf : M.region b = .fifo := D.later_is_fifo a b h_unit ha hb h_ab
  -- Q4 at x's run: b is queued and has not run, so x does not rank after b
  have hxb : ¬ Lex2 (rank M b) (rank M x) :=
    D.runs_lead x b hx hb (Nat.le_of_lt (Nat.lt_trans hcb h_ax)) h_xb
  -- so x is a fifo claim too, since anything else would rank after b
  have hxf : M.region x = .fifo := by
    rcases Classical.em (M.region x = .fifo) with h | h
    · exact h
    · exact absurd (fifo_lt_of_ne M hbf h) hxb
  -- x and b are distinct fifo claims, so x ranks strictly before b
  have hxb' : Lex2 (rank M x) (rank M b) := by
    rcases lex2_total (rank M x) (rank M b) with h | h | h
    · exact h
    · exact absurd (congrArg E.unit (D.fifo_rank_inj x b hxf hbf h)) h_foreign_b
    · exact absurd h hxb
  rcases Nat.lt_or_ge (E.ranAt a) (E.claimed x) with hlate | hearly
  · -- x claimed its position after a ran, so b claimed before x did; but x ranks before b,
    -- and O5 makes claim order follow rank order across units
    exact Nat.lt_irrefl _ (Nat.lt_trans (Nat.lt_trans hcb hlate)
      (D.fifo_claim_order x b hxf hbf h_foreign_b hxb'))
  · -- x was already queued when a ran, so Q4 at a's run puts a before x; then x lies
    -- strictly inside the unit's rank interval, which O6 forbids
    have hax : ¬ Lex2 (rank M x) (rank M a) := D.runs_lead a x ha hx hearly h_ax
    have haf : M.region a = .fifo := by
      rcases Classical.em (M.region a = .fifo) with hf | hf
      · exact hf
      · exact absurd (fifo_lt_of_ne M hxf hf) hax
    have hax' : Lex2 (rank M a) (rank M x) := by
      rcases lex2_total (rank M a) (rank M x) with h | h | h
      · exact h
      · exact absurd (congrArg E.unit (D.fifo_rank_inj a x haf hxf h).symm) h_foreign
      · exact absurd h hax
    exact h_foreign (D.unit_interval a b x h_unit hax' hxb')

/-! ## 5. Sanity: the hypothesis bundles are satisfiable

All three theorems are of the form "anything satisfying these hypotheses has this
property", and `isolated` concludes `False` outright.  A contradictory hypothesis bundle
would make them all true and all worthless, and that is not visible by inspection - so it
is discharged here, once, with explicit witnesses: `WaitEdges` (`M2`/`R2`),
`exists_runnable`'s extra four (`MN`/`RN`/`RunnableN`/`live = [0,1]`) and `Discipline`
(`MN`/`EN`), each with a non-degeneracy statement beside it.  These are not models of
Accord; they exist only to show the structures are inhabitable in a non-degenerate way.
-/

namespace Sanity

/-- Two fifo tasks with distinct stamps and one wait edge from the later to the earlier. -/
def M2 : Model (Fin 2) :=
  { region := fun _ => .fifo, fifoAt := fun i => i.val + 1, ordAt := fun _ => 0 }

def R2 : Fin 2 → Fin 2 → Prop := fun a b => a.val = 1 ∧ b.val = 0

theorem waitEdges_sat : WaitEdges M2 R2 := by
  constructor <;> intro a b hab <;> simp [M2, R2, Region.layer] at * <;> omega

/-- ...and the edge really is there, so `wait_acyclic` is not vacuous. -/
theorem R2_nonempty : R2 1 0 := by simp [R2]

theorem acyclic_sat : Acyclic R2 := wait_acyclic M2 R2 waitEdges_sat

/-! `exists_runnable` carries four hypotheses the other two theorems do not - `Runnable`,
`h_blocked`, `live`, `h_closed` - so its bundle needs a witness of its own.  Tasks are
naturals, all fifo with distinct stamps (`MN`, below); task 1 waits for task 0 and is the
only task that cannot run. -/

def RN : Nat → Nat → Prop := fun a b => a = 1 ∧ b = 0

/-- "can run now".  Note it is required to cover only the tasks in `live`, so nothing here
has to pretend a finished task is runnable. -/
def RunnableN : Nat → Prop := fun t => t ≠ 1

def MN : Model Nat :=
  { region := fun _ => .fifo, fifoAt := fun n => n + 1, ordAt := fun _ => 0 }

theorem waitEdges_satN : WaitEdges MN RN := by
  constructor <;> intro a b hab <;> simp [MN, RN, Region.layer] at * <;> omega

theorem h_blockedN : ∀ t ∈ [0, 1], ¬ RunnableN t → ∃ u, RN t u := by
  intro t _ h
  have ht : t = 1 := Classical.byContradiction (fun hne => h hne)
  exact ⟨0, ht, rfl⟩

theorem h_closedN : ∀ a ∈ [0, 1], ∀ b, RN a b → b ∈ ([0, 1] : List Nat) := by
  intro a _ b hb
  obtain ⟨_, hb0⟩ := hb
  subst hb0
  simp

theorem runnable_sat : ∃ m, m ∈ ([0, 1] : List Nat) ∧ RunnableN m :=
  exists_runnable MN RN waitEdges_satN RunnableN [0, 1] (by intro h; cases h)
    h_blockedN h_closedN

/-- ...and non-degenerately: there is a real edge, and a task that really cannot run, so
`h_blocked` is not satisfied merely because everything is runnable. -/
theorem runnable_nondegenerate : RN 1 0 ∧ ¬ RunnableN 1 :=
  ⟨⟨rfl, rfl⟩, by simp [RunnableN]⟩

/-- For `Discipline`, tasks are naturals with `unit n = n / 2`, so `{0,1}` is one atomic
unit, `{2,3}` the next, and so on.  All fifo with distinct stamps; run order is task
order.  Note `claimed` must be strictly below the *predecessor's* run step (`handover`),
which is why `ranAt` starts at 5 rather than 0. -/
def EN : History Nat :=
  { ran := fun _ => True, ranAt := fun n => n + 5,
    claimed := fun n => n / 2, unit := fun n => n / 2 }

theorem lexN (x y : Nat) : Lex2 (rank MN x) (rank MN y) ↔ x < y := by
  simp [Lex2, rank, MN, Region.layer]

theorem discipline_sat : Discipline MN EN := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro t y _ _ _ hlt h; rw [lexN] at h; simp [EN] at hlt; omega
  · intro a b hu _ _ hlt; simp [EN] at hu hlt ⊢; omega
  · intro a b _ _ _ _; rfl
  · intro a b y hu h1 h2; rw [lexN] at h1 h2; simp [EN] at hu ⊢; omega
  · intro x y _ _ hu hlex; rw [lexN] at hlex; simp [EN] at hu ⊢; omega
  · intro x y _ _ h; simp [rank, MN, Region.layer] at h; omega

/-- ...and the configuration is non-degenerate: a two-member unit, both of whose members
ran, plus a foreign task that also ran.  So `isolated` is a real constraint on this model
rather than a statement about an empty situation. -/
theorem discipline_nondegenerate :
    EN.unit 0 = EN.unit 1 ∧ EN.ranAt 0 < EN.ranAt 1 ∧ EN.ran 2 ∧ EN.unit 2 ≠ EN.unit 0 := by
  refine ⟨?_, ?_, trivial, ?_⟩ <;> simp [EN]

theorem no_interleave (a x b : Nat)
    (h1 : EN.ranAt a < EN.ranAt x) (h2 : EN.ranAt x < EN.ranAt b)
    (hu : EN.unit a = EN.unit b) (hf : EN.unit x ≠ EN.unit a) : False :=
  isolated MN EN discipline_sat a x b trivial trivial trivial h1 h2 hu hf

end Sanity

/-!
## 6. What must be true of the implementation

Nothing here is proved about the implementation; three theorems are proved about anything
satisfying the hypotheses below.  So the whole review burden is this table, and a reader can
take it field by field without reading a proof.  Each row names the invariant in
`INVARIANTS.md`, and what would catch its violation - a control profile in `matrix.py`
(which must break the property, so the row is not vacuous), or an assertion in the code.

**The two theorems are discharged in opposite directions, and it matters.**

For acyclicity the correspondence is exact: `waitEdges_iff_rankOK` proves that the four
`WaitEdges` fields are *equivalent* to the single `RankOK` invariant TLC checks, so TLC
does establish the hypotheses - all four at once, not one at a time.  A reader chasing an
individual row should read it as "this is the case of `RankOK` that would break".

For isolation it is the other way round: TLC checks `Inv_Isolation`, the *conclusion*, and
none of the six `Discipline` fields has a counterpart invariant in `AccordExec.tla`.  Two
of them are not even expressible there - `handover` and `fifo_claim_order` quantify over
`History.claimed`, and the operational model has no claim-time variable, only the run order
in `plog`.  So for isolation the Lean theorem and the model check are two independent
arguments for the same property rather than two halves of one, and the "violation caught
by" column below names what would catch a violation *of the property*, not of the
hypothesis.  Closing that gap needs a `claimedAt` variable in the model; until then the
rows marked (argued) are exactly that.

| hypothesis | is | violation caught by |
|---|---|---|
| `Model.region : Task -> Region` | O3, the region is a function of the task, not of (task, entry) | `isUnsequenced(entry)`'s `Invariants.require`; `ctl-unseq-incr-txn` |
| `h_layer` | Q4/O2, a wait edge never points up a layer | `RankOK` (= this, by `waitEdges_iff_rankOK`); `probe-bag-interleaves` |
| `h_fifo` | Q5/O6 + O8, fifo order is by `fifoAt`, and a lock holder is the least-stamped claim | `RankOK`; `Inv_LockLeads`; `addFifo`'s `Invariants.expect`; `ctl-no-upgrade` |
| `h_ord` | Q1/O1, the sorted region is in `compare()` order | `RankOK`; `validate()`'s Q1 check |
| `h_no_bag` | Q3, bag members do not wait for one another | `RankOK` (bag members share key 0, so a bag-bag edge shows up as a violation) |
| `h_blocked` | `CanRun`: a position outside a runnable prefix and a foreign `HOLD_QUEUE` lock are the *only* reasons a LIVE task cannot run (finished tasks are excluded, which is why `Runnable` need not be stretched to cover them) | `waitToRunExclusive`'s paranoid check; TLC against `CanRun` |
| `h_closed` | R6, a finished or failed task holds no position | `requireNotTerminal`; `postRunExclusive` |
| `runs_lead` | Q4 + the lock-time re-check: a task processes an entry only while leading it | (argued) `REQUIRE_RUNNABLE`; `probe-bag-interleaves` breaks `Inv_Isolation` |
| `handover` | O13, a unit's next member claims before its predecessor releases | (argued, not expressible in the model) `ctl-defer-submit` breaks `Inv_Isolation` |
| `later_is_fifo` | O5, an ATOMIC member is a fifo claim from setup, and the later runner in a unit is a descendant | (argued) `preSetup`'s `setCacheQueuedFifoExclusive` |
| `unit_interval` | O6 + A3, a unit occupies a contiguous rank interval: members share an inherited stamp, and no foreign stamp can be drawn between two of theirs because only one task runs at a time | (argued) `submitExclusiveMayThrow`'s paranoid `fifoAt` check |
| `fifo_claim_order` | O5, a fifo claim takes every position it will hold when it is stamped | (argued, not expressible in the model) `ctl-fifo-adopt` breaks `Inv_Isolation`; `adoptCachedKeyExclusive`'s `require` |
| `fifo_rank_inj` | O1/O6, `createdAt` is unique per store, so distinct fifo claims have distinct ranks | (argued) `SafeTask`'s `createdAt` contract |

That every bundle is *satisfiable*, and non-degenerately so, is §5 - without which
`isolated` in particular could be true only because its hypotheses cannot all hold.

Two modelling assumptions are not hypotheses because they are built into the shape of the
statements, and are worth stating explicitly:

* **one run per task per entry** (`History.ranAt` is a function).  A key belongs to exactly
  one batch of one round - `prepareExclusive` removes what it locks - so a task processes a
  given entry once.  If that changed, `isolated` would need a per-run index.
* **the histories are per entry** (`isolated` is about one `History`).  That is not a
  restriction: `Inv_Isolation` is a per-entry property, and the argument never relates two
  entries.

### Where this leaves the two headline risks

**`h_layer` is where the cost of the scheme sits.**  `.bag` is the top layer and its members
share key `0`, so no rank of this shape can accommodate a bag member that is waited on.
UNSEQUENCED work was *intended* to interleave with sequenced work, which would produce
exactly such an edge; Q4 forbids it instead.  `PBagInterleaves` shows what the intended
semantics would cost - `RankOK` always, and a real cycle on some topologies.  Recovering it
needs a rank for an interleaving bag member that is pair-determined and orders *below* every
sequenced task that may wait for it, which is what "unsequenced" denies it.

**`Model.region` is where a per-entry region would break everything.**  If the declared
`ExecutionSequence` were not applied, or `isUnsequenced(entry)`'s assertion were relaxed, an
unsequenced INCR task with a txnId would be ORDERED on its command entries and BAGGED on its
keys.  The region would no longer be a function of the task, the type above could not be
written, and two such tasks can wait for each other in opposite orders on two entries.
-/

end AccordAcyclic
