---- MODULE BasicSpec ----
\* Basic TLA+ specification template (pure TLA+, no PlusCal)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS NULL

VARIABLES state, data

vars == <<state, data>>

\* --- Type definitions ---
StateSet == {"init", "running", "done"}
DataSet == 0..10

\* --- Initial state ---
Init ==
  /\ state = "init"
  /\ data = 0

\* --- Actions ---
Start ==
  /\ state = "init"
  /\ state' = "running"
  /\ UNCHANGED data

Step ==
  /\ state = "running"
  /\ data < 10
  /\ data' = data + 1
  /\ UNCHANGED state

Finish ==
  /\ state = "running"
  /\ data = 10
  /\ state' = "done"
  /\ UNCHANGED data

\* --- Next state relation ---
Next == Start \/ Step \/ Finish

\* --- Specification ---
Spec == Init /\ [][Next]_vars

\* --- Properties ---
TypeInvariant ==
  /\ state \in StateSet
  /\ data \in DataSet

Safety == state = "done" => data = 10

Liveness == <>(state = "done")

FairSpec == Spec /\ WF_vars(Next)
====
