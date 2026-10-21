---- MODULE DistributedSpec ----
\* Distributed system specification template
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Nodes, NULL, MaxMsgs

VARIABLES
  node_state,    \* [Nodes -> NodeStateSet]
  msgs,          \* Messages in flight
  history        \* Auxiliary for properties

vars == <<node_state, msgs, history>>

\* ---- Message Types ----
RequestMsg  == [type: {"request"},  from: Nodes, to: Nodes, data: Nat]
ResponseMsg == [type: {"response"}, from: Nodes, to: Nodes, data: Nat]
AllMsgTypes == RequestMsg \union ResponseMsg

\* ---- Helpers ----
Send(m)    == msgs' = msgs \union {m}
Discard(m) == msgs' = msgs \ {m}
NoMsgChange == UNCHANGED msgs

\* ---- Initial State ----
Init ==
  /\ node_state = [n \in Nodes |-> "idle"]
  /\ msgs = {}
  /\ history = <<>>

\* ---- Actions ----
SendRequest(n) ==
  /\ node_state[n] = "idle"
  /\ \E dest \in Nodes \ {n}:
       /\ node_state' = [node_state EXCEPT ![n] = "waiting"]
       /\ Send([type |-> "request", from |-> n, to |-> dest, data |-> 0])
  /\ UNCHANGED history

HandleRequest(n) ==
  /\ \E m \in msgs:
       /\ m.type = "request"
       /\ m.to = n
       /\ node_state[n] = "idle"
       /\ node_state' = [node_state EXCEPT ![n] = "idle"]
       /\ msgs' = (msgs \ {m}) \union
            {[type |-> "response", from |-> n, to |-> m.from, data |-> m.data]}
       /\ history' = Append(history, m)

HandleResponse(n) ==
  /\ \E m \in msgs:
       /\ m.type = "response"
       /\ m.to = n
       /\ node_state[n] = "waiting"
       /\ node_state' = [node_state EXCEPT ![n] = "done"]
       /\ Discard(m)
       /\ history' = Append(history, m)

\* ---- Network Failures (optional) ----
DropMessage ==
  /\ msgs # {}
  /\ \E m \in msgs: msgs' = msgs \ {m}
  /\ UNCHANGED <<node_state, history>>

\* ---- Next State ----
Next ==
  \/ \E n \in Nodes:
       \/ SendRequest(n)
       \/ HandleRequest(n)
       \/ HandleResponse(n)
  \* \/ DropMessage    \* uncomment to model lossy network

\* ---- Properties ----
TypeInvariant ==
  /\ node_state \in [Nodes -> {"idle", "waiting", "done"}]
  /\ msgs \subseteq AllMsgTypes

\* No response without a request
Safety ==
  \A m \in msgs:
    m.type = "response" =>
      \E h \in Range(history): h.type = "request" /\ h.from = m.to

\* Every request eventually gets a response
Liveness ==
  \A n \in Nodes:
    node_state[n] = "waiting" ~> node_state[n] = "done"

\* ---- Bounding ----
StateConstraint ==
  /\ Cardinality(msgs) <= MaxMsgs
  /\ Len(history) <= MaxMsgs * 2

\* ---- Specification ----
Spec == Init /\ [][Next]_vars
FairSpec == Spec /\ \A n \in Nodes: WF_vars(Next)

\* ---- Helpers ----
Range(s) == {s[i] : i \in 1..Len(s)}
====
