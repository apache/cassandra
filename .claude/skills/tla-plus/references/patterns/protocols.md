# Protocol Specification Patterns

## Protocol Structure Template

A protocol spec typically has:
1. **State variables** for each participant
2. **Message types** as sets of records
3. **Actions** for each protocol step (one per label/action)
4. **Invariants** for safety properties
5. **Liveness** for progress guarantees

```tla
---- MODULE ProtocolName ----
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Nodes, NULL, MaxMsgs

VARIABLES
  node_state,     \* [Nodes -> StateSet]
  msgs,           \* set or sequence of messages
  history         \* auxiliary variable for properties

vars == <<node_state, msgs, history>>

\* --- Message Types ---
MsgType == [type: {"req", "ack", "nack"}, from: Nodes, to: Nodes, data: DataType]

\* --- Helpers ---
Send(m) == msgs' = msgs \union {m}
Discard(m) == msgs' = msgs \ {m}

\* --- Actions ---
Init ==
  /\ node_state = [n \in Nodes |-> "idle"]
  /\ msgs = {}
  /\ history = <<>>

SendRequest(n) ==
  /\ node_state[n] = "idle"
  /\ node_state' = [node_state EXCEPT ![n] = "waiting"]
  /\ \E dest \in Nodes \ {n}:
       Send([type |-> "req", from |-> n, to |-> dest, data |-> "payload"])
  /\ UNCHANGED history

HandleRequest(n) ==
  /\ \E m \in msgs:
       /\ m.type = "req" /\ m.to = n
       /\ node_state' = [node_state EXCEPT ![n] = "processing"]
       /\ Send([type |-> "ack", from |-> n, to |-> m.from, data |-> m.data])
       /\ Discard(m)     \* careful: updating msgs twice — may need to combine
  /\ UNCHANGED history

Next == \E n \in Nodes: SendRequest(n) \/ HandleRequest(n)

\* --- Properties ---
TypeInvariant ==
  /\ node_state \in [Nodes -> {"idle", "waiting", "processing", "done"}]
  /\ msgs \subseteq MsgType

Safety == \* domain-specific

Liveness == \A n \in Nodes: node_state[n] = "waiting" ~> node_state[n] = "done"

Spec == Init /\ [][Next]_vars /\ \A n \in Nodes: WF_vars(Next)
====
```

## Request-Response Protocol

```tla
\* Client sends request, server responds
\* Invariant: every response corresponds to a prior request

VARIABLES
  client_state,   \* [Clients -> {"idle", "waiting", "done"}]
  server_state,   \* [Servers -> {"idle", "processing"}]
  requests,       \* set of request messages in flight
  responses,      \* set of response messages in flight
  request_log     \* auxiliary: tracks all requests ever sent

SendRequest(c) ==
  /\ client_state[c] = "idle"
  /\ client_state' = [client_state EXCEPT ![c] = "waiting"]
  /\ \E s \in Servers:
       requests' = requests \union {[from |-> c, to |-> s, id |-> next_id]}
  /\ request_log' = request_log \union {[from |-> c, id |-> next_id]}
  /\ UNCHANGED <<server_state, responses>>

HandleRequest(s) ==
  /\ \E req \in requests:
       /\ req.to = s
       /\ server_state' = [server_state EXCEPT ![s] = "processing"]
       /\ responses' = responses \union {[from |-> s, to |-> req.from, id |-> req.id]}
       /\ requests' = requests \ {req}
  /\ UNCHANGED <<client_state, request_log>>

\* Safety: every response has a matching request
ResponseMatchesRequest ==
  \A resp \in responses:
    \E req \in request_log: req.id = resp.id /\ req.from = resp.to
```

## Handshake Protocols

```tla
\* Three-phase handshake (like TCP SYN/SYN-ACK/ACK)
VARIABLE
  conn_state   \* [Nodes \X Nodes -> {"closed", "syn_sent", "syn_rcvd", "established"}]

Init == conn_state = [p \in Nodes \X Nodes |-> "closed"]

SynSend(a, b) ==
  /\ conn_state[<<a,b>>] = "closed"
  /\ conn_state' = [conn_state EXCEPT ![<<a,b>>] = "syn_sent"]
  /\ msgs' = msgs \union {[type |-> "SYN", from |-> a, to |-> b]}

SynAck(b, a) ==
  /\ \E m \in msgs: m.type = "SYN" /\ m.from = a /\ m.to = b
  /\ conn_state[<<b,a>>] = "closed"
  /\ conn_state' = [conn_state EXCEPT ![<<b,a>>] = "syn_rcvd"]
  /\ msgs' = msgs \union {[type |-> "SYN_ACK", from |-> b, to |-> a]}

Ack(a, b) ==
  /\ \E m \in msgs: m.type = "SYN_ACK" /\ m.from = b /\ m.to = a
  /\ conn_state[<<a,b>>] = "syn_sent"
  /\ conn_state' = [conn_state EXCEPT ![<<a,b>>] = "established"]
  /\ msgs' = msgs \union {[type |-> "ACK", from |-> a, to |-> b]}
```

## Token-Based Protocols

```tla
\* Mutual exclusion via token passing
VARIABLES
  token_holder,   \* Nodes \union {NULL}
  in_cs           \* [Nodes -> BOOLEAN]

TokenPass(from, to) ==
  /\ token_holder = from
  /\ ~in_cs[from]
  /\ token_holder' = to
  /\ UNCHANGED in_cs

EnterCS(n) ==
  /\ token_holder = n
  /\ ~in_cs[n]
  /\ in_cs' = [in_cs EXCEPT ![n] = TRUE]
  /\ UNCHANGED token_holder

ExitCS(n) ==
  /\ in_cs[n]
  /\ in_cs' = [in_cs EXCEPT ![n] = FALSE]
  /\ UNCHANGED token_holder

\* Safety: at most one node in CS
MutualExclusion ==
  \A n1, n2 \in Nodes:
    (in_cs[n1] /\ in_cs[n2]) => n1 = n2
```

## Gossip Protocol

```tla
VARIABLES
  knowledge   \* [Nodes -> SUBSET Information]

Init == knowledge = [n \in Nodes |-> {info_n}]   \* each knows its own info

Gossip(a, b) ==
  /\ knowledge' = [knowledge EXCEPT
       ![a] = @ \union knowledge[b],
       ![b] = @ \union knowledge[a]]

\* Convergence: eventually everyone knows everything
AllInfo == UNION {knowledge[n] : n \in Nodes}
EventualConvergence == <>(\A n \in Nodes: knowledge[n] = AllInfo)
```

## Retry with Backoff (Abstract)

```tla
VARIABLES
  attempt,    \* [Agents -> Nat]
  status      \* [Agents -> {"pending", "success", "failed"}]

Retry(a) ==
  /\ status[a] = "failed"
  /\ attempt[a] < MaxRetries
  /\ attempt' = [attempt EXCEPT ![a] = @ + 1]
  /\ status' = [status EXCEPT ![a] = "pending"]

Succeed(a) ==
  /\ status[a] = "pending"
  /\ status' = [status EXCEPT ![a] = "success"]
  /\ UNCHANGED attempt

Fail(a) ==
  /\ status[a] = "pending"
  /\ status' = [status EXCEPT ![a] = "failed"]
  /\ UNCHANGED attempt
```
