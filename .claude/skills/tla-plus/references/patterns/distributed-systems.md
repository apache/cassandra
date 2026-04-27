# Distributed Systems Patterns

## Message Passing with Queues

The most common pattern for distributed systems. Model each node as a process, communication as queue operations.

### FIFO Queue (Mutable)

```tla
variables
  queue = <<>>;   \* sequence of messages

\* Send
queue := Append(queue, msg);

\* Receive (destructive)
await queue # <<>>;
msg := Head(queue);
queue := Tail(queue);
```

### Per-Process Queues

```tla
variables
  queues = [p \in Processes |-> <<>>];   \* each process has inbox

\* Send to one process
queues[dest] := Append(queues[dest], msg);

\* Broadcast to all
queues := [p \in Processes |-> Append(queues[p], msg)];

\* Send to subset (models unreliable delivery)
with recipients \in SUBSET Processes do
  queues := [p \in recipients |-> Append(queues[p], msg)] @@ queues;
end with;

\* Receive from own queue
await queues[self] # <<>>;
msg := Head(queues[self]);
queues[self] := Tail(queues[self]);
```

### Message Types

```tla
\* Typed messages with discriminator
RequestMsg  == [type: {"request"},  from: Nodes, data: DataType, id: 1..MaxId]
ResponseMsg == [type: {"response"}, from: Nodes, data: DataType, id: 1..MaxId]
MessageType == RequestMsg \union ResponseMsg
```

## Network Failures

### Message Loss (At-Most-Once Delivery)

```tla
\* Instead of deterministic send, nondeterministically drop
either
  queues[dest] := Append(queues[dest], msg);   \* delivered
or
  skip;                                         \* lost
end either;
```

### Message Reordering

```tla
\* Use a SET instead of a sequence for the channel
variables network = [p \in Processes |-> {}];

\* Send
network[dest] := network[dest] \union {msg};

\* Receive any message (unordered)
with m \in network[self] do
  msg := m;
  network[self] := network[self] \ {m};
end with;
```

### Node Crashes

```tla
\* Model with nondeterministic crash action
process node \in Nodes
variables alive = TRUE;
begin
  Run:
    while alive do
      either
        \* normal operation
      or
        alive := FALSE;     \* crash
      end either;
    end while;
end process;
```

## Consensus Patterns

### Two-Phase Commit

```tla
---- MODULE TwoPhaseCommit ----
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Coordinators, Participants, NULL
ASSUME Cardinality(Coordinators) = 1

(*--algorithm two_phase_commit
variables
  coord_state = [c \in Coordinators |-> "init"];
  part_state  = [p \in Participants |-> "working"];
  prepared    = [c \in Coordinators |-> {}];
  msgs        = {};

define
  TypeInvariant ==
    /\ coord_state \in [Coordinators -> {"init", "waiting", "committed", "aborted"}]
    /\ part_state  \in [Participants -> {"working", "prepared", "committed", "aborted"}]

  \* Safety: no participant commits while another aborts
  Consistency ==
    \A p1, p2 \in Participants:
      ~(part_state[p1] = "committed" /\ part_state[p2] = "aborted")
end define;

process coordinator \in Coordinators
begin
  C_Prepare:
    coord_state[self] := "waiting";
    msgs := msgs \union {[type |-> "prepare", from |-> self]};

  C_Decide:
    await prepared[self] = Participants \/ \E p \in Participants: part_state[p] = "aborted";
    if prepared[self] = Participants then
      coord_state[self] := "committed";
      msgs := msgs \union {[type |-> "commit", from |-> self]};
    else
      coord_state[self] := "aborted";
      msgs := msgs \union {[type |-> "abort", from |-> self]};
    end if;
end process;

process participant \in Participants
begin
  P_Prepare:
    await \E m \in msgs: m.type = "prepare";
    either
      part_state[self] := "prepared";
      with c \in Coordinators do
        prepared[c] := prepared[c] \union {self};
      end with;
    or
      part_state[self] := "aborted";
    end either;

  P_Decide:
    either
      await \E m \in msgs: m.type = "commit";
      part_state[self] := "committed";
    or
      await \E m \in msgs: m.type = "abort";
      part_state[self] := "aborted";
    end either;
end process;
end algorithm; *)
====
```

### Leader Election (Bully Algorithm Sketch)

```tla
variables
  leader = NULL;
  election_in_progress = FALSE;
  candidates = {};

define
  \* At most one leader at any time
  AtMostOneLeader ==
    \A n1, n2 \in Nodes:
      (leader = n1 /\ leader = n2) => n1 = n2

  \* Eventually a leader is elected (liveness)
  EventuallyLeader == <>(leader # NULL)
end define;
```

## State Machine Pattern

Model systems as explicit state machines with transition functions.

```tla
VARIABLE state

Trans(from, to) ==
  /\ state = from
  /\ state' = to

Init == state = "idle"

Next ==
  \/ Trans("idle", "connecting")
  \/ Trans("connecting", "connected")
  \/ Trans("connecting", "failed")
  \/ Trans("connected", "disconnecting")
  \/ Trans("disconnecting", "idle")
  \/ Trans("failed", "idle")

Spec == Init /\ [][Next]_state
```

## Clocks and Logical Time

### Lamport Clocks

```tla
variables
  clock = [p \in Processes |-> 0];

\* Local event
clock[self] := clock[self] + 1;

\* Send: increment, attach clock
clock[self] := clock[self] + 1;
msg := [data |-> payload, ts |-> clock[self]];

\* Receive: merge clocks
clock[self] := IF msg.ts > clock[self]
               THEN msg.ts + 1
               ELSE clock[self] + 1;
```

## Quorum Systems

```tla
CONSTANT Nodes

Quorum == {Q \in SUBSET Nodes : Cardinality(Q) * 2 > Cardinality(Nodes)}

\* Any two quorums overlap
QuorumOverlap == \A Q1, Q2 \in Quorum: Q1 \intersect Q2 # {}
```

## Replication

```tla
variables
  replicas = [n \in Nodes |-> [key \in Keys |-> NULL]];

define
  \* Eventually consistent: if no more writes, all replicas converge
  EventualConsistency ==
    <>[](\A n1, n2 \in Nodes, k \in Keys:
           replicas[n1][k] = replicas[n2][k])

  \* Strong consistency: all replicas always agree
  StrongConsistency ==
    \A n1, n2 \in Nodes, k \in Keys:
      replicas[n1][k] = replicas[n2][k]
end define;
```
