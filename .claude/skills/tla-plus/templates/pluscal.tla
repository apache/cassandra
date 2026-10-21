---- MODULE PlusCalSpec ----
\* PlusCal specification template
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS NumWorkers, NULL

Workers == 1..NumWorkers

(*--algorithm spec_name
variables
  shared_state = 0;
  queue = <<>>;

define
  \* ---- Type Invariant ----
  TypeInvariant ==
    /\ shared_state \in 0..100
    /\ queue \in Seq(1..NumWorkers)

  \* ---- Safety ----
  AllDone == \A w \in Workers: pc[w] = "Done"
  Correct == AllDone => shared_state = NumWorkers

  \* ---- Helpers ----
  QueueNotFull == Len(queue) < 10
end define;

\* ---- Macros ----
macro enqueue(q, val) begin
  q := Append(q, val);
end macro;

macro dequeue(q, var) begin
  await q # <<>>;
  var := Head(q);
  q := Tail(q);
end macro;

\* ---- Processes ----
fair process worker \in Workers
variables local = 0;
begin
  Start:
    local := shared_state;
  Work:
    shared_state := local + 1;
end process;

end algorithm; *)

\* BEGIN TRANSLATION (chksum(pcal) = "..." /\ chksum(tla) = "...")
\* END TRANSLATION

====
