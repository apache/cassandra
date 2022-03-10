# Light-weight transactions and Paxos algorithm

Our implementation of light-weight transactions (LWT) is loosely based on the classic Paxos single-decision algorithm.
It contains a range of modifications that make it different from a direct application of a sequence of independent
decisions.

Below we will describe the basic process (used by both V1 and V2 implementations) with the actions that each participant
takes and sketch a proof why the approach is safe, and then talk about various later improvements that do not change the
correctness of the basic scheme.

A key consideration in the design was the fact that we apply LWTs independently to the partitions of a table, and the
necessary additional infrastructure (such as improved failure detection, client-side routing, etc) to efficiently 
utilise a stable leader (i.e. Multi-Paxos). Instead of solving this we prefer to better support independent writes by 
a leaderless scheme, where each coordinator attempts to direct its operations to completion. On congestion this runs 
the chance of repeated clashes between coordinators attempting to perform writes to the same partition, in which case 
we use randomized exponential backoff to achieve progress.

The descriptions and proofs below assume no non-LWT modifications to the partition. Any other write (regardless of the
requested consistency level) may reach a minority of replicas and leave the partition in an inconsistent state where the
following operations can go different ways depending on which set of replicas respond to a quorum read. In particular,
if two compare-and-set (CAS) operations with the same condition are executed with no intervening operations, such a
state would make it possible for the first to fail and the second to succeed.

## Basic scheme

For each partition with LWT modification, we maintain the following registers:

- The current `promised` ballot number.
- The latest `accepted` proposal together with its ballot number and a `committed` flag.

We define an ordering on the latter by ballot number and committed flag, where `committed` true is interpreted as
greater on equal ballot numbers. Two proposals are equal when their ballot numbers and committed flag match.

The basic scheme includes the following stages:

1. A coordinator selects a fresh ballot number (based on time and made unique by including a source node identifier).
2. The coordinator sends a `prepare` message to all replicas responsible for the targeted partition with the given
   ballot number and a request to read the data required by the operation.
3. By accepting the message, a replica declares that it will not accept any `prepare` or `propose` message with smaller
   ballot number:
    1. If the replica's `promised` register is greater than the number included in the message, the replica rejects it,
       sending back its current `promised` number in a `rejected` message.
    2. Otherwise, the replica stores the ballot number from the message in its `promised` register and replies with a
       `promise` message which includes the contents of the current `accepted` register together with the requested data
       from the database.
4. On receipt of `promise`s from a quorum of nodes, the coordinator compiles the "most recently accepted" (`MRA`) value as
   the greatest among the accepted values in the promises and then:
    1. If the `MRA` is not null and its committed flag is not true, there is an in-progress Paxos session that needs to be
       completed and committed. Controller reproposes the value with the new ballot (i.e. follows the steps from (iv)
       below with a proposed value equal to the `MRA`'s) and then restarts the process.
    2. If the `MRA` is not null, its committed flag is true and it is not a match for the `accepted` value of a quorum of
       promises, controller sends a `commit` message to all replicas whose value did not match and awaits responses
       until they form a quorum of replicas with a matching `accepted` value.
    3. The coordinator then creates a proposal, a partition update which is the result of applying the operation, using
       a read result obtained by resolving the quorum of read responses; the partition update has a timestamp that
       corresponds to the proposal's ballot number, and is empty if the operation was a read or the CAS failed.
    4. It then sends the proposal as a `propose` message with the value and the current ballot number to all replicas.
5. A replica accepts the proposal if it has not promised that it will not do so:
    1. If its `promised` ballot number is not higher than the proposal's, it sets its `accepted` register to the
       proposal with its ballot number and a false `committed` flag, updates its `promised` register to the proposal's
       ballot and sends back an `accepted` message.
    2. Otherwise it rejects the proposal by sending the current `promised` number in a `rejected` message.
6. On receipt of `accepted` messages from a quorum of nodes, the Paxos session has reached a decision. The coordinator
   completes it by sending `commit` messages to all replicas, attaching the proposal value with its ballot number.
    1. It can return completion without waiting for receipt of any commit messages.
7. A replica accepts a commit unconditionally, by applying the attached partition update. If the commit's ballot number
   is greater than the replica's `accepted` ballot number, it sets its `accepted` register to the message's value and
   ballot with true `committed` flag, and the `promised` register to its ballot number.
8. If at any stage of the process that requires a quorum the quorum is not reached, the coordinator backs off and then
   restarts the process from the beginning using a fresh ballot that is greater than the ballot contained in any
   `rejected` message received.

We can't directly map multi-instance classic Paxos onto this, but we can follow its correctness proofs to ensure the
following propositions:

1. Once a value (possibly empty) has been decided (i.e. accepted by a majority), no earlier proposal can be reproposed.
2. Once a value has been decided, in any further round (i.e. action taken by any proposer) it will either be accepted
   and/or committed, or it will have been committed in an earlier round and will be witnessed by at least one member of
   the quorum.

Suppose the value V was decided in the round with ballot number E0.

Proposition 1 is true because new proposals can only be made after a successful promise round with ballot number E > E0.
To be successful, it needs a quorum which must intersect in at least one replica with E0's decision quorum. Since that
replica accepted that proposal, it cannot have made a promise on E before that and must thus return an accepted value
whose ballot number is at least E0. This is true because both `propose` and `commit` actions can only replace the
`accepted` value with one with a higher or equal ballot number.

Proposition 2 can be proven true by induction on the following invariant: for any successful ballot number E >= E0,
either:

1. For all quorums of replicas, commit V with some ballot number G < E, G >= E0 was witnessed by some replica in the
   quorum.
2. For all quorums of replicas, the `accepted` value with the highest ballot number among the replicas in the quorum is
   V with some ballot number G where G <= E, G >= E0.

For round E == E0 the invariant is true because all quorums contain a replica that accepted V with ballot E0, and E0 is
the newest ballot number.

Suppose the invariant is true for some round E and examine the behaviour of the algorithm on F = succ(E).

- If 1. was true for E, it remains true for F.
- Otherwise 2. was true for E and:
    - If the promise pass did not reach a quorum, no accepted values change and hence 2. is still true.
    - If the promise reached a quorum, the collected `MRA` is V with ballot G.
        - If the `MRA`'s committed flag is not true, the value V is reproposed with ballot F.
            - If the proposal reaches no node, no accepted values change and hence 2. is still true.
            - If the proposal reaches a minority of nodes, any quorum that includes one of these nodes will have V with
              ballot F as their highest `accepted` value. All other quorums will not have changed and still have V as their
              highest accepted value with an earlier ballot >= E0. In any case, 2. is still true.
            - If the proposal reaches a majority of nodes, all quorums have V with ballot F as the highest `accepted` value
              and thus satisfy 2.
            - Any `commit` message that is issued after a majority can only change the accepted value's `committed` flag --
              all quorums will still satisfy 2.
        - If the `MRA`'s committed flag is true but it is not matched in all responses, a `commit` with this `MRA` is sent to
          all replicas. By the reasoning above, 2. is still true regardless how many of them (if any) are received and
          processed.
        - If the committed `MRA` matches in all responses (initially or because of commits issued in the last step), then 1.
          is true for G <= E < F regardless of any further action taken by the coordinator.

Proposition 1 ensures that we can only commit one value in any concurrent operation. Proposition 2 means that any
accepted proposal must have started its promise after the previous value was committed to at least one replica in any
quorum, and hence must be able to see its effects in its read responses. This is also true for every other value that
was accepted in any previous ballot.

Note that each commit may modify separate parts of the partition or overwrite previous values. Each of these updates may
be witnessed by a different replica, but they must all be witnessed in the responses the coordinator receives prior to
making a proposal or completing a read. By virtue of having their timestamps reflect ballot order, the read resolution
process can correctly restore the combined state.

As writes are only done in the `commit` stage after a value has been accepted, no undecided writes can be reflected in
read responses.

## Insubstantial differences with the actual code

Instead of using a unique node identifier as part of the ballot number, we generate a 64-bit random integer. This has an
extremely low chance of collision that can be assumed to be immaterial.

Instead of storing an `accepted` proposal with a committed flag, for historical reasons the actual implementation
separately stores the latest `accepted` and `committed` values. The coordinator computes most recent values for both
after receiving promises, and acts on the higher of the two. In other words, instead of checking the `committed` flag on
the most recently accepted, it checks if whether the `committed` value has a higher ballot than the `accepted` one.

When accepting a proposal (which is conditional on having no newer promise or accepted/committed proposal), it stores it
in the `accepted` register. When accepting a commit (which is unconditional), it replaces the `commit` register only if
the commit has a newer ballot. It will clear the `accepted` value to null if that value does not have a newer ballot.

Additionally, the `promised` value is not adjusted with accepted proposals and commits. Instead, whenever the code
decides whether to promise or accept, it collects the maximum of the promised, accepted and committed ballots.

Version 2 of the Paxos implementation performs reads as described here, by combining them with the `prepare`/`promise`
messages. Version 1 runs quorum reads separately after receiving a promise; the read cannot complete if any write
reaches consensus after that promise and, if successful, it will in turn invalidate and proposals that it may fail to
see.

These differences do not materially change the logic, but make correctness a little harder to prove directly.

## Skipping commit for empty proposals (Versions 1 and 2)

Since empty proposals make no modifications to the database state, it is not necessary to commit them.

More precisely, in the basic scheme above we can treat the case of an empty partition update as the most-recently
accepted value in the coordinator's preparation phase like we would treat a null `MRA`, skipping phases i and ii. In other
words, we can change 4(i) to:

4.
    1. If the `MRA` is not null or empty, and its committed flag is not true, there is an in-progress Paxos session that
       needs to be completed and committed. Controller reproposes the value with the new ballot (i.e. follow the steps
       below with a proposed value equal to the `MRA`'s) and then restart the process.

With this modified step Proposition 1 is still true, as is Proposition 2 restricted to committing and witnessing
non-empty proposals. Their combination still ensures that no update made concurrently with any operation (including a
read or a failing CAS) can resurface after that operation is decided, and that any operation will see the results of
applying any previous.

## Skipping proposal for reads (Version 2 only)

To ensure correct ordering of reads and unsuccessful CAS operations, the algorithm above forces invalidation of any
concurrent operation by issuing an empty update. It is possible, however, to recognize if a concurrent operation may
have started at the time a `promise` is given. If no such operation is present, the read may proceed without issuing an
empty update.

To recognize this, the current `promised` value is returned with `promise` messages. During the proposal generation
phase, the coordinator compiles the maximum returned `promised` number and compares it against the `MRA`'s. If higher, a
concurrent operation is in place and all reads must issue an empty update to ensure proper ordering. If not, the empty
update may be skipped.

In other words, step 3(ii) changes to the following:

3.
    2. Otherwise, the replica stores the ballot number from the message in its `promised` register and replies with a
       `promise` message which includes the contents of the current `accepted` and previous `promised` registers together
       with the requested data from the database.

and a new step is inserted before 4(iv) (which becomes 4(v)):

4.
    4. If the proposal is empty (i.e. the operation was a read or the CAS failed), the coordinator checks the maximum of
       the quorum's `promised` values agains the `MRA`'s ballot number. If that maximum isn't higher, the operation
       completes.

Since we do not change issued proposals or make new ones, Proposition 1 is still in force. For Proposition 2 we must
consider the possibility of a no-propose read missing an update with an earlier ballot number that was decided on
concurrently. The difference in the new scheme is that this read will not invalidate an incomplete concurrent write, and
thus an undecided entry could be decided after the read executes. However, to propose a new entry, a coordinator must
first obtain a quorum of promises using a ballot number greater than the last committed or empty value's. Given such a
promise and a read executing with higher ballot, at least one of the reader's quorum replicas must return its ballot
number or higher in the `promised` field (otherwise the read's promise will have executed before the write's and the
preparation would have been rejected). As a result, the coordinator will see a concurrent operation (either in a
non-committed `MRA`, or a `promised` value higher than a committed or empty `MRA`) and will proceed to issue an invalidating
empty proposal.

## Concurrent reads (Version 2 only)

As stated, the above optimization is only useful once per successful proposal, because a read executed in this way does
not complete and will be treated as concurrent with any operation started after it. To improve this, we can use the fact
that reads do not affect other reads, i.e. they are commutative operations and the order of execution of a set of reads
with no concurrent writes is not significant, and separately store and issue read-only and write promises.

More precisely, `prepare` messages are augmented with an `isWrite` flag, and an additional register called
`promisedWrite` is maintained. The latter is updated when a promise is given for a `prepare` message with a true
`isWrite` field, and is returned with all `promise` messages (in addition to `promised` as above). When a promise is
requested with a ballot number lower than the current `promised` but higher than `promisedWrite`, the replica does not
reject the request, but issues a "read-only promise" (note that this can be a normal `promise` message, recognized by
`promised` being greater than the coordinator-issued ballot number), which cannot be used for making any proposal.

The condition for making a no-proposal read is that the maximum returned `promisedWrite` number is not higher than the
`MRA`'s (i.e. concurrent reads are permitted, but not concurrent writes). Provided that this is the case, the coordinator
can use a read-only promise to execute no-proposal reads and failing CAS's. If they are not, it must restart the
process, treating the read-only promises as rejections.

Steps 2, 3, 4 and 8 are changed to accommodate this. The modified algorithm becomes:

1. A coordinator selects a fresh ballot number (based on time and made unique by including a source node identifier).
2. The coordinator sends a `prepare` message to all replicas responsible for the targeted partition with the given
   ballot number, `isWrite` set to true if the operation is a CAS and false if it is a read, and a request to read the
   data required by the operation.
3. By accepting the message, a replica declares that it will not accept any `propose` message or issue write promises
   with smaller ballot number:
    1. If the replica's `promisedWrite` register is greater than the number included in the message, the replica rejects
       it, sending back its current `promised` number in a `rejected` message.
    2. A `read-only` flag is initialized to false.
    3. If the `promised` register contains a lower value than the one supplied by the message, the `promised` register
       is updated. Otherwise, the `read-only` flag is set to true.
    4. If the message's `isWrite` flag is true and `read-only` is still false, the `promisedWrite` register is updated
       to the passed ballot number. Otherwise, `read-only` is set to true.
    5. The replica replies with a `promise` message which includes the `read-only` flag, the contents of the current
       `accepted` and previous `promised` and `promisedWrite` registers together with the requested data from the
       database.
4. On receipt of `promise`s from a quorum of nodes, the coordinator compiles the "most recently accepted" (`MRA`) value as
   the greatest among the accepted values in the promises and then:
    1. If the `MRA` is not null or empty, and its committed flag is not true, there is an in-progress Paxos session that
       needs to be completed and committed. The coordinator prepares a reproposal of the value with the new ballot,
       continuing with step (v) below, and then restarts the process.
    2. If the `MRA` is not null, its committed flag is true and it is not a match for the `accepted` value of a quorum of
       promises, controller sends a `commit` message to all replicas whose value did not match and awaits responses
       until they form a quorum of replicas with a matching `accepted` value.
    3. The coordinator then creates a proposal, a partition update which is the result of applying the operation, using
       a read result obtained by resolving the quorum of read responses; the partition update is empty if the operation
       was a read or the CAS failed.
    4. If the proposal is empty (i.e. the operation was a read or the CAS failed), the coordinator checks the maximum of
       the quorum's `promisedWrite` values agains the `MRA`'s ballot number. If that maximum isn't higher, the operation
       completes.
    5. If there was no quorum of promises with false `read-only` flag, the coordinator restarts the process (step 8).
    6. Otherwise, it sends the proposal as a `propose` message with the value and the current ballot number to all
       replicas.
5. A replica accepts the proposal if it has not promised that it will not do so:
    1. If its `promised` ballot number is not higher than the proposal's, it sets its `accepted` register to the
       proposal with its ballot number and a false `committed` flag, updates its `promised` register to the proposal's
       ballot and sends back an `accepted` message.
    2. Otherwise, it rejects the proposal by sending the current `promised` number in a `rejected` message.
6. On receipt of `accepted` messages from a quorum of nodes, the Paxos session has reached a decision. The coordinator
   completes it by sending `commit` messages to all replicas, attaching the proposal value and its ballot number.
    1. It can return completion without waiting for receipt of any commit messages.
7. A replica accepts a commit unconditionally, by applying the attached partition update. If the commit's ballot number
   is greater than the replica's `accepted` ballot number, it sets its `accepted` register to the message's value and
   ballot with true `committed` flag, and the `promised` register to its ballot number.
8. If at any stage of the process that requires a quorum the quorum is not reached, the coordinator backs off and then
   restarts the process from the beginning using a fresh ballot that is greater than the `promised` ballot contained in
   any `rejected` and `promise` message received.

With respect to any operation that issues a proposal, this algorithm fully matches the earlier version. For operations
that do not (including all operations executing with a read-only promise), it allows for multiple to execute
concurrently as long as no write promise quorum has been reached after the last commit. The reasoning of the previous
paragraph is still valid and proves that no older proposal can be agreed on after a no-proposal read.

## Paxos system table expiration (Version 1 only)

The Paxos state registers used by the algorithm are persisted in the Paxos system table. For every partition with LWTs,
this table will contain an entry specifying the current values of all registers (promised, promisedWrite, accepted,
committed). Because this information is per-partition, there is a high chance that this table will quickly become very
large if LWTs are used with many independent partitions.

To make sure the overhead of the Paxos system table remains limited, Version 1 of the Cassandra Paxos implementation
specifies a time-to-live (TTL) for all writes. That is, after a certain period of time with no LWT to a partition, the
replica will forget the partition's Paxos state.

If this data expires, any in-progress operations may fail to be brought to completion. With the algorithm as described
above, one of the effects of this is that some writes that are reported complete may fail to ever be committed on a
majority of replicas, or even on any replica (if e.g. connection with the replicas is lost before commits are sent, and
the TTL expires before any new LWT operation on the permition is initiated).

To avoid this problem, Version 1 of the implementation only reports success on writes after the commit stage has reached
a requested consistency level. This solves the problem of reporting success, but only guarantees LWT writes to behave
like non-LWT ones: a write may still reach a minority of nodes and leave the partition in an inconsistent state, which
permits linearity guarantees to be violated.

## Paxos repair (Version 2 only)

In the second version of the implementation the Paxos system table does not expire. Instead, clearing up state is
performed by a "Paxos repair" process which actively processes unfinished Paxos sessions and only deletes state that is
known to have been brought to completion (i.e. successful majority commit).

The repair process starts with taking a snapshot of the uncommitted operations in the Paxos system table. It then takes
their ballots' maximum, which is then distributed to all nodes to be used as a lower bound on all promises, i.e.
replicas stop accepting messages with earlier ballots. The process then proceeds to perform the equivalent of an empty
read on all partitions in the snapshot. Upon completion, it can guarantee that all proposals with a ballot lower than
the bound have been completed, i.e. either accepted and committed or superseded in a majority of nodes.

What this means is that no LWT with earlier ballot can be incomplete, and thus no longer need any earlier state in the
Paxos system table. The actual data deletion happens on compaction, where we drop all data that has lower ballots than
what we know to have been repaired.

## Correctness in the presence of range movements (Version 2 only)

The crucial requirements for any Paxos-like scheme to work is to only make progress when we are guaranteed that all
earlier decision points have at least one representative among the replicas that reply to a message (in other words,
that all quorums intersect). When the node set is fixed this is achieved by requesting that a quorum contains more than
half the replicas for the given partition.

Range movements (i.e. joining or leaving nodes), however, can change the set of replicas that are responsible for a
partition. In the exteme case, after multiple range movements it is possible to have a completely different set of
replicas responsible for the partition (i.e. no quorum can exist that contains a replica for all earlier transactions).
To deal with this problem, we must ensure that:

- While operations in an incomplete state are ongoing, a quorum is formed in such a way that it contains a majority for
  the current replica set _as well as_ for the replica set before the operation was started (as well as any intermediate
  set, if it possible to perform multiple range movements in parallel).
- By the time we transition fully to a new set of replicas responsible for a partition, we have completed moving all
  committed mutations from any source replica to its replacement.

The Paxos repair process above gives us a way to complete ongoing operations and a point in time when we can assume that
all earlier LWT operations are complete. In combination with streaming, which moves all committed data to the new
replica, this means that from the point when both complete forward we can safely use the new replica in quorums in place
of the source.