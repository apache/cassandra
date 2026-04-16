/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator.satellites;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.CoordinationPlan;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ResponseTracker;
import org.apache.cassandra.locator.SatelliteReplicationStrategy;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.replication.MutationTrackingSyncCoordinator;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanup;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState;
import org.apache.cassandra.tcm.transformations.AdvanceSatelliteFailoverState.TargetState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;


/**
 * Orchestrates the satellite failover process
 * <p>
 * The failover process runs independently for each range in parallel
 * <p>
 * Pipeline for TRANSITION_ACK ranges:
 *   epoch ack → paxos repair → commit(TRANSITION) → MT barrier → commit(NORMAL)
 * <p>
 * Pipeline for TRANSITION ranges (already past gate 1):
 *   MT barrier → commit(NORMAL)
 */
public class SatelliteFailoverProcess
{
    private static final Logger logger = LoggerFactory.getLogger(SatelliteFailoverProcess.class);
    private static final Future<?> SUCCESS = ImmediateFuture.success(null);

    private final List<ProcessRange> ranges;
    private final SharedContext ctx;
    private final MessageDelivery messaging;
    private final Keyspace keyspace;
    private final SatelliteReplicationStrategy strategy;
    private final boolean skipPaxosRepair;
    private final Epoch minEpoch;
    private final String fromDC;

    private static class ProcessRange
    {
        final Range<Token> range;
        final SatelliteFailover.State state;

        public ProcessRange(Range<Token> range, SatelliteFailover.State state)
        {
            this.range = range;
            this.state = state;
        }
    }

    private static List<ProcessRange> getRanges(List<Range<Token>> inputRanges, KeyspaceFailoverState failoverState)
    {
        List<ProcessRange> processRanges = new ArrayList<>();
        for (Range<Token> fr : inputRanges)
        {
            failoverState.forEachRange((range, state) -> {
                for (Range<Token> intersection : range.intersectionWith(fr))
                    processRanges.add(new ProcessRange(intersection, state));
            });
        }

        return processRanges;
    }

    /**
     * {@code inputRanges} must each be contained within a single replica set — {@link #create} guarantees this by
     * intersecting the requested ranges with the local node's ranges. The steps of the process plan against
     * {@code range.right} and apply that plan to the whole range, so a range spanning replica sets would contact
     * the wrong replicas for part of itself.
     */
    public SatelliteFailoverProcess(List<Range<Token>> inputRanges,
                                    SharedContext ctx,
                                    MessageDelivery messaging,
                                    Keyspace keyspace,
                                    SatelliteReplicationStrategy strategy,
                                    KeyspaceFailoverState failoverState)
    {
        this.ranges = getRanges(inputRanges, failoverState);
        this.ctx = ctx;
        this.messaging = messaging;
        this.keyspace = keyspace;
        this.strategy = strategy;
        this.minEpoch = failoverState.processStarted;
        this.fromDC = failoverState.fromDC;
        this.skipPaxosRepair = strategy.isDisabled(fromDC);
    }

    public static SatelliteFailoverProcess create(List<Range<Token>> inputRanges,
                                                  SharedContext ctx,
                                                  MessageDelivery messaging,
                                                  ClusterMetadata metadata,
                                                  String keyspace)
    {
        return create(inputRanges, ctx, messaging, metadata, keyspace, FBUtilities.getBroadcastAddressAndPort());
    }

    public static SatelliteFailoverProcess create(List<Range<Token>> inputRanges,
                                                  SharedContext ctx,
                                                  MessageDelivery messaging,
                                                  ClusterMetadata metadata,
                                                  String keyspace,
                                                  InetAddressAndPort localEndpoint)
    {
        KeyspaceFailoverState failoverState = metadata.satelliteFailoverState.getKeyspaceState(keyspace);
        if (failoverState == null)
            return null;

        KeyspaceMetadata ksm;
        try
        {
            ksm = metadata.schema.getKeyspaceMetadata(keyspace);
        }
        catch (NoSuchElementException e)
        {
            return null;
        }

        if (!(ksm.replicationStrategy instanceof SatelliteReplicationStrategy))
            return null;

        SatelliteReplicationStrategy srs = (SatelliteReplicationStrategy) ksm.replicationStrategy;

        RangesAtEndpoint localRanges = srs.getAddressReplicas(metadata, localEndpoint);
        List<Range<Token>> filteredRanges = new ArrayList<>();

        for (Replica local : localRanges)
        {
            for (Range<Token> ir : inputRanges)
                filteredRanges.addAll(local.range().intersectionWith(ir));
        }

        return new SatelliteFailoverProcess(filteredRanges,
                                            ctx,
                                            messaging,
                                            Keyspace.open(keyspace),
                                            srs,
                                            failoverState);
    }

    private Future<?> start(ProcessRange processRange, ExecutorPlus executor, boolean ackOnly, boolean barrierOnly, boolean force)
    {
        Future<?> future = SUCCESS;
        switch (processRange.state)
        {
            case TRANSITION_ACK:

                if (barrierOnly && !force)
                    return future;

                if (!force)
                {
                    future = queryMinEpoch(processRange.range);
                    if (!skipPaxosRepair)
                        future = future.flatMap(unused -> runPaxosRepair(processRange.range, executor));
                }

                future = future.map(unused -> {
                     commitAdvance(processRange.range, TargetState.TRANSITION);
                     return null;
                 }, executor);

            case TRANSITION:

                if (ackOnly)
                    return future;

                if (!force)
                    future = future.flatMap(unused -> runMTBarrier(processRange.range));

                future = future.map(unused -> {
                    commitAdvance(processRange.range, TargetState.NORMAL);
                    return null;
                }, executor);
                break;

            default:
                throw new IllegalStateException("Unhandled failover state: " + processRange.state);
        }

        return future;
    }

    public Future<?> start(ExecutorPlus executor, boolean ackOnly, boolean barrierOnly, boolean force)
    {
        List<Future<?>> futures = new ArrayList<>();

        for (ProcessRange processRange : ranges)
        {
            try
            {
                futures.add(start(processRange, executor, ackOnly, barrierOnly, force));
            }
            catch (Throwable t)
            {
                futures.add(ImmediateFuture.failure(t));
            }
        }

        return FutureCombiner.allOf(futures);
    }

    /**
     * Whether the step expecting {@code expected} still needs to run for {@code range}.
     *
     * A range may be only partially advanced, so we can't decide this from any single token in it: a concurrent
     * driver on another replica node may have moved some sub-ranges forward while we were working. We skip only
     * when the <i>entire</i> range has moved past {@code expected} (including the case where the keyspace's
     * transfer has completed altogether, leaving no failover state at all). If any part of it is still at
     * {@code expected} we run the step over the whole range: paxos repair and the MT barrier are idempotent, and
     * a range is always contained within a single replica set, so re-running over the sub-ranges that have
     * already advanced costs work but changes nothing.
     *
     * A sub-range <i>behind</i> {@code expected} means the range regressed, which monotonic state advancement
     * forbids (see {@link SatelliteFailover.State#failoverProgress()}). No caller can reach that, so this is an
     * invariant assertion rather than an operator-facing error.
     */
    private boolean shouldRun(ClusterMetadata metadata, Range<Token> range, SatelliteFailover.State expected)
    {
        SatelliteFailover.State least = strategy.getFailoverInfo(metadata).leastAdvancedState(range);
        if (least.failoverProgress() < expected.failoverProgress())
            throw new IllegalStateException(String.format("Range %s of keyspace %s is in state %s, expected at least %s",
                                                          range, keyspace.getName(), least, expected));

        return least.failoverProgress() == expected.failoverProgress();
    }

    private static class EpochCallback extends AsyncPromise<Void> implements RequestCallbackWithFailure<Epoch>
    {
        private final Epoch minEpoch;
        private final ResponseTracker tracker;

        public EpochCallback(Epoch minEpoch, ResponseTracker tracker)
        {
            this.minEpoch = minEpoch;
            this.tracker = tracker;
        }

        private void response(InetAddressAndPort endpoint, boolean outcome)
        {
            if (tracker.isComplete())
                return;

            if (outcome)
                tracker.onResponse(endpoint);
            else
                tracker.onFailure(endpoint);

            if (!tracker.isComplete())
                return;

            if (tracker.isSuccessful())
            {
                trySuccess(null);
            }
            else
            {
                tryFailure(new Exception("Epoch checkpoint failed"));
            }
        }

        @Override
        public void onResponse(Message<Epoch> msg)
        {
            response(msg.from(), msg.payload.isEqualOrAfter(minEpoch));
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailure failure)
        {
            response(from, false);
        }
    }

    /**
     * TRANSITION_ACK -> TRANSITION step 1: Verify QoQ of replicas in both old and new query groups have observed the schema change.
     *
     * If a QoQ have seen the schema change then it will be impossible for the old primary to execute any paxos operations.
     * This means that once we start paxos repair we won't be racing with any client coordinated paxos operations.
     */
    private Future<Void> queryMinEpoch(Range<Token> range)
    {
        // A concurrent driver on another replica node may have already advanced this range past
        // TRANSITION_ACK. If the whole range has advanced, the epoch check has effectively been done and we
        // skip it. The same metadata snapshot is passed to the plan so its precondition sees exactly the state
        // we just checked.
        ClusterMetadata metadata = ClusterMetadata.current();
        if (!shouldRun(metadata, range, SatelliteFailover.State.TRANSITION_ACK))
            return ImmediateFuture.success(null);

        CoordinationPlan.ForTokenRead plan = strategy.planForFailoverEpochCheck(metadata, keyspace, range);

        Message<Epoch> msg = Message.out(Verb.TCM_CURRENT_EPOCH_REQ, minEpoch);

        EpochCallback callback = new EpochCallback(minEpoch, plan.responses());

        for (InetAddressAndPort endpoint : plan.replicas().contacts().endpoints())
        {
            messaging.sendWithCallback(msg, endpoint, callback);
        }

        return callback;
    }

    /**
     * TRANSITION_ACK -> TRANSITION step 2: Run paxos repair on old primary DC nodes to complete in-flight committed operations.
     *
     * Running paxos repair on the old primary completes any pending paxos operations. This guards against a few problems
     * related with primary transfer.
     *
     * First, if a paxos operation failed to commit at QoQ but still committed at a minority, then MT could
     * asynchronously replicate this write after the new primary starts running paxos operations, breaking linearizability
     *
     * Second, it poisons any partially accepted proposals in flight in the old primary. The paxos prepare phase attempts
     * to complete proposals that have been partially accepted. This means that without a paxos repair step, if we
     * transferred the primary role from DC1 -> DC2, then back to DC1, new paxos operations could attempt to complete
     * partially accepted proposals from before the initial primary transfer, again breaking linearizability.
     * Paxos repair, however, prevents this by poisoning partiall completed proposals by committing a noop with a higher
     * ballot than any partially completed (including only prepared) operation it encounters.
     *
     * This step is skipped if the old primary has been disabled. Presumably the DC is not reachable, so paxos repair
     * isn't possible, and there's a cleanup process that's run during the re-enable process that supersedes paxos repair.
     */
    private Future<?> runPaxosRepair(Range<Token> range, ExecutorPlus executor)
    {
        // TODO: rework paxos repair so we can succeed with quorums
        ClusterMetadata metadata = ClusterMetadata.current();
        // Skip if a concurrent driver already advanced the whole range past TRANSITION_ACK.
        if (!shouldRun(metadata, range, SatelliteFailover.State.TRANSITION_ACK))
            return SUCCESS;
        CoordinationPlan.ForWrite plan = strategy.planForFailoverPaxosRepair(metadata, keyspace, range);
        List<InetAddressAndPort> endpoints = plan.replicas().contacts().endpointList();

        if (endpoints.isEmpty())
            throw new IllegalStateException("No endpoints found for old primary DC " + fromDC + " for range " + range);

        KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(keyspace.getName());
        List<Range<Token>> singleRange = List.of(range);

        List<Future<?>> futures = new ArrayList<>();
        for (TableMetadata table : ksm.tables)
        {
            logger.debug("Starting paxos repair for {}.{} range {} on DC {}", keyspace, table.name, range, fromDC);
            PaxosCleanup cleanup = PaxosCleanup.cleanup(ctx, endpoints, table, singleRange, false, executor);
            futures.add(cleanup);
        }

        return FutureCombiner.allOf(futures);
    }

    /**
     * Commit a state advancement for a single range via TCM.
     */
    private void commitAdvance(Range<Token> range, TargetState targetState)
    {
        NormalizedRanges<Token> ranges = NormalizedRanges.normalizedRanges(Collections.singletonList(range));
        ClusterMetadataService.instance().commit(
            new AdvanceSatelliteFailoverState(keyspace.getName(), ranges, targetState),
            metadata -> {
                logger.info("Advanced range {} to {} in keyspace {}", range, targetState, keyspace);
                return metadata;
            },
            // The keyspace's failover transfer may have already completed (and the state been removed)
            // by a concurrent driver on another replica node. The transformation then rejects with
            // "no active failover transfer", which is a benign no-op for us — not a failure to surface
            // to the operator.
            (code, message) -> {
                logger.info("Skipping advance of range {} to {} in keyspace {}: {} ({})",
                            range, targetState, keyspace, message, code);
                return ClusterMetadata.current();
            });
    }

    /**
     * TRANSITION -> NORMAL step: Mutation tracking barrier - ensures all mutations are reconciled across replicas.
     */
    private Future<?> runMTBarrier(Range<Token> range)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        // Skip if a concurrent driver already advanced the whole range past TRANSITION (e.g. to NORMAL).
        if (!shouldRun(metadata, range, SatelliteFailover.State.TRANSITION))
            return SUCCESS;
        KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(keyspace.getName());
        CoordinationPlan.ForTokenRead plan = strategy.planForFailoverBarrier(metadata, keyspace, range);
        Set<InetAddressAndPort> participants = plan.replicas().contacts().endpoints();
        List<Future<Void>> futures = new ArrayList<>();

        for (TableMetadata table : ksm.tables)
        {
            RepairJobDesc desc = new RepairJobDesc(TimeUUID.Generator.nextTimeUUID(),
                                                   TimeUUID.Generator.nextTimeUUID(),
                                                   keyspace.getName(),
                                                   table.name,
                                                   Collections.singletonList(range));

            // TODO: rework so we can succeed with quorums
            MutationTrackingSyncCoordinator coordinator = new MutationTrackingSyncCoordinator(
                ctx, desc, participants, metadata);
            coordinator.start();
            futures.add(coordinator.future());
        }

        return FutureCombiner.allOf(futures);
    }
}
