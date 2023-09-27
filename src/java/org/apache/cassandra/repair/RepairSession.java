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
package org.apache.cassandra.repair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.repair.consistent.LocalSession;
import org.apache.cassandra.repair.consistent.LocalSessions;
import org.apache.cassandra.repair.messages.SyncResponse;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.state.SessionState;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

/**
 * Coordinates the (active) repair of a list of non overlapping token ranges.
 *
 * A given RepairSession repairs a set of replicas for a given set of ranges on a list
 * of column families. For each of the column family to repair, RepairSession
 * creates a {@link RepairJob} that handles the repair of that CF.
 *
 * A given RepairJob has the 3 main phases:
 * <ol>
 *   <li>
 *     Paxos repair: unfinished paxos operations in the range/keyspace/table are first completed
 *   </li>
 *   <li>Validation phase: the job requests merkle trees from each of the replica involves
 *      ({@link org.apache.cassandra.repair.ValidationTask}) and waits until all trees are received (in
 *      validationComplete()).
 *   </li>
 *   <li>Synchronization phase: once all trees are received, the job compares each tree with  all the others. If there is
 *       difference between 2 trees, the differences between the 2 endpoints will be streamed with a {@link SyncTask}.
 *   </li>
 * </ol>
 * The job is done once all its SyncTasks are done (i.e. have either computed no differences
 * or the streaming they started is done (syncComplete())).
 *
 * A given session will execute the first phase (validation phase) of each of it's job
 * sequentially. In other words, it will start the first job and only start the next one
 * once that first job validation phase is complete. This is done so that the replica only
 * create one merkle tree per range at a time, which is our way to ensure that such creation starts
 * roughly at the same time on every node (see CASSANDRA-2816). However the synchronization
 * phases are allowed to run concurrently (with each other and with validation phases).
 *
 * A given RepairJob has 2 modes: either sequential or not (RepairParallelism). If sequential,
 * it will requests merkle tree creation from each replica in sequence (though in that case
 * we still first send a message to each node to flush and snapshot data so each merkle tree
 * creation is still done on similar data, even if the actual creation is not
 * done simulatneously). If not sequential, all merkle tree are requested in parallel.
 * Similarly, if a job is sequential, it will handle one SymmetricSyncTask at a time, but will handle
 * all of them in parallel otherwise.
 */
public class RepairSession extends AsyncFuture<RepairSessionResult> implements IEndpointStateChangeSubscriber,
                                                                                  IFailureDetectionEventListener,
                                                                                  LocalSessions.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(RepairSession.class);

    public final SessionState state;
    public final RepairParallelism parallelismDegree;
    public final boolean pullRepair;

    /** Range to repair */
    public final boolean isIncremental;
    public final PreviewKind previewKind;
    public final boolean repairPaxos;
    public final boolean paxosOnly;

    private final AtomicBoolean isFailed = new AtomicBoolean(false);

    // Each validation task waits response from replica in validating ConcurrentMap (keyed by CF name and endpoint address)
    private final ConcurrentMap<Pair<RepairJobDesc, InetAddressAndPort>, ValidationTask> validating = new ConcurrentHashMap<>();
    // Remote syncing jobs wait response in syncingTasks map
    private final ConcurrentMap<Pair<RepairJobDesc, SyncNodePair>, CompletableRemoteSyncTask> syncingTasks = new ConcurrentHashMap<>();

    // Tasks(snapshot, validate request, differencing, ...) are run on taskExecutor
    public final SafeExecutor taskExecutor;
    public final boolean optimiseStreams;
    public final SharedContext ctx;
    private volatile List<RepairJob> jobs = Collections.emptyList();

    private volatile boolean terminated = false;

    /**
     * Create new repair session.
     * @param parentRepairSession the parent sessions id
     * @param commonRange ranges to repair
     * @param keyspace name of keyspace
     * @param parallelismDegree specifies the degree of parallelism when calculating the merkle trees
     * @param pullRepair true if the repair should be one way (from remote host to this host and only applicable between two hosts--see RepairOption)
     * @param repairPaxos true if incomplete paxos operations should be completed as part of repair
     * @param paxosOnly true if we should only complete paxos operations, not run a normal repair
     * @param cfnames names of columnfamilies
     */
    public RepairSession(SharedContext ctx,
                         TimeUUID parentRepairSession,
                         CommonRange commonRange,
                         String keyspace,
                         RepairParallelism parallelismDegree,
                         boolean isIncremental,
                         boolean pullRepair,
                         PreviewKind previewKind,
                         boolean optimiseStreams,
                         boolean repairPaxos,
                         boolean paxosOnly,
                         String... cfnames)
    {
        this.ctx = ctx;
        this.repairPaxos = repairPaxos;
        this.paxosOnly = paxosOnly;
        assert cfnames.length > 0 : "Repairing no column families seems pointless, doesn't it";
        this.state = new SessionState(ctx.clock(), parentRepairSession, keyspace, cfnames, commonRange);
        this.parallelismDegree = parallelismDegree;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
        this.pullRepair = pullRepair;
        this.optimiseStreams = optimiseStreams;
        this.taskExecutor = new SafeExecutor(createExecutor(ctx));
    }

    @VisibleForTesting
    protected ExecutorPlus createExecutor(SharedContext ctx)
    {
        return ctx.executorFactory().pooled("RepairJobTask", Integer.MAX_VALUE);
    }

    public TimeUUID getId()
    {
        return state.id;
    }

    public Collection<Range<Token>> ranges()
    {
        return state.commonRange.ranges;
    }

    public Collection<InetAddressAndPort> endpoints()
    {
        return state.commonRange.endpoints;
    }

    public synchronized void trackValidationCompletion(Pair<RepairJobDesc, InetAddressAndPort> key, ValidationTask task)
    {
        if (terminated)
        {
            task.abort(new RuntimeException("Session terminated"));
            return;
        }
        validating.put(key, task);
    }

    public synchronized void trackSyncCompletion(Pair<RepairJobDesc, SyncNodePair> key, CompletableRemoteSyncTask task)
    {
        if (terminated)
            return;
        syncingTasks.put(key, task);
    }

    /**
     * Receive merkle tree response or failed response from {@code endpoint} for current repair job.
     *
     * @param desc repair job description
     * @param message containing the merkle trees or an error
     */
    public void validationComplete(RepairJobDesc desc, Message<ValidationResponse> message)
    {
        InetAddressAndPort endpoint = message.from();
        MerkleTrees trees = message.payload.trees;
        ValidationTask task = validating.remove(Pair.create(desc, endpoint));
        // replies without a callback get dropped, so if in mixed mode this should be ignored
        ctx.messaging().send(message.emptyResponse(), message.from());
        if (task == null)
        {
            // The trees may be off-heap, and will therefore need to be released.
            if (trees != null)
                trees.release();

            // either the session completed so the validation is no longer needed, or this is a retry; in both cases there is nothing to do
            return;
        }

        String msg = String.format("Received merkle tree for %s from %s", desc.columnFamily, endpoint);
        logger.info("{} {}", previewKind.logPrefix(getId()), msg);
        Tracing.traceRepair(msg);
        task.treesReceived(trees);
    }

    /**
     * Notify this session that sync completed/failed with given {@code SyncNodePair}.
     *
     * @param desc synced repair job
     * @param message nodes that completed sync and if they were successful
     */
    public void syncComplete(RepairJobDesc desc, Message<SyncResponse> message)
    {
        SyncNodePair nodes = message.payload.nodes;
        CompletableRemoteSyncTask task = syncingTasks.remove(Pair.create(desc, nodes));
        // replies without a callback get dropped, so if in mixed mode this should be ignored
        ctx.messaging().send(message.emptyResponse(), message.from());
        if (task == null)
            return;

        if (logger.isDebugEnabled())
            logger.debug("{} Repair completed between {} and {} on {}", previewKind.logPrefix(getId()), nodes.coordinator, nodes.peer, desc.columnFamily);
        task.syncComplete(message.payload.success, message.payload.summaries);
    }

    @VisibleForTesting
    Map<Pair<RepairJobDesc, SyncNodePair>, CompletableRemoteSyncTask> getSyncingTasks()
    {
        return Collections.unmodifiableMap(syncingTasks);
    }

    private String repairedNodes()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(FBUtilities.getBroadcastAddressAndPort());
        for (InetAddressAndPort ep : state.commonRange.endpoints)
            sb.append(", ").append(ep);
        return sb.toString();
    }

    /**
     * Start RepairJob on given ColumnFamilies.
     *
     * This first validates if all replica are available, and if they are,
     * creates RepairJobs and submit to run on given executor.
     *
     * @param executor Executor to run validation
     */
    public void start(ExecutorPlus executor)
    {
        state.phase.start();
        String message;
        if (terminated)
            return;

        logger.info("{} parentSessionId = {}: new session: will sync {} on range {} for {}.{}",
                    previewKind.logPrefix(getId()), state.parentRepairSession, repairedNodes(), state.commonRange, state.keyspace, Arrays.toString(state.cfnames));
        Tracing.traceRepair("Syncing range {}", state.commonRange);
        if (!previewKind.isPreview() && !paxosOnly)
        {
            SystemDistributedKeyspace.startRepairs(getId(), state.parentRepairSession, state.keyspace, state.cfnames, state.commonRange);
        }

        if (state.commonRange.endpoints.isEmpty())
        {
            logger.info("{} {}", previewKind.logPrefix(getId()), message = String.format("No neighbors to repair with on range %s: session completed", state.commonRange));
            state.phase.skip(message);
            Tracing.traceRepair(message);
            trySuccess(new RepairSessionResult(state.id, state.keyspace, state.commonRange.ranges, Lists.<RepairResult>newArrayList(), state.commonRange.hasSkippedReplicas));
            if (!previewKind.isPreview())
            {
                SystemDistributedKeyspace.failRepairs(getId(), state.keyspace, state.cfnames, new RuntimeException(message));
            }
            return;
        }

        // Checking all nodes are live
        for (InetAddressAndPort endpoint : state.commonRange.endpoints)
        {
            if (!ctx.failureDetector().isAlive(endpoint) && !state.commonRange.hasSkippedReplicas)
            {
                message = String.format("Cannot proceed on repair because a neighbor (%s) is dead: session failed", endpoint);
                state.phase.fail(message);
                logger.error("{} {}", previewKind.logPrefix(getId()), message);
                Exception e = new IOException(message);
                tryFailure(e);
                if (!previewKind.isPreview())
                {
                    SystemDistributedKeyspace.failRepairs(getId(), state.keyspace, state.cfnames, e);
                }
                return;
            }
        }

        // Create and submit RepairJob for each ColumnFamily
        state.phase.jobsSubmitted();
        List<RepairJob> jobs = new ArrayList<>(state.cfnames.length);
        for (String cfname : state.cfnames)
        {
            RepairJob job = new RepairJob(this, cfname);
            state.register(job.state);
            executor.execute(job);
            jobs.add(job);
        }
        this.jobs = jobs;

        // When all RepairJobs are done without error, cleanup and set the final result
        FBUtilities.allOf(jobs).addCallback(new FutureCallback<List<RepairResult>>()
        {
            public void onSuccess(List<RepairResult> results)
            {
                state.phase.success();
                // this repair session is completed
                logger.info("{} {}", previewKind.logPrefix(getId()), "Session completed successfully");
                Tracing.traceRepair("Completed sync of range {}", state.commonRange);
                trySuccess(new RepairSessionResult(state.id, state.keyspace, state.commonRange.ranges, results, state.commonRange.hasSkippedReplicas));

                // mark this session as terminated
                terminate(null);
                taskExecutor.shutdown();
            }

            public void onFailure(Throwable t)
            {
                state.phase.fail(t);
                String msg = "{} Session completed with the following error";
                if (Throwables.anyCauseMatches(t, RepairException::shouldWarn))
                    logger.warn(msg+ ": {}", previewKind.logPrefix(getId()), t.getMessage());
                else
                    logger.error(msg, previewKind.logPrefix(getId()), t);
                Tracing.traceRepair("Session completed with the following error: {}", t);
                forceShutdown(t);
            }
        }, taskExecutor);
    }

    public synchronized void terminate(@Nullable Throwable reason)
    {
        terminated = true;
        List<RepairJob> jobs = this.jobs;
        if (jobs != null)
        {
            for (RepairJob job : jobs)
                job.abort(reason);
        }
        this.jobs = null;
        validating.clear();
        syncingTasks.clear();
    }

    /**
     * clear all RepairJobs and terminate this session.
     *
     * @param reason Cause of error for shutdown
     */
    public void forceShutdown(Throwable reason)
    {
        tryFailure(reason);
        terminate(reason);
        taskExecutor.shutdown();
    }
    public void onRemove(InetAddressAndPort endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddressAndPort endpoint, double phi)
    {
        if (!state.commonRange.endpoints.contains(endpoint))
            return;

        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        // Though unlikely, it is possible to arrive here multiple time and we
        // want to avoid print an error message twice
        if (!isFailed.compareAndSet(false, true))
            return;

        Exception exception = new IOException(String.format("Endpoint %s died", endpoint));
        logger.error("{} session completed with the following error", previewKind.logPrefix(getId()), exception);
        // If a node failed, we stop everything (though there could still be some activity in the background)
        forceShutdown(exception);
    }

    public void onIRStateChange(LocalSession session)
    {
        // we should only be registered as listeners for PreviewKind.REPAIRED, but double check here
        if (previewKind == PreviewKind.REPAIRED &&
            session.getState() == ConsistentSession.State.FINALIZED &&
            includesTables(session.tableIds))
        {
            for (Range<Token> range : session.ranges)
            {
                if (range.intersects(ranges()))
                {
                    logger.warn("{} An intersecting incremental repair with session id = {} finished, preview repair might not be accurate", previewKind.logPrefix(getId()), session.sessionID);
                    forceShutdown(RepairException.warn("An incremental repair with session id "+session.sessionID+" finished during this preview repair runtime"));
                    return;
                }
            }
        }
    }

    private boolean includesTables(Set<TableId> tableIds)
    {
        Keyspace ks = Keyspace.open(state.keyspace);
        if (ks != null)
        {
            for (String table : state.cfnames)
            {
                ColumnFamilyStore cfs = ks.getColumnFamilyStore(table);
                if (tableIds.contains(cfs.metadata.id))
                    return true;
            }
        }
        return false;
    }

    private static class SafeExecutor implements Executor
    {
        private final ExecutorPlus delegate;

        private SafeExecutor(ExecutorPlus delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command)
        {
            try
            {
                delegate.execute(command);
            }
            catch (RejectedExecutionException e)
            {
                // task executor was shutdown, so fall back to a known good executor to finish callbacks
                Stage.INTERNAL_RESPONSE.execute(command);
            }
        }

        public void shutdown()
        {
            delegate.shutdown();
        }
    }
}
