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
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.Pair;

/**
 * Coordinates the (active) repair of a token range.
 *
 * A given RepairSession repairs a set of replicas for a given range on a list
 * of column families. For each of the column family to repair, RepairSession
 * creates a {@link RepairJob} that handles the repair of that CF.
 *
 * A given RepairJob has the 2 main phases:
 * <ol>
 *   <li>Validation phase: the job requests merkle trees from each of the replica involves
 *      ({@link org.apache.cassandra.repair.ValidationTask}) and waits until all trees are received (in
 *      validationComplete()).
 *   </li>
 *   <li>Synchronization phase: once all trees are received, the job compares each tree with
 *      all the other using a so-called {@link SyncTask}. If there is difference between 2 trees, the
 *      concerned SyncTask will start a streaming of the difference between the 2 endpoint concerned.
 *   </li>
 * </ol>
 * The job is done once all its SyncTasks are done (i.e. have either computed no differences
 * or the streaming they started is done (syncComplete())).
 *
 * A given session will execute the first phase (validation phase) of each of it's job
 * sequentially. In other words, it will start the first job and only start the next one
 * once that first job validation phase is complete. This is done so that the replica only
 * create one merkle tree at a time, which is our way to ensure that such creation starts
 * roughly at the same time on every node (see CASSANDRA-2816). However the synchronization
 * phases are allowed to run concurrently (with each other and with validation phases).
 *
 * A given RepairJob has 2 modes: either sequential or not (isSequential flag). If sequential,
 * it will requests merkle tree creation from each replica in sequence (though in that case
 * we still first send a message to each node to flush and snapshot data so each merkle tree
 * creation is still done on similar data, even if the actual creation is not
 * done simulatneously). If not sequential, all merkle tree are requested in parallel.
 * Similarly, if a job is sequential, it will handle one SyncTask at a time, but will handle
 * all of them in parallel otherwise.
 */
public class RepairSession extends AbstractFuture<RepairSessionResult> implements IEndpointStateChangeSubscriber,
                                                                                 IFailureDetectionEventListener
{
    private static Logger logger = LoggerFactory.getLogger(RepairSession.class);

    public final UUID parentRepairSession;
    /** Repair session ID */
    private final UUID id;
    public final String keyspace;
    private final String[] cfnames;
    public final RepairParallelism parallelismDegree;
    /** Range to repair */
    public final Range<Token> range;
    public final Set<InetAddress> endpoints;
    private final long repairedAt;

    // number of validations left to be performed
    private final AtomicInteger validationRemaining;

    private final AtomicBoolean isFailed = new AtomicBoolean(false);

    // Each validation task waits response from replica in validating ConcurrentMap (keyed by CF name and endpoint address)
    private final ConcurrentMap<Pair<RepairJobDesc, InetAddress>, ValidationTask> validating = new ConcurrentHashMap<>();
    // Remote syncing jobs wait response in syncingTasks map
    private final ConcurrentMap<Pair<RepairJobDesc, NodePair>, RemoteSyncTask> syncingTasks = new ConcurrentHashMap<>();

    // Tasks(snapshot, validate request, differencing, ...) are run on taskExecutor
    private final ListeningExecutorService taskExecutor = MoreExecutors.listeningDecorator(DebuggableThreadPoolExecutor.createCachedThreadpoolWithMaxSize("RepairJobTask"));

    private volatile boolean terminated = false;

    /**
     * Create new repair session.
     *
     * @param parentRepairSession the parent sessions id
     * @param id this sessions id
     * @param range range to repair
     * @param keyspace name of keyspace
     * @param parallelismDegree specifies the degree of parallelism when calculating the merkle trees
     * @param endpoints the data centers that should be part of the repair; null for all DCs
     * @param repairedAt when the repair occurred (millis)
     * @param cfnames names of columnfamilies
     */
    public RepairSession(UUID parentRepairSession,
                         UUID id,
                         Range<Token> range,
                         String keyspace,
                         RepairParallelism parallelismDegree,
                         Set<InetAddress> endpoints,
                         long repairedAt,
                         String... cfnames)
    {
        assert cfnames.length > 0 : "Repairing no column families seems pointless, doesn't it";

        this.parentRepairSession = parentRepairSession;
        this.id = id;
        this.parallelismDegree = parallelismDegree;
        this.keyspace = keyspace;
        this.cfnames = cfnames;
        this.range = range;
        this.endpoints = endpoints;
        this.repairedAt = repairedAt;
        this.validationRemaining = new AtomicInteger(cfnames.length);
    }

    public UUID getId()
    {
        return id;
    }

    public Range<Token> getRange()
    {
        return range;
    }

    public void waitForValidation(Pair<RepairJobDesc, InetAddress> key, ValidationTask task)
    {
        validating.put(key, task);
    }

    public void waitForSync(Pair<RepairJobDesc, NodePair> key, RemoteSyncTask task)
    {
        syncingTasks.put(key, task);
    }

    /**
     * Receive merkle tree response or failed response from {@code endpoint} for current repair job.
     *
     * @param desc repair job description
     * @param endpoint endpoint that sent merkle tree
     * @param tree calculated merkle tree, or null if validation failed
     */
    public void validationComplete(RepairJobDesc desc, InetAddress endpoint, MerkleTree tree)
    {
        ValidationTask task = validating.remove(Pair.create(desc, endpoint));
        if (task == null)
        {
            assert terminated;
            return;
        }

        String message = String.format("Received merkle tree for %s from %s", desc.columnFamily, endpoint);
        logger.info("[repair #{}] {}", getId(), message);
        Tracing.traceRepair(message);
        task.treeReceived(tree);

        // Unregister from FailureDetector once we've completed synchronizing Merkle trees.
        // After this point, we rely on tcp_keepalive for individual sockets to notify us when a connection is down.
        // See CASSANDRA-3569
        if (validationRemaining.decrementAndGet() == 0)
        {
            FailureDetector.instance.unregisterFailureDetectionEventListener(this);
        }
    }

    /**
     * Notify this session that sync completed/failed with given {@code NodePair}.
     *
     * @param desc synced repair job
     * @param nodes nodes that completed sync
     * @param success true if sync succeeded
     */
    public void syncComplete(RepairJobDesc desc, NodePair nodes, boolean success)
    {
        RemoteSyncTask task = syncingTasks.get(Pair.create(desc, nodes));
        if (task == null)
        {
            assert terminated;
            return;
        }

        logger.debug(String.format("[repair #%s] Repair completed between %s and %s on %s", getId(), nodes.endpoint1, nodes.endpoint2, desc.columnFamily));
        task.syncComplete(success);
    }

    private String repairedNodes()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(FBUtilities.getBroadcastAddress());
        for (InetAddress ep : endpoints)
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
    public void start(ListeningExecutorService executor)
    {
        String message;
        if (terminated)
            return;

        logger.info(String.format("[repair #%s] new session: will sync %s on range %s for %s.%s", getId(), repairedNodes(), range, keyspace, Arrays.toString(cfnames)));
        Tracing.traceRepair("Syncing range {}", range);
        SystemDistributedKeyspace.startRepairs(getId(), parentRepairSession, keyspace, cfnames, range, endpoints);

        if (endpoints.isEmpty())
        {
            logger.info("[repair #{}] {}", getId(), message = String.format("No neighbors to repair with on range %s: session completed", range));
            Tracing.traceRepair(message);
            set(new RepairSessionResult(id, keyspace, range, Lists.<RepairResult>newArrayList()));
            SystemDistributedKeyspace.failRepairs(getId(), keyspace, cfnames, new RuntimeException(message));
            return;
        }

        // Checking all nodes are live
        for (InetAddress endpoint : endpoints)
        {
            if (!FailureDetector.instance.isAlive(endpoint))
            {
                message = String.format("Cannot proceed on repair because a neighbor (%s) is dead: session failed", endpoint);
                logger.error("[repair #{}] {}", getId(), message);
                Exception e = new IOException(message);
                setException(e);
                SystemDistributedKeyspace.failRepairs(getId(), keyspace, cfnames, e);
                return;
            }
        }

        // Create and submit RepairJob for each ColumnFamily
        List<ListenableFuture<RepairResult>> jobs = new ArrayList<>(cfnames.length);
        for (String cfname : cfnames)
        {
            RepairJob job = new RepairJob(this, cfname, parallelismDegree, repairedAt, taskExecutor);
            executor.execute(job);
            jobs.add(job);
        }

        // When all RepairJobs are done without error, cleanup and set the final result
        Futures.addCallback(Futures.allAsList(jobs), new FutureCallback<List<RepairResult>>()
        {
            public void onSuccess(List<RepairResult> results)
            {
                // this repair session is completed
                logger.info("[repair #{}] {}", getId(), "Session completed successfully");
                Tracing.traceRepair("Completed sync of range {}", range);
                set(new RepairSessionResult(id, keyspace, range, results));

                taskExecutor.shutdown();
                // mark this session as terminated
                terminate();
            }

            public void onFailure(Throwable t)
            {
                logger.error(String.format("[repair #%s] Session completed with the following error", getId()), t);
                Tracing.traceRepair("Session completed with the following error: {}", t);
                forceShutdown(t);
            }
        });
    }

    public void terminate()
    {
        terminated = true;
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
        setException(reason);
        taskExecutor.shutdownNow();
        terminate();
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        if (!endpoints.contains(endpoint))
            return;

        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        // Though unlikely, it is possible to arrive here multiple time and we
        // want to avoid print an error message twice
        if (!isFailed.compareAndSet(false, true))
            return;

        Exception exception = new IOException(String.format("Endpoint %s died", endpoint));
        logger.error(String.format("[repair #%s] session completed with the following error", getId()), exception);
        // If a node failed, we stop everything (though there could still be some activity in the background)
        forceShutdown(exception);
    }
}
