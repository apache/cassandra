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

import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.repair.asymmetric.DifferenceHolder;
import org.apache.cassandra.repair.asymmetric.HostDifferences;
import org.apache.cassandra.repair.asymmetric.PreferedNodeFilter;
import org.apache.cassandra.repair.asymmetric.ReduceHelper;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob extends AbstractFuture<RepairResult> implements Runnable
{
    private static Logger logger = LoggerFactory.getLogger(RepairJob.class);

    private final RepairSession session;
    private final RepairJobDesc desc;
    private final RepairParallelism parallelismDegree;
    private final ListeningExecutorService taskExecutor;
    private final boolean isIncremental;
    private final PreviewKind previewKind;
    private final boolean optimiseStreams;

    /**
     * Create repair job to run on specific columnfamily
     *
     * @param session RepairSession that this RepairJob belongs
     * @param columnFamily name of the ColumnFamily to repair
     */
    public RepairJob(RepairSession session, String columnFamily, boolean isIncremental, PreviewKind previewKind, boolean optimiseStreams)
    {
        this.session = session;
        this.desc = new RepairJobDesc(session.parentRepairSession, session.getId(), session.keyspace, columnFamily, session.getRanges());
        this.taskExecutor = session.taskExecutor;
        this.parallelismDegree = session.parallelismDegree;
        this.isIncremental = isIncremental;
        this.previewKind = previewKind;
        this.optimiseStreams = optimiseStreams;
    }

    /**
     * Runs repair job.
     *
     * This sets up necessary task and runs them on given {@code taskExecutor}.
     * After submitting all tasks, waits until validation with replica completes.
     */
    public void run()
    {
        Keyspace ks = Keyspace.open(desc.keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(desc.columnFamily);
        cfs.metric.repairsStarted.inc();
        List<InetAddress> allEndpoints = new ArrayList<>(session.endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddress());

        ListenableFuture<List<TreeResponse>> validations;
        // Create a snapshot at all nodes unless we're using pure parallel repairs
        if (parallelismDegree != RepairParallelism.PARALLEL)
        {
            ListenableFuture<List<InetAddress>> allSnapshotTasks;
            if (isIncremental)
            {
                // consistent repair does it's own "snapshotting"
                allSnapshotTasks = Futures.immediateFuture(allEndpoints);
            }
            else
            {
                // Request snapshot to all replica
                List<ListenableFuture<InetAddress>> snapshotTasks = new ArrayList<>(allEndpoints.size());
                for (InetAddress endpoint : allEndpoints)
                {
                    SnapshotTask snapshotTask = new SnapshotTask(desc, endpoint);
                    snapshotTasks.add(snapshotTask);
                    taskExecutor.execute(snapshotTask);
                }
                allSnapshotTasks = Futures.allAsList(snapshotTasks);
            }

            // When all snapshot complete, send validation requests
            validations = Futures.transformAsync(allSnapshotTasks, new AsyncFunction<List<InetAddress>, List<TreeResponse>>()
            {
                public ListenableFuture<List<TreeResponse>> apply(List<InetAddress> endpoints)
                {
                    if (parallelismDegree == RepairParallelism.SEQUENTIAL)
                        return sendSequentialValidationRequest(endpoints);
                    else
                        return sendDCAwareValidationRequest(endpoints);
                }
            }, taskExecutor);
        }
        else
        {
            // If not sequential, just send validation request to all replica
            validations = sendValidationRequest(allEndpoints);
        }

        // When all validations complete, submit sync tasks
        ListenableFuture<List<SyncStat>> syncResults = Futures.transformAsync(validations, optimiseStreams && !session.pullRepair ? optimisedSyncing() : standardSyncing(), taskExecutor);

        // When all sync complete, set the final result
        Futures.addCallback(syncResults, new FutureCallback<List<SyncStat>>()
        {
            public void onSuccess(List<SyncStat> stats)
            {
                if (!previewKind.isPreview())
                {
                    logger.info("{} {} is fully synced", previewKind.logPrefix(session.getId()), desc.columnFamily);
                    SystemDistributedKeyspace.successfulRepairJob(session.getId(), desc.keyspace, desc.columnFamily);
                }
                cfs.metric.repairsCompleted.inc();
                set(new RepairResult(desc, stats));
            }

            /**
             * Snapshot, validation and sync failures are all handled here
             */
            public void onFailure(Throwable t)
            {
                if (!previewKind.isPreview())
                {
                    logger.warn("{} {} sync failed", previewKind.logPrefix(session.getId()), desc.columnFamily);
                    SystemDistributedKeyspace.failedRepairJob(session.getId(), desc.keyspace, desc.columnFamily, t);
                }
                cfs.metric.repairsCompleted.inc();
                setException(t);
            }
        }, taskExecutor);
    }

    private AsyncFunction<List<TreeResponse>, List<SyncStat>> standardSyncing()
    {
        return trees ->
        {
            InetAddress local = FBUtilities.getLocalAddress();

            List<SyncTask> syncTasks = new ArrayList<>();
            // We need to difference all trees one against another
            for (int i = 0; i < trees.size() - 1; ++i)
            {
                TreeResponse r1 = trees.get(i);
                for (int j = i + 1; j < trees.size(); ++j)
                {
                    TreeResponse r2 = trees.get(j);
                    SyncTask task;
                    if (r1.endpoint.equals(local) || r2.endpoint.equals(local))
                    {
                        task = new LocalSyncTask(desc, r1, r2, isIncremental ? desc.parentSessionId : null, session.pullRepair, session.previewKind);
                    }
                    else
                    {
                        task = new RemoteSyncTask(desc, r1, r2, session.previewKind);
                        // RemoteSyncTask expects SyncComplete message sent back.
                        // Register task to RepairSession to receive response.
                        session.waitForSync(Pair.create(desc, new NodePair(r1.endpoint, r2.endpoint)), (RemoteSyncTask) task);
                    }
                    syncTasks.add(task);
                    taskExecutor.submit(task);
                }
            }
            return Futures.allAsList(syncTasks);
        };
    }

    private AsyncFunction<List<TreeResponse>, List<SyncStat>> optimisedSyncing()
    {
        return trees ->
        {
            InetAddress local = FBUtilities.getLocalAddress();

            List<AsymmetricSyncTask> syncTasks = new ArrayList<>();
            // We need to difference all trees one against another
            DifferenceHolder diffHolder = new DifferenceHolder(trees);

            logger.debug("diffs = {}", diffHolder);
            PreferedNodeFilter preferSameDCFilter = (streaming, candidates) ->
                                                    candidates.stream()
                                                              .filter(node -> getDC(streaming)
                                                                              .equals(getDC(node)))
                                                              .collect(Collectors.toSet());
            ImmutableMap<InetAddress, HostDifferences> reducedDifferences = ReduceHelper.reduce(diffHolder, preferSameDCFilter);

            for (int i = 0; i < trees.size(); i++)
            {
                InetAddress address = trees.get(i).endpoint;
                HostDifferences streamsFor = reducedDifferences.get(address);
                if (streamsFor != null)
                {
                    assert streamsFor.get(address).isEmpty() : "We should not fetch ranges from ourselves";
                    for (InetAddress fetchFrom : streamsFor.hosts())
                    {
                        List<Range<Token>> toFetch = streamsFor.get(fetchFrom);
                        logger.debug("{} is about to fetch {} from {}", address, toFetch, fetchFrom);
                        AsymmetricSyncTask task;
                        if (address.equals(local))
                        {
                            task = new AsymmetricLocalSyncTask(desc, fetchFrom, toFetch, isIncremental ? desc.parentSessionId : null, previewKind);
                        }
                        else
                        {
                            task = new AsymmetricRemoteSyncTask(desc, address, fetchFrom, toFetch, previewKind);
                            session.waitForSync(Pair.create(desc, new NodePair(address, fetchFrom)),(AsymmetricRemoteSyncTask)task);
                        }
                        syncTasks.add(task);
                        taskExecutor.submit(task);
                    }
                }
                else
                {
                    logger.debug("Node {} has nothing to stream", address);
                }
            }
            return Futures.allAsList(syncTasks);
        };
    }

    private String getDC(InetAddress address)
    {
        return DatabaseDescriptor.getEndpointSnitch().getDatacenter(address);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor in parallel.
     *
     * @param endpoints Endpoint addresses to send validation request
     * @return Future that can get all {@link TreeResponse} from replica, if all validation succeed.
     */
    private ListenableFuture<List<TreeResponse>> sendValidationRequest(Collection<InetAddress> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());
        for (InetAddress endpoint : endpoints)
        {
            ValidationTask task = new ValidationTask(desc, endpoint, nowInSec, previewKind);
            tasks.add(task);
            session.waitForValidation(Pair.create(desc, endpoint), task);
            taskExecutor.execute(task);
        }
        return Futures.allAsList(tasks);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially.
     */
    private ListenableFuture<List<TreeResponse>> sendSequentialValidationRequest(Collection<InetAddress> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Queue<InetAddress> requests = new LinkedList<>(endpoints);
        InetAddress address = requests.poll();
        ValidationTask firstTask = new ValidationTask(desc, address, nowInSec, previewKind);
        logger.info("Validating {}", address);
        session.waitForValidation(Pair.create(desc, address), firstTask);
        tasks.add(firstTask);
        ValidationTask currentTask = firstTask;
        while (requests.size() > 0)
        {
            final InetAddress nextAddress = requests.poll();
            final ValidationTask nextTask = new ValidationTask(desc, nextAddress, nowInSec, previewKind);
            tasks.add(nextTask);
            Futures.addCallback(currentTask, new FutureCallback<TreeResponse>()
            {
                public void onSuccess(TreeResponse result)
                {
                    logger.info("Validating {}", nextAddress);
                    session.waitForValidation(Pair.create(desc, nextAddress), nextTask);
                    taskExecutor.execute(nextTask);
                }

                // failure is handled at root of job chain
                public void onFailure(Throwable t) {}
            });
            currentTask = nextTask;
        }
        // start running tasks
        taskExecutor.execute(firstTask);
        return Futures.allAsList(tasks);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially within each dc.
     */
    private ListenableFuture<List<TreeResponse>> sendDCAwareValidationRequest(Collection<InetAddress> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Map<String, Queue<InetAddress>> requestsByDatacenter = new HashMap<>();
        for (InetAddress endpoint : endpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            Queue<InetAddress> queue = requestsByDatacenter.get(dc);
            if (queue == null)
            {
                queue = new LinkedList<>();
                requestsByDatacenter.put(dc, queue);
            }
            queue.add(endpoint);
        }

        for (Map.Entry<String, Queue<InetAddress>> entry : requestsByDatacenter.entrySet())
        {
            Queue<InetAddress> requests = entry.getValue();
            InetAddress address = requests.poll();
            ValidationTask firstTask = new ValidationTask(desc, address, nowInSec, previewKind);
            logger.info("Validating {}", address);
            session.waitForValidation(Pair.create(desc, address), firstTask);
            tasks.add(firstTask);
            ValidationTask currentTask = firstTask;
            while (requests.size() > 0)
            {
                final InetAddress nextAddress = requests.poll();
                final ValidationTask nextTask = new ValidationTask(desc, nextAddress, nowInSec, previewKind);
                tasks.add(nextTask);
                Futures.addCallback(currentTask, new FutureCallback<TreeResponse>()
                {
                    public void onSuccess(TreeResponse result)
                    {
                        logger.info("Validating {}", nextAddress);
                        session.waitForValidation(Pair.create(desc, nextAddress), nextTask);
                        taskExecutor.execute(nextTask);
                    }

                    // failure is handled at root of job chain
                    public void onFailure(Throwable t) {}
                });
                currentTask = nextTask;
            }
            // start running tasks
            taskExecutor.execute(firstTask);
        }
        return Futures.allAsList(tasks);
    }
}
