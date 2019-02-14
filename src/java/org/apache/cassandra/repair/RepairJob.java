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

import java.util.*;
import java.util.function.Predicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob extends AbstractFuture<RepairResult> implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(RepairJob.class);

    private final RepairSession session;
    private final RepairJobDesc desc;
    private final RepairParallelism parallelismDegree;
    private final ListeningExecutorService taskExecutor;

    /**
     * Create repair job to run on specific columnfamily
     *
     * @param session RepairSession that this RepairJob belongs
     * @param columnFamily name of the ColumnFamily to repair
     */
    public RepairJob(RepairSession session, String columnFamily)
    {
        this.session = session;
        this.desc = new RepairJobDesc(session.parentRepairSession, session.getId(), session.keyspace, columnFamily, session.commonRange.ranges);
        this.taskExecutor = session.taskExecutor;
        this.parallelismDegree = session.parallelismDegree;
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
        List<InetAddressAndPort> allEndpoints = new ArrayList<>(session.commonRange.endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddressAndPort());

        ListenableFuture<List<TreeResponse>> validations;
        // Create a snapshot at all nodes unless we're using pure parallel repairs
        if (parallelismDegree != RepairParallelism.PARALLEL)
        {
            ListenableFuture<List<InetAddressAndPort>> allSnapshotTasks;
            if (session.isIncremental)
            {
                // consistent repair does it's own "snapshotting"
                allSnapshotTasks = Futures.immediateFuture(allEndpoints);
            }
            else
            {
                // Request snapshot to all replica
                List<ListenableFuture<InetAddressAndPort>> snapshotTasks = new ArrayList<>(allEndpoints.size());
                for (InetAddressAndPort endpoint : allEndpoints)
                {
                    SnapshotTask snapshotTask = new SnapshotTask(desc, endpoint);
                    snapshotTasks.add(snapshotTask);
                    taskExecutor.execute(snapshotTask);
                }
                allSnapshotTasks = Futures.allAsList(snapshotTasks);
            }

            // When all snapshot complete, send validation requests
            validations = Futures.transformAsync(allSnapshotTasks, new AsyncFunction<List<InetAddressAndPort>, List<TreeResponse>>()
            {
                public ListenableFuture<List<TreeResponse>> apply(List<InetAddressAndPort> endpoints)
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
        ListenableFuture<List<SyncStat>> syncResults = Futures.transformAsync(validations,
                                                                              session.optimiseStreams && !session.pullRepair ? this::optimisedSyncing : this::standardSyncing,
                                                                              taskExecutor);

        // When all sync complete, set the final result
        Futures.addCallback(syncResults, new FutureCallback<List<SyncStat>>()
        {
            public void onSuccess(List<SyncStat> stats)
            {
                if (!session.previewKind.isPreview())
                {
                    logger.info("{} {} is fully synced", session.previewKind.logPrefix(session.getId()), desc.columnFamily);
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
                if (!session.previewKind.isPreview())
                {
                    logger.warn("{} {} sync failed", session.previewKind.logPrefix(session.getId()), desc.columnFamily);
                    SystemDistributedKeyspace.failedRepairJob(session.getId(), desc.keyspace, desc.columnFamily, t);
                }
                cfs.metric.repairsCompleted.inc();
                setException(t);
            }
        }, taskExecutor);
    }

    private boolean isTransient(InetAddressAndPort ep)
    {
        return session.commonRange.transEndpoints.contains(ep);
    }

    private ListenableFuture<List<SyncStat>> standardSyncing(List<TreeResponse> trees)
    {
        List<SyncTask> syncTasks = createStandardSyncTasks(desc,
                                                           trees,
                                                           FBUtilities.getLocalAddressAndPort(),
                                                           this::isTransient,
                                                           session.isIncremental,
                                                           session.pullRepair,
                                                           session.previewKind);
        return executeTasks(syncTasks);
    }

    static List<SyncTask> createStandardSyncTasks(RepairJobDesc desc,
                                                  List<TreeResponse> trees,
                                                  InetAddressAndPort local,
                                                  Predicate<InetAddressAndPort> isTransient,
                                                  boolean isIncremental,
                                                  boolean pullRepair,
                                                  PreviewKind previewKind)
    {
        List<SyncTask> syncTasks = new ArrayList<>();
        // We need to difference all trees one against another
        for (int i = 0; i < trees.size() - 1; ++i)
        {
            TreeResponse r1 = trees.get(i);
            for (int j = i + 1; j < trees.size(); ++j)
            {
                TreeResponse r2 = trees.get(j);

                // Avoid streming between two tansient replicas
                if (isTransient.test(r1.endpoint) && isTransient.test(r2.endpoint))
                    continue;

                List<Range<Token>> differences = MerkleTrees.difference(r1.trees, r2.trees);

                // Nothing to do
                if (differences.isEmpty())
                    continue;

                SyncTask task;
                if (r1.endpoint.equals(local) || r2.endpoint.equals(local))
                {
                    TreeResponse self = r1.endpoint.equals(local) ? r1 : r2;
                    TreeResponse remote = r2.endpoint.equals(local) ? r1 : r2;

                    // pull only if local is full
                    boolean requestRanges = !isTransient.test(self.endpoint);
                    // push only if remote is full; additionally check for pull repair
                    boolean transferRanges = !isTransient.test(remote.endpoint) && !pullRepair;

                    // Nothing to do
                    if (!requestRanges && !transferRanges)
                        continue;

                    task = new LocalSyncTask(desc, self.endpoint, remote.endpoint, differences, isIncremental ? desc.parentSessionId : null,
                                             requestRanges, transferRanges, previewKind);
                }
                else if (isTransient.test(r1.endpoint) || isTransient.test(r2.endpoint))
                {
                    // Stream only from transient replica
                    TreeResponse streamFrom = isTransient.test(r1.endpoint) ? r1 : r2;
                    TreeResponse streamTo = isTransient.test(r1.endpoint) ? r2 : r1;
                    task = new AsymmetricRemoteSyncTask(desc, streamTo.endpoint, streamFrom.endpoint, differences, previewKind);
                }
                else
                {
                    task = new SymmetricRemoteSyncTask(desc, r1.endpoint, r2.endpoint, differences, previewKind);
                }
                syncTasks.add(task);
            }
        }
        return syncTasks;
    }

    private ListenableFuture<List<SyncStat>> optimisedSyncing(List<TreeResponse> trees)
    {
        List<SyncTask> syncTasks = createOptimisedSyncingSyncTasks(desc,
                                                                   trees,
                                                                   FBUtilities.getLocalAddressAndPort(),
                                                                   this::isTransient,
                                                                   this::getDC,
                                                                   session.isIncremental,
                                                                   session.previewKind);

        return executeTasks(syncTasks);
    }

    @VisibleForTesting
    ListenableFuture<List<SyncStat>> executeTasks(List<SyncTask> syncTasks)
    {
        for (SyncTask task : syncTasks)
        {
            if (!task.isLocal())
                session.trackSyncCompletion(Pair.create(desc, task.nodePair()), (CompletableRemoteSyncTask) task);
            taskExecutor.submit(task);
        }

        return Futures.allAsList(syncTasks);
    }

    static List<SyncTask> createOptimisedSyncingSyncTasks(RepairJobDesc desc,
                                                          List<TreeResponse> trees,
                                                          InetAddressAndPort local,
                                                          Predicate<InetAddressAndPort> isTransient,
                                                          Function<InetAddressAndPort, String> getDC,
                                                          boolean isIncremental,
                                                          PreviewKind previewKind)
    {
        List<SyncTask> syncTasks = new ArrayList<>();
        // We need to difference all trees one against another
        DifferenceHolder diffHolder = new DifferenceHolder(trees);

        logger.debug("diffs = {}", diffHolder);
        PreferedNodeFilter preferSameDCFilter = (streaming, candidates) ->
                                                candidates.stream()
                                                          .filter(node -> getDC.apply(streaming)
                                                                          .equals(getDC.apply(node)))
                                                          .collect(Collectors.toSet());
        ImmutableMap<InetAddressAndPort, HostDifferences> reducedDifferences = ReduceHelper.reduce(diffHolder, preferSameDCFilter);

        for (int i = 0; i < trees.size(); i++)
        {
            InetAddressAndPort address = trees.get(i).endpoint;

            // we don't stream to transient replicas
            if (isTransient.test(address))
                continue;

            HostDifferences streamsFor = reducedDifferences.get(address);
            if (streamsFor != null)
            {
                Preconditions.checkArgument(streamsFor.get(address).isEmpty(), "We should not fetch ranges from ourselves");
                for (InetAddressAndPort fetchFrom : streamsFor.hosts())
                {
                    List<Range<Token>> toFetch = streamsFor.get(fetchFrom);
                    assert !toFetch.isEmpty();

                    logger.debug("{} is about to fetch {} from {}", address, toFetch, fetchFrom);
                    SyncTask task;
                    if (address.equals(local))
                    {
                        task = new LocalSyncTask(desc, address, fetchFrom, toFetch, isIncremental ? desc.parentSessionId : null,
                                                 true, false, previewKind);
                    }
                    else
                    {
                        task = new AsymmetricRemoteSyncTask(desc, address, fetchFrom, toFetch, previewKind);
                    }
                    syncTasks.add(task);

                }
            }
            else
            {
                logger.debug("Node {} has nothing to stream", address);
            }
        }
        return syncTasks;
    }

    private String getDC(InetAddressAndPort address)
    {
        return DatabaseDescriptor.getEndpointSnitch().getDatacenter(address);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor in parallel.
     *
     * @param endpoints Endpoint addresses to send validation request
     * @return Future that can get all {@link TreeResponse} from replica, if all validation succeed.
     */
    private ListenableFuture<List<TreeResponse>> sendValidationRequest(Collection<InetAddressAndPort> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", session.previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());
        for (InetAddressAndPort endpoint : endpoints)
        {
            ValidationTask task = new ValidationTask(desc, endpoint, nowInSec, session.previewKind);
            tasks.add(task);
            session.trackValidationCompletion(Pair.create(desc, endpoint), task);
            taskExecutor.execute(task);
        }
        return Futures.allAsList(tasks);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially.
     */
    private ListenableFuture<List<TreeResponse>> sendSequentialValidationRequest(Collection<InetAddressAndPort> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", session.previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Queue<InetAddressAndPort> requests = new LinkedList<>(endpoints);
        InetAddressAndPort address = requests.poll();
        ValidationTask firstTask = new ValidationTask(desc, address, nowInSec, session.previewKind);
        logger.info("Validating {}", address);
        session.trackValidationCompletion(Pair.create(desc, address), firstTask);
        tasks.add(firstTask);
        ValidationTask currentTask = firstTask;
        while (requests.size() > 0)
        {
            final InetAddressAndPort nextAddress = requests.poll();
            final ValidationTask nextTask = new ValidationTask(desc, nextAddress, nowInSec, session.previewKind);
            tasks.add(nextTask);
            Futures.addCallback(currentTask, new FutureCallback<TreeResponse>()
            {
                public void onSuccess(TreeResponse result)
                {
                    logger.info("Validating {}", nextAddress);
                    session.trackValidationCompletion(Pair.create(desc, nextAddress), nextTask);
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
    private ListenableFuture<List<TreeResponse>> sendDCAwareValidationRequest(Collection<InetAddressAndPort> endpoints)
    {
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", session.previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = FBUtilities.nowInSeconds();
        List<ListenableFuture<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Map<String, Queue<InetAddressAndPort>> requestsByDatacenter = new HashMap<>();
        for (InetAddressAndPort endpoint : endpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            Queue<InetAddressAndPort> queue = requestsByDatacenter.get(dc);
            if (queue == null)
            {
                queue = new LinkedList<>();
                requestsByDatacenter.put(dc, queue);
            }
            queue.add(endpoint);
        }

        for (Map.Entry<String, Queue<InetAddressAndPort>> entry : requestsByDatacenter.entrySet())
        {
            Queue<InetAddressAndPort> requests = entry.getValue();
            InetAddressAndPort address = requests.poll();
            ValidationTask firstTask = new ValidationTask(desc, address, nowInSec, session.previewKind);
            logger.info("Validating {}", address);
            session.trackValidationCompletion(Pair.create(desc, address), firstTask);
            tasks.add(firstTask);
            ValidationTask currentTask = firstTask;
            while (requests.size() > 0)
            {
                final InetAddressAndPort nextAddress = requests.poll();
                final ValidationTask nextTask = new ValidationTask(desc, nextAddress, nowInSec, session.previewKind);
                tasks.add(nextTask);
                Futures.addCallback(currentTask, new FutureCallback<TreeResponse>()
                {
                    public void onSuccess(TreeResponse result)
                    {
                        logger.info("Validating {}", nextAddress);
                        session.trackValidationCompletion(Pair.create(desc, nextAddress), nextTask);
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