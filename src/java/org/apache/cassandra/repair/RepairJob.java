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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.Function;
import java.util.stream.Collectors;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.*;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.repair.state.JobState;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
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
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanup;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.config.DatabaseDescriptor.paxosRepairEnabled;
import static org.apache.cassandra.service.paxos.Paxos.useV2;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * RepairJob runs repair on given ColumnFamily.
 */
public class RepairJob extends AsyncFuture<RepairResult> implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(RepairJob.class);

    public final JobState state;
    private final RepairJobDesc desc;
    private final RepairSession session;
    private final RepairParallelism parallelismDegree;
    private final ExecutorPlus taskExecutor;

    @VisibleForTesting
    final List<ValidationTask> validationTasks = new CopyOnWriteArrayList<>();

    @VisibleForTesting
    final List<SyncTask> syncTasks = new CopyOnWriteArrayList<>();

    /**
     * Create repair job to run on specific columnfamily
     *  @param session RepairSession that this RepairJob belongs
     * @param columnFamily name of the ColumnFamily to repair
     */
    public RepairJob(RepairSession session, String columnFamily)
    {
        this.session = session;
        this.taskExecutor = session.taskExecutor;
        this.parallelismDegree = session.parallelismDegree;
        this.desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(), session.state.keyspace, columnFamily, session.state.commonRange.ranges);
        this.state = new JobState(desc, session.state.commonRange.endpoints);
    }

    public int getNowInSeconds()
    {
        int nowInSeconds = FBUtilities.nowInSeconds();
        if (session.previewKind == PreviewKind.REPAIRED)
        {
            return nowInSeconds + DatabaseDescriptor.getValidationPreviewPurgeHeadStartInSec();
        }
        else
        {
            return nowInSeconds;
        }
    }

    /**
     * Runs repair job.
     * <p/>
     * This sets up necessary task and runs them on given {@code taskExecutor}.
     * After submitting all tasks, waits until validation with replica completes.
     */
    public void run()
    {
        state.phase.start();
        Keyspace ks = Keyspace.open(desc.keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(desc.columnFamily);
        cfs.metric.repairsStarted.inc();
        List<InetAddressAndPort> allEndpoints = new ArrayList<>(session.state.commonRange.endpoints);
        allEndpoints.add(FBUtilities.getBroadcastAddressAndPort());

        Future<Void> paxosRepair;
        if (paxosRepairEnabled() && ((useV2() && session.repairPaxos) || session.paxosOnly))
        {
            logger.info("{} {}.{} starting paxos repair", session.previewKind.logPrefix(session.getId()), desc.keyspace, desc.columnFamily);
            TableMetadata metadata = Schema.instance.getTableMetadata(desc.keyspace, desc.columnFamily);
            paxosRepair = PaxosCleanup.cleanup(allEndpoints, metadata, desc.ranges, session.state.commonRange.hasSkippedReplicas, taskExecutor);
        }
        else
        {
            logger.info("{} {}.{} not running paxos repair", session.previewKind.logPrefix(session.getId()), desc.keyspace, desc.columnFamily);
            paxosRepair = ImmediateFuture.success(null);
        }

        if (session.paxosOnly)
        {
            paxosRepair.addCallback(new FutureCallback<Void>()
            {
                public void onSuccess(Void v)
                {
                    logger.info("{} {}.{} paxos repair completed", session.previewKind.logPrefix(session.getId()), desc.keyspace, desc.columnFamily);
                    trySuccess(new RepairResult(desc, Collections.emptyList()));
                }

                /**
                 * Snapshot, validation and sync failures are all handled here
                 */
                public void onFailure(Throwable t)
                {
                    logger.warn("{} {}.{} paxos repair failed", session.previewKind.logPrefix(session.getId()), desc.keyspace, desc.columnFamily);
                    tryFailure(t);
                }
            }, taskExecutor);
            return;
        }

        // Create a snapshot at all nodes unless we're using pure parallel repairs
        final Future<?> allSnapshotTasks;
        if (parallelismDegree != RepairParallelism.PARALLEL)
        {
            if (session.isIncremental)
            {
                // consistent repair does it's own "snapshotting"
                allSnapshotTasks = paxosRepair.map(input -> allEndpoints);
            }
            else
            {
                // Request snapshot to all replica
                allSnapshotTasks = paxosRepair.flatMap(input -> {
                    List<Future<InetAddressAndPort>> snapshotTasks = new ArrayList<>(allEndpoints.size());
                    state.phase.snapshotsSubmitted();
                    for (InetAddressAndPort endpoint : allEndpoints)
                    {
                        SnapshotTask snapshotTask = new SnapshotTask(desc, endpoint);
                        snapshotTasks.add(snapshotTask);
                        taskExecutor.execute(snapshotTask);
                    }
                    return FutureCombiner.allOf(snapshotTasks).map(a -> {
                        state.phase.snapshotsCompleted();
                        return a;
                    });
                });
            }
        }
        else
        {
            allSnapshotTasks = null;
        }

        // Run validations and the creation of sync tasks in the scheduler, so it can limit the number of Merkle trees
        // that there are in memory at once. When all validations complete, submit sync tasks out of the scheduler.
        Future<List<SyncStat>> syncResults = session.validationScheduler.schedule(() -> createSyncTasks(paxosRepair, allSnapshotTasks, allEndpoints), taskExecutor)
                                                                        .flatMap(this::executeTasks, taskExecutor);

        // When all sync complete, set the final result
        syncResults.addCallback(new FutureCallback<List<SyncStat>>()
        {
            @Override
            public void onSuccess(List<SyncStat> stats)
            {
                state.phase.success();
                if (!session.previewKind.isPreview())
                {
                    logger.info("{} {}.{} is fully synced", session.previewKind.logPrefix(session.getId()), desc.keyspace, desc.columnFamily);
                    SystemDistributedKeyspace.successfulRepairJob(session.getId(), desc.keyspace, desc.columnFamily);
                }
                cfs.metric.repairsCompleted.inc();
                trySuccess(new RepairResult(desc, stats));
            }

            /**
             * Snapshot, validation and sync failures are all handled here
             */
            @Override
            public void onFailure(Throwable t)
            {
                state.phase.fail(t);
                // Make sure all validation tasks have cleaned up the off-heap Merkle trees they might contain.
                validationTasks.forEach(ValidationTask::abort);
                syncTasks.forEach(SyncTask::abort);

                if (!session.previewKind.isPreview())
                {
                    logger.warn("{} {}.{} sync failed", session.previewKind.logPrefix(session.getId()), desc.keyspace, desc.columnFamily);
                    SystemDistributedKeyspace.failedRepairJob(session.getId(), desc.keyspace, desc.columnFamily, t);
                }
                cfs.metric.repairsCompleted.inc();
                tryFailure(t instanceof NoSuchRepairSessionExceptionWrapper
                           ? ((NoSuchRepairSessionExceptionWrapper) t).wrapped
                           : t);
            }
        }, taskExecutor);
    }

    private Future<List<SyncTask>> createSyncTasks(Future<Void> paxosRepair, Future<?> allSnapshotTasks, List<InetAddressAndPort> allEndpoints)
    {
        Future<List<TreeResponse>> treeResponses;
        if (allSnapshotTasks != null)
        {
            // When all snapshot complete, send validation requests
            treeResponses = allSnapshotTasks.flatMap(endpoints -> {
                if (parallelismDegree == RepairParallelism.SEQUENTIAL)
                    return sendSequentialValidationRequest(allEndpoints);
                else
                    return sendDCAwareValidationRequest(allEndpoints);
            }, taskExecutor);
        }
        else
        {
            // If not sequential, just send validation request to all replica
            treeResponses = paxosRepair.flatMap(input -> sendValidationRequest(allEndpoints));
        }

        treeResponses = treeResponses.map(a -> {
            state.phase.validationCompleted();
            return a;
        });

        return treeResponses.map(session.optimiseStreams && !session.pullRepair
                                 ? this::createOptimisedSyncingSyncTasks
                                 : this::createStandardSyncTasks, taskExecutor);
    }

    private boolean isTransient(InetAddressAndPort ep)
    {
        return session.state.commonRange.transEndpoints.contains(ep);
    }

    private List<SyncTask> createStandardSyncTasks(List<TreeResponse> trees)
    {
        return createStandardSyncTasks(desc,
                                       trees,
                                       FBUtilities.getLocalAddressAndPort(),
                                       this::isTransient,
                                       session.isIncremental,
                                       session.pullRepair,
                                       session.previewKind);
    }

    @VisibleForTesting
    static List<SyncTask> createStandardSyncTasks(RepairJobDesc desc,
                                                  List<TreeResponse> trees,
                                                  InetAddressAndPort local,
                                                  Predicate<InetAddressAndPort> isTransient,
                                                  boolean isIncremental,
                                                  boolean pullRepair,
                                                  PreviewKind previewKind)
    {
        long startedAt = currentTimeMillis();
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
            trees.get(i).trees.release();
        }
        trees.get(trees.size() - 1).trees.release();
        logger.info("Created {} sync tasks based on {} merkle tree responses for {} (took: {}ms)",
                    syncTasks.size(), trees.size(), desc.parentSessionId, currentTimeMillis() - startedAt);
        return syncTasks;
    }

    @VisibleForTesting
    Future<List<SyncStat>> executeTasks(List<SyncTask> tasks)
    {
        try
        {
            ActiveRepairService.instance.getParentRepairSession(desc.parentSessionId);
            syncTasks.addAll(tasks);

            for (SyncTask task : tasks)
            {
                if (!task.isLocal())
                    session.trackSyncCompletion(Pair.create(desc, task.nodePair()), (CompletableRemoteSyncTask) task);
                taskExecutor.execute(task);
            }

            return FutureCombiner.allOf(tasks);
        }
        catch (NoSuchRepairSessionException e)
        {
            return ImmediateFuture.failure(new NoSuchRepairSessionExceptionWrapper(e));
        }
    }

    // provided so we can throw NoSuchRepairSessionException from executeTasks without
    // having to make it unchecked. Required as this is called as from standardSyncing/
    // optimisedSyncing passed as a Function to transform merkle tree responses and so
    // can't throw checked exceptions. These are unwrapped in the onFailure callback of
    // that transformation so as to not pollute the checked usage of
    // NoSuchRepairSessionException in the rest of the codebase.
    private static class NoSuchRepairSessionExceptionWrapper extends RuntimeException
    {
        private final NoSuchRepairSessionException wrapped;
        private NoSuchRepairSessionExceptionWrapper(NoSuchRepairSessionException wrapped)
        {
            this.wrapped = wrapped;
        }
    }

    private List<SyncTask> createOptimisedSyncingSyncTasks(List<TreeResponse> trees)
    {
        return createOptimisedSyncingSyncTasks(desc,
                                               trees,
                                               FBUtilities.getLocalAddressAndPort(),
                                               this::isTransient,
                                               this::getDC,
                                               session.isIncremental,
                                               session.previewKind);
    }

    static List<SyncTask> createOptimisedSyncingSyncTasks(RepairJobDesc desc,
                                                          List<TreeResponse> trees,
                                                          InetAddressAndPort local,
                                                          Predicate<InetAddressAndPort> isTransient,
                                                          Function<InetAddressAndPort, String> getDC,
                                                          boolean isIncremental,
                                                          PreviewKind previewKind)
    {
        long startedAt = currentTimeMillis();
        List<SyncTask> syncTasks = new ArrayList<>();
        // We need to difference all trees one against another
        DifferenceHolder diffHolder = new DifferenceHolder(trees);

        logger.trace("diffs = {}", diffHolder);
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
                    List<Range<Token>> toFetch = new ArrayList<>(streamsFor.get(fetchFrom));
                    assert !toFetch.isEmpty();

                    logger.trace("{} is about to fetch {} from {}", address, toFetch, fetchFrom);
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
                logger.trace("Node {} has nothing to stream", address);
            }
        }
        logger.info("Created {} optimised sync tasks based on {} merkle tree responses for {} (took: {}ms)",
                    syncTasks.size(), trees.size(), desc.parentSessionId, currentTimeMillis() - startedAt);
        logger.trace("Optimised sync tasks for {}: {}", desc.parentSessionId, syncTasks);
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
    private Future<List<TreeResponse>> sendValidationRequest(Collection<InetAddressAndPort> endpoints)
    {
        state.phase.validationSubmitted();
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", session.previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = getNowInSeconds();
        List<Future<TreeResponse>> tasks = new ArrayList<>(endpoints.size());
        for (InetAddressAndPort endpoint : endpoints)
        {
            ValidationTask task = newValidationTask(endpoint, nowInSec);
            tasks.add(task);
            session.trackValidationCompletion(Pair.create(desc, endpoint), task);
            taskExecutor.execute(task);
        }
        return FutureCombiner.allOf(tasks);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially.
     */
    private Future<List<TreeResponse>> sendSequentialValidationRequest(Collection<InetAddressAndPort> endpoints)
    {
        state.phase.validationSubmitted();
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", session.previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = getNowInSeconds();
        List<Future<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Queue<InetAddressAndPort> requests = new LinkedList<>(endpoints);
        InetAddressAndPort address = requests.poll();
        ValidationTask firstTask = newValidationTask(address, nowInSec);
        logger.info("{} Validating {}", session.previewKind.logPrefix(desc.sessionId), address);
        session.trackValidationCompletion(Pair.create(desc, address), firstTask);
        tasks.add(firstTask);
        ValidationTask currentTask = firstTask;
        while (requests.size() > 0)
        {
            final InetAddressAndPort nextAddress = requests.poll();
            final ValidationTask nextTask = newValidationTask(nextAddress, nowInSec);
            tasks.add(nextTask);
            currentTask.addCallback(new FutureCallback<TreeResponse>()
            {
                public void onSuccess(TreeResponse result)
                {
                    logger.info("{} Validating {}", session.previewKind.logPrefix(desc.sessionId), nextAddress);
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
        return FutureCombiner.allOf(tasks);
    }

    /**
     * Creates {@link ValidationTask} and submit them to task executor so that tasks run sequentially within each dc.
     */
    private Future<List<TreeResponse>> sendDCAwareValidationRequest(Collection<InetAddressAndPort> endpoints)
    {
        state.phase.validationSubmitted();
        String message = String.format("Requesting merkle trees for %s (to %s)", desc.columnFamily, endpoints);
        logger.info("{} {}", session.previewKind.logPrefix(desc.sessionId), message);
        Tracing.traceRepair(message);
        int nowInSec = getNowInSeconds();
        List<Future<TreeResponse>> tasks = new ArrayList<>(endpoints.size());

        Map<String, Queue<InetAddressAndPort>> requestsByDatacenter = new HashMap<>();
        for (InetAddressAndPort endpoint : endpoints)
        {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(endpoint);
            Queue<InetAddressAndPort> queue = requestsByDatacenter.computeIfAbsent(dc, k -> new LinkedList<>());
            queue.add(endpoint);
        }

        for (Map.Entry<String, Queue<InetAddressAndPort>> entry : requestsByDatacenter.entrySet())
        {
            Queue<InetAddressAndPort> requests = entry.getValue();
            InetAddressAndPort address = requests.poll();
            ValidationTask firstTask = newValidationTask(address, nowInSec);
            logger.info("{} Validating {}", session.previewKind.logPrefix(session.getId()), address);
            session.trackValidationCompletion(Pair.create(desc, address), firstTask);
            tasks.add(firstTask);
            ValidationTask currentTask = firstTask;
            while (requests.size() > 0)
            {
                final InetAddressAndPort nextAddress = requests.poll();
                final ValidationTask nextTask = newValidationTask(nextAddress, nowInSec);
                tasks.add(nextTask);
                currentTask.addCallback(new FutureCallback<TreeResponse>()
                {
                    public void onSuccess(TreeResponse result)
                    {
                        logger.info("{} Validating {}", session.previewKind.logPrefix(session.getId()), nextAddress);
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
        return FutureCombiner.allOf(tasks);
    }

    private ValidationTask newValidationTask(InetAddressAndPort endpoint, int nowInSec)
    {
        ValidationTask task = new ValidationTask(desc, endpoint, nowInSec, session.previewKind);
        validationTasks.add(task);
        return task;
    }
}
