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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.RepairMetrics;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.repair.consistent.SyncStatSummary;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.repair.consistent.CoordinatorSession;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

public class RepairRunnable implements Runnable, ProgressEventNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(RepairRunnable.class);

    private final StorageService storageService;
    private final int cmd;
    private final RepairOption options;
    private final String keyspace;

    private final String tag;
    private final AtomicInteger progressCounter = new AtomicInteger();
    private final int totalProgress;

    private final long creationTimeMillis = System.currentTimeMillis();
    private final UUID parentSession = UUIDGen.getTimeUUID();

    private final List<ProgressListener> listeners = new ArrayList<>();

    private static final AtomicInteger threadCounter = new AtomicInteger(1);
    private final AtomicReference<Throwable> firstError = new AtomicReference<>(null);

    private TraceState traceState;

    public RepairRunnable(StorageService storageService, int cmd, RepairOption options, String keyspace)
    {
        this.storageService = storageService;
        this.cmd = cmd;
        this.options = options;
        this.keyspace = keyspace;

        this.tag = "repair:" + cmd;
        // get valid column families, calculate neighbors, validation, prepare for repair + number of ranges to repair
        this.totalProgress = 4 + options.getRanges().size();
    }

    @Override
    public void addProgressListener(ProgressListener listener)
    {
        listeners.add(listener);
    }

    @Override
    public void removeProgressListener(ProgressListener listener)
    {
        listeners.remove(listener);
    }


    protected void fireProgressEvent(ProgressEvent event)
    {
        for (ProgressListener listener : listeners)
        {
            listener.progress(tag, event);
        }
    }

    public void notification(String msg)
    {
        logger.info(msg);
        fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, progressCounter.get(), totalProgress, msg));
    }

    private void skip(String msg)
    {
        notification("Repair " + parentSession + " skipped: " + msg);
        success(msg);
    }

    private void success(String msg)
    {
        fireProgressEvent(new ProgressEvent(ProgressEventType.SUCCESS, progressCounter.get(), totalProgress, msg));
        ActiveRepairService.instance.recordRepairStatus(cmd, ActiveRepairService.ParentRepairStatus.COMPLETED,
                                                        ImmutableList.of(msg));
        complete(null);
    }

    public void notifyError(Throwable error)
    {
        // exception should be ignored
        if (error instanceof SomeRepairFailedException)
            return;
        logger.error("Repair {} failed:", parentSession, error);

        StorageMetrics.repairExceptions.inc();
        String errorMessage = String.format("Repair command #%d failed with error %s", cmd, error.getMessage());
        fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progressCounter.get(), totalProgress, errorMessage));
        firstError.compareAndSet(null, error);

        // since this can fail, update table only after updating in-memory and notification state
        maybeStoreParentRepairFailure(error);
    }

    private void fail(String reason)
    {
        if (reason == null)
        {
            Throwable error = firstError.get();
            reason = error != null ? error.getMessage() : "Some repair failed";
        }
        String completionMessage = String.format("Repair command #%d finished with error", cmd);

        // Note we rely on the first message being the reason for the failure
        // when inspecting this state from RepairRunner.queryForCompletedRepair
        ActiveRepairService.instance.recordRepairStatus(cmd, ParentRepairStatus.FAILED,
                                                        ImmutableList.of(reason, completionMessage));

        complete(completionMessage);
    }

    private void complete(String msg)
    {
        long durationMillis = System.currentTimeMillis() - creationTimeMillis;
        if (msg == null)
        {
            String duration = DurationFormatUtils.formatDurationWords(durationMillis, true, true);
            msg = String.format("Repair command #%d finished in %s", cmd, duration);
        }

        fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progressCounter.get(), totalProgress, msg));
        logger.info(options.getPreviewKind().logPrefix(parentSession) + msg);

        ActiveRepairService.instance.removeParentRepairSession(parentSession);
        TraceState localState = traceState;
        if (options.isTraced() && localState != null)
        {
            for (ProgressListener listener : listeners)
                localState.removeProgressListener(listener);
            // Because DebuggableThreadPoolExecutor#afterExecute and this callback
            // run in a nondeterministic order (within the same thread), the
            // TraceState may have been nulled out at this point. The TraceState
            // should be traceState, so just set it without bothering to check if it
            // actually was nulled out.
            Tracing.instance.set(localState);
            Tracing.traceRepair(msg);
            Tracing.instance.stopSession();
        }

        Keyspace.open(keyspace).metric.repairTime.update(durationMillis, TimeUnit.MILLISECONDS);
    }

    public void run()
    {
        try
        {
            runMayThrow();
        }
        catch (SkipRepairException e)
        {
            skip(e.getMessage());
        }
        catch (Exception | Error e)
        {
            notifyError(e);
            fail(e.getMessage());
        }
    }

    private void runMayThrow() throws Exception
    {
        ActiveRepairService.instance.recordRepairStatus(cmd, ParentRepairStatus.IN_PROGRESS, ImmutableList.of());

        List<ColumnFamilyStore> columnFamilies = getColumnFamilies();
        String[] cfnames = columnFamilies.stream().map(cfs -> cfs.name).toArray(String[]::new);

        this.traceState = maybeCreateTraceState(columnFamilies);

        notifyStarting();

        NeighborsAndRanges neighborsAndRanges = getNeighborsAndRanges();

        maybeStoreParentRepairStart(cfnames);

        prepare(columnFamilies, neighborsAndRanges.participants, neighborsAndRanges.shouldExcludeDeadParticipants);

        repair(cfnames, neighborsAndRanges);
    }

    private List<ColumnFamilyStore> getColumnFamilies() throws IOException
    {
        String[] columnFamilies = options.getColumnFamilies().toArray(new String[options.getColumnFamilies().size()]);
        Iterable<ColumnFamilyStore> validColumnFamilies = storageService.getValidColumnFamilies(false, false, keyspace, columnFamilies);
        progressCounter.incrementAndGet();

        if (Iterables.isEmpty(validColumnFamilies))
            throw new SkipRepairException(String.format("%s Empty keyspace, skipping repair: %s", parentSession, keyspace));
        return Lists.newArrayList(validColumnFamilies);
    }

    private TraceState maybeCreateTraceState(Iterable<ColumnFamilyStore> columnFamilyStores)
    {
        if (!options.isTraced())
            return null;

        StringBuilder cfsb = new StringBuilder();
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfsb.append(", ").append(cfs.keyspace.getName()).append(".").append(cfs.name);

        UUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
        TraceState traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", keyspace, "columnFamilies",
                                                                                 cfsb.substring(2)));
        traceState.enableActivityNotification(tag);
        for (ProgressListener listener : listeners)
            traceState.addProgressListener(listener);
        Thread queryThread = createQueryThread(cmd, sessionId);
        queryThread.setName("RepairTracePolling");
        queryThread.start();
        return traceState;
    }

    private void notifyStarting()
    {
        String message = String.format("Starting repair command #%d (%s), repairing keyspace %s with %s", cmd, parentSession, keyspace,
                                       options);
        logger.info(message);
        Tracing.traceRepair(message);
        fireProgressEvent(new ProgressEvent(ProgressEventType.START, 0, 100, message));
    }

    private NeighborsAndRanges getNeighborsAndRanges()
    {
        Set<InetAddressAndPort> allNeighbors = new HashSet<>();
        List<CommonRange> commonRanges = new ArrayList<>();

        //pre-calculate output of getLocalReplicas and pass it to getNeighbors to increase performance and prevent
        //calculation multiple times
        Iterable<Range<Token>> keyspaceLocalRanges = storageService.getLocalReplicas(keyspace).ranges();

        for (Range<Token> range : options.getRanges())
        {
            EndpointsForRange neighbors = ActiveRepairService.getNeighbors(keyspace, keyspaceLocalRanges, range,
                                                                           options.getDataCenters(),
                                                                           options.getHosts());
            // local RF = 1 or given range is not part of local range, neighbors would be empty.
            if (neighbors.isEmpty())
            {
                if (options.ignoreUnreplicatedKeyspaces())
                {
                    logger.info("{} Found no neighbors for range {} for {} - ignoring since repairing with --ignore-unreplicated-keyspaces", parentSession, range, keyspace);
                    continue;
                }
                else
                {
                    throw new RuntimeException(String.format("Nothing to repair for %s in %s - aborting", range, keyspace));
                }
            }
            addRangeToNeighbors(commonRanges, range, neighbors);
            allNeighbors.addAll(neighbors.endpoints());
        }

        if (options.ignoreUnreplicatedKeyspaces() && allNeighbors.isEmpty())
        {
            throw new SkipRepairException(String.format("Nothing to repair for %s in %s - unreplicated keyspace is ignored since repair was called with --ignore-unreplicated-keyspaces",
                                                        options.getRanges(),
                                                        keyspace));
        }

        progressCounter.incrementAndGet();

        boolean shouldExcludeDeadParticipants = options.isForcedRepair();

        if (shouldExcludeDeadParticipants)
        {
            Set<InetAddressAndPort> actualNeighbors = Sets.newHashSet(Iterables.filter(allNeighbors, FailureDetector.instance::isAlive));
            shouldExcludeDeadParticipants = !allNeighbors.equals(actualNeighbors);
            allNeighbors = actualNeighbors;
        }
        return new NeighborsAndRanges(shouldExcludeDeadParticipants, allNeighbors, commonRanges);
    }

    private void maybeStoreParentRepairStart(String[] cfnames)
    {
        if (!options.isPreview())
        {
            SystemDistributedKeyspace.startParentRepair(parentSession, keyspace, cfnames, options);
        }
    }

    private void maybeStoreParentRepairSuccess(Collection<Range<Token>> successfulRanges)
    {
        if (!options.isPreview())
        {
            SystemDistributedKeyspace.successfulParentRepair(parentSession, successfulRanges);
        }
    }

    private void maybeStoreParentRepairFailure(Throwable error)
    {
        if (!options.isPreview())
        {
            SystemDistributedKeyspace.failParentRepair(parentSession, error);
        }
    }

    private void prepare(List<ColumnFamilyStore> columnFamilies, Set<InetAddressAndPort> allNeighbors, boolean force)
    {
        try (Timer.Context ignore = Keyspace.open(keyspace).metric.repairPrepareTime.time())
        {
            ActiveRepairService.instance.prepareForRepair(parentSession, FBUtilities.getBroadcastAddressAndPort(), allNeighbors, options, force, columnFamilies);
            progressCounter.incrementAndGet();
        }
    }

    private void repair(String[] cfnames, NeighborsAndRanges neighborsAndRanges)
    {
        if (options.isPreview())
        {
            previewRepair(parentSession,
                          creationTimeMillis,
                          neighborsAndRanges.filterCommonRanges(keyspace, cfnames),
                          neighborsAndRanges.participants,
                          cfnames);
        }
        else if (options.isIncremental())
        {
            incrementalRepair(parentSession,
                              creationTimeMillis,
                              traceState,
                              neighborsAndRanges,
                              neighborsAndRanges.participants,
                              cfnames);
        }
        else
        {
            normalRepair(parentSession,
                         creationTimeMillis,
                         traceState,
                         neighborsAndRanges.filterCommonRanges(keyspace, cfnames),
                         neighborsAndRanges.participants,
                         cfnames);
        }
    }

    private void normalRepair(UUID parentSession,
                              long startTime,
                              TraceState traceState,
                              List<CommonRange> commonRanges,
                              Set<InetAddressAndPort> preparedEndpoints,
                              String... cfnames)
    {

        // Set up RepairJob executor for this repair command.
        ListeningExecutorService executor = createExecutor();

        // Setting the repairedAt time to UNREPAIRED_SSTABLE causes the repairedAt times to be preserved across streamed sstables
        final ListenableFuture<List<RepairSessionResult>> allSessions = submitRepairSessions(parentSession, false, executor, commonRanges, cfnames);

        // After all repair sessions completes(successful or not),
        // run anticompaction if necessary and send finish notice back to client
        final Collection<Range<Token>> successfulRanges = new ArrayList<>();
        final AtomicBoolean hasFailure = new AtomicBoolean();
        ListenableFuture repairResult = Futures.transformAsync(allSessions, new AsyncFunction<List<RepairSessionResult>, Object>()
        {
            @SuppressWarnings("unchecked")
            public ListenableFuture apply(List<RepairSessionResult> results)
            {
                logger.debug("Repair result: {}", results);
                // filter out null(=failed) results and get successful ranges
                for (RepairSessionResult sessionResult : results)
                {
                    if (sessionResult != null)
                    {
                        // don't record successful repair if we had to skip ranges
                        if (!sessionResult.skippedReplicas)
                        {
                            successfulRanges.addAll(sessionResult.ranges);
                        }
                    }
                    else
                    {
                        hasFailure.compareAndSet(false, true);
                    }
                }
                return Futures.immediateFuture(null);
            }
        }, MoreExecutors.directExecutor());
        Futures.addCallback(repairResult,
                            new RepairCompleteCallback(parentSession,
                                                       successfulRanges,
                                                       preparedEndpoints,
                                                       startTime,
                                                       traceState,
                                                       hasFailure,
                                                       executor),
                            MoreExecutors.directExecutor());
    }

    private void incrementalRepair(UUID parentSession,
                                   long startTime,
                                   TraceState traceState,
                                   NeighborsAndRanges neighborsAndRanges,
                                   Set<InetAddressAndPort> preparedEndpoints,
                                   String... cfnames)
    {
        // the local node also needs to be included in the set of participants, since coordinator sessions aren't persisted
        Set<InetAddressAndPort> allParticipants = ImmutableSet.<InetAddressAndPort>builder()
                                                  .addAll(neighborsAndRanges.participants)
                                                  .add(FBUtilities.getBroadcastAddressAndPort())
                                                  .build();
        // Not necessary to include self for filtering. The common ranges only contains neighbhor node endpoints.
        List<CommonRange> allRanges = neighborsAndRanges.filterCommonRanges(keyspace, cfnames);

        CoordinatorSession coordinatorSession = ActiveRepairService.instance.consistent.coordinated.registerSession(parentSession, allParticipants, neighborsAndRanges.shouldExcludeDeadParticipants);
        ListeningExecutorService executor = createExecutor();
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        ListenableFuture repairResult = coordinatorSession.execute(() -> submitRepairSessions(parentSession, true, executor, allRanges, cfnames),
                                                                   hasFailure);
        Collection<Range<Token>> ranges = new HashSet<>();
        for (Collection<Range<Token>> range : Iterables.transform(allRanges, cr -> cr.ranges))
        {
            ranges.addAll(range);
        }
        Futures.addCallback(repairResult,
                            new RepairCompleteCallback(parentSession, ranges, preparedEndpoints, startTime, traceState, hasFailure, executor),
                            MoreExecutors.directExecutor());
    }

    private void previewRepair(UUID parentSession,
                               long startTime,
                               List<CommonRange> commonRanges,
                               Set<InetAddressAndPort> preparedEndpoints,
                               String... cfnames)
    {

        logger.debug("Starting preview repair for {}", parentSession);
        // Set up RepairJob executor for this repair command.
        ListeningExecutorService executor = createExecutor();

        final ListenableFuture<List<RepairSessionResult>> allSessions = submitRepairSessions(parentSession, false, executor, commonRanges, cfnames);

        Futures.addCallback(allSessions, new FutureCallback<List<RepairSessionResult>>()
        {
            public void onSuccess(List<RepairSessionResult> results)
            {
                try
                {
                    if (results == null || results.stream().anyMatch(s -> s == null))
                    {
                        // something failed
                        fail(null);
                        return;
                    }
                    PreviewKind previewKind = options.getPreviewKind();
                    Preconditions.checkState(previewKind != PreviewKind.NONE, "Preview is NONE");
                    SyncStatSummary summary = new SyncStatSummary(true);
                    summary.consumeSessionResults(results);

                    final String message;
                    if (summary.isEmpty())
                    {
                        message = previewKind == PreviewKind.REPAIRED ? "Repaired data is in sync" : "Previewed data was in sync";
                    }
                    else
                    {
                        message = (previewKind == PreviewKind.REPAIRED ? "Repaired data is inconsistent\n" : "Preview complete\n") + summary.toString();
                        RepairMetrics.previewFailures.inc();
                        if (previewKind == PreviewKind.REPAIRED)
                            maybeSnapshotReplicas(parentSession, keyspace, results);
                    }
                    notification(message);

                    success("Repair preview completed successfully");
                    ActiveRepairService.instance.cleanUp(parentSession, preparedEndpoints);
                }
                catch (Throwable t)
                {
                    logger.error("Error completing preview repair", t);
                    onFailure(t);
                }
                finally
                {
                    executor.shutdownNow();
                }
            }

            public void onFailure(Throwable t)
            {
                notifyError(t);
                fail("Error completing preview repair: " + t.getMessage());
                executor.shutdownNow();
            }
        }, MoreExecutors.directExecutor());
    }

    private void maybeSnapshotReplicas(UUID parentSession, String keyspace, List<RepairSessionResult> results)
    {
        if (!DatabaseDescriptor.snapshotOnRepairedDataMismatch())
            return;

        try
        {
            Set<String> mismatchingTables = new HashSet<>();
            Set<InetAddressAndPort> nodes = new HashSet<>();
            for (RepairSessionResult sessionResult : results)
            {
                for (RepairResult repairResult : emptyIfNull(sessionResult.repairJobResults))
                {
                    for (SyncStat stat : emptyIfNull(repairResult.stats))
                    {
                        if (stat.numberOfDifferences > 0)
                            mismatchingTables.add(repairResult.desc.columnFamily);
                        // snapshot all replicas, even if they don't have any differences
                        nodes.add(stat.nodes.coordinator);
                        nodes.add(stat.nodes.peer);
                    }
                }
            }

            String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);
            for (String table : mismatchingTables)
            {
                // we can just check snapshot existence locally since the repair coordinator is always a replica (unlike in the read case)
                if (!Keyspace.open(keyspace).getColumnFamilyStore(table).snapshotExists(snapshotName))
                {
                    logger.info("{} Snapshotting {}.{} for preview repair mismatch with tag {} on instances {}",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName, nodes);
                    DiagnosticSnapshotService.repairedDataMismatch(Keyspace.open(keyspace).getColumnFamilyStore(table).metadata(), nodes);
                }
                else
                {
                    logger.info("{} Not snapshotting {}.{} - snapshot {} exists",
                                options.getPreviewKind().logPrefix(parentSession),
                                keyspace, table, snapshotName);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("{} Failed snapshotting replicas", options.getPreviewKind().logPrefix(parentSession), e);
        }
    }

    private static <T> Iterable<T> emptyIfNull(Iterable<T> iter)
    {
        if (iter == null)
            return Collections.emptyList();
        return iter;
    }

    private ListenableFuture<List<RepairSessionResult>> submitRepairSessions(UUID parentSession,
                                                                             boolean isIncremental,
                                                                             ListeningExecutorService executor,
                                                                             List<CommonRange> commonRanges,
                                                                             String... cfnames)
    {
        List<ListenableFuture<RepairSessionResult>> futures = new ArrayList<>(options.getRanges().size());

        for (CommonRange commonRange : commonRanges)
        {
            logger.info("Starting RepairSession for {}", commonRange);
            RepairSession session = ActiveRepairService.instance.submitRepairSession(parentSession,
                                                                                     commonRange,
                                                                                     keyspace,
                                                                                     options.getParallelism(),
                                                                                     isIncremental,
                                                                                     options.isPullRepair(),
                                                                                     options.getPreviewKind(),
                                                                                     options.optimiseStreams(),
                                                                                     executor,
                                                                                     cfnames);
            if (session == null)
                continue;
            Futures.addCallback(session, new RepairSessionCallback(session), MoreExecutors.directExecutor());
            futures.add(session);
        }
        return Futures.successfulAsList(futures);
    }

    private ListeningExecutorService createExecutor()
    {
        return MoreExecutors.listeningDecorator(new JMXEnabledThreadPoolExecutor(options.getJobThreads(),
                                                                                 Integer.MAX_VALUE,
                                                                                 TimeUnit.SECONDS,
                                                                                 new LinkedBlockingQueue<>(),
                                                                                 new NamedThreadFactory("Repair#" + cmd),
                                                                                 "internal"));
    }

    private class RepairSessionCallback implements FutureCallback<RepairSessionResult>
    {
        private final RepairSession session;

        public RepairSessionCallback(RepairSession session)
        {
            this.session = session;
        }

        public void onSuccess(RepairSessionResult result)
        {
            String message = String.format("Repair session %s for range %s finished", session.getId(),
                                           session.ranges().toString());
            logger.info(message);
            fireProgressEvent(new ProgressEvent(ProgressEventType.PROGRESS,
                                                progressCounter.incrementAndGet(),
                                                totalProgress,
                                                message));
        }

        public void onFailure(Throwable t)
        {
            String message = String.format("Repair session %s for range %s failed with error %s",
                                           session.getId(), session.ranges().toString(), t.getMessage());
            notifyError(new RuntimeException(message, t));
        }
    }

    private class RepairCompleteCallback implements FutureCallback<Object>
    {
        final UUID parentSession;
        final Collection<Range<Token>> successfulRanges;
        final Set<InetAddressAndPort> preparedEndpoints;
        final long startTime;
        final TraceState traceState;
        final AtomicBoolean hasFailure;
        final ExecutorService executor;

        public RepairCompleteCallback(UUID parentSession,
                                      Collection<Range<Token>> successfulRanges,
                                      Set<InetAddressAndPort> preparedEndpoints,
                                      long startTime,
                                      TraceState traceState,
                                      AtomicBoolean hasFailure,
                                      ExecutorService executor)
        {
            this.parentSession = parentSession;
            this.successfulRanges = successfulRanges;
            this.preparedEndpoints = preparedEndpoints;
            this.startTime = startTime;
            this.traceState = traceState;
            this.hasFailure = hasFailure;
            this.executor = executor;
        }

        public void onSuccess(Object result)
        {
            maybeStoreParentRepairSuccess(successfulRanges);
            if (hasFailure.get())
            {
                fail(null);
            }
            else
            {
                success("Repair completed successfully");
                ActiveRepairService.instance.cleanUp(parentSession, preparedEndpoints);
            }
            executor.shutdownNow();
        }

        public void onFailure(Throwable t)
        {
            notifyError(t);
            fail(t.getMessage());
            executor.shutdownNow();
        }
    }

    private static void addRangeToNeighbors(List<CommonRange> neighborRangeList, Range<Token> range, EndpointsForRange neighbors)
    {
        Set<InetAddressAndPort> endpoints = neighbors.endpoints();
        Set<InetAddressAndPort> transEndpoints = neighbors.filter(Replica::isTransient).endpoints();

        for (CommonRange commonRange : neighborRangeList)
        {
            if (commonRange.matchesEndpoints(endpoints, transEndpoints))
            {
                commonRange.ranges.add(range);
                return;
            }
        }

        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(range);
        neighborRangeList.add(new CommonRange(endpoints, transEndpoints, ranges));
    }

    private Thread createQueryThread(final int cmd, final UUID sessionId)
    {
        return NamedThreadFactory.createThread(new WrappedRunnable()
        {
            // Query events within a time interval that overlaps the last by one second. Ignore duplicates. Ignore local traces.
            // Wake up upon local trace activity. Query when notified of trace activity with a timeout that doubles every two timeouts.
            public void runMayThrow() throws Exception
            {
                TraceState state = Tracing.instance.get(sessionId);
                if (state == null)
                    throw new Exception("no tracestate");

                String format = "select event_id, source, source_port, activity from %s.%s where session_id = ? and event_id > ? and event_id < ?;";
                String query = String.format(format, SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.EVENTS);
                SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare(ClientState.forInternalCalls());

                ByteBuffer sessionIdBytes = ByteBufferUtil.bytes(sessionId);
                InetAddressAndPort source = FBUtilities.getBroadcastAddressAndPort();

                HashSet<UUID>[] seen = new HashSet[]{ new HashSet<>(), new HashSet<>() };
                int si = 0;
                UUID uuid;

                long tlast = System.currentTimeMillis(), tcur;

                TraceState.Status status;
                long minWaitMillis = 125;
                long maxWaitMillis = 1000 * 1024L;
                long timeout = minWaitMillis;
                boolean shouldDouble = false;

                while ((status = state.waitActivity(timeout)) != TraceState.Status.STOPPED)
                {
                    if (status == TraceState.Status.IDLE)
                    {
                        timeout = shouldDouble ? Math.min(timeout * 2, maxWaitMillis) : timeout;
                        shouldDouble = !shouldDouble;
                    }
                    else
                    {
                        timeout = minWaitMillis;
                        shouldDouble = false;
                    }
                    ByteBuffer tminBytes = ByteBufferUtil.bytes(UUIDGen.minTimeUUID(tlast - 1000));
                    ByteBuffer tmaxBytes = ByteBufferUtil.bytes(UUIDGen.maxTimeUUID(tcur = System.currentTimeMillis()));
                    QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(sessionIdBytes,
                                                                                                                  tminBytes,
                                                                                                                  tmaxBytes));
                    ResultMessage.Rows rows = statement.execute(QueryState.forInternalCalls(), options, System.nanoTime());
                    UntypedResultSet result = UntypedResultSet.create(rows.result);

                    for (UntypedResultSet.Row r : result)
                    {
                        int port = DatabaseDescriptor.getStoragePort();
                        if (r.has("source_port"))
                            port = r.getInt("source_port");
                        InetAddressAndPort eventNode = InetAddressAndPort.getByAddressOverrideDefaults(r.getInetAddress("source"), port);
                        if (source.equals(eventNode))
                            continue;
                        if ((uuid = r.getUUID("event_id")).timestamp() > (tcur - 1000) * 10000)
                            seen[si].add(uuid);
                        if (seen[si == 0 ? 1 : 0].contains(uuid))
                            continue;
                        String message = String.format("%s: %s", r.getInetAddress("source"), r.getString("activity"));
                        notification(message);
                    }
                    tlast = tcur;

                    si = si == 0 ? 1 : 0;
                    seen[si].clear();
                }
            }
        }, "Repair-Runnable-" + threadCounter.incrementAndGet());
    }

    private static final class SkipRepairException extends RuntimeException
    {
        SkipRepairException(String message)
        {
            super(message);
        }
    }

    static final class NeighborsAndRanges
    {
        private final boolean shouldExcludeDeadParticipants;
        private final Set<InetAddressAndPort> participants;
        private final List<CommonRange> commonRanges;

        NeighborsAndRanges(boolean shouldExcludeDeadParticipants, Set<InetAddressAndPort> participants, List<CommonRange> commonRanges)
        {
            this.shouldExcludeDeadParticipants = shouldExcludeDeadParticipants;
            this.participants = participants;
            this.commonRanges = commonRanges;
        }

        /**
         * When in the force mode, removes dead nodes from common ranges (not contained within `allNeighbors`),
         * and exludes ranges left without any participants
         * When not in the force mode, no-op.
         */
        List<CommonRange> filterCommonRanges(String keyspace, String[] tableNames)
        {
            if (!shouldExcludeDeadParticipants)
            {
                return commonRanges;
            }
            else
            {
                logger.debug("force flag set, removing dead endpoints if possible");

                List<CommonRange> filtered = new ArrayList<>(commonRanges.size());

                for (CommonRange commonRange : commonRanges)
                {
                    Set<InetAddressAndPort> endpoints = ImmutableSet.copyOf(Iterables.filter(commonRange.endpoints, participants::contains));
                    Set<InetAddressAndPort> transEndpoints = ImmutableSet.copyOf(Iterables.filter(commonRange.transEndpoints, participants::contains));
                    Preconditions.checkState(endpoints.containsAll(transEndpoints), "transEndpoints must be a subset of endpoints");

                    // this node is implicitly a participant in this repair, so a single endpoint is ok here
                    if (!endpoints.isEmpty())
                    {
                        Set<InetAddressAndPort> skippedReplicas = Sets.difference(commonRange.endpoints, endpoints);
                        skippedReplicas.forEach(endpoint -> logger.info("Removing a dead node {} from repair for ranges {} due to -force", endpoint, commonRange.ranges));
                        filtered.add(new CommonRange(endpoints, transEndpoints, commonRange.ranges, !skippedReplicas.isEmpty()));
                    }
                    else
                    {
                        logger.warn("Skipping forced repair for ranges {} of tables {} in keyspace {}, as no neighbor nodes are live.",
                                    commonRange.ranges, Arrays.asList(tableNames), keyspace);
                    }
                }
                Preconditions.checkState(!filtered.isEmpty(), "Not enough live endpoints for a repair");
                return filtered;
            }
        }
    }
}
