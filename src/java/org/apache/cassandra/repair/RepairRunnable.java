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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.*;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.Replica;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.repair.consistent.SyncStatSummary;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.consistent.CoordinatorSession;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.RepairOption;
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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

public class RepairRunnable implements Runnable, ProgressEventNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(RepairRunnable.class);

    public final RepairState state;
    private final StorageService storageService;
    private final int cmd;

    private final String tag;
    private final AtomicInteger progressCounter = new AtomicInteger();
    private final int totalProgress;

    private final List<ProgressListener> listeners = new ArrayList<>();

    private static final AtomicInteger threadCounter = new AtomicInteger(1);

    // lazy loaded; should only be started/access in run() method
    private ListeningExecutorService executor;

    public RepairRunnable(StorageService storageService, int cmd, RepairOption options, String keyspace)
    {
        this.state = new RepairState(UUIDGen.getTimeUUID(), cmd, keyspace, options);
        this.storageService = storageService;
        this.cmd = cmd;

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

    @Override
    public void run()
    {
        try
        {
            Pair<Tuple, String> started = setup();
            if (started.right != null)
            {
                onSkip(started.right);
                return;
            }
            Tuple tuple = started.left;
            assert tuple != null : "start() returned null state";

            start(tuple);
        }
        catch (Throwable e)
        {
            onError(e);
            JVMStabilityInspector.inspectThrowable(e);
        }
    }

    private Pair<Tuple, String> setup() throws Exception
    {
        onSetup();

        String[] columnFamilies = state.options.getColumnFamilies().toArray(new String[state.options.getColumnFamilies().size()]);
        Iterable<ColumnFamilyStore> validColumnFamilies = storageService.getValidColumnFamilies(false, false, state.keyspace, columnFamilies);
        progressCounter.incrementAndGet();

        if (Iterables.isEmpty(validColumnFamilies))
        {
            return Pair.create(null, String.format("Empty keyspace, skipping repair: %s", state.keyspace));
        }

        final TraceState traceState;
        if (state.options.isTraced())
        {
            StringBuilder cfsb = new StringBuilder();
            for (ColumnFamilyStore cfs : validColumnFamilies)
                cfsb.append(", ").append(cfs.keyspace.getName()).append(".").append(cfs.name);

            UUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
            traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", state.keyspace, "columnFamilies",
                                                                          cfsb.substring(2)));
            traceState.enableActivityNotification(tag);
            for (ProgressListener listener : listeners)
                traceState.addProgressListener(listener);
            Thread queryThread = createQueryThread(cmd, sessionId);
            queryThread.setName("RepairTracePolling");
            queryThread.start();
        }
        else
        {
            traceState = null;
        }

        Set<InetAddressAndPort> allNeighbors = new HashSet<>();
        List<CommonRange> commonRanges = new ArrayList<>();

        //pre-calculate output of getLocalReplicas and pass it to getNeighbors to increase performance and prevent
        //calculation multiple times
        Iterable<Range<Token>> keyspaceLocalRanges = storageService.getLocalReplicas(state.keyspace).ranges();

        for (Range<Token> range : state.options.getRanges())
        {
            EndpointsForRange neighbors = ActiveRepairService.getNeighbors(state.keyspace, keyspaceLocalRanges, range,
                                                                           state.options.getDataCenters(),
                                                                           state.options.getHosts());

            addRangeToNeighbors(commonRanges, range, neighbors);
            allNeighbors.addAll(neighbors.endpoints());
        }

        progressCounter.incrementAndGet();

        // Validate columnfamilies
        List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>();
        Iterables.addAll(columnFamilyStores, validColumnFamilies);
        progressCounter.incrementAndGet();

        String[] cfnames = new String[columnFamilyStores.size()];
        for (int i = 0; i < columnFamilyStores.size(); i++)
        {
            cfnames[i] = columnFamilyStores.get(i).name;
        }

        Tuple tuple = new Tuple(traceState, allNeighbors, commonRanges, columnFamilyStores, cfnames);
        return Pair.create(tuple, null);
    }

    private void start(Tuple tuple) throws Exception
    {
        onStart(tuple);

        @Nullable TraceState traceState = tuple.traceState;
        Set<InetAddressAndPort> allNeighbors = tuple.allNeighbors;
        List<CommonRange> commonRanges = tuple.commonRanges;
        List<ColumnFamilyStore> columnFamilyStores = tuple.columnFamilyStores;
        String[] cfnames = tuple.cfnames;
        long startTimeMillis = state.getStateTimeMillis(RepairState.State.SETUP);

        boolean force = state.options.isForcedRepair();

        if (force && state.options.isIncremental())
        {
            Set<InetAddressAndPort> actualNeighbors = Sets.newHashSet(Iterables.filter(allNeighbors, FailureDetector.instance::isAlive));
            force = !allNeighbors.equals(actualNeighbors);
            allNeighbors = actualNeighbors;
        }

        try (Timer.Context ctx = Keyspace.open(state.keyspace).metric.repairPrepareTime.time())
        {
            state.phasePrepareStart();
            ActiveRepairService.instance.prepareForRepair(state.id, FBUtilities.getBroadcastAddressAndPort(), allNeighbors, state.options, force, columnFamilyStores);
            state.phasePrepareComplete();
            progressCounter.incrementAndGet();
        }


        //TODO can repair return a future and do cleanup there rather than each callbac?
        // Looks possible, RepairCompleteCallback is incrementla and normal, and that is defining success; preview could do the same
        // so may be much cleaner to only have repair work on happy case and this caller attach a listener for failures
        this.executor = createExecutor();
        final ListenableFuture<String> repairFutures;
        if (state.options.isPreview())
        {
            repairFutures = previewRepair(state.id, startTimeMillis, commonRanges, cfnames);
        }
        else if (state.options.isIncremental())
        {
            repairFutures = incrementalRepair(state.id, startTimeMillis, force, traceState, allNeighbors, commonRanges, cfnames);
        }
        else
        {
            repairFutures = normalRepair(state.id, startTimeMillis, traceState, commonRanges, cfnames);
        }

        Futures.addCallback(repairFutures, new FutureCallback<String>()
        {
            public void onSuccess(String s)
            {
                RepairRunnable.this.onSuccess(s);
            }

            public void onFailure(Throwable throwable)
            {
                onError(throwable);
            }
        }, MoreExecutors.directExecutor());
    }

    private ListenableFuture<String> normalRepair(UUID parentSession,
                                                  long startTime,
                                                  TraceState traceState,
                                                  List<CommonRange> commonRanges,
                                                  String... cfnames)
    {
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
                state.phaseSessionsCompleted();
                // filter out null(=failed) results and get successful ranges
                for (RepairSessionResult sessionResult : results)
                {
                    logger.debug("Repair result: {}", results);
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
        return Futures.transform(repairResult, new RepairCompleteCallback(parentSession, successfulRanges, startTime, traceState, hasFailure, executor), MoreExecutors.directExecutor());
    }

    /**
     * removes dead nodes from common ranges, and exludes ranges left without any participants
     */
    @VisibleForTesting
    static List<CommonRange> filterCommonRanges(List<CommonRange> commonRanges, Set<InetAddressAndPort> liveEndpoints, boolean force)
    {
        if (!force)
        {
            return commonRanges;
        }
        else
        {
            List<CommonRange> filtered = new ArrayList<>(commonRanges.size());

            for (CommonRange commonRange: commonRanges)
            {
                Set<InetAddressAndPort> endpoints = ImmutableSet.copyOf(Iterables.filter(commonRange.endpoints, liveEndpoints::contains));
                Set<InetAddressAndPort> transEndpoints = ImmutableSet.copyOf(Iterables.filter(commonRange.transEndpoints, liveEndpoints::contains));
                Preconditions.checkState(endpoints.containsAll(transEndpoints), "transEndpoints must be a subset of endpoints");

                // this node is implicitly a participant in this repair, so a single endpoint is ok here
                if (!endpoints.isEmpty())
                {
                    filtered.add(new CommonRange(endpoints, transEndpoints, commonRange.ranges));
                }
            }
            Preconditions.checkState(!filtered.isEmpty(), "Not enough live endpoints for a repair");
            return filtered;
        }
    }

    private ListenableFuture<String> incrementalRepair(UUID parentSession,
                                                       long startTime,
                                                       boolean forceRepair,
                                                       TraceState traceState,
                                                       Set<InetAddressAndPort> allNeighbors,
                                                       List<CommonRange> commonRanges,
                                                       String... cfnames)
    {
        // the local node also needs to be included in the set of participants, since coordinator sessions aren't persisted
        Set<InetAddressAndPort> allParticipants = ImmutableSet.<InetAddressAndPort>builder()
                                           .addAll(allNeighbors)
                                           .add(FBUtilities.getBroadcastAddressAndPort())
                                           .build();

        List<CommonRange> allRanges = filterCommonRanges(commonRanges, allParticipants, forceRepair);

        CoordinatorSession coordinatorSession = ActiveRepairService.instance.consistent.coordinated.registerSession(parentSession, allParticipants, forceRepair);
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        ListenableFuture repairResult = coordinatorSession.execute(() -> submitRepairSessions(parentSession, true, executor, allRanges, cfnames),
                                                                   hasFailure);
        Collection<Range<Token>> ranges = new HashSet<>();
        for (Collection<Range<Token>> range : Iterables.transform(allRanges, cr -> cr.ranges))
        {
            ranges.addAll(range);
        }
        return Futures.transform(repairResult, new RepairCompleteCallback(parentSession, ranges, startTime, traceState, hasFailure, executor), MoreExecutors.directExecutor());
    }

    private ListenableFuture<String> previewRepair(UUID parentSession,
                                                   long startTime,
                                                   List<CommonRange> commonRanges,
                                                   String... cfnames)
    {

        logger.debug("Starting preview repair for {}", parentSession);
        // Set up RepairJob executor for this repair command.

        final ListenableFuture<List<RepairSessionResult>> allSessions = submitRepairSessions(parentSession, false, executor, commonRanges, cfnames);
        return Futures.transform(allSessions, results -> {
            state.phaseSessionsCompleted();
            PreviewKind previewKind = state.options.getPreviewKind();
            assert previewKind != PreviewKind.NONE;
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
            }

            logger.info(message);
            RepairRunnable.this.notify(message);
            String duration = DurationFormatUtils.formatDurationWords(System.currentTimeMillis() - startTime,
                                                                      true, true);
            return String.format("Repair preview #%d finished in %s", cmd, duration);
        }, MoreExecutors.directExecutor());
    }

    private ListenableFuture<List<RepairSessionResult>> submitRepairSessions(UUID parentSession,
                                                                             boolean isIncremental,
                                                                             ListeningExecutorService executor,
                                                                             List<CommonRange> commonRanges,
                                                                             String... cfnames)
    {
        state.phasSessionsSubmitted();
        List<ListenableFuture<RepairSessionResult>> futures = new ArrayList<>(state.options.getRanges().size());

        // we do endpoint filtering at the start of an incremental repair,
        // so repair sessions shouldn't also be checking liveness
        boolean force = state.options.isForcedRepair() && !isIncremental;
        for (CommonRange commonRange : commonRanges)
        {
            logger.info("Starting RepairSession for {}", commonRange);
            RepairSession session = ActiveRepairService.instance.submitRepairSession(state, commonRange, executor);
            if (session == null)
                continue;
            Futures.addCallback(session, new RepairSessionCallback(session), MoreExecutors.directExecutor());
            futures.add(session);
        }
        return Futures.successfulAsList(futures);
    }

    private ListeningExecutorService createExecutor()
    {
        return MoreExecutors.listeningDecorator(new JMXConfigurableThreadPoolExecutor(state.options.getJobThreads(),
                                                                                      Integer.MAX_VALUE,
                                                                                      TimeUnit.SECONDS,
                                                                                      new LinkedBlockingQueue<>(),
                                                                                      new NamedThreadFactory("Repair#" + cmd),
                                                                                      "internal"));
    }

    private void notify(String message)
    {
        logger.info(message);
        fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, progressCounter.get(), totalProgress, message));
    }

    private void progress(String message)
    {
        logger.info(message);
        fireProgressEvent(new ProgressEvent(ProgressEventType.PROGRESS, progressCounter.incrementAndGet(), totalProgress, message));
    }

    private void onSetup()
    {
        state.phaseSetup();
        ActiveRepairService.instance.recordRepairStatus(cmd, ParentRepairStatus.IN_PROGRESS, ImmutableList.of());
    }

    private void onStart(Tuple tuple)
    {
        state.phaseStart(tuple.cfnames, tuple.commonRanges);
        String message = String.format("Starting repair command #%d (%s), repairing keyspace %s with %s", cmd, state.id, state.keyspace, state.options);
        logger.info(message);
        if (tuple.traceState != null)
        {
            message = message + " tracing with " + tuple.traceState.sessionId;
            Tracing.traceRepair(message); // TODO this is a timing regression; this was done before setup, but now its done on-start
        }
        fireProgressEvent(new ProgressEvent(ProgressEventType.START, 0, 100, message));

        if (!state.options.isPreview())
        {
            SystemDistributedKeyspace.startParentRepair(state.id, state.keyspace, tuple.cfnames, state.options);
        }
    }

    private void onSkip(String reason)
    {
        logger.info("Repair {} skipped: {}", state.id, reason);
        state.skip(reason);
        onComplete(ParentRepairStatus.COMPLETED, reason);
    }

    private void onError(Throwable t)
    {
        onError(t, null);
    }

    private void onError(Throwable t, String reason)
    {
        logger.error("Repair {} failed:", state.id, t);
        state.fail(t);
        if (!state.options.isPreview())
        {
            SystemDistributedKeyspace.failParentRepair(state.id, t);
        }
        else
        {
            logger.error("Error completing preview repair", t); // mostly for backwards comptable logging
        }

        StorageMetrics.repairExceptions.inc();
        int progressCount = progressCounter.get();
        String errorMessage = reason != null ? reason : String.format("Repair command #%d failed with error %s", cmd, t.getMessage());
        fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progressCount, totalProgress, errorMessage));

        onComplete(ParentRepairStatus.FAILED, errorMessage);
    }

    private void onSuccess(String msg)
    {
        state.success();
        logger.debug("Repair {} completed", state.id);
        fireProgressEvent(new ProgressEvent(ProgressEventType.SUCCESS, progressCounter.get(), totalProgress, msg));

        onComplete(ParentRepairStatus.COMPLETED, msg);
    }

    private void onComplete(ParentRepairStatus status, String message)
    {
        fireProgressEvent(new ProgressEvent(ProgressEventType.COMPLETE, progressCounter.get(), totalProgress, message));
        ActiveRepairService.instance.recordRepairStatus(cmd, status, ImmutableList.of(message));
        if (ActiveRepairService.instance.hasParentRepairSession(state.id))
            ActiveRepairService.instance.removeParentRepairSession(state.id);

        //TODO traceState
//        if (state.options.isTraced() && traceState != null)
//        {
//            for (ProgressListener listener : listeners)
//                traceState.removeProgressListener(listener);
//            // Because DebuggableThreadPoolExecutor#afterExecute and this callback
//            // run in a nondeterministic order (within the same thread), the
//            // TraceState may have been nulled out at this point. The TraceState
//            // should be traceState, so just set it without bothering to check if it
//            // actually was nulled out.
//            Tracing.instance.set(traceState);
//            Tracing.traceRepair(message);
//            Tracing.instance.stopSession();
//        }

        if (executor != null)
            executor.shutdownNow();
        executor = null;

        //TODO use progress time
//        long durationMillis = System.currentTimeMillis() - startTime;
//        Keyspace.open(state.keyspace).metric.repairTime.update(durationMillis, TimeUnit.MILLISECONDS);
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
            progress(message);
        }

        public void onFailure(Throwable t)
        {
            onError(t); //TODO will this double count?
        }
    }

    private class RepairCompleteCallback implements Function<Object, String>
    {
        final UUID parentSession;
        final Collection<Range<Token>> successfulRanges;
        final long startTimeMillis;
        final TraceState traceState;
        final AtomicBoolean hasFailure;
        final ExecutorService executor;

        public RepairCompleteCallback(UUID parentSession,
                                      Collection<Range<Token>> successfulRanges,
                                      long startTimeMillis,
                                      TraceState traceState,
                                      AtomicBoolean hasFailure,
                                      ExecutorService executor)
        {
            this.parentSession = parentSession;
            this.successfulRanges = successfulRanges;
            this.startTimeMillis = startTimeMillis;
            this.traceState = traceState;
            this.hasFailure = hasFailure;
            this.executor = executor;
        }

        public String apply(Object o)
        {
            // Update the parent_repair_history table to show the ranges which were successful.
            // There is a assumption this code makes that the successful ranges may be a subset of repair ranges
            // since some may have failed (and should be marked as such with the hasFailure field).
            if (!state.options.isPreview())
            {
                SystemDistributedKeyspace.successfulParentRepair(parentSession, successfulRanges);
            }
            if (hasFailure.get())
            {
                //TODO should get the exception from the session rather than create a new one
                //TODO complete message regression here
                //TODO onError should have been called by session already, so do we need here?
                throw new RuntimeException("Some repair failed");
            }
            else
            {
                long durationMillis = System.currentTimeMillis() - startTimeMillis;
                String duration = DurationFormatUtils.formatDurationWords(durationMillis,true, true);
                return String.format("Repair command #%d finished in %s", cmd, duration);
            }
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

                HashSet<UUID>[] seen = new HashSet[] { new HashSet<>(), new HashSet<>() };
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
                        RepairRunnable.this.notify(message);
                    }
                    tlast = tcur;

                    si = si == 0 ? 1 : 0;
                    seen[si].clear();
                }
            }
        }, "Repair-Runnable-" + threadCounter.incrementAndGet());
    }

    private static final class Tuple
    {
        public final @Nullable TraceState traceState;
        public final Set<InetAddressAndPort> allNeighbors;
        public final List<CommonRange> commonRanges;
        public final List<ColumnFamilyStore> columnFamilyStores;
        public final String[] cfnames;

        private Tuple(@Nullable TraceState traceState,
                      Set<InetAddressAndPort> allNeighbors,
                      List<CommonRange> commonRanges,
                      List<ColumnFamilyStore> columnFamilyStores,
                      String[] cfnames)
        {
            this.traceState = traceState;
            this.allNeighbors = allNeighbors;
            this.commonRanges = commonRanges;
            this.columnFamilyStores = columnFamilyStores;
            this.cfnames = cfnames;
        }
    }
}
