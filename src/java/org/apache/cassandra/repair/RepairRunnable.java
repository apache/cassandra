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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.JMXConfigurableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
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
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

public class RepairRunnable extends WrappedRunnable implements ProgressEventNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(RepairRunnable.class);

    private final StorageService storageService;
    private final int cmd;
    private final RepairOption options;
    private final String keyspace;

    private final String tag;
    private final AtomicInteger progress = new AtomicInteger();
    private final int totalProgress;

    private final List<ProgressListener> listeners = new ArrayList<>();

    private static final AtomicInteger threadCounter = new AtomicInteger(1);

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

    protected void fireProgressEvent(String tag, ProgressEvent event)
    {
        for (ProgressListener listener : listeners)
        {
            listener.progress(tag, event);
        }
    }

    protected void fireErrorAndComplete(String tag, int progressCount, int totalProgress, String message)
    {
        fireProgressEvent(tag, new ProgressEvent(ProgressEventType.ERROR, progressCount, totalProgress, message));
        fireProgressEvent(tag, new ProgressEvent(ProgressEventType.COMPLETE, progressCount, totalProgress, String.format("Repair command #%d finished with error", cmd)));
    }

    protected void runMayThrow() throws Exception
    {
        final TraceState traceState;
        final UUID parentSession = UUIDGen.getTimeUUID();
        final String tag = "repair:" + cmd;

        final AtomicInteger progress = new AtomicInteger();
        final int totalProgress = 4 + options.getRanges().size(); // get valid column families, calculate neighbors, validation, prepare for repair + number of ranges to repair

        String[] columnFamilies = options.getColumnFamilies().toArray(new String[options.getColumnFamilies().size()]);
        Iterable<ColumnFamilyStore> validColumnFamilies;
        try
        {
            validColumnFamilies = storageService.getValidColumnFamilies(false, false, keyspace, columnFamilies);
            progress.incrementAndGet();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Repair failed:", e);
            fireErrorAndComplete(tag, progress.get(), totalProgress, e.getMessage());
            return;
        }

        final long startTime = System.currentTimeMillis();
        String message = String.format("Starting repair command #%d (%s), repairing keyspace %s with %s", cmd, parentSession, keyspace,
                                       options);
        logger.info(message);
        if (options.isTraced())
        {
            StringBuilder cfsb = new StringBuilder();
            for (ColumnFamilyStore cfs : validColumnFamilies)
                cfsb.append(", ").append(cfs.keyspace.getName()).append(".").append(cfs.name);

            UUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
            traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", keyspace, "columnFamilies",
                                                                          cfsb.substring(2)));
            message = message + " tracing with " + sessionId;
            fireProgressEvent(tag, new ProgressEvent(ProgressEventType.START, 0, 100, message));
            Tracing.traceRepair(message);
            traceState.enableActivityNotification(tag);
            for (ProgressListener listener : listeners)
                traceState.addProgressListener(listener);
            Thread queryThread = createQueryThread(cmd, sessionId);
            queryThread.setName("RepairTracePolling");
            queryThread.start();
        }
        else
        {
            fireProgressEvent(tag, new ProgressEvent(ProgressEventType.START, 0, 100, message));
            traceState = null;
        }

        final Set<InetAddress> allNeighbors = new HashSet<>();
        List<Pair<Set<InetAddress>, ? extends Collection<Range<Token>>>> commonRanges = new ArrayList<>();

        //pre-calculate output of getLocalRanges and pass it to getNeighbors to increase performance and prevent
        //calculation multiple times
        Collection<Range<Token>> keyspaceLocalRanges = storageService.getLocalRanges(keyspace);

        try
        {
            for (Range<Token> range : options.getRanges())
            {
                Set<InetAddress> neighbors = ActiveRepairService.getNeighbors(keyspace, keyspaceLocalRanges, range,
                                                                              options.getDataCenters(),
                                                                              options.getHosts());

                addRangeToNeighbors(commonRanges, range, neighbors);
                allNeighbors.addAll(neighbors);
            }

            progress.incrementAndGet();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Repair failed:", e);
            fireErrorAndComplete(tag, progress.get(), totalProgress, e.getMessage());
            return;
        }

        // Validate columnfamilies
        List<ColumnFamilyStore> columnFamilyStores = new ArrayList<>();
        try
        {
            Iterables.addAll(columnFamilyStores, validColumnFamilies);
            progress.incrementAndGet();
        }
        catch (IllegalArgumentException e)
        {
            fireErrorAndComplete(tag, progress.get(), totalProgress, e.getMessage());
            return;
        }

        String[] cfnames = new String[columnFamilyStores.size()];
        for (int i = 0; i < columnFamilyStores.size(); i++)
        {
            cfnames[i] = columnFamilyStores.get(i).name;
        }

        if (!options.isPreview())
        {
            SystemDistributedKeyspace.startParentRepair(parentSession, keyspace, cfnames, options);
        }

        long repairedAt;
        try (Timer.Context ctx = Keyspace.open(keyspace).metric.repairPrepareTime.time())
        {
            ActiveRepairService.instance.prepareForRepair(parentSession, FBUtilities.getBroadcastAddress(), allNeighbors, options, columnFamilyStores);
            repairedAt = ActiveRepairService.instance.getParentRepairSession(parentSession).getRepairedAt();
            progress.incrementAndGet();
        }
        catch (Throwable t)
        {
            if (!options.isPreview())
            {
                SystemDistributedKeyspace.failParentRepair(parentSession, t);
            }
            fireErrorAndComplete(tag, progress.get(), totalProgress, t.getMessage());
            return;
        }

        if (options.isPreview())
        {
            previewRepair(parentSession, repairedAt, startTime, traceState, allNeighbors, commonRanges, cfnames);
        }
        else if (options.isIncremental())
        {
            consistentRepair(parentSession, repairedAt, startTime, traceState, allNeighbors, commonRanges, cfnames);
        }
        else
        {
            normalRepair(parentSession, startTime, traceState, allNeighbors, commonRanges, cfnames);
        }
    }

    private void normalRepair(UUID parentSession,
                              long startTime,
                              TraceState traceState,
                              Set<InetAddress> allNeighbors,
                              List<Pair<Set<InetAddress>, ? extends Collection<Range<Token>>>> commonRanges,
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
        ListenableFuture repairResult = Futures.transform(allSessions, new AsyncFunction<List<RepairSessionResult>, Object>()
        {
            @SuppressWarnings("unchecked")
            public ListenableFuture apply(List<RepairSessionResult> results)
            {
                // filter out null(=failed) results and get successful ranges
                for (RepairSessionResult sessionResult : results)
                {
                    if (sessionResult != null)
                    {
                        successfulRanges.addAll(sessionResult.ranges);
                    }
                    else
                    {
                        hasFailure.compareAndSet(false, true);
                    }
                }
                return Futures.immediateFuture(null);
            }
        });
        Futures.addCallback(repairResult, new RepairCompleteCallback(parentSession, successfulRanges, startTime, traceState, hasFailure, executor));
    }

    private void consistentRepair(UUID parentSession,
                                  long repairedAt,
                                  long startTime,
                                  TraceState traceState,
                                  Set<InetAddress> allNeighbors,
                                  List<Pair<Set<InetAddress>, ? extends Collection<Range<Token>>>> commonRanges,
                                  String... cfnames)
    {
        // the local node also needs to be included in the set of
        // participants, since coordinator sessions aren't persisted
        Set<InetAddress> allParticipants = new HashSet<>(allNeighbors);
        allParticipants.add(FBUtilities.getBroadcastAddress());

        CoordinatorSession coordinatorSession = ActiveRepairService.instance.consistent.coordinated.registerSession(parentSession, allParticipants);
        ListeningExecutorService executor = createExecutor();
        AtomicBoolean hasFailure = new AtomicBoolean(false);
        ListenableFuture repairResult = coordinatorSession.execute(executor,
                                                                   () -> submitRepairSessions(parentSession, true, executor, commonRanges, cfnames),
                                                                   hasFailure);
        Collection<Range<Token>> ranges = new HashSet<>();
        for (Collection<Range<Token>> range : Iterables.transform(commonRanges, cr -> cr.right))
        {
            ranges.addAll(range);
        }
        Futures.addCallback(repairResult, new RepairCompleteCallback(parentSession, ranges, startTime, traceState, hasFailure, executor));
    }

    private void previewRepair(UUID parentSession,
                               long repairedAt,
                               long startTime,
                               TraceState traceState,
                               Set<InetAddress> allNeighbors,
                               List<Pair<Set<InetAddress>, ? extends Collection<Range<Token>>>> commonRanges,
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
                    PreviewKind previewKind = options.getPreviewKind();
                    assert previewKind != PreviewKind.NONE;
                    SyncStatSummary summary = new SyncStatSummary(true);
                    summary.consumeSessionResults(results);

                    if (summary.isEmpty())
                    {
                        String message = previewKind == PreviewKind.REPAIRED ? "Repaired data is in sync" : "Previewed data was in sync";
                        logger.info(message);
                        fireProgressEvent(tag, new ProgressEvent(ProgressEventType.NOTIFICATION, progress.get(), totalProgress, message));
                    }
                    else
                    {
                        String message =  (previewKind == PreviewKind.REPAIRED ? "Repaired data is inconsistent\n" : "Preview complete\n") + summary.toString();
                        logger.info(message);
                        fireProgressEvent(tag, new ProgressEvent(ProgressEventType.NOTIFICATION, progress.get(), totalProgress, message));
                    }

                    fireProgressEvent(tag, new ProgressEvent(ProgressEventType.SUCCESS, progress.get(), totalProgress,
                                                             "Repair preview completed successfully"));
                    complete();
                }
                catch (Throwable t)
                {
                    logger.error("Error completing preview repair", t);
                    onFailure(t);
                }
            }

            public void onFailure(Throwable t)
            {
                fireProgressEvent(tag, new ProgressEvent(ProgressEventType.ERROR, progress.get(), totalProgress, t.getMessage()));
                logger.error("Error completing preview repair", t);
                complete();
            }

            private void complete()
            {
                logger.debug("Preview repair {} completed", parentSession);

                String duration = DurationFormatUtils.formatDurationWords(System.currentTimeMillis() - startTime,
                                                                          true, true);
                String message = String.format("Repair preview #%d finished in %s", cmd, duration);
                fireProgressEvent(tag, new ProgressEvent(ProgressEventType.COMPLETE, progress.get(), totalProgress, message));
                executor.shutdownNow();
            }
        });
    }

    private ListenableFuture<List<RepairSessionResult>> submitRepairSessions(UUID parentSession,
                                                                             boolean isConsistent,
                                                                             ListeningExecutorService executor,
                                                                             List<Pair<Set<InetAddress>, ? extends Collection<Range<Token>>>> commonRanges,
                                                                             String... cfnames)
    {
        List<ListenableFuture<RepairSessionResult>> futures = new ArrayList<>(options.getRanges().size());
        for (Pair<Set<InetAddress>, ? extends Collection<Range<Token>>> p : commonRanges)
        {
            RepairSession session = ActiveRepairService.instance.submitRepairSession(parentSession,
                                                                                     p.right,
                                                                                     keyspace,
                                                                                     options.getParallelism(),
                                                                                     p.left,
                                                                                     isConsistent,
                                                                                     options.isPullRepair(),
                                                                                     options.getPreviewKind(),
                                                                                     executor,
                                                                                     cfnames);
            if (session == null)
                continue;
            Futures.addCallback(session, new RepairSessionCallback(session));
            futures.add(session);
        }
        return Futures.successfulAsList(futures);
    }

    private ListeningExecutorService createExecutor()
    {
        return MoreExecutors.listeningDecorator(new JMXConfigurableThreadPoolExecutor(options.getJobThreads(),
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
                                           session.getRanges().toString());
            logger.info(message);
            fireProgressEvent(tag, new ProgressEvent(ProgressEventType.PROGRESS,
                                                     progress.incrementAndGet(),
                                                     totalProgress,
                                                     message));
        }

        public void onFailure(Throwable t)
        {
            String message = String.format("Repair session %s for range %s failed with error %s",
                                           session.getId(), session.getRanges().toString(), t.getMessage());
            logger.error(message, t);
            fireProgressEvent(tag, new ProgressEvent(ProgressEventType.ERROR,
                                                     progress.incrementAndGet(),
                                                     totalProgress,
                                                     message));
        }
    }

    private class RepairCompleteCallback implements FutureCallback<Object>
    {
        final UUID parentSession;
        final Collection<Range<Token>> successfulRanges;
        final long startTime;
        final TraceState traceState;
        final AtomicBoolean hasFailure;
        final ExecutorService executor;

        public RepairCompleteCallback(UUID parentSession,
                                      Collection<Range<Token>> successfulRanges,
                                      long startTime,
                                      TraceState traceState,
                                      AtomicBoolean hasFailure,
                                      ExecutorService executor)
        {
            this.parentSession = parentSession;
            this.successfulRanges = successfulRanges;
            this.startTime = startTime;
            this.traceState = traceState;
            this.hasFailure = hasFailure;
            this.executor = executor;
        }

        public void onSuccess(Object result)
        {
            if (!options.isPreview())
            {
                SystemDistributedKeyspace.successfulParentRepair(parentSession, successfulRanges);
            }
            if (hasFailure.get())
            {
                fireProgressEvent(tag, new ProgressEvent(ProgressEventType.ERROR, progress.get(), totalProgress,
                                                         "Some repair failed"));
            }
            else
            {
                fireProgressEvent(tag, new ProgressEvent(ProgressEventType.SUCCESS, progress.get(), totalProgress,
                                                         "Repair completed successfully"));
            }
            repairComplete();
        }

        public void onFailure(Throwable t)
        {
            fireProgressEvent(tag, new ProgressEvent(ProgressEventType.ERROR, progress.get(), totalProgress, t.getMessage()));
            if (!options.isPreview())
            {
                SystemDistributedKeyspace.failParentRepair(parentSession, t);
            }
            repairComplete();
        }

        private void repairComplete()
        {
            ActiveRepairService.instance.removeParentRepairSession(parentSession);
            long durationMillis = System.currentTimeMillis() - startTime;
            String duration = DurationFormatUtils.formatDurationWords(durationMillis, true, true);
            String message = String.format("Repair command #%d finished in %s", cmd, duration);
            fireProgressEvent(tag, new ProgressEvent(ProgressEventType.COMPLETE, progress.get(), totalProgress, message));
            logger.info(message);
            if (options.isTraced() && traceState != null)
            {
                for (ProgressListener listener : listeners)
                    traceState.removeProgressListener(listener);
                // Because DebuggableThreadPoolExecutor#afterExecute and this callback
                // run in a nondeterministic order (within the same thread), the
                // TraceState may have been nulled out at this point. The TraceState
                // should be traceState, so just set it without bothering to check if it
                // actually was nulled out.
                Tracing.instance.set(traceState);
                Tracing.traceRepair(message);
                Tracing.instance.stopSession();
            }
            executor.shutdownNow();
            Keyspace.open(keyspace).metric.repairTime.update(durationMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void addRangeToNeighbors(List<Pair<Set<InetAddress>, ? extends Collection<Range<Token>>>> neighborRangeList, Range<Token> range, Set<InetAddress> neighbors)
    {
        for (int i = 0; i < neighborRangeList.size(); i++)
        {
            Pair<Set<InetAddress>, ? extends Collection<Range<Token>>> p = neighborRangeList.get(i);

            if (p.left.containsAll(neighbors))
            {
                p.right.add(range);
                return;
            }
        }

        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(range);
        neighborRangeList.add(Pair.create(neighbors, ranges));
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

                String format = "select event_id, source, activity from %s.%s where session_id = ? and event_id > ? and event_id < ?;";
                String query = String.format(format, SchemaConstants.TRACE_KEYSPACE_NAME, TraceKeyspace.EVENTS);
                SelectStatement statement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;

                ByteBuffer sessionIdBytes = ByteBufferUtil.bytes(sessionId);
                InetAddress source = FBUtilities.getBroadcastAddress();

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
                        if (source.equals(r.getInetAddress("source")))
                            continue;
                        if ((uuid = r.getUUID("event_id")).timestamp() > (tcur - 1000) * 10000)
                            seen[si].add(uuid);
                        if (seen[si == 0 ? 1 : 0].contains(uuid))
                            continue;
                        String message = String.format("%s: %s", r.getInetAddress("source"), r.getString("activity"));
                        fireProgressEvent("repair:" + cmd,
                                          new ProgressEvent(ProgressEventType.NOTIFICATION, 0, 0, message));
                    }
                    tlast = tcur;

                    si = si == 0 ? 1 : 0;
                    seen[si].clear();
                }
            }
        }, "Repair-Runnable-" + threadCounter.incrementAndGet());
    }
}
