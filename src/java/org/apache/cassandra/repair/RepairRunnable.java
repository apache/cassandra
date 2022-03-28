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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class RepairRunnable implements Runnable, ProgressEventNotifier, RepairNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(RepairRunnable.class);

    private final StorageService storageService;
    private final int cmd;
    private final RepairOption options;
    private final String keyspace;

    private final String tag;
    private final AtomicInteger progressCounter = new AtomicInteger();
    private final int totalProgress;

    private final long creationTimeMillis = currentTimeMillis();
    private final TimeUUID parentSession = nextTimeUUID();

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

    @Override
    public void notification(String msg)
    {
        logger.info(msg);
        fireProgressEvent(new ProgressEvent(ProgressEventType.NOTIFICATION, progressCounter.get(), totalProgress, msg));
    }

    @Override
    public void notifyError(Throwable error)
    {
        // exception should be ignored
        if (error instanceof SomeRepairFailedException)
            return;

        if (Throwables.anyCauseMatches(error, RepairException::shouldWarn))
        {
            logger.warn("Repair {} aborted: {}", parentSession, error.getMessage());
            if (logger.isDebugEnabled())
                logger.debug("Repair {} aborted: ", parentSession, error);
        }
        else
        {
            logger.error("Repair {} failed:", parentSession, error);
        }

        StorageMetrics.repairExceptions.inc();
        String errorMessage = String.format("Repair command #%d failed with error %s", cmd, error.getMessage());
        fireProgressEvent(new ProgressEvent(ProgressEventType.ERROR, progressCounter.get(), totalProgress, errorMessage));
        firstError.compareAndSet(null, error);

        // since this can fail, update table only after updating in-memory and notification state
        maybeStoreParentRepairFailure(error);
    }

    @Override
    public void notifyProgress(String message)
    {
        logger.info(message);
        fireProgressEvent(new ProgressEvent(ProgressEventType.PROGRESS,
                                            progressCounter.incrementAndGet(),
                                            totalProgress,
                                            message));
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
        long durationMillis = currentTimeMillis() - creationTimeMillis;
        if (msg == null)
        {
            String duration = DurationFormatUtils.formatDurationWords(Math.max(0, durationMillis), true, true);
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
            // Because ExecutorPlus#afterExecute and this callback
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

        TimeUUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
        TraceState traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", keyspace, "columnFamilies",
                                                                                 cfsb.substring(2)));
        traceState.enableActivityNotification(tag);
        for (ProgressListener listener : listeners)
            traceState.addProgressListener(listener);
        Thread queryThread = createQueryThread(sessionId);
        queryThread.setName("RepairTracePolling");
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

    private NeighborsAndRanges getNeighborsAndRanges() throws RepairException
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
            if (neighbors.isEmpty())
            {
                if (options.ignoreUnreplicatedKeyspaces())
                {
                    logger.info("{} Found no neighbors for range {} for {} - ignoring since repairing with --ignore-unreplicated-keyspaces", parentSession, range, keyspace);
                    continue;
                }
                else
                {
                    throw RepairException.warn(String.format("Nothing to repair for %s in %s - aborting", range, keyspace));
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
        RepairTask task;
        if (options.isPreview())
        {
            task = new PreviewRepairTask(options, keyspace, this, parentSession, neighborsAndRanges.filterCommonRanges(keyspace, cfnames), cfnames);
        }
        else if (options.isIncremental())
        {
            task = new IncrementalRepairTask(options, keyspace, this, parentSession, neighborsAndRanges, cfnames);
        }
        else
        {
            task = new NormalRepairTask(options, keyspace, this, parentSession, neighborsAndRanges.filterCommonRanges(keyspace, cfnames), cfnames);
        }

        ExecutorPlus executor = createExecutor();
        Future<CoordinatedRepairResult> f = task.perform(executor);
        f.addCallback((result, failure) -> {
            try
            {
                if (failure != null)
                {
                    notifyError(failure);
                    fail(failure.getMessage());
                }
                else
                {
                    maybeStoreParentRepairSuccess(result.successfulRanges);
                    if (result.hasFailed())
                    {
                        fail(null);
                    }
                    else
                    {
                        success(task.name() + " completed successfully");
                        ActiveRepairService.instance.cleanUp(parentSession, neighborsAndRanges.participants);
                    }
                }
            }
            finally
            {
                executor.shutdown();
            }
        });
    }

    private ExecutorPlus createExecutor()
    {
        return executorFactory()
                .localAware()
                .withJmxInternal()
                .pooled("Repair#" + cmd, options.getJobThreads());
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

    private Thread createQueryThread(final TimeUUID sessionId)
    {
        return executorFactory().startThread("Repair-Runnable-" + threadCounter.incrementAndGet(), new WrappedRunnable()
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

                ByteBuffer sessionIdBytes = sessionId.toBytes();
                InetAddressAndPort source = FBUtilities.getBroadcastAddressAndPort();

                HashSet<UUID>[] seen = new HashSet[]{ new HashSet<>(), new HashSet<>() };
                int si = 0;
                UUID uuid;

                long tlast = currentTimeMillis(), tcur;

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
                    ByteBuffer tminBytes = TimeUUID.minAtUnixMillis(tlast - 1000).toBytes();
                    ByteBuffer tmaxBytes = TimeUUID.maxAtUnixMillis(tcur = currentTimeMillis()).toBytes();
                    QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(sessionIdBytes,
                                                                                                                  tminBytes,
                                                                                                                  tmaxBytes));
                    ResultMessage.Rows rows = statement.execute(forInternalCalls(), options, nanoTime());
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
        });
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
        final boolean shouldExcludeDeadParticipants;
        final Set<InetAddressAndPort> participants;
        final List<CommonRange> commonRanges;

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
