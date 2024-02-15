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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.state.CoordinatorState;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ActiveRepairService.ParentRepairStatus;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.ProgressListener;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.repair.state.AbstractState.COMPLETE;
import static org.apache.cassandra.repair.state.AbstractState.INIT;
import static org.apache.cassandra.service.QueryState.forInternalCalls;

public class RepairCoordinator implements Runnable, ProgressEventNotifier, RepairNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(RepairCoordinator.class);

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(1);

    public final CoordinatorState state;
    private final String tag;
    private final BiFunction<String, String[], Iterable<ColumnFamilyStore>> validColumnFamilies;
    private final Function<String, RangesAtEndpoint> getLocalReplicas;

    private final List<ProgressListener> listeners = new ArrayList<>();
    private final AtomicReference<Throwable> firstError = new AtomicReference<>(null);

    public volatile NeighborsAndRanges neighborsAndRanges;

    final SharedContext ctx;
    final Scheduler validationScheduler;

    private TraceState traceState;

    // These will be null before `runMayThrow` populates them
    @Nullable
    private List<ColumnFamilyStore> columnFamilies;
    @Nullable
    private List<String> columnFamilyNames;

    public RepairCoordinator(StorageService storageService, int cmd, RepairOption options, String keyspace)
    {
        this(SharedContext.Global.instance,
                (ks, tables) -> storageService.getValidColumnFamilies(false, false, ks, tables),
                storageService::getLocalReplicas,
                cmd, options, keyspace);
    }

    RepairCoordinator(SharedContext ctx,
                      BiFunction<String, String[], Iterable<ColumnFamilyStore>> validColumnFamilies,
                      Function<String, RangesAtEndpoint> getLocalReplicas,
                      int cmd, RepairOption options, String keyspace)
    {
        this.ctx = ctx;
        this.validationScheduler = Scheduler.build(DatabaseDescriptor.getConcurrentMerkleTreeRequests());
        this.state = new CoordinatorState(ctx.clock(), cmd, keyspace, options);
        this.tag = "repair:" + cmd;
        this.validColumnFamilies = validColumnFamilies;
        this.getLocalReplicas = getLocalReplicas;
        ctx.repair().register(state);
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
        fireProgressEvent(jmxEvent(ProgressEventType.NOTIFICATION, msg));
    }

    @Override
    public void notifyError(Throwable error)
    {
        // exception should be ignored
        if (error instanceof SomeRepairFailedException)
            return;

        if (Throwables.anyCauseMatches(error, RepairException::shouldWarn))
        {
            logger.warn("Repair {} aborted: {}", state.id, error.getMessage());
            if (logger.isDebugEnabled())
                logger.debug("Repair {} aborted: ", state.id, error);
        }
        else
        {
            logger.error("Repair {} failed:", state.id, error);
        }

        StorageMetrics.repairExceptions.inc();
        String errorMessage = String.format("Repair command #%d failed with error %s", state.cmd, error.getMessage());
        fireProgressEvent(jmxEvent(ProgressEventType.ERROR, errorMessage));
        firstError.compareAndSet(null, error);

        // since this can fail, update table only after updating in-memory and notification state
        maybeStoreParentRepairFailure(error);
    }

    @Override
    public void notifyProgress(String message)
    {
        logger.info(message);
        fireProgressEvent(jmxEvent(ProgressEventType.PROGRESS, message));
    }

    private void skip(String msg)
    {
        state.phase.skip(msg);
        notification("Repair " + state.id + " skipped: " + msg);
        success(msg);
    }

    private void success(String msg)
    {
        state.phase.success(msg);
        fireProgressEvent(jmxEvent(ProgressEventType.SUCCESS, msg));
        ctx.repair().recordRepairStatus(state.cmd, ActiveRepairService.ParentRepairStatus.COMPLETED,
                                                        ImmutableList.of(msg));
        complete(null);
    }

    private void fail(String reason)
    {
        if (reason == null)
        {
            Throwable error = firstError.get();
            reason = error != null ? error.toString() : "Some repair failed";
        }
        state.phase.fail(reason);
        String completionMessage = String.format("Repair command #%d finished with error", state.cmd);

        // Note we rely on the first message being the reason for the failure
        // when inspecting this state from RepairRunner.queryForCompletedRepair
        ctx.repair().recordRepairStatus(state.cmd, ParentRepairStatus.FAILED,
                                                        ImmutableList.of(reason, completionMessage));

        complete(completionMessage);
    }

    private void complete(String msg)
    {
        long durationMillis = state.getDurationMillis();
        if (msg == null)
        {
            String duration = DurationFormatUtils.formatDurationWords(durationMillis, true, true);
            msg = String.format("Repair command #%d finished in %s", state.cmd, duration);
        }

        fireProgressEvent(jmxEvent(ProgressEventType.COMPLETE, msg));
        logger.info(state.options.getPreviewKind().logPrefix(state.id) + msg);

        ctx.repair().removeParentRepairSession(state.id);
        TraceState localState = traceState;
        if (state.options.isTraced() && localState != null)
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

        Keyspace.open(state.keyspace).metric.repairTime.update(durationMillis, TimeUnit.MILLISECONDS);
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
        catch (Throwable e)
        {
            notifyError(e);
            fail(e.getMessage());
        }
    }

    private void validate(RepairOption options)
    {
        if (options.paxosOnly() && options.accordOnly())
            throw new IllegalArgumentException("Cannot specify a repair as both paxos only and accord only");

        for (ColumnFamilyStore cfs : columnFamilies)
        {
            TableMetadata metadata = cfs.metadata();
            if (options.paxosOnly() && !metadata.supportsPaxosOperations())
                throw new IllegalArgumentException(String.format("Cannot run paxos only repair on %s.%s, which isn't configured for paxos operations", cfs.keyspace.getName(), cfs.name));

            if (options.accordOnly() && !metadata.requiresAccordSupport())
                throw new IllegalArgumentException(String.format("Cannot run accord only repair on %s.%s, which isn't configured for accord operations", cfs.keyspace.getName(), cfs.name));
        }
    }

    private void runMayThrow() throws Throwable
    {
        state.phase.setup();
        ctx.repair().recordRepairStatus(state.cmd, ParentRepairStatus.IN_PROGRESS, ImmutableList.of());

        populateColumnFamilies();
        validate(state.options);

        this.traceState = maybeCreateTraceState(columnFamilies);
        notifyStarting();
        NeighborsAndRanges neighborsAndRanges = getNeighborsAndRanges();

        // We test to validate the start JMX notification is seen before we compute neighbors and ranges
        // but in state (vtable) tracking, we rely on getNeighborsAndRanges to know where we are running repair...
        // JMX start != state start, its possible we fail in getNeighborsAndRanges and state start is never reached
        state.phase.start(columnFamilies, neighborsAndRanges);

        maybeStoreParentRepairStart();

        prepare()
        .flatMap(ignore -> repair())
        .addCallback((pair, failure) -> {
            if (failure != null)
            {
                notifyError(failure);
                fail(failure.getMessage());
            }
            else
            {
                state.phase.repairCompleted();
                CoordinatedRepairResult result = pair.left;
                maybeStoreParentRepairSuccess(result.successfulRanges);
                if (result.hasFailed())
                {
                    fail(null);
                }
                else
                {
                    success(pair.right.get());
                    ctx.repair().cleanUp(state.id, neighborsAndRanges.participants);
                }
            }
        });
    }

    private void populateColumnFamilies()
    {
        String[] columnFamilyNames = state.options.getColumnFamilies().toArray(new String[state.options.getColumnFamilies().size()]);
        Iterable<ColumnFamilyStore> validColumnFamilies = this.validColumnFamilies.apply(state.keyspace, columnFamilyNames);

        if (Iterables.isEmpty(validColumnFamilies))
            throw new SkipRepairException(String.format("%s Empty keyspace, skipping repair: %s", state.id, state.keyspace));
        columnFamilies = Lists.newArrayList(validColumnFamilies);
        this.columnFamilyNames = columnFamilies.stream().map(cfs -> cfs.name).collect(toImmutableList());
    }

    public @Nullable List<ColumnFamilyStore> getColumnFamilies()
    {
        return columnFamilies;
    }

    public @Nullable List<String> getColumnFamilyNames()
    {
        return columnFamilyNames;
    }

    private TraceState maybeCreateTraceState(Iterable<ColumnFamilyStore> columnFamilyStores)
    {
        if (!state.options.isTraced())
            return null;

        StringBuilder cfsb = new StringBuilder();
        for (ColumnFamilyStore cfs : columnFamilyStores)
            cfsb.append(", ").append(cfs.getKeyspaceName()).append(".").append(cfs.name);

        TimeUUID sessionId = Tracing.instance.newSession(Tracing.TraceType.REPAIR);
        TraceState traceState = Tracing.instance.begin("repair", ImmutableMap.of("keyspace", state.keyspace, "columnFamilies",
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
        String message = String.format("Starting repair command #%d (%s), repairing keyspace %s with %s", state.cmd, state.id, state.keyspace,
                                       state.options);
        logger.info(message);
        Tracing.traceRepair(message);
        fireProgressEvent(jmxEvent(ProgressEventType.START, message));
    }

    // Made this slightly more complicated by memoizing so it is constructed in `runMayThrow` thread
    // but it might be fine to run it in the constructor and convert the exceptions to not be checked?
    // It's also initialized from a singled threaded context, but just to be safe made it thread safe.
    private NeighborsAndRanges getNeighborsAndRanges() throws RepairException
    {
        if (neighborsAndRanges != null)
            return neighborsAndRanges;
        synchronized (this)
        {
            if (neighborsAndRanges != null)
                return neighborsAndRanges;
            Set<InetAddressAndPort> allNeighbors = new HashSet<>();
            List<CommonRange> commonRanges = new ArrayList<>();

            //pre-calculate output of getLocalReplicas and pass it to getNeighbors to increase performance and prevent
            //calculation multiple times
            Iterable<Range<Token>> keyspaceLocalRanges = getLocalReplicas.apply(state.keyspace).ranges();

            for (Range<Token> range : state.options.getRanges())
            {
                EndpointsForRange neighbors = ctx.repair().getNeighbors(state.keyspace, keyspaceLocalRanges, range,
                                                                        state.options.getDataCenters(),
                                                                        state.options.getHosts());
                if (neighbors.isEmpty())
                {
                    if (state.options.ignoreUnreplicatedKeyspaces())
                    {
                        logger.info("{} Found no neighbors for range {} for {} - ignoring since repairing with --ignore-unreplicated-keyspaces", state.id, range, state.keyspace);
                        continue;
                    }
                    else
                    {
                        throw RepairException.warn(String.format("Nothing to repair for %s in %s - aborting", range, state.keyspace));
                    }
                }
                addRangeToNeighbors(commonRanges, range, neighbors);
                allNeighbors.addAll(neighbors.endpoints());
            }

            if (state.options.ignoreUnreplicatedKeyspaces() && allNeighbors.isEmpty())
            {
                throw new SkipRepairException(String.format("Nothing to repair for %s in %s - unreplicated keyspace is ignored since repair was called with --ignore-unreplicated-keyspaces",
                                                            state.options.getRanges(),
                                                            state.keyspace));
            }

            boolean excludedDeadParticipants = state.options.isForcedRepair();

            if (excludedDeadParticipants)
            {
                Set<InetAddressAndPort> actualNeighbors = Sets.newHashSet(Iterables.filter(allNeighbors, ctx.failureDetector()::isAlive));
                excludedDeadParticipants = !allNeighbors.equals(actualNeighbors);
                allNeighbors = actualNeighbors;
            }
            neighborsAndRanges = new NeighborsAndRanges(excludedDeadParticipants, allNeighbors, commonRanges);
        }
        return neighborsAndRanges;
    }

    private void maybeStoreParentRepairStart()
    {
        if (!state.options.isPreview())
        {
            SystemDistributedKeyspace.startParentRepair(state.id, state.keyspace, columnFamilyNames, state.options);
        }
    }

    private void maybeStoreParentRepairSuccess(Collection<Range<Token>> successfulRanges)
    {
        if (!state.options.isPreview())
        {
            SystemDistributedKeyspace.successfulParentRepair(state.id, successfulRanges);
        }
    }

    private void maybeStoreParentRepairFailure(Throwable error)
    {
        if (!state.options.isPreview())
        {
            SystemDistributedKeyspace.failParentRepair(state.id, error);
        }
    }

    private Future<?> prepare()
    {
        state.phase.prepareStart();
        Timer timer = Keyspace.open(state.keyspace).metric.repairPrepareTime;
        long startNanos = ctx.clock().nanoTime();
        return ctx.repair().prepareForRepair(state.id, ctx.broadcastAddressAndPort(), neighborsAndRanges.participants, state.options, neighborsAndRanges.excludedDeadParticipants, columnFamilies)
                  .map(ignore -> {
                      timer.update(ctx.clock().nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                      state.phase.prepareComplete();
                      return null;
                  });
    }

    private Future<Pair<CoordinatedRepairResult, Supplier<String>>> repair()
    {
        RepairTask task;
        if (state.options.isPreview())
        {
            task = new PreviewRepairTask(this);
        }
        else if (state.options.isIncremental())
        {
            task = new IncrementalRepairTask(this);
        }
        else
        {
            task = new NormalRepairTask(this);
        }

        ExecutorPlus executor = createExecutor();
        state.phase.repairSubmitted();
        return task.perform(executor, validationScheduler)
                   // after adding the callback java could no longer infer the type...
                   .<Pair<CoordinatedRepairResult, Supplier<String>>>map(r -> Pair.create(r, task::successMessage))
                   .addCallback((s, f) -> executor.shutdown());
    }

    private ExecutorPlus createExecutor()
    {
        return ctx.executorFactory()
                .localAware()
                .withJmxInternal()
                .pooled("Repair#" + state.cmd, state.options.getJobThreads());
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
        return ctx.executorFactory().startThread("Repair-Runnable-" + THREAD_COUNTER.incrementAndGet(), new WrappedRunnable()
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
                InetAddressAndPort source = ctx.broadcastAddressAndPort();

                HashSet<UUID>[] seen = new HashSet[]{ new HashSet<>(), new HashSet<>() };
                int si = 0;
                UUID uuid;

                long tlast = ctx.clock().currentTimeMillis(), tcur;

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
                    ByteBuffer tmaxBytes = TimeUUID.maxAtUnixMillis(tcur = ctx.clock().currentTimeMillis()).toBytes();
                    QueryOptions options = QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(sessionIdBytes,
                                                                                                                  tminBytes,
                                                                                                                  tmaxBytes));
                    ResultMessage.Rows rows = statement.execute(forInternalCalls(), options, ctx.clock().nanoTime());
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

    private ProgressEvent jmxEvent(ProgressEventType type, String msg)
    {
        int length = CoordinatorState.State.values().length + 1; // +1 to include completed state
        int currentState = state.getCurrentState();
        return new ProgressEvent(type, currentState == INIT ? 0 : currentState == COMPLETE ? length : currentState, length, msg);
    }

    private static final class SkipRepairException extends RuntimeException
    {
        SkipRepairException(String message)
        {
            super(message);
        }
    }

    public static final class NeighborsAndRanges
    {
        final boolean excludedDeadParticipants;
        public final Set<InetAddressAndPort> participants;
        public final List<CommonRange> commonRanges;

        public NeighborsAndRanges(boolean excludedDeadParticipants, Set<InetAddressAndPort> participants, List<CommonRange> commonRanges)
        {
            this.excludedDeadParticipants = excludedDeadParticipants;
            this.participants = participants;
            this.commonRanges = commonRanges;
        }

        /**
         * When in the force mode, removes dead nodes from common ranges (not contained within `allNeighbors`),
         * and exludes ranges left without any participants
         * When not in the force mode, no-op.
         */
        public List<CommonRange> filterCommonRanges(String keyspace, List<String> tableNames)
        {
            if (!excludedDeadParticipants)
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
