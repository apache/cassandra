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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.LocalConfig;
import accord.api.Result;
import accord.coordinate.Barrier;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.Exhausted;
import accord.coordinate.FailureAccumulator;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.TopologyMismatch;
import accord.impl.AbstractConfigurationService;
import accord.impl.CoordinateDurabilityScheduling;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.DefaultRequestTimeouts;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.Command;
import accord.local.CommandStore;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SaveStatus;
import accord.local.ShardDistributor.EvenSplit;
import accord.local.Status;
import accord.local.cfk.CommandsForKey;
import accord.messages.Request;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.api.AccordTopologySorter;
import org.apache.cassandra.service.accord.api.CompositeTopologySorter;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.exceptions.ReadExhaustedException;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.accord.interop.AccordInteropAdapter.AccordInteropFactory;
import org.apache.cassandra.service.accord.repair.RepairSyncPointAdapter;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Blocking;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.messages.SimpleReply.Ok;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.utils.Invariants.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);

    private enum State {INIT, STARTED, SHUTDOWN}

    private static final Future<Void> BOOTSTRAP_SUCCESS = ImmediateFuture.success(null);

    private final Node node;
    private final Shutdownable nodeShutdown;
    private final AccordMessageSink messageSink;
    private final AccordConfigurationService configService;
    private final AccordFastPathCoordinator fastPathCoordinator;
    private final AccordScheduler scheduler;
    private final AccordDataStore dataStore;
    private final AccordJournal journal;
    private final CoordinateDurabilityScheduling durabilityScheduling;
    private final AccordVerbHandler<? extends Request> requestHandler;
    private final LocalConfig configuration;
    @GuardedBy("this")
    private State state = State.INIT;

    private static final IAccordService NOOP_SERVICE = new IAccordService()
    {
        @Override
        public IVerbHandler<? extends Request> verbHandler()
        {
            return null;
        }

        @Override
        public Seekables barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Seekables barrier(@Nonnull Seekables keysOrRanges, long minEpoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
        {
            throw new UnsupportedOperationException("No accord barriers should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public Seekables repair(@Nonnull Seekables keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
        {
            throw new UnsupportedOperationException("No accord repairs should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull AsyncTxnResult coordinateAsync(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public TxnResult getTxnResult(AsyncTxnResult asyncTxnResult, boolean isWrite, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public long currentEpoch()
        {
            throw new UnsupportedOperationException("Cannot return epoch when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public void setCacheSize(long kb) { }

        @Override
        public TopologyManager topology()
        {
            throw new UnsupportedOperationException("Cannot return topology when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public void startup()
        {
            try
            {
                AccordTopologySorter.checkSnitchSupported(DatabaseDescriptor.getEndpointSnitch());
            }
            catch (Throwable t)
            {
                logger.warn("Current snitch  is not compatable with Accord, make sure to fix the snitch before enabling Accord; {}", t.toString());
            }
        }

        @Override
        public void shutdownAndWait(long timeout, TimeUnit unit) { }

        @Override
        public AccordScheduler scheduler()
        {
            return null;
        }

        @Override
        public Future<Void> epochReady(Epoch epoch)
        {
            return BOOTSTRAP_SUCCESS;
        }

        @Override
        public void receive(Message<List<AccordSyncPropagator.Notification>> message) {}

        @Override
        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>(), DurableBefore.EMPTY);
        }

        @Override
        public List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId)
        {
            return Collections.emptyList();
        }
    };

    private static volatile IAccordService instance = null;

    @VisibleForTesting
    public static void unsafeSetNewAccordService()
    {
        instance = null;
    }

    @VisibleForTesting
    public static void unsafeSetNoop()
    {
        instance = NOOP_SERVICE;
    }

    public static boolean isSetup()
    {
        return instance != null;
    }

    public static IVerbHandler<? extends Request> verbHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        return instance().verbHandler();
    }

    public synchronized static void startup(NodeId tcmId)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
        {
            instance = NOOP_SERVICE;
            return;
        }
        AccordService as = new AccordService(AccordTopology.tcmIdToAccord(tcmId));
        as.startup();
        if (StorageService.instance.isReplacingSameAddress())
        {
            // when replacing another node but using the same ip the hostId will also match, this causes no TCM transactions
            // to be committed...
            // In order to bootup correctly, need to pull in the current epoch
            ClusterMetadata current = ClusterMetadata.current();
            as.configurationService().notifyPostCommit(current, current, false);
        }
        instance = as;

        as.journal().replay();
    }

    public static void shutdownServiceAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        IAccordService i = instance;
        if (i == null)
            return;
        i.shutdownAndWait(timeout, unit);
    }

    public static IAccordService instance()
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return NOOP_SERVICE;
        IAccordService i = instance;
        Invariants.checkState(i != null, "AccordService was not started");
        return i;
    }

    public static long now()
    {
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
    }

    private AccordService(Id localId)
    {
        Invariants.checkState(localId != null, "static localId must be set before instantiating AccordService");
        logger.info("Starting accord with nodeId {}", localId);
        AccordAgent agent = FBUtilities.construct(CassandraRelevantProperties.ACCORD_AGENT_CLASS.getString(AccordAgent.class.getName()), "AccordAgent");
        this.configService = new AccordConfigurationService(localId);
        this.fastPathCoordinator = AccordFastPathCoordinator.create(localId, configService);
        this.messageSink = new AccordMessageSink(agent, configService);
        this.scheduler = new AccordScheduler();
        this.dataStore = new AccordDataStore();
        this.configuration = new AccordConfiguration(DatabaseDescriptor.getRawConfig());
        this.journal = new AccordJournal(DatabaseDescriptor.getAccord().journal);
        this.node = new Node(localId,
                             messageSink,
                             configService,
                             AccordService::now,
                             NodeTimeService.elapsedWrapperFromMonotonicSource(NANOSECONDS, Clock.Global::nanoTime),
                             () -> dataStore,
                             new KeyspaceSplitter(new EvenSplit<>(DatabaseDescriptor.getAccordShardCount(), getPartitioner().accordSplitter())),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             CompositeTopologySorter.create(SizeOfIntersectionSorter.SUPPLIER,
                                                            new AccordTopologySorter.Supplier(configService, DatabaseDescriptor.getEndpointSnitch())),
                             DefaultRemoteListeners::new,
                             DefaultRequestTimeouts::new,
                             DefaultProgressLogs::new,
                             DefaultLocalListeners.Factory::new,
                             AccordCommandStores.factory(journal),
                             new AccordInteropFactory(agent, configService),
                             configuration);
        this.nodeShutdown = toShutdownable(node);
        this.durabilityScheduling = new CoordinateDurabilityScheduling(node);
        this.requestHandler = new AccordVerbHandler<>(node, configService);
    }

    @Override
    public synchronized void startup()
    {
        if (state != State.INIT)
            return;
        journal.start(node);
        configService.start();

        fastPathCoordinator.start();
        ClusterMetadataService.instance().log().addListener(fastPathCoordinator);
        durabilityScheduling.setGlobalCycleTime(Ints.checkedCast(DatabaseDescriptor.getAccordGlobalDurabilityCycle(SECONDS)), SECONDS);
        durabilityScheduling.setShardCycleTime(Ints.checkedCast(DatabaseDescriptor.getAccordShardDurabilityCycle(SECONDS)), SECONDS);
        durabilityScheduling.setTxnIdLag(Ints.checkedCast(DatabaseDescriptor.getAccordScheduleDurabilityTxnIdLag(SECONDS)), TimeUnit.SECONDS);
        durabilityScheduling.setFrequency(Ints.checkedCast(DatabaseDescriptor.getAccordScheduleDurabilityFrequency(SECONDS)), SECONDS);
        durabilityScheduling.start();
        state = State.STARTED;
    }

    @Override
    public IVerbHandler<? extends Request> verbHandler()
    {
        return requestHandler;
    }

    private <S extends Seekables<?, ?>> Seekables barrier(@Nonnull S keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, BiFunction<Node, S, AsyncResult<SyncPoint<S>>> syncPoint)
    {
        Stopwatch sw = Stopwatch.createStarted();
        keysOrRanges = (S) intersectionWithAccordManagedRanges(keysOrRanges);
        // It's possible none of them were Accord managed and we aren't going to treat that as an error
        if (keysOrRanges.isEmpty())
        {
            logger.info("Skipping barrier because there are no ranges managed by Accord");
            return keysOrRanges;
        }

        AccordClientRequestMetrics metrics = isForWrite ? accordWriteMetrics : accordReadMetrics;
        try
        {
            logger.debug("Starting barrier key: {} epoch: {} barrierType: {} isForWrite {}", keysOrRanges, epoch, barrierType, isForWrite);
            AsyncResult<TxnId> asyncResult = syncPoint == null
                                                 ? Barrier.barrier(node, keysOrRanges, epoch, barrierType)
                                                 : Barrier.barrier(node, keysOrRanges, epoch, barrierType, syncPoint);
            long deadlineNanos = requestTime.startedAtNanos() + timeoutNanos;
            Timestamp barrierExecuteAt = AsyncChains.getBlocking(asyncResult, deadlineNanos - nanoTime(), NANOSECONDS);
            logger.debug("Completed barrier attempt in {}ms, {}ms since attempts start, barrier key: {} epoch: {} barrierType: {} isForWrite {}",
                         sw.elapsed(MILLISECONDS),
                         NANOSECONDS.toMillis(nanoTime() - requestTime.startedAtNanos()),
                         keysOrRanges, epoch, barrierType, isForWrite);
            return keysOrRanges;
        }
        catch (ExecutionException e)
        {
            Throwable cause = Throwables.getRootCause(e);
            if (cause instanceof Timeout)
            {
                TxnId txnId = ((Timeout) cause).txnId();
                metrics.timeouts.mark();
                throw newBarrierTimeout(txnId, barrierType, isForWrite, keysOrRanges);
            }
            if (cause instanceof Preempted)
            {
                TxnId txnId = ((Preempted) cause).txnId();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw newBarrierPreempted(txnId, barrierType, isForWrite, keysOrRanges);
            }
            if (cause instanceof Exhausted)
            {
                TxnId txnId = ((Exhausted) cause).txnId();
                // this case happens when a non-timeout exception is seen, and we are unable to move forward
                metrics.failures.mark();
                throw newBarrierExhausted(txnId, barrierType, isForWrite, keysOrRanges);
            }
            // unknown error
            metrics.failures.mark();
            throw new RuntimeException(cause);
        }
        catch (InterruptedException e)
        {
            metrics.failures.mark();
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            metrics.timeouts.mark();
            throw newBarrierTimeout(null, barrierType, isForWrite, keysOrRanges);
        }
        finally
        {
            // TODO Should barriers have a dedicated latency metric? Should it be a read/write metric?
            // What about counts for timeouts/failures/preempts?
            metrics.addNano(nanoTime() - requestTime.startedAtNanos());
        }
    }

    @Override
    public Seekables barrier(@Nonnull Seekables keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
    {
        return barrier(keysOrRanges, epoch, requestTime, timeoutNanos, barrierType, isForWrite, null);
    }

    public static <S extends Seekables<?, ?>> BiFunction<Node, S, AsyncResult<SyncPoint<S>>> repairSyncPoint(Set<Node.Id> allNodes)
    {
        return (node, seekables) -> CoordinateSyncPoint.coordinate(node, Kind.SyncPoint, seekables, RepairSyncPointAdapter.create(allNodes));
    }

    @Override
    public Seekables repair(@Nonnull Seekables keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
    {
        Set<Node.Id> allNodes = allEndpoints.stream().map(configService::mappedId).collect(Collectors.toUnmodifiableSet());
        return barrier(keysOrRanges, epoch, requestTime, timeoutNanos, barrierType, isForWrite, repairSyncPoint(allNodes));
    }

    private static <S extends Seekables<?, ?>> Seekables intersectionWithAccordManagedRanges(Seekables<?, ?> keysOrRanges)
    {
        TableId tableId = null;
        for (Seekable seekable : keysOrRanges)
        {
            TableId newTableId;
            if (keysOrRanges.domain() == Key)
                newTableId = ((PartitionKey) seekable).table();
            else if (keysOrRanges.domain() == Range)
                newTableId = ((TokenRange) seekable).table();
            else
                throw new IllegalStateException("Unexpected domain " + keysOrRanges.domain());

            if (tableId == null)
                tableId = newTableId;
            else if (!tableId.equals(newTableId))
                throw new IllegalArgumentException("Currently only one table is handled here.");
        }

        ClusterMetadata cm = ClusterMetadata.current();
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        TableMetadata tm = cfs.metadata();

        // Barriers can be needed just because it's an Accord managed range, but it could also be a migration back to Paxos
        // in which case we do want to barrier the migrating/migrated ranges even though the target for the migration is not Accord
        // In either case Accord should be aware of those ranges and not generate a topology mismatch
        if (tm.params.transactionalMode != TransactionalMode.off || tm.params.transactionalMigrationFrom.from != TransactionalMode.off)
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);
            // null is fine could be completely migrated or was always an Accord table on creation
            if (tms == null)
                return keysOrRanges;
            Ranges migratingAndMigratedRanges = AccordTopology.toAccordRanges(tms.tableId, tms.migratingAndMigratedRanges);
            return keysOrRanges.slice(migratingAndMigratedRanges);
        }

        switch (keysOrRanges.domain())
        {
            case Key:
                return Keys.EMPTY;
            case Range:
                return Ranges.EMPTY;
            default:
                throw new IllegalStateException("Only keys and ranges are supported");
        }
    }

    @VisibleForTesting
    static ReadTimeoutException newBarrierTimeout(TxnId txnId, BarrierType barrierType, boolean isForWrite, Seekables<?, ?> keysOrRanges)
    {
        return new ReadTimeoutException(barrierType.global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, String.format("Timeout waiting on barrier %s / %s / %s; impacted ranges %s", txnId, barrierType, isForWrite ? "write" : "not write", keysOrRanges));
    }

    @VisibleForTesting
    static ReadTimeoutException newBarrierPreempted(TxnId txnId, BarrierType barrierType, boolean isForWrite, Seekables<?, ?> keysOrRanges)
    {
        return new ReadPreemptedException(barrierType.global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, String.format("Preempted waiting on barrier %s / %s / %s; impacted ranges %s", txnId, barrierType, isForWrite ? "write" : "not write", keysOrRanges));
    }

    @VisibleForTesting
    static ReadExhaustedException newBarrierExhausted(TxnId txnId, BarrierType barrierType, boolean isForWrite, Seekables<?, ?> keysOrRanges)
    {
        return new ReadExhaustedException(barrierType.global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, String.format("Exhausted (too many failures from peers) waiting on barrier %s / %s / %s; impacted ranges %s", txnId, barrierType, isForWrite ? "write" : "not write", keysOrRanges));
    }

    @VisibleForTesting
    static boolean isTimeout(Throwable t)
    {
        return t instanceof Timeout || t instanceof ReadTimeoutException || t instanceof Preempted || t instanceof ReadPreemptedException;
    }

    @VisibleForTesting
    static Seekables doWithRetries(Blocking blocking, Supplier<Seekables> action, int retryAttempts, long initialBackoffMillis, long maxBackoffMillis) throws InterruptedException
    {
        // Since we could end up having the barrier transaction or the transaction it listens to invalidated
        Throwable existingFailures = null;
        Seekables success = null;
        long backoffMillis = initialBackoffMillis;
        for (int attempt = 0; attempt < retryAttempts; attempt++)
        {
            try
            {
                success = action.get();
                break;
            }
            catch (TopologyMismatch topologyMismatch)
            {
                // Retry topology mismatch immediately because we should be able calculate the correct ranges immediately
                backoffMillis = 0;
            }
            catch (RequestExecutionException | CoordinationFailed newFailures)
            {
                logger.error("Had failure on barrier", newFailures);
                existingFailures = FailureAccumulator.append(existingFailures, newFailures, AccordService::isTimeout);

                try
                {
                    blocking.sleep(backoffMillis);
                }
                catch (InterruptedException e)
                {
                    if (existingFailures != null)
                        e.addSuppressed(existingFailures);
                    throw e;
                }
                backoffMillis = Math.min(backoffMillis * 2, maxBackoffMillis);
            }
            catch (Throwable t)
            {
                // if an unknown/unexpected error happens retry stops right away
                if (existingFailures != null)
                    t.addSuppressed(existingFailures);
                existingFailures = t;
                break;
            }
        }
        if (success == null)
        {
            logger.error("Ran out of retries for barrier");
            checkState(existingFailures != null, "Didn't have success, but also didn't have failures");
            Throwables.throwIfUnchecked(existingFailures);
            throw new RuntimeException(existingFailures);
        }
        return success;
    }

    @Override
    public Seekables barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
    {
        return doWithRetries(Blocking.Default.instance, () -> AccordService.instance().barrier(keysOrRanges, minEpoch, Dispatcher.RequestTime.forImmediateExecution(), DatabaseDescriptor.getAccordRangeBarrierTimeoutNanos(), barrierType, isForWrite),
                             DatabaseDescriptor.getAccordBarrierRetryAttempts(),
                             DatabaseDescriptor.getAccordBarrierRetryInitialBackoffMillis(),
                             DatabaseDescriptor.getAccordBarrierRetryMaxBackoffMillis());
    }

    @Override
    public Seekables repairWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
    {
        return doWithRetries(Blocking.Default.instance, () -> AccordService.instance().repair(keysOrRanges, minEpoch, Dispatcher.RequestTime.forImmediateExecution(), DatabaseDescriptor.getAccordRangeBarrierTimeoutNanos(), barrierType, isForWrite, allEndpoints),
                             DatabaseDescriptor.getAccordBarrierRetryAttempts(),
                             DatabaseDescriptor.getAccordBarrierRetryInitialBackoffMillis(),
                             DatabaseDescriptor.getAccordBarrierRetryMaxBackoffMillis());
    }

    @Override
    public long currentEpoch()
    {
        return configService.currentEpoch();
    }

    @Override
    public TopologyManager topology()
    {
        return node.topology();
    }

    /**
     * Consistency level is just echoed back in timeouts, in the future it may be used for interoperability
     * with non-Accord operations.
     */
    @Override
    public @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        AsyncTxnResult asyncTxnResult = coordinateAsync(txn, consistencyLevel, requestTime);
        return getTxnResult(asyncTxnResult, txn.isWrite(), consistencyLevel, requestTime);
    }

    @Override
    public @Nonnull AsyncTxnResult coordinateAsync(Txn txn, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        TxnId txnId = node.nextTxnId(txn.kind(), txn.keys().domain());
        AccordClientRequestMetrics metrics = txn.isWrite() ? accordWriteMetrics : accordReadMetrics;
        metrics.keySize.update(txn.keys().size());
        AsyncResult<Result> asyncResult = node.coordinate(txnId, txn);
        AsyncTxnResult asyncTxnResult = new AsyncTxnResult(txnId);
        asyncResult.addCallback((success, failure) -> {
            long durationNanos = nanoTime() - requestTime.startedAtNanos();
            metrics.addNano(durationNanos);
            Throwable cause = failure != null ? Throwables.getRootCause(failure) : null;
            if (success != null)
            {
                if (((TxnResult) success).kind() == TxnResult.Kind.retry_new_protocol)
                {
                    metrics.retryDifferentSystem.mark();
                    Tracing.trace("Got retry different system error from Accord, will retry");
                }
                asyncTxnResult.trySuccess((TxnResult) success);
                return;
            }

            if (cause instanceof Timeout)
            {
                // Don't mark the metric here, should be done in getTxnResult to ensure it only happens once
                // since both Accord and the thread blocked on the result can trigger a timeout
                asyncTxnResult.tryFailure(newTimeout(txnId, txn.isWrite(), consistencyLevel));
                return;
            }
            if (cause instanceof Preempted)
            {
                metrics.preempted.mark();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                asyncTxnResult.tryFailure(newPreempted(txnId, txn.isWrite(), consistencyLevel));
                return;
            }
            if (cause instanceof TopologyMismatch)
            {
                metrics.topologyMismatches.mark();
                asyncTxnResult.tryFailure(RequestValidations.invalidRequest(cause.getMessage()));
                return;
            }
            metrics.failures.mark();
            asyncTxnResult.tryFailure(new RuntimeException(cause));
        });
        return asyncTxnResult;
    }

    @Override
    public TxnResult getTxnResult(AsyncTxnResult asyncTxnResult, boolean isWrite, @Nullable ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        AccordClientRequestMetrics metrics = isWrite ? accordWriteMetrics : accordReadMetrics;
        try
        {
            long deadlineNanos = requestTime.computeDeadline(DatabaseDescriptor.getTransactionTimeout(NANOSECONDS));
            TxnResult result = asyncTxnResult.get(deadlineNanos - nanoTime(), NANOSECONDS);
            return result;
        }
        catch (ExecutionException e)
        {
            // Metrics except timeout have already been handled
            Throwable cause = e.getCause();
            if (cause instanceof RequestTimeoutException)
            {
                // Mark here instead of in coordinate async since this is where the request timeout actually occurs
                metrics.timeouts.mark();
                cause.addSuppressed(e);
                throw (RequestTimeoutException) cause;
            }
            else if (cause instanceof RuntimeException)
                throw (RuntimeException) cause;
            else
                throw new RuntimeException(cause);
        }
        catch (InterruptedException e)
        {
            metrics.failures.mark();
            throw new UncheckedInterruptedException(e);
        }
        catch (TimeoutException e)
        {
            metrics.timeouts.mark();
            throw newTimeout(asyncTxnResult.txnId, isWrite, consistencyLevel);
        }
    }

    private static RequestTimeoutException newTimeout(TxnId txnId, boolean isWrite, ConsistencyLevel consistencyLevel)
    {
        // Client protocol doesn't handle null consistency level so use ANY
        if (consistencyLevel == null)
            consistencyLevel = ConsistencyLevel.ANY;
        return isWrite ? new WriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                       : new ReadTimeoutException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static RuntimeException newPreempted(TxnId txnId, boolean isWrite, ConsistencyLevel consistencyLevel)
    {
        if (consistencyLevel == null)
            consistencyLevel = ConsistencyLevel.ANY;
        return isWrite ? new WritePreemptedException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                       : new ReadPreemptedException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    @Override
    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCapacity(bytes);
    }

    @Override
    public boolean isTerminated()
    {
        return scheduler.isTerminated();
    }

    @Override
    public synchronized void shutdown()
    {
        if (state != State.STARTED)
            return;
        ExecutorUtils.shutdown(shutdownableSubsystems());
        state = State.SHUTDOWN;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, shutdownableSubsystems());
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    private List<Shutdownable> shutdownableSubsystems()
    {
        return Arrays.asList(scheduler, nodeShutdown, journal, configService);
    }

    @VisibleForTesting
    @Override
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdown();
        ExecutorUtils.shutdownAndWait(timeout, unit, this);
    }

    @Override
    public AccordScheduler scheduler()
    {
        return scheduler;
    }

    public Id nodeId()
    {
        return node.id();
    }

    @Override
    public List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId)
    {
        AsyncChain<List<CommandStoreTxnBlockedGraph>> states = loadDebug(txnId);
        try
        {
            return AsyncChains.getBlocking(states);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    public AsyncChain<List<CommandStoreTxnBlockedGraph>> loadDebug(TxnId original)
    {
        CommandStores commandStores = node.commandStores();
        if (commandStores.count() == 0)
            return AsyncChains.success(Collections.emptyList());
        int[] ids = commandStores.ids();
        List<AsyncChain<CommandStoreTxnBlockedGraph>> chains = new ArrayList<>(ids.length);
        for (int id : ids)
            chains.add(loadDebug(original, commandStores.forId(id)));
        return AsyncChains.all(chains);
    }

    private AsyncChain<CommandStoreTxnBlockedGraph> loadDebug(TxnId txnId, CommandStore store)
    {
        CommandStoreTxnBlockedGraph.Builder state = new CommandStoreTxnBlockedGraph.Builder(store.id());
        return populate(state, store, txnId).map(ignore -> state.build());
    }

    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, CommandStore store, TxnId txnId)
    {
        AsyncChain<AsyncChain<Void>> submit = store.submit(PreLoadContext.contextFor(txnId), in -> {
            AsyncChain<Void> chain = populate(state, (AccordSafeCommandStore) in, txnId);
            return chain == null ? AsyncChains.success(null) : chain;
        });
        return submit.flatMap(Function.identity());
    }

    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, CommandStore commandStore, PartitionKey blockedBy, TxnId txnId, Timestamp executeAt)
    {
        AsyncChain<AsyncChain<Void>> submit = commandStore.submit(PreLoadContext.contextFor(txnId, Keys.of(blockedBy), KeyHistory.COMMANDS), in -> {
            AsyncChain<Void> chain = populate(state, (AccordSafeCommandStore) in, blockedBy, txnId, executeAt);
            return chain == null ? AsyncChains.success(null) : chain;
        });
        return submit.flatMap(Function.identity());
    }

    @Nullable
    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TxnId txnId)
    {
        AccordSafeCommand safeCommand = safeStore.getIfLoaded(txnId);
        Invariants.nonNull(safeCommand, "Txn %s is not in the cache", txnId);
        if (safeCommand.current() == null || safeCommand.current().saveStatus() == SaveStatus.Uninitialised)
            return null;
        CommandStoreTxnBlockedGraph.TxnState cmdTxnState = populate(state, safeCommand.current());
        if (cmdTxnState.notBlocked())
            return null;
        //TODO (safety): check depth
        List<AsyncChain<Void>> chains = new ArrayList<>();
        for (TxnId blockedBy : cmdTxnState.blockedBy)
        {
            if (state.knows(blockedBy)) continue;
            // need to fetch the state
            if (safeStore.getIfLoaded(blockedBy) != null)
            {
                AsyncChain<Void> chain = populate(state, safeStore, blockedBy);
                if (chain != null)
                    chains.add(chain);
            }
            else
            {
                // go fetch it
                chains.add(populate(state, safeStore.commandStore(), blockedBy));
            }
        }
        for (PartitionKey blockedBy : cmdTxnState.blockedByKey)
        {
            if (state.keys.containsKey(blockedBy)) continue;
            if (safeStore.getCommandsForKeyIfLoaded(blockedBy) != null)
            {
                AsyncChain<Void> chain = populate(state, safeStore, blockedBy, txnId, safeCommand.current().executeAt());
                if (chain != null)
                    chains.add(chain);
            }
            else
            {
                // go fetch it
                chains.add(populate(state, safeStore.commandStore(), blockedBy, txnId, safeCommand.current().executeAt()));
            }
        }
        if (chains.isEmpty())
            return null;
        return AsyncChains.all(chains).map(ignore -> null);
    }

    private static AsyncChain<Void> populate(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, PartitionKey pk, TxnId txnId, Timestamp executeAt)
    {
        AccordSafeCommandsForKey commandsForKey = safeStore.getCommandsForKeyIfLoaded(pk);
        TxnId blocking = commandsForKey.current().blockedOnTxnId(txnId, executeAt);
        if (blocking instanceof CommandsForKey.TxnInfo)
            blocking = ((CommandsForKey.TxnInfo) blocking).plainTxnId();
        state.keys.put(pk, blocking);
        if (state.txns.containsKey(blocking)) return null;
        if (safeStore.getIfLoaded(blocking) != null) return populate(state, safeStore, blocking);
        return populate(state, safeStore.commandStore(), blocking);
    }

    private static CommandStoreTxnBlockedGraph.TxnState populate(CommandStoreTxnBlockedGraph.Builder state, Command cmd)
    {
        CommandStoreTxnBlockedGraph.Builder.TxnBuilder cmdTxnState = state.txn(cmd.txnId(), cmd.executeAt(), cmd.saveStatus());
        if (!cmd.hasBeen(Status.Applied) && cmd.isCommitted())
        {
            // check blocking state
            Command.WaitingOn waitingOn = cmd.asCommitted().waitingOn();
            waitingOn.waitingOn.reverseForEach(null, null, null, null, (i1, i2, i3, i4, i) -> {
                if (i < waitingOn.txnIdCount())
                {
                    // blocked on txn
                    cmdTxnState.blockedBy.add(waitingOn.txnId(i));
                }
                else
                {
                    // blocked on key
                    cmdTxnState.blockedByKey.add((PartitionKey) waitingOn.keys.get(i - waitingOn.txnIdCount()));
                }
            });
        }
        return cmdTxnState.build();
    }

    public Node node()
    {
        return node;
    }

    public AccordJournal journal()
    {
        return journal;
    }

    @Override
    public Future<Void> epochReady(Epoch epoch)
    {
        AsyncPromise<Void> promise = new AsyncPromise<>();
        AsyncChain<Void> ready = configService.epochReady(epoch.getEpoch());
        ready.begin((result, failure) -> {
            if (failure == null) promise.trySuccess(result);
            else promise.tryFailure(failure);
        });
        return promise;
    }

    @Override
    public void receive(Message<List<Notification>> message)
    {
        receive(MessagingService.instance(), configService, message);
    }

    @VisibleForTesting
    public static void receive(MessageDelivery sink, AbstractConfigurationService<?, ?> configService, Message<List<Notification>> message)
    {
        List<AccordSyncPropagator.Notification> notifications = message.payload;
        notifications.forEach(notification -> {
            notification.syncComplete.forEach(id -> configService.receiveRemoteSyncComplete(id, notification.epoch));
            if (!notification.closed.isEmpty())
                configService.receiveClosed(notification.closed, notification.epoch);
            if (!notification.redundant.isEmpty())
                configService.receiveRedundant(notification.redundant, notification.epoch);
        });
        sink.respond(Ok, message);
    }

    private static Shutdownable toShutdownable(Node node)
    {
        return new Shutdownable() {
            private volatile boolean isShutdown = false;

            @Override
            public boolean isTerminated()
            {
                // we don't know about terminiated... so settle for shutdown!
                return isShutdown;
            }

            @Override
            public void shutdown()
            {
                isShutdown = true;
                node.shutdown();
            }

            @Override
            public Object shutdownNow()
            {
                // node doesn't offer shutdownNow
                shutdown();
                return null;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit units)
            {
                // node doesn't offer
                return true;
            }
        };
    }

    @VisibleForTesting
    public AccordConfigurationService configurationService()
    {
        return configService;
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        Int2ObjectHashMap<RedundantBefore> redundantBefores = new Int2ObjectHashMap<>();
        Int2ObjectHashMap<CommandStores.RangesForEpoch> ranges = new Int2ObjectHashMap<>();
        AtomicReference<DurableBefore> durableBefore = new AtomicReference<>(DurableBefore.EMPTY);
        AsyncChains.getBlockingAndRethrow(node.commandStores().forEach(safeStore -> {
            synchronized (redundantBefores)
            {
                redundantBefores.put(safeStore.commandStore().id(), safeStore.commandStore().redundantBefore());
                ranges.put(safeStore.commandStore().id(), safeStore.ranges());
            }
            durableBefore.set(DurableBefore.merge(durableBefore.get(), safeStore.commandStore().durableBefore()));
        }));
        return new CompactionInfo(redundantBefores, ranges, durableBefore.get());
    }
}
