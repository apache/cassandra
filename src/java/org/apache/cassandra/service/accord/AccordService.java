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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import accord.coordinate.Barrier;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.Exhausted;
import accord.coordinate.FailureAccumulator;
import accord.coordinate.TopologyMismatch;
import accord.impl.CoordinateDurabilityScheduling;
import accord.local.CommandStores;
import accord.primitives.SyncPoint;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.statements.RequestValidations;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.accord.exceptions.ReadExhaustedException;
import org.apache.cassandra.service.accord.interop.AccordInteropAdapter.AccordInteropFactory;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.service.accord.repair.RepairSyncPointAdapter;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.service.accord.api.*;
import org.apache.cassandra.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.Result;
import accord.config.LocalConfig;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.AbstractConfigurationService;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.RedundantBefore;
import accord.local.ShardDistributor.EvenSplit;
import accord.messages.LocalRequest;
import accord.messages.Request;
import accord.primitives.Seekables;
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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.messages.SimpleReply.Ok;
import static accord.utils.Invariants.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);

    private enum State { INIT, STARTED, SHUTDOWN}

    public static final AccordClientRequestMetrics readMetrics = new AccordClientRequestMetrics("AccordRead");
    public static final AccordClientRequestMetrics writeMetrics = new AccordClientRequestMetrics("AccordWrite");
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
        public long barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long barrier(@Nonnull Seekables keysOrRanges, long minEpoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
        {
            throw new UnsupportedOperationException("No accord barriers should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public long repair(@Nonnull Seekables keysOrRanges, long epoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
        {
            throw new UnsupportedOperationException("No accord repairs should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull long queryStartNanos)
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

    public static long uniqueNow()
    {
        // TODO (now, correctness): This is not unique it's just currentTimeMillis as microseconds
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
    }

    public static long unix(TimeUnit timeUnit)
    {
        Preconditions.checkArgument(timeUnit != TimeUnit.NANOSECONDS, "Nanoseconds since the epoch doesn't fit in a long");
        return timeUnit.convert(Clock.Global.currentTimeMillis(), TimeUnit.MILLISECONDS);
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
        this.journal = new AccordJournal(configService, DatabaseDescriptor.getAccord().journal);
        this.node = new Node(localId,
                             messageSink,
                             this::handleLocalRequest,
                             configService,
                             AccordService::uniqueNow,
                             NodeTimeService.unixWrapper(TimeUnit.MICROSECONDS, AccordService::uniqueNow),
                             () -> dataStore,
                             new KeyspaceSplitter(new EvenSplit<>(DatabaseDescriptor.getAccordShardCount(), getPartitioner().accordSplitter())),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             CompositeTopologySorter.create(SizeOfIntersectionSorter.SUPPLIER,
                                                            new AccordTopologySorter.Supplier(configService, DatabaseDescriptor.getEndpointSnitch())),
                             SimpleProgressLog::new,
                             AccordCommandStores.factory(journal),
                             new AccordInteropFactory(agent, configService),
                             configuration);
        this.nodeShutdown = toShutdownable(node);
        this.durabilityScheduling = new CoordinateDurabilityScheduling(node);
        this.requestHandler = new AccordVerbHandler<>(node, configService, journal);
    }

    @Override
    public synchronized void startup()
    {
        if (state != State.INIT)
            return;
        journal.start(node);
        configService.start();
        ClusterMetadataService.instance().log().addListener(configService);
        fastPathCoordinator.start();
        ClusterMetadataService.instance().log().addListener(fastPathCoordinator);
        durabilityScheduling.setGlobalCycleTime(Ints.checkedCast(DatabaseDescriptor.getAccordGlobalDurabilityCycle(SECONDS)), SECONDS);
        durabilityScheduling.setShardCycleTime(Ints.checkedCast(DatabaseDescriptor.getAccordShardDurabilityCycle(SECONDS)), SECONDS);
        durabilityScheduling.setTxnIdLag(Ints.checkedCast(DatabaseDescriptor.getAccordScheduleDurabilityTxnIdLag(SECONDS)), TimeUnit.SECONDS);
        durabilityScheduling.setFrequency(Ints.checkedCast(DatabaseDescriptor.getAccordScheduleDurabilityFrequency(SECONDS)), SECONDS);
//        durabilityScheduling.start();
        state = State.STARTED;
    }

    @Override
    public IVerbHandler<? extends Request> verbHandler()
    {
        return requestHandler;
    }

    private <S extends Seekables<?, ?>> long barrier(@Nonnull S keysOrRanges, long epoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite, BiFunction<Node, S, AsyncResult<SyncPoint<S>>> syncPoint)
    {
        AccordClientRequestMetrics metrics = isForWrite ? accordWriteMetrics : accordReadMetrics;
        TxnId txnId = null;
        try
        {
            logger.debug("Starting barrier key: {} epoch: {} barrierType: {} isForWrite {}", keysOrRanges, epoch, barrierType, isForWrite);
            txnId = node.nextTxnId(Kind.SyncPoint, keysOrRanges.domain());
            AsyncResult<TxnId> asyncResult = syncPoint == null
                                                 ? Barrier.barrier(node, keysOrRanges, epoch, barrierType)
                                                 : Barrier.barrier(node, keysOrRanges, epoch, barrierType, syncPoint);
            long deadlineNanos = queryStartNanos + timeoutNanos;
            Timestamp barrierExecuteAt = AsyncChains.getBlocking(asyncResult, deadlineNanos - nanoTime(), NANOSECONDS);
            logger.debug("Completed in {}ms barrier key: {} epoch: {} barrierType: {} isForWrite {}",
                         NANOSECONDS.toMillis(nanoTime() - queryStartNanos),
                         keysOrRanges, epoch, barrierType, isForWrite);
            return barrierExecuteAt.epoch();
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Timeout)
            {
                metrics.timeouts.mark();
                throw newBarrierTimeout(txnId, barrierType.global);
            }
            if (cause instanceof Preempted)
            {
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw newBarrierPreempted(txnId, barrierType.global);
            }
            if (cause instanceof Exhausted)
            {
                // this case happens when a non-timeout exception is seen, and we are unable to move forward
                metrics.failures.mark();
                throw newBarrierExhausted(txnId, barrierType.global);
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
            throw newBarrierTimeout(txnId, barrierType.global);
        }
        finally
        {
            // TODO Should barriers have a dedicated latency metric? Should it be a read/write metric?
            // What about counts for timeouts/failures/preempts?
            metrics.addNano(nanoTime() - queryStartNanos);
        }
    }

    @Override
    public long barrier(@Nonnull Seekables keysOrRanges, long epoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
    {
        return barrier(keysOrRanges, epoch, queryStartNanos, timeoutNanos, barrierType, isForWrite, null);
    }

    public static <S extends Seekables<?, ?>> BiFunction<Node, S, AsyncResult<SyncPoint<S>>> repairSyncPoint(Set<Node.Id> allNodes)
    {
        return (node, seekables) -> CoordinateSyncPoint.coordinate(node, Kind.SyncPoint, seekables, RepairSyncPointAdapter.create(allNodes));
    }

    public long repair(@Nonnull Seekables keysOrRanges, long epoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
    {
        Set<Node.Id> allNodes = allEndpoints.stream().map(configService::mappedId).collect(Collectors.toUnmodifiableSet());
        return barrier(keysOrRanges, epoch, queryStartNanos, timeoutNanos, barrierType, isForWrite, repairSyncPoint(allNodes));
    }

    @VisibleForTesting
    static ReadTimeoutException newBarrierTimeout(TxnId txnId, boolean global)
    {
        return new ReadTimeoutException(global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, txnId.toString());
    }

    @VisibleForTesting
    static ReadTimeoutException newBarrierPreempted(TxnId txnId, boolean global)
    {
        return new ReadPreemptedException(global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, txnId.toString());
    }

    @VisibleForTesting
    static ReadExhaustedException newBarrierExhausted(TxnId txnId, boolean global)
    {
        //TODO (usability): not being able to show the txn is a bad UX, this becomes harder to trace back in logs
        return new ReadExhaustedException(global ? ConsistencyLevel.ANY : ConsistencyLevel.QUORUM, 0, 0, false, ImmutableMap.of());
    }

    @VisibleForTesting
    static boolean isTimeout(Throwable t)
    {
        return t instanceof Timeout || t instanceof ReadTimeoutException || t instanceof Preempted || t instanceof ReadPreemptedException;
    }

    @VisibleForTesting
    static long doWithRetries(Blocking blocking, LongSupplier action, int retryAttempts, long initialBackoffMillis, long maxBackoffMillis) throws InterruptedException
    {
        // Since we could end up having the barrier transaction or the transaction it listens to invalidated
        Throwable existingFailures = null;
        Long success = null;
        long backoffMillis = initialBackoffMillis;
        for (int attempt = 0; attempt < retryAttempts; attempt++)
        {
            try
            {
                success = action.getAsLong();
                break;
            }
            catch (RequestExecutionException | CoordinationFailed newFailures)
            {
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
            checkState(existingFailures != null, "Didn't have success, but also didn't have failures");
            Throwables.throwIfUnchecked(existingFailures);
            throw new RuntimeException(existingFailures);
        }
        return success;
    }

    @Override
    public long barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
    {
        return doWithRetries(Blocking.Default.instance, () -> AccordService.instance().barrier(keysOrRanges, minEpoch, Clock.Global.nanoTime(), DatabaseDescriptor.getAccordRangeBarrierTimeoutNanos(), barrierType, isForWrite),
                      DatabaseDescriptor.getAccordBarrierRetryAttempts(),
                      DatabaseDescriptor.getAccordBarrierRetryInitialBackoffMillis(),
                      DatabaseDescriptor.getAccordBarrierRetryMaxBackoffMillis());
    }

    @Override
    public long repairWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
    {
        return doWithRetries(Blocking.Default.instance, () -> AccordService.instance().repair(keysOrRanges, minEpoch, Clock.Global.nanoTime(), DatabaseDescriptor.getAccordRangeBarrierTimeoutNanos(), barrierType, isForWrite, allEndpoints),
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
    public @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, long queryStartNanos)
    {
        AccordClientRequestMetrics metrics = txn.isWrite() ? accordWriteMetrics : accordReadMetrics;
        TxnId txnId = null;
        try
        {
            metrics.keySize.update(txn.keys().size());
            txnId = node.nextTxnId(txn.kind(), txn.keys().domain());
            long deadlineNanos = queryStartNanos + DatabaseDescriptor.getTransactionTimeout(NANOSECONDS);
            AsyncResult<Result> asyncResult = node.coordinate(txnId, txn);
            Result result = AsyncChains.getBlocking(asyncResult, deadlineNanos - nanoTime(), NANOSECONDS);
            return (TxnResult) result;
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Timeout)
            {
                metrics.timeouts.mark();
                throw newTimeout(txnId, txn, consistencyLevel);
            }
            if (cause instanceof Preempted)
            {
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw newPreempted(txnId, txn, consistencyLevel);
            }
            if (cause instanceof TopologyMismatch)
            {
                throw RequestValidations.invalidRequest(cause.getMessage());
            }
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
            throw newTimeout(txnId, txn, consistencyLevel);
        }
        finally
        {
            metrics.addNano(nanoTime() - queryStartNanos);
        }
    }

    private <R> void handleLocalRequest(LocalRequest<R> request, BiConsumer<? super R, Throwable> callback, Node node)
    {
        // currently, we only create LocalRequests that have side effects and need to be persisted
        Invariants.checkState(request.type().hasSideEffects());
        journal.processLocalRequest(request, callback);
    }

    private static RequestTimeoutException newTimeout(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadTimeoutException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static RuntimeException newPreempted(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WritePreemptedException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
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
        Int2ObjectHashMap<CommandStores.RangesForEpoch>ranges = new Int2ObjectHashMap<>();
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
