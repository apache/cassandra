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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.AbstractConfigurationService;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.RedundantBefore;
import accord.local.ShardDistributor.EvenSplit;
import accord.messages.LocalMessage;
import accord.messages.Request;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.journal.AsyncWriteCallback;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.exceptions.ReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.WritePreemptedException;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.messages.SimpleReply.Ok;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);

    public static final AccordClientRequestMetrics readMetrics = new AccordClientRequestMetrics("AccordRead");
    public static final AccordClientRequestMetrics writeMetrics = new AccordClientRequestMetrics("AccordWrite");
    private static final Future<Void> BOOTSTRAP_SUCCESS = ImmediateFuture.success(null);

    private final Node node;
    private final Shutdownable nodeShutdown;
    private final AccordMessageSink messageSink;
    private final AccordConfigurationService configService;
    private final AccordScheduler scheduler;
    private final AccordDataStore dataStore;
    private final AccordJournal journal;
    private final AccordVerbHandler<? extends Request> verbHandler;

    private static final IAccordService NOOP_SERVICE = new IAccordService()
    {
        @Override
        public IVerbHandler<? extends Request> verbHandler()
        {
            return null;
        }

        @Override
        public TxnData coordinate(Txn txn, ConsistencyLevel consistencyLevel)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public long currentEpoch()
        {
            throw new UnsupportedOperationException("Cannot return epoch when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public void setCacheSize(long kb) { }

        @Override
        public TopologyManager topology()
        {
            throw new UnsupportedOperationException("Cannot return topology when accord_transactions_enabled = false in cassandra.yaml");
        }

        @Override
        public void startup() {}

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
        public boolean isAccordManagedKeyspace(String keyspace)
        {
            return false;
        }

        @Override
        public Pair<Int2ObjectHashMap<RedundantBefore>, DurableBefore> getRedundantBeforesAndDurableBefore()
        {
            return Pair.create(new Int2ObjectHashMap<>(), DurableBefore.EMPTY);
        }
    };

    private static volatile Node.Id localId = null;

    private static class Handle
    {
        public static final AccordService instance = new AccordService();
    }

    public static boolean isSetup()
    {
        return localId != null;
    }

    public static IVerbHandler<? extends Request> verbHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        return instance().verbHandler();
    }

    public static void startup(NodeId tcmId)
    {
        localId = AccordTopologyUtils.tcmIdToAccord(tcmId);
        instance().startup();
    }

    public static void shutdownServiceAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        if (localId == null)
            return;
        instance().shutdownAndWait(timeout, unit);
    }

    public static IAccordService instance()
    {
        return DatabaseDescriptor.getAccordTransactionsEnabled() ? Handle.instance : NOOP_SERVICE;
    }

    public static long uniqueNow()
    {
        // TODO (correctness, now): This is not unique it's just currentTimeMillis as microseconds
        return TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
    }

    public static long unix(TimeUnit timeUnit)
    {
        Preconditions.checkArgument(timeUnit != TimeUnit.NANOSECONDS, "Nanoseconds since the epoch doesn't fit in a long");
        return timeUnit.convert(Clock.Global.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    private AccordService()
    {
        Invariants.checkState(localId != null, "static localId must be set before instantiating AccordService");
        logger.info("Starting accord with nodeId {}", localId);
        AccordAgent agent = new AccordAgent();
        this.configService = new AccordConfigurationService(localId);
        this.messageSink = new AccordMessageSink(agent, configService);
        this.scheduler = new AccordScheduler();
        this.dataStore = new AccordDataStore();
        this.journal = new AccordJournal();
        this.node = new Node(localId,
                             messageSink,
                             this::handleLocalMessage,
                             configService,
                             AccordService::uniqueNow,
                             NodeTimeService.unixWrapper(TimeUnit.MICROSECONDS, AccordService::uniqueNow),
                             () -> dataStore,
                             new KeyspaceSplitter(new EvenSplit<>(DatabaseDescriptor.getAccordShardCount(), getPartitioner().accordSplitter())),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             SizeOfIntersectionSorter.SUPPLIER,
                             SimpleProgressLog::new,
                             AccordCommandStores.factory(journal));
        this.nodeShutdown = toShutdownable(node);
        this.verbHandler = new AccordVerbHandler<>(node, configService, journal);
    }

    @Override
    public void startup()
    {
        journal.start();
        configService.start();
        ClusterMetadataService.instance().log().addListener(configService);
    }

    @Override
    public IVerbHandler<? extends Request> verbHandler()
    {
        return verbHandler;
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
    public TxnData coordinate(Txn txn, ConsistencyLevel consistencyLevel)
    {
        AccordClientRequestMetrics metrics = txn.isWrite() ? writeMetrics : readMetrics;
        TxnId txnId = null;
        final long startNanos = nanoTime();
        try
        {
            metrics.keySize.update(txn.keys().size());
            txnId = node.nextTxnId(txn.kind(), txn.keys().domain());
            AsyncResult<Result> asyncResult = node.coordinate(txnId, txn);
            Result result = AsyncChains.getBlocking(asyncResult, DatabaseDescriptor.getTransactionTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            return (TxnData) result;
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            if (cause instanceof Timeout)
            {
                metrics.timeouts.mark();
                throw throwTimeout(txnId, txn, consistencyLevel);
            }
            if (cause instanceof Preempted)
            {
                metrics.preempts.mark();
                //TODO need to improve
                // Coordinator "could" query the accord state to see whats going on but that doesn't exist yet.
                // Protocol also doesn't have a way to denote "unknown" outcome, so using a timeout as the closest match
                throw throwPreempted(txnId, txn, consistencyLevel);
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
            throw throwTimeout(txnId, txn, consistencyLevel);
        }
        finally
        {
            metrics.addNano(nanoTime() - startNanos);
        }
    }

    private void handleLocalMessage(LocalMessage message, Node node)
    {
        if (!message.type().hasSideEffects())
        {
            message.process(node);
            return;
        }

        journal.appendMessage(message, ImmediateExecutor.INSTANCE, new AsyncWriteCallback()
        {
            @Override
            public void run()
            {
                // TODO (performance, expected): do not retain references to messages beyond a certain total
                //      cache threshold; in case of flush lagging behind, read the messages from journal and
                //      deserialize instead before processing, to prevent memory pressure buildup from messages
                //      pending flush to disk.
                message.process(node);
            }

            @Override
            public void onFailure(Throwable error)
            {
                if (message instanceof MapReduceConsume)
                    ((MapReduceConsume<?,?>) message).accept(null, error);
                else
                    node.agent().onUncaughtException(error);
            }
        });
    }

    private static RuntimeException throwTimeout(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadTimeoutException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    private static RuntimeException throwPreempted(TxnId txnId, Txn txn, ConsistencyLevel consistencyLevel)
    {
        throw txn.isWrite() ? new WritePreemptedException(WriteType.CAS, consistencyLevel, 0, 0, txnId.toString())
                            : new ReadPreemptedException(consistencyLevel, 0, 0, false, txnId.toString());
    }

    @Override
    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCacheSize(bytes);
    }

    @Override
    public boolean isTerminated()
    {
        return scheduler.isTerminated();
    }

    @Override
    public void shutdown()
    {
        ExecutorUtils.shutdown(Arrays.asList(scheduler, nodeShutdown, journal));
    }

    @Override
    public Object shutdownNow()
    {
        ExecutorUtils.shutdownNow(Arrays.asList(scheduler, nodeShutdown, journal));
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        try
        {
            ExecutorUtils.awaitTermination(timeout, units, Arrays.asList(scheduler, nodeShutdown, journal));
            return true;
        }
        catch (TimeoutException e)
        {
            return false;
        }
    }

    @VisibleForTesting
    @Override
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        scheduler.shutdownNow();
        ExecutorUtils.shutdownAndWait(timeout, unit, this);
    }

    @Override
    public AccordScheduler scheduler()
    {
        return scheduler;
    }

    @VisibleForTesting
    public Node node()
    {
        return node;
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

    public boolean isAccordManagedKeyspace(String keyspace)
    {
        return ClusterMetadata.current().accordKeyspaces.contains(keyspace);
    }

    @Override
    public Pair<Int2ObjectHashMap<RedundantBefore>, DurableBefore> getRedundantBeforesAndDurableBefore()
    {
        Int2ObjectHashMap<RedundantBefore> redundantBefores = new Int2ObjectHashMap<>();
        AtomicReference<DurableBefore> durableBefore = new AtomicReference<>(DurableBefore.EMPTY);
        AsyncChains.getBlockingAndRethrow(node.commandStores().forEach(safeStore -> {
            synchronized (redundantBefores)
            {
                redundantBefores.put(safeStore.commandStore().id(), safeStore.commandStore().redundantBefore());
            }
            durableBefore.set(DurableBefore.merge(durableBefore.get(), safeStore.commandStore().durableBefore()));
        }));
        return Pair.create(redundantBefores, durableBefore.get());
    }
}
