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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Journal;
import accord.api.ProtocolModifiers;
import accord.coordinate.CoordinateMaxConflict;
import accord.coordinate.CoordinateTransaction;
import accord.coordinate.KeyBarriers;
import accord.impl.AbstractConfigurationService;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.RequestCallbacks;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.ShardDistributor.EvenSplit;
import accord.local.UniqueTimeService.AtomicUniqueTimeWithStaleReservation;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.durability.DurabilityService;
import accord.local.durability.ShardDurability;
import accord.messages.Reply;
import accord.messages.Request;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.TimeOnlyRequestBookkeeping.LatencyRequestBookkeeping;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutableKey;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.api.AccordTimeService;
import org.apache.cassandra.service.accord.api.AccordTopologySorter;
import org.apache.cassandra.service.accord.api.CompositeTopologySorter;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.api.TokenKey.KeyspaceSplitter;
import org.apache.cassandra.service.accord.interop.AccordInteropAdapter.AccordInteropFactory;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.api.ProtocolModifiers.Toggles.FastExec.MAY_BYPASS_SAFESTORE;
import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static accord.local.durability.DurabilityService.SyncLocal.Self;
import static accord.local.durability.DurabilityService.SyncRemote.All;
import static accord.messages.SimpleReply.Ok;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.topology.TopologyManager.TopologyRange;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordCommandStoreShardCount;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordGlobalDurabilityCycle;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordShardDurabilityCycle;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordShardDurabilityMaxSplits;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordShardDurabilityTargetSplits;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.journal.Params.ReplayMode.RESET;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadBookkeeping;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteBookkeeping;
import static org.apache.cassandra.service.accord.journal.AccordTopologyUpdate.ImmutableTopoloyImage;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.getTableMetadata;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    public static class MetadataChangeListener implements ChangeListener.Async
    {
        // Listener is initialized before Accord is initialized
        public static MetadataChangeListener instance = new MetadataChangeListener();

        private MetadataChangeListener() {}

        private final AtomicReference<ChangeListener> collector = new AtomicReference<>(new PreInitStateCollector());

        @Override
        public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
        {
            collector.get().notifyPreCommit(prev, next, fromSnapshot);
        }

        @Override
        public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
        {
            collector.get().notifyPostCommit(prev, next, fromSnapshot);
        }

        @VisibleForTesting
        public void resetForTesting(ClusterMetadata metadata)
        {
            PreInitStateCollector stateCollector = new PreInitStateCollector();
            stateCollector.items.add(metadata);
            collector.set(stateCollector);
        }

        /**
         * Collects TCM events from startup util full Accord initialization to avoid races with TCM and creating gaps between
         * epochs restored from journal and reported by TCM.
         **/

        static class PreInitStateCollector implements ChangeListener
        {
            private final List<ClusterMetadata> items = new ArrayList<>(4);

            @Override
            public synchronized void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
            {
                items.add(next);
            }

            public synchronized List<ClusterMetadata> getItems()
            {
                return new ArrayList<>(items);
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AccordService.class);
    private static final Future<Void> EPOCH_READY = ImmediateFuture.success(null);
    static
    {
        ProtocolModifiers.Toggles.setPermitLocalExecution(true);
        ProtocolModifiers.Toggles.setRequiresUniqueHlcs(true);
        ProtocolModifiers.Toggles.setFastReadExecMayResendTxn(true);
        ProtocolModifiers.Toggles.setFastReadExec(MAY_BYPASS_SAFESTORE);
        ProtocolModifiers.Toggles.setFastWriteExec(MAY_BYPASS_SAFESTORE);
        ProtocolModifiers.Toggles.setDataStoreDetectsFutureReads(true);
    }

    private enum State { INIT, STARTED, SHUTTING_DOWN, SHUTDOWN }

    private final Node node;
    private final Shutdownable nodeShutdown;
    private final AccordMessageSink messageSink;
    private final AccordConfigurationService configService;
    private final AccordFastPathCoordinator fastPathCoordinator;
    private final AccordScheduler scheduler;
    private final AccordDataStore dataStore;
    private final AccordJournal journal;
    private final AccordVerbHandler<? extends Request> requestHandler;
    private final AccordResponseVerbHandler<? extends Reply> responseHandler;

    @GuardedBy("this")
    private State state = State.INIT;

    private static final IAccordService NOOP_SERVICE = new NoOpAccordService();

    private static volatile IAccordService instance = null;

    @VisibleForTesting
    public static void unsafeSetNewAccordService(IAccordService service)
    {
        instance = service;
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

    public static IVerbHandler<Void> watermarkHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        AccordService i = (AccordService) instance();
        return i.configService().watermarkCollector.handler;
    }

    public static IVerbHandler<? extends Request> requestHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        return instance().requestHandler();
    }

    public static IVerbHandler<? extends Reply> responseHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        return instance().responseHandler();
    }

    @VisibleForTesting
    public synchronized static AccordService startup(NodeId tcmId)
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
        {
            instance = NOOP_SERVICE;
            return null;
        }

        if (instance != null)
            return (AccordService) instance;

        AccordService as = new AccordService(AccordTopology.tcmIdToAccord(tcmId));
        as.startup();
        instance = as;

        replayJournal(as);

        as.finishInitialization();

        as.configService.start();
        as.configService.unsafeMarkTruncated();
        as.fastPathCoordinator.start();

        ClusterMetadataService.instance().log().addListener(as.fastPathCoordinator);
        as.node.durability().shards().reconfigure(Ints.checkedCast(getAccordShardDurabilityTargetSplits()),
                                                  Ints.checkedCast(getAccordShardDurabilityMaxSplits()),
                                                  Ints.checkedCast(getAccordShardDurabilityCycle(SECONDS)), SECONDS);
        as.node.durability().global().setGlobalCycleTime(Ints.checkedCast(getAccordGlobalDurabilityCycle(SECONDS)), SECONDS);
        as.state = State.STARTED;
        // Only enable durability scheduling _after_ we have fully replayed journal
        as.configService.registerListener(as.node.durability());
        as.node.durability().start();

        WatermarkCollector.fetchAndReportWatermarksAsync(as.configService);
        return as;
    }

    @VisibleForTesting
    public static void replayJournal(AccordService as)
    {
        logger.info("Starting journal replay.");
        long before = Clock.Global.nanoTime();
        CommandsForKey.disableLinearizabilityViolationsReporting();
        try
        {
            if (as.journalConfiguration().replayMode() == RESET)
                AccordKeyspace.truncateCommandsForKey();

            as.node.commandStores().forEachCommandStore(cs -> cs.unsafeProgressLog().stop());
            as.journal().replay(as.node().commandStores());
            logger.info("Waiting for command stores to quiesce.");
            ((AccordCommandStores)as.node.commandStores()).waitForQuiescense();
            as.journal.unsafeSetStarted();
            as.node.commandStores().forEachCommandStore(cs -> cs.unsafeProgressLog().start());
        }
        finally
        {
            CommandsForKey.enableLinearizabilityViolationsReporting();
        }

        long after = Clock.Global.nanoTime();
        logger.info("Finished journal replay. {}ms elapsed", NANOSECONDS.toMillis(after - before));
    }

    public static void shutdownServiceAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        IAccordService i = instance;
        if (i == null)
            return;
        i.shutdownAndWait(timeout, unit);
    }

    @Override
    public boolean shouldAcceptMessages()
    {
        return state == State.STARTED && journal.started();
    }

    public static IAccordService instance()
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return NOOP_SERVICE;
        IAccordService i = instance;
        Invariants.require(i != null, "AccordService was not started");
        return i;
    }

    public static boolean started()
    {
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
            return false;
        return instance != null;
    }

    @VisibleForTesting
    public AccordService(Id localId)
    {
        Invariants.require(localId != null, "static localId must be set before instantiating AccordService");
        logger.info("Starting accord with nodeId {}", localId);
        AccordAgent agent = FBUtilities.construct(CassandraRelevantProperties.ACCORD_AGENT_CLASS.getString(AccordAgent.class.getName()), "AccordAgent");
        agent.setNodeId(localId);
        AccordTimeService time = new AccordTimeService();
        final RequestCallbacks callbacks = new RequestCallbacks(time);
        this.scheduler = new AccordScheduler();
        this.dataStore = new AccordDataStore();
        this.journal = new AccordJournal(DatabaseDescriptor.getAccord().journal);
        this.configService = new AccordConfigurationService(localId, agent);
        this.fastPathCoordinator = AccordFastPathCoordinator.create(localId, configService);
        this.messageSink = new AccordMessageSink(agent, configService, callbacks);
        this.node = new Node(localId,
                             messageSink,
                             configService,
                             time, new AtomicUniqueTimeWithStaleReservation(time),
                             () -> dataStore,
                             new KeyspaceSplitter(new EvenSplit<>(getAccordCommandStoreShardCount(), getPartitioner().accordSplitter())),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             CompositeTopologySorter.create(SizeOfIntersectionSorter.SUPPLIER,
                                                            new AccordTopologySorter.Supplier(configService, DatabaseDescriptor.getNodeProximity())),
                             DefaultRemoteListeners::new,
                             ignore -> callbacks,
                             DefaultProgressLogs::new,
                             DefaultLocalListeners.Factory::new,
                             AccordCommandStores.factory(),
                             new AccordInteropFactory(configService),
                             journal.durableBeforePersister(),
                             journal);
        this.nodeShutdown = toShutdownable(node);
        this.requestHandler = new AccordVerbHandler<>(node, configService);
        this.responseHandler = new AccordResponseVerbHandler<>(callbacks, configService);
    }

    @Override
    public synchronized void startup()
    {
        if (state != State.INIT)
            return;
        journal.start(node);
        node.load();

        ClusterMetadata metadata = ClusterMetadata.current();
        configService.updateMapping(metadata);

        List<ImmutableTopoloyImage> images = new ArrayList<>();

        // Collect locally known topologies
        Iterator<ImmutableTopoloyImage> iter = journal.replayTopologies();
        Journal.TopologyUpdate prev = null;
        while (iter.hasNext())
        {
            ImmutableTopoloyImage next = iter.next();
            // Due to partial compaction, we can clean up only some of the old epochs, creating gaps. We skip these epochs here.
            if (prev != null && next.global.epoch() > prev.global.epoch() + 1)
                images.clear();

            images.add(next);
            prev = next;
        }

        // Instantiate latest topology from the log, if known
        if (prev != null)
        {
            node.commandStores().initializeTopologyUnsafe(prev);
        }

        // Replay local epochs
        for (ImmutableTopoloyImage image : images)
            configService.reportTopology(image.global);
    }

    /**
     * Startup is broken up in two phases: local and distributed startup. During local startup, we replay up to
     *  the latest epoch known to the node prior to restart. After that, we replay journal itself, and only after
     *  that we finish initializaiton and replay the rest of epochs.
     */
    @VisibleForTesting
    public void finishInitialization()
    {
        configService.updateMapping(ClusterMetadata.current());
        long highestKnown = -1;
        if (configService.currentTopology() != null)
            highestKnown = configService.currentEpoch();
        try
        {
            TopologyRange remote = fetchTopologies(highestKnown + 1);

            if (remote != null)
                remote.forEach(configService::reportTopology, highestKnown + 1, Integer.MAX_VALUE);

            // Subscribe to TCM events
            ChangeListener prevListener = MetadataChangeListener.instance.collector.getAndSet(new ChangeListener()
            {
                @Override
                public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
                {
                    if (state != State.SHUTDOWN)
                        configService.maybeReportMetadata(next);
                }
            });

            Invariants.require((prevListener instanceof MetadataChangeListener.PreInitStateCollector),
                               "Listener should have been initialized with Accord pre-init state collector, but was " + prevListener.getClass());

            MetadataChangeListener.PreInitStateCollector preinit = (MetadataChangeListener.PreInitStateCollector) prevListener;
            for (ClusterMetadata item : preinit.getItems())
            {
                if (item.epoch.getEpoch() > minEpoch())
                    configService.maybeReportMetadata(item);
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
    /**
     * Queries peers to discover min epoch, and then fetches all topologies between min and current epochs
     */
    private TopologyRange fetchTopologies(long from) throws ExecutionException, InterruptedException
    {
        ClusterMetadata metadata = ClusterMetadata.current();

        Set<InetAddressAndPort> peers = new HashSet<>();
        peers.addAll(metadata.directory.allAddresses());
        peers.remove(FBUtilities.getBroadcastAddressAndPort());

        // No peers: single node cluster or first node to boot
        if (peers.isEmpty())
            return null;

        try
        {
            logger.info("Fetching topologies for epochs [{}, {}] from {}", from, metadata.epoch.getEpoch(), peers);
            Invariants.require(from <= metadata.epoch.getEpoch(),
                               "Accord epochs should never be ahead of TCM ones, but %d was ahead of %d", from, metadata.epoch.getEpoch());

            Future<TopologyRange> futures = FetchTopologies.fetch(SharedContext.Global.instance,
                                                                  peers,
                                                                  from,
                                                                  Long.MAX_VALUE);
            TopologyRange response = futures.get();
            logger.info("Fetched topologies {}", response);

            // We're behind and need to catch up CMS first.
            if (response.current > ClusterMetadata.current().epoch.getEpoch())
                ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(response.current));

            if (response.current >= from)
                return response;
        }
        catch (Throwable e)
        {
            logger.info("Failed to fetch epochs [{}, {}] from {}", from, metadata.epoch.getEpoch(), peers);
        }

        // After trying to contact all peers, and retrying according to retry spec on them, we give up.
        // If there were no new known TCM epochs, we still allow Accord to start up, assuming there are no new epochs.
        return null;
    }

    @Override
    public IVerbHandler<? extends Request> requestHandler()
    {
        return requestHandler;
    }

    @Override
    public IVerbHandler<? extends Reply> responseHandler()
    {
        return responseHandler;
    }

    public ShardDurability.ImmutableView shardDurability()
    {
        return node.durability().shards().immutableView();
    }

    @Override
    public AsyncChain<Void> sync(Object requestedBy, Timestamp minBound, Ranges ranges, @Nullable Collection<Id> include, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote, long timeout, TimeUnit timeoutUnits)
    {
        return node.durability().sync(requestedBy, ExclusiveSyncPoint, minBound, ranges, include, syncLocal, syncRemote, timeout, timeoutUnits);
    }

    @Override
    public AsyncChain<Void> sync(Timestamp minBound, Keys keys, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote)
    {
        if (keys.size() != 1)
            return syncInternal(minBound, keys, syncLocal, syncRemote);

        return KeyBarriers.find(node, minBound, keys.get(0).toUnseekable(), syncLocal, syncRemote)
                          .flatMap(found -> KeyBarriers.await(node, node.someSequentialExecutor(), found, syncLocal, syncRemote))
                          .flatMap(success -> {
                              if (success)
                                  return null;
                              return syncInternal(minBound, keys, syncLocal, syncRemote);
                          });
    }

    private AsyncChain<Void> syncInternal(Timestamp minBound, Keys keys, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote)
    {
        TxnId txnId = node.nextTxnId(minBound, keys, Write);
        FullRoute<?> route = node.computeRoute(txnId, keys);
        return node.withEpochAtLeast(txnId.epoch(), null, () -> {
            Txn txn = new Txn.InMemory(Write, keys, TxnRead.createNoOpRead(keys), TxnQuery.UNSAFE_EMPTY, TxnUpdate.empty(), new TableMetadatasAndKeys(TableMetadatas.none(), keys));
            return CoordinateTransaction.coordinate(node, route, txnId, txn)
                                        .map(ignore -> (Void) null).beginAsResult();
        }).beginAsResult();
    }

    @Override
    public AsyncChain<Timestamp> maxConflict(Ranges ranges)
    {
        return CoordinateMaxConflict.maxConflict(node, ranges);
    }

    public static <V> V getBlocking(AsyncChain<V> async, Seekables<?, ?> keysOrRanges, RequestBookkeeping bookkeeping, long startedAt, long deadline, boolean isTxnRequest)
    {
        return getBlocking(async, null, keysOrRanges, bookkeeping, startedAt, deadline, isTxnRequest);
    }

    public static <V> V getBlocking(AsyncChain<V> async, @Nullable TxnId txnId, Seekables<?, ?> keysOrRanges, RequestBookkeeping bookkeeping, long startedAt, long deadline, boolean isTxnRequest)
    {
        AccordResult<V> result = new AccordResult<>(txnId, keysOrRanges, bookkeeping, startedAt, deadline, isTxnRequest);
        async.begin(result);
        return result.awaitAndGet();
    }
    public static <V> V getBlocking(AsyncChain<V> async, Seekables<?, ?> keysOrRanges, RequestBookkeeping bookkeeping, long startedAt, long deadline)
    {
        return getBlocking(async, keysOrRanges, bookkeeping, startedAt, deadline, false);
    }

    public static Keys intersecting(Keys keys)
    {
        if (keys.isEmpty())
            return keys;

        TableId tableId = tableId(keys, r -> ((AccordRoutableKey)r).table());
        return sliceToAccord(tableId, keys, Keys::slice);
    }

    public static Ranges intersecting(Ranges ranges)
    {
        if (ranges.isEmpty())
            return ranges;

        TableId tableId = tableId(ranges, r -> ((TokenRange)r).table());
        return sliceToAccord(tableId, ranges, Ranges::slice);
    }

    private static <C extends Seekables<?, ?>> C sliceToAccord(TableId tableId, C collection, BiFunction<C, Ranges, C> slice)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        TableMetadata tm = getTableMetadata(cm, tableId);

        // Barriers can be needed just because it's an Accord managed range, but it could also be a migration back to Paxos
        // in which case we do want to barrier the migrating/migrated ranges even though the target for the migration is not Accord
        // In either case Accord should be aware of those ranges and not generate a topology mismatch
        if (tm.params.transactionalMode != TransactionalMode.off || tm.params.transactionalMigrationFrom.migratingFromAccord())
        {
            TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);
            // null is fine could be completely migrated or was always an Accord table on creation
            if (tms == null)
                return collection;
            // Use migratingAndMigratedRanges (not accordSafeToReadRanges) because barriers are allowed even if Accord can't perform
            // a read because they are only finishing/recovering existing Accord transactions
            Ranges migratingAndMigratedRanges = AccordTopology.toAccordRanges(tms.tableId, tms.migratingAndMigratedRanges);
            return slice.apply(collection, migratingAndMigratedRanges);
        }

        return slice.apply(collection, Ranges.EMPTY);
    }


    private static <S extends Seekable, C extends Seekables<S, ?>> TableId tableId(C collection, Function<S, TableId> getTableId)
    {
        TableId tableId = getTableId.apply(collection.get(0));
        for (int i = 1, maxi = collection.size() ; i < maxi ; ++i)
        {
            TableId check = getTableId.apply(collection.get(i));
            Invariants.require(tableId.equals(check), "Currently only one table is handled here.");
        }
        return tableId;
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
    public @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime, long minHlc) throws RequestExecutionException
    {
        return coordinateAsync(minEpoch, txn, consistencyLevel, requestTime, minHlc).awaitAndGet();
    }

    @Override
    public List<AccordExecutor> executors()
    {
        return ((AccordCommandStores)node.commandStores()).executors();
    }

    @Override
    public @Nonnull IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime, long minHlc)
    {
        TxnId txnId = node.nextTxnId(minHlc >= 0 ? minHlc : 0, txn.kind(), txn.keys().domain(), cardinality(txn.keys()));
        long timeout = txnId.isWrite() ? DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS) : DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS);
        ClientRequestBookkeeping bookkeeping = txn.isWrite() ? accordWriteBookkeeping : accordReadBookkeeping;
        bookkeeping.metrics.keySize.update(txn.keys().size());
        long deadlineNanos = requestTime.computeDeadline(timeout);
        AccordResult<TxnResult> result = new AccordResult<>(txnId, txn.keys(), bookkeeping, requestTime.startedAtNanos(), deadlineNanos, true);
        ((AsyncResult)node.coordinate(txnId, txn, minEpoch, deadlineNanos)).begin(result);
        return result;
    }

    @Override
    public void setCacheSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setCapacity(bytes);
    }

    @Override
    public void setWorkingSetSize(long kb)
    {
        long bytes = kb << 10;
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        commandStores.setWorkingSetSize(bytes);
    }

    @Override
    public boolean isTerminated()
    {
        return scheduler.isTerminated();
    }

    public synchronized Future<Void> flushCaches()
    {
        class Ready extends AsyncResults.CountingResult implements Runnable
        {
            public Ready() { super(1); }
            @Override public void run() { decrement(); }
        }
        Ready ready = new Ready();
        AccordCommandStores commandStores = (AccordCommandStores) node.commandStores();
        AsyncChains.getBlockingAndRethrow(commandStores.forEach((PreLoadContext.Empty)() -> "Flush Caches", safeStore -> {
            AccordCommandStore commandStore = (AccordCommandStore)safeStore.commandStore();
            try (AccordCommandStore.ExclusiveCaches caches = commandStore.lockCaches())
            {
                caches.commandsForKeys().forEach(entry -> {
                    if (entry.isModified())
                    {
                        ready.increment();
                        caches.global().saveWhenReadyExclusive(entry, ready);
                    }
                });
            }
        }));
        ready.decrement();
        AsyncPromise<Void> result = new AsyncPromise<>();
        ready.begin((success, fail) -> {
            if (fail != null) result.tryFailure(fail);
            else result.trySuccess(null);
        });
        return result;
    }

    public synchronized void markShuttingDown()
    {
        state = State.SHUTTING_DOWN;
    }

    @Override
    public synchronized void shutdown()
    {
        if (state != State.STARTED && state != State.SHUTTING_DOWN)
            return;
        state = State.SHUTTING_DOWN;
        shutdownAndWait(1, TimeUnit.MINUTES);
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
    public void shutdownAndWait(long timeout, TimeUnit unit)
    {
        if (!ExecutorUtils.shutdownSequentiallyAndWait(shutdownableSubsystems(), timeout, unit))
            logger.error("One or more subsystems did not shut down cleanly.");
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
        return AsyncChains.allOf(chains);
    }

    private AsyncChain<CommandStoreTxnBlockedGraph> loadDebug(TxnId txnId, CommandStore store)
    {
        CommandStoreTxnBlockedGraph.Builder state = new CommandStoreTxnBlockedGraph.Builder(store.id());
        populateAsync(state, store, txnId);
        return state;
    }

    private static void populate(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TxnId blockedBy)
    {
        if (safeStore.ifLoadedAndInitialised(blockedBy) != null) populateSync(state, safeStore, blockedBy);
        else populateAsync(state, safeStore.commandStore(), blockedBy);
    }

    private static void populateAsync(CommandStoreTxnBlockedGraph.Builder state, CommandStore store, TxnId txnId)
    {
        state.asyncTxns.incrementAndGet();
        store.execute(PreLoadContext.contextFor(txnId, "Populate txn_blocked_by"), in -> {
            populateSync(state, (AccordSafeCommandStore) in, txnId);
            if (0 == state.asyncTxns.decrementAndGet() && 0 == state.asyncKeys.get())
                state.complete();
        });
    }

    @Nullable
    private static void populateSync(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TxnId txnId)
    {
        try
        {
            if (state.txns.containsKey(txnId))
                return; // could plausibly request same txn twice

            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            Invariants.nonNull(safeCommand, "Txn %s is not in the cache", txnId);
            if (safeCommand.current() == null || safeCommand.current().saveStatus() == SaveStatus.Uninitialised)
                return;

            CommandStoreTxnBlockedGraph.TxnState cmdTxnState = populateSync(state, safeCommand.current());
            if (cmdTxnState.notBlocked())
                return;

            for (TxnId blockedBy : cmdTxnState.blockedBy)
            {
                if (!state.knows(blockedBy))
                    populate(state, safeStore, blockedBy);
            }
            for (TokenKey blockedBy : cmdTxnState.blockedByKey)
            {
                if (!state.keys.containsKey(blockedBy))
                    populate(state, safeStore, blockedBy, txnId, safeCommand.current().executeAt());
            }
        }
        catch (Throwable t)
        {
            state.tryFailure(t);
        }
    }

    private static void populate(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TokenKey blockedBy, TxnId txnId, Timestamp executeAt)
    {
        if (safeStore.ifLoadedAndInitialised(txnId) != null && safeStore.ifLoadedAndInitialised(blockedBy) != null) populateSync(state, safeStore, blockedBy, txnId, executeAt);
        else populateAsync(state, safeStore.commandStore(), blockedBy, txnId, executeAt);
    }

    private static void populateAsync(CommandStoreTxnBlockedGraph.Builder state, CommandStore commandStore, TokenKey blockedBy, TxnId txnId, Timestamp executeAt)
    {
        state.asyncKeys.incrementAndGet();
        commandStore.execute(PreLoadContext.contextFor(txnId, RoutingKeys.of(blockedBy.toUnseekable()), SYNC, READ_WRITE, "Populate txn_blocked_by"), in -> {
            populateSync(state, (AccordSafeCommandStore) in, blockedBy, txnId, executeAt);
            if (0 == state.asyncKeys.decrementAndGet() && 0 == state.asyncTxns.get())
                state.complete();
        });
    }

    private static void populateSync(CommandStoreTxnBlockedGraph.Builder state, AccordSafeCommandStore safeStore, TokenKey pk, TxnId txnId, Timestamp executeAt)
    {
        try
        {
            SafeCommandsForKey commandsForKey = safeStore.ifLoadedAndInitialised(pk);
            TxnId blocking = commandsForKey.current().blockedOnTxnId(txnId, executeAt);
            if (blocking instanceof CommandsForKey.TxnInfo)
                blocking = ((CommandsForKey.TxnInfo) blocking).plainTxnId();
            state.keys.put(pk, blocking);
            if (state.txns.containsKey(blocking))
                return;
            populate(state, safeStore, blocking);
        }
        catch (Throwable t)
        {
            state.tryFailure(t);
        }
    }

    private static CommandStoreTxnBlockedGraph.TxnState populateSync(CommandStoreTxnBlockedGraph.Builder state, Command cmd)
    {
        CommandStoreTxnBlockedGraph.Builder.TxnBuilder cmdTxnState = state.txn(cmd.txnId(), cmd.executeAt(), cmd.saveStatus());
        if (!cmd.hasBeen(Status.Applied) && cmd.hasBeen(Status.Stable))
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
                    cmdTxnState.blockedByKey.add((TokenKey) waitingOn.keys.get(i - waitingOn.txnIdCount()));
                }
            });
        }
        return cmdTxnState.build();
    }

    @Override
    public long minEpoch()
    {
        return node.topology().minEpoch();
    }

    public Node node()
    {
        return node;
    }

    @Override
    public void ensureMinHlc(long minHlc)
    {
        node.updateMinHlc(minHlc >= 0 ? minHlc : 0);
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
    public Future<Void> epochReadyFor(ClusterMetadata metadata)
    {
        if (!metadata.schema.hasAccordKeyspaces())
            return EPOCH_READY;

        return epochReady(metadata.epoch);
    }

    @Override
    public void receive(Message<Notification> message)
    {
        receive(MessagingService.instance(), configService, message);
    }

    @VisibleForTesting
    public static void receive(MessageDelivery sink, AbstractConfigurationService<?, ?> configService, Message<Notification> message)
    {
        AccordSyncPropagator.Notification notification = message.payload;
        notification.syncComplete.forEach(id -> configService.receiveRemoteSyncComplete(id, notification.epoch));
        if (!notification.closed.isEmpty())
            configService.receiveClosed(notification.closed, notification.epoch);
        if (!notification.retired.isEmpty())
            configService.receiveRetired(notification.retired, notification.epoch);
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
                // TODO (required): expose awaitTermination in Node
                // node doesn't offer
                return true;
            }
        };
    }

    @VisibleForTesting
    public AccordConfigurationService configService()
    {
        return configService;
    }

    @Override
    public AccordCompactionInfos getCompactionInfo()
    {
        AccordCompactionInfos compactionInfos = new AccordCompactionInfos(node.durableBefore(), node.topology().minEpoch());
        node.commandStores().forEachCommandStore(commandStore -> {
            compactionInfos.put(commandStore.id(), ((AccordCommandStore)commandStore).getCompactionInfo());
        });
        return compactionInfos;
    }

    @Override
    public AccordAgent agent()
    {
        return (AccordAgent) node.agent();
    }

    @Override
    public void awaitDone(TableId id, long epoch)
    {
        // Need to make sure no existing txn are still being processed for this table... this is only used by DROP TABLE so NEW txn are expected to be blocked, so just need to "wait" for existing ones to complete
        Topology topology = node.topology().current();
        List<TokenRange> rangeList = new ArrayList<>();
        for (Shard shard : topology.shards())
        {
            TokenRange range = (TokenRange) shard.range;
            if (id.equals(range.table()))
                rangeList.add(range);
        }
        if (rangeList.isEmpty()) return; // nothing to see here

        Ranges ranges = Ranges.of(rangeList.toArray(accord.primitives.Range[]::new));
        long timeout = DatabaseDescriptor.getAccordRepairTimeoutNanos();
        long startedAt = nanoTime();
        long deadline = startedAt + timeout;
        // TODO (required): relax this requirement - too expensive
        getBlocking(node.durability().sync("Drop Keyspace/Table (Epoch " + epoch + ')', ExclusiveSyncPoint, TxnId.minForEpoch(epoch), ranges, Self, All, DatabaseDescriptor.getAccordRangeSyncPointTimeoutNanos(), NANOSECONDS), ranges, new LatencyRequestBookkeeping(null), startedAt, deadline, false);
    }

    public Params journalConfiguration()
    {
        return journal.configuration();
    }
}
