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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProtocolModifiers;
import accord.coordinate.CoordinateMaxConflict;
import accord.coordinate.CoordinateTransaction;
import accord.coordinate.KeyBarriers;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.RequestCallbacks;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.progresslog.DefaultProgressLog;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.local.Catchup;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.ShardDistributor.EvenSplit;
import accord.local.UniqueTimeService.AtomicUniqueTimeWithStaleReservation;
import accord.local.durability.DurabilityService;
import accord.local.durability.ShardDurability;
import accord.messages.Reply;
import accord.messages.Request;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.ActiveEpochs;
import accord.topology.EpochReady;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.topology.TopologyRange;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspace.BootstrapState;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.AccordExecutorMetrics;
import org.apache.cassandra.metrics.AccordReplicaMetrics;
import org.apache.cassandra.metrics.AccordSystemMetrics;
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
import org.apache.cassandra.service.accord.api.AccordViolationHandler;
import org.apache.cassandra.service.accord.api.CompositeTopologySorter;
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
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.api.Journal.TopologyUpdate;
import static accord.api.ProtocolModifiers.Toggles.FastExec.MAY_BYPASS_SAFESTORE;
import static accord.impl.progresslog.DefaultProgressLog.ModeFlag.CATCH_UP;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.durability.DurabilityService.SyncLocal.Self;
import static accord.local.durability.DurabilityService.SyncRemote.All;
import static accord.messages.SimpleReply.Ok;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordCommandStoreShardCount;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordGlobalDurabilityCycle;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordShardDurabilityCycle;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordShardDurabilityMaxSplits;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordShardDurabilityTargetSplits;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.db.SystemKeyspace.BootstrapState.COMPLETED;
import static org.apache.cassandra.journal.Params.ReplayMode.RESET;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadBookkeeping;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteBookkeeping;
import static org.apache.cassandra.service.accord.AccordTopology.tcmIdToAccord;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.getTableMetadata;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordService implements IAccordService, Shutdownable
{
    public static class MetadataChangeListener implements ChangeListener.Async
    {
        // Listener is initialized before Accord is initialized
        public static MetadataChangeListener instance = new MetadataChangeListener();

        interface Sink
        {
            Sink update(ClusterMetadata next);
        }

        /**
         * Collects TCM events from startup util full Accord initialization to avoid races with TCM and creating gaps between
         * epochs restored from journal and reported by TCM.
         **/
        static final class Collector implements Sink
        {
            static final Collector EMPTY = new Collector(new ClusterMetadata[0]);
            final ClusterMetadata[] saved;

            Collector(ClusterMetadata[] saved)
            {
                this.saved = saved;
            }

            @Override
            public Sink update(ClusterMetadata next)
            {
                ClusterMetadata[] newSaved = Arrays.copyOf(saved, saved.length + 1);
                newSaved[saved.length] = next;
                return new Collector(newSaved);
            }

            public List<ClusterMetadata> getItems()
            {
                return Arrays.asList(saved);
            }
        }

        private final AtomicReference<Sink> sink = new AtomicReference<>(Collector.EMPTY);

        private MetadataChangeListener() {}

        @Override
        public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
        {
            while (true)
            {
                Sink curSink = sink.get();
                Sink nextSink = curSink.update(next);
                if (nextSink == curSink || sink.compareAndSet(curSink, nextSink))
                    return;
            }
        }

        Collector replaceSinkIfEmpty(Sink newSink)
        {
            while (true)
            {
                Sink curSink = sink.get();
                Invariants.require(curSink instanceof Collector);
                if (curSink == Collector.EMPTY)
                {
                    if (sink.compareAndSet(curSink, newSink))
                        return null;
                }
                else
                {
                    if (sink.compareAndSet(curSink, Collector.EMPTY))
                        return (Collector) curSink;
                }
            }
        }

        @VisibleForTesting
        public void unsafeResetForTesting(ClusterMetadata metadata)
        {
            sink.set(Collector.EMPTY.update(metadata));
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
    private final AccordMessageSink messageSink;
    private final AccordEndpointMapper endpointMapper;
    private final AccordTopologyService topologyService;
    private final AccordFastPathCoordinator fastPathCoordinator;
    private final AccordScheduler scheduler;
    private final AccordDataStore dataStore;
    private final AccordJournal journal;
    private final AccordVerbHandler<? extends Request> requestHandler;
    private final AccordResponseVerbHandler<? extends Reply> responseHandler;

    @GuardedBy("this")
    private volatile State state = State.INIT;

    private static final IAccordService NOOP_SERVICE = new NoOpAccordService();

    // TODO (expected): wrap this in an inner class that is statically initialised and final
    //  tests can specify a DelegatingService if they want to override
    private static IAccordService instance;
    private static IAccordService unsafeInstance;

    @VisibleForTesting
    public static void unsafeSetNewAccordService(IAccordService service)
    {
        unsafeInstance = instance = service;
    }

    @VisibleForTesting
    public static void unsafeSetNoop()
    {
        unsafeInstance = instance = NOOP_SERVICE;
    }

    public static IAccordService tryGetUnsafe()
    {
        return unsafeInstance;
    }

    public static boolean isSetup()
    {
        return instance != null;
    }

    public static boolean isSetupOrStarting()
    {
        return unsafeInstance != null;
    }

    public static boolean isStarted()
    {
        return isSetup() && unsafeInstance instanceof AccordService && ((AccordService) unsafeInstance).state == State.STARTED;
    }

    public static IVerbHandler<Void> watermarkHandlerOrNoop()
    {
        if (!isSetup()) return ignore -> {};
        AccordService i = (AccordService) instance();
        return i.topologyService().watermarkCollector().handler;
    }

    public static IVerbHandler<? extends Request> requestHandlerOrNoop()
    {
        if (instance == null) return ignore -> {};
        return instance.requestHandler();
    }

    public static IVerbHandler<? extends Reply> responseHandlerOrNoop()
    {
        if (unsafeInstance == null) return ignore -> {};
        return unsafeInstance.responseHandler();
    }

    @VisibleForTesting
    public synchronized static void localStartup(NodeId tcmId)
    {
        Invariants.require(instance == null);
        if (!DatabaseDescriptor.getAccordTransactionsEnabled())
        {
            unsafeInstance = instance = NOOP_SERVICE;
        }
        else
        {
            AccordService as = new AccordService(tcmIdToAccord(tcmId));
            unsafeInstance = as;
            as.localStartup();
        }
    }

    public synchronized static AccordService distributedStartup()
    {
        if (unsafeInstance == NOOP_SERVICE)
            return null;

        AccordService as = (AccordService) unsafeInstance;
        if (as.state != State.INIT)
            return as;

        as.distributedStartupInternal();
        instance = as;

        AccordReplicaMetrics.touch();
        AccordSystemMetrics.touch();
        AccordExecutorMetrics.touch();
        AccordViolationHandler.setup();
        return as;
    }

    @VisibleForTesting
    public static boolean replayJournal(AccordService as)
    {
        logger.info("Starting journal replay.");
        long start = nanoTime();
        if (as.journalConfiguration().replayMode() == RESET)
            AccordKeyspace.truncateCommandsForKey();

        as.node.commandStores().forAllUnsafe(cs -> cs.unsafeProgressLog().stop());
        as.journal().replay(as.node().commandStores());
        logger.info("Waiting for command stores to quiesce.");
        ((AccordCommandStores)as.node.commandStores()).waitForQuiescence();
        as.journal.unsafeSetStarted();
        as.node.commandStores().forAllUnsafe(cs -> cs.unsafeProgressLog().start());

        long end = nanoTime();
        logger.info("Finished journal replay. {}s elapsed", String.format("%.2f", NANOSECONDS.toMillis(end - start)/1000.0));
        return true;
    }

    public static IAccordService instance()
    {
        IAccordService i = instance;
        Invariants.require(i != null, "AccordService was not started");
        return i;
    }

    public static IAccordService unsafeInstance()
    {
        IAccordService i = unsafeInstance;
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
        this.endpointMapper = new EndpointMapping.Updateable();
        this.topologyService = new AccordTopologyService(localId, endpointMapper);
        this.fastPathCoordinator = AccordFastPathCoordinator.create(localId, endpointMapper);
        this.messageSink = new AccordMessageSink(endpointMapper, callbacks);
        this.node = new Node(localId,
                             messageSink,
                             topologyService,
                             time, new AtomicUniqueTimeWithStaleReservation(time),
                             () -> dataStore,
                             new KeyspaceSplitter(new EvenSplit<>(getAccordCommandStoreShardCount(), getPartitioner().accordSplitter())),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             CompositeTopologySorter.create(SizeOfIntersectionSorter.SUPPLIER,
                                                            new AccordTopologySorter.Supplier(endpointMapper, DatabaseDescriptor.getNodeProximity())),
                             DefaultRemoteListeners::new,
                             ignore -> callbacks,
                             DefaultProgressLogs::new,
                             DefaultLocalListeners.Factory::new,
                             AccordCommandStores.factory(),
                             new AccordInteropFactory(endpointMapper),
                             journal.durableBeforePersister(),
                             journal);
        this.requestHandler = new AccordVerbHandler<>(node, endpointMapper);
        this.responseHandler = new AccordResponseVerbHandler<>(callbacks, endpointMapper);
    }

    @Override
    public synchronized void localStartup()
    {
        if (state != State.INIT)
            return;

        node.unsafeSetReplaying(true);
        try
        {
            journal.start(node);
            node.load();


            ClusterMetadata metadata = ClusterMetadata.current();
            endpointMapper.updateMapping(metadata);

            List<TopologyUpdate> images = journal.replayTopologies();
            if (!images.isEmpty())
            {
                // Initialise command stores using latest topology from the log;
                // if there are no local command stores, don't report any topologies and simply fetch the latest known in the cluster
                // this avoids a registered (not joined) node learning of topologies, then later restarting with some intervening
                // epochs having been garbage collected by the other nodes in the cluster
                TopologyUpdate last = images.get(images.size() - 1);
                if (!last.commandStores.isEmpty())
                {
                    node.commandStores().initializeTopologyUnsafe(last);

                    // Replay local epochs
                    for (TopologyUpdate image : images)
                        node.topology().reportTopology(image.global);
                }
            }
            replayJournal(this);
        }
        finally
        {
            node.unsafeSetReplaying(false);
        }
    }

    private void distributedStartupInternal()
    {
        finishTopologyInitialization();
        WatermarkCollector.fetchAndReportWatermarksAsync(topology());

        catchup();

        fastPathCoordinator.start();
        ClusterMetadataService.instance().log().addListener(fastPathCoordinator);

        node.durability().shards().reconfigure(Ints.checkedCast(getAccordShardDurabilityTargetSplits()),
                                               Ints.checkedCast(getAccordShardDurabilityMaxSplits()),
                                               Ints.checkedCast(getAccordShardDurabilityCycle(SECONDS)), SECONDS);
        node.durability().global().setGlobalCycleTime(Ints.checkedCast(getAccordGlobalDurabilityCycle(SECONDS)), SECONDS);
        // Only enable durability scheduling and progress logs _after_ we have fully replayed journal
        node.durability().start();
        // we set ourselves to STARTED before starting progress logs as this is the condition we use to decide if we
        // start the progress log on command store initialisation (so creates a synchronisation point)
        state = State.STARTED;
        node.commandStores().forAll("", safeStore -> safeStore.progressLog().start());
    }

    void catchup()
    {
        AccordSpec spec = DatabaseDescriptor.getAccord();
        if (!spec.catchup_on_start)
        {
            logger.info("Catchup disabled; continuing to startup");
            return;
        }

        BootstrapState bootstrapState = SystemKeyspace.getBootstrapState();
        if (bootstrapState == COMPLETED)
        {
            node.commandStores().forAllUnsafe(commandStore -> ((DefaultProgressLog)commandStore.unsafeProgressLog()).setMode(CATCH_UP));
            try
            {
                long maxLatencyNanos = spec.catchup_on_start_fail_latency.toNanoseconds();
                int attempts = 1;
                while (true)
                {
                    logger.info("Catchup with quorum...");
                    long start = nanoTime();
                    long failAt = start + maxLatencyNanos;
                    Future<Void> f = toFuture(Catchup.catchup(node));
                    if (!f.awaitUntilThrowUncheckedOnInterrupt(failAt))
                    {
                        if (spec.catchup_on_start_exit_on_failure)
                        {
                            logger.error("Catchup exceeded maximum latency of {}ns; shutting down", maxLatencyNanos);
                            throw new RuntimeException("Could not catchup with peers");
                        }
                        logger.error("Catchup exceeded maximum latency of {}ns; continuing to startup", maxLatencyNanos);
                        break;
                    }

                    Throwable failed = f.cause();
                    if (failed != null)
                    {
                        if (spec.catchup_on_start_exit_on_failure)
                            throw new RuntimeException("Could not catchup with peers", failed);

                        logger.error("Could not catchup with peers; continuing to startup");
                        break;
                    }

                    long end = nanoTime();
                    double seconds = NANOSECONDS.toMillis(end - start)/1000.0;
                    logger.info("Finished catchup with all quorums. {}s elapsed.", String.format("%.2f", seconds));

                    if (seconds <= spec.catchup_on_start_success_latency.toSeconds())
                        break;

                    if (++attempts > spec.catchup_on_start_max_attempts)
                    {
                        if (spec.catchup_on_start_exit_on_failure)
                        {
                            logger.error("Catchup was slow, aborting after {} attempts and shutting down", attempts);
                            throw new RuntimeException("Could not catchup with peers");
                        }

                        logger.info("Catchup was slow; continuing to startup after {} attempts.", attempts - 1);
                        break;
                    }

                    logger.info("Catchup was slow, so we may behind again; retrying");
                }
            }
            finally
            {
                node.commandStores().forAllUnsafe(commandStore -> ((DefaultProgressLog)commandStore.unsafeProgressLog()).unsetMode(CATCH_UP));
            }
        }
        else
        {
            logger.info("No catchup, as bootstrap state is {}", bootstrapState);
        }
    }

    /**
     * Startup is broken up in two phases: local and distributed startup. During local startup, we replay up to
     *  the latest epoch known to the node prior to restart. After that, we replay journal itself, and only after
     *  that we finish initialization and replay the rest of epochs.
     */
    void finishTopologyInitialization()
    {
        endpointMapper.updateMapping(ClusterMetadata.current());
        TopologyManager topology = node.topology();
        long highestKnown = -1;
        if (!topology.active().isEmpty())
        {
            highestKnown = topology.active().epoch();
        }
        try
        {
            TopologyRange remote = fetchTopologies(highestKnown + 1);

            if (remote != null) // TODO (required): if remote.min > highestKnown + 1, should we decide if we need to truncate our local topologies? Probably not until startup has finished.
            {
                remote.forEach(node.topology()::reportTopology, remote.min, Integer.MAX_VALUE);
                if (remote.current > highestKnown)
                    highestKnown = remote.current;
            }

            // Subscribe to TCM events, and collect any we may have missed to report now
            MetadataChangeListener.Sink sink = new MetadataChangeListener.Sink()
            {
                @Override
                public MetadataChangeListener.Sink update(ClusterMetadata next)
                {
                    if (state != State.SHUTDOWN)
                        maybeReportMetadata(next);
                    return this;
                }
            };

            while (true)
            {
                MetadataChangeListener.Collector collector = MetadataChangeListener.instance.replaceSinkIfEmpty(sink);
                if (collector == null)
                    break;

                for (ClusterMetadata item : collector.getItems())
                {
                    if (item.epoch.getEpoch() > highestKnown)
                        maybeReportMetadata(item);
                }
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

    private void maybeReportMetadata(ClusterMetadata metadata)
    {
        endpointMapper.updateMapping(metadata);

        ActiveEpochs epochs = topology().active();
        if (!metadata.schema.hasAccordKeyspaces() && epochs.isEmpty())
            return;

        if (epochs.epoch() >= metadata.epoch.getEpoch())
            return;

        // On first boot, we have 2 options:
        //
        //  - we can start listening to TCM _before_ we replay topologies
        //  - we can start listening to TCM _after_ we replay topologies
        //
        // If we start listening to TCM _before_ we replay topologies from other nodes,
        // we may end up in a situation where TCM reports metadata that would create an
        // `epoch - 1` epoch state that is not associated with any topologies, and
        // therefore should not be listened upon.
        //
        // If we start listening to TCM _after_ we replay topologies, we may end up in a
        // situation where TCM reports metadata that is 1 (or more) epochs _ahead_ of the
        // last known epoch. Previous implementations were using TCM peer catch up, which
        // could have resulted in gaps.
        //
        // Current protocol solves both problems by _first_ replaying topologies form peers,
        // then subscribing to TCM _and_, if there are still any gaps, filling them again.
        // However, it still has a slight chance of creating an `epoch - 1` epoch state
        // not associated with any topologies, which under "right" circumstances could
        // have been waited upon with `epochReady`. This check precludes creation of this
        // epoch: by the time this code can be called, remote topology replay is already
        // done, so TCM listener will only report epochs that are _at least_ min epoch.
        Topology topology = AccordTopology.createAccordTopology(metadata);
        topology().reportTopology(topology);
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
    public AsyncResult<Void> sync(Object requestedBy, Timestamp minBound, Ranges ranges, @Nullable Collection<Id> include, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote, long timeout, TimeUnit timeoutUnits)
    {
        return node.durability().sync(requestedBy, ExclusiveSyncPoint, minBound, ranges, include, syncLocal, syncRemote, timeout, timeoutUnits);
    }

    @Override
    public AsyncChain<Void> sync(Timestamp minBound, Keys keys, DurabilityService.SyncLocal syncLocal, DurabilityService.SyncRemote syncRemote)
    {
        if (keys.size() != 1)
            return syncInternal(minBound, keys, syncLocal, syncRemote);

        return KeyBarriers.find(node, minBound, keys.get(0).toUnseekable(), syncLocal, syncRemote).chain()
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
        return node.withEpochAtLeast(txnId.epoch(), null, () -> {
            Txn txn = new Txn.InMemory(Write, keys, TxnRead.createNoOpRead(keys), TxnQuery.UNSAFE_EMPTY, TxnUpdate.empty(), new TableMetadatasAndKeys(TableMetadatas.none(), keys));
            return CoordinateTransaction.coordinate(node, txnId, txn)
                                        .mapToNull();
        });
    }

    @Override
    public AsyncChain<Timestamp> maxConflict(Ranges ranges)
    {
        return CoordinateMaxConflict.maxConflict(node, ranges);
    }

    static class AsyncFutureCallback<V> extends AsyncFuture<V> implements BiConsumer<V, Throwable>
    {
        @Override
        public void accept(V v, Throwable fail)
        {
            if (fail == null) trySuccess(v);
            else tryFailure(fail);
        }
    }

    public static <V> Future<V> toFuture(AsyncChain<V> chain)
    {
        AsyncFutureCallback<V> future = new AsyncFutureCallback<>();
        chain.begin(future);
        return future;
    }

    public static <V> Future<V> toFuture(AsyncResult<V> result)
    {
        if (result instanceof Future<?>)
            return (Future<V>) result;

        AsyncPromise<V> promise = new AsyncPromise<>();
        result.invoke((success, failure) -> {
            if (failure == null) promise.trySuccess(success);
            else promise.tryFailure(failure);
        });
        return promise;
    }

    public static <V> V getBlocking(AsyncChain<V> async)
    {
        return syncAndRethrow(toFuture(async)).getNow();
    }

    public static <V> V getBlocking(AsyncResult<V> async)
    {
        return syncAndRethrow(toFuture(async)).getNow();
    }

    private static <V> Future<V> syncAndRethrow(Future<V> future)
    {
        future.syncThrowUncheckedOnInterrupt()
              .rethrowIfFailed();
        return future;
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

    public static <V> V getBlocking(AsyncResult<V> async, Seekables<?, ?> keysOrRanges, RequestBookkeeping bookkeeping, long startedAt, long deadline, boolean isTxnRequest)
    {
        return getBlocking(async, null, keysOrRanges, bookkeeping, startedAt, deadline, isTxnRequest);
    }

    public static <V> V getBlocking(AsyncResult<V> async, @Nullable TxnId txnId, Seekables<?, ?> keysOrRanges, RequestBookkeeping bookkeeping, long startedAt, long deadline, boolean isTxnRequest)
    {
        AccordResult<V> result = new AccordResult<>(txnId, keysOrRanges, bookkeeping, startedAt, deadline, isTxnRequest);
        async.invoke(result);
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
        return sliceToAccord(tableId, keys, Keys::overlapping);
    }

    public static Ranges intersecting(Ranges ranges)
    {
        if (ranges.isEmpty())
            return ranges;

        TableId tableId = tableId(ranges, r -> ((TokenRange)r).table());
        return sliceToAccord(tableId, ranges, Ranges::overlapping);
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
        return topology().epoch();
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
        return coordinateAsync(minEpoch, minHlc, txn, consistencyLevel, requestTime).awaitAndGet();
    }

    @Override
    public List<AccordExecutor> executors()
    {
        return ((AccordCommandStores)node.commandStores()).executors();
    }

    @Override
    public boolean isEnabled()
    {
        return true;
    }

    @Override
    public @Nonnull IAccordResult<TxnResult> coordinateAsync(long minEpoch, long minHlc, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime)
    {
        TxnId txnId = node.nextTxnId(minEpoch, minHlc, txn);
        long timeout = txnId.isWrite() ? DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS) : DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS);
        ClientRequestBookkeeping bookkeeping = txn.isWrite() ? accordWriteBookkeeping : accordReadBookkeeping;
        bookkeeping.metrics.keySize.update(txn.keys().size());
        long deadlineNanos = requestTime.computeDeadline(timeout);
        AccordResult<TxnResult> result = new AccordResult<>(txnId, txn.keys(), bookkeeping, requestTime.startedAtNanos(), deadlineNanos, true);
        node.coordinate(txnId, txn, minEpoch, deadlineNanos).begin((BiConsumer) result);
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
        commandStores.forAllUnsafe(unsafeStore -> {
            AccordCommandStore commandStore = (AccordCommandStore)unsafeStore;
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
        });
        ready.decrement();
        AsyncPromise<Void> result = new AsyncPromise<>();
        ready.invoke((success, fail) -> {
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
        return Arrays.asList((AccordCommandStores)node.commandStores(), journal, topologyService, scheduler);
    }

    @VisibleForTesting
    @Override
    public void shutdownAndWait(long timeout, TimeUnit unit)
    {
        if (!ExecutorUtils.shutdownThenWait(shutdownableSubsystems(), timeout, unit))
            logger.error("One or more subsystems did not shut down cleanly.");

        node.commandStores().forAllUnsafe(commandStore -> {
            logger.info("{} stopping with durability: {}", commandStore, commandStore.unsafeGetRedundantBefore().map(b -> b == null ? null : b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE), TxnId[]::new));
        });
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
        toFuture(node.updateMinHlc(minHlc >= 0 ? minHlc : 0)).syncUninterruptibly();
    }

    public AccordJournal journal()
    {
        return journal;
    }

    @Override
    public Future<Void> epochReady(Epoch epoch, Function<EpochReady, AsyncResult<Void>> get)
    {
        return toFuture(topology().epochReady(epoch.getEpoch(), get));
    }

    @Override
    public Future<Void> epochReadyFor(ClusterMetadata metadata, Function<EpochReady, AsyncResult<Void>> get)
    {
        if (!metadata.schema.hasAccordKeyspaces())
            return EPOCH_READY;

        return epochReady(metadata.epoch, get);
    }

    @Override
    public void receive(Message<Notification> message)
    {
        receive(MessagingService.instance(), node.topology(), message);
    }

    @VisibleForTesting
    public static void receive(MessageDelivery sink, TopologyManager topologyManager, Message<Notification> message)
    {
        AccordSyncPropagator.Notification notification = message.payload;
        notification.readyToCoordinate.forEach(id -> topologyManager.onReadyToCoordinate(id, notification.epoch));
        if (!notification.closed.isEmpty())
            topologyManager.onEpochClosed(notification.closed, notification.epoch);
        if (!notification.retired.isEmpty())
            topologyManager.onEpochRetired(notification.retired, notification.epoch);
        sink.respond(Ok, message);
    }

    @VisibleForTesting
    public AccordEndpointMapper endpointMapper()
    {
        return endpointMapper;
    }

    @Override
    public AccordTopologyService topologyService()
    {
        return topologyService;
    }

    @Override
    public AccordCompactionInfos getCompactionInfo()
    {
        AccordCompactionInfos compactionInfos = new AccordCompactionInfos(node.durableBefore(), node.topology().minEpoch());
        node.commandStores().forAllUnsafe(commandStore -> {
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

    @Override
    public Map<TableId, Set<org.apache.cassandra.dht.Range<Token>>> getInUseRangesAndMarkRetiredRangesUnsafeToRead()
    {
        Map<TableId, Set<org.apache.cassandra.dht.Range<Token>>> tableToRangeMap = new HashMap<>();
        List<Ranges> globalRanges = getBlocking(node.commandStores().getInUseRangesAndMarkRetiredRangesUnsafeToRead());

        for (Ranges ranges : globalRanges)
        {
            for (accord.primitives.Range r : ranges)
            {
                TokenRange tokenRange = (TokenRange) r;
                tableToRangeMap.computeIfAbsent(tokenRange.table(), tableId -> new HashSet<>())
                               .add(tokenRange.toKeyspaceRange());
            }
        }

        return tableToRangeMap;
    }

    public Params journalConfiguration()
    {
        return journal.configuration();
    }
}
