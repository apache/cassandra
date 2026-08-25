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
package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.MutationTrackingSpec;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.MutationTrackingMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.SyncTasks;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.reads.tracked.TrackedLocalReads;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorSemantics.NORMAL;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

// TODO (expected): persistence (handle restarts)
// TODO (expected): handle topology changes
public class MutationTrackingService implements MutationTrackingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=MutationTrackingService";
    public static final String DISABLED_MESSAGE = "Mutation tracking is not enabled. (See mutation_tracking.enabled in cassandra.yaml)";

    private static final MutationTrackingService instance;
    private static final ScheduledExecutorPlus executor;

    private static final MutationTrackingSpec config;

    static
    {
        config = DatabaseDescriptor.getMutationTrackingConfig();

        if (config.enabled)
        {
            instance = new MutationTrackingService();
            executor = executorFactory().scheduled("Mutation-Tracking-Service", NORMAL);
            MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
        }
        else
        {
            instance = null;
            executor = null;
        }
    }

    /**
     * Callers of this method should have validated that mutation tracking is enabled
     */
    public static MutationTrackingService instance()
    {
        if (instance == null)
            throw new IllegalStateException(DISABLED_MESSAGE);
        return instance;
    }

    public static boolean isEnabled()
    {
        return config.enabled;
    }

    public static void ensureEnabled()
    {
        if (!config.enabled)
            throw new IllegalStateException(DISABLED_MESSAGE);
    }

    public static ClusterMetadata register(ChangeListener listener)
    {
        ClusterMetadataService.instance().log().addListener(listener);
        return ClusterMetadata.current();
    }

    public static void start(Function<ChangeListener, ClusterMetadata> register)
    {
        if (!isEnabled())
            return;
        instance().startInternal(register);
    }

    public static void start()
    {
        start(MutationTrackingService::register);
    }

    public static void shutdown() throws InterruptedException
    {
        if (!isEnabled())
            return;
        instance().shutdownBlocking();
    }

    /**
     * Split ranges into this many shards.
     * <p>
     * REVIEW: Reset back to 1 because for transfers, replicas need to know each others' shards, since transfers are
     * sliced to fit within shards. Can we achieve sharding via split range ownership, instead of it being local-only?
     * <p>
     * TODO (expected): ability to rebalance / change this constant
     */
    private static final int SHARD_MULTIPLIER = 1;

    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingService.class);

    private final TrackedLocalReads localReads = new TrackedLocalReads();
    private ConcurrentHashMap<String, KeyspaceShards> keyspaceShards = new ConcurrentHashMap<>();
    private ConcurrentHashMap<CoordinatorLogId, Shard> log2ShardMap = new ConcurrentHashMap<>();
    private final ChangeListener tcmListener;

    // The highest TCM epoch we have applied to keyspaceShards via onNewClusterMetadata.
    // Updates with next.epoch <= this value are skipped. Protects against state going
    // backwards in time when events are delivered out of order
    private volatile Epoch lastAppliedEpoch = Epoch.EMPTY;

    // prevents a race between topology changes (shard recreation) and coordinator log creation.
    //
    // coordinator log creation can race with topology updates and be lost if shard recreation discards the old
    // KeyspaceShards containing newly created logs.
    //
    // the following usage patterns will guard against state corruption during topology changes
    // - Read lock: All normal operations (log creation, mutations, reads)
    // - Write lock: Topology changes only (shard recreation during cluster membership changes)
    //
    // Topology changes are rare vs shard recreation speed, so brief blocking during cluster changes seems acceptable
    // for correctness vs complex protocols topology updates. You could make the case that mutable state would be
    // a better tradeoff for node replacement, but it seems likely that handling token movements will be simpler
    // if we use a copy on write pattern for topology changes.
    // TODO (expected): consider StampedLock or other approaches to avoid theoretical topology change starvation
    private final ReentrantReadWriteLock shardLock = new ReentrantReadWriteLock();

    private final ReplicatedOffsetsBroadcaster offsetsBroadcaster = new ReplicatedOffsetsBroadcaster();
    private final LogStatePersister offsetsPersister = new LogStatePersister();
    private final ActiveLogReconciler activeReconciler = new ActiveLogReconciler();
    private final BackgroundReconciler backgroundReconciler = new BackgroundReconciler();

    private final IncomingMutations incomingMutations = new IncomingMutations();
    private final OutgoingMutations outgoingMutations = new OutgoingMutations();

    private final Map<String, Set<MutationTrackingSyncCoordinator>> syncCoordinatorsByKeyspace = new ConcurrentHashMap<>();

    private volatile boolean started = false;

    private MutationTrackingService()
    {
        this.tcmListener = new ChangeListener()
        {
            @Override
            public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
            {
                onNewClusterMetadata(prev, next);
            }
        };
    }

    private synchronized void startInternal(Function<ChangeListener, ClusterMetadata> register)
    {
        if (started)
            return;

        prevHostLogId = loadHostLogIdFromSystemTable();

        logger.info("Starting mutation tracking service. Previous host log id: {}", prevHostLogId);

        ClusterMetadata metadata = register.apply(tcmListener);

        if (metadata.myNodeId() != null)
            for (KeyspaceShards ks : KeyspaceShards.loadFromSystemTables(metadata, this::nextLogId, this::onNewLog))
                keyspaceShards.put(ks.keyspace, ks);

        onNewClusterMetadata(null, metadata);

        if (!keyspaceShards.isEmpty() && !config.background_reconciliation_enabled)
            logBackgroundReconciliationDisabledWarning(keyspaceShards.keySet());

        offsetsBroadcaster.start();
        offsetsPersister.start();
        backgroundReconciler.start();

        ExpiredStatePurger.instance.register(incomingMutations);

        started = true;
    }

    @Override
    public void setMutationTrackingBackgroundReconciliationEnabled(boolean enabled)
    {
        if (enabled != config.background_reconciliation_enabled)
        {
            logger.info("{} mutation tracking background reconciliation", enabled ? "Enabling" : "Disabling");
            config.background_reconciliation_enabled = enabled;
        }
    }

    @Override
    public boolean getMutationTrackingBackgroundReconciliationEnabled()
    {
        return config.background_reconciliation_enabled;
    }

    @Override
    public void setMutationTrackingBackgroundReconciliationIntervalMilliseconds(long intervalMilliseconds)
    {
        if (intervalMilliseconds  != config.background_reconciliation_interval.toMilliseconds())
        {
            DurationSpec.LongMillisecondsBound backgroundReconciliationInterval =
            new DurationSpec.LongMillisecondsBound(intervalMilliseconds, TimeUnit.MILLISECONDS);
            logger.info("Setting mutation tracking background reconciliation interval from {} to {}",
                        config.background_reconciliation_interval, backgroundReconciliationInterval);
            config.background_reconciliation_interval = backgroundReconciliationInterval;
        }
    }

    @Override
    public long getMutationTrackingBackgroundReconciliationIntervalMilliseconds()
    {
        return config.background_reconciliation_interval.toMilliseconds();
    }

    public void pauseOffsetBroadcast(boolean pause)
    {
        offsetsBroadcaster.pauseOffsetBroadcast(pause);
    }

    /**
     * Creates a ShardReconciledOffsets containing reconciled offsets and ranges for multiple keyspaces.
     */
    public ReconciledLogSnapshot snapshotReconciledLogs()
    {
        ReconciledLogSnapshot.Builder builder = ReconciledLogSnapshot.builder();

        shardLock.readLock().lock();
        try
        {
            keyspaceShards.forEach((keyspace, ksShards) -> {
                ksShards.collectShardReconciledOffsetsToBuilder(builder);
            });
        }
        finally
        {
            shardLock.readLock().unlock();
        }

        return builder.build();
    }

    public synchronized boolean isStarted()
    {
        return started;
    }

    private void shutdownBlocking() throws InterruptedException
    {
        ClusterMetadataService.instance().log().removeListener(tcmListener);
        activeReconciler.shutdownBlocking();
        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES))
            logger.warn("Mutation tracking executor did not terminate within 1 minute; forcing shutdown");

        // attempt to persist offsets and mark segments as
        // not needing replay one last time before shutdown
        if (isStarted())
            offsetsPersister.run(true);
        ExpiredStatePurger.instance.shutdownBlocking();
    }

    public TrackedLocalReads localReads()
    {
        return localReads;
    }

    public MutationId nextMutationId(String keyspace, Token token)
    {
        shardLock.readLock().lock();
        try
        {
            MutationId id = getOrCreateShards(keyspace).nextMutationId(token);
            logger.trace("Created new mutation id {}", id);
            return id;
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    // Requires that ranges is aligned to a single shard
    public MutationId nextMutationId(String keyspace, Collection<Range<Token>> ranges)
    {
        shardLock.readLock().lock();
        try
        {
            KeyspaceShards shards = getOrCreateShards(keyspace);
            Shard shard = null;
            for (Range<Token> range : ranges)
            {
                Shard curShard = shards.lookUp(range);
                if (curShard == null)
                    throw new UnknownShardException(range, shards.groups);
                if (shard == null)
                    shard = curShard;
                else if (shard != curShard)
                    throw new IllegalStateException(String.format("Cannot generate a mutation ID for ranges (%s) that span across more than one shard (%s, %s)", ranges, shard, curShard));
            }
            Preconditions.checkNotNull(shard);
            MutationId id = shard.nextId();
            logger.trace("Created new mutation id {}", id);
            return id;
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void sentWriteRequest(Mutation mutation, IntHashSet toHostIds)
    {
        Preconditions.checkArgument(!mutation.id().isNone());
//        outgoingMutations.sentWriteRequest(mutation, toHostIds);
    }

    public void receivedWriteResponse(ShortMutationId mutationId, InetAddressAndPort fromHost)
    {
        shardLock.readLock().lock();
        try
        {
            Preconditions.checkArgument(!mutationId.isNone());
            Shard shard = getShardNullable(mutationId.asLogId());
            // A response to the coordinator (for a forwarded write) won't have the coordinator log matching it
            if (shard != null)
                shard.receivedWriteResponse(mutationId, fromHost);
//        outgoingMutations.receivedWriteResponse(mutationId, ClusterMetadata.current().directory.peerId(fromHost).id());
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void receivedActivationResponse(CoordinatedTransfer transfer, InetAddressAndPort fromHost)
    {
        shardLock.readLock().lock();
        try
        {
            logger.debug("{} receivedActivationAck from {}", transfer.logPrefix(), fromHost);
            Preconditions.checkArgument(!transfer.id().isNone());

            Shard shard = getShardNullable(transfer.id().asLogId());
            // Local activation acknowledged in MutationTrackingService.activateLocal
            if (shard != null && !fromHost.equals(FBUtilities.getBroadcastAddressAndPort()))
                shard.receivedActivationResponse(transfer, fromHost);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void retryFailedWrite(ShortMutationId mutationId, InetAddressAndPort onHost, RequestFailure reason)
    {
        Preconditions.checkArgument(!mutationId.isNone());
//        outgoingMutations.writeFailed(mutationId, reason, onHost);
        activeReconciler.schedule(mutationId, onHost, ActiveLogReconciler.Priority.REGULAR);
    }

    public void retryFailedTransfer(CoordinatedTransfer transfer, InetAddressAndPort onHost, Throwable cause)
    {
        if (transfer.isCommitted())
        {
            logger.debug("Failed transfer {} to {} is already committed, skipping reconciliation", transfer, onHost, cause);
            return;
        }
        logger.debug("Retrying failed transfer {} to {} with exception", transfer, onHost, cause);
        Preconditions.checkArgument(!transfer.id().isNone());
        activeReconciler.schedule(transfer.id(), onHost, ActiveLogReconciler.Priority.REGULAR);
    }

    public void updateReplicatedOffsets(String keyspace, Range<Token> range, List<? extends Offsets> offsets, boolean durable, InetAddressAndPort onHost)
    {
        shardLock.readLock().lock();
        try
        {
            KeyspaceShards shards = getOrCreateShards(keyspace);
            // The keyspace may have been dropped locally while this broadcast was in flight; ignore it.
            if (shards == null)
                logger.debug("Ignoring replicated offsets broadcast from {} for unknown (likely dropped) keyspace {}", onHost, keyspace);
            else
                shards.updateReplicatedOffsets(range, offsets, durable, onHost);
        }
        finally
        {
            shardLock.readLock().unlock();
        }

        // Notify any registered sync coordinators about the offset update
        Set<MutationTrackingSyncCoordinator> coordinators = syncCoordinatorsByKeyspace.get(keyspace);
        if (coordinators != null)
        {
            for (MutationTrackingSyncCoordinator coordinator : coordinators)
            {
                if (range.intersects(coordinator.getRange()))
                {
                    coordinator.onOffsetsReceived();
                }
            }
        }
    }

    public void recordFullyReconciledOffsets(ReconciledLogSnapshot reconciledSnapshot)
    {
        shardLock.readLock().lock();
        try
        {
            reconciledSnapshot.forEach((keyspace, keyspaceOffsets) -> {
                KeyspaceShards ksShards = getOrCreateShards(keyspace);
                if (ksShards != null)
                    ksShards.recordFullyReconciledOffsets(keyspaceOffsets);
            });
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public boolean startWriting(Mutation mutation)
    {
        shardLock.readLock().lock();
        try
        {
            Preconditions.checkArgument(!mutation.id().isNone());
            return getOrCreateShards(mutation.getKeyspaceName()).startWriting(mutation);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void finishWriting(Mutation mutation)
    {
        shardLock.readLock().lock();
        try
        {
            Preconditions.checkArgument(!mutation.id().isNone());
            getOrCreateShards(mutation.getKeyspaceName()).finishWriting(mutation);
            incomingMutations.invokeListeners(mutation.id());
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    /**
     * Register to be notified to an incoming mutation.
     * @return true if this is the first active listener added for this id
     */
    public boolean registerMutationCallback(ShortMutationId mutationId, IncomingMutations.Callback callback)
    {
        return incomingMutations.subscribe(mutationId, callback);
    }

    /**
     * Register a sync coordinator to be notified when offset updates arrive.
     */
    public void registerSyncCoordinator(MutationTrackingSyncCoordinator coordinator)
    {
        syncCoordinatorsByKeyspace.computeIfAbsent(coordinator.getKeyspace(), k -> ConcurrentHashMap.newKeySet())
                                  .add(coordinator);
    }

    /**
     * Unregister a sync coordinator.
     */
    public void unregisterSyncCoordinator(MutationTrackingSyncCoordinator coordinator)
    {
        Set<MutationTrackingSyncCoordinator> coordinators = syncCoordinatorsByKeyspace.get(coordinator.getKeyspace());
        if (coordinators != null)
        {
            coordinators.remove(coordinator);

            if (coordinators.isEmpty())
                syncCoordinatorsByKeyspace.remove(coordinator.getKeyspace(), coordinators);
        }
    }

    public void executeTransfers(String keyspace, Set<SSTableReader> sstables, ConsistencyLevel cl)
    {
        shardLock.readLock().lock();
        try
        {
            logger.info("Creating tracked bulk transfers for keyspace '{}' SSTables {}...", keyspace, sstables);

            KeyspaceShards shards = checkNotNull(keyspaceShards.get(keyspace));
            TrackedImportTransfers transfers = TrackedImportTransfers.create(keyspace, shards, sstables, cl);
            logger.info("Split input SSTables into transfers {}", transfers);

            for (TrackedImportTransfer transfer : transfers)
                transfer.execute();
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void received(PendingLocalTransfer transfer)
    {
        logger.debug("Received pending transfer for tracked table {}", transfer);
        TransferTrackingService.instance().received(transfer);
    }

    void activateLocal(ActivationRequest request)
    {
        boolean committed = false;
        String keyspace = request.keyspace;
        Bounds<Token> bounds;
        PendingLocalTransfer pending = null;

        if (request.operation == StreamOperation.REPAIR)
        {
            bounds = new Bounds<>(request.range.left.nextValidToken(), request.range.right);

            // A sync task does not necessarily stream to both replicas, which means there may be a plan ID without a
            // pending local transfer. In this case, we simply treat this as an already committed transfer and update
            // the required offsets in the log (unless we're just preparing).
            committed = request.isCommit();

            // If we have no plan ID, it means this replica did not participate in a sync.
            if (request.planId != null)
                pending = TransferTrackingService.instance().getPendingTransfer(request.planId);
        }
        else if (request.operation == StreamOperation.IMPORT)
        {
            pending = TransferTrackingService.instance().getPendingTransfer(request.planId);
            if (pending == null)
                throw new IllegalStateException(String.format("Cannot activate unknown local pending transfer %s", request));

            bounds = ActivatedTransfers.covering(pending.sstables);
        }
        else
        {
            throw new IllegalArgumentException("Cannot activate transfer for stream operation " + request.operation);
        }

        if (pending != null)
            committed = pending.activate(request, bounds);

        shardLock.readLock().lock();
        try
        {
            if (committed)
            {
                keyspaceShards.get(keyspace).lookUp(request.range).finishActivation(bounds, request);
                incomingMutations.invokeListeners(request.transferId);
            }
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
    {
        shardLock.readLock().lock();
        try
        {
            MutationSummary summary = getOrCreateShards(tableId).createSummaryForKey(key, tableId, includePending);
            MutationTrackingMetrics.instance().readSummarySize.update(summary.size());
            return summary;
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public MutationSummary createSummaryForRange(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending)
    {
        shardLock.readLock().lock();
        try
        {
            MutationSummary summary = getOrCreateShards(tableId).createSummaryForRange(range, tableId, includePending);
            MutationTrackingMetrics.instance().readSummarySize.update(summary.size());
            return summary;
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public MutationSummary createSummaryForRange(Range<Token> range, TableId tableId, boolean includePending)
    {
        return createSummaryForRange(Range.makeRowRange(range), tableId, includePending);
    }

    void forEachKeyspace(Consumer<KeyspaceShards> consumer)
    {
        shardLock.readLock().lock();
        try
        {
            for (KeyspaceShards keyspaceShards : keyspaceShards.values())
                consumer.accept(keyspaceShards);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public long getUnreconciledMutationCount()
    {
        if (!isStarted())
            return 0L;

        final long[] count = {0L};
        forEachKeyspace(ks -> {
            ks.forEachShard(shard -> {
                count[0] += shard.getUnreconciledCount();
            });
        });
        return count[0];
    }

    public Iterable<Shard> getShards()
    {
        List<Shard> shards = new ArrayList<>();
        shardLock.readLock().lock();
        try
        {
            keyspaceShards.forEach((keyspace, ksShards) -> {
                ksShards.forEachShard(shards::add);
            });
        }
        finally
        {
            shardLock.readLock().unlock();
        }
        return shards;
    }

    public void forEachShardInKeyspace(String keyspace, Consumer<Shard> consumer)
    {
        shardLock.readLock().lock();
        try
        {
            KeyspaceShards ksShards = keyspaceShards.get(keyspace);
            if (ksShards != null)
                ksShards.forEachShard(consumer);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    /**
     * Collects the union of witnessed offsets for all shards in the given keyspace that overlap
     * with the specified ranges. Used by the mutation tracking repair protocol to establish
     * a happens-before relationship.
     *
     * @param keyspace the keyspace to collect offsets for
     * @param ranges the token ranges to find overlapping shards for
     * @return a map from shard range to the union of witnessed offsets per coordinator log
     */
    public Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> collectWitnessedOffsetsForRanges(String keyspace, Collection<Range<Token>> ranges, Set<Integer> liveHostIds)
    {
        Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> result = new HashMap<>();
        shardLock.readLock().lock();
        try
        {
            KeyspaceShards ksShards = keyspaceShards.get(keyspace);
            if (ksShards != null)
            {
                ksShards.forEachShard(shard -> {
                    for (Range<Token> range : ranges)
                    {
                        if (shard.range.intersects(range))
                        {
                            result.put(shard.range, shard.collectUnionOfWitnessedOffsetsPerLog(liveHostIds));
                            break;
                        }
                    }
                });
            }
        }
        finally
        {
            shardLock.readLock().unlock();
        }
        return result;
    }

    public void collectLocallyMissingMutations(MutationSummary remoteSummary, Log2OffsetsMap.Mutable into)
    {
        shardLock.readLock().lock();
        try
        {
            Iterator<Offsets> iterator = remoteSummary.onlyUnreconciled();
            while (iterator.hasNext())
            {
                Offsets offsets = iterator.next();
                Shard shard = getShardNullable(offsets.logId);
                if (shard == null)
                    into.add(offsets); // if the log/shard are unknown, then all the offsets are also unkown/missing
                else
                    shard.collectLocallyMissingMutations(offsets, into);
            }
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void collectRemotelyMissingMutations(Offsets localOffsets, IntArrayList remoteNodeIds, Node2OffsetsMap into)
    {
        shardLock.readLock().lock();
        try
        {
            Shard shard = getShard(localOffsets.logId());
            shard.collectRemotelyMissingMutations(localOffsets, remoteNodeIds, into);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public void requestMissingMutations(Offsets offsets, InetAddressAndPort forHost, ActiveLogReconciler.Priority priority)
    {
        activeReconciler.schedule(offsets, forHost, priority);
    }

    @Nullable
    private Shard getShardNullable(CoordinatorLogId logId)
    {
        return log2ShardMap.get(logId);
    }

    @Nonnull
    private Shard getShard(CoordinatorLogId logId)
    {
        return Preconditions.checkNotNull(log2ShardMap.get(logId));
    }

    private KeyspaceShards getOrCreateShards(TableId tableId)
    {
        //noinspection DataFlowIssue
        return getOrCreateShards(Schema.instance.getTableMetadata(tableId).keyspace);
    }

    @Nullable
    private KeyspaceShards getOrCreateShards(String keyspace)
    {
        KeyspaceShards ks = keyspaceShards.get(keyspace);
        if (ks != null)
            return ks;

        ClusterMetadata csm = ClusterMetadata.current();
        // The keyspace may have been dropped locally while messages referencing it (e.g. offset broadcasts
        // from peers) are still in flight. Return null so inbound handlers can ignore them gracefully.
        KeyspaceMetadata ksm = csm.schema.maybeGetKeyspaceMetadata(keyspace).orElse(null);
        if (ksm == null)
            return null;
        return keyspaceShards.computeIfAbsent(keyspace, ignore -> KeyspaceShards.make(ksm, csm, this::nextLogId, this::onNewLog));
    }

    private long nextLogId()
    {
        NodeId nodeId = ClusterMetadata.current().myNodeId();
        Preconditions.checkNotNull(nodeId);
        return CoordinatorLogId.asLong(nodeId.id(), nextHostLogId());
    }

    /*
     * Allocate and persist the next host log id.
     * We only do this on startup and when rotating logs.
     */
    private int nextHostLogId()
    {
        int nextHostLogId = ++prevHostLogId;
        persistHostLogIdToSystemTable(nextHostLogId);
        return nextHostLogId;
    }
    private int prevHostLogId;

    public boolean isDurablyReconciled(ShortMutationId id)
    {
        shardLock.readLock().lock();
        try
        {
            long logId = id.logId();
            Shard shard = getShardNullable(new CoordinatorLogId(logId));
            if (shard == null)
                throw new IllegalStateException("Could not find shard for logId " + logId);

            return shard.isDurablyReconciled(id);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    public boolean isDurablyReconciled(ImmutableCoordinatorLogOffsets logOffsets)
    {
        shardLock.readLock().lock();
        try
        {
            Iterable<Long> mutations = logOffsets.mutations();
            Iterable<Long> transfers = Iterables.transform(logOffsets.transfers(), ShortMutationId::logId);
            Iterable<Long> logIds = Iterables.concat(mutations, transfers);
            for (Long logId : logIds)
            {
                Shard shard = getShardNullable(new CoordinatorLogId(logId));
                if (shard == null)
                    throw new IllegalStateException("Could not find shard for logId " + logId);

                if (!shard.isDurablyReconciled(logId, logOffsets))
                    return false;
            }
            return true;
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    private synchronized void onNewClusterMetadata(@Nullable ClusterMetadata prev, ClusterMetadata next)
    {
        if (logger.isTraceEnabled())
            logger.trace("Processing cluster metadata change - epoch {} -> {}",
                        prev != null ? prev.epoch : "none", next.epoch);

        if (!next.epoch.isAfter(lastAppliedEpoch))
            return;

        shardLock.readLock().lock();
        try
        {
            if (!shardUpdateNeeded(keyspaceShards, prev, next))
                return;
        }
        finally
        {
            shardLock.readLock().unlock();
        }

        shardLock.writeLock().lock();
        ConcurrentHashMap<CoordinatorLogId, Shard> originalLog2ShardMap = log2ShardMap;
        ConcurrentHashMap<String, KeyspaceShards> originalKeyspaceShards = keyspaceShards;
        try
        {
            if (!next.epoch.isAfter(lastAppliedEpoch))
                return;

            if (!shardUpdateNeeded(keyspaceShards, prev, next))
                return;

            // recalculating the shards will repopulate this via the existing callbacks
            log2ShardMap = new ConcurrentHashMap<>();
            keyspaceShards = applyUpdatedMetadata(keyspaceShards, prev, next, this::nextLogId, this::onNewLog);

            if (!config.background_reconciliation_enabled)
            {
                Set<String> newKeyspaces = new HashSet<>(keyspaceShards.keySet());
                newKeyspaces.removeAll(originalKeyspaceShards.keySet());
                if (!newKeyspaces.isEmpty())
                    logBackgroundReconciliationDisabledWarning(newKeyspaces);
            }

            lastAppliedEpoch = next.epoch;
        }
        catch (Throwable t)
        {
            log2ShardMap = originalLog2ShardMap;
            keyspaceShards = originalKeyspaceShards;
            throw t;
        }
        finally
        {
            shardLock.writeLock().unlock();
        }

    }

    private static boolean shardUpdateNeeded(Map<String, KeyspaceShards> current, @Nullable ClusterMetadata prev, ClusterMetadata next)
    {
        Preconditions.checkNotNull(next);

        current = new HashMap<>(current);

        Set<String> allKeyspaces = new HashSet<>();
        allKeyspaces.addAll(current.keySet());
        allKeyspaces.addAll(next.schema.getKeyspaces().names());
        if (prev != null)
            allKeyspaces.addAll(prev.schema.getKeyspaces().names());

        for (String keyspace : allKeyspaces)
        {
            KeyspaceShards.UpdateDecision decision = KeyspaceShards.UpdateDecision.decisionForTopologyChange(keyspace, prev, next, current.containsKey(keyspace));
            if (decision != KeyspaceShards.UpdateDecision.NONE)
                return true;
        }

        return false;
    }

    private static ConcurrentHashMap<String, KeyspaceShards> applyUpdatedMetadata(Map<String, KeyspaceShards> keyspaceShardsMap, @Nullable ClusterMetadata prev, ClusterMetadata next, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
    {
        Preconditions.checkNotNull(next);

        Map<String, KeyspaceShards>  currentShards = new HashMap<>(keyspaceShardsMap);
        ConcurrentHashMap<String, KeyspaceShards> updated = new ConcurrentHashMap<>();

        Set<String> allKeyspaces = new HashSet<>();
        allKeyspaces.addAll(currentShards.keySet());
        allKeyspaces.addAll(next.schema.getKeyspaces().names());
        if (prev != null)
            allKeyspaces.addAll(prev.schema.getKeyspaces().names());


        for (String keyspace : allKeyspaces)
        {
            KeyspaceShards current = currentShards.remove(keyspace);
            KeyspaceShards.UpdateDecision decision = KeyspaceShards.UpdateDecision.decisionForTopologyChange(keyspace, prev, next, current != null);
            switch (decision)
            {
                case MIGRATE_FROM:
                    // TODO (expected): Implement shard deletion for tracked → untracked migration completion (CASSANDRA-20955)
                case NONE:
                    if (current != null)
                        updated.put(keyspace, current);
                    break;
                case DROP:
                    // Don't carry forward the state for the dropped keyspace
                    break;
                case REPLICA_GROUP:
                    // if there's an existing keyspace shards instance, update it, otherwise fall through to CREATE
                    if (current != null)
                    {
                        KeyspaceShards ksShards = current.withUpdatedMetadata(next.schema.getKeyspaceMetadata(keyspace), next, logIdProvider, onNewLog);
                        updated.put(keyspace, ksShards);
                        break;
                    }
                case CREATE:
                case MIGRATE_TO:
                    Preconditions.checkState(current == null,
                                             "Attempted to create a new keyspace shard for keyspace %s, but it already exists", keyspace);
                    KeyspaceShards ksShards = KeyspaceShards.make(next.schema.getKeyspaceMetadata(keyspace),
                                                                  next,
                                                                  logIdProvider,
                                                                  onNewLog);
                    updated.put(keyspace, ksShards);
                    break;
                default:
                    throw new IllegalStateException("Unsupported keyspace shard update: " + decision);
            }
        }

        if (!currentShards.isEmpty())
            throw new IllegalStateException("At least one keyspace shards instance wasn't migrated: " + currentShards);

        return updated;
    }

    // TODO (expected): when topology and state truncation is implemented, implement cleanup of this map as well
    private void onNewLog(Shard shard, CoordinatorLog log)
    {
        shardLock.readLock().lock();
        try
        {
            log2ShardMap.put(log.logId, shard);
        }
        finally
        {
            shardLock.readLock().unlock();
        }
    }

    private void truncateMutationJournal()
    {
        Log2OffsetsMap.Mutable reconciledOffsets = new Log2OffsetsMap.Mutable();
        collectDurablyReconciledOffsets(reconciledOffsets);
        MutationJournal.instance().dropReconciledSegments(reconciledOffsets);
    }

    /**
     * Collect every log's durably reconciled offsets. Every mutation covered
     * by these offsets can be compacted away by the journal, assuming that all
     * relevant memtables had been flushed to disk.
     */
    private void collectDurablyReconciledOffsets(Log2OffsetsMap.Mutable into)
    {
        forEachKeyspace(keyspace -> keyspace.collectDurablyReconciledOffsets(into));
    }

    public SyncTasks alignToShardBoundaries(Keyspace keyspace, List<SyncTask> tasks)
    {
        Preconditions.checkArgument(keyspace.getMetadata().replicationStrategy.replicationType.isTracked(), "Keyspace " + keyspace.getName() + " is not tracked");

        KeyspaceShards shards = keyspaceShards.get(keyspace.getName());
        Map<Shard, List<SyncTask>> tasksByShard = new HashMap<>();

        // Shard ranges do not wrap, so unwrap the task ranges before we start comparing them.
        for (SyncTask task : unwrapped(tasks))
        {
            Set<Shard> intersectingShards = new HashSet<>();
            shards.forEachIntersectingShard(task.rangesToSync, intersectingShards::add);
            for (Shard shard : intersectingShards)
            {
                // Ensure that we don't expand outside the ranges of the original sync tasks.
                Set<Range<Token>> intersectingSyncRanges = new HashSet<>();
                for (Range<Token> syncRange : task.rangesToSync)
                    intersectingSyncRanges.addAll(syncRange.intersectionWith(shard.range));

                if (!intersectingSyncRanges.isEmpty())
                    tasksByShard.computeIfAbsent(shard, key -> new ArrayList<>()).add(task.withRanges(intersectingSyncRanges));
            }
        }

        SyncTasks into = new SyncTasks();

        for (Map.Entry<Shard, List<SyncTask>> entry : tasksByShard.entrySet())
        {
            Shard shard = entry.getKey();
            Collection<SyncTask> syncTasks = entry.getValue();

            // Assign a new transfer ID to each sync task and add to the tasks container
            for (SyncTask task : syncTasks)
                into.add(shard, task.withTransferId(shard.nextId()));
        }

        return into;
    }

    private static List<SyncTask> unwrapped(Collection<SyncTask> tasks)
    {
        List<SyncTask> unwrapped = new ArrayList<>();

        for (SyncTask task : tasks)
        {
            List<Range<Token>> unwrappedRanges = new ArrayList<>();
            for (Range<Token> range : task.rangesToSync)
            {
                if (range.isTrulyWrapAround())
                    unwrappedRanges.addAll(range.unwrap());
                else
                    unwrappedRanges.add(range);
            }

            unwrapped.add(task.withRanges(unwrappedRanges));
        }

        return unwrapped;
    }

    private void logBackgroundReconciliationDisabledWarning(Set<String> keyspaces)
    {
        logger.warn("Background reconciliation is disabled but mutation tracking keyspaces exist: {}. " +
                    "Unreconciled mutations will not be automatically repaired in the background.", keyspaces);
    }

    public static class KeyspaceShards
    {
        private enum UpdateDecision
        {
            NONE,
            CREATE,
            DROP,
            REPLICA_GROUP,
            MIGRATE_TO,
            MIGRATE_FROM;

            static UpdateDecision decisionForTopologyChange(String keyspace, ClusterMetadata prev, ClusterMetadata next, boolean hasExisting)
            {
                KeyspaceMetadata prevKsm = prev != null ? prev.schema.getKeyspaces().get(keyspace).orElse(null) : null;
                KeyspaceMetadata nextKsm = next.schema.getKeyspaces().get(keyspace).orElse(null);

                if (prevKsm == null && nextKsm == null)
                {
                    if (hasExisting)
                        throw new IllegalStateException(String.format("Mutation tracking exists for unknown keyspace %s", keyspace));
                    return NONE;
                }

                // TODO (CASSANDRA-20955): hasExisting should not be possible here once shard cleanup is implemented
                if (prevKsm == null)
                    return nextKsm.useMutationTracking() ? (hasExisting ? UpdateDecision.REPLICA_GROUP : UpdateDecision.CREATE) : UpdateDecision.NONE;

                if (nextKsm == null)
                    return prevKsm.useMutationTracking() ? UpdateDecision.DROP : UpdateDecision.NONE;

                if (!prevKsm.useMutationTracking() && !nextKsm.useMutationTracking())
                {
                    // TODO: drop shards after migration to untracked
//                    Preconditions.checkState(!hasExisting, "Existing shards found for keyspace, but prev & current ksm has mutation tracking disabled");
                    return UpdateDecision.NONE;
                }

                if (prevKsm.useMutationTracking() && !nextKsm.useMutationTracking())
                {
                    return UpdateDecision.MIGRATE_FROM;
                }

                if (!prevKsm.useMutationTracking() && nextKsm.useMutationTracking())
                {
                    // TODO (CASSANDRA-20955): hasExisting should not be possible here once shard cleanup is implemented.
                    // Shards from a prior tracked phase can survive a round-trip migration
                    // (tracked→untracked→tracked) because shard cleanup is not yet implemented.
                    // Update the replica group instead of failing.
//                    Preconditions.checkState(!hasExisting, "Existing shard found for keyspace, but prev ksn has mutation tracking disabled");
                    return hasExisting ? UpdateDecision.REPLICA_GROUP : UpdateDecision.MIGRATE_TO;
                }

                if (!calculateParticipantsForRange(nextKsm, next).equals(calculateParticipantsForRange(prevKsm, prev)))
                    return UpdateDecision.REPLICA_GROUP;

                return UpdateDecision.NONE;
            }
        }

        private final String keyspace;
        private final Map<Range<Token>, Shard> shards;
        private final ReplicaGroups groups;

        private transient final Map<Range<PartitionPosition>, Shard> ppShards;

        private static class ParticipantForRange
        {
            final Participants participants;
            final VersionedEndpoints.ForRange forRange;

            public ParticipantForRange(Participants participants, VersionedEndpoints.ForRange forRange)
            {
                this.participants = participants;
                this.forRange = forRange;
            }
        }

        private static Map<Range<Token>, ParticipantForRange> calculateParticipantsForRange(KeyspaceMetadata keyspace, ClusterMetadata cluster)
        {
            Map<Range<Token>, ParticipantForRange> result = new HashMap<>();
            cluster.placements.get(keyspace.params.replication).writes.forEach((fullTokenRange, forRange) -> {
                if (!forRange.endpoints().contains(FBUtilities.getBroadcastAddressAndPort()))
                    return;

                IntArrayList participantList = new IntArrayList(forRange.size(), IntArrayList.DEFAULT_NULL_VALUE);
                for (InetAddressAndPort endpoint : forRange.endpoints())
                    participantList.add(cluster.directory.peerId(endpoint).id());
                Participants participants = new Participants(participantList);

                result.put(fullTokenRange, new ParticipantForRange(participants, forRange));
            });
            return result;
        }

        private static Set<Range<Token>> splitRange(Range<Token> range)
        {
            Optional<Splitter> splitter = range.left.getPartitioner().splitter();
            return splitter.isPresent() && SHARD_MULTIPLIER > 1
                   ? splitter.get().split(range, SHARD_MULTIPLIER)
                   : Collections.singleton(range);
        }

        static KeyspaceShards make(KeyspaceMetadata keyspace, ClusterMetadata cluster, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
        {
            Preconditions.checkArgument(keyspace.params.replicationType.isTracked() || cluster.mutationTrackingMigrationState.isMigrating(keyspace.name));

            Map<Range<Token>, Shard> shards = new HashMap<>();
            Map<Range<Token>, VersionedEndpoints.ForRange> groups = new HashMap<>();

            calculateParticipantsForRange(keyspace, cluster).forEach((fullTokenRange, participantForRange) -> {
                Participants participants = participantForRange.participants;
                VersionedEndpoints.ForRange forRange = participantForRange.forRange;

                Set<Range<Token>> ranges = splitRange(fullTokenRange);

                for (Range<Token> tokenRange : ranges)
                {
                    shards.put(tokenRange, new Shard(cluster.myNodeId().id(), keyspace.name, tokenRange, participants, logIdProvider, onNewLog));
                    groups.put(tokenRange, forRange.map(original -> original.withRange(tokenRange)));
                }
            });
            KeyspaceShards keyspaceShards = new KeyspaceShards(keyspace.name, shards, new ReplicaGroups(groups));
            keyspaceShards.persistToSystemTables();
            return keyspaceShards;
        }

        KeyspaceShards(String keyspace, Map<Range<Token>, Shard> shards, ReplicaGroups groups)
        {
            this.keyspace = keyspace;
            this.shards = shards;
            this.groups = groups;

            HashMap<Range<PartitionPosition>, Shard> ppShards = new HashMap<>();
            shards.forEach((range, shard) -> ppShards.put(Range.makeRowRange(range), shard));
            this.ppShards = ppShards;
        }

        KeyspaceShards withUpdatedMetadata(KeyspaceMetadata keyspace, ClusterMetadata cluster, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
        {
            Map<Range<Token>, Shard> currentShards = new HashMap<>(shards);
            Map<Range<Token>, Shard> newShards = new HashMap<>();
            Map<Range<Token>, VersionedEndpoints.ForRange> newGroups = new HashMap<>();

            calculateParticipantsForRange(keyspace, cluster).forEach((fullTokenRange, participantForRange) -> {
                Participants participants = participantForRange.participants;
                VersionedEndpoints.ForRange forRange = participantForRange.forRange;

                Set<Range<Token>> ranges = splitRange(fullTokenRange);

                for (Range<Token> tokenRange : ranges)
                {
                    Shard currentShard = currentShards.remove(tokenRange);
                    if (currentShard != null)
                    {
                        newShards.put(tokenRange, currentShard.withParticipants(participants));
                        newGroups.put(tokenRange, forRange.map(original -> original.withRange(tokenRange)));
                    }
                    else
                    {
                        newShards.put(tokenRange, new Shard(cluster.myNodeId().id(), keyspace.name, tokenRange, participants, logIdProvider, onNewLog));
                        newGroups.put(tokenRange, forRange.map(original -> original.withRange(tokenRange)));
                    }
                }
            });

            newShards.values().forEach(Shard::reportAllLogsToCallback);

            return new KeyspaceShards(keyspace.name, newShards, new ReplicaGroups(newGroups));
        }

        MutationId nextMutationId(Token token)
        {
            return lookUp(token).nextId();
        }

        void updateReplicatedOffsets(Range<Token> range, List<? extends Offsets> offsets, boolean durable, InetAddressAndPort onHost)
        {
            Shard shard = shards.get(range);
            if (shard == null)
                return;
            shard.updateReplicatedOffsets(offsets, durable, onHost);
        }

        boolean startWriting(Mutation mutation)
        {
            return lookUp(mutation).startWriting(mutation);
        }

        void finishWriting(Mutation mutation)
        {
            lookUp(mutation).finishWriting(mutation);
        }

        MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            lookUp(key.getToken()).addSummaryForKey(key.getToken(), includePending, builder);
            return builder.build();
        }

        MutationSummary createSummaryForRange(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            forEachIntersectingShard(range, shard -> shard.addSummaryForRange(range, includePending, builder));
            return builder.build();
        }

        private void forEachIntersectingShard(AbstractBounds<PartitionPosition> bounds, Consumer<Shard> consumer)
        {
            ppShards.forEach((range, shard) -> {
                // TODO (expected): partial workaround - is there a better way to do this?
                //  SELECT * statements create Bounds[min,min], (PartitionKeyRestrictions.java:L174) not Range(min,min],
                //  which Ranges generally won't intersect with (Range.java:L148), so contains is used here to make it work
                if (bounds.contains(range.right) || range.intersects(bounds))
                    consumer.accept(shard);
            });
        }

        private void forEachIntersectingShard(Collection<Range<Token>> ranges, Consumer<Shard> consumer)
        {
            shards.forEach((range0, shard) -> {
                if (shard.range.intersects(ranges))
                    consumer.accept(shard);
            });
        }

        void collectShardReconciledOffsetsToBuilder(ReconciledLogSnapshot.Builder builder)
        {
            ReconciledKeyspaceOffsets.Builder keyspaceBuilder = builder.getKeyspaceBuilder(keyspace);
            ppShards.values().forEach(shard -> shard.collectShardReconciledOffsetsToBuilder(keyspaceBuilder));
        }

        void recordFullyReconciledOffsets(ReconciledKeyspaceOffsets keyspaceOffsets)
        {
            keyspaceOffsets.forEach((logId, entry) -> {
                // Find the shard that should contain this log based on the range
                Shard shard = shards.get(entry.range);
                if (shard != null)
                    shard.recordFullyReconciledOffsets(logId, entry.offsets);
            });
        }

        void collectDurablyReconciledOffsets(Log2OffsetsMap.Mutable into)
        {
            forEachShard(shard -> shard.collectDurablyReconciledOffsets(into));
        }

        void forEachShard(Consumer<Shard> consumer)
        {
            for (Shard shard : shards.values())
                consumer.accept(shard);
        }

        Shard lookUp(Mutation mutation)
        {
            return lookUp(mutation.key());
        }

        Shard lookUp(DecoratedKey key)
        {
            return lookUp(key.getToken());
        }

        Shard lookUp(Token token)
        {
            VersionedEndpoints.ForRange forRange = groups.matchToken(token);
            if (forRange == null)
                throw new UnknownShardException(token, groups);
            return shards.get(forRange.range());
        }

        Shard lookUp(Range<Token> range)
        {
            VersionedEndpoints.ForRange forRange = groups.matchRange(range);
            if (forRange == null)
                throw new UnknownShardException(range, groups);
            return shards.get(forRange.range());
        }

        void persistToSystemTables()
        {
            for (Shard shard : shards.values()) shard.persistToSystemTables();
        }

        static List<KeyspaceShards> loadFromSystemTables(ClusterMetadata cluster, LongSupplier logIdProvider, BiConsumer<Shard, CoordinatorLog> onNewLog)
        {
            Map<String, Map<Range<Token>, Shard>> groupedShards = new HashMap<>();
            for (Shard shard : Shard.loadFromSystemTables(cluster.myNodeId().id(), logIdProvider, onNewLog))
                groupedShards.computeIfAbsent(shard.keyspace, k -> new HashMap<>()).put(shard.range, shard);
            List<KeyspaceShards> keyspaceShards = new ArrayList<>();
            for (Map.Entry<String, Map<Range<Token>, Shard>> entry : groupedShards.entrySet())
            {
                ReplicationParams params = cluster.schema.getKeyspaceMetadata(entry.getKey()).params.replication;
                ReplicaGroups originalGroups = cluster.placements.get(params).writes; // prior to splitting

                Map<Range<Token>, VersionedEndpoints.ForRange> splitGroups = new HashMap<>();
                for (Range<Token> splitRange : entry.getValue().keySet())
                    splitGroups.put(splitRange, originalGroups.matchRange(splitRange));

                keyspaceShards.add(new KeyspaceShards(entry.getKey(), entry.getValue(), new ReplicaGroups(splitGroups)));
            }
            return keyspaceShards;
        }
    }

    private static final String HOST_LOG_ID_KEY = "local";

    private static final String INSERT_QUERY =
        format("INSERT INTO %s.%s (key, host_log_id) VALUES (?, ?)",
               SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.HOST_LOG_ID);

    static void persistHostLogIdToSystemTable(int hostLogId)
    {
        executeInternal(INSERT_QUERY, HOST_LOG_ID_KEY, hostLogId);
    }

    private static final String SELECT_QUERY =
        format("SELECT * FROM %s.%s WHERE key = ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.HOST_LOG_ID);

    static int loadHostLogIdFromSystemTable()
    {
        UntypedResultSet rows = executeInternal(SELECT_QUERY, HOST_LOG_ID_KEY);
        if (rows.isEmpty())
            return 0;
        return rows.one().getInt("host_log_id");
    }

    private static class BackgroundReconciler
    {
        void start()
        {
            scheduleNext();
        }

        private void scheduleNext()
        {
            long intervalMillis = config.background_reconciliation_interval.toMilliseconds();
            executor.schedule(this::runAndReschedule, intervalMillis, TimeUnit.MILLISECONDS);
        }

        private void runAndReschedule()
        {
            try
            {
                run();
            }
            finally
            {
                scheduleNext();
            }
        }

        void run()
        {
            MutationTrackingService.instance().forEachKeyspace(this::run);
        }

        private void run(KeyspaceShards shards)
        {
            if (config.background_reconciliation_enabled)
                shards.forEachShard(this::run);
        }

        private void run(Shard shard)
        {
            try
            {
                List<Offsets.Immutable> missing = shard.collectLocallyMissingOffsets();
                if (missing.isEmpty()) return;

                for (Offsets.Immutable offsets : missing)
                {
                    // Prefer pulling from the coordinator
                    int coordinatorHostId = offsets.logId().hostId();
                    InetAddressAndPort coordinator = ClusterMetadata.current().directory.endpoint(new NodeId(coordinatorHostId));
                    InetAddressAndPort pullFrom = FailureDetector.instance.isAlive(coordinator)
                                                  ? coordinator
                                                  : findAliveReplica(shard, coordinatorHostId);
                    if (pullFrom == null)
                    {
                        logger.debug("No coordinator or replica is available to process the pull mutation request for missing offset {}",
                                     offsets);
                        continue; // No reachable source
                    }

                    // TODO (expected): backoff, rate limits, per host and total
                    PullMutationsRequest request = new PullMutationsRequest(offsets, ActiveLogReconciler.Priority.REGULAR);
                    logger.trace("Requesting pull mutation request from replica {} for missing offset {}", pullFrom, offsets);
                    MessagingService.instance().send(Message.out(Verb.MT_PULL_MUTATIONS_REQ, request), pullFrom);
                }
            }
            catch (Throwable throwable)
            {
                // Avoid throwing an exception in the reconciliation step to prevent the scheduled task from
                // being killed
                logger.error("Exception encountered during background reconciliation of shard={}", shard, throwable);
            }
        }

        private InetAddressAndPort findAliveReplica(Shard shard, int excludeHostId)
        {
            for (InetAddressAndPort replica : shard.remoteReplicas())
            {
                int replicaId = ClusterMetadata.current().directory.peerId(replica).id();
                if (replicaId != excludeHostId && FailureDetector.instance.isAlive(replica))
                {
                    logger.trace("Found alive replica {} with replica id {}", replica, replicaId);
                    return replica;
                }
            }
            return null;
        }
    }

    // TODO (later): a more intelligent heuristic for offsets included in broadcasts
    private static class ReplicatedOffsetsBroadcaster
    {
        // TODO (later): a more intelligent heuristic for scheduling broadcasts
        // TODO: Revert before merge, just increased frequency for test
        private static final long TRANSIENT_BROADCAST_INTERVAL_MILLIS = 1_000;
        private static final long DURABLE_BROADCAST_INTERVAL_MILLIS = 1_000;

        private volatile boolean isPaused = false;

        void start()
        {
            executor.scheduleWithFixedDelay(() -> run(false),
                                            TRANSIENT_BROADCAST_INTERVAL_MILLIS,
                                            TRANSIENT_BROADCAST_INTERVAL_MILLIS,
                                            TimeUnit.MILLISECONDS);
            executor.scheduleWithFixedDelay(() -> run(true),
                                            DURABLE_BROADCAST_INTERVAL_MILLIS,
                                            DURABLE_BROADCAST_INTERVAL_MILLIS,
                                            TimeUnit.MILLISECONDS);
        }

        public void pauseOffsetBroadcast(boolean pause)
        {
            isPaused = pause;
        }

        public void run(boolean durable)
        {
            MutationTrackingService.instance().forEachKeyspace(ks -> run(ks, durable));
        }

        private void run(KeyspaceShards shards, boolean durable)
        {
            if (!isPaused)
                shards.forEachShard(sh -> run(sh, durable));
        }

        private void run(Shard shard, boolean durable)
        {
            BroadcastLogOffsets replicatedOffsets = shard.collectReplicatedOffsets(durable);
            if (replicatedOffsets.isEmpty())
                return;

            Message<BroadcastLogOffsets> message = Message.out(Verb.MT_BROADCAST_LOG_OFFSETS, replicatedOffsets);

            for (InetAddressAndPort target : shard.remoteReplicas())
                if (FailureDetector.instance.isAlive(target))
                    MessagingService.instance().send(message, target);
        }
    }

    /**
     * Persists per-log witnessed offsets, and durably marks needsReplay=false on any segments that have become eligible
     * for it since the most recent run of this class. These 2 operations need to performed in a specific sequence to avoid
     * correctness problems.
     *
     * For background, mutation tracking needs to keep a record of every mutation id it's written locally. For correctness
     * purposes, a nodes view of mutation ids it's written locally needs to exactly match the data it has on disk.
     * Having data on disk you dont have an id for, or thinking you have ids on disk that you don't breaks the mutation
     * tracking consistency mechanism.
     *
     * To improve startup, we periodically save our view of mutation ids that we've witnessed to disk as part of this
     * class. Any ids witnessed since the last time this class was run are reconstructed by replaying the journal.
     *
     * However, if an sstable is flushed after the most recent LogStatePersister run, AND it marks a segment as no
     * longer needing replay, AND the node is stopped before the next LogStatePersister, then the offsets witnessed
     * between the LogStatePersister and sstable flush will be forgotten on startup.
     *
     * This is a correctness problem for mutation tracking because it means that we will be returning data in reads that
     * are not included in our mutation summaries, which breaks reconciliation and read monotonicity.
     *
     * To prevent this, witnessed offsets are flushed and segments are marked as not needing replay together in 3 steps.
     *
     * 1. Snapshot the set of journal segments that have been marked as needing their need replay flag set to false (but not yet updated on disk)
     * 2. Flush per-log witnessed offsets to the system table
     * 3. Durably mark the snapshotted segments as not needing replay
     *
     * This guarantees that, on startup, we will always replay all segments that may contain offsets not persisted to
     * system.coordinator_logs
     */
    private static class LogStatePersister implements Runnable
    {
        // TODO (expected): consider a different interval
        // TODO: Revert before merge, just increased frequency for test
        // private static final long PERSIST_INTERVAL_MILLIS = 60_000;
        private static final long PERSIST_INTERVAL_MILLIS = 1_000;

        private volatile boolean isPaused = false;

        void start()
        {
            executor.scheduleWithFixedDelay(this, PERSIST_INTERVAL_MILLIS, PERSIST_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }

        void pauseForTesting(boolean pause)
        {
            isPaused = pause;
        }

        @Override
        public void run()
        {
            if (isPaused)
                return;
            run(true);
        }

        private void run(boolean dropSegments)
        {

            MutationJournal.PendingClearReplay toDrain = MutationJournal.instance().snapshotPendingClearReplay();

            boolean writesOk;
            try
            {
                MutationTrackingService.instance().forEachKeyspace(this::run);
                writesOk = true;
            }
            catch (Throwable t)
            {
                writesOk = false;
                logger.error("LogStatePersister write to system.coordinator_logs failed; deferring segment cleanup drain to next tick", t);
            }

            if (writesOk)
                MutationJournal.instance().drainCleanup(toDrain);

            if (dropSegments)
                MutationTrackingService.instance().truncateMutationJournal();
        }

        private void run(KeyspaceShards shards)
        {
            shards.forEachShard(this::run);
        }

        private void run(Shard shard)
        {
            shard.updateLogsInSystemTable();
        }
    }

    @VisibleForTesting
    public void persistLogStateForTesting()
    {
        offsetsPersister.run();
    }

    @VisibleForTesting
    public void persistLogStateForTesting(boolean dropSegments)
    {
        offsetsPersister.run(dropSegments);
    }

    @VisibleForTesting
    public void broadcastOffsetsForTesting()
    {
        offsetsBroadcaster.run(false);
        offsetsBroadcaster.run(true);
    }

    @VisibleForTesting
    public void pauseActiveReconciler()
    {
        activeReconciler.pauseForTesting();
    }

    @VisibleForTesting
    public void resumeActiveReconciler()
    {
        activeReconciler.resumeForTesting();
    }

    @VisibleForTesting
    public void pauseOffsetsPersisterForTesting()
    {
        offsetsPersister.pauseForTesting(true);
    }

    @VisibleForTesting
    public void resumeOffsetsPersisterForTesting()
    {
        offsetsPersister.pauseForTesting(false);
    }

    /**
     * Pause only regular-priority (background write retry) delivery in the active reconciler.
     * High-priority tasks (needed by tracked read reconciliation) continue to be processed.
     */
    @VisibleForTesting
    public void pauseActiveReconcilerRegularPriority()
    {
        activeReconciler.pauseRegularPriorityForTesting();
    }

    @VisibleForTesting
    public void resumeActiveReconcilerRegularPriority()
    {
        activeReconciler.resumeRegularPriorityForTesting();
    }

    @VisibleForTesting
    public void reconcileForTesting()
    {
        backgroundReconciler.run();
    }

    @VisibleForTesting
    public void pauseBackgroundReconciler()
    {
        config.background_reconciliation_enabled = false;
    }

    @VisibleForTesting
    public void resumeBackgroundReconciler()
    {
        config.background_reconciliation_enabled = true;
    }

    @VisibleForTesting
    public static class TestAccess
    {
        public static MutationTrackingService create()
        {
            return new MutationTrackingService();
        }

        public static KeyspaceShards getKeyspaceShards(MutationTrackingService service, String keyspace)
        {
            return service.keyspaceShards.get(keyspace);
        }

        /**
         * Creates a test KeyspaceShards with the given shard ranges.
         * The shards are created with minimal configuration suitable for testing.
         */
        public static KeyspaceShards createTestKeyspaceShards(String keyspace, Set<Range<Token>> shardRanges)
        {
            Map<Range<Token>, Shard> shards = new HashMap<>();
            Map<Range<Token>, VersionedEndpoints.ForRange> groups = new HashMap<>();

            int localNodeId = 1;
            AtomicInteger hostLogId = new AtomicInteger(0);
            LongSupplier logId = () -> CoordinatorLogId.asLong(localNodeId, hostLogId.getAndIncrement());
            Participants participants = new Participants(List.of(localNodeId));
            for (Range<Token> range : shardRanges)
            {
                shards.put(range, new Shard(localNodeId, keyspace, range, participants, logId, (s, l) -> {}));
                groups.put(range, VersionedEndpoints.forRange(Epoch.EMPTY, EndpointsForRange.empty(range)));
            }

            return new KeyspaceShards(keyspace, shards, new ReplicaGroups(groups));
        }

        /**
         * Sets the keyspace shards for testing purposes.
         */
        public static void setKeyspaceShards(MutationTrackingService service, String keyspace, KeyspaceShards shards)
        {
            service.keyspaceShards.put(keyspace, shards);
        }
    }
}
