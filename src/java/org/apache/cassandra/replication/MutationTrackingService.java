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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.reads.tracked.TrackedLocalReads;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorSemantics.NORMAL;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

// TODO (expected): persistence (handle restarts)
// TODO (expected): handle topology changes
public class MutationTrackingService
{
    private static final ScheduledExecutorPlus executor = executorFactory().scheduled("Mutation-Tracking-Service", NORMAL);

    /**
     * Split ranges into this many shards.
     *
     * TODO (expected): ability to rebalance / change this constant
     */
    private static final int SHARD_MULTIPLIER = 8;

    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingService.class);
    public static final MutationTrackingService instance = new MutationTrackingService();

    private final TrackedLocalReads localReads = new TrackedLocalReads();
    private ConcurrentHashMap<String, KeyspaceShards> keyspaceShards = new ConcurrentHashMap<>();
    private ConcurrentHashMap<CoordinatorLogId, Shard> log2ShardMap = new ConcurrentHashMap<>();
    private final ChangeListener tcmListener;

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

    private final IncomingMutations incomingMutations = new IncomingMutations();
    private final OutgoingMutations outgoingMutations = new OutgoingMutations();

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

    public synchronized void start(ClusterMetadata metadata)
    {
        if (started)
            return;

        prevHostLogId = loadHostLogIdFromSystemTable();

        logger.info("Starting mutation tracking service. Previous host log id: {}", prevHostLogId);

        if (metadata.myNodeId() != null)
            for (KeyspaceShards ks : KeyspaceShards.loadFromSystemTables(metadata, this::nextLogId, this::onNewLog))
                keyspaceShards.put(ks.keyspace, ks);

        onNewClusterMetadata(null, metadata);

        offsetsBroadcaster.start();
        offsetsPersister.start();

        ExpiredStatePurger.instance.register(incomingMutations);

        started = true;
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

    public void registerTCMListener()
    {
        ClusterMetadataService.instance().log().addListener(tcmListener);
    }

    public synchronized boolean isStarted()
    {
        return started;
    }

    public void shutdownBlocking() throws InterruptedException
    {
        ClusterMetadataService.instance().log().removeListener(tcmListener);
        activeReconciler.shutdownBlocking();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
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
            Shard shard = getShardNullable(mutationId);
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

    public void retryFailedWrite(ShortMutationId mutationId, InetAddressAndPort onHost, RequestFailure reason)
    {
        Preconditions.checkArgument(!mutationId.isNone());
//        outgoingMutations.writeFailed(mutationId, reason, onHost);
        activeReconciler.schedule(mutationId, onHost, ActiveLogReconciler.Priority.REGULAR);
    }

    public void updateReplicatedOffsets(String keyspace, Range<Token> range, List<? extends Offsets> offsets, boolean durable, InetAddressAndPort onHost)
    {
        shardLock.readLock().lock();
        try
        {
            getOrCreateShards(keyspace).updateReplicatedOffsets(range, offsets, durable, onHost);
        }
        finally
        {
            shardLock.readLock().unlock();
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

    public MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
    {
        shardLock.readLock().lock();
        try
        {
            return getOrCreateShards(tableId).createSummaryForKey(key, tableId, includePending);
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
            return getOrCreateShards(tableId).createSummaryForRange(range, tableId, includePending);
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

    public void requestMissingMutations(Offsets offsets, InetAddressAndPort forHost)
    {
        activeReconciler.schedule(offsets, forHost, ActiveLogReconciler.Priority.HIGH);
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

    private KeyspaceShards getOrCreateShards(String keyspace)
    {
        KeyspaceShards ks = keyspaceShards.get(keyspace);
        if (ks != null)
            return ks;

        ClusterMetadata csm = ClusterMetadata.current();
        KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
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

    public boolean isDurablyReconciled(ImmutableCoordinatorLogOffsets logOffsets)
    {
        shardLock.readLock().lock();
        try
        {
            // Could pass through SSTable bounds to exclude shards for non-overlapping ranges, but this will mostly be
            // called on flush for L0 SSTables with wide bounds.
            for (Long logId : logOffsets)
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

    private void onNewClusterMetadata(@Nullable ClusterMetadata prev, ClusterMetadata next)
    {
        if (logger.isTraceEnabled())
            logger.trace("Processing cluster metadata change - epoch {} -> {}",
                        prev != null ? prev.epoch : "none", next.epoch);

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
            if (!shardUpdateNeeded(keyspaceShards, prev, next))
                return;

            // recalculating the shards will repopulate this via the existing callbacks
            log2ShardMap = new ConcurrentHashMap<>();
            keyspaceShards = applyUpdatedMetadata(keyspaceShards, prev, next, this::nextLogId, this::onNewLog);
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
                case NONE:
                    if (current != null)
                        updated.put(keyspace, current);
                    break;
                case DROP:
                    // Don't carry forward the state for the dropped keyspace
                    break;
                case REPLICA_GROUP:
                    // if there's an existing keyspace shards instance, update it, otherwise call through to CREATE
                    if (current != null)
                    {
                        KeyspaceShards ksShards = current.withUpdatedMetadata(next.schema.getKeyspaceMetadata(keyspace), next, logIdProvider, onNewLog);
                        updated.put(keyspace, ksShards);
                        break;
                    }
                case CREATE:
                    Preconditions.checkState(current == null,
                                             "Attempted to create a new keyspace shard for keyspace %s, but it already exists", keyspace);
                    KeyspaceShards ksShards = KeyspaceShards.make(next.schema.getKeyspaceMetadata(keyspace),
                                                                  next,
                                                                  logIdProvider,
                                                                  onNewLog);
                    updated.put(keyspace, ksShards);
                    break;
                case MIGRATE_TO:
                case MIGRATE_FROM:
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

                if (prevKsm == null)
                    return nextKsm.useMutationTracking() ? UpdateDecision.CREATE : UpdateDecision.NONE;

                if (nextKsm == null)
                    return prevKsm.useMutationTracking() ? UpdateDecision.DROP : UpdateDecision.NONE;

                if (!prevKsm.useMutationTracking() && !nextKsm.useMutationTracking())
                {
                    Preconditions.checkState(!hasExisting, "Existing shards found for keyspace, but prev & current ksm has mutation tracking disabled");
                    return UpdateDecision.NONE;
                }

                if (prevKsm.useMutationTracking() && !nextKsm.useMutationTracking())
                {
                    return UpdateDecision.MIGRATE_FROM;
                }

                if (!prevKsm.useMutationTracking() && nextKsm.useMutationTracking())
                {
                    Preconditions.checkState(!hasExisting, "Existing shard found for keyspace, but prev ksn has mutation tracking disabled");
                    return UpdateDecision.MIGRATE_TO;
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
            Preconditions.checkArgument(keyspace.params.replicationType.isTracked());

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
            return shards.get(groups.forRange(token).range());
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

    // TODO (later): a more intelligent heuristic for offsets included in broadcasts
    private static class ReplicatedOffsetsBroadcaster
    {
        // TODO (later): a more intelligent heuristic for scheduling broadcasts
        private static final long TRANSIENT_BROADCAST_INTERVAL_MILLIS = 200;
        private static final long DURABLE_BROADCAST_INTERVAL_MILLIS = 60_000;

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
            MutationTrackingService.instance.forEachKeyspace(ks -> run(ks, durable));
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

            Message<BroadcastLogOffsets> message = Message.out(Verb.BROADCAST_LOG_OFFSETS, replicatedOffsets);

            for (InetAddressAndPort target : shard.remoteReplicas())
                if (FailureDetector.instance.isAlive(target))
                    MessagingService.instance().send(message, target);
        }
    }

    private static class LogStatePersister implements Runnable
    {
        // TODO (expected): consider a different interval
        private static final long PERSIST_INTERVAL_MINUTES = 1;

        void start()
        {
            executor.scheduleWithFixedDelay(this, PERSIST_INTERVAL_MINUTES, PERSIST_INTERVAL_MINUTES, TimeUnit.MINUTES);
        }

        @Override
        public void run()
        {
            MutationTrackingService.instance.forEachKeyspace(this::run);
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
    }
}
