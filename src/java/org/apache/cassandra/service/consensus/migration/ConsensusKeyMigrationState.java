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

package org.apache.cassandra.service.consensus.migration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.api.BarrierType;
import accord.primitives.Seekables;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigratedAt;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair.Failure;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair.Result;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.net.Verb.CONSENSUS_KEY_MIGRATION;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget.paxos;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.TableMigrationState;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Tracks the migration state of individual keys storing the migration (or not) in system.consensus_migration_state
 * with an in-memory cache in front. Only locally replicated keys are tracked here to avoid storing too much
 * state when token aware routing is not used.
 *
 * It is safe to migrate keys multiple times so no effort is made to ensure exactly once behavior and the system table
 * expires key migration state after 7 days.
 */
public abstract class ConsensusKeyMigrationState
{
    /*
     * Used to notify other replicas when key migration has occurred so they can
     * also cache that the key migration was done
     */
    public static class ConsensusKeyMigrationFinished
    {
        @Nonnull
        private final UUID tableId;
        @Nonnull
        private final ByteBuffer partitionKey;
        @Nonnull
        private final ConsensusMigratedAt consensusMigratedAt;

        private ConsensusKeyMigrationFinished(@Nonnull UUID tableId,
                                              @Nonnull ByteBuffer partitionKey,
                                              @Nonnull ConsensusMigratedAt consensusMigratedAt)
        {
            this.tableId = tableId;
            this.partitionKey = partitionKey;
            this.consensusMigratedAt = consensusMigratedAt;
        }

        public static final IVersionedSerializer<ConsensusKeyMigrationFinished> serializer = new IVersionedSerializer<ConsensusKeyMigrationFinished>()
        {
            @Override
            public void serialize(ConsensusKeyMigrationFinished t, DataOutputPlus out, int version) throws IOException
            {
                UUIDSerializer.serializer.serialize(t.tableId, out, version);
                ByteBufferUtil.writeWithVIntLength(t.partitionKey, out);
                ConsensusMigratedAt.serializer.serialize(t.consensusMigratedAt, out, version);
            }

            @Override
            public ConsensusKeyMigrationFinished deserialize(DataInputPlus in, int version) throws IOException
            {
                UUID tableId = UUIDSerializer.serializer.deserialize(in, version);
                ByteBuffer partitionKey = ByteBufferUtil.readWithVIntLength(in);
                ConsensusMigratedAt consensusMigratedAt = ConsensusMigratedAt.serializer.deserialize(in, version);
                return new ConsensusKeyMigrationFinished(tableId, partitionKey, consensusMigratedAt);
            }

            @Override
            public long serializedSize(ConsensusKeyMigrationFinished t, int version)
            {
                return UUIDSerializer.serializer.serializedSize(t.tableId, version)
                       + ByteBufferUtil.serializedSizeWithVIntLength(t.partitionKey)
                       + ConsensusMigratedAt.serializer.serializedSize(t.consensusMigratedAt, version);
            }
        };
    }

    /*
     * Bundles various aspects of key migration state together to avoid multiple lookups
     * and to communicate multiple result values and state
     */
    public static class KeyMigrationState
    {
        static final KeyMigrationState MIGRATION_NOT_NEEDED = new KeyMigrationState(null, null, null, null);

        public final ConsensusMigratedAt consensusMigratedAt;

        public final Epoch currentEpoch;

        public final TableMigrationState tableMigrationState;

        public final DecoratedKey key;

        private KeyMigrationState(ConsensusMigratedAt consensusMigratedAt, Epoch currentEpoch,
                                  TableMigrationState tableMigrationState, DecoratedKey key)
        {
            this.consensusMigratedAt = consensusMigratedAt;
            this.currentEpoch = currentEpoch;
            this.tableMigrationState = tableMigrationState;
            this.key = key;
        }

        /*
         * This will trigger a distributed migration for the key, but will only block on local completion
         * so Paxos reads can return a result as soon as the local state is ready
         */
        public void maybePerformAccordToPaxosKeyMigration(boolean isForWrite)
        {
            if (paxosReadSatisfiedByKeyMigration())
                return;

            // TODO (desired): Better query start time
            TableMigrationState tms = tableMigrationState;
            repairKeyAccord(key, tms.keyspaceName, tms.tableId, tms.minMigrationEpoch(key.getToken()).getEpoch(), nanoTime(), false, isForWrite);
        }

        private boolean paxosReadSatisfiedByKeyMigration()
        {
            // No migration in progress, it's safe
            if (tableMigrationState == null)
                return true;

            return tableMigrationState.paxosReadSatisfiedByKeyMigrationAtEpoch(key, consensusMigratedAt);
        }
    }

    private static final int EMPTY_KEY_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(Pair.create(null, UUID.randomUUID())));
    private static final int VALUE_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(new ConsensusMigratedAt(Epoch.EMPTY, ConsensusMigrationTarget.accord)));

    private static final CacheLoader<Pair<ByteBuffer, UUID>, ConsensusMigratedAt> LOADING_FUNCTION = k -> SystemKeyspace.loadConsensusKeyMigrationState(k.left, k.right);
    private static final Weigher<Pair<ByteBuffer, UUID>, ConsensusMigratedAt> WEIGHER_FUNCTION = (k, v) -> EMPTY_KEY_SIZE + Ints.checkedCast(ByteBufferUtil.estimatedSizeOnHeap(k.left)) + VALUE_SIZE;
    private static final LoadingCache<Pair<ByteBuffer, UUID>, ConsensusMigratedAt> MIGRATION_STATE_CACHE =
        Caffeine.newBuilder()
                .maximumWeight(DatabaseDescriptor.getConsensusMigrationCacheSizeInMiB() << 20)
                .weigher(WEIGHER_FUNCTION)
                .executor(ImmediateExecutor.INSTANCE)
                .build(LOADING_FUNCTION);

    public static final IVerbHandler<ConsensusKeyMigrationFinished> consensusKeyMigrationFinishedHandler = message -> {
        saveConsensusKeyMigrationLocally(message.payload.partitionKey, message.payload.tableId, message.payload.consensusMigratedAt);
    };

    private ConsensusKeyMigrationState() {}

    @VisibleForTesting
    public static void reset()
    {
        MIGRATION_STATE_CACHE.invalidateAll();
    }

    public static void maybeSaveAccordKeyMigrationLocally(PartitionKey partitionKey, Epoch epoch)
    {
        TableId tableId = partitionKey.table();
        UUID tableUUID = tableId.asUUID();
        DecoratedKey dk = partitionKey.partitionKey();
        ByteBuffer key = dk.getKey();

        TableMigrationState tms = ClusterMetadata.current().consensusMigrationState.tableStates.get(tableId);
        if (tms == null)
            return;

        ConsensusMigratedAt migratedAt = new ConsensusMigratedAt(epoch, paxos);
        if (!tms.paxosReadSatisfiedByKeyMigrationAtEpoch(dk, migratedAt))
            return;

        saveConsensusKeyMigrationLocally(key, tableUUID, migratedAt);
    }

    /*
     * Should be called where we know we replicate the key so that the system table contains useful information
     * about whether the migration already occurred.
     *
     * This is a more expensive check that might read from the system table to determine if migration occurred.
     */
    public static KeyMigrationState getKeyMigrationState(TableId tableId, DecoratedKey key)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
        // No state means no migration for this table
        if (tms == null)
            return KeyMigrationState.MIGRATION_NOT_NEEDED;

        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingRanges))
        {
            ConsensusMigratedAt consensusMigratedAt = getConsensusMigratedAt(tableId, key);
            if (consensusMigratedAt == null)
                return new KeyMigrationState(null, cm.epoch, tms, key);
            return new KeyMigrationState(consensusMigratedAt, cm.epoch, tms, key);
        }

        return KeyMigrationState.MIGRATION_NOT_NEEDED;
    }

    public static @Nullable ConsensusMigratedAt getConsensusMigratedAt(TableId tableId, DecoratedKey key)
    {
        return MIGRATION_STATE_CACHE.get(Pair.create(key.getKey(), tableId.asUUID()));
    }

    /*
     * Trigger a distributed repair of Accord state for this key.
     */
    static void repairKeyAccord(DecoratedKey key,
                                String keyspace,
                                TableId tableId,
                                long minEpoch,
                                long queryStartNanos,
                                boolean global,
                                boolean isForWrite)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (isForWrite)
            ClientRequestsMetricsHolder.casWriteMetrics.accordKeyMigrations.mark();
        else
            ClientRequestsMetricsHolder.casReadMetrics.accordKeyMigrations.mark();
        long start = nanoTime();
        try
        {
            // Global will always create a transaction to effect the barrier so all replicas
            // will soon be ready to execute, but only waits for the local replica to be ready
            // Local will only create a transaction if it can't find an existing one to wait on
            BarrierType barrierType = global ? BarrierType.global_async : BarrierType.local;
            AccordService.instance().barrier(Seekables.of(new PartitionKey(tableId, key)), minEpoch, queryStartNanos, DatabaseDescriptor.getTransactionTimeout(TimeUnit.NANOSECONDS), barrierType, isForWrite);
            // We don't save the state to the cache here. Accord will notify the agent every time a barrier happens.
        }
        finally
        {
            cfs.metric.keyMigration.addNano(nanoTime() - start);
        }
    }

    static void repairKeyPaxos(EndpointsForToken naturalReplicas,
                               Epoch currentEpoch,
                               DecoratedKey key,
                               ColumnFamilyStore cfs,
                               ConsistencyLevel consistencyLevel,
                               long queryStartNanos,
                               long timeoutNanos,
                               boolean isLocallyReplicated,
                               boolean isForWrite)
    {
        if (isForWrite)
            ClientRequestsMetricsHolder.accordWriteMetrics.paxosKeyMigrations.mark();
        else
            ClientRequestsMetricsHolder.accordReadMetrics.paxosKeyMigrations.mark();
        TableMetadata tableMetadata = cfs.metadata();
        PaxosRepair repair = PaxosRepair.create(consistencyLevel, key, tableMetadata, timeoutNanos);
        long start = nanoTime();
        repair.start(queryStartNanos);
        Result result;
        try
        {
            result = repair.await();
            switch (result.outcome)
            {
                default:
                case CANCELLED:
                    throw new IllegalStateException("Unexpected PaxosRepair outcome " + result.outcome);
                case DONE:
                    // Don't want to repeatedly save this in the non-token aware case
                    if (isLocallyReplicated)
                        saveConsensusKeyMigration(naturalReplicas,
                                                  new ConsensusKeyMigrationFinished(tableMetadata.id.asUUID(),
                                                                                    key.getKey(),
                                                                                    new ConsensusMigratedAt(currentEpoch, ConsensusMigrationTarget.accord)));
                    return;
                case FAILURE:
                    Failure failure = (Failure)result;
                    if (failure.failure == null)
                        throw new CasWriteTimeoutException(WriteType.CAS, consistencyLevel, 0, 0, 0);
                    throw new RuntimeException(failure.failure);
            }
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            cfs.metric.keyMigration.addNano(nanoTime() - start);
        }
    }

    private static void saveConsensusKeyMigration(EndpointsForToken replicas, ConsensusKeyMigrationFinished finished)
    {
        Message<ConsensusKeyMigrationFinished> out = Message.out(CONSENSUS_KEY_MIGRATION, finished);
        replicas.endpoints();
        for (Replica replica : replicas)
        {
            if (replica.isSelf())
                saveConsensusKeyMigrationLocally(finished.partitionKey, finished.tableId, finished.consensusMigratedAt);
            else
                MessagingService.instance().send(out, replica.endpoint());
        }
    }

    private static void saveConsensusKeyMigrationLocally(ByteBuffer partitionKey, UUID tableId, ConsensusMigratedAt consensusMigratedAt)
    {
        // Order doesn't matter, existing values don't matter, version doesn't matter
        // If any of this races or goes backwards the result is that key migration is
        // reattempted and it should be very rare
        MIGRATION_STATE_CACHE.put(Pair.create(partitionKey, tableId), consensusMigratedAt);
        Stage.MUTATION.execute(() -> SystemKeyspace.saveConsensusKeyMigrationState(partitionKey, tableId, consensusMigratedAt));
    }
}
