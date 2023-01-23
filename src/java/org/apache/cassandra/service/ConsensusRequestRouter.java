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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config.LegacyPaxosStrategy;
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
import org.apache.cassandra.service.ConsensusMigrationStateStore.ConsensusMigratedAt;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair.Failure;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair.Result;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.net.Verb.CONSENSUS_KEY_MIGRATION_FINISHED;
import static org.apache.cassandra.service.ConsensusMigrationStateStore.ConsensusMigrationTarget;
import static org.apache.cassandra.service.ConsensusMigrationStateStore.ConsensusMigrationTarget.paxos;
import static org.apache.cassandra.service.ConsensusMigrationStateStore.TableMigrationState;
import static org.apache.cassandra.service.ConsensusRequestRouter.ConsensusRoutingDecision.accord;
import static org.apache.cassandra.service.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV1;
import static org.apache.cassandra.service.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV2;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Helper class to decide where to route a request that requires consensus, migrating a key if necessary
 * before rerouting.
 *
 */
public class ConsensusRequestRouter
{

    public static volatile ConsensusRequestRouter instance = new ConsensusRequestRouter();

    @VisibleForTesting
    public static void setInstance(ConsensusRequestRouter testInstance)
    {
        instance = testInstance;
    }

    @VisibleForTesting
    public static void resetInstance()
    {
        instance = new ConsensusRequestRouter();
        MIGRATION_STATE_CACHE.invalidateAll();
    }

    private static final int EMPTY_KEY_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(Pair.create(null, UUID.randomUUID())));
    private static final int VALUE_SIZE = Ints.checkedCast(ObjectSizes.measureDeep(new ConsensusMigrationStateStore.ConsensusMigratedAt(Epoch.EMPTY, ConsensusMigrationTarget.accord)));
    private static final Cache<Pair<ByteBuffer, UUID>, ConsensusMigrationStateStore.ConsensusMigratedAt> MIGRATION_STATE_CACHE = Caffeine.newBuilder()
                                                                                                                                         .maximumWeight(DatabaseDescriptor.getConsensusMigrationCacheSizeInMiB() << 20)
                                                                                                                                         .<Pair<ByteBuffer, UUID>, ConsensusMigrationStateStore.ConsensusMigratedAt>weigher((k, v) -> EMPTY_KEY_SIZE + Ints.checkedCast(ByteBufferUtil.estimatedSizeOnHeap(k.left)) + VALUE_SIZE)
                                                                                                                                         .executor(ImmediateExecutor.INSTANCE)
                                                                                                                                         .build();
    private static final Function<Pair<ByteBuffer, UUID>, ConsensusMigrationStateStore.ConsensusMigratedAt> LOADING_FUNCTION = k -> SystemKeyspace.loadConsensusKeyMigrationState(k.left, k.right);

    protected ConsensusRequestRouter() {}

    /*
     * This will trigger a distributed migration for the key, but will only block on local completion
     * so Paxos reads can return a result as soon as the local state is ready
     */
    public void maybePerformAccordToPaxosKeyMigration(KeyMigrationState keyMigrationStatus, boolean isForWrite)
    {
        if (keyMigrationStatus.paxosReadSatisfiedByKeyMigration())
            return;

        // TODO better query start time?
        TableMigrationState tms = keyMigrationStatus.tableMigrationState;
        repairKeyAccord(keyMigrationStatus.key, tms.tableId, nanoTime(), true, isForWrite);
    }

    public void maybeSaveAccordKeyMigrationLocally(PartitionKey partitionKey, Epoch epoch)
    {
        TableId tableId = partitionKey.tableId();
        UUID tableUUID = tableId.asUUID();
        DecoratedKey dk = partitionKey.partitionKey();
        ByteBuffer key = dk.getKey();

        TableMigrationState tms = ClusterMetadata.current().migrationStateSnapshot.tableStates.get(tableId);
        if (tms == null)
            return;

        ConsensusMigratedAt migratedAt = new ConsensusMigratedAt(epoch, paxos);
        if (!tms.paxosReadSatisfiedByKeyMigrationAtEpoch(dk, migratedAt))
            return;

        MIGRATION_STATE_CACHE.put(Pair.create(key, tableUUID), migratedAt);
        Stage.MUTATION.execute(() -> SystemKeyspace.saveConsensusKeyMigrationState(key, tableUUID, migratedAt));
    }

    public enum ConsensusRoutingDecision
    {
        paxosV1,
        paxosV2,
        accord,
    }

    public static class ConsensusKeyMigrationFinished
    {
        @Nonnull
        private final UUID tableId;
        @Nonnull
        private final ByteBuffer partitionKey;
        @Nonnull
        private final ConsensusMigratedAt consensusMigratedAt;

        private ConsensusKeyMigrationFinished(@Nonnull UUID tableId, @Nonnull ByteBuffer partitionKey, @Nonnull ConsensusMigratedAt consensusMigratedAt)
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

    public static final IVerbHandler<ConsensusKeyMigrationFinished> consensusKeyMigrationFinishedHandler = new IVerbHandler<>()
    {
        @Override
        public void doVerb(Message<ConsensusKeyMigrationFinished> message)
        {
            // Order doesn't matter, existing values don't matter, version doesn't matter
            // If any of this races or goes backwards the result is that key migration is
            // reattempted and it should be very rare
            MIGRATION_STATE_CACHE.put(Pair.create(message.payload.partitionKey, message.payload.tableId), message.payload.consensusMigratedAt);
            SystemKeyspace.saveConsensusKeyMigrationState(message.payload.partitionKey, message.payload.tableId, message.payload.consensusMigratedAt);
        }
    };

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull DecoratedKey key, @Nonnull String keyspace, @Nonnull String table, ConsistencyLevel consistencyLevel, long queryStartNanoTime, long timeoutNanos, boolean isForWrite)
    {
        if (DatabaseDescriptor.getLegacyPaxosStrategy() == LegacyPaxosStrategy.accord)
            return accord;

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, table);
        if (cfs == null)
            throw new IllegalStateException("Can't route consensus request to nonexistent CFS %s.%s".format(keyspace, table));
        return routeAndMaybeMigrate(key, cfs, consistencyLevel, queryStartNanoTime, timeoutNanos, isForWrite);
    }

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull DecoratedKey key, @Nonnull TableId tableId, ConsistencyLevel consistencyLevel,  long queryStartNanotime, long timeoutNanos, boolean isForWrite)
    {
        // In accord mode there might be migration state in CM (unless cleanup gets added), but it doesn't
        // matter. All other consensus protocols are not used.
        // TODO This is based off of file based config, we should really use TrM so the cluster
        // always agrees regardless of file based configuration. For new clusters maybe we use this to set an inital value
        // and rename to initial paxos strategy?
        if (DatabaseDescriptor.getLegacyPaxosStrategy() == LegacyPaxosStrategy.accord)
            return accord;

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (cfs == null)
            throw new IllegalStateException("Can't route consensus request for nonexistent table %s".format(tableId.toString()));
        return routeAndMaybeMigrate(key, cfs, consistencyLevel, queryStartNanotime, timeoutNanos, isForWrite);
    }

    protected ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull  DecoratedKey key, @Nonnull ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanoTime, long timeoutNanos, boolean isForWrite)
    {
        ClusterMetadata cm = ClusterMetadata.current();

        TableMigrationState tms = cm.migrationStateSnapshot.tableStates.get(cfs.getTableId());
        if (tms == null)
            return pickPaxos();

        // TODO copy a fast range intersection check from Accord
        if (Range.isInNormalizedRanges(key.getToken(), tms.migratedRanges))
            return pickMigrated(tms.targetProtocol);

        // If we arrive here because an Accord operation was rejected we don't actually need to run repair if we
        // know the epoch the Accord txn ran at since it might satisfy the migration for the key
        // This should be pretty rare since it can only occur for requests that race with the range beginning
        // migration so didn't bother plumbing through the Epoch.
        // Paxos transactions as currently implemented don't guarantee repair before returning so we have to do it here.
        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingRanges))
            return pickBasedOnKeyMigrationStatus(cm.epoch, tms, key, cfs, consistencyLevel, queryStartNanoTime, timeoutNanos, isForWrite);

        // It's not migrated so infer the protocol from the target
        return pickNotMigrated(tms.targetProtocol);
    }

    /**
     * If the key was already migrated then we can pick the target protocol otherwise
     * we have to run on Paxos which might trigger migration or find out if it was migrated (such as
     * when token aware routing is not used)
     */
    private static ConsensusRoutingDecision pickBasedOnKeyMigrationStatus(Epoch currentEpoch, TableMigrationState tms, DecoratedKey key, ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanos, long timeoutNanos, boolean isForWrite)
    {
        checkState(pickPaxos() != paxosV1, "Can't migrate from PaxosV1 to anything");

        // If it is locally replicated we can check our local migration state to see if it was already migrated
        EndpointsForToken naturalReplicas = cfs.keyspace.getReplicationStrategy().getNaturalReplicasForToken(key);
        boolean isLocallyReplicated = naturalReplicas.lookup(FBUtilities.getBroadcastAddressAndPort()) != null;
        if (isLocallyReplicated)
        {
            ConsensusMigrationStateStore.ConsensusMigratedAt consensusMigratedAt = MIGRATION_STATE_CACHE.get(Pair.create(key.getKey(), cfs.getTableId().asUUID()), LOADING_FUNCTION);
            // Check that key migration that was performed satisfies the requirements of the current in flight migration
            // for the range
            // Be aware that for Accord->Paxos the cache only tells us if the key was repaired locally
            // This ends up still being safe because every single Paxos read (in a migrating range) during migration will check
            // locally to see if the local Accord repair occurred and trigger it if necessary
            // TODO Lookup efficiency?
            if (consensusMigratedAt != null && tms.satisfiedByKeyMigrationAtEpoch(key, consensusMigratedAt))
                return pickMigrated(tms.targetProtocol);

            if (tms.targetProtocol == paxos)
            {
                // Run the Accord barrier txn now so replicas don't start independent
                // barrier transactions to accomplish the migration
                // They will still need to go through the fast path for barrier txns
                // at each replica but won't need to send any messages in the happy path
                repairKeyAccord(key, tms.tableId, queryStartNanos, false, isForWrite);
                return paxosV2;
            }
            // Fall through for repairKeyPaxos
        }

        // If it's not locally replicated then:
        // Accord -> Paxos - Paxos will ask Accord to migrate in the read at each replica if necessary
        // Paxos -> Accord - Paxos needs to be repaired before Accord runs so do it here
        if (tms.targetProtocol == paxos)
        {
            return paxosV2;
        }
        else
        {
            // Should exit exceptionally if the repair is not done
            repairKeyPaxos(naturalReplicas, currentEpoch, key, cfs, consistencyLevel, queryStartNanos, timeoutNanos, isLocallyReplicated, isForWrite);
        }

        return pickMigrated(tms.targetProtocol);
    }

    private static void saveConsensusKeyMigration(EndpointsForToken replicas, ConsensusKeyMigrationFinished finished)
    {
        Message<ConsensusKeyMigrationFinished> out = Message.out(CONSENSUS_KEY_MIGRATION_FINISHED, finished);
        replicas.endpoints();
        for (Replica replica : replicas)
        {
            if (replica.isSelf())
                saveConsensusKeyMigrationLocally(finished);
            else
                MessagingService.instance().send(out, replica.endpoint());
        }
    }

    private static void saveConsensusKeyMigrationLocally(ConsensusKeyMigrationFinished finished)
    {
        MIGRATION_STATE_CACHE.put(Pair.create(finished.partitionKey, finished.tableId), finished.consensusMigratedAt);
        Stage.MUTATION.execute(() -> SystemKeyspace.saveConsensusKeyMigrationState(finished.partitionKey, finished.tableId, finished.consensusMigratedAt));
    }

    /*
     * Trigger a distributed repair of Accord state for this key.
     */
    private static void repairKeyAccord(DecoratedKey key, TableId tableId, long queryStartNanos, boolean onlyBlockOnLocalRepair, boolean isForWrite)
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (isForWrite)
             ClientRequestsMetricsHolder.casWriteMetrics.accordKeyMigrations.mark();
        else
            ClientRequestsMetricsHolder.casReadMetrics.accordKeyMigrations.mark();
        long start = nanoTime();
        try
        {
            // Pretend we did it LOL
            saveConsensusKeyMigrationLocally(new ConsensusKeyMigrationFinished(tableId.asUUID(), key.getKey(), new ConsensusMigratedAt(ClusterMetadata.current().epoch, paxos)));
        }
        finally
        {
            cfs.metric.keyMigration.addNano(nanoTime() - start);
        }
//        Seekables keys = null;
//        TableMetadata metadata = cfs.metadata();
//        // We don't actually intend to read the data, it should be skipped automatically by Accord,
//        // but if migration is cancelled it might actually execute so create a limit 1 read here
//        // TODO Is there a better way to create a dummy read that does no work?
//        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(metadata, 0, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.LIMIT_ONE, key, ALL);
//        // Don't need barrier all read because we are only trying to bring this node up to date
//        Read read = TxnRead.createSerialRead(command, consistencyLevel);
//        // Create a barrier txn
//        Txn.InMemory barrierTxn = new Txn.InMemory(keys, read, TxnQuery.ALL);
//        // TODO add proper timeout
//        // TODO support only blocking on the local repair
//        AccordService.instance().coordinate(barrierTxn, consistencyLevel, queryStartNanos);
    }

    private static void repairKeyPaxos(EndpointsForToken naturalReplicas, Epoch currentEpoch, DecoratedKey key, ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanos, long timeoutNanos, boolean isLocallyReplicated, boolean isForWrite)
    {
        if (isForWrite)
            ClientRequestsMetricsHolder.accordWriteMetrics.paxosKeyMigrations.mark();
        else
            ClientRequestsMetricsHolder.accordReadMetrics.paxosKeyMigrations.mark();
        TableMetadata tableMetadata = cfs.metadata();
        PaxosRepair repair = PaxosRepair.create(consistencyLevel, key, null, tableMetadata, timeoutNanos);
        long start = nanoTime();
        repair.start(queryStartNanos);
        // TODO Migration makes a mess of metrics because it changes the type of operation as it goes, and only
        // some of the operations attempted actually record latency
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
                        saveConsensusKeyMigration(naturalReplicas, new ConsensusKeyMigrationFinished(tableMetadata.id.asUUID(), key.getKey(), new ConsensusMigratedAt(currentEpoch, ConsensusMigrationTarget.accord)));
                    return;
                case FAILURE:
                    Failure failure = (Failure)result;
                    // TODO better error handling and more accurate exception values
                    // null is the way it populates timeouts
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

    public static class KeyMigrationState
    {
        static final KeyMigrationState MIGRATION_NOT_NEEDED = new KeyMigrationState(null, null, null, null);

        public final ConsensusMigratedAt consensusMigratedAt;

        public final Epoch currentEpoch;

        public final TableMigrationState tableMigrationState;

        public final DecoratedKey key;

        private KeyMigrationState(ConsensusMigratedAt consensusMigratedAt, Epoch currentEpoch, TableMigrationState tableMigrationState, DecoratedKey key)
        {
            this.consensusMigratedAt = consensusMigratedAt;
            this.currentEpoch = currentEpoch;
            this.tableMigrationState = tableMigrationState;
            this.key = key;
        }

        public boolean paxosReadSatisfiedByKeyMigration()
        {
            // No migration in progress, it's safe
            if (tableMigrationState == null)
                return true;

            return tableMigrationState.paxosReadSatisfiedByKeyMigrationAtEpoch(key, consensusMigratedAt);
        }
    }

    // Allows tests to inject specific responses
    @VisibleForTesting
    public boolean isKeyInMigratingOrMigratedRangeDuringPaxosBegin(TableId tableId, DecoratedKey key)
    {
        return isKeyInMigratingOrMigratedRangeFromPaxos(tableId, key);
    }

    // Allows tests to inject specific responses
    @VisibleForTesting
    public boolean isKeyInMigratingOrMigratedRangeDuringPaxosAccept(TableId tableId, DecoratedKey key)
    {
        return isKeyInMigratingOrMigratedRangeFromPaxos(tableId, key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state
     */
    public boolean isKeyInMigratingRangeFromPaxos(TableMigrationState tms, DecoratedKey key)
    {
        // No state means no migration for this table
        if (tms == null)
            return false;

        if (tms.targetProtocol == paxos)
            return false;

        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingRanges))
        {
            return true;
        }

        return false;
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state
     */
    public boolean isKeyInMigratingOrMigratedRangeFromPaxos(TableId tableId, DecoratedKey key)
    {
        TableMigrationState tms = ClusterMetadata.current().migrationStateSnapshot.tableStates.get(tableId);
        // No state means no migration for this table
        if (tms == null)
            return false;

        if (tms.targetProtocol == ConsensusMigrationTarget.paxos)
            return false;

        // The coordinator will need to retry either on Accord if they are trying
        // to propose their own value, or by setting the consensus migration epoch to recover an incomplete transaction
        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingAndMigratedRanges))
        {
            return true;
        }

        return false;
    }

    // TODO should this bein ConsensusMigrationStateStore
    // Used by callers to avoid looking up the TMS multiple times
    public @Nullable TableMigrationState getTableMigrationState(long epoch, TableId tableId)
    {
        ClusterMetadata cm = ClusterMetadataService.instance.maybeCatchup(Epoch.create(0, epoch));
        TableMigrationState tms = cm.migrationStateSnapshot.tableStates.get(tableId);
        return tms;
    }

    public boolean isKeyInMigratingOrMigratedRangeFromAccord(Epoch epoch, TableId tableId, DecoratedKey key)
    {
        ClusterMetadata cm = ClusterMetadataService.instance.maybeCatchup(epoch);
        TableMigrationState tms = cm.migrationStateSnapshot.tableStates.get(tableId);
        return isKeyInMigratingOrMigratedRangeFromAccord(tms, key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state
     */
    public boolean isKeyInMigratingOrMigratedRangeFromAccord(TableMigrationState tms, DecoratedKey key)
    {
        // No state means no migration for this table
        if (tms == null)
            return false;

        if (tms.targetProtocol == ConsensusMigrationTarget.accord)
            return false;

        // The coordinator will need to retry either on Accord if they are trying
        // to propose their own value, or by setting the consensus migration epoch to recover an incomplete transaction
        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingAndMigratedRanges))
        {
            return true;
        }

        return false;
    }

    /*
     * Should be called where we know we replicate the key so that the system table contains useful information
     * about whether the migration already occurred.
     *
     * This is a more expensive check that might read from the system table to determine if migration occurred.
     */
    public KeyMigrationState getKeyMigrationState(TableId tableId, DecoratedKey key)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        TableMigrationState tms = cm.migrationStateSnapshot.tableStates.get(tableId);
        // No state means no migration for this table
        if (tms == null)
            return KeyMigrationState.MIGRATION_NOT_NEEDED;

        // The coordinator will need to retry either on Accord if they are trying
        // to propose their own value, or by setting the consensus migration epoch to recover an incomplete transaction
        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingRanges))
        {
            ConsensusMigrationStateStore.ConsensusMigratedAt consensusMigratedAt = MIGRATION_STATE_CACHE.get(Pair.create(key.getKey(), tableId.asUUID()), LOADING_FUNCTION);
            if (consensusMigratedAt == null)
                return new KeyMigrationState(null, cm.epoch, tms, key);
            return new KeyMigrationState(consensusMigratedAt, cm.epoch, tms, key);
        }

        return KeyMigrationState.MIGRATION_NOT_NEEDED;
    }

    private static ConsensusRoutingDecision pickMigrated(ConsensusMigrationTarget targetProtocol)
    {
        if (targetProtocol.equals(ConsensusMigrationTarget.accord))
            return accord;
        else
            return pickPaxos();
    }

    private static ConsensusRoutingDecision pickNotMigrated(ConsensusMigrationTarget targetProtocol)
    {
        if (targetProtocol.equals(ConsensusMigrationTarget.accord))
            return pickPaxos();
        else
            return accord;
    }

    private static ConsensusRoutingDecision pickPaxos()
    {
        return Paxos.useV2() ? paxosV2 : paxosV1;
    }
}
