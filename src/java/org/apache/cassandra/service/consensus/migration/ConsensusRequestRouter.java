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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import accord.primitives.Routable.Domain;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.KeyMigrationState;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.dht.Range.compareRightToken;
import static org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.getConsensusMigratedAt;
import static org.apache.cassandra.service.consensus.migration.ConsensusMigrationTarget.paxos;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingTarget.accord;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingTarget.paxosV1;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingTarget.paxosV2;

/**
 * Helper class to decide where to route a request that requires consensus, migrating a key if necessary
 * before rerouting.
 *
 * This router has to be used for all SERIAL reads and writes to ensure the correct operation of Paxos/Acocrd during migration
 * and for all non-SERIAL reads because non-SERIAL reads may end up being routed to Accord and Accord needs CRR to manage
 * any key migrations that need to be performed
 */
public class ConsensusRequestRouter
{
    public enum ConsensusRoutingTarget
    {
        paxosV1,
        paxosV2,
        accord,
    }

    public static class ConsensusRoutingDecision
    {
        public static final ConsensusRoutingDecision PAXOSV1 = new ConsensusRoutingDecision(paxosV1, IAccordService.NO_HLC);
        public static final ConsensusRoutingDecision PAXOSV2 =  new ConsensusRoutingDecision(paxosV2, IAccordService.NO_HLC);
        public static final ConsensusRoutingDecision ACCORD = new ConsensusRoutingDecision(accord, IAccordService.NO_HLC);

        public final ConsensusRoutingTarget target;
        public final long minHLC;
        public ConsensusRoutingDecision(ConsensusRoutingTarget target, long maxHLC)
        {
            this.target = target;
            this.minHLC = maxHLC + 1;
        }

        public static final ConsensusRoutingDecision decisionForTarget(ConsensusRoutingTarget target)
        {
            switch (target)
            {
                default:
                    throw new IllegalArgumentException("Unhandled target " + target);
                case paxosV1:
                    return PAXOSV1;
                case paxosV2:
                    return PAXOSV2;
                case accord:
                    return ACCORD;
            }
        }

        @Override
        public String toString()
        {
            return "ConsensusRoutingDecision{" +
                   "target=" + target +
                   ", minHLC=" + minHLC +
                   '}';
        }
    }

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
    }

    protected ConsensusRequestRouter() {}

    ConsensusRoutingDecision decisionFor(TransactionalMode transactionalMode)
    {
        if (transactionalMode.accordIsEnabled)
            return ConsensusRoutingDecision.decisionForTarget(accord);

        return pickPaxos();
    }

    /*
     * Accord never handles local tables, but if the table doesn't exist then we need to generate the correct
     * InvalidRequestException.
     */
    private static TableMetadata metadata(ClusterMetadata cm, String keyspace, String table)
    {
        Optional<KeyspaceMetadata> ksm = cm.schema.maybeGetKeyspaceMetadata(keyspace);
        if (ksm.isEmpty())
        {
            // It's a non-distributed table which is fine, but we want to error if it doesn't exist
            // We should never actually reach here unless there is a race with dropping the table
            Keyspaces localKeyspaces = Schema.instance.localKeyspaces();
            KeyspaceMetadata ksm2 = localKeyspaces.getNullable(keyspace);
            if (ksm2 == null)
                throw new InvalidRequestException("Keyspace " + keyspace + " does not exist");
            // Explicitly including views in case they get used in non-distributed tables
            TableMetadata tbm2 = ksm2.getTableOrViewNullable(table);
            if (tbm2 == null)
                throw new InvalidRequestException("Table " + keyspace + "." + table + " does not exist");
            return null;
        }
        TableMetadata tbm = ksm.get().getTableNullable(table);
        if (tbm == null)
            throw new InvalidRequestException("Table " + keyspace + "." + table + " does not exist");

        return tbm;
    }

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull ClusterMetadata cm, @Nonnull DecoratedKey key, @Nonnull TableId tableId, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        TableMetadata metadata = getTableMetadata(cm, tableId);
        // Non-distributed tables always take the Paxos path
        if (metadata == null)
            pickPaxos();
        return routeAndMaybeMigrate(cm, metadata, key, consistencyLevel, requestTime, timeoutNanos, isForWrite);
    }

    public static TableMetadata getTableMetadata(ClusterMetadata cm, TableId tableId)
    {
        TableMetadata tm = cm.schema.getTableMetadata(tableId);
        if (tm == null)
        {
            // It's a non-distributed table which is fine, but we want to error if it doesn't exist
            // We should never actually reach here unless there is a race with dropping the table
            Keyspaces localKeyspaces = Schema.instance.localKeyspaces();
            TableMetadata tm2 = localKeyspaces.getTableOrViewNullable(tableId);
            if (tm2 == null)
                throw new InvalidRequestException("Table with id " + tableId + " does not exist");
            return null;
        }
        return tm;
    }

    protected ConsensusRoutingDecision routeAndMaybeMigrate(ClusterMetadata cm, @Nonnull TableMetadata tmd, @Nonnull DecoratedKey key, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        if (!tmd.params.transactionalMigrationFrom.isMigrating())
            return decisionFor(tmd.params.transactionalMode);

        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tmd.id);
        if (tms == null)
            return decisionFor(tmd.params.transactionalMigrationFrom.from);

        Token token = key.getToken();
        if (tms.migratedRanges.intersects(token))
            return pickMigrated(tms.targetProtocol, IAccordService.NO_HLC);

        if (tms.migratingRanges.intersects(token))
            return pickBasedOnKeyMigrationStatus(cm, tmd, tms, key, consistencyLevel, requestTime, timeoutNanos, isForWrite);

        // It's not migrated so infer the protocol from the target
        return pickNotMigrated(tms.targetProtocol);
    }

    /**
     * If the key was already migrated then we can pick the target protocol otherwise
     * we have to run a repair operation on the key to migrate it.
     */
    private static ConsensusRoutingDecision pickBasedOnKeyMigrationStatus(ClusterMetadata cm, TableMetadata tmd, TableMigrationState tms, DecoratedKey key, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        checkState(pickPaxos() != ConsensusRoutingDecision.PAXOSV1, "Can't migrate from PaxosV1 to anything");

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tmd.id);
        if (cfs == null)
            throw new InvalidRequestException("Can't route consensus request to nonexistent CFS %s.%s".format(tmd.keyspace, tmd.name));

        // Migration to accord has two phases for each range, in the first phase we can't do key migration because Accord
        // can't safely read until the range has had its data repaired so Paxos continues to be used for all reads
        // and writes
        Token token = key.getToken();
        if (tms.targetProtocol == ConsensusMigrationTarget.accord && tms.repairPendingRanges.intersects(token))
            return pickPaxos();

        // If it is locally replicated we can check our local migration state to see if it was already migrated
        EndpointsForToken naturalReplicas = ReplicaLayout.forNonLocalStrategyTokenRead(cm, cfs.keyspace.getMetadata(), token);
        boolean isLocallyReplicated = naturalReplicas.lookup(FBUtilities.getBroadcastAddressAndPort()) != null;
        if (isLocallyReplicated)
        {
            ConsensusMigratedAt consensusMigratedAt = getConsensusMigratedAt(tms.tableId, key);
            // Check that key migration that was performed satisfies the requirements of the current in flight migration
            // for the range
            // Be aware that for Accord->Paxos the cache only tells us if the key was repaired locally
            // This ends up still being safe because every single Paxos read (in a migrating range) during migration will check
            // locally to see if repair is necessary
            if (consensusMigratedAt != null && tms.satisfiedByKeyMigrationAtEpoch(key, consensusMigratedAt))
                return pickMigrated(tms.targetProtocol, consensusMigratedAt.maxHLC);

            if (tms.targetProtocol == paxos)
            {
                // Run the Accord barrier txn now so replicas don't start independent
                // barrier transactions to accomplish the migration
                // They still might need to go through the fast local path for barrier txns
                // at each replica, but they won't create their own txn since we created it here
                long maxHLC = ConsensusKeyMigrationState.repairKeyAccord(key, tms.tableId, tms.minMigrationEpoch(token).getEpoch(), requestTime, true, isForWrite);
                return pickPaxos(maxHLC);
            }
            // Fall through for repairKeyPaxos
        }

        // If it's not locally replicated then:
        // Accord -> Paxos - Paxos will ask Accord to migrate in the read at each replica if necessary
        // Paxos -> Accord - Paxos needs to be repaired before Accord runs so do it here
        if (tms.targetProtocol == paxos)
            return pickPaxos();
        else
        {
            if (tms.accordSafeToReadRanges.intersects(key.getToken()))
            {
                // Should exit exceptionally if the repair is not done
                long maxHLC = ConsensusKeyMigrationState.repairKeyPaxos(naturalReplicas, cm.epoch, key, cfs, consistencyLevel, requestTime, timeoutNanos, isLocallyReplicated, isForWrite);
                return new ConsensusRoutingDecision(accord, maxHLC);
            }
            else
            {
                return pickPaxos();
            }
        }
    }

    // Allows tests to inject specific responses
    public boolean isKeyInMigratingOrMigratedRangeDuringPaxosBegin(TableId tableId, DecoratedKey key)
    {
        return isKeyInMigratingOrMigratedRangeFromPaxos(tableId, key);
    }

    // Allows tests to inject specific responses
    public boolean isKeyInMigratingOrMigratedRangeDuringPaxosAccept(TableId tableId, DecoratedKey key)
    {
        return isKeyInMigratingOrMigratedRangeFromPaxos(tableId, key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state.
     */
    public boolean isKeyInMigratingOrMigratedRangeFromPaxos(TableId tableId, DecoratedKey key)
    {
        TableMigrationState tms = ClusterMetadata.current().consensusMigrationState.tableStates.get(tableId);
        // No state means no migration for this table
        if (tms == null)
            return false;

        // We assume that key migration was already performed and it's safe to execute this on Paxos
        if (tms.targetProtocol == ConsensusMigrationTarget.paxos)
            return false;

        Token token = key.getToken();
        // Migration from Paxos to Accord has two phases and in the first phase we continue to run Paxos
        // until the data has been repaired for the range so that Accord can safely read it after Paxos key migration
        if (tms.repairPendingRanges.intersects(token))
            return false;
        // The coordinator will need to retry either on Accord if they are trying
        // to propose their own value, or by setting the consensus migration epoch to recover an incomplete transaction
        if (tms.migratingAndMigratedRanges.intersects(token))
            return true;

        return false;
    }

    public boolean isRangeManagedByAccordForReadAndWrite(ClusterMetadata cm, TableId tableId, TokenRange range)
    {
        TableMetadata metadata = getTableMetadata(cm, tableId);
        TransactionalMode transactionalMode = metadata.params.transactionalMode;
        TransactionalMigrationFromMode transactionalMigrationFromMode = metadata.params.transactionalMigrationFrom;
        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
        if (tms == null)
        {
            checkState(transactionalMigrationFromMode == TransactionalMigrationFromMode.none, "TableMigrationState shouldn't be null during migration");
            return transactionalMode.nonSerialReadsThroughAccord;
        }

        // = token ends up as a min and max key bound in C* parlance and min and max token key in Accord parlance
        // and the conversion to a C* range results in the unintentional creation of a wrap around range.
        // Instead treat it like a key and do that check.
        if (range.start().isTokenSentinel()
            && !range.end().isSentinel()
            && range.start().token().equals(range.end().token()))
        {
            checkState(!range.end().isTokenSentinel(), "Unexpected empty range");
            return isTokenManagedByAccordForReadAndWrite(metadata, tms, range.start().token());
        }
        else if (range.start().isTokenSentinel())
        {
            // Start is particularly problematic because we use min MinTokenKey to make start inclusive and this is something
            // that isn't possible to mimic at all with Range<Token>, for end it's less problematic because just the token
            // is sufficient for Accord to route the query and select the correct shards even if it might accidentally run
            // on an extra shard, the filtering will take care of it. There is nothing to do here but convert to a bounds
            // and use the bounds check
            PartitionPosition startPP = range.start().token().minKeyBound();
            PartitionPosition endPP;
            if (range.end().isTableSentinel())
                endPP = DatabaseDescriptor.getPartitioner().getMinimumToken().maxKeyBound();
            else if (range.end().isTokenSentinel())
                endPP = range.end().token().minKeyBound();
            else
                endPP = range.end().token().maxKeyBound();
            Bounds<PartitionPosition> bounds = new Bounds<>(startPP, endPP);
            return isBoundsExclusivelyManagedByAccordForRead(transactionalMode, transactionalMigrationFromMode, tms, bounds);
        }
        else
        {
            return isRangeManagedByAccordForReadAndWrite(metadata,
                                                         cm.consensusMigrationState.tableStates.get(tableId),
                                                         range.toKeyspaceRange());
        }
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the range has already been migrated
     * using local system table state. It just assumes that the key migration has already been done.
     *
     * This version is for is full read write transactions
     */
    public boolean isRangeManagedByAccordForReadAndWrite(TableMetadata metadata, TableMigrationState tms, Range<Token> range)
    {
        checkState(!range.isTrulyWrapAround(), "Accidentally created a wrap around range");
        TransactionalMode transactionalMode = metadata.params.transactionalMode;
        TransactionalMigrationFromMode migrationFrom = metadata.params.transactionalMigrationFrom;

        if (migrationFrom.isMigrating())
            checkState(tms != null, "Can't have migration in progress without tms");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;
            // Accord can only read/write the key if it is in a safe to read (repaired) range
            if (Range.intersects(tms.accordSafeToReadRanges, ImmutableList.of(range)))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !Range.intersects(tms.migratingAndMigratedRanges, ImmutableList.of(range)))
                return true;
        }

        return false;
    }

    public boolean isKeyManagedByAccordForReadAndWrite(ClusterMetadata cm, TableId tableId, DecoratedKey key)
    {
        return isTokenManagedByAccordForReadAndWrite(getTableMetadata(cm, tableId),
                                                   cm.consensusMigrationState.tableStates.get(tableId),
                                                   key.getToken());
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state. It just assumes that the key migration has already been done.
     *
     * This version is for is full read write transactions
     */
    public boolean isTokenManagedByAccordForReadAndWrite(TableMetadata metadata, TableMigrationState tms, Token token)
    {
        TransactionalMode transactionalMode = metadata.params.transactionalMode;
        TransactionalMigrationFromMode migrationFrom = metadata.params.transactionalMigrationFrom;

        if (migrationFrom.isMigrating())
            checkState(tms != null, "Can't have migration in progress without tms");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;
            // Accord can only read/write the key if it is in a safe to read (repaired) range
            if (tms.accordSafeToReadRanges.intersects(token))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }

        return false;
    }

    public boolean isKeyManagedByAccordForWrite(ClusterMetadata cm, TableId tableId, DecoratedKey key)
    {
        return isKeyManagedByAccordForWrite(getTableMetadata(cm, tableId),
                                            cm.consensusMigrationState.tableStates.get(tableId),
                                            key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state. It just assumes that the key migration has already been done.
     *
     * This version is for writes through Accord before Accord is able to safely read.
     */
    public boolean isKeyManagedByAccordForWrite(TableMetadata metadata, TableMigrationState tms, DecoratedKey key)
    {
        TransactionalMode transactionalMode = metadata.params.transactionalMode;
        TransactionalMigrationFromMode migrationFrom = metadata.params.transactionalMigrationFrom;
        Token token = key.getToken();

        if (migrationFrom.isMigrating())
            checkState(tms != null, "Can't have migration in progress without tms");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;
            // Accord can blind write to the key even if it isn't safe to read from it so use migratingAndMigratedRanges
            if (tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }
        else
        {
            // We can always allow writes through Accord and it's necessary to do that so that
            // andy premigration txns aren't exposed to non-transactional writes
            if (migrationFrom.nonSerialWritesThroughAccord() && !tms.migratedRanges.intersects(token))
                return true;
        }

        return false;
    }

    public static Txn.Kind shouldReadEphemerally(Seekables<?, ?> keys, TableParams tableParams, Txn.Kind kind)
    {
        if (kind != Kind.Read)
            return kind;
        if (!DatabaseDescriptor.getAccordEphemeralReadEnabledEnabled())
            return kind;
        // TODO (nicetohave): this could be enhanced to check the token or the range during migration or work in other modes besides full
        if (tableParams.transactionalMode != TransactionalMode.full || tableParams.transactionalMigrationFrom != TransactionalMigrationFromMode.none)
            return kind;
        // Number of ranges doesn't matter
        if (keys.domain() == Domain.Range)
            return Kind.EphemeralRead;
        if (keys.size() > 1)
            return kind;
        return Kind.EphemeralRead;
    }

    private static ConsensusRoutingDecision pickMigrated(ConsensusMigrationTarget targetProtocol, long maxHlc)
    {
        if (targetProtocol.equals(ConsensusMigrationTarget.accord))
            return new ConsensusRoutingDecision(accord, maxHlc);
        else
            return pickPaxos(maxHlc);
    }

    private static ConsensusRoutingDecision pickNotMigrated(ConsensusMigrationTarget targetProtocol)
    {
        if (targetProtocol.equals(ConsensusMigrationTarget.accord))
            return pickPaxos();
        else
            return ConsensusRoutingDecision.decisionForTarget(accord);
    }

    private static ConsensusRoutingDecision pickPaxos()
    {
        return ConsensusRoutingDecision.decisionForTarget(Paxos.useV2() ? paxosV2 : paxosV1);
    }

    private static ConsensusRoutingDecision pickPaxos(long maxHLC)
    {
        return new ConsensusRoutingDecision(Paxos.useV2() ? paxosV2 : paxosV1, maxHLC);
    }

    public static void validateSafeToReadNonTransactionally(ReadCommand command, @Nullable ClusterMetadata cm)
    {
        if (command.potentialTxnConflicts().allowed)
            return;

        String keyspace = command.metadata().keyspace;
        // System keyspaces are never managed by Accord
        if (SchemaConstants.isSystemKeyspace(keyspace))
            return;

        // Local keyspaces are never managed by Accord
        if (Schema.instance.localKeyspaces().containsKeyspace(keyspace))
            return;

        cm = cm == null ? ClusterMetadata.current() : cm;
        TableId tableId = command.metadata().id;
        TableMetadata tableMetadata = getTableMetadata(cm, tableId);
        // Null for local or dropped tables
        if (tableMetadata == null)
            return;

        TransactionalMode transactionalMode = tableMetadata.params.transactionalMode;
        TransactionalMigrationFromMode transactionalMigrationFromMode = tableMetadata.params.transactionalMigrationFrom;
        if (!transactionalMode.nonSerialReadsThroughAccord && !transactionalMigrationFromMode.nonSerialReadsThroughAccord())
            return;

        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);

        // Null with a transaction mode that reads through Accord indicates a completed migration or table created
        // to use Accord initially
        if (tms == null)
        {
            checkState(transactionalMigrationFromMode == TransactionalMigrationFromMode.none);
            if (transactionalMode.nonSerialReadsThroughAccord)
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
                if (cfs != null)
                    cfs.metric.readsRejectedOnWrongSystem.mark();
                throw new RetryOnDifferentSystemException();
            }
        }

        boolean isExclusivelyReadableFromAccord;
        if (command.isRangeRequest())
            isExclusivelyReadableFromAccord = isBoundsExclusivelyManagedByAccordForRead(transactionalMode, transactionalMigrationFromMode, tms, command.dataRange().keyRange());
        else
            isExclusivelyReadableFromAccord = isTokenExclusivelyManagedByAccordForRead(transactionalMode, transactionalMigrationFromMode, tms, ((SinglePartitionReadCommand)command).partitionKey().getToken());

        if (isExclusivelyReadableFromAccord)
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
            if (cfs != null)
                cfs.metric.readsRejectedOnWrongSystem.mark();
            throw new RetryOnDifferentSystemException();
        }
    }

    private static boolean isTokenExclusivelyManagedByAccordForRead(@Nonnull TransactionalMode transactionalMode,
                                                                     @Nonnull TransactionalMigrationFromMode migrationFrom,
                                                                     @Nonnull TableMigrationState tms,
                                                                     @Nonnull Token token)
    {
        checkNotNull(transactionalMode, "transactionalMode is null");
        checkNotNull(migrationFrom, "migrationFrom is null");
        checkNotNull(tms, "tms (TableMigrationState) is null");
        checkNotNull(token, "bounds is null");

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;

            // Accord is exclusive once the range is fully migrated to Accord, but possible to read from safely
            // when accordSafeToReadRanges covers the entire bound
            if (tms.migratedRanges.intersects(token))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !tms.migratingAndMigratedRanges.intersects(token))
                return true;
        }

        return false;
    }

    // Returns true if any part of the bound
    private static boolean isBoundsExclusivelyManagedByAccordForRead(@Nonnull TransactionalMode transactionalMode,
                                                                     @Nonnull TransactionalMigrationFromMode migrationFrom,
                                                                     @Nonnull TableMigrationState tms,
                                                                     @Nonnull AbstractBounds<PartitionPosition> bounds)
    {
        checkNotNull(transactionalMode, "transactionalMode is null");
        checkNotNull(migrationFrom, "migrationFrom is null");
        checkNotNull(tms, "tms (TableMigrationState) is null");
        checkNotNull(bounds, "bounds is null");

        BiPredicate<AbstractBounds<PartitionPosition>, NormalizedRanges<Token>> intersects = (testBounds, testRanges) -> {
            // TODO (nicetohave): Efficiency of this intersection
            for (org.apache.cassandra.dht.Range<Token> range : testRanges)
            {
                Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> intersectionAndRemainder = Range.intersectionAndRemainder(testBounds, range);
                return intersectionAndRemainder.left != null;
            }
            return false;
        };

        if (bounds.left.getToken().equals(bounds.right.getToken()) && !bounds.inclusiveLeft() && bounds.inclusiveRight())
        {
            return isTokenExclusivelyManagedByAccordForRead(transactionalMode, migrationFrom, tms, bounds.left.getToken());
        }

        if (transactionalMode.accordIsEnabled)
        {
            if (!migrationFrom.isMigrating())
                return true;
            if (migrationFrom.migratingFromAccord())
                return true;

            // Accord is exclusive once the range is fully migrated to Accord, but possible to read from safely
            // when accordSafeToReadRanges covers the entire bound
            if (intersects.test(bounds, tms.migratedRanges))
                return true;
        }
        else
        {
            // Once the migration starts only barriers are allowed to run for the key in Accord
            if (migrationFrom.migratingFromAccord() && !intersects.test(bounds, tms.migratingAndMigratedRanges))
                return true;
        }

        return false;
    }

    public enum RangeReadTarget
    {
        accord,
        normal
    }

    public static class RangeReadWithTarget
    {
        public final PartitionRangeReadCommand read;
        public final RangeReadTarget target;

        private RangeReadWithTarget(PartitionRangeReadCommand read, RangeReadTarget target)
        {
            this.read = read;
            this.target = target;
        }

        @Override
        public String toString()
        {
            return "RangeReadWithTarget{" +
                   "read=" + read +
                   ", target=" + target +
                   '}';
        }
    }

    /**
     * While it's possible to map the Accord read to a single txn it doesn't seem worth it since it's a pretty unusual
     * scenario where we do this during migration and have a lot of different read commands.
     */
    public static List<RangeReadWithTarget> splitReadIntoAccordAndNormal(ClusterMetadata cm, PartitionRangeReadCommand read, ReadCoordinator readCoordinator, Dispatcher.RequestTime requestTime)
    {
        if (!readCoordinator.isEventuallyConsistent())
            return ImmutableList.of(new RangeReadWithTarget(read, RangeReadTarget.normal));
        TableMetadata tm = getTableMetadata(cm, read.metadata().id);
        if (tm == null || (!tm.params.transactionalMode.nonSerialReadsThroughAccord && !tm.params.transactionalMigrationFrom.nonSerialReadsThroughAccord()))
            return ImmutableList.of(new RangeReadWithTarget(read, RangeReadTarget.normal));

        List<RangeReadWithTarget> result = null;
        TransactionalMode transactionalMode = tm.params.transactionalMode;
        TransactionalMigrationFromMode transactionalMigrationFromMode = tm.params.transactionalMigrationFrom;
        boolean transactionalModeReadsThroughAccord = transactionalMode.nonSerialReadsThroughAccord;
        RangeReadTarget migrationToTarget = transactionalModeReadsThroughAccord ? RangeReadTarget.accord : RangeReadTarget.normal;
        boolean migrationFromReadsThroughAccord = transactionalMigrationFromMode.nonSerialReadsThroughAccord();
        RangeReadTarget migrationFromTarget = migrationFromReadsThroughAccord ? RangeReadTarget.accord : RangeReadTarget.normal;
        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);
        if (tms == null)
        {
            if (transactionalMigrationFromMode == TransactionalMigrationFromMode.none)
                // There is no migration and no TMS so do what the schema says since no migration should be required
                return ImmutableList.of(new RangeReadWithTarget(read, transactionalModeReadsThroughAccord ? RangeReadTarget.accord : RangeReadTarget.normal));
            else
                // If we are migrating from something and there is no migration state the migration hasn't begun
                // so continue to do what we are migrating from does until the range is marked as migrating
                return ImmutableList.of(new RangeReadWithTarget(read, migrationFromReadsThroughAccord ? RangeReadTarget.accord : RangeReadTarget.normal));
        }


        // AbstractBounds can potentially be left/right inclusive while Range used to track migration is only right inclusive
        // The right way to tackle this seems to be to find the tokens that intersect the key range and then split until
        // until nothing intersects
        AbstractBounds<PartitionPosition> keyRange = read.dataRange().keyRange();
        AbstractBounds<PartitionPosition> remainder = keyRange;

        // Migrating to Accord we only read through Accord when the range is fully migrated, but migrating back
        // we stop reading from Accord as soon as the range is marked migrating and do key migration on read
        NormalizedRanges<Token> migratedRanges = transactionalModeReadsThroughAccord ? tms.migratedRanges : tms.migratingAndMigratedRanges;

        // Add the preceding range if any
        if (!migratedRanges.isEmpty())
        {
            Token firstMigratingToken = migratedRanges.get(0).left.getToken();
            int leftCmp = keyRange.left.getToken().compareTo(firstMigratingToken);
            int rightCmp = compareRightToken(keyRange.right.getToken(), firstMigratingToken);
            if (leftCmp <= 0)
            {
                if (rightCmp <= 0)
                    return ImmutableList.of(new RangeReadWithTarget(read, migrationFromTarget));
                AbstractBounds<PartitionPosition> precedingRange = keyRange.withNewRight(rightCmp <= 0 ? keyRange.right : firstMigratingToken.maxKeyBound());
                // Could be an empty bound, it's fine to let a min KeyBound and max KeyBound through as that isn't empty
                if (!precedingRange.left.equals(precedingRange.right))
                {
                    result = new ArrayList<>();
                    result.add(new RangeReadWithTarget(read.forSubRange(precedingRange, true), migrationFromTarget));
                }
            }
        }

        boolean hadAccordReads = false;
        for (Range<Token> r : migratedRanges)
        {
            Pair<AbstractBounds<PartitionPosition>, AbstractBounds<PartitionPosition>> intersectionAndRemainder = Range.intersectionAndRemainder(remainder, r);
            if (intersectionAndRemainder.left != null)
            {
                if (result == null)
                    result = new ArrayList<>();
                PartitionRangeReadCommand subRead = read.forSubRange(intersectionAndRemainder.left, result.isEmpty() ? true : false);
                result.add(new RangeReadWithTarget(subRead, migrationToTarget));
                hadAccordReads = true;
            }
            remainder = intersectionAndRemainder.right;
            if (remainder == null)
                break;
        }

        if (remainder != null)
        {
            if (result != null)
                result.add(new RangeReadWithTarget(read.forSubRange(remainder, false), migrationFromTarget));
            else
                return ImmutableList.of(new RangeReadWithTarget(read.forSubRange(remainder, true), migrationFromTarget));
        }

        checkState(result != null && !result.isEmpty(), "Shouldn't have null or empty result");
        checkState(result.get(0).read.dataRange().startKey().equals(read.dataRange().startKey()), "Split reads should encompass entire range");
        checkState(result.get(result.size() - 1).read.dataRange().stopKey().equals(read.dataRange().stopKey()), "Split reads should encompass entire range");
        if (result.size() > 1)
        {
            for (int i = 0; i < result.size() - 1; i++)
            {
                checkState(result.get(i).read.dataRange().stopKey().equals(result.get(i + 1).read.dataRange().startKey()), "Split reads should all be adjacent");
                checkState(result.get(i).target != result.get(i + 1).target, "Split reads should be for different targets");
            }
        }

        //TODO (later): https://issues.apache.org/jira/browse/CASSANDRA-20211 Range reads could use a barrier
        if (hadAccordReads)
        {
            // do barrier
        }

        return result;
    }

    /**
     * Result of splitting mutations across Accord and non-transactional boundaries
     */
    public static class SplitReads
    {
        @Nullable
        public final SinglePartitionReadCommand.Group accordReads;

        @Nullable
        public final SinglePartitionReadCommand.Group normalReads;

        private SplitReads(SinglePartitionReadCommand.Group accordReads, SinglePartitionReadCommand.Group normalReads)
        {
            this.accordReads = accordReads;
            this.normalReads = normalReads;
        }
    }

    public static SplitReads splitReadsIntoAccordAndNormal(ClusterMetadata cm, SinglePartitionReadCommand.Group reads, ReadCoordinator coordinator, Dispatcher.RequestTime requestTime)
    {
        if (!coordinator.isEventuallyConsistent())
            return new SplitReads(null, reads);
        List<SinglePartitionReadCommand> accordReads = null;
        List<SinglePartitionReadCommand> normalReads = null;

        TableMetadata tm = getTableMetadata(cm, reads.queries.get(0).metadata().id);
        if (tm == null || (!tm.params.transactionalMode.nonSerialReadsThroughAccord && !tm.params.transactionalMigrationFrom.nonSerialReadsThroughAccord()))
            return new SplitReads(null, reads);

        TransactionalMode transactionalMode = tm.params.transactionalMode;
        TransactionalMigrationFromMode transactionalMigrationFromMode = tm.params.transactionalMigrationFrom;
        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tm.id);

        for (SinglePartitionReadCommand command : reads.queries)
        {
            if (tokenShouldBeReadThroughAccord(tms, command.partitionKey().getToken(), transactionalMode, transactionalMigrationFromMode))
            {
                if (accordReads == null)
                    accordReads = new ArrayList<>(reads.queries.size());
                accordReads.add(command);
            }
            else
            {
                if (normalReads == null)
                    normalReads = new ArrayList<>(reads.queries.size());
                normalReads.add(command);
            }
        }

        // When migrating from Accord -> Paxos we need to do the Accord barrier to have acknowledged Accord writes
        // be visible to non-SERIAL reads, but from Paxos -> Accord we don't need to because read only transactions
        // don't have recovery determinism issues and Accord will honor read consistency levels and match the behavior
        // of non-serially reading Paxos transactions. Since it's a non-SERIAL read there is no guarantee of seeing
        // in-flight Paxos operations, for that you would need to read at SERIAL.
        // If the migration direction is from a mode that used to read through Accord then Accord would be
        // doing async commit so we need barriers if this mode is no longer reading through Accord.
        if (transactionalMigrationFromMode.isMigrating() && transactionalMigrationFromMode.nonSerialReadsThroughAccord() && !transactionalMode.nonSerialReadsThroughAccord && normalReads != null)
        {
            checkState(!normalReads.isEmpty());
            List<DecoratedKey> keysNeedingBarrier = null;
            long maxRequiredEpoch = Long.MIN_VALUE;
            for (SinglePartitionReadCommand readCommand : normalReads)
            {
                DecoratedKey key = readCommand.partitionKey();
                KeyMigrationState kms = ConsensusKeyMigrationState.getKeyMigrationState(cm, tms, key);
                if (!kms.paxosReadSatisfiedByKeyMigration())
                {
                    if (keysNeedingBarrier == null)
                        keysNeedingBarrier = new ArrayList<>(normalReads.size());
                    keysNeedingBarrier.add(key);
                    maxRequiredEpoch = Math.max(tms.minMigrationEpoch(key.getToken()).getEpoch(), maxRequiredEpoch);
                }
            }

            if (keysNeedingBarrier != null)
            {
                checkState(!keysNeedingBarrier.isEmpty());
                checkState(maxRequiredEpoch != Long.MIN_VALUE);
                // Local barriers don't support multiple keys so create a global one unless there is a single key
                // See BarrierType enum for explanation of global vs local
                boolean global = keysNeedingBarrier.size() > 1 ? true : false;
                ConsensusKeyMigrationState.repairKeysAccord(keysNeedingBarrier, tm.id, maxRequiredEpoch, requestTime, global, false);
            }
        }

        SinglePartitionReadCommand.Group accordGroup = accordReads != null ? SinglePartitionReadCommand.Group.create(accordReads, reads.limits()) : null;
        SinglePartitionReadCommand.Group normalGroup = normalReads != null ? SinglePartitionReadCommand.Group.create(normalReads, reads.limits()) : null;
        return new SplitReads(accordGroup, normalGroup);
    }

    private static boolean tokenShouldBeReadThroughAccord(TableMigrationState tms,
                                                          @Nonnull Token token,
                                                          @Nonnull TransactionalMode transactionalMode,
                                                          TransactionalMigrationFromMode transactionalMigrationFromMode)
    {
        boolean transactionalModeReadsThroughAccord = transactionalMode.nonSerialReadsThroughAccord;
        boolean migrationFromReadsThroughAccord = transactionalMigrationFromMode.nonSerialReadsThroughAccord();

        if (transactionalModeReadsThroughAccord && migrationFromReadsThroughAccord)
            return true;

        // Could be migrating or could be completely migrated, if it's migrating check if the key for this mutation
        if (transactionalModeReadsThroughAccord || migrationFromReadsThroughAccord)
        {
            if (tms == null)
            {
                if (transactionalMigrationFromMode == TransactionalMigrationFromMode.none)
                    // There is no migration and no TMS so do what the schema says since no migration should be required
                    return transactionalModeReadsThroughAccord;
                else
                    // If we are migrating from something and there is no migration state the migration hasn't begun
                    // so continue to do what we are migrating from does until the range is marked as migrating
                    return migrationFromReadsThroughAccord;
            }

            // In theory we can start reading from Accord immediately because we know these transactions are 100%
            // read only but then that impacts performance more so wait for the range to be completely migrated
            // when it can potentially do single replica reads
            if (transactionalModeReadsThroughAccord)
                return tms.migratedRanges.intersects(token);

            // If we are migrating from a mode that used to write to Accord then any range that isn't migrating/migrated
            // should continue to write through Accord.
            // It's not completely symmetrical because Paxos is able to read Accord's writes by performing a single key barrier
            // and regular mutations will be able to do the same thing (needs to be added along with non-transactional reads)
            // This means that migrating ranges don't need to be written through Accord because we are running Paxos now
            // and not Accord. When migrating to Accord we need to do all the writes through Accord even if we aren't
            // reading through Accord so that repair + Accord metadata is sufficient for Accord to be able to read
            // safely and deterministically from any coordinator
            if (migrationFromReadsThroughAccord)
                return !tms.migratingAndMigratedRanges.intersects(token);
        }
        return false;
    }
}
