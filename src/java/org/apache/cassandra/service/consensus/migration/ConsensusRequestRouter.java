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

import java.util.Optional;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import accord.primitives.Routable.Domain;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.getConsensusMigratedAt;
import static org.apache.cassandra.service.consensus.migration.ConsensusMigrationTarget.paxos;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.accord;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV1;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV2;

/**
 * Helper class to decide where to route a request that requires consensus, migrating a key if necessary
 * before rerouting.
 */
public class ConsensusRequestRouter
{
    public enum ConsensusRoutingDecision
    {
        paxosV1,
        paxosV2,
        accord,
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
            return accord;

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

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull ClusterMetadata cm, @Nonnull DecoratedKey key, @Nonnull String keyspace, @Nonnull String table, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
    {
        TableMetadata metadata = metadata(cm, keyspace, table);

        // Non-distributed tables always take the Paxos path
        if (metadata == null)
            return pickPaxos();
        return routeAndMaybeMigrate(cm, metadata, key, consistencyLevel, requestTime, timeoutNanos, isForWrite);
    }

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull ClusterMetadata cm, @Nonnull DecoratedKey key, @Nonnull TableId tableId, ConsistencyLevel consistencyLevel,  Dispatcher.RequestTime requestTime, long timeoutNanos, boolean isForWrite)
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
            return pickMigrated(tms.targetProtocol);

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
        checkState(pickPaxos() != paxosV1, "Can't migrate from PaxosV1 to anything");

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
                return pickMigrated(tms.targetProtocol);

            if (tms.targetProtocol == paxos)
            {
                // Run the Accord barrier txn now so replicas don't start independent
                // barrier transactions to accomplish the migration
                // They still might need to go through the fast local path for barrier txns
                // at each replica, but they won't create their own txn since we created it here
                ConsensusKeyMigrationState.repairKeyAccord(key, tms.tableId, tms.minMigrationEpoch(token).getEpoch(), requestTime, true, isForWrite);
                return paxosV2;
            }
            // Fall through for repairKeyPaxos
        }

        // If it's not locally replicated then:
        // Accord -> Paxos - Paxos will ask Accord to migrate in the read at each replica if necessary
        // Paxos -> Accord - Paxos needs to be repaired before Accord runs so do it here
        if (tms.targetProtocol == paxos)
            return paxosV2;
        else
            // Should exit exceptionally if the repair is not done
            ConsensusKeyMigrationState.repairKeyPaxos(naturalReplicas, cm.epoch, key, cfs, consistencyLevel, requestTime, timeoutNanos, isLocallyReplicated, isForWrite);

        return pickMigrated(tms.targetProtocol);
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

    public boolean isKeyInMigratingOrMigratedRangeFromAccord(Epoch epoch, TableId tableId, DecoratedKey key)
    {
        ClusterMetadata cm = ClusterMetadataService.instance().fetchLogFromCMS(epoch);
        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
        return isKeyInMigratingOrMigratedRangeFromAccord(getTableMetadata(cm, tableId), tms, key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state.
     */
    public boolean isKeyInMigratingOrMigratedRangeFromAccord(TableMetadata metadata, TableMigrationState tms, DecoratedKey key)
    {
        if (!metadata.params.transactionalMigrationFrom.isMigrating())
            return false;

        // No state means no migration for this table
        if (tms == null)
            return false;

        if (tms.targetProtocol == ConsensusMigrationTarget.accord)
            return false;

        if (tms.migratingAndMigratedRanges.intersects(key.getToken()))
            return true;

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
