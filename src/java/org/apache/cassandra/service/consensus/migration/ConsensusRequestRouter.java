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

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.Config.LWTStrategy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigratedAt;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.service.consensus.migration.ConsensusKeyMigrationState.getConsensusMigratedAt;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.accord;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV1;
import static org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter.ConsensusRoutingDecision.paxosV2;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.ConsensusMigrationTarget.paxos;
import static org.apache.cassandra.service.consensus.migration.ConsensusTableMigrationState.TableMigrationState;

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

    public ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull DecoratedKey key, @Nonnull String keyspace, @Nonnull String table, ConsistencyLevel consistencyLevel, long queryStartNanoTime, long timeoutNanos, boolean isForWrite)
    {
        if (DatabaseDescriptor.getLWTStrategy() == LWTStrategy.accord)
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
        if (DatabaseDescriptor.getLWTStrategy() == LWTStrategy.accord)
            return accord;

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (cfs == null)
            throw new IllegalStateException("Can't route consensus request for nonexistent table %s".format(tableId.toString()));
        return routeAndMaybeMigrate(key, cfs, consistencyLevel, queryStartNanotime, timeoutNanos, isForWrite);
    }

    protected ConsensusRoutingDecision routeAndMaybeMigrate(@Nonnull  DecoratedKey key, @Nonnull ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanoTime, long timeoutNanos, boolean isForWrite)
    {
        ClusterMetadata cm = ClusterMetadata.current();

        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(cfs.getTableId());
        if (tms == null)
            return pickPaxos();

        if (Range.isInNormalizedRanges(key.getToken(), tms.migratedRanges))
            return pickMigrated(tms.targetProtocol);

        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingRanges))
            return pickBasedOnKeyMigrationStatus(cm, tms, key, cfs, consistencyLevel, queryStartNanoTime, timeoutNanos, isForWrite);

        // It's not migrated so infer the protocol from the target
        return pickNotMigrated(tms.targetProtocol);
    }

    /**
     * If the key was already migrated then we can pick the target protocol otherwise
     * we have to run a repair operation on the key to migrate it.
     */
    private static ConsensusRoutingDecision pickBasedOnKeyMigrationStatus(ClusterMetadata cm, TableMigrationState tms, DecoratedKey key, ColumnFamilyStore cfs, ConsistencyLevel consistencyLevel, long queryStartNanos, long timeoutNanos, boolean isForWrite)
    {
        checkState(pickPaxos() != paxosV1, "Can't migrate from PaxosV1 to anything");

        // If it is locally replicated we can check our local migration state to see if it was already migrated
        EndpointsForToken naturalReplicas = ReplicaLayout.forNonLocalStrategyTokenRead(cm, cfs.keyspace.getMetadata(), key.getToken());
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
                ConsensusKeyMigrationState.repairKeyAccord(key, tms.keyspaceName, tms.tableId, tms.minMigrationEpoch(key.getToken()).getEpoch(), queryStartNanos, true, isForWrite);
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
            ConsensusKeyMigrationState.repairKeyPaxos(naturalReplicas, cm.epoch, key, cfs, consistencyLevel, queryStartNanos, timeoutNanos, isLocallyReplicated, isForWrite);

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
     * using local system table state
     */
    public boolean isKeyInMigratingOrMigratedRangeFromPaxos(TableId tableId, DecoratedKey key)
    {
        TableMigrationState tms = ClusterMetadata.current().consensusMigrationState.tableStates.get(tableId);
        // No state means no migration for this table
        if (tms == null)
            return false;

        if (tms.targetProtocol == ConsensusMigrationTarget.paxos)
            return false;

        // The coordinator will need to retry either on Accord if they are trying
        // to propose their own value, or by setting the consensus migration epoch to recover an incomplete transaction
        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingAndMigratedRanges))
            return true;

        return false;
    }

    public boolean isKeyInMigratingOrMigratedRangeFromAccord(Epoch epoch, TableId tableId, DecoratedKey key)
    {
        ClusterMetadata cm = ClusterMetadataService.instance().fetchLogFromCMS(epoch);
        TableMigrationState tms = cm.consensusMigrationState.tableStates.get(tableId);
        return isKeyInMigratingOrMigratedRangeFromAccord(tms, key);
    }

    /*
     * A lightweight check against cluster metadata that doesn't check if the key has already been migrated
     * using local system table state.
     */
    public boolean isKeyInMigratingOrMigratedRangeFromAccord(TableMigrationState tms, DecoratedKey key)
    {
        // No state means no migration for this table
        if (tms == null)
            return false;

        if (tms.targetProtocol == ConsensusMigrationTarget.accord)
            return false;

        if (Range.isInNormalizedRanges(key.getToken(), tms.migratingAndMigratedRanges))
            return true;

        return false;
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
