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

package org.apache.cassandra.service.replication.migration;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

/**
 * Cluster wide per keyspace mutation tracking migration state.
 * Tracks ranges needing migration per keyspace
 * Only schema changes and repair coordinators execute TCM transformations; replicas read for routing of reads/writes.
 */
public class MutationTrackingMigrationState implements MetadataValue<MutationTrackingMigrationState>
{
    public static final MutationTrackingMigrationState EMPTY =
        new MutationTrackingMigrationState(Epoch.EMPTY, ImmutableMap.of());

    @Nonnull
    public final ImmutableMap<String, KeyspaceMigrationInfo> keyspaceInfo;

    @Nonnull
    public final Epoch lastModified;

    public MutationTrackingMigrationState(@Nonnull Epoch lastModified,
                                         @Nonnull Map<String, KeyspaceMigrationInfo> keyspaceInfo)
    {
        checkNotNull(lastModified);
        checkNotNull(keyspaceInfo);
        this.lastModified = lastModified;
        this.keyspaceInfo = ImmutableMap.copyOf(keyspaceInfo);
    }

    @Override
    public MutationTrackingMigrationState withLastModified(Epoch epoch)
    {
        return new MutationTrackingMigrationState(epoch, keyspaceInfo);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    private MutationTrackingMigrationState withUpdatedKeyspaceInfo(Epoch epoch, KeyspaceMigrationInfo info)
    {
        ImmutableMap.Builder<String, KeyspaceMigrationInfo> updated = ImmutableMap.builder();
        for (Map.Entry<String, KeyspaceMigrationInfo> entry : keyspaceInfo.entrySet())
        {
            if (!entry.getKey().equals(info.keyspace))
                updated.put(entry.getKey(), entry.getValue());
        }

        if (info != null && !info.isComplete())
            updated.put(info.keyspace, info);

        return new MutationTrackingMigrationState(epoch, updated.build());
    }

    private MutationTrackingMigrationState withoutKeyspace(Epoch epoch, String keyspace)
    {
        ImmutableMap.Builder<String, KeyspaceMigrationInfo> updated = ImmutableMap.builder();
        for (Map.Entry<String, KeyspaceMigrationInfo> entry : keyspaceInfo.entrySet())
        {
            if (!entry.getKey().equals(keyspace))
                updated.put(entry.getKey(), entry.getValue());
        }

        return new MutationTrackingMigrationState(epoch, updated.build());
    }

    /**
     * Start migration for keyspace with full ring pending for all tables.
     *
     * @param keyspace keyspace name
     * @param tableIds collection of table IDs to migrate
     * @param epoch epoch for this state change
     */
    public MutationTrackingMigrationState withKeyspaceMigrating(String keyspace, Collection<TableId> tableIds, Epoch epoch)
    {
        checkNotNull(keyspace);
        checkNotNull(tableIds);
        checkNotNull(epoch);

        KeyspaceMigrationInfo existingInfo = keyspaceInfo.get(keyspace);

        // If migration info already exists, we need to reverse direction
        if (existingInfo != null)
            return withUpdatedKeyspaceInfo(epoch, existingInfo.withDirectionReversed(tableIds, epoch));

        // Compute full ring range...
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Range<Token> fullRing = new Range<>(minimumToken, minimumToken);
        Set<Range<Token>> fullRingSet = Collections.singleton(fullRing);

        // ...and assign it to each table
        ImmutableMap.Builder<TableId, NormalizedRanges<Token>> pendingRangesBuilder = ImmutableMap.builder();
        for (TableId tableId : tableIds)
        {
            pendingRangesBuilder.put(tableId, NormalizedRanges.normalizedRanges(fullRingSet));
        }

        // Create new migration info with all tables pending full ring
        KeyspaceMigrationInfo newInfo = new KeyspaceMigrationInfo(
            keyspace,
            pendingRangesBuilder.build(),
            epoch
        );

        return withUpdatedKeyspaceInfo(epoch, newInfo);
    }

    /**
     * Subtract the repaired ranges from table's pending set.
     * Removes keyspace from state when all tables have been fully repaired.
     */
    public MutationTrackingMigrationState withRangesRepairedForTable(@Nonnull String keyspace,
                                                                     @Nonnull TableId tableId,
                                                                     @Nonnull Collection<Range<Token>> repairedRanges,
                                                                     @Nonnull Epoch epoch)
    {
        checkNotNull(keyspace);
        checkNotNull(tableId);
        checkNotNull(repairedRanges);
        checkNotNull(epoch);

        // noop if we raced with a migration completing repair
        KeyspaceMigrationInfo info = keyspaceInfo.get(keyspace);
        if (info == null)
            return this;

        // Subtract repaired ranges from table's pending set
        KeyspaceMigrationInfo updated = info.withRangesRepairedForTable(epoch, tableId, repairedRanges);

        // if all tables fully repaired, remove keyspace (migration complete)
        if (updated.isComplete())
            return withoutKeyspace(epoch, keyspace);

        return withUpdatedKeyspaceInfo(epoch, updated);
    }

    /**
     * Remove keyspaces from migration state
     */
    public MutationTrackingMigrationState dropKeyspaces(Epoch nextEpoch, @Nonnull Set<String> removed)
    {
        checkNotNull(removed);

        if (keyspaceInfo.isEmpty() || Sets.intersection(keyspaceInfo.keySet(), removed).isEmpty())
            return this;

        MutationTrackingMigrationState nextState = this;
        for (String keyspace : removed)
            nextState = nextState.withoutKeyspace(nextEpoch, keyspace);

        return nextState;
    }

    /**
     * Remove dropped tables from migration states.
     * Completes keyspace migration if all tables removed.
     */
    public MutationTrackingMigrationState dropTables(@Nonnull Set<TableId> tableIds,
                                                     @Nonnull Epoch epoch)
    {
        checkNotNull(tableIds);
        checkNotNull(epoch);

        if (tableIds.isEmpty() || keyspaceInfo.isEmpty())
            return this;

        ImmutableMap.Builder<String, KeyspaceMigrationInfo> updated = ImmutableMap.builder();
        boolean anyChanged = false;

        for (Map.Entry<String, KeyspaceMigrationInfo> entry : keyspaceInfo.entrySet())
        {
            String keyspace = entry.getKey();
            KeyspaceMigrationInfo info = entry.getValue();

            // Remove dropped tables from this keyspace's migration
            KeyspaceMigrationInfo newInfo = info.withTablesRemoved(tableIds);

            if (newInfo == null || newInfo.isComplete())
            {
                // All tables removed - migration complete, don't add back to map
                anyChanged = true;
            }
            else if (newInfo != info)
            {
                // Some tables removed
                updated.put(keyspace, newInfo);
                anyChanged = true;
            }
            else
            {
                // No tables removed (none were in this keyspace)
                updated.put(keyspace, info);
            }
        }

        if (!anyChanged)
            return this;

        return new MutationTrackingMigrationState(epoch, updated.build());
    }

    public KeyspaceMigrationInfo getKeyspaceInfo(String keyspace)
    {
        return keyspaceInfo.get(keyspace);
    }

    public boolean isMigrating(String keyspace)
    {
        return keyspaceInfo.containsKey(keyspace);
    }

    public boolean hasMigratingKeyspaces()
    {
        return !keyspaceInfo.isEmpty();
    }

    /**
     * Validate migration state against schema.
     * Lenient for keyspaces being added/removed in same transaction.
     */
    public void validateAgainstSchema(DistributedSchema schema)
    {
        for (Map.Entry<String, KeyspaceMigrationInfo> entry : keyspaceInfo.entrySet())
        {
            String keyspace = entry.getKey();
            KeyspaceMigrationInfo info = entry.getValue();

            // Skip validation if keyspace doesn't exist - it may be being created or was dropped
            KeyspaceMetadata ksm = schema.getKeyspaces().getNullable(keyspace);
            if (ksm == null)
                continue;

            // Validate all tables in migration exist in schema
            for (TableId tableId : info.pendingRangesPerTable.keySet())
            {
                TableMetadata table = schema.getTableMetadata(tableId);
                checkState(table != null,
                          "Migration state for keyspace %s references non-existent table: %s",
                          keyspace, tableId);
            }
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MutationTrackingMigrationState that = (MutationTrackingMigrationState) o;
        return keyspaceInfo.equals(that.keyspaceInfo) &&
               lastModified.equals(that.lastModified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaceInfo, lastModified);
    }

    @Override
    public String toString()
    {
        return "MutationTrackingMigrationState{" +
               "keyspaceInfo=" + keyspaceInfo.keySet() +
               ", lastModified=" + lastModified +
               '}';
    }

    private static final MetadataSerializer<String> stringSerializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(String t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t);
        }

        @Override
        public String deserialize(DataInputPlus in, Version version) throws IOException
        {
            return in.readUTF();
        }

        @Override
        public long serializedSize(String t, Version version)
        {
            return TypeSizes.sizeof(t);
        }
    };

    public static final MetadataSerializer<MutationTrackingMigrationState> serializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(MutationTrackingMigrationState t, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            serializeMap(t.keyspaceInfo, out, version, stringSerializer, KeyspaceMigrationInfo.serializer);
        }

        @Override
        public MutationTrackingMigrationState deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            Map<String, KeyspaceMigrationInfo> keyspaceInfo = deserializeMap(in, version, stringSerializer, KeyspaceMigrationInfo.serializer, Maps::newHashMapWithExpectedSize);
            return new MutationTrackingMigrationState(lastModified, keyspaceInfo);
        }

        @Override
        public long serializedSize(MutationTrackingMigrationState t, Version version)
        {
            return Epoch.serializer.serializedSize(t.lastModified, version)
                   + serializedMapSize(t.keyspaceInfo, version, stringSerializer, KeyspaceMigrationInfo.serializer);
        }
    };
}
