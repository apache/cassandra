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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.PojoToString;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.newHashMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

// TODO this will mostly go away once we can move TableMigrationState into the table schema
public class ConsensusMigrationState implements MetadataValue<ConsensusMigrationState>
{
    public static ConsensusMigrationState EMPTY = new ConsensusMigrationState(Epoch.EMPTY, ImmutableMap.of());
    @Nonnull
    public final Map<TableId, TableMigrationState> tableStates;

    public final Epoch lastModified;

    public ConsensusMigrationState(@Nonnull Epoch lastModified, @Nonnull Map<TableId, TableMigrationState> tableStates)
    {
        checkNotNull(tableStates, "tableStates is null");
        checkNotNull(lastModified, "lastModified is null");
        this.lastModified = lastModified;
        this.tableStates = ImmutableMap.copyOf(tableStates);
    }

    public Map<String, Object> toMap(@Nullable Set<String> keyspaceNames, @Nullable Set<String> tableNames)
    {
        return ImmutableMap.of("lastModifiedEpoch", lastModified.getEpoch(),
                               "tableStates", tableStatesAsMaps(keyspaceNames, tableNames),
                               "version", PojoToString.CURRENT_VERSION);
    }

    private List<Map<String, Object>> tableStatesAsMaps(@Nullable Set<String> keyspaceNames,
                                                        @Nullable Set<String> tableNames)
    {
        ImmutableList.Builder<Map<String, Object>> builder = ImmutableList.builder();
        for (TableMigrationState tms : tableStates.values())
        {
            if (keyspaceNames != null && !keyspaceNames.contains(tms.keyspaceName))
                continue;
            if (tableNames != null && !tableNames.contains(tms.tableName))
                continue;
            builder.add(tms.toMap());
        }
        return builder.build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsensusMigrationState that = (ConsensusMigrationState) o;
        return tableStates.equals(that.tableStates);
    }

    public ConsensusMigrationState withReversedMigrations(Map<TableId, TableMetadata> tables, Epoch epoch)
    {
        if (tables.isEmpty())
            return this;

        ImmutableMap.Builder<TableId, TableMigrationState> updated = ImmutableMap.builder();

        tableStates.forEach((id, state) -> {
            if (!tables.containsKey(id))
                updated.put(id, state);
        });

        tables.values().forEach(metadata -> {
            TableMigrationState state = tableStates.get(metadata.id);
            if (state != null)
                updated.put(metadata.id, state.reverseMigration(ConsensusMigrationTarget.fromTransactionalMode(metadata.params.transactionalMode), epoch));
        });

        return new ConsensusMigrationState(lastModified, updated.build());
    }

    private static void withRangesMigrating(Map<TableId, TableMigrationState> current, ImmutableMap.Builder<TableId, TableMigrationState> next, TableMetadata metadata, List<Range<Token>> ranges, boolean overwrite)
    {
        TableMigrationState tableState;
        ConsensusMigrationTarget target = ConsensusMigrationTarget.fromTransactionalMode(metadata.params.transactionalMode);
        if (!overwrite && current.containsKey(metadata.id))
        {
            tableState = current.get(metadata.id).withRangesMigrating(ranges, target);
        }
        else
        {
            tableState = new TableMigrationState(metadata.keyspace, metadata.name, metadata.id, target, ImmutableSet.of(), ImmutableMap.of(Epoch.EMPTY, ranges));
        }
        next.put(metadata.id, tableState);
    }

    private static void putUnchanged(Map<TableId, TableMigrationState> current, ImmutableMap.Builder<TableId, TableMigrationState> next, Set<TableId> changed)
    {
        current.forEach((id, migrationState) -> {
            if (!changed.contains(id))
                next.put(id, migrationState);
        });
    }

    private static void putUnchanged(Map<TableId, TableMigrationState> current, ImmutableMap.Builder<TableId, TableMigrationState> next, Collection<TableMetadata> changed)
    {
        Set<TableId> changedIds = changed.stream().map(TableMetadata::id).collect(Collectors.toSet());
        putUnchanged(current, next, changedIds);
    }

    public ConsensusMigrationState withRangesMigrating(Collection<TableMetadata> tables, List<Range<Token>> ranges, boolean overwrite)
    {
        ImmutableMap.Builder<TableId, TableMigrationState> updated = ImmutableMap.builder();
        putUnchanged(tableStates, updated, tables);
        tables.forEach(metadata -> withRangesMigrating(tableStates, updated, metadata, ranges, overwrite));
        return new ConsensusMigrationState(lastModified, updated.build());
    }

    public ConsensusMigrationState withMigrationsCompletedFor(Collection<TableId> completed)
    {
        ImmutableMap.Builder<TableId, TableMigrationState> updated = ImmutableMap.builder();
        putUnchanged(tableStates, updated, new HashSet<>(completed));
        for (Map.Entry<TableId, TableMigrationState> entry : tableStates.entrySet())
        {
            if (completed.contains(entry.getKey()))
                continue;
            updated.put(entry);
        }
        return new ConsensusMigrationState(lastModified, updated.build());
    }

    public ConsensusMigrationState withRangesRepairedAtEpoch(TableMetadata metadata, List<Range<Token>> ranges, Epoch minEpoch)
    {
        TableMigrationState state = Preconditions.checkNotNull(tableStates.get(metadata.id));
        state = state.withRangesRepairedAtEpoch(ranges, minEpoch);

        if (state.hasMigratedFullTokenRange(metadata.partitioner))
        {
            return withMigrationsCompletedFor(Collections.singleton(metadata.id));
        }
        else
        {
            ImmutableMap.Builder<TableId, TableMigrationState> updated = ImmutableMap.builder();
            putUnchanged(tableStates, updated, Collections.singleton(metadata.id));
            updated.put(metadata.id, state);
            return new ConsensusMigrationState(lastModified, updated.build());
        }

    }

    public ConsensusMigrationState withMigrationsRemovedFor(Set<TableId> removed)
    {
        ImmutableMap.Builder<TableId, TableMigrationState> updated = ImmutableMap.builder();
        putUnchanged(tableStates, updated, removed);
        return new ConsensusMigrationState(lastModified, updated.build());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableStates);
    }

    @Override
    public ConsensusMigrationState withLastModified(Epoch epoch)
    {
        ImmutableMap.Builder<TableId, TableMigrationState> newMap = ImmutableMap.builderWithExpectedSize(tableStates.size());
        tableStates.forEach((tableId, tableState) -> {
            newMap.put(tableId, tableState.withReplacementForEmptyEpoch(epoch));
        });
        return new ConsensusMigrationState(epoch, newMap.build());
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public void validateAgainstSchema(DistributedSchema schema)
    {
        tableStates.forEach((id, migrationState) -> {
            TableMetadata metadata = schema.getTableMetadata(id);
            Preconditions.checkState(ConsensusMigrationTarget.fromTransactionalMode(metadata.params.transactionalMode).equals(migrationState.targetProtocol));
        });
    }

    public static final MetadataSerializer<ConsensusMigrationState> serializer = new MetadataSerializer<ConsensusMigrationState>()
    {
        @Override
        public void serialize(ConsensusMigrationState consensusMigrationState, DataOutputPlus out, Version version) throws IOException
        {
            Epoch.serializer.serialize(consensusMigrationState.lastModified, out, version);
            serializeMap(consensusMigrationState.tableStates, out, version, TableId.metadataSerializer, TableMigrationState.serializer);
        }

        @Override
        public ConsensusMigrationState deserialize(DataInputPlus in, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            Map<TableId, TableMigrationState> tableMigrationStates = deserializeMap(in, version, TableId.metadataSerializer, TableMigrationState.serializer, newHashMap());
            return new ConsensusMigrationState(lastModified, tableMigrationStates);
        }

        @Override
        public long serializedSize(ConsensusMigrationState t, Version version)
        {
            return Epoch.serializer.serializedSize(t.lastModified, version)
                   + serializedMapSize(t.tableStates, version, TableId.metadataSerializer, TableMigrationState.serializer);
        }
    };
}
