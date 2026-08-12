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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

/**
 * TCM state tracking mutation tracking migration for a keyspace. Since repair advances the migration, and
 * and repair sessions operate against tables, this class tracks repairs on every table that existed in the
 * keyspace when the migration started.
 * At the beginning of a migration, the full range is added to the pendingRanges, and as repairs are completed, the
 * repaired ranges are subtracted from the pending ranges. When the pending range list is empty, the migration is finished.
 */
public class KeyspaceMigrationInfo
{
    @Nonnull public final String keyspace;
    @Nonnull public final Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable;
    @Nonnull public final Epoch startedAtEpoch;

    public KeyspaceMigrationInfo(@Nonnull String keyspace,
                                 @Nonnull Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable,
                                 @Nonnull Epoch startedAtEpoch)
    {
        this.keyspace = Objects.requireNonNull(keyspace);
        this.pendingRangesPerTable = ImmutableMap.copyOf(pendingRangesPerTable);
        this.startedAtEpoch = Objects.requireNonNull(startedAtEpoch);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        KeyspaceMigrationInfo that = (KeyspaceMigrationInfo) o;
        return Objects.equals(keyspace, that.keyspace) && Objects.equals(pendingRangesPerTable, that.pendingRangesPerTable) && Objects.equals(startedAtEpoch, that.startedAtEpoch);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Reverse migration direction. Since unfinished migrations can be aborted, ranges that have not completed migrating
     * in the previous direction are immediately rolled back. For ranges that did complete migration, or tables that were
     * added since migration started, migration in the other direction is now required, so they're marked pending.
     */
    public KeyspaceMigrationInfo withDirectionReversed(@Nonnull Collection<TableId> allTableIds,
                                                       @Nonnull Epoch epoch)
    {
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();

        // Reset all tables to full ring pending (includes tables currently migrating, added during migration, or already migrated)
        ImmutableMap.Builder<TableId, NormalizedRanges<Token>> reversedPendingBuilder = ImmutableMap.builder();

        for (TableId tableId : allTableIds)
        {
            Range<Token> fullRing = new Range<>(minimumToken, minimumToken);
            NormalizedRanges<Token> reversedRanges = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));

            NormalizedRanges<Token> existingPending = pendingRangesPerTable.get(tableId);
            if (existingPending != null)
            {
                Set<Range<Token>> ranges = Range.subtract(Collections.singletonList(fullRing), existingPending);
                reversedRanges = NormalizedRanges.normalizedRanges(ranges);
            }

            if (!reversedRanges.isEmpty())
                reversedPendingBuilder.put(tableId, reversedRanges);
        }

        return new KeyspaceMigrationInfo(
            keyspace,
            reversedPendingBuilder.build(),
            epoch
        );
    }

    /**
     * Remove tables from migration state. Returns null if all tables removed.
     */
    public KeyspaceMigrationInfo withTablesRemoved(@Nonnull Set<TableId> tablesToRemove)
    {
        if (tablesToRemove.isEmpty())
            return this;

        ImmutableMap.Builder<TableId, NormalizedRanges<Token>> builder = ImmutableMap.builder();
        boolean anyRemoved = false;

        for (Map.Entry<TableId, NormalizedRanges<Token>> entry : pendingRangesPerTable.entrySet())
        {
            if (!tablesToRemove.contains(entry.getKey()))
            {
                builder.put(entry.getKey(), entry.getValue());
            }
            else
            {
                anyRemoved = true;
            }
        }

        if (!anyRemoved)
            return this;

        Map<TableId, NormalizedRanges<Token>> newPending = builder.build();

        if (newPending.isEmpty())
            return null;

        return new KeyspaceMigrationInfo(
            keyspace,
            newPending,
            startedAtEpoch
        );
    }

    /**
     * Subtract repaired ranges from table's pending set.
     * Automatically removes table if all ranges repaired.
     */
    public KeyspaceMigrationInfo withRangesRepairedForTable(@Nonnull Epoch repairStartedEpoch,
                                                            @Nonnull TableId tableId,
                                                            @Nonnull Collection<Range<Token>> repairedRanges)
    {
        if (repairStartedEpoch.isBefore(startedAtEpoch))
            return this;

        NormalizedRanges<Token> currentPendingForTable = pendingRangesPerTable.get(tableId);
        if (currentPendingForTable == null)
        {
            return this;
        }

        NormalizedRanges<Token> normalizedRepaired = NormalizedRanges.normalizedRanges(repairedRanges);
        NormalizedRanges<Token> remainingForTable = currentPendingForTable.subtract(normalizedRepaired);

        ImmutableMap.Builder<TableId, NormalizedRanges<Token>> builder = ImmutableMap.builder();
        for (Map.Entry<TableId, NormalizedRanges<Token>> entry : pendingRangesPerTable.entrySet())
        {
            if (entry.getKey().equals(tableId))
            {
                if (!remainingForTable.isEmpty())
                    builder.put(tableId, remainingForTable);
            }
            else
            {
                builder.put(entry.getKey(), entry.getValue());
            }
        }

        return new KeyspaceMigrationInfo(keyspace, builder.build(), startedAtEpoch);
    }

    /**
     * Check if migration is complete (no tables have pending ranges).
     * Migration is complete when all tables have been fully repaired and removed from the map.
     */
    public boolean isComplete()
    {
        return pendingRangesPerTable.isEmpty();
    }

    public NormalizedRanges<Token> getPendingRangesForTable(@Nonnull TableId tableId)
    {
        NormalizedRanges<Token> ranges = pendingRangesPerTable.get(tableId);
        return ranges != null ? ranges : NormalizedRanges.empty();
    }

    /** The entire ring, which every table starts a migration needing to repair. */
    public static NormalizedRanges<Token> fullRing()
    {
        Token minimumToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
        return NormalizedRanges.normalizedRanges(Collections.singleton(new Range<>(minimumToken, minimumToken)));
    }

    /** Ranges that have finished migrating for a table: the full ring minus whatever is still pending. */
    public NormalizedRanges<Token> getMigratedRangesForTable(@Nonnull TableId tableId)
    {
        return fullRing().subtract(getPendingRangesForTable(tableId));
    }

    /**
     * Check if token is in any pending range.
     * Used for routing decisions during migration.
     *
     * @param token token to check
     * @return true if token is in a pending range
     */
    public boolean isTokenInPendingRange(TableId tableId, Token token)
    {
        NormalizedRanges<Token> tableRanges = pendingRangesPerTable.get(tableId);
        if (tableRanges == null)
            return false;
        return tableRanges.intersects(token);
    }

    /**
     * Check if a range intersects with any pending migration range for the given table.
     */
    public boolean isRangeInPendingMigration(TableId tableId, Range<Token> range)
    {
        NormalizedRanges<Token> tableRanges = pendingRangesPerTable.get(tableId);
        if (tableRanges == null)
            return false;
        return tableRanges.intersects(range);
    }

    /**
     * Check if the range defined by startToken and endToken intersects with any pending migration range
     * for the given table.
     */
    public boolean isRangeInPendingMigration(TableId tableId, Token startToken, Token endToken)
    {
        return isRangeInPendingMigration(tableId, new Range<>(startToken, endToken));
    }

    /**
     * Determine if read operations on a token should use tracked replication during migration.
     *
     * We only use tracked reads for ranges that have completed migrating _to_ tracked replication.
     */
    public boolean shouldUseTrackedForReads(boolean isTracked, TableId tableId, Token token)
    {
        return isTracked && !isTokenInPendingRange(tableId, token);
    }

    /**
     * Determine if write operations on a token should use tracked replication during migration.
     *
     * The only time we don't use tracked writes is when a range has completed migration to untracked replication
     */
    public boolean shouldUseTrackedForWrites(boolean isTracked, TableId tableId, Token token)
    {
        return isTracked || isTokenInPendingRange(tableId, token);
    }

    /**
     * Asserts that the given ranges are either entirely inside or entirely outside the pending
     * migration set for each specified table. During migration, reads for pending ranges continue
     * to use the untracked read path with blocking read repair, so it is safe to exclude mutation
     * tracking streaming for those ranges. However, partial overlap is not supported — repair must
     * operate on ranges that are fully migrated or fully pending.
     *
     * @param ranges the normalized ranges to check
     * @param tables the tables to check against
     * @throws IllegalStateException if ranges partially overlap with pending ranges for any table
     */
    public void assertRangesNotMixedMigration(@Nonnull NormalizedRanges<Token> ranges,
                                              @Nonnull Iterable<TableMetadata> tables)
    {
        for (TableMetadata table : tables)
        {
            NormalizedRanges<Token> pendingRanges = getPendingRangesForTable(table.id);
            if (pendingRanges.isEmpty())
                continue;

            NormalizedRanges<Token> overlap = pendingRanges.intersection(ranges);
            if (overlap.isEmpty())
                continue;

            // Some ranges overlap with pending — verify ALL ranges are pending for this table
            NormalizedRanges<Token> outside = ranges.subtract(pendingRanges);
            if (!outside.isEmpty())
                throw new IllegalStateException(String.format(
                    "Ranges for keyspace %s partially overlap with migration pending ranges for table %s. " +
                    "Ranges must be entirely inside or entirely outside the pending set.",
                    keyspace, table.name));
        }
    }

    /**
     * Convenience overload that resolves column family names to table metadata before checking.
     *
     * @param ranges the normalized ranges to check
     * @param ksm the keyspace metadata for resolving table names
     * @param columnFamilies specific table names to check, or null for all tables
     * @throws IllegalStateException if ranges partially overlap with pending ranges for any table
     */
    public void assertRangesNotMixedMigration(@Nonnull NormalizedRanges<Token> ranges,
                                              @Nonnull KeyspaceMetadata ksm,
                                              @Nullable Collection<String> columnFamilies)
    {
        checkArgument(columnFamilies == null || !columnFamilies.isEmpty(), "columnFmilies must not be empty");
        Iterable<TableMetadata> tables;
        if (columnFamilies != null)
        {
            List<TableMetadata> tableList = new ArrayList<>(columnFamilies.size());
            for (String cf : columnFamilies)
            {
                TableMetadata table = ksm.tables.getNullable(cf);
                if (table != null)
                    tableList.add(table);
            }
            tables = tableList;
        }
        else
        {
            tables = ksm.tables;
        }
        assertRangesNotMixedMigration(ranges, tables);
    }

    /**
     * Determines whether the given ranges for a table should use the tracked transfer path
     * (coordinated activation via TrackedRepairTransfer) or the untracked streaming path.
     * <p>
     * Returns true (use tracked) when:
     * - No migration is in progress for this keyspace, OR
     * - The ranges don't overlap with pending migration ranges for this table
     * <p>
     * Returns false (use untracked) when:
     * - A migration is in progress AND the ranges overlap with pending ranges
     *
     * @param metadata cluster metadata snapshot
     * @param keyspace keyspace name
     * @param tableId table to check
     * @param ranges the ranges being repaired or streamed
     * @return true if tracked transfers should be used
     */
    public static boolean shouldUseTrackedTransfers(@Nonnull ClusterMetadata metadata,
                                                    @Nonnull String keyspace,
                                                    @Nonnull TableId tableId,
                                                    @Nonnull Collection<Range<Token>> ranges)
    {
        KeyspaceMigrationInfo migrationInfo = metadata.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);
        if (migrationInfo == null)
            return true;

        NormalizedRanges<Token> pendingRanges = migrationInfo.getPendingRangesForTable(tableId);
        if (pendingRanges.isEmpty())
            return true;

        NormalizedRanges<Token> normalizedRanges = NormalizedRanges.normalizedRanges(ranges);
        NormalizedRanges<Token> overlap = pendingRanges.intersection(normalizedRanges);
        return overlap.isEmpty();
    }

    @Override
    public String toString()
    {
        return String.format("KeyspaceMigrationInfo{keyspace=%s, pendingTables=%d, started=%s}",
                             keyspace, pendingRangesPerTable.size(), startedAtEpoch);
    }

    private static final MetadataSerializer<NormalizedRanges<Token>> normalizedRangesSerializer = new MetadataSerializer<NormalizedRanges<Token>>()
    {
        @Override
        public void serialize(NormalizedRanges<Token> ranges, DataOutputPlus out, Version version) throws IOException
        {
            serializeCollection(ranges, out, version, Range.serializer);
        }

        @Override
        public NormalizedRanges<Token> deserialize(DataInputPlus in, Version version) throws IOException
        {
            List<Range<Token>> rangeList = deserializeList(in, version, Range.serializer);
            return NormalizedRanges.normalizedRanges(rangeList);
        }

        @Override
        public long serializedSize(NormalizedRanges<Token> ranges, Version version)
        {
            return serializedCollectionSize(ranges, version, Range.serializer);
        }
    };

    public static final MetadataSerializer<KeyspaceMigrationInfo> serializer = new MetadataSerializer<KeyspaceMigrationInfo>()
    {
        @Override
        public void serialize(KeyspaceMigrationInfo info, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(info.keyspace);
            serializeMap(info.pendingRangesPerTable, out, version, TableId.metadataSerializer, normalizedRangesSerializer);
            Epoch.serializer.serialize(info.startedAtEpoch, out, version);
        }

        @Override
        public KeyspaceMigrationInfo deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = deserializeMap(in, version, TableId.metadataSerializer, normalizedRangesSerializer, Maps::newHashMapWithExpectedSize);
            Epoch startedAtEpoch = Epoch.serializer.deserialize(in, version);
            return new KeyspaceMigrationInfo(keyspace, pendingRangesPerTable, startedAtEpoch);
        }

        @Override
        public long serializedSize(KeyspaceMigrationInfo info, Version version)
        {
            return sizeof(info.keyspace) +
                   serializedMapSize(info.pendingRangesPerTable, version, TableId.metadataSerializer, normalizedRangesSerializer) +
                   Epoch.serializer.serializedSize(info.startedAtEpoch, version);
        }
    };
}
