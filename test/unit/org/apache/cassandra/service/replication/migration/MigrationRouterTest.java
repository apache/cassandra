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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.Clock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class MigrationRouterTest
{
    private static final String TEST_KEYSPACE = "test_ks";
    private static final String TEST_TABLE = "test_table";
    private static final String SYSTEM_KEYSPACE = SchemaConstants.SYSTEM_KEYSPACE_NAME;
    private static TableMetadata systemTable;
    private static Murmur3Partitioner partitioner;

    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
        partitioner = (Murmur3Partitioner) DatabaseDescriptor.getPartitioner();

        systemTable = TableMetadata.builder(SYSTEM_KEYSPACE, "system_table")
                                   .addPartitionKeyColumn("pk", UTF8Type.instance)
                                   .addRegularColumn("value", UTF8Type.instance)
                                   .partitioner(partitioner)
                                   .build();
    }

    /**
     * Helper method to create a PartitionRangeReadCommand for a specific token range.
     */
    private PartitionRangeReadCommand createRangeCommand(TableMetadata table, Token start, Token end)
    {
        Range<PartitionPosition> keyRange = new Range<>(start.minKeyBound(), end.maxKeyBound());
        DataRange dataRange = DataRange.forKeyRange(keyRange);

        return PartitionRangeReadCommand.create(table,
                                                0, // nowInSec
                                                ColumnFilter.all(table),
                                                RowFilter.none(),
                                                DataLimits.NONE,
                                                dataRange);
    }

    private Token createToken(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    private enum BoundaryType
    {
        RANGE,
        BOUNDS,
        INCLUDING_EXCLUDING_BOUNDS,
        EXCLUDING_BOUNDS
    }

    private PartitionRangeReadCommand createRangeCommandWithBoundaryType(TableMetadata table, Token start, Token end, BoundaryType boundaryType)
    {
        AbstractBounds<PartitionPosition> keyRange;

        switch (boundaryType)
        {
            case RANGE:
                keyRange = new Range<>(start.maxKeyBound(), end.maxKeyBound());
                break;
            case BOUNDS:
                keyRange = new Bounds<>(start.minKeyBound(), end.maxKeyBound());
                break;
            case INCLUDING_EXCLUDING_BOUNDS:
                keyRange = new IncludingExcludingBounds<>(start.minKeyBound(), end.minKeyBound());
                break;
            case EXCLUDING_BOUNDS:
                keyRange = new ExcludingBounds<>(start.maxKeyBound(), end.minKeyBound());
                break;
            default:
                throw new IllegalArgumentException("Unknown boundary type: " + boundaryType);
        }

        DataRange dataRange = new DataRange(keyRange, new ClusteringIndexSliceFilter(Slices.ALL, false));

        return PartitionRangeReadCommand.create(table,
                                                0,
                                                ColumnFilter.all(table),
                                                RowFilter.none(),
                                                DataLimits.NONE,
                                                dataRange);
    }

    private KeyspaceMetadata createKeyspaceMetadata(String keyspace, ReplicationType replicationType, String... tableNames)
    {
        TableMetadata[] tables = new TableMetadata[tableNames.length];
        for (int i=0; i<tableNames.length; i++)
        {
            tables[i] = TableMetadata.builder(keyspace, tableNames[i])
                                     .addPartitionKeyColumn("pk", UTF8Type.instance)
                                     .addRegularColumn("value", UTF8Type.instance)
                                     .partitioner(partitioner)
                                     .keyspaceReplicationType(replicationType)
                                     .build();
        }

        Map<String, String> replication = ImmutableMap.of("class", "SimpleStrategy", "replication_factor", "3");
        return KeyspaceMetadata.create(keyspace, KeyspaceParams.create(false, replication, replicationType), Tables.of(tables));
    }

    private ClusterMetadata withKeyspace(ClusterMetadata cm, KeyspaceMetadata ksm)
    {
        Assert.assertFalse(cm.schema.getKeyspaces().containsKeyspace(ksm.name));
        return cm.transformer().with(new DistributedSchema(cm.schema.getKeyspaces().withAddedOrUpdated(ksm))).build().metadata;
    }

    private ClusterMetadata withMigrationInfo(ClusterMetadata cm, MutationTrackingMigrationState migrationState)
    {
        return cm.transformer().with(migrationState).build().metadata;
    }

    private KeyspaceMigrationInfo createMigrationInfo(Collection<TableId> tableIds, List<Range<Token>> pendingRanges)
    {
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable;

        if (pendingRanges.isEmpty())
        {
            pendingRangesPerTable = Collections.emptyMap();
        }
        else
        {
            NormalizedRanges<Token> normalizedRanges = NormalizedRanges.normalizedRanges(pendingRanges);
            pendingRangesPerTable = tableIds
                                    .stream()
                                    .collect(Collectors.toMap(tableId -> tableId, tableId -> normalizedRanges));
        }

        return new KeyspaceMigrationInfo(
            TEST_KEYSPACE,
            pendingRangesPerTable,
            Epoch.create(1));
    }

    private KeyspaceMigrationInfo createMigrationInfo(KeyspaceMetadata ksm, List<Range<Token>> pendingRanges)
    {
        return createMigrationInfo(ksm.tables.stream().map(t -> t.id).collect(Collectors.toList()), pendingRanges);
    }

    private ClusterMetadata createMetadata(boolean isTracked, List<Range<Token>> pendingRanges)
    {
        ClusterMetadata metadata = new ClusterMetadata(partitioner);

        ReplicationType replicationType = isTracked ? ReplicationType.tracked : ReplicationType.untracked;
        KeyspaceMetadata ksm = createKeyspaceMetadata(TEST_KEYSPACE, replicationType, TEST_TABLE);

        metadata = withKeyspace(metadata, ksm);

        KeyspaceMigrationInfo migrationInfo = createMigrationInfo(ksm, pendingRanges);
        MutationTrackingMigrationState migrationState = new MutationTrackingMigrationState(Epoch.create(1), Collections.singletonMap(TEST_KEYSPACE, migrationInfo));

        return withMigrationInfo(metadata, migrationState);
    }

    /**
     * Confirm that range reads don't get split up when there's not an active migration for them
     */
    @Test
    public void testNoPendingRanges_NoSplit()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);

        ClusterMetadata metadata = createMetadata(true, Collections.emptyList());
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);

        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        assertEquals(1, splits.size());

        MigrationRouter.RangeReadWithReplication split = splits.get(0);

        assertTrue(split.useTracked);

        // Verify range covers entire query range
        assertEquals(queryStart.minKeyBound(), split.read.dataRange().keyRange().left);
        assertEquals(queryEnd.maxKeyBound(), split.read.dataRange().keyRange().right);
    }

    @Test
    public void testSinglePendingRangeInMiddle_Splits()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);
        Token pendingStart = createToken(-400L);
        Token pendingEnd = createToken(400L);

        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);

        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        // Should split into 3 parts: [-800..-400) uses tracked, [-400..400) uses untracked, [400..800] uses tracked
        assertEquals(3, splits.size());

        // First split: [-800..-400) - before pending range, uses tracked
        MigrationRouter.RangeReadWithReplication split1 = splits.get(0);
        assertTrue(split1.useTracked);
        assertEquals(queryStart.minKeyBound(), split1.read.dataRange().keyRange().left);

        // Second split: [-400..400) - pending range, uses untracked
        MigrationRouter.RangeReadWithReplication split2 = splits.get(1);
        assertFalse(split2.useTracked);

        // Third split: [400..800] - after pending range, uses tracked
        MigrationRouter.RangeReadWithReplication split3 = splits.get(2);
        assertTrue(split3.useTracked);
        assertEquals(queryEnd.maxKeyBound(), split3.read.dataRange().keyRange().right);
    }

    @Test
    public void testMultiplePendingRanges_Splits()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);
        Token pending1Start = createToken(-400L);
        Token pending1End = createToken(-200L);
        Token pending2Start = createToken(0L);
        Token pending2End = createToken(200L);

        List<Range<Token>> pendingRanges = new ArrayList<>();
        pendingRanges.add(new Range<>(pending1Start, pending1End));
        pendingRanges.add(new Range<>(pending2Start, pending2End));

        ClusterMetadata metadata = createMetadata(true, pendingRanges);
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);

        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        // Should split into 5 parts:
        // [-800..-400) tracked, [-400..-200) untracked, [-200..0) tracked, [0..200) untracked, [200..800] tracked
        assertEquals(5, splits.size());

        assertTrue(splits.get(0).useTracked);
        assertFalse(splits.get(1).useTracked);
        assertTrue(splits.get(2).useTracked);
        assertFalse(splits.get(3).useTracked);
        assertTrue(splits.get(4).useTracked);
    }

    @Test
    public void testRangeBeforeAllPending_NoSplit()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(-600L);
        Token pendingStart = createToken(0L);
        Token pendingEnd = createToken(200L);

        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);

        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        // Should return single tracked split since not in pending range
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).useTracked);
    }

    @Test
    public void testRangeAfterAllPending_NoSplit()
    {
        Token queryStart = createToken(600L);
        Token queryEnd = createToken(800L);
        Token pendingStart = createToken(-400L);
        Token pendingEnd = createToken(400L);

        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);

        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        // Should return single untracked split since not in pending range
        assertEquals(1, splits.size());
        assertTrue(splits.get(0).useTracked);
    }

    @Test
    public void testToTrackedDirection_CorrectProtocols()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);
        Token pendingStart = createToken(0L);
        Token pendingEnd = createToken(200L);

        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);
        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        assertEquals(3, splits.size());

        // Before pending: tracked
        assertTrue(splits.get(0).useTracked);

        // Pending range: untracked
        assertFalse(splits.get(1).useTracked);

        // After pending: tracked
        assertTrue(splits.get(2).useTracked);
    }

    @Test
    public void testToUntrackedDirection_CorrectProtocols()
    {
        // For migration to untracked: all reads use untracked (no splitting needed)
        // This matches single partition behavior: shouldUseTrackedForReads returns false for all ranges
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);
        Token pendingStart = createToken(0L);
        Token pendingEnd = createToken(200L);

        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        ClusterMetadata metadata = createMetadata(false, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);
        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        // Should have 1 split (no splitting needed - all reads use untracked)
        assertEquals(1, splits.size());

        // Entire range: untracked
        assertFalse(splits.get(0).useTracked);

        // Verify it covers the entire query range
        assertEquals(queryStart, splits.get(0).read.dataRange().startKey().getToken());
        assertEquals(queryEnd, splits.get(0).read.dataRange().stopKey().getToken());
    }

    @Test
    public void testSplitsAreContiguousAndCoverEntireRange()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);
        Token pending1Start = createToken(-400L);
        Token pending1End = createToken(-200L);
        Token pending2Start = createToken(0L);
        Token pending2End = createToken(200L);

        List<Range<Token>> pendingRanges = new ArrayList<>();
        pendingRanges.add(new Range<>(pending1Start, pending1End));
        pendingRanges.add(new Range<>(pending2Start, pending2End));

        ClusterMetadata metadata = createMetadata(true, pendingRanges);
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        PartitionRangeReadCommand command = createRangeCommand(testTable, queryStart, queryEnd);
        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        // verify query bounds
        assertEquals(queryStart.minKeyBound(), splits.get(0).read.dataRange().keyRange().left);
        assertEquals(queryEnd.maxKeyBound(), splits.get(splits.size() - 1).read.dataRange().keyRange().right);

        // verify split to split continuity and tracked/untracked alternation
        for (int i = 0; i < splits.size() - 1; i++)
        {
            PartitionPosition currentEnd = splits.get(i).read.dataRange().keyRange().right;
            PartitionPosition nextStart = splits.get(i + 1).read.dataRange().keyRange().left;

            assertEquals(currentEnd, nextStart);
            assertNotEquals(splits.get(i).useTracked, splits.get(i + 1).useTracked);
        }
    }

    @Test
    public void testSystemKeyspacesAlwaysUntracked()
    {
        Token queryStart = createToken(-800L);
        Token queryEnd = createToken(800L);
        Token pendingStart = createToken(0L);
        Token pendingEnd = createToken(200L);

        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));

        PartitionRangeReadCommand command = createRangeCommand(systemTable, queryStart, queryEnd);

        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        assertEquals(1, splits.size());

        // system keyspaces always use untracked path
        assertFalse(splits.get(0).useTracked);

        assertEquals(queryStart.minKeyBound(), splits.get(0).read.dataRange().keyRange().left);
        assertEquals(queryEnd.maxKeyBound(), splits.get(0).read.dataRange().keyRange().right);
    }

    /**
     * Helper method to test range splitting with a specific boundary type and expected split count.
     * Migration boundary is at token 0: (minToken, 0] is pending/migrated.
     */
    private void assertRangeSplit(BoundaryType boundaryType, long startToken, long endToken, int expectedSplits, String description)
    {
        // Setup: pending range is (minToken, 0] - meaning tokens <= 0 are migrating
        Token splitPoint = createToken(0L);
        Range<Token> pendingRange = new Range<>(partitioner.getMinimumToken(), splitPoint);

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        Token start = createToken(startToken);
        Token end = createToken(endToken);

        PartitionRangeReadCommand command = createRangeCommandWithBoundaryType(testTable, start, end, boundaryType);
        List<MigrationRouter.RangeReadWithReplication> splits = MigrationRouter.splitRangeRead(metadata, command);

        String testDesc = String.format("%s with %s [%d, %d]", boundaryType, description, startToken, endToken);

        assertEquals(testDesc + " - wrong number of splits", expectedSplits, splits.size());

        // Verify splits are contiguous and cover entire range
        if (splits.size() > 0)
        {
            assertEquals(testDesc + " - first split doesn't start at query start",
                        command.dataRange().keyRange().left,
                        splits.get(0).read.dataRange().keyRange().left);

            assertEquals(testDesc + " - last split doesn't end at query end",
                        command.dataRange().keyRange().right,
                        splits.get(splits.size() - 1).read.dataRange().keyRange().right);

            for (int i = 0; i < splits.size() - 1; i++)
            {
                assertEquals(testDesc + " - splits not contiguous at index " + i,
                            splits.get(i).read.dataRange().keyRange().right,
                            splits.get(i + 1).read.dataRange().keyRange().left);
            }

            if (splits.size() > 1)
            {
                for (int i = 0; i < splits.size() - 1; i++)
                {
                    assertNotEquals(testDesc + " - tracked/untracked should alternate at index " + i,
                                  splits.get(i).useTracked,
                                  splits.get(i + 1).useTracked);
                }
            }
        }
    }

    /**
     * Test range splitting correctness
     */
    @Test
    public void testRangeSplitWithAllBoundaryTypes()
    {
        // Range evenly crossing split point
        assertRangeSplit(BoundaryType.RANGE, -100L, 100L, 2, "evenly crossing");
        assertRangeSplit(BoundaryType.BOUNDS, -100L, 100L, 2, "evenly crossing");
        assertRangeSplit(BoundaryType.INCLUDING_EXCLUDING_BOUNDS, -100L, 100L, 2, "evenly crossing");
        assertRangeSplit(BoundaryType.EXCLUDING_BOUNDS, -100L, 100L, 2, "evenly crossing");

        // Range ending at split point
        assertRangeSplit(BoundaryType.RANGE, -100L, 0L, 1, "ending at split"); // (start, 0] includes 0, all pending
        assertRangeSplit(BoundaryType.BOUNDS, -100L, 0L, 1, "ending at split"); // [start, 0] includes 0, all pending
        assertRangeSplit(BoundaryType.INCLUDING_EXCLUDING_BOUNDS, -100L, 0L, 1, "ending at split"); // [start, 0) excludes 0, all pending
        assertRangeSplit(BoundaryType.EXCLUDING_BOUNDS, -100L, 0L, 1, "ending at split"); // (start, 0) excludes 0, all pending

        // Range starting at split point
        assertRangeSplit(BoundaryType.RANGE, 0L, 100L, 1, "starting at split"); // (0, end] excludes 0, all non-pending
        assertRangeSplit(BoundaryType.BOUNDS, 0L, 100L, 2, "starting at split"); // [0, end] includes 0, needs split
        assertRangeSplit(BoundaryType.INCLUDING_EXCLUDING_BOUNDS, 0L, 100L, 2, "starting at split"); // [0, end) includes 0, needs split
        assertRangeSplit(BoundaryType.EXCLUDING_BOUNDS, 0L, 100L, 1, "starting at split"); // (0, end) excludes 0, all non-pending

        // Range entirely before split
        assertRangeSplit(BoundaryType.RANGE, -100L, -50L, 1, "entirely before");
        assertRangeSplit(BoundaryType.BOUNDS, -100L, -50L, 1, "entirely before");
        assertRangeSplit(BoundaryType.INCLUDING_EXCLUDING_BOUNDS, -100L, -50L, 1, "entirely before");
        assertRangeSplit(BoundaryType.EXCLUDING_BOUNDS, -100L, -50L, 1, "entirely before");

        // Range entirely after split
        assertRangeSplit(BoundaryType.RANGE, 50L, 100L, 1, "entirely after");
        assertRangeSplit(BoundaryType.BOUNDS, 50L, 100L, 1, "entirely after");
        assertRangeSplit(BoundaryType.INCLUDING_EXCLUDING_BOUNDS, 50L, 100L, 1, "entirely after");
        assertRangeSplit(BoundaryType.EXCLUDING_BOUNDS, 50L, 100L, 1, "entirely after");
    }

    /**
     * Test write routing through MigrationRouter for migration to tracked replication.
     * Writes always use tracked replication regardless of pendingRanges.
     */
    @Test
    public void testWriteRoutingToTracked_AlwaysTracked()
    {
        Token tokenInPending = createToken(0L);
        Token tokenOutsidePending = createToken(500L);

        Range<Token> pendingRange = new Range<>(createToken(-200L), createToken(200L));

        ClusterMetadata metadata = createMetadata(true, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        assertTrue(MigrationRouter.shouldUseTrackedForWrites(metadata, TEST_KEYSPACE, testTable.id, tokenInPending));
        assertTrue(MigrationRouter.shouldUseTrackedForWrites(metadata, TEST_KEYSPACE, testTable.id, tokenOutsidePending));
    }

    /**
     * Test write routing through MigrationRouter for migration to untracked replication.
     * Writes use tracked for tokens in pending ranges (still migrating),untracked for completed ranges.
     */
    @Test
    public void testWriteRoutingToUntracked_PerRangeRouting()
    {
        Token tokenInPending = createToken(0L);
        Token tokenOutsidePending = createToken(500L);

        Range<Token> pendingRange = new Range<>(createToken(-200L), createToken(200L));

        ClusterMetadata metadata = createMetadata(false, Collections.singletonList(pendingRange));
        TableMetadata testTable = metadata.schema.getKeyspaceMetadata(TEST_KEYSPACE).getTableOrViewNullable(TEST_TABLE);

        assertTrue(MigrationRouter.shouldUseTrackedForWrites(metadata, TEST_KEYSPACE, testTable.id, tokenInPending));
        assertFalse(MigrationRouter.shouldUseTrackedForWrites(metadata, TEST_KEYSPACE, testTable.id, tokenOutsidePending));
    }

    @Test
    public void testMultiTableMutationRouting_ToUntracked()
    {
        ClusterMetadata metadata = new ClusterMetadata(partitioner);

        KeyspaceMetadata ksm = createKeyspaceMetadata(TEST_KEYSPACE, ReplicationType.untracked, "table1", "table2");
        metadata = withKeyspace(metadata, ksm);

        TableMetadata table1 = ksm.getTableNullable("table1");
        TableMetadata table2 = ksm.getTableNullable("table2");

        ClusterMetadata.Transformer transformer = metadata.transformer();

        // table1 migrating to untracked, table2 complete
        MutationTrackingMigrationState migrationState = metadata.mutationTrackingMigrationState.withKeyspaceMigrating(ksm.name, Collections.singleton(table1.id), transformer.epoch());
        metadata = transformer.with(migrationState).build().metadata;

        // Create a mutation with both tables
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        PartitionUpdate update1 = PartitionUpdate.emptyUpdate(table1, key);
        PartitionUpdate update2 = PartitionUpdate.emptyUpdate(table2, key);

        Mutation mutation = new Mutation(MutationId.none(), TEST_KEYSPACE, key, ImmutableMap.of(table1.id, update1, table2.id, update2), Clock.Global.nanoTime(), ReadCommand.PotentialTxnConflicts.ALLOW);

        MigrationRouter.RoutedMutations routed = MigrationRouter.routeMutations(metadata, Collections.singletonList(mutation));

        Mutation trackedMutation = (Mutation) routed.trackedMutations.get(0);
        Mutation untrackedMutation = (Mutation) routed.untrackedMutations.get(0);

        // table 1 is still migrating, so it should be in the tracked mutation
        assertEquals(Collections.singleton(table1.id), trackedMutation.getTableIds());

        // table 2 is done migrating, so it should appear in the untracked mutation
        assertEquals(Collections.singleton(table2.id), untrackedMutation.getTableIds());
    }

    /**
     * Test mutation routing with multiple tables - some tracked, some untracked.
     * This verifies that routeMutations correctly filters mutations to separate tracked/untracked tables.
     */
    @Test
    public void testMultiTableMutationRouting_ToTracked()
    {
        ClusterMetadata metadata = new ClusterMetadata(partitioner);

        KeyspaceMetadata ksm = createKeyspaceMetadata(TEST_KEYSPACE, ReplicationType.tracked, "table1", "table2");
        metadata = withKeyspace(metadata, ksm);

        TableMetadata table1 = ksm.getTableNullable("table1");
        TableMetadata table2 = ksm.getTableNullable("table2");

        ClusterMetadata.Transformer transformer = metadata.transformer();

        // table1 migrating to untracked, table2 complete
        MutationTrackingMigrationState migrationState = metadata.mutationTrackingMigrationState.withKeyspaceMigrating(ksm.name, Collections.singleton(table1.id), transformer.epoch());
        metadata = transformer.with(migrationState).build().metadata;

        // Create a mutation with both tables
        DecoratedKey key = partitioner.decorateKey(UTF8Type.instance.decompose("key"));
        PartitionUpdate update1 = PartitionUpdate.emptyUpdate(table1, key);
        PartitionUpdate update2 = PartitionUpdate.emptyUpdate(table2, key);

        Mutation mutation = new Mutation(MutationId.none(), TEST_KEYSPACE, key, ImmutableMap.of(table1.id, update1, table2.id, update2), Clock.Global.nanoTime(), ReadCommand.PotentialTxnConflicts.ALLOW);

        MigrationRouter.RoutedMutations routed = MigrationRouter.routeMutations(metadata, Collections.singletonList(mutation));

        Mutation trackedMutation = (Mutation) routed.trackedMutations.get(0);

        // since we're migrating to tracked replication, both updates should be tracked as well
        assertEquals(Set.of(table1.id, table2.id), trackedMutation.getTableIds());

        assertEquals(0, routed.untrackedMutations.size());
    }
}
