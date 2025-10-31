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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeVersion;

import static org.junit.Assert.*;

public class KeyspaceMigrationInfoTest
{
    private static IPartitioner partitioner;
    private static TableId testTableId;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.prepareServerNoRegister();
        partitioner = DatabaseDescriptor.getPartitioner();
        assertTrue(partitioner instanceof Murmur3Partitioner);
        testTableId = TableId.generate();
    }

    @Test
    public void testConstruction()
    {
        Epoch epoch = Epoch.create(1);
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.emptyMap();

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            epoch
        );

        assertEquals("test_ks", info.keyspace);
        assertEquals(epoch, info.startedAtEpoch);
        assertTrue(info.isComplete());
        assertTrue(info.pendingRangesPerTable.isEmpty());
    }

    @Test
    public void testWithRangesRepairedForTable()
    {
        List<Range<Token>> ranges = createTestRanges();
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        // Start with full ring as pending
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            epoch2
        );

        // info should be unchanged if a repair started before migration started
        KeyspaceMigrationInfo noop = info.withRangesRepairedForTable(epoch1, testTableId, Collections.singleton(ranges.get(0)));
        assertSame(info, noop);

        // Subtract first range
        KeyspaceMigrationInfo updated = info.withRangesRepairedForTable(epoch2, testTableId, Collections.singleton(ranges.get(0)));

        assertFalse(updated.pendingRangesPerTable.get(testTableId).intersects(ranges.get(0).right));
        assertTrue(updated.pendingRangesPerTable.get(testTableId).intersects(ranges.get(1).right));
        assertFalse(updated.isComplete());

        // Subtract second range
        KeyspaceMigrationInfo updated2 = updated.withRangesRepairedForTable(epoch2, testTableId, Collections.singleton(ranges.get(1)));

        assertFalse(updated2.pendingRangesPerTable.get(testTableId).intersects(ranges.get(0).right));
        assertFalse(updated2.pendingRangesPerTable.get(testTableId).intersects(ranges.get(1).right));
        assertFalse(updated2.isComplete());
    }

    @Test
    public void testSerialization() throws IOException
    {
        List<Range<Token>> ranges = createTestRanges();
        Epoch epoch = Epoch.create(42);

        NormalizedRanges<Token> normalizedRanges = NormalizedRanges.normalizedRanges(ranges.subList(0, 2));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, normalizedRanges);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            epoch
        );

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        KeyspaceMigrationInfo.serializer.serialize(info, out, NodeVersion.CURRENT.serializationVersion());

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        KeyspaceMigrationInfo deserialized = KeyspaceMigrationInfo.serializer.deserialize(in, NodeVersion.CURRENT.serializationVersion());

        assertEquals(info, deserialized);
    }

    @Test
    public void testWithDirectionReversed_PartialCompletion()
    {
        List<Range<Token>> ranges = createTestRanges();
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        // Start with full ring
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            epoch1
        );

        // Repair one range
        Range<Token> completedRange = ranges.get(0);
        KeyspaceMigrationInfo afterRepair = info.withRangesRepairedForTable(epoch1, testTableId, Collections.singleton(completedRange));

        // Verify the range was removed
        Token tokenInCompletedRange = completedRange.right;
        assertFalse(afterRepair.getPendingRangesForTable(testTableId).intersects(tokenInCompletedRange));

        // reverse the direction
        KeyspaceMigrationInfo reversed = afterRepair.withDirectionReversed(Collections.singletonList(testTableId), epoch2);
        assertEquals(epoch2, reversed.startedAtEpoch);

        assertTrue(reversed.getPendingRangesForTable(testTableId).intersects(tokenInCompletedRange));

        // Verify that the only ranges now being migratated are the only ranges that completed the initial migration
        assertFalse(reversed.isComplete());
        assertEquals(NormalizedRanges.normalizedRanges(Collections.singletonList(completedRange)), reversed.pendingRangesPerTable.get(testTableId));
    }

    @Test
    public void testWithDirectionReversed_NoCompletion()
    {
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        // Start with full ring
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            epoch1
        );
        assertFalse(info.isComplete());

        // Reverse without any progress
        KeyspaceMigrationInfo reversed = info.withDirectionReversed(Collections.singletonList(testTableId), epoch2);

        assertEquals(epoch2, reversed.startedAtEpoch);

        // Should be complete, full ring was subtracted from full ring
        assertTrue(reversed.isComplete());
    }

    @Test
    public void testWithDirectionReversed_TableAddedDuringMigration()
    {
        List<Range<Token>> ranges = createTestRanges();
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        // Start with full ring migrating
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            epoch1
        );

        // Repair one range
        Range<Token> repairedRange = ranges.get(0);
        KeyspaceMigrationInfo afterRepair = info.withRangesRepairedForTable(epoch1, testTableId, Collections.singleton(repairedRange));

        // reverse migration direction with new table
        TableId newTableId = TableId.generate();
        List<TableId> allTables = Arrays.asList(testTableId, newTableId);
        KeyspaceMigrationInfo reversed = afterRepair.withDirectionReversed(allTables, epoch2);
        assertFalse(reversed.isComplete());

        // The original table should pending ranges, since some ranges were migrated
        assertFalse(reversed.getPendingRangesForTable(testTableId).isEmpty());
        assertEquals(NormalizedRanges.normalizedRanges(Collections.singleton(repairedRange)), reversed.getPendingRangesForTable(testTableId));

        // New table should have a full ranges since it was created fully migrated
        assertFalse(reversed.getPendingRangesForTable(newTableId).isEmpty());
        assertEquals(NormalizedRanges.normalizedRanges(Collections.singleton(fullRing)), reversed.getPendingRangesForTable(newTableId));
    }

    @Test
    public void testWithTablesRemoved_SingleTable()
    {
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));

        TableId table1 = TableId.generate();
        TableId table2 = TableId.generate();

        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = new HashMap<>();
        pendingRangesPerTable.put(table1, fullRingNormalized);
        pendingRangesPerTable.put(table2, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            Epoch.create(1)
        );

        // Remove one table
        KeyspaceMigrationInfo updated = info.withTablesRemoved(Collections.singleton(table1));

        assertNotNull(updated);
        assertFalse(updated.isComplete());
        assertNull(updated.pendingRangesPerTable.get(table1));
        assertNotNull(updated.pendingRangesPerTable.get(table2));
        assertEquals(1, updated.pendingRangesPerTable.size());
    }

    @Test
    public void testWithTablesRemoved_AllTables()
    {
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));

        TableId table1 = TableId.generate();

        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(table1, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            Epoch.create(1)
        );

        // Remove the only table - should return null (migration complete)
        KeyspaceMigrationInfo updated = info.withTablesRemoved(Collections.singleton(table1));

        assertNull(updated);
    }

    @Test
    public void testWithTablesRemoved_NonExistentTable()
    {
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));

        TableId table1 = TableId.generate();
        TableId nonExistentTable = TableId.generate();

        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(table1, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            Epoch.create(1)
        );

        // Try to remove a table that doesn't exist - should return same instance
        KeyspaceMigrationInfo updated = info.withTablesRemoved(Collections.singleton(nonExistentTable));

        assertSame(info, updated);
    }

    @Test
    public void testWithTablesRemoved_EmptySet()
    {
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        NormalizedRanges<Token> fullRingNormalized = NormalizedRanges.normalizedRanges(Collections.singleton(fullRing));

        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, fullRingNormalized);

        KeyspaceMigrationInfo info = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            Epoch.create(1)
        );

        // Remove empty set - should return same instance
        KeyspaceMigrationInfo updated = info.withTablesRemoved(Collections.emptySet());

        assertSame(info, updated);
    }

    @Test
    public void testReadAndWriteRouting_ToTracked()
    {
        Token tokenInPending = partitioner.getTokenFactory().fromString("100");
        Token tokenOutsidePending = partitioner.getTokenFactory().fromString("500");

        Token pendingStart = partitioner.getTokenFactory().fromString("-200");
        Token pendingEnd = partitioner.getTokenFactory().fromString("200");
        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        NormalizedRanges<Token> normalizedRanges = NormalizedRanges.normalizedRanges(Collections.singleton(pendingRange));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, normalizedRanges);

        KeyspaceMigrationInfo migrationInfo = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            Epoch.create(1)
        );

        // all writes should be tracked
        assertTrue(migrationInfo.shouldUseTrackedForWrites(true, testTableId, tokenInPending));
        assertTrue(migrationInfo.shouldUseTrackedForWrites(true, testTableId, tokenOutsidePending));

        // Verify reads have different behavior
        assertFalse(migrationInfo.shouldUseTrackedForReads(true, testTableId, tokenInPending));
        assertTrue(migrationInfo.shouldUseTrackedForReads(true, testTableId, tokenOutsidePending));
    }

    @Test
    public void testReadAndWriteRouting_ToUntracked()
    {
        Token tokenInPending = partitioner.getTokenFactory().fromString("100");
        Token tokenOutsidePending = partitioner.getTokenFactory().fromString("500");

        Token pendingStart = partitioner.getTokenFactory().fromString("-200");
        Token pendingEnd = partitioner.getTokenFactory().fromString("200");
        Range<Token> pendingRange = new Range<>(pendingStart, pendingEnd);

        NormalizedRanges<Token> normalizedRanges = NormalizedRanges.normalizedRanges(Collections.singleton(pendingRange));
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable = Collections.singletonMap(testTableId, normalizedRanges);

        KeyspaceMigrationInfo migrationInfo = new KeyspaceMigrationInfo(
            "test_ks",
            pendingRangesPerTable,
            Epoch.create(1)
        );

        // only writes for pending ranges are tracked
        assertTrue(migrationInfo.shouldUseTrackedForWrites(false, testTableId, tokenInPending));
        assertFalse(migrationInfo.shouldUseTrackedForWrites(false, testTableId, tokenOutsidePending));

        // reads are always untracked
        assertFalse(migrationInfo.shouldUseTrackedForReads(false, testTableId, tokenInPending));
        assertFalse(migrationInfo.shouldUseTrackedForReads(false, testTableId, tokenOutsidePending));
    }

    private List<Range<Token>> createTestRanges()
    {
        Token t1 = partitioner.getTokenFactory().fromString("100");
        Token t2 = partitioner.getTokenFactory().fromString("200");
        Token t3 = partitioner.getTokenFactory().fromString("300");
        Token t4 = partitioner.getTokenFactory().fromString("400");

        return Arrays.asList(
            new Range<>(t1, t2),
            new Range<>(t2, t3),
            new Range<>(t3, t4)
        );
    }
}
