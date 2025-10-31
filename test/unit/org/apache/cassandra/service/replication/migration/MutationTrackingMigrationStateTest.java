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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
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

public class MutationTrackingMigrationStateTest
{
    private static Murmur3Partitioner partitioner;
    private static TableId testTableId;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.prepareServerNoRegister();
        partitioner = Murmur3Partitioner.instance;
        testTableId = TableId.generate();
    }

    @Test
    public void testEmptyState()
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY;
        assertNotNull(state);
        assertEquals(Epoch.EMPTY, state.lastModified);
        assertTrue(state.keyspaceInfo.isEmpty());
        assertFalse(state.hasMigratingKeyspaces());
    }

    @Test
    public void testWithKeyspaceMigrating()
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY;
        Epoch epoch = Epoch.create(1);

        MutationTrackingMigrationState updated = state.withKeyspaceMigrating(
            "test_ks",
            Collections.singletonList(testTableId),
            epoch
        );

        assertNotSame(state, updated);
        assertTrue(state.keyspaceInfo.isEmpty());

        assertTrue(updated.hasMigratingKeyspaces());

        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        KeyspaceMigrationInfo expected = createExpectedKeyspaceMigrationInfo(
            "test_ks",
            testTableId,
            Collections.singleton(fullRing),
            epoch
        );

        KeyspaceMigrationInfo actual = updated.getKeyspaceInfo("test_ks");

        assertEquals(expected, actual);
    }

    @Test
    public void testStateTransitions()
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY;
        List<Range<Token>> ranges = createTestRanges();
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        // Start migration
        state = state.withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);
        assertTrue(state.hasMigratingKeyspaces());

        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        KeyspaceMigrationInfo expectedAfterStart = createExpectedKeyspaceMigrationInfo(
            "test_ks",
            testTableId,
            Collections.singleton(fullRing),
            epoch1
        );

        assertEquals(expectedAfterStart, state.getKeyspaceInfo("test_ks"));
        assertFalse(state.getKeyspaceInfo("test_ks").isComplete());

        // Subtract migrated ranges
        state = state.withRangesRepairedForTable("test_ks", testTableId, ranges, epoch2);

        Set<Range<Token>> expectedRemaining = Range.subtract(Collections.singleton(fullRing), ranges);

        KeyspaceMigrationInfo expectedAfterRepair = createExpectedKeyspaceMigrationInfo(
            "test_ks",
            testTableId,
            expectedRemaining,
            epoch1
        );

        assertEquals(expectedAfterRepair, state.getKeyspaceInfo("test_ks"));
    }

    @Test
    public void testWithMigrationsCompleted()
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY;
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        // Start migration
        state = state.withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);
        assertTrue(state.hasMigratingKeyspaces());

        // Complete migration
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        MutationTrackingMigrationState completed = state.withRangesRepairedForTable("test_ks", testTableId, Collections.singleton(fullRing), epoch2);

        assertNotSame(state, completed);

        assertFalse(completed.hasMigratingKeyspaces());
        assertNull(completed.getKeyspaceInfo("test_ks"));
    }

    @Test
    public void testWithMigrationsRemoved()
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY;
        Epoch epoch = Epoch.create(1);

        // Start migration
        state = state.withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch);

        // Remove migration
        MutationTrackingMigrationState removed = state.dropKeyspaces(epoch, Collections.singleton("test_ks"));

        assertFalse(removed.hasMigratingKeyspaces());
        assertNull(removed.getKeyspaceInfo("test_ks"));
    }

    @Test
    public void testSerializationRoundtrip() throws IOException
    {
        List<Range<Token>> ranges = createTestRanges();
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        MutationTrackingMigrationState original = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1)
            .withRangesRepairedForTable("test_ks", testTableId, ranges, epoch2);

        // Serialize
        DataOutputBuffer out = new DataOutputBuffer();
        MutationTrackingMigrationState.serializer.serialize(original, out, NodeVersion.CURRENT_METADATA_VERSION);

        // Deserialize
        DataInputBuffer in = new DataInputBuffer(out.unsafeGetBufferAndFlip(), false);
        MutationTrackingMigrationState deserialized = MutationTrackingMigrationState.serializer.deserialize(in, NodeVersion.CURRENT_METADATA_VERSION);

        assertEquals(original.lastModified, deserialized.lastModified);
        assertEquals(original.keyspaceInfo.size(), deserialized.keyspaceInfo.size());
        for (String ks : original.keyspaceInfo.keySet())
        {
            KeyspaceMigrationInfo origInfo = original.keyspaceInfo.get(ks);
            KeyspaceMigrationInfo deserInfo = deserialized.keyspaceInfo.get(ks);
            assertNotNull(deserInfo);
            assertEquals(origInfo, deserInfo);
        }
    }

    @Test
    public void testWithLastModified()
    {
        Epoch epoch1 = Epoch.create(1);
        Epoch epoch2 = Epoch.create(2);

        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);

        MutationTrackingMigrationState updated = state.withLastModified(epoch2);

        assertNotSame(state, updated);

        assertEquals(epoch1, state.lastModified);
        assertEquals(epoch2, updated.lastModified);
    }

    @Test
    public void testMultipleKeyspaces()
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY;
        Epoch epoch = Epoch.create(1);
        TableId table2Id = TableId.generate();

        // Start migrations for multiple keyspaces
        state = state.withKeyspaceMigrating("ks1", Collections.singletonList(testTableId), epoch);
        state = state.withKeyspaceMigrating("ks2", Collections.singletonList(table2Id), epoch);

        assertEquals(2, state.keyspaceInfo.size());

        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        KeyspaceMigrationInfo expectedKs1 = createExpectedKeyspaceMigrationInfo(
            "ks1",
            testTableId,
            Collections.singleton(fullRing),
            epoch
        );
        KeyspaceMigrationInfo expectedKs2 = createExpectedKeyspaceMigrationInfo(
            "ks2",
            table2Id,
            Collections.singleton(fullRing),
            epoch
        );

        assertEquals(expectedKs1, state.getKeyspaceInfo("ks1"));
        assertEquals(expectedKs2, state.getKeyspaceInfo("ks2"));

        // Complete one keyspace
        state = state.withRangesRepairedForTable("ks1", testTableId, Collections.singleton(fullRing), epoch);

        assertEquals(1, state.keyspaceInfo.size());
        assertNull(state.getKeyspaceInfo("ks1"));

        // ks2 should still have full ring pending
        KeyspaceMigrationInfo expectedKs2AfterKs1Complete = createExpectedKeyspaceMigrationInfo(
            "ks2",
            table2Id,
            Collections.singleton(fullRing),
            epoch
        );

        assertEquals(expectedKs2AfterKs1Complete, state.getKeyspaceInfo("ks2"));
    }

    private KeyspaceMigrationInfo createExpectedKeyspaceMigrationInfo(
        String keyspace,
        TableId tableId,
        Collection<Range<Token>> ranges,
        Epoch startedAtEpoch)
    {
        Map<TableId, NormalizedRanges<Token>> pendingRanges =
            Collections.singletonMap(tableId, NormalizedRanges.normalizedRanges(ranges));
        return new KeyspaceMigrationInfo(keyspace, pendingRanges, startedAtEpoch);
    }

    private List<Range<Token>> createTestRanges()
    {
        Token t1 = partitioner.getTokenFactory().fromString("100");
        Token t2 = partitioner.getTokenFactory().fromString("200");
        Token t3 = partitioner.getTokenFactory().fromString("300");

        return Arrays.asList(
            new Range<>(t1, t2),
            new Range<>(t2, t3)
        );
    }
}
