/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.cassandra.tcm.transformations;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;

import static org.junit.Assert.*;

/**
 * Tests for AdvanceMutationTrackingMigrationRanges transformation.
 */
public class AdvanceMutationTrackingMigrationTest
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
    public void testAdvanceRangesForMigratingKeyspace()
    {
        Epoch epoch1 = Epoch.create(1);

        // Create initial state with migrating keyspace (TO_TRACKED)
        MutationTrackingMigrationState initialState = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);

        ClusterMetadata metadata = new ClusterMetadata(partitioner);
        ClusterMetadata prev = metadata.forceEpoch(epoch1).transformer()
            .with(initialState)
            .build().metadata;

        // Create ranges to mark as completed
        Collection<Range<Token>> completedRanges = createTestRanges();

        // Apply transformation (with TableId)
        AdvanceMutationTrackingMigration transformation =
            new AdvanceMutationTrackingMigration("test_ks", testTableId, completedRanges);

        Transformation.Result result = transformation.execute(prev);

        // Verify success
        assertTrue(result.isSuccess());

        ClusterMetadata updated = result.success().metadata;
        KeyspaceMigrationInfo actual = updated.mutationTrackingMigrationState.getKeyspaceInfo("test_ks");

        Range<Token> fullRing = fullRing();
        Collection<Range<Token>> expectedRemainingRanges = Range.subtract(
            Collections.singleton(fullRing),
            completedRanges
        );

        KeyspaceMigrationInfo expected = createExpectedInfo(
            "test_ks",
            testTableId,
            expectedRemainingRanges,
            epoch1
        );

        assertEquals(expected, actual);

        assertFalse(actual.isComplete());
        assertTrue(updated.mutationTrackingMigrationState.hasMigratingKeyspaces());
    }

    @Test
    public void testAdvanceRangesCompleteMigration()
    {
        Epoch epoch1 = Epoch.create(1);

        // Create initial state with migrating keyspace (TO_TRACKED)
        MutationTrackingMigrationState initialState = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);

        ClusterMetadata metadata = new ClusterMetadata(partitioner);
        ClusterMetadata prev = metadata.forceEpoch(epoch1).transformer()
            .with(initialState)
            .build().metadata;

        // Complete the full ring
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        Collection<Range<Token>> completedRanges = Collections.singleton(fullRing);

        // Apply transformation (with TableId)
        AdvanceMutationTrackingMigration transformation =
            new AdvanceMutationTrackingMigration("test_ks", testTableId, completedRanges);

        Transformation.Result result = transformation.execute(prev);

        // Verify success
        assertTrue(result.isSuccess());

        ClusterMetadata updated = result.success().metadata;

        // Verify migration was auto-completed (keyspace removed from state)
        assertFalse(updated.mutationTrackingMigrationState.hasMigratingKeyspaces());
        assertFalse(updated.mutationTrackingMigrationState.isMigrating("test_ks"));
    }

    @Test
    public void testAdvanceRangesForNonMigratingKeyspace()
    {
        Epoch epoch1 = Epoch.create(1);

        // Create state without any migrating keyspaces
        MutationTrackingMigrationState initialState = MutationTrackingMigrationState.EMPTY;

        ClusterMetadata metadata = new ClusterMetadata(partitioner);
        ClusterMetadata prev = metadata.forceEpoch(epoch1).transformer()
            .with(initialState)
            .build().metadata;

        // Try to advance ranges for non-migrating keyspace
        Collection<Range<Token>> completedRanges = createTestRanges();

        AdvanceMutationTrackingMigration transformation =
            new AdvanceMutationTrackingMigration("test_ks", testTableId, completedRanges);

        Transformation.Result result = transformation.execute(prev);

        // Verify rejection
        assertTrue(result.isRejected());
        assertTrue(result.rejected().reason.contains("not migrating"));
    }

    @Test
    public void testAdvanceRangesForWrongTable()
    {
        Epoch epoch1 = Epoch.create(1);

        MutationTrackingMigrationState initialState = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);

        ClusterMetadata metadata = new ClusterMetadata(partitioner);
        ClusterMetadata prev = metadata.forceEpoch(epoch1).transformer()
            .with(initialState)
            .build().metadata;

        // Try to advance ranges for a table ID not being migrated
        TableId wrongTableId = TableId.generate();
        Collection<Range<Token>> completedRanges = createTestRanges();

        AdvanceMutationTrackingMigration transformation =
            new AdvanceMutationTrackingMigration("test_ks", wrongTableId, completedRanges);

        Transformation.Result result = transformation.execute(prev);

        // confirm noop
        assertTrue(result.isSuccess());
        ClusterMetadata updated = result.success().metadata;

        KeyspaceMigrationInfo expected = createExpectedInfo(
            "test_ks",
            testTableId,
            Collections.singleton(fullRing()),
            epoch1
        );

        assertEquals(expected, updated.mutationTrackingMigrationState.getKeyspaceInfo("test_ks"));
    }

    @Test
    public void testAdvancePartialThenComplete()
    {
        Epoch epoch1 = Epoch.create(1);

        MutationTrackingMigrationState initialState = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Collections.singletonList(testTableId), epoch1);

        ClusterMetadata metadata = new ClusterMetadata(partitioner);
        ClusterMetadata prev = metadata.forceEpoch(epoch1).transformer()
            .with(initialState)
            .build().metadata;

        // advance some ranges
        Collection<Range<Token>> partialRanges = createTestRanges();
        AdvanceMutationTrackingMigration partial = new AdvanceMutationTrackingMigration("test_ks", testTableId, partialRanges);

        Transformation.Result result1 = partial.execute(prev);
        assertTrue(result1.isSuccess());
        ClusterMetadata afterPartial = result1.success().metadata;

        KeyspaceMigrationInfo afterPartialInfo =
            afterPartial.mutationTrackingMigrationState.getKeyspaceInfo("test_ks");
        assertNotNull(afterPartialInfo);
        assertFalse(afterPartialInfo.isComplete());

        // advance the rest
        Range<Token> fullRing = fullRing();
        AdvanceMutationTrackingMigration complete =
            new AdvanceMutationTrackingMigration("test_ks", testTableId,
                                                 Collections.singleton(fullRing));

        Transformation.Result result2 = complete.execute(afterPartial);
        assertTrue(result2.isSuccess());
        ClusterMetadata afterComplete = result2.success().metadata;

        assertFalse(afterComplete.mutationTrackingMigrationState.isMigrating("test_ks"));
        assertFalse(afterComplete.mutationTrackingMigrationState.hasMigratingKeyspaces());
    }

    @Test
    public void testAdvanceMultipleTables()
    {
        Epoch epoch1 = Epoch.create(1);
        TableId table2Id = TableId.generate();

        // 2 tables migrating
        MutationTrackingMigrationState initialState = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Arrays.asList(testTableId, table2Id), epoch1);

        ClusterMetadata metadata = new ClusterMetadata(partitioner);
        ClusterMetadata prev = metadata.forceEpoch(epoch1).transformer()
            .with(initialState)
            .build().metadata;

        // Complete first table
        Range<Token> fullRing = fullRing();
        AdvanceMutationTrackingMigration completeTable1 =
            new AdvanceMutationTrackingMigration("test_ks", testTableId,
                                                 Collections.singleton(fullRing));

        Transformation.Result result1 = completeTable1.execute(prev);
        assertTrue(result1.isSuccess());
        ClusterMetadata afterTable1 = result1.success().metadata;

        KeyspaceMigrationInfo info =
            afterTable1.mutationTrackingMigrationState.getKeyspaceInfo("test_ks");
        assertNotNull(info);
        assertFalse(info.isComplete()); // Keyspace not complete yet
        assertTrue(info.pendingRangesPerTable.containsKey(table2Id));
        assertFalse(info.pendingRangesPerTable.containsKey(testTableId)); // table1 removed

        // Complete second table
        AdvanceMutationTrackingMigration completeTable2 =
            new AdvanceMutationTrackingMigration("test_ks", table2Id,
                                                 Collections.singleton(fullRing));

        Transformation.Result result2 = completeTable2.execute(afterTable1);
        assertTrue(result2.isSuccess());
        ClusterMetadata afterTable2 = result2.success().metadata;

        assertFalse(afterTable2.mutationTrackingMigrationState.isMigrating("test_ks"));
        assertFalse(afterTable2.mutationTrackingMigrationState.hasMigratingKeyspaces());
    }

    /**
     * Helper to create expected KeyspaceMigrationInfo for assertions
     */
    private KeyspaceMigrationInfo createExpectedInfo(String keyspace,
                                                      TableId tableId,
                                                      Collection<Range<Token>> pendingRanges,
                                                      Epoch startedAtEpoch)
    {
        NormalizedRanges<Token> normalized = NormalizedRanges.normalizedRanges(pendingRanges);
        Map<TableId, NormalizedRanges<Token>> pendingRangesPerTable =
            Collections.singletonMap(tableId, normalized);
        return new KeyspaceMigrationInfo(keyspace, pendingRangesPerTable, startedAtEpoch);
    }

    /**
     * Helper to create full ring range
     */
    private Range<Token> fullRing()
    {
        return new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
    }

    private Collection<Range<Token>> createTestRanges()
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
