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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.cql3.CQLTester.schemaChange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for AlterSchema auto-starting mutation tracking migration when replication type changes.
 */
public class AlterSchemaMutationTrackingTest
{
    private static final AtomicInteger ksCounter = new AtomicInteger();
    private static Murmur3Partitioner partitioner;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
        MutationJournal.start();
        partitioner = (Murmur3Partitioner) DatabaseDescriptor.getPartitioner();
    }

    private static String nextKsName()
    {
        return "ks" + ksCounter.incrementAndGet();
    }

    @Test
    public void testAutoStartToTrackedMigration() throws Throwable
    {
        String ksName = nextKsName();
        // untracked replication
        schemaChange( "CREATE KEYSPACE " + ksName +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'untracked'"
        );
        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));

        ClusterMetadata metadata = ClusterMetadata.current();
        assertNull(metadata.mutationTrackingMigrationState.getKeyspaceInfo(ksName));

        // Alter tracked replication
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));

        metadata = ClusterMetadata.current();
        TableId tableId = metadata.schema.getKeyspaceMetadata(ksName).getTableOrViewNullable("tbl").id;
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());

        MutationTrackingMigrationState actualState = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo actualInfo = actualState.getKeyspaceInfo(ksName);

        MutationTrackingMigrationState expectedState = createExpectedState(
            actualState.lastModified,
            ksName,
            tableId,
            fullRing,
            actualInfo.startedAtEpoch
        );

        assertStatesEqual(expectedState, actualState, ksName);
    }

    @Test
    public void testAutoStartToUntrackedMigration() throws Throwable
    {
        String ksName = nextKsName();
        // tracked replication
        schemaChange("CREATE KEYSPACE " + ksName +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'tracked'"
        );

        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));

        ClusterMetadata metadata = ClusterMetadata.current();
        assertNull(metadata.mutationTrackingMigrationState.getKeyspaceInfo(ksName));

        // Alter keyspace to untracked
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'untracked'", ksName));

        metadata = ClusterMetadata.current();
        TableId tableId = metadata.schema.getKeyspaceMetadata(ksName).getTableOrViewNullable("tbl").id;
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());

        MutationTrackingMigrationState actualState = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo actualInfo = actualState.getKeyspaceInfo(ksName);

        MutationTrackingMigrationState expectedState = createExpectedState(
            actualState.lastModified,
            ksName,
            tableId,
            fullRing,
            actualInfo.startedAtEpoch
        );

        assertStatesEqual(expectedState, actualState, ksName);
    }

    @Test
    public void testNoMigrationWhenReplicationTypeUnchanged() throws Throwable
    {
        String ksName = nextKsName();
        // untracked replication
        schemaChange("CREATE KEYSPACE " + ksName +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'untracked'"
        );
        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));
        assertNull(ClusterMetadata.current().mutationTrackingMigrationState.getKeyspaceInfo(ksName));

        // Alter keyspace without changing replication type
        schemaChange(String.format(
            "ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}",
            ksName
        ));

        // confirm no migrations were started
        assertNull(ClusterMetadata.current().mutationTrackingMigrationState.getKeyspaceInfo(ksName));
    }

    @Test
    public void testMultipleKeyspaceMigrations() throws Throwable
    {
        String ks1 = nextKsName();
        // untracked replication
        schemaChange("CREATE KEYSPACE " + ks1 +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'untracked'"
        );
        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ks1));

        String ks2 = nextKsName();
        // tracked replication
        schemaChange("CREATE KEYSPACE " + ks2 +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'tracked'"
        );
        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ks2));

        // Alter to tracked
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ks1));

        // Alter to untracked
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'untracked'", ks2));

        ClusterMetadata metadata = ClusterMetadata.current();
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());

        TableId table1Id = metadata.schema.getKeyspaceMetadata(ks1).getTableOrViewNullable("tbl").id;
        TableId table2Id = metadata.schema.getKeyspaceMetadata(ks2).getTableOrViewNullable("tbl").id;

        MutationTrackingMigrationState actualState = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo actual1 = actualState.getKeyspaceInfo(ks1);
        KeyspaceMigrationInfo actual2 = actualState.getKeyspaceInfo(ks2);

        ImmutableMap.Builder<String, KeyspaceMigrationInfo> expectedKeyspaces = ImmutableMap.builder();
        expectedKeyspaces.put(ks1, createKeyspaceMigrationInfo(ks1, table1Id, fullRing, actual1.startedAtEpoch));
        expectedKeyspaces.put(ks2, createKeyspaceMigrationInfo(ks2, table2Id, fullRing, actual2.startedAtEpoch));

        MutationTrackingMigrationState expectedState = new MutationTrackingMigrationState(
            actualState.lastModified,
            expectedKeyspaces.build()
        );

        assertStatesEqual(expectedState, actualState, ks1, ks2);
    }

    @Test
    public void testReverseMigrationDirection() throws Throwable
    {
        String ksName = nextKsName();
        // untracked replication
        schemaChange("CREATE KEYSPACE " + ksName +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'untracked'"
        );

        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));

        ClusterMetadata metadata = ClusterMetadata.current();
        assertNull("Should have no migration before first alter for " + ksName,
                   metadata.mutationTrackingMigrationState.getKeyspaceInfo(ksName));

        // Alter to tracked (untracked → tracked)
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));

        metadata = ClusterMetadata.current();
        TableId tableId = metadata.schema.getKeyspaceMetadata(ksName).getTableOrViewNullable("tbl").id;
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());

        MutationTrackingMigrationState actualAfterFirstAlter = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo actualInfo1 = actualAfterFirstAlter.getKeyspaceInfo(ksName);

        MutationTrackingMigrationState expectedAfterFirstAlter = createExpectedState(
            actualAfterFirstAlter.lastModified,
            ksName,
            tableId,
            fullRing,
            actualInfo1.startedAtEpoch
        );

        assertStatesEqual(expectedAfterFirstAlter, actualAfterFirstAlter, ksName);

        // Alter back to untracked
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'untracked'", ksName));

        // this should auto-complete the migration, since none of the ranges from the initial alter completed migration
        metadata = ClusterMetadata.current();
        assertNull(metadata.mutationTrackingMigrationState.getKeyspaceInfo(ksName));

        // Alter back to tracked again
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));
        metadata = ClusterMetadata.current();

        MutationTrackingMigrationState actualAfterThirdAlter = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo actualInfo3 = actualAfterThirdAlter.getKeyspaceInfo(ksName);

        MutationTrackingMigrationState expectedAfterThirdAlter = createExpectedState(
            actualAfterThirdAlter.lastModified,
            ksName,
            tableId,
            fullRing,
            actualInfo3.startedAtEpoch
        );

        assertStatesEqual(expectedAfterThirdAlter, actualAfterThirdAlter, ksName);
    }

    @Test
    public void testDropKeyspaceDuringMigration() throws Throwable
    {
        String ksName = nextKsName();
        schemaChange("CREATE KEYSPACE " + ksName +
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
            "AND replication_type = 'untracked'"
        );
        schemaChange(String.format("CREATE TABLE %s.tbl (pk int PRIMARY KEY, val int)", ksName));

        // Alter to tracked
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));

        ClusterMetadata metadata = ClusterMetadata.current();
        assertNotNull(metadata.mutationTrackingMigrationState.getKeyspaceInfo(ksName));

        // Drop the keyspace & confirm migration is also removed
        schemaChange(String.format("DROP KEYSPACE %s", ksName));

        ClusterMetadata afterDrop = ClusterMetadata.current();
        assertNull(afterDrop.mutationTrackingMigrationState.getKeyspaceInfo(ksName));
    }

    @Test
    public void testDropTableDuringMigration() throws Throwable
    {
        String ksName = nextKsName();
        schemaChange("CREATE KEYSPACE " + ksName +
                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
                     "AND replication_type = 'untracked'"
        );
        schemaChange(String.format("CREATE TABLE %s.tbl1 (pk int PRIMARY KEY, val int)", ksName));
        schemaChange(String.format("CREATE TABLE %s.tbl2 (pk int PRIMARY KEY, val int)", ksName));

        // Alter to tracked
        schemaChange(String.format("ALTER KEYSPACE %s WITH replication_type = 'tracked'", ksName));

        ClusterMetadata metadata = ClusterMetadata.current();
        TableId table1Id = metadata.schema.getKeyspaceMetadata(ksName).getTableOrViewNullable("tbl1").id;
        TableId table2Id = metadata.schema.getKeyspaceMetadata(ksName).getTableOrViewNullable("tbl2").id;
        Range<Token> fullRing = new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());

        MutationTrackingMigrationState actualWithBothTables = metadata.mutationTrackingMigrationState;
        KeyspaceMigrationInfo actualInfo = actualWithBothTables.getKeyspaceInfo(ksName);

        ImmutableMap.Builder<TableId, NormalizedRanges<Token>> pendingRangesBuilder = ImmutableMap.builder();
        pendingRangesBuilder.put(table1Id, NormalizedRanges.normalizedRanges(Collections.singleton(fullRing)));
        pendingRangesBuilder.put(table2Id, NormalizedRanges.normalizedRanges(Collections.singleton(fullRing)));

        KeyspaceMigrationInfo expectedInfo = new KeyspaceMigrationInfo(
            ksName,
            pendingRangesBuilder.build(),
            actualInfo.startedAtEpoch
        );

        MutationTrackingMigrationState expectedWithBothTables = new MutationTrackingMigrationState(
            actualWithBothTables.lastModified,
            ImmutableMap.of(ksName, expectedInfo)
        );

        assertStatesEqual(expectedWithBothTables, actualWithBothTables, ksName);

        schemaChange(String.format("DROP TABLE %s.tbl2", ksName));

        metadata = ClusterMetadata.current();

        MutationTrackingMigrationState actualWithOnlyTable1 = metadata.mutationTrackingMigrationState;

        MutationTrackingMigrationState expectedWithOnlyTable1 = createExpectedState(
            actualWithOnlyTable1.lastModified,
            ksName,
            table1Id,
            fullRing,
            actualInfo.startedAtEpoch
        );

        assertStatesEqual(expectedWithOnlyTable1, actualWithOnlyTable1, ksName);
    }

    private MutationTrackingMigrationState createExpectedState(Epoch lastModified,
                                                                String keyspace,
                                                                TableId tableId,
                                                                Range<Token> fullRing,
                                                                Epoch startedAtEpoch)
    {
        KeyspaceMigrationInfo info = createKeyspaceMigrationInfo(keyspace, tableId, fullRing, startedAtEpoch);
        return new MutationTrackingMigrationState(lastModified, ImmutableMap.of(keyspace, info));
    }

    private KeyspaceMigrationInfo createKeyspaceMigrationInfo(String keyspace,
                                                               TableId tableId,
                                                               Range<Token> fullRing,
                                                               Epoch startedAtEpoch)
    {
        Map<TableId, NormalizedRanges<Token>> pendingRanges =
            ImmutableMap.of(tableId, NormalizedRanges.normalizedRanges(Collections.singleton(fullRing)));
        return new KeyspaceMigrationInfo(keyspace, pendingRanges, startedAtEpoch);
    }

    /**
     * Assert two MutationTrackingMigrationState objects are equal for the given keyspaces
     */
    private void assertStatesEqual(MutationTrackingMigrationState expected, MutationTrackingMigrationState actual, String... keyspaces)
    {
        assertFalse(keyspaces.length == 0);
        assertEquals(expected.lastModified, actual.lastModified);

        for (String keyspace : keyspaces)
        {
            KeyspaceMigrationInfo expectedInfo = expected.getKeyspaceInfo(keyspace);
            KeyspaceMigrationInfo actualInfo = actual.getKeyspaceInfo(keyspace);
            assertEquals(expectedInfo, actualInfo);
        }
    }
}
