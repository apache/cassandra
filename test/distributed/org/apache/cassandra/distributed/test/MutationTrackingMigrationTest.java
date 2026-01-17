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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for mutation tracking migration between tracked and untracked replication types.
 */
public class MutationTrackingMigrationTest extends TestBaseImpl
{
    private static final String TEST_TABLE = "tbl";
    private static final int NUM_NODES = 3;

    private static Cluster SHARED_CLUSTER;
    private static ICoordinator coordinator;

    private enum ExpectedKeyspaceState
    {
        UNTRACKED,
        MIGRATING_TO_TRACKED,
        MIGRATING_TO_UNTRACKED,
        TRACKED,
        DROPPED
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        ServerTestUtils.daemonInitialization();
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);

        SHARED_CLUSTER = init(Cluster.build(NUM_NODES)
                                     .withConfig(config -> config.with(Feature.NETWORK)
                                                                 .with(Feature.GOSSIP))
                                     .start());

        coordinator = SHARED_CLUSTER.coordinator(1);
    }

    /**
     * Wait for all nodes to catch up to the epoch of the given node
     */
    private static void waitForEpochOf(Cluster cluster, int node)
    {
        long epoch = cluster.get(node).callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());

        for (int nodeId = 1; nodeId <= NUM_NODES; nodeId++)
        {
            cluster.get(nodeId).runOnInstance(() -> {
                try
                {
                    ClusterMetadataService.instance().awaitAtLeast(Epoch.create(epoch));
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static int countJournalEntries()
    {
        return SHARED_CLUSTER.get(1).callOnInstance(() -> {
            try
            {
                int[] count = new int[1];

                MutationJournal.instance().snapshot().readAll((segment, position, key, buffer, version) -> {
                    count[0]++;
                });

                return count[0];
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to count journal entries", e);
            }
        });
    }

    /**
     * Verify migration state on all nodes matches expected state.
     */
    private void verifyKeyspaceState(String keyspace, ExpectedKeyspaceState expectedState) throws Exception
    {
        for (int nodeId = 1; nodeId <= NUM_NODES; nodeId++)
        {
            SHARED_CLUSTER.get(nodeId).runOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                KeyspaceMetadata ksm = expectedState != ExpectedKeyspaceState.DROPPED ? metadata.schema.getKeyspaceMetadata(keyspace) : null;
                MutationTrackingMigrationState migrationState = metadata.mutationTrackingMigrationState;
                boolean migrating = migrationState.isMigrating(keyspace);

                switch (expectedState)
                {
                    case UNTRACKED:
                        assertTrue(!ksm.params.replicationType.isTracked());
                        assertFalse(migrating);
                        break;

                    case MIGRATING_TO_TRACKED:
                        assertTrue(ksm.params.replicationType.isTracked());
                        assertTrue(migrating);
                        break;

                    case MIGRATING_TO_UNTRACKED:
                        assertTrue(!ksm.params.replicationType.isTracked());
                        assertTrue(migrating);
                        break;

                    case TRACKED:
                        assertTrue(ksm.params.replicationType.isTracked());
                        assertFalse(migrating);
                        break;
                    case DROPPED:
                        assertFalse(migrating);
                        break;
                    default:
                        throw new AssertionError("Unexpected state: " + expectedState);

                }
            });
        }
    }

    @Test
    public void testUntrackedToTrackedMigration() throws Exception
    {
        String testKeyspace = "untracked_to_tracked_test";

        // untracked keyspace
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);


        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        long journalEntriesBefore = countJournalEntries();

        for (int i = 0; i < 100; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'initial_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // no journal entries written while untracked
        long journalEntriesAfterUntracked = countJournalEntries();
        assertEquals(journalEntriesBefore, journalEntriesAfterUntracked);

        Object[][] initialResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                       ConsistencyLevel.QUORUM);
        assertEquals(100, initialResults.length);

        // start migration to tracked replication
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        long journalEntriesBeforeMigrationWrites = countJournalEntries();

        for (int i = 100; i < 200; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'migration_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // writes should be tracked during migration
        long journalEntriesAfterMigrationWrites = countJournalEntries();
        assertTrue(journalEntriesAfterMigrationWrites > journalEntriesBeforeMigrationWrites);

        // complete migration
        SHARED_CLUSTER.get(1).nodetoolResult("repair", testKeyspace, TEST_TABLE).asserts().success();

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.TRACKED);

        long journalEntriesBeforeTracked = countJournalEntries();

        for (int i = 200; i < 210; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'tracked_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // writes should also be tracked after migration
        long journalEntriesAfterTracked = countJournalEntries();
        assertTrue(journalEntriesAfterTracked > journalEntriesBeforeTracked);

        Object[][] finalResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                     ConsistencyLevel.QUORUM);
        assertEquals(210, finalResults.length);

        Object[][] initialRecord = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 50", testKeyspace, TEST_TABLE),
                                                      ConsistencyLevel.QUORUM);
        assertEquals("initial_50", initialRecord[0][0]);

        Object[][] migrationRecord = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 150", testKeyspace, TEST_TABLE),
                                                        ConsistencyLevel.QUORUM);
        assertEquals("migration_150", migrationRecord[0][0]);
    }

    @Test
    public void testTrackedToUntrackedMigration() throws Exception
    {
        String testKeyspace = "tracked_to_untracked_test";

        // tracked keyspace
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.TRACKED);

        long journalEntriesBefore = countJournalEntries();

        for (int i = 0; i < 100; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'initial_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // writes should be tracked before migration
        long journalEntriesAfterTracked = countJournalEntries();
        assertTrue(journalEntriesAfterTracked > journalEntriesBefore);

        Object[][] initialResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                       ConsistencyLevel.QUORUM);
        assertEquals(100, initialResults.length);

        // start migration to untracked replication
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_UNTRACKED);

        long journalEntriesBeforeMigrationWrites = countJournalEntries();

        // Write more data during migration
        for (int i = 100; i < 200; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'migration_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // writes should also be tracked during migration
        long journalEntriesAfterMigrationWrites = countJournalEntries();
        assertTrue("Migration writes should still create journal entries (tracked mechanism still active)",
                   journalEntriesAfterMigrationWrites > journalEntriesBeforeMigrationWrites);

        // complete migration
        SHARED_CLUSTER.get(1).nodetoolResult("repair", testKeyspace, TEST_TABLE).asserts().success();

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        long journalEntriesBeforeUntracked = countJournalEntries();

        for (int i = 200; i < 210; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'untracked_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // but they should not be tracked after migration
        long journalEntriesAfterUntracked = countJournalEntries();
        assertEquals("Post-migration untracked writes should NOT create journal entries",
                     journalEntriesBeforeUntracked, journalEntriesAfterUntracked);

        Object[][] finalResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                     ConsistencyLevel.QUORUM);
        assertEquals(210, finalResults.length);

        Object[][] initialRecord = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 50", testKeyspace, TEST_TABLE),
                                                      ConsistencyLevel.QUORUM);
        assertEquals("initial_50", initialRecord[0][0]);

        Object[][] migrationRecord = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 150", testKeyspace, TEST_TABLE),
                                                        ConsistencyLevel.QUORUM);
        assertEquals("migration_150", migrationRecord[0][0]);
    }

    @Test
    public void testMigrationReversal() throws Exception
    {
        String testKeyspace = "migration_reversal_test";

        // untracked keyspace
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        for (int i = 0; i < 50; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'initial_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // Start migration to tracked
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        for (int i = 50; i < 100; i++)
        {
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, 'migrating_%d')",
                                     testKeyspace, TEST_TABLE, i, i),
                              ConsistencyLevel.QUORUM);
        }

        // only repair the primary range so the migration isn't complete and we have something to reverse
        SHARED_CLUSTER.get(1).nodetoolResult("repair", "-pr", testKeyspace, TEST_TABLE).asserts().success();
        waitForEpochOf(SHARED_CLUSTER, 1);
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // Reverse the migration by changing back to untracked
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_UNTRACKED);

        // Complete the reversed migration
        SHARED_CLUSTER.get(1).nodetoolResult("repair", "-pr", testKeyspace, TEST_TABLE).asserts().success();

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        Object[][] results = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                ConsistencyLevel.QUORUM);
        assertEquals(100, results.length);
    }

    /**
     * Test table added during migration then reversed:
     * 1. Start untracked → tracked migration
     * 2. Create new table during migration
     * 3. ALTER back to untracked (reverses migration)
     * 4. Verify new table included in reversed migration
     */
    @Test
    public void testTableAddedDuringMigrationThenReversed() throws Exception
    {
        String testKeyspace = "table_added_reversal_test";
        String newTable = "tbl2";

        // untracked keyspace
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        // Start migration to tracked
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // add a new table during migration and write to it
        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, newTable),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (1, 'new_table_data')", testKeyspace, newTable),
                          ConsistencyLevel.QUORUM);

        // Reverse the migration
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_UNTRACKED);

        // Complete migration (both tables should be in migration)
        SHARED_CLUSTER.get(1).nodetoolResult("repair", testKeyspace).asserts().success();

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        Object[][] results = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 1", testKeyspace, newTable),
                                                ConsistencyLevel.QUORUM);
        assertEquals("New table data should be readable after reversal", "new_table_data", results[0][0]);
    }

    /**
     * Test table dropped during migration:
     * 1. Start untracked → tracked migration
     * 2. Drop one of the tables
     * 3. Verify dropped table removed from migration state
     * 4. Complete migration for remaining tables
     */
    @Test
    public void testTableDroppedDuringMigration() throws Exception
    {
        String testKeyspace = "table_dropped_test";
        String droppedTable = "tbl_to_drop";

        // untracked keyspace
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.ALL);
        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, droppedTable),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (1, 'keep_this')", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.QUORUM);
        coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (1, 'drop_this')", testKeyspace, droppedTable),
                          ConsistencyLevel.QUORUM);

        // Start migration to tracked
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // drop single table during migration
        coordinator.execute(format("DROP TABLE %s.%s", testKeyspace, droppedTable),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        // Migration should still be in progress (remaining table not yet repaired)
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // Complete migration for remaining table
        SHARED_CLUSTER.get(1).nodetoolResult("repair", testKeyspace, TEST_TABLE).asserts().success();

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.TRACKED);

        Object[][] results = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 1", testKeyspace, TEST_TABLE),
                                                ConsistencyLevel.QUORUM);
        assertEquals("Remaining table data should be readable", "keep_this", results[0][0]);
    }

    /**
     * Test keyspace dropped during migration:
     * 1. Start untracked → tracked migration
     * 2. Drop the entire keyspace
     * 3. Verify migration state completely removed
     */
    @Test
    public void testKeyspaceDroppedDuringMigration() throws Exception
    {
        String testKeyspace = "keyspace_dropped_test";

        // untracked keyspace
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (1, 'test_data')", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.QUORUM);

        // Start migration to tracked
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'",
                                 testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // Drop the entire keyspace
        coordinator.execute(format("DROP KEYSPACE %s", testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        // Verify migration state completely removed
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.DROPPED);
    }
}
