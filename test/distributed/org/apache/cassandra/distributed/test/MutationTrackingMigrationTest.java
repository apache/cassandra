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
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.io.sstable.SSTableProvenance;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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
import static org.junit.Assert.assertNotEquals;
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

    private static void assertNoMixedSSTable(String keyspace, String table)
    {
        for (int nodeId = 1; nodeId <= NUM_NODES; nodeId++)
        {
            SHARED_CLUSTER.get(nodeId).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                for (SSTableReader sstable : cfs.getLiveSSTables())
                {
                    assertNotEquals("an unrepaired sstable carries both journal offsets and a commit log span, so it "
                                    + "mixes two logs: " + sstable + ' ' + sstable.getSSTableMetadata().commitLogIntervals,
                                    SSTableProvenance.BOTH, SSTableProvenance.of(sstable));
                }
            });
        }
    }

    /**
     * Counts, on node 1, how many of a table's sstables came out of each log.
     *
     * @return the journal-derived count, then the commit-log-derived count
     */
    private static int[] countByProvenance(String keyspace, String table)
    {
        return SHARED_CLUSTER.get(1).callOnInstance(() -> {
            int journalDerived = 0;
            int commitLogDerived = 0;
            ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                switch (SSTableProvenance.of(sstable))
                {
                    case MUTATION_JOURNAL:
                        journalDerived++;
                        break;
                    case COMMIT_LOG:
                        commitLogDerived++;
                        break;
                    default:
                        break;
                }
            }
            return new int[]{ journalDerived, commitLogDerived };
        });
    }

    private static void flushEverywhere(String keyspace, String table)
    {
        for (int nodeId = 1; nodeId <= NUM_NODES; nodeId++)
            SHARED_CLUSTER.get(nodeId).nodetoolResult("flush", keyspace, table).asserts().success();
    }

    private static void insert(String keyspace, String table, int from, int to, String tag)
    {
        for (int i = from; i < to; i++)
            coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (%d, '%s_%d')",
                                       keyspace, table, i, tag, i),
                                ConsistencyLevel.QUORUM);
    }

    private static void createKeyspaceWithTable(String keyspace, String replicationType)
    {
        coordinator.execute(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                                   + "'replication_factor': 3} AND replication_type='%s'", keyspace, replicationType),
                            ConsistencyLevel.ALL);
        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", keyspace, TEST_TABLE),
                            ConsistencyLevel.ALL);
        waitForEpochOf(SHARED_CLUSTER, 1);
    }

    private static void alterReplicationType(String keyspace, String replicationType)
    {
        coordinator.execute(format("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', "
                                   + "'replication_factor': 3} AND replication_type='%s'", keyspace, replicationType),
                            ConsistencyLevel.ALL);
        waitForEpochOf(SHARED_CLUSTER, 1);
    }

    /**
     * Check sstable provenance info correctness across migration to and from tracked replication
     */
    @Test
    public void sstableProvenanceCorrectnessAcrossMigrationAndReversal() throws Exception
    {
        String testKeyspace = "migration_bounds_test";

        createKeyspaceWithTable(testKeyspace, "untracked");

        // Untracked: everything is commit-log-derived.
        insert(testKeyspace, TEST_TABLE, 0, 50, "untracked");
        flushEverywhere(testKeyspace, TEST_TABLE);
        assertNoMixedSSTable(testKeyspace, TEST_TABLE);
        assertEquals("an untracked table produces no journal-derived sstable",
                     0, countByProvenance(testKeyspace, TEST_TABLE)[0]);

        alterReplicationType(testKeyspace, "tracked");
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        insert(testKeyspace, TEST_TABLE, 50, 100, "migrating");
        flushEverywhere(testKeyspace, TEST_TABLE);
        assertNoMixedSSTable(testKeyspace, TEST_TABLE);

        // Repair only the primary range, so some ranges complete and some stay pending. From here the two logs take
        // writes for the same table.
        SHARED_CLUSTER.get(1).nodetoolResult("repair", "-pr", testKeyspace, TEST_TABLE).asserts().success();
        waitForEpochOf(SHARED_CLUSTER, 1);
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        insert(testKeyspace, TEST_TABLE, 100, 200, "partial");
        flushEverywhere(testKeyspace, TEST_TABLE);
        assertNoMixedSSTable(testKeyspace, TEST_TABLE);
        int[] partial = countByProvenance(testKeyspace, TEST_TABLE);
        assertTrue("a partially migrated table should have taken journal writes; counts were "
                   + partial[0] + " journal-derived, " + partial[1] + " commit-log-derived",
                   partial[0] > 0);

        // Repeated flush rounds while both logs are in use, which is the steady state a long migration sits in.
        int rowsPerRound = 40;
        int rounds = 10;
        for (int round = 0; round < rounds; round++)
        {
            insert(testKeyspace, TEST_TABLE, 200 + round * rowsPerRound, 200 + (round + 1) * rowsPerRound,
                   "round" + round);
            flushEverywhere(testKeyspace, TEST_TABLE);
            assertNoMixedSSTable(testKeyspace, TEST_TABLE);
        }

        int[] steadyState = countByProvenance(testKeyspace, TEST_TABLE);
        assertTrue("the table should still hold sstables from both logs; counts were "
                   + steadyState[0] + " journal-derived, " + steadyState[1] + " commit-log-derived",
                   steadyState[0] > 0 && steadyState[1] > 0);

        // tracked -> untracked is instant, so routing goes back to the commit log with no pending window.
        int afterRounds = 200 + rowsPerRound * rounds;
        alterReplicationType(testKeyspace, "untracked");
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        insert(testKeyspace, TEST_TABLE, afterRounds, afterRounds + 50, "reversed");
        flushEverywhere(testKeyspace, TEST_TABLE);
        assertNoMixedSSTable(testKeyspace, TEST_TABLE);

        Object[][] results = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                ConsistencyLevel.QUORUM);
        assertEquals("no write is lost across the migration, the steady state and the reversal",
                     afterRounds + 50, results.length);
    }

    @Test
    public void testUntrackedToTrackedMigration() throws Exception
    {
        String testKeyspace = "untracked_to_tracked_test";

        // untracked keyspace
        createKeyspaceWithTable(testKeyspace, "untracked");

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        long journalEntriesBefore = countJournalEntries();

        insert(testKeyspace, TEST_TABLE, 0, 100, "initial");

        // no journal entries written while untracked
        long journalEntriesAfterUntracked = countJournalEntries();
        assertEquals(journalEntriesBefore, journalEntriesAfterUntracked);

        Object[][] initialResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                       ConsistencyLevel.QUORUM);
        assertEquals(100, initialResults.length);

        // start migration to tracked replication
        alterReplicationType(testKeyspace, "tracked");

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        long journalEntriesBeforeMigrationWrites = countJournalEntries();

        insert(testKeyspace, TEST_TABLE, 100, 200, "migration");

        // writes should be tracked during migration
        long journalEntriesAfterMigrationWrites = countJournalEntries();
        assertTrue(journalEntriesAfterMigrationWrites > journalEntriesBeforeMigrationWrites);

        // complete migration
        long logMark = SHARED_CLUSTER.get(1).logs().mark();
        SHARED_CLUSTER.get(1).nodetoolResult("repair", testKeyspace, TEST_TABLE).asserts().success();

        waitForEpochOf(SHARED_CLUSTER, 1);

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.TRACKED);

        // Completion is logged once, and the contributing repair names itself and its ranges
        List<String> completionLines = SHARED_CLUSTER.get(1).logs()
                                                     .watchFor(logMark, Duration.ofMinutes(1), "Mutation tracking migration completed for keyspace " + testKeyspace)
                                                     .getResult();
        assertEquals(1, completionLines.size());

        List<String> advancementLines = SHARED_CLUSTER.get(1).logs()
                                                     .grep(logMark, "advanced mutation tracking migration of " + testKeyspace + '.' + TEST_TABLE)
                                                     .getResult();
        assertFalse(advancementLines.isEmpty());
        String advancement = advancementLines.get(0);
        assertTrue(advancement, advancement.contains("parent session"));
        assertTrue(advancement, advancement.contains("range(s) remain to be repaired"));
        assertTrue(advancement, advancement.contains("range(s) already repaired"));

        long journalEntriesBeforeTracked = countJournalEntries();

        insert(testKeyspace, TEST_TABLE, 200, 210, "tracked");

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
        createKeyspaceWithTable(testKeyspace, "tracked");

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.TRACKED);

        long journalEntriesBefore = countJournalEntries();

        insert(testKeyspace, TEST_TABLE, 0, 100, "initial");

        // writes should be tracked before migration
        long journalEntriesAfterTracked = countJournalEntries();
        assertTrue(journalEntriesAfterTracked > journalEntriesBefore);

        Object[][] initialResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                       ConsistencyLevel.QUORUM);
        assertEquals(100, initialResults.length);

        // switch to untracked replication - tracked→untracked is instant, no migration needed
        alterReplicationType(testKeyspace, "untracked");

        // Should go directly to UNTRACKED - no migration state
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.UNTRACKED);

        long journalEntriesBeforeUntracked = countJournalEntries();

        insert(testKeyspace, TEST_TABLE, 100, 210, "untracked");

        // writes should not be tracked after instant switch to untracked
        long journalEntriesAfterUntracked = countJournalEntries();
        assertEquals("Post-switch untracked writes should NOT create journal entries",
                     journalEntriesBeforeUntracked, journalEntriesAfterUntracked);

        Object[][] finalResults = coordinator.execute(format("SELECT * FROM %s.%s", testKeyspace, TEST_TABLE),
                                                     ConsistencyLevel.QUORUM);
        assertEquals(210, finalResults.length);

        Object[][] initialRecord = coordinator.execute(format("SELECT value FROM %s.%s WHERE pk = 50", testKeyspace, TEST_TABLE),
                                                      ConsistencyLevel.QUORUM);
        assertEquals("initial_50", initialRecord[0][0]);
    }

    @Test
    public void testMigrationReversal() throws Exception
    {
        String testKeyspace = "migration_reversal_test";

        // untracked keyspace
        createKeyspaceWithTable(testKeyspace, "untracked");

        insert(testKeyspace, TEST_TABLE, 0, 50, "initial");

        // Start migration to tracked
        alterReplicationType(testKeyspace, "tracked");

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        insert(testKeyspace, TEST_TABLE, 50, 100, "migrating");

        // only repair the primary range so the migration isn't complete and we have something to reverse
        SHARED_CLUSTER.get(1).nodetoolResult("repair", "-pr", testKeyspace, TEST_TABLE).asserts().success();
        waitForEpochOf(SHARED_CLUSTER, 1);
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // Reverse the migration by changing back to untracked - tracked→untracked is instant
        alterReplicationType(testKeyspace, "untracked");

        // Should go directly to UNTRACKED - no migration state
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
        createKeyspaceWithTable(testKeyspace, "untracked");

        // Start migration to tracked
        alterReplicationType(testKeyspace, "tracked");

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // add a new table during migration and write to it
        coordinator.execute(format("CREATE TABLE %s.%s (pk int PRIMARY KEY, value text)", testKeyspace, newTable),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (1, 'new_table_data')", testKeyspace, newTable),
                          ConsistencyLevel.QUORUM);

        // Reverse the migration - tracked→untracked is instant
        alterReplicationType(testKeyspace, "untracked");

        // Should go directly to UNTRACKED - no migration state
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
        alterReplicationType(testKeyspace, "tracked");

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
        createKeyspaceWithTable(testKeyspace, "untracked");

        coordinator.execute(format("INSERT INTO %s.%s (pk, value) VALUES (1, 'test_data')", testKeyspace, TEST_TABLE),
                          ConsistencyLevel.QUORUM);

        // Start migration to tracked
        alterReplicationType(testKeyspace, "tracked");

        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.MIGRATING_TO_TRACKED);

        // Drop the entire keyspace
        coordinator.execute(format("DROP KEYSPACE %s", testKeyspace),
                          ConsistencyLevel.ALL);

        waitForEpochOf(SHARED_CLUSTER, 1);

        // Verify migration state completely removed
        verifyKeyspaceState(testKeyspace, ExpectedKeyspaceState.DROPPED);
    }
}
