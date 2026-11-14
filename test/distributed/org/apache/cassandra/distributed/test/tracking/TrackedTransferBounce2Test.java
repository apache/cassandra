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

package org.apache.cassandra.distributed.test.tracking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInstanceInitializer;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.waitForEpochOf;
import static org.apache.cassandra.replication.ActivationRequest.Phase.COMMIT;
import static org.junit.Assert.assertTrue;

public class TrackedTransferBounce2Test extends TrackedTransferTestBase
{
    @Test
    public void testBounceAfterMigrationRepairDoesNotUsePendingDirZeroCopy() throws IOException
    {
        testBounceAfterMigrationRepairDoesNotUsePendingDir(ZCS_CONFIG);
    }

    @Test
    public void testBounceAfterMigrationRepairDoesNotUsePendingDirNonZeroCopy() throws IOException
    {
        testBounceAfterMigrationRepairDoesNotUsePendingDir(NON_ZCS_CONFIG);
    }

    private static void testBounceAfterMigrationRepairDoesNotUsePendingDir(Consumer<IInstanceConfig> config) throws IOException
    {
        String migrationKeyspace = "migration_pending_test";
        try (Cluster cluster = cluster(config))
        {
            cluster.schemaChange("CREATE KEYSPACE " + migrationKeyspace + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'");
            cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(migrationKeyspace) + " (pk BLOB PRIMARY KEY, v INT)");
            waitForEpochOf(cluster, 1);

            cluster.get(2).executeInternal("INSERT INTO " + tableWithKeyspace(migrationKeyspace) + " (pk, v) VALUES (?, 7)", KEY_201);

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201));
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201), row(KEY_201, 7));
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201));

            cluster.schemaChange("ALTER KEYSPACE " + migrationKeyspace + " WITH replication_type='tracked'");
            waitForEpochOf(cluster, 1);

            boolean migrating = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(migrationKeyspace));
            assertTrue("Keyspace should be in themiddle of migration before repair", migrating);

            cluster.get(1).nodetoolResult("repair", "--full", migrationKeyspace).asserts().success();

            // The row is visible locally on node1 immediately after repair
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201), row(KEY_201, 7));

            assertTrue("Migration repair should not stream into the pending directory on node1",
                       getPendingSSTableDirs(cluster.get(1), migrationKeyspace).isEmpty());

            bounce(cluster);

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201), row(KEY_201, 7));
        }
    }

    @Test
    public void testIncrementalRepairDuringMigrationDoesNotUsePendingDirZeroCopy() throws IOException
    {
        testIncrementalRepairDuringMigrationDoesNotUsePendingDir(ZCS_CONFIG);
    }

    @Test
    public void testIncrementalRepairDuringMigrationDoesNotUsePendingDirNonZeroCopy() throws IOException
    {
        testIncrementalRepairDuringMigrationDoesNotUsePendingDir(NON_ZCS_CONFIG);
    }

    private static void testIncrementalRepairDuringMigrationDoesNotUsePendingDir(Consumer<IInstanceConfig> config) throws IOException
    {
        String migrationKeyspace = "migration_incremental_test";
        try (Cluster cluster = cluster(config))
        {
            cluster.schemaChange("CREATE KEYSPACE " + migrationKeyspace + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'");
            cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(migrationKeyspace) + " (pk BLOB PRIMARY KEY, v INT)");
            waitForEpochOf(cluster, 1);

            // Data only on node2, so the repair has to stream it to the other replicas.
            cluster.get(2).executeInternal("INSERT INTO " + tableWithKeyspace(migrationKeyspace) + " (pk, v) VALUES (?, 7)", KEY_201);

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201));
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201), row(KEY_201, 7));
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201));

            cluster.schemaChange("ALTER KEYSPACE " + migrationKeyspace + " WITH replication_type='tracked'");
            waitForEpochOf(cluster, 1);

            boolean migrating = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(migrationKeyspace));
            assertTrue("Keyspace should be migrating before the incremental repair", migrating);

            // trigger an incremental repair session while the keyspace is migrating
            cluster.get(1).nodetoolResult("repair", migrationKeyspace).asserts().success();

            // The row was streamed to node1, and none of the repair output is treated as a tracked transfer
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201), row(KEY_201, 7));
            for (int node = 1; node <= NODES; node++)
                assertTrue("Incremental repair during migration should not stream into the pending directory on node" + node,
                           getPendingSSTableDirs(cluster.get(node), migrationKeyspace).isEmpty());

            bounce(cluster);

            // Data survives the bounce because it was made live
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(migrationKeyspace) + " WHERE pk = ?", KEY_201), row(KEY_201, 7));
        }
    }

    @Test
    public void testBounceAfterSuccessfulTrackedRepairActivationZeroCopy() throws IOException
    {
        testBounceAfterSuccessfulTrackedRepairActivation(ZCS_CONFIG);
    }

    @Test
    public void testBounceAfterSuccessfulTrackedRepairActivationNonZeroCopy() throws IOException
    {
        testBounceAfterSuccessfulTrackedRepairActivation(NON_ZCS_CONFIG);
    }

    private static void testBounceAfterSuccessfulTrackedRepairActivation(Consumer<IInstanceConfig> config) throws IOException
    {
        try (Cluster cluster = cluster(config))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
            cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(KEYSPACE) + " (pk BLOB PRIMARY KEY, v INT)");

            cluster.get(1).executeInternal("INSERT INTO " + tableWithKeyspace(KEYSPACE) + " (pk, v) VALUES (?, 1)", KEY_100);

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100), row(KEY_100, 1));
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));

            // Run repair without SkipActivation (normal activation succeeds on coordinator and followers)
            cluster.get(1).nodetoolResult("repair", "--start-token", SHARD_ALIGNED_RANGE_1.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_1.right.toString(), "--full", "--skip-sync", KEYSPACE).asserts().success();

            for (int node = 1; node <= NODES; node++)
                assertRows(cluster.get(node).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100), row(KEY_100, 1));

            bounce(cluster);

            for (int node = 1; node <= NODES; node++)
            {
                assertRows(cluster.get(node).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100), row(KEY_100, 1));
                assertTrue("Node " + node + " should have cleaned up its pending transfer directory after activation and bounce",
                           getPendingSSTableDirs(cluster.get(node), KEYSPACE).isEmpty());
            }
        }
    }

    @Test
    public void testImportRespectsPendingMigrationRanges() throws IOException
    {
        String migrationKeyspace = "migration_import_test";
        IInstanceInitializer initializer = ByteBuddyInjections.SkipActivation.install(1, 2, 3);
        try (Cluster cluster = cluster(initializer))
        {
            ByteBuddyInjections.SkipActivation.setup(cluster, COMMIT);
            startMigrationAndMigrateFirstShard(cluster, migrationKeyspace);

            // KEY_100 falls inside the now fully-migrated shard, so the import should take the tracked path
            Assertions.assertThatThrownBy(() -> importRows(cluster, migrationKeyspace, false, row(KEY_100, 1)))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Tracked transfer failed during COMMIT");
            assertPendingActivation(cluster, migrationKeyspace);
            // The tracked transfer was never committed, so the data must not have leaked into the live set
            assertRows(select(cluster, migrationKeyspace, KEY_100));

            // KEY_201 falls inside a still-pending shard, so the import should take the untracked path
            List<String> failed = importRows(cluster, migrationKeyspace, false, row(KEY_201, 2));
            Assertions.assertThat(failed).isEmpty();
            assertRows(select(cluster, migrationKeyspace, KEY_201), row(KEY_201, 2));

            // Once activation is no longer skipped, the tracked path imports the data
            ByteBuddyInjections.SkipActivation.setup(cluster, null, false);
            failed = importRows(cluster, migrationKeyspace, false, row(KEY_100, 3));
            Assertions.assertThat(failed).isEmpty();
            assertRows(select(cluster, migrationKeyspace, KEY_100), row(KEY_100, 3));
        }
    }

    @Test
    public void testImportMixedTrackedAndUntrackedDoesNotAddUntrackedOnTrackedFailure() throws IOException
    {
        String migrationKeyspace = "migration_import_mixed_test";
        IInstanceInitializer initializer = ByteBuddyInjections.SkipActivation.install(1, 2, 3);
        try (Cluster cluster = cluster(initializer))
        {
            ByteBuddyInjections.SkipActivation.setup(cluster, COMMIT);
            startMigrationAndMigrateFirstShard(cluster, migrationKeyspace);

            // One import with an SSTable for the migrated shard (tracked) and one for a pending shard (untracked).
            // The tracked transfer fails at COMMIT, and the untracked SSTable is not made live.
            Assertions.assertThatThrownBy(() -> importRows(cluster, migrationKeyspace, true, row(KEY_100, 1), row(KEY_201, 2)))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Tracked transfer failed during COMMIT");
            assertRows(select(cluster, migrationKeyspace, KEY_100));
            assertRows(select(cluster, migrationKeyspace, KEY_201));
        }
    }

    @Test
    public void testImportRejectsSSTableSpanningMigratedAndPendingRanges() throws IOException
    {
        String migrationKeyspace = "migration_import_mixed_ranges_test";
        try (Cluster cluster = cluster())
        {
            startMigrationAndMigrateFirstShard(cluster, migrationKeyspace);

            // A single SSTable with data in both a migrated and a pending shard can be imported neither way (tracked or untracked)
            List<String> failed = importRows(cluster, migrationKeyspace, false, row(KEY_100, 1), row(KEY_201, 2));
            Assertions.assertThat(failed).hasSize(1);
            assertRows(select(cluster, migrationKeyspace, KEY_100));
            assertRows(select(cluster, migrationKeyspace, KEY_201));
        }
    }

    @Test
    public void testImportSSTableSpanningShardsWithoutMigration() throws IOException
    {
        String trackedKeyspace = "tracked_import_multi_shard_test";
        try (Cluster cluster = cluster())
        {
            cluster.schemaChange("CREATE KEYSPACE " + trackedKeyspace + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(trackedKeyspace) + " (pk BLOB PRIMARY KEY, v INT)");
            waitForEpochOf(cluster, 1);

            // KEY_100 and KEY_201 are in different shards, the tracked import splits the SSTable per shard
            List<String> failed = importRows(cluster, trackedKeyspace, false, row(KEY_100, 1), row(KEY_201, 2));
            Assertions.assertThat(failed).isEmpty();
            assertRows(select(cluster, trackedKeyspace, KEY_100), row(KEY_100, 1));
            assertRows(select(cluster, trackedKeyspace, KEY_201), row(KEY_201, 2));
        }
    }

    /**
     * Starts a migration of shard containing KEY_100
     */
    private static void startMigrationAndMigrateFirstShard(Cluster cluster, String keyspace)
    {
        cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = " +
                             "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'");
        cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)");
        waitForEpochOf(cluster, 1);

        cluster.schemaChange("ALTER KEYSPACE " + keyspace + " WITH replication_type='tracked'");
        waitForEpochOf(cluster, 1);

        boolean migrating = cluster.get(1).callOnInstance(() -> ClusterMetadata.current().mutationTrackingMigrationState.isMigrating(keyspace));
        assertTrue("Keyspace should be in the middle of migration before repair", migrating);

        cluster.get(1).nodetoolResult("repair", "--start-token", Long.toString(Long.MIN_VALUE),
                                      "--end-token", "-3074457345618258603", keyspace)
               .asserts().success();
    }

    private static Object[][] select(Cluster cluster, String keyspace, ByteBuffer key)
    {
        return cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(keyspace) + " WHERE pk = ?", key);
    }

    private static List<String> importRows(Cluster cluster, String keyspace, boolean separateSSTables, Object[]... rows) throws IOException
    {
        String file = Files.createTempDirectory(TrackedTransferBounce2Test.class.getSimpleName()).toString();

        List<Object[][]> batches = new ArrayList<>();
        if (separateSSTables)
            for (Object[] row : rows)
                batches.add(new Object[][]{ row });
        else
            batches.add(rows);

        for (Object[][] batch : batches)
        {
            CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder()
                                                               .forTable("CREATE TABLE " + tableWithKeyspace(keyspace) + " (pk BLOB PRIMARY KEY, v INT)")
                                                               .inDirectory(file)
                                                               .using("INSERT INTO " + tableWithKeyspace(keyspace) + " (pk, v) VALUES (?, ?)");

            try (CQLSSTableWriter writer = builder.build())
            {
                for (Object[] row : batch)
                    writer.addRow(row[0], row[1]);
            }
        }

        IInvokableInstance target = cluster.get(1);
        return target.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, TABLE);
            return cfs.importNewSSTables(Set.of(file), true, true, true, true, true, true, true);
        });
    }

    static List<String> getPendingSSTableDirs(IInvokableInstance instance, String keyspace)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(keyspace, TABLE);
            Set<File> pendingLocations = cfs.getDirectories().getPendingLocations();

            List<String> pendingUuidDirs = new ArrayList<>();
            for (File pendingDir : pendingLocations)
            {
                File[] uuidDirs = pendingDir.listUnchecked(File::isDirectory);
                for (File dir : uuidDirs)
                    pendingUuidDirs.add(dir.absolutePath());
            }
            return pendingUuidDirs;
        });
    }
}
