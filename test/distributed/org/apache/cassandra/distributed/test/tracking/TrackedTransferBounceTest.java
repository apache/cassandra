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
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.waitForEpochOf;
import static org.apache.cassandra.replication.ActivationRequest.Phase.COMMIT;
import static org.junit.Assert.assertTrue;

public class TrackedTransferBounceTest extends TrackedTransferTestBase
{
    /*
     * When an import fails, bounce must not move the pending SSTables into the live set.
     */
    @Test
    public void testBounceAfterPendingImport() throws Throwable
    {
        IInstanceInitializer initializer = ByteBuddyInjections.SkipActivation.install(1, 2, 3);
        try (Cluster cluster = cluster(initializer))
        {
            ByteBuddyInjections.SkipActivation.setup(cluster, COMMIT);
            createSchema(cluster);

            Assertions.assertThatThrownBy(() -> doImport(cluster))
                      .hasMessageContaining("Failed adding SSTables")
                      .cause()
                      .hasMessageContaining("Tracked transfer failed during COMMIT");

            assertPendingActivation(cluster);
            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));

            bounce(cluster);

            assertPendingActivation(cluster);
            assertLocalSelect(cluster, rows -> assertRows(rows, EMPTY_ROWS));
        }
    }

    @Test
    public void testBounceAfterPendingShardAlignedZeroCopy() throws IOException
    {
        testBounceAfterPendingRepair(ZCS_CONFIG, "repair", "--start-token", SHARD_ALIGNED_RANGE_1.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_1.right.toString(), "--full", KEYSPACE);
    }

    @Test
    public void testBounceAfterPendingAcrossShardsZeroCopy() throws IOException
    {
        testBounceAfterPendingRepair(ZCS_CONFIG, "repair", "--full", KEYSPACE);
    }

    @Test
    public void testBounceAfterPendingShardAlignedNonZeroCopy() throws IOException
    {
        testBounceAfterPendingRepair(NON_ZCS_CONFIG, "repair", "--start-token", SHARD_ALIGNED_RANGE_1.left.toString(), "--end-token", SHARD_ALIGNED_RANGE_1.right.toString(), "--full", KEYSPACE);
    }

    @Test
    public void testBounceAfterPendingAcrossShardsNonZeroCopy() throws IOException
    {
        testBounceAfterPendingRepair(NON_ZCS_CONFIG, "repair", "--full", KEYSPACE);
    }

    /*
     * When a repair fails, bounce must not move the pending SSTables into the live set.
     */
    private static void testBounceAfterPendingRepair(Consumer<IInstanceConfig> config, String... repairCommandAndArgs) throws IOException
    {
        IInstanceInitializer initializer = ByteBuddyInjections.SkipActivation.install(1, 2, 3);

        try (Cluster cluster = cluster(config, initializer))
        {
            // Make sure we fail on commit...
            ByteBuddyInjections.SkipActivation.setup(cluster, COMMIT);

            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
            cluster.schemaChange("CREATE TABLE " + tableWithKeyspace(KEYSPACE) + " (pk BLOB PRIMARY KEY, v INT)");

            cluster.get(1).executeInternal("INSERT INTO " + tableWithKeyspace(KEYSPACE) + " (pk, v) VALUES (?, 1)", KEY_100);

            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100), row(KEY_100, 1));
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));

            cluster.get(1).nodetoolResult(repairCommandAndArgs).asserts().failure().errorContains("Tracked transfer failed during COMMIT");

            assertPendingActivation(cluster);
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100), row(KEY_100, 1));
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));

            bounce(cluster);

            assertPendingActivation(cluster);
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100), row(KEY_100, 1));
            assertRows(cluster.get(2).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + tableWithKeyspace(KEYSPACE) + " WHERE pk = ?", KEY_100));
        }
    }

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

    private static List<String> getPendingSSTableDirs(IInvokableInstance instance, String keyspace)
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
