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
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInstanceInitializer;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.replication.ActivationRequest.Phase.COMMIT;

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
}
