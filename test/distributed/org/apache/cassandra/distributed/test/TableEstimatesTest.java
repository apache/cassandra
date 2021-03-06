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

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.QueryResult;
import org.assertj.core.api.Assertions;

public class TableEstimatesTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(1).start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    /**
     * Replaces Python Dtest: nodetool_test.py#test_refresh_size_estimates_clears_invalid_entries
     */
    @Test
    public void refreshTableEstimatesClearsInvalidEntries()
    {
        String table_estimatesInsert = "INSERT INTO system.table_estimates (keyspace_name, table_name, range_type, range_start, range_end, mean_partition_size, partitions_count) VALUES (?, ?, ?, ?, ?, ?, ?)";
        IInstance node = CLUSTER.get(1);

        try
        {
            node.executeInternal(table_estimatesInsert, "system_auth", "bad_table", "local_primary", "-5", "5", 0L, 0L);
            node.executeInternal(table_estimatesInsert, "bad_keyspace", "bad_table", "local_primary", "-5", "5", 0L, 0L);
        }
        catch (Exception e)
        {
            // to make this test portable (with the intent to extract out), handle the case where the table_estimates isn't defined
            Assertions.assertThat(e.getClass().getCanonicalName()).isEqualTo("org.apache.cassandra.exceptions.InvalidRequestException");
            Assertions.assertThat(e).hasMessageContaining("does not exist");
            Assume.assumeTrue("system.table_estimates not present", false);
        }

        node.nodetoolResult("refreshsizeestimates").asserts().success();

        QueryResult qr = CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system.table_estimates WHERE keyspace_name=? AND table_name=?", ConsistencyLevel.ONE, "system_auth", "bad_table");
        Assertions.assertThat(qr).isExhausted();

        qr = CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system.table_estimates WHERE keyspace_name=?", ConsistencyLevel.ONE, "bad_keyspace");
        Assertions.assertThat(qr).isExhausted();
    }

    /**
     * Replaces Python Dtest: nodetool_test.py#test_refresh_size_estimates_clears_invalid_entries
     */
    @Test
    public void refreshSizeEstimatesClearsInvalidEntries()
    {
        String size_estimatesInsert = "INSERT INTO system.size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES (?, ?, ?, ?, ?, ?)";
        IInstance node = CLUSTER.get(1);

        node.executeInternal(size_estimatesInsert, "system_auth", "bad_table", "-5", "5", 0L, 0L);
        node.executeInternal(size_estimatesInsert, "bad_keyspace", "bad_table", "-5", "5", 0L, 0L);

        node.nodetoolResult("refreshsizeestimates").asserts().success();

        QueryResult qr = CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system.size_estimates WHERE keyspace_name=? AND table_name=?", ConsistencyLevel.ONE, "system_auth", "bad_table");
        Assertions.assertThat(qr).isExhausted();

        qr = CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system.size_estimates WHERE keyspace_name=?", ConsistencyLevel.ONE, "bad_keyspace");
        Assertions.assertThat(qr).isExhausted();
    }
}
