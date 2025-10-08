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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

/**
 * Tests for MV backfill happy path scenarios with SSTable streaming.
 *
 * This test class focuses on successful MV backfill operations under
 * various cluster configurations and data volumes to ensure the
 * streaming functionality works correctly in normal conditions.
 */
public class MVBackfillHappyPathTest extends MVBackfillTestBase
{
    /**
     * Test MV backfill with streaming in a 3-node cluster with RF=1 and small dataset.
     * Creates two MVs: one with same partition key, one with different partition key.
     */
    @Test
    public void testMVBackfillWithStreaming1() throws Exception
    {
        backfillTestHelper(3, 1, 100);
    }

    /**
     * Test MV backfill with streaming in a 3-node cluster with RF=1 and medium dataset.
     */
    @Test
    public void testMVBackfillWithStreaming2() throws Exception
    {
        backfillTestHelper(3, 1, 10000);
    }

    /**
     * Test MV backfill with streaming in a 6-node cluster with RF=3 and large dataset.
     */
    @Test
    public void testMVBackfillWithStreaming3() throws Exception
    {
        backfillTestHelper(6, 3, 10000);
    }

    /**
     * Test MV backfill with streaming in a 5-node cluster with RF=2 and large dataset.
     */
    @Test
    public void testMVBackfillWithStreaming4() throws Exception
    {
        backfillTestHelper(5, 2, 10000);
    }

    /**
     * Helper method for happy path backfill testing with various cluster configurations.
     * This method sets up a cluster, populates data, creates MVs, performs backfill,
     * and verifies the results.
     */
    private void backfillTestHelper(int nodeCount, int replicationFactor, int rowCount) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(nodeCount)
                                           .withConfig(config -> config.with(Feature.values())
                                                                       .set("materialized_view_auto_backfill_enabled", false)
                                                                       .set("materialized_views_enabled", true))
                                           .start()))
        {
            // Step 1: Create keyspace and base table
            createSchema(cluster, replicationFactor);

            // Step 2: Populate base table with data
            populateBaseTable(cluster, rowCount);

            // Step 3: Create materialized views
            createMaterializedViews(cluster);

            // Step 4: Perform backfill with streaming
            performBackfillWithStreamingSuccessful(cluster);

            // Step 5: Verify data consistency
            verifyDataConsistency(cluster);

            // Step 6: verify the generated MV SSTables are removed
            verifyMVBackfillFilesRemoved(cluster);
        }
    }
}
