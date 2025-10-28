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
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * Distributed tests for MV backfill functionality with SSTable streaming.
 *
 * This test creates a multi-node cluster, populates a base table with data,
 * creates materialized views, and tests the backfill process with streaming
 * to ensure data consistency across the cluster.
 */
public class MVBackfillNodetoolTest extends MVBackfillTestBase
{

    /**
     * Test MV backfill with streaming in a cluster.
     * Creates two MVs: one with same partition key, one with different partition key.
     */
    @Test
    public void testMVBackfillWithPrimaryRangeOnly() throws Exception
    {
        backfillTestHelper(3, 1, 10000, false);
    }

    @Test
    public void testMVBackfillWithPrimaryRangeOnly1() throws Exception
    {
        backfillTestHelper(3, 3, 10000, false);
    }

    @Test
    public void testMVBackfillWithRanges() throws Exception
    {
        backfillTestHelper(3, 1, 10000, true);
    }

    @Test
    public void testMVBackfillWithRanges1() throws Exception
    {
        backfillTestHelper(6, 3, 10000, true);
    }

    private void backfillTestHelper(int nodeCount, int replicationFactor, int rowCount, boolean withSubrange) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(nodeCount)
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(nodeCount, 1))
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
            if (withSubrange)
            {
                performBackfillWithSubRanges(cluster, nodeCount);
            }
            else
            {
                performBackfillWithStreaming(cluster);
            }

            // Step 5: Verify data consistency
            verifyDataConsistency(cluster);

            // Step 6: verify the generated MV SSTables are removed
            verifyMVBackfillFilesRemoved(cluster);
        }
    }

    private void performBackfillWithStreaming(Cluster cluster)
    {
        // Perform backfill on each node for both MVs
        cluster.forEach(instance -> {
            performNodeBackfill(instance, MV_SAME_PK);
            performNodeBackfill(instance, MV_DIFF_PK);
        });
        cluster.forEach(instance -> {
            verifyLocalBackfillStatusPrimaryRangeFinished(instance, MV_SAME_PK);
            verifyLocalBackfillStatusPrimaryRangeFinished(instance, MV_DIFF_PK);
        });
    }

    private void performBackfillWithSubRanges(Cluster cluster, int nodeCount)
    {
        // Perform backfill on each node for both MVs
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(nodeCount, 1);
        String startToken = tokenSupplier.tokens(nodeCount).iterator().next(); // 9223372036854775805
        for (int i = 1; i <= nodeCount; i++)
        {
            String endToken = tokenSupplier.tokens(i).iterator().next();
            String shiftedToken = String.valueOf(Long.parseLong(endToken) - 100000);
            performNodeBackfillWithRange(cluster.get(i), MV_SAME_PK, startToken, shiftedToken);
            performNodeBackfillWithRange(cluster.get(i), MV_DIFF_PK, startToken, shiftedToken);
            performNodeBackfillWithRange(cluster.get(i), MV_SAME_PK, shiftedToken, endToken);
            performNodeBackfillWithRange(cluster.get(i), MV_DIFF_PK, shiftedToken, endToken);
            startToken = endToken;
        }
    }

    private static void performNodeBackfill(IInvokableInstance instance, String viewName)
    {
        NodeToolResult result =  instance.nodetoolResult("mvbackfill", KEYSPACE + "." + viewName);
        result.asserts().success();
    }

    private static void performNodeBackfillWithRange(IInvokableInstance instance, String viewName, String from, String to)
    {
        NodeToolResult result = instance.nodetoolResult("mvbackfill", KEYSPACE + "." + viewName, "-st", from, "-et", to);
        result.asserts().success();
    }

    private static void verifyLocalBackfillStatusPrimaryRangeFinished(IInvokableInstance instance, String viewName)
    {
        NodeToolResult result =  instance.nodetoolResult("checklocalmvbackfillstatus", KEYSPACE + "." + viewName);
        result.asserts().success();
        assertThat(result.getStdout(),containsString("Primary Ranges Finished: true"));
    }
}
