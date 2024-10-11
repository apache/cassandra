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

package org.apache.cassandra.distributed.upgrade;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.metrics.DefaultNameFactory;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Test read repair on rows with TTLs when the cluster nodes are on different versions.
 */
public class MixedModeReadRepairTTLTest extends UpgradeTestBase
{
    public static final String BLOCKING_REPAIR_METRIC_NAME =
        new DefaultNameFactory("ReadRepair").createMetricName("RepairedBlocking").getMetricName();

    @Test
    public void mixedModeReadRepairRowTTL() throws Throwable
    {
        Object[] row1 = row(0, 1, 10);

        allUpgrades(2, 1)
        .setup(cluster -> cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c))")))
        .runBeforeClusterUpgrade(cluster -> {
            // Insert a row with a TTL, so digests will contain a non-empty local deletion time:
            String insert = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?) USING TTL 86000");
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            // Ensure that read-repair is not triggered with nodes on different versions:
            String query = withKeyspace("SELECT * FROM %s.t WHERE k = 0");
            long before = cluster.get(node).metrics().getCounter(BLOCKING_REPAIR_METRIC_NAME);
            assertRows(cluster.coordinator(node).execute(query, ConsistencyLevel.ALL), row1);
            long after = cluster.get(node).metrics().getCounter(BLOCKING_REPAIR_METRIC_NAME);
            assertEquals(before, after);
        })
        .run();
    }
}
