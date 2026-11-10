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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.impl.INodeProvisionStrategy;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * CONTAINS / CONTAINS KEY on an unindexed frozen collection must return the same rows whichever consistency level is
 * used, and whether the data is repaired or not. Whether a replica applies the intersection of its restrictions
 * strictly depends on both (see {@code RowFilter#isStrict} and {@code FilterTree}), so this exercises the paths a
 * single node unit test can't.
 */
public class FrozenCollectionContainsConsistencyTest extends TestBaseImpl
{
    private static final ConsistencyLevel[] LEVELS = { ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.ALL };

    @Test
    public void testUnrepairedData() throws IOException
    {
        test(false);
    }

    @Test
    public void testRepairedData() throws IOException
    {
        test(true);
    }

    private static void test(boolean repair) throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withNodeProvisionStrategy(INodeProvisionStrategy.Strategy.OneNetworkInterface)
                                      .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                      .start())
        {
            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, s frozen<set<int>>, m frozen<map<int, int>>, v int)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.tbl(v) USING 'sai'"));

            // only k = 1 has v = 1 and an s / m that matches
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, s, m, v) VALUES (0, {1}, {1: 10}, 1)"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, s, m, v) VALUES (1, {2}, {2: 20}, 1)"), ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, s, m, v) VALUES (2, {2}, {2: 20}, 2)"), ConsistencyLevel.ALL);

            cluster.forEach(instance -> instance.flush(KEYSPACE));
            if (repair)
                cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();

            for (ConsistencyLevel cl : LEVELS)
            {
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT k FROM %s.tbl WHERE v = 1 AND s CONTAINS 2 ALLOW FILTERING"), cl), row(1));
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT k FROM %s.tbl WHERE v = 1 AND m CONTAINS KEY 2 ALLOW FILTERING"), cl), row(1));
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT k FROM %s.tbl WHERE v = 1 AND m CONTAINS 20 ALLOW FILTERING"), cl), row(1));
                assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT k FROM %s.tbl WHERE v = 1 AND s CONTAINS 3 ALLOW FILTERING"), cl));
            }
        }
    }
}
