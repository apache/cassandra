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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests the case where a coordinator that is NOT a replica MUST forward
 * the counter write to a replica leader.
 *
 * With 6 nodes and RF=3, we can pick a coordinator that is definitely
 * not a replica for a specific partition.
 */
public class TrackedCounterForwardingTest extends TestBaseImpl
{
    @Test
    public void testForwardedTrackedCounterWrites() throws Throwable
    {
        try (Cluster cluster = Cluster.build(6)
                                      .withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'");

            cluster.schemaChange("CREATE TABLE k.counters (pk int PRIMARY KEY, count counter)");

            ConsistencyLevel cl = ConsistencyLevel.QUORUM;

            // Test counter writes from all nodes
            // Some will be local (coordinator is replica), some will be forwarded (coordinator not replica)
            for (int coordinatorNode = 1; coordinatorNode <= 6; coordinatorNode++)
            {
                ICoordinator coordinator = cluster.coordinator(coordinatorNode);
                int pk = coordinatorNode * 100;

                // Increment
                coordinator.execute("UPDATE k.counters SET count = count + 5 WHERE pk = ?", cl, pk);
                assertRows(coordinator.execute("SELECT count FROM k.counters WHERE pk = ?", cl, pk), row(5L));

                // Increment again
                coordinator.execute("UPDATE k.counters SET count = count + 3 WHERE pk = ?", cl, pk);
                assertRows(coordinator.execute("SELECT count FROM k.counters WHERE pk = ?", cl, pk), row(8L));
            }

            // Verify all nodes can read all counters
            for (int node = 1; node <= 6; node++)
            {
                ICoordinator coordinator = cluster.coordinator(node);
                for (int pk = 100; pk <= 600; pk += 100)
                {
                    assertRows(coordinator.execute("SELECT count FROM k.counters WHERE pk = ?", ConsistencyLevel.ONE, pk),
                               row(8L));
                }
            }
        }
    }
}