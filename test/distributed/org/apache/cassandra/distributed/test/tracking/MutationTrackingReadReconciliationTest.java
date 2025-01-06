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

import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.assertIdsForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getIdsForKey;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.numLogReconciliations;

public class MutationTrackingReadReconciliationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingReadReconciliationTest.class);

    private static Object[] row(Object... objs)
    {
        return objs;
    }

    private static Object[][] rows(Object[][]... objs)
    {
        return objs;
    }

    /**
     * Test a read reconciliation where the coordinator doesn't have a read response it needs to apply
     * additional mutations to
     */
    @Test
    public void testBasicReadReconciliation() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='logged';", keyspaceName));

            cluster.forEach(node -> {
                logger.info(">>> {}", node);
                node.runOnInstance(() -> {
                    KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    Assert.assertEquals(ReplicationType.logged, ksm.params.replicationType);
                });
            });

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);
            Set<MutationId> firstIds = getIdsForKey(cluster.get(1), keyspaceName, "tbl", 1);
            MutationId firstId = Iterables.getOnlyElement(firstIds);

            cluster.get(2, 3).forEach(node -> {
                assertIdsForKey(node, keyspaceName, tableName, 1, firstIds);
            });

            // block messages to node 3 and perform a write at quorum
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            Set<MutationId> allIds = getIdsForKey(cluster.get(1), keyspaceName, "tbl", 1);
            Assert.assertEquals(2, allIds.size());
            Assert.assertTrue(allIds.contains(firstId));
            MutationId secondId = Iterables.getOnlyElement(Sets.difference(allIds, firstIds));
            Assert.assertNotEquals(secondId, firstId);

            // second node should have the new id, third should not
            assertIdsForKey(cluster.get(2), keyspaceName, tableName, 1, allIds);
            assertIdsForKey(cluster.get(3), keyspaceName, tableName, 1, firstIds);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();


            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = cluster.coordinator(1).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1)), result);

            // check that node3 has the new ids
            assertIdsForKey(cluster.get(3), keyspaceName, tableName, 1, allIds);
        }
    }

    /**
     * Test a read reconciliation where the coordinator needs to receive and apply mutations missing
     * from its data response
     */
    @Test
    public void testReadReconciliationApplyMutations() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_reconciliation_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                        "AND replication_type='logged';", keyspaceName));

            cluster.forEach(node -> {
                logger.info(">>> {}", node);
                node.runOnInstance(() -> {
                    KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    Assert.assertEquals(ReplicationType.logged, ksm.params.replicationType);
                });
            });

            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            // insert a row at all, confirm it's present on all nodes
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 0, 0)", keyspaceName, tableName), ConsistencyLevel.ALL);
            Set<MutationId> firstIds = getIdsForKey(cluster.get(1), keyspaceName, "tbl", 1);
            MutationId firstId = Iterables.getOnlyElement(firstIds);

            cluster.get(2, 3).forEach(node -> {
                assertIdsForKey(node, keyspaceName, tableName, 1, firstIds);
            });

            // block messages to node 3 and perform a write at quorum
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();

            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (1, 1, 1)", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            Set<MutationId> allIds = getIdsForKey(cluster.get(1), keyspaceName, "tbl", 1);
            Assert.assertEquals(2, allIds.size());
            Assert.assertTrue(allIds.contains(firstId));
            MutationId secondId = Iterables.getOnlyElement(Sets.difference(allIds, firstIds));
            Assert.assertNotEquals(secondId, firstId);

            // second node should have the new id, third should not
            assertIdsForKey(cluster.get(2), keyspaceName, tableName, 1, allIds);
            assertIdsForKey(cluster.get(3), keyspaceName, tableName, 1, firstIds);

            // reverse the partition and do a read
            cluster.filters().reset();
            cluster.filters().allVerbs().to(2).drop();
            cluster.filters().allVerbs().from(2).drop();


            Assert.assertEquals(0, numLogReconciliations(cluster.get(1)));
            Object[][] result = cluster.coordinator(3).execute(format("SELECT * FROM %s.%s WHERE k=1", keyspaceName, tableName), ConsistencyLevel.QUORUM);
            Assert.assertEquals(row(row(1, 0, 0), row(1, 1, 1)), result);

            // check that node3 has the new ids
            assertIdsForKey(cluster.get(3), keyspaceName, tableName, 1, allIds);
        }
    }
}
