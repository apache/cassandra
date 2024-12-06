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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;

public class MutationTrackingTest extends TestBaseImpl
{
    @Test
    public void test() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='logged';"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                Assert.assertEquals(ReplicationType.logged, keyspace.params.replicationType);
            });

            // test keyspace inheritance
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));
            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                Assert.assertEquals(TableParams.TableReplicationType.keyspace, table.params.replicationType);
                Assert.assertTrue(table.hasLoggedReplication());
            });

            // test table override
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl2 (k int primary key, v int) WITH replication_type='legacy';"));
            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl2");
                Assert.assertEquals(TableParams.TableReplicationType.legacy, table.params.replicationType);
                Assert.assertFalse(table.hasLoggedReplication());
            });

        }
    }
}
