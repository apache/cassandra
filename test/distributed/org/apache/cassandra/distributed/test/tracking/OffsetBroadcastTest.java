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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;

public class OffsetBroadcastTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(OffsetBroadcastTest.class);

    @Test
    public void testBroadcastOffsets() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            for (int i = 1; i <= cluster.size(); ++i)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance.broadcastOffsetsForTesting());

            for (int i = 1; i <= cluster.size(); ++i)
            {
                cluster.get(i).runOnInstance(() -> {
                    TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                    DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                    MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(dk, table.id, false);
                    MutationSummary.CoordinatorSummary coordinatorSummary = summary.get(getOnlyLogId(summary));
                    Assert.assertEquals(1, coordinatorSummary.reconciled.offsetCount());
                    Assert.assertEquals(0, coordinatorSummary.unreconciled.offsetCount());
                });
            }
        }
    }
}
