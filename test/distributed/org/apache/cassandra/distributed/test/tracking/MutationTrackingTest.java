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

import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.simple.SimpleMutationSummary;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.*;

public class MutationTrackingTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingTest.class);

    @Test
    public void testBasicWritePath() throws Throwable
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

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                Assert.assertEquals(ReplicationType.logged, keyspace.params.replicationType);
            });

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            MutationId id = decodeId(cluster.get(1).callOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                SimpleMutationSummary summary = (SimpleMutationSummary) MutationTrackingService.instance().summaryForKey(table.id, dk);
                return encodeId(Iterables.getOnlyElement(summary.ids.get(dk)));
            }));

            logger.info(">>> Mutation id: {}", id);
        }
    }

    @Test
    public void testHintsNotWrittenOnFailedWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {

            String keyspaceName = KEYSPACE;
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='logged';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // block messages to node 3
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            UUID node3HostId = cluster.get(3).callOnInstance(() -> StorageService.instance.getLocalHostUUID());
            long hints = cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount());

            // confirm no hints for node 3
            cluster.get(1).runOnInstance(() -> Assert.assertEquals(0, HintsService.instance.getTotalHintsSize(node3HostId)));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            // wait for write timeout
            Thread.sleep(5000);

            // TODO: confirm hints aren't written
            cluster.get(1).runOnInstance(() -> {
                Assert.assertEquals(hints, StorageMetrics.totalHints.getCount());
            });
        }
    }

    /**
     * Finds the highest epoch in the cluster and waits for all nodes to be on it
     */
    private static void awaitHighEpoch(Cluster cluster)
    {
        long max = Epoch.EMPTY.getEpoch();
        for (IInvokableInstance instance : cluster)
        {
            long epoch = instance.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
            max = Math.max(max, epoch);
        }

        long maxEpoch = max;
        for (IInvokableInstance instance : cluster)
        {
            instance.runOnInstance(() -> {
                try
                {
                    ClusterMetadataService.instance().awaitAtLeast(Epoch.create(maxEpoch));
                }
                catch (InterruptedException | TimeoutException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }


        return;

    }
}
