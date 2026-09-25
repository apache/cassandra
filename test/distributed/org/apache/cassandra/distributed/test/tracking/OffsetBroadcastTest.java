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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
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
        try (Cluster cluster = disableBackgroundReconciler(Cluster.build(3)
                                                                  .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP)))
                                                                  .start())
        {

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            for (int i = 1; i <= cluster.size(); ++i)
                cluster.get(i).runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting());

            for (int i = 1; i <= cluster.size(); ++i)
            {
                cluster.get(i).runOnInstance(() -> {
                    TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                    DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                    MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(dk, table.id, false);
                    MutationSummary.CoordinatorSummary coordinatorSummary = summary.get(getOnlyLogId(summary));
                    Assert.assertEquals(1, coordinatorSummary.reconciled.offsetCount());
                    Assert.assertEquals(0, coordinatorSummary.unreconciled.offsetCount());
                });
            }
        }
    }

    /**
     * Offsets name the keyspace they cover, so a broadcast collected before its sender learned of a DROP KEYSPACE
     * arrives at a node for which that keyspace is already gone. Handling it has to discard those offsets rather
     * than look the keyspace up in metadata that no longer contains it, which throws out of the single threaded
     * MISC stage {@link Verb#MT_BROADCAST_LOG_OFFSETS} is processed on and takes the stage down with it.
     */
    @Test(timeout = 300_000)
    public void testBroadcastOffsetsForDroppedKeyspace() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK).with(Feature.GOSSIP))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 2} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // gives node2 offsets to include in the broadcasts it makes on its own schedule
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.ALL);

            IInvokableInstance node1 = cluster.get(1);
            long mark = node1.logs().mark();

            // The inbound sink runs on the stage the verb will be handled on, so a matcher blocking here holds a
            // broadcast node1 has received but not yet handled; permitting it hands it to the handler on this thread.
            CountDownLatch received = new CountDownLatch(1);
            CountDownLatch keyspaceDropped = new CountDownLatch(1);
            cluster.filters().inbound().verbs(Verb.MT_BROADCAST_LOG_OFFSETS.id).from(2).to(1).messagesMatching((from, to, message) -> {
                received.countDown();
                Uninterruptibles.awaitUninterruptibly(keyspaceDropped);
                return false;
            }).drop();

            Assert.assertTrue("node2 never broadcast offsets to node1", received.await(1, TimeUnit.MINUTES));
            cluster.schemaChange("DROP KEYSPACE " + KEYSPACE);
            keyspaceDropped.countDown();

            // MISC is single threaded and is running the held broadcast, so a task queued behind it cannot run until
            // that broadcast has been handled and anything escaping the handler reported by its exception handler.
            boolean drained = node1.callOnInstance(() -> {
                CountDownLatch handled = new CountDownLatch(1);
                Stage.MISC.execute(handled::countDown);
                return Uninterruptibles.awaitUninterruptibly(handled, 1, TimeUnit.MINUTES);
            });
            Assert.assertTrue("node1's MISC stage never drained", drained);

            List<String> died = node1.logs().grep(mark, "Exception in thread.*MiscStage").getResult();
            Assert.assertTrue("offsets for a dropped keyspace killed the stage they arrived on: " + died, died.isEmpty());
        }
    }
}
