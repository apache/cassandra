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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

public class MutationTrackingForwardedWriteDownReplicaTest extends TestBaseImpl
{
    private static final String KEYSPACE = "fwd_down_ks";
    private static final String TABLE = "tbl";
    private static final int RF = 3;
    private static final int ROWS = 60;

    @Test
    public void forwardedWritesWithDownReplicaReconcileAfterRestart() throws Throwable
    {
        try (Cluster cluster = Cluster.build(4)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': " + RF + "} " +
                                 "AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int PRIMARY KEY, v int)");

            AtomicInteger forwarded = new AtomicInteger();
            cluster.filters().verbs(Verb.MT_FORWARD_WRITE_REQ.id).messagesMatching((from, to, msg) -> {
                forwarded.incrementAndGet();
                return false;
            }).drop();

            ClusterUtils.stopUnchecked(cluster.get(4));
            for (int i = 1; i <= 3; i++)
                ClusterUtils.awaitRingStatus(cluster.get(i), cluster.get(4), "Down");

            for (int k = 0; k < ROWS; k++)
                cluster.coordinator((k % 3) + 1)
                       .execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (k, v) VALUES (?, ?)", ConsistencyLevel.QUORUM, k, k);

            Assert.assertTrue("expected some writes to be forwarded", forwarded.get() > 0);

            cluster.get(4).startup();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            int reconciled = 0;
            int unreconciled = -1;
            while (System.nanoTime() < deadline)
            {
                reconciled = 0;
                unreconciled = 0;
                for (IInvokableInstance instance : cluster)
                {
                    reconciled += instance.callOnInstance(() -> summary().reconciledIds());
                    unreconciled += instance.callOnInstance(() -> summary().unreconciledIds());
                }
                if (unreconciled == 0 && reconciled == ROWS * RF)
                    break;
                Thread.sleep(1000);
            }

            Assert.assertEquals(0, unreconciled);
            Assert.assertEquals(ROWS * RF, reconciled);

            Object[][] rows = cluster.get(4).executeInternal("SELECT k FROM " + KEYSPACE + '.' + TABLE);
            Assert.assertTrue("node 4 should have received missed mutations", rows.length > 0);
        }
    }

    private static MutationSummary summary()
    {
        Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
        Range<Token> fullRange = new Range<>(token, token);
        TableId tableId = Schema.instance.getTableMetadata(KEYSPACE, TABLE).id;
        return MutationTrackingService.instance().createSummaryForRange(fullRange, tableId, true);
    }
}
