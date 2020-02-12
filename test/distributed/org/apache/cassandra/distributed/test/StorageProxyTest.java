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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageProxy;

//REVIEWER - I don't know how to write this test witout dtest... StorageProxy needs to be called and couldn't find good example tests
public class StorageProxyTest extends DistributedTestBase implements Serializable
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void create() throws IOException
    {
        CLUSTER = init(Cluster.create(1));
    }

    @AfterClass
    public static void teardown()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void mutateUpdatesWriteLatency() throws IOException
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore table = Keyspace.open("system").getColumnFamilyStore("peers");
            TableMetadata tableMeta = table.metadata();

            PartitionUpdate update = PartitionUpdate.emptyUpdate(tableMeta, table.getPartitioner().decorateKey(ByteBuffer.wrap(new byte[0])));

            List<IMutation> mutations = new ArrayList<>();
            mutations.add(new Mutation(update));

            long beforeMutate = table.metric.coordinatorWriteLatency.getCount();

            // this start time is because the test fails if you just use now.  It looks like we now try to scale
            // down the timeout based off how much time has elapsed and this keeps failing; but running in the future
            // solves it!
            long queryStartNanoTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            StorageProxy.mutate(mutations, ConsistencyLevel.ALL, queryStartNanoTime);

            long afterMutate = table.metric.coordinatorWriteLatency.getCount();

            Assert.assertEquals("Should only see one mutation", beforeMutate + 1, afterMutate);
        });
    }

    @Test
    public void mutlipleMutateUpdatesWriteLatencyOnce() throws IOException
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore table = Keyspace.open("system").getColumnFamilyStore("peers");
            TableMetadata tableMeta = table.metadata();

            PartitionUpdate update = PartitionUpdate.emptyUpdate(tableMeta, table.getPartitioner().decorateKey(ByteBuffer.wrap(new byte[0])));

            List<IMutation> mutations = new ArrayList<>();
            for (int i = 0; i < 10; i++)
                mutations.add(new Mutation(update));

            long beforeMutate = table.metric.coordinatorWriteLatency.getCount();

            // this start time is because the test fails if you just use now.  It looks like we now try to scale
            // down the timeout based off how much time has elapsed and this keeps failing; but running in the future
            // solves it!
            long queryStartNanoTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            StorageProxy.mutate(mutations, ConsistencyLevel.ALL, queryStartNanoTime);

            long afterMutate = table.metric.coordinatorWriteLatency.getCount();

            Assert.assertEquals("Should only see one mutation", beforeMutate + 1, afterMutate);
        });
    }

    @Test
    public void mutlipleMutateFailInMiddleDoesNotUpdateUnaffectedTables() throws IOException
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore peers = Keyspace.open("system").getColumnFamilyStore("peers");
            ColumnFamilyStore local = Keyspace.open("system").getColumnFamilyStore("local");
            ColumnFamilyStore paxos = Keyspace.open("system").getColumnFamilyStore("paxos");

            PartitionUpdate peersUpdate = PartitionUpdate.emptyUpdate(peers.metadata(), peers.getPartitioner().decorateKey(ByteBuffer.wrap(new byte[0])));
            PartitionUpdate localUpdate = PartitionUpdate.emptyUpdate(local.metadata(), peers.getPartitioner().decorateKey(ByteBuffer.wrap(new byte[0])));
            PartitionUpdate paxosUpdate = PartitionUpdate.emptyUpdate(paxos.metadata(), peers.getPartitioner().decorateKey(ByteBuffer.wrap(new byte[0])));

            List<IMutation> mutations = new ArrayList<>();
            mutations.add(new Mutation(peersUpdate));
            mutations.add(new Mutation(localUpdate) {
                public void apply()
                {
                    throw new RuntimeException("What happens?");
                }
            }); // this will fail
            mutations.add(new Mutation(paxosUpdate)); // mutations happen concurrently, so even though 'local' fails, this will be seen

            long peersBeforeMutate = peers.metric.coordinatorWriteLatency.getCount();
            long localBeforeMutate = local.metric.coordinatorWriteLatency.getCount();
            long paxosBeforeMutate = paxos.metric.coordinatorWriteLatency.getCount();

            // this start time is because the test fails if you just use now.  It looks like we now try to scale
            // down the timeout based off how much time has elapsed and this keeps failing; but running in the future
            // solves it!
            long queryStartNanoTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            try {
                StorageProxy.mutate(mutations, ConsistencyLevel.ALL, queryStartNanoTime);
                Assert.fail("Mutation should fail");
            } catch (RuntimeException e) {
                // expected
            }

            Assert.assertEquals("Should only see one mutation", peersBeforeMutate + 1, peers.metric.coordinatorWriteLatency.getCount());
            Assert.assertEquals("Should only see one mutation", localBeforeMutate + 1, local.metric.coordinatorWriteLatency.getCount());
            Assert.assertEquals("Should only see one mutation", paxosBeforeMutate + 1, paxos.metric.coordinatorWriteLatency.getCount());
        });
    }
}
