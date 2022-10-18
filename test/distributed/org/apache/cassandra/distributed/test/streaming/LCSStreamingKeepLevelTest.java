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

package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertTrue;

public class LCSStreamingKeepLevelTest extends TestBaseImpl
{
    @Test
    public void testDecom() throws IOException
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .withoutVNodes()
                                        .withDataDirCount(1)
                                        .start())
        {
            populate(cluster);

            cluster.get(4).nodetoolResult("decommission").asserts().success();

            assertEmptyL0(cluster);
        }
    }

    @Test
    public void testMove() throws IOException
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .withoutVNodes()
                                        .withDataDirCount(1)
                                        .start())
        {
            populate(cluster);

            long tokenVal = ((Murmur3Partitioner.LongToken)cluster.tokens().get(3).getToken()).token;
            long prevTokenVal = ((Murmur3Partitioner.LongToken)cluster.tokens().get(2).getToken()).token;
            // move node 4 to the middle point between its current position and the previous node
            long newToken = (tokenVal + prevTokenVal) / 2;
            cluster.get(4).nodetoolResult("move", String.valueOf(newToken)).asserts().success();

            assertEmptyL0(cluster);
        }
    }

    private static void populate(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.decom_test (id int PRIMARY KEY, value int) with compaction = { 'class':'LeveledCompactionStrategy', 'enabled':'false' }"));

        for (int i = 0; i < 500; i++)
        {
            cluster.coordinator(1).execute(withKeyspace("insert into %s.decom_test (id, value) VALUES (?, ?)"), ConsistencyLevel.ALL, i, i);
            if (i % 100 == 0)
                cluster.forEach((inst) -> inst.flush(KEYSPACE));
        }
        cluster.forEach((i) -> i.flush(KEYSPACE));
        relevel(cluster);
    }

    private static void relevel(Cluster cluster)
    {
        for (IInvokableInstance i : cluster)
        {
            i.runOnInstance(() -> {
                Set<SSTableReader> sstables = Keyspace.open(KEYSPACE).getColumnFamilyStore("decom_test").getLiveSSTables();
                int lvl = 1;
                for (SSTableReader sstable : sstables)
                {
                    try
                    {
                        sstable.mutateLevelAndReload(lvl++);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        assertEmptyL0(cluster);
    }

    private static void assertEmptyL0(Cluster cluster)
    {
        for (IInvokableInstance i : cluster)
        {
            i.runOnInstance(() -> {
                for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore("decom_test").getLiveSSTables())
                    assertTrue(sstable.getSSTableLevel() > 0);
            });
        }
    }
}
