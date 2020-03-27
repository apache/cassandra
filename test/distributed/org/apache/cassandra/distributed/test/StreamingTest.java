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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessage;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.streaming.StreamSession.State.*;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.*;

public class StreamingTest extends TestBaseImpl
{

    private void testStreaming(int nodes, int replicationFactor, int rowCount, String compactionStrategy) throws Throwable
    {
        try (Cluster cluster = (Cluster) builder().withNodes(nodes).withConfig(config -> config.with(NETWORK)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'true'}", KEYSPACE, compactionStrategy));

            for (int i = 0 ; i < rowCount ; ++i)
            {
                for (int n = 1 ; n < nodes ; ++n)
                    cluster.get(n).executeInternal(String.format("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');", KEYSPACE), Integer.toString(i));
            }

            cluster.get(nodes).executeInternal("TRUNCATE system.available_ranges;");
            {
                Object[][] results = cluster.get(nodes).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(0, results.length);
            }

            // collect message and state
            registerSink(cluster);

            cluster.get(nodes).runOnInstance(() -> StorageService.instance.rebuild(null, KEYSPACE, null, null));
            {
                Object[][] results = cluster.get(nodes).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(1000, results.length);
                Arrays.sort(results, Comparator.comparingInt(a -> Integer.parseInt((String) a[0])));
                for (int i = 0 ; i < results.length ; ++i)
                {
                    Assert.assertEquals(Integer.toString(i), results[i][0]);
                    Assert.assertEquals("value1", results[i][1]);
                    Assert.assertEquals("value2", results[i][2]);
                }
            }

            // verify stream messages and stream state transition
            int initiator = nodes;
            for (int follower = 1; follower < nodes; follower++)
            {
                // verify initiator
                assertMessages(Arrays.asList(PREPARE_SYNACK, STREAM, StreamMessage.Type.COMPLETE), getMessages(initiator, follower, cluster));
                assertStates(Arrays.asList(PREPARING, STREAMING, WAIT_COMPLETE, StreamSession.State.COMPLETE), getStates(initiator, follower, cluster));

                // verify follower
                assertMessages(Arrays.asList(STREAM_INIT, PREPARE_SYN, PREPARE_ACK, RECEIVED, StreamMessage.Type.COMPLETE), getMessages(follower, initiator, cluster));
                assertStates(Arrays.asList(PREPARING, STREAMING, WAIT_COMPLETE, StreamSession.State.COMPLETE), getStates(follower, initiator, cluster));
            }
        }
    }

    @Test
    public void test() throws Throwable
    {
        testStreaming(2, 2, 1000, "LeveledCompactionStrategy");
    }

    public static void registerSink(Cluster cluster)
    {
        for (int i = 1; i <= cluster.size(); i++)
            cluster.get(i).runOnInstance(() -> StreamSession.sink.enable());
    }

    public static Deque<Integer> getMessages(int host, int from, Cluster cluster)
    {
        InetSocketAddress fromAddress = cluster.get(from).broadcastAddress();
        return cluster.get(host).callOnInstance(() -> StreamSession.sink.getMessages(fromAddress.getAddress()));
    }

    public static Deque<Integer> getStates(int host, int from, Cluster cluster)
    {
        InetSocketAddress fromAddress = cluster.get(from).broadcastAddress();
        return cluster.get(host).callOnInstance(() -> StreamSession.sink.getStates(fromAddress.getAddress()));
    }

    public static void assertMessages(List<StreamMessage.Type> expected, Deque<Integer> actual)
    {
        Assert.assertEquals(expected.size(), actual.size());
        Iterator<Integer> actualItr = actual.iterator();
        Iterator<StreamMessage.Type> expectedItr = expected.iterator();

        while (actualItr.hasNext())
        {
            StreamMessage.Type expectedMessage = expectedItr.next();
            Integer actualMessage = actualItr.next();

            Assert.assertEquals(expectedMessage, StreamMessage.Type.values()[actualMessage]);
        }
    }

    public static void assertStates(List<StreamSession.State> expected, Deque<Integer> actual)
    {
        Assert.assertEquals(expected.size(), actual.size());
        Iterator<Integer> actualItr = actual.iterator();
        Iterator<StreamSession.State> expectedItr = expected.iterator();

        while (actualItr.hasNext())
        {
            StreamSession.State expectedState = expectedItr.next();
            Integer actualState = actualItr.next();

            Assert.assertEquals(expectedState, StreamSession.State.values()[actualState]);
        }
    }
}
