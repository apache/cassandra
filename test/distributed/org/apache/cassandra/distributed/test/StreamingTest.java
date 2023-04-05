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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessage;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.streaming.StreamSession.State.PREPARING;
import static org.apache.cassandra.streaming.StreamSession.State.STREAMING;
import static org.apache.cassandra.streaming.StreamSession.State.WAIT_COMPLETE;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_ACK;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_SYN;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_SYNACK;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.RECEIVED;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.STREAM;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.STREAM_INIT;

public class StreamingTest extends TestBaseImpl
{

    private void testStreaming(int nodes, int replicationFactor, int rowCount, String compactionStrategy) throws Throwable
    {
        try (Cluster cluster = builder().withNodes(nodes)
                                        .withDataDirCount(1) // this test expects there to only be a single sstable to stream (with ddirs = 3, we get 3 sstables)
                                        .withConfig(config -> config.with(NETWORK)).start())
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
            registerSink(cluster, nodes);

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
        }
    }

    @Test
    public void test() throws Throwable
    {
        testStreaming(2, 2, 1000, "LeveledCompactionStrategy");
    }

    public static void registerSink(Cluster cluster, int initiatorNodeId)
    {
        IInvokableInstance initiatorNode = cluster.get(initiatorNodeId);
        InetSocketAddress initiator = initiatorNode.broadcastAddress();
        MessageStateSinkImpl initiatorSink = new MessageStateSinkImpl();

        for (int node = 1; node <= cluster.size(); node++)
        {
            if (initiatorNodeId == node)
                continue;

            IInvokableInstance followerNode = cluster.get(node);
            InetSocketAddress follower = followerNode.broadcastAddress();

            // verify on initiator's stream session
            initiatorSink.messages(follower, Arrays.asList(PREPARE_SYNACK, STREAM, StreamMessage.Type.COMPLETE));
            initiatorSink.states(follower, Arrays.asList(PREPARING, STREAMING, WAIT_COMPLETE, StreamSession.State.COMPLETE));

            // verify on follower's stream session
            MessageStateSinkImpl followerSink = new MessageStateSinkImpl();
            followerSink.messages(initiator, Arrays.asList(STREAM_INIT, PREPARE_SYN, PREPARE_ACK, RECEIVED));
            // why 2 completes?  There is a race condition bug with sending COMPLETE where the socket gets closed
            // by the initator, which then triggers a ClosedChannelException, which then checks the current state (PREPARING)
            // to solve this, COMPLETE is set before sending the message, and reset when closing the stream
            followerSink.states(initiator,  Arrays.asList(PREPARING, STREAMING, StreamSession.State.COMPLETE, StreamSession.State.COMPLETE));
            followerNode.runOnInstance(() -> StreamSession.sink = followerSink);
        }

        cluster.get(initiatorNodeId).runOnInstance(() -> StreamSession.sink = initiatorSink);
    }

    @VisibleForTesting
    public static class MessageStateSinkImpl implements StreamSession.MessageStateSink, Serializable
    {
        // use enum ordinal instead of enum to walk around inter-jvm class loader issue, only classes defined in
        // InstanceClassLoader#sharedClassNames are shareable between server jvm and test jvm
        public final Map<InetAddress, Queue<Integer>> messageSink = new ConcurrentHashMap<>();
        public final Map<InetAddress, Queue<Integer>> stateTransitions = new ConcurrentHashMap<>();

        public void messages(InetSocketAddress peer, List<StreamMessage.Type> messages)
        {
            messageSink.put(peer.getAddress(), messages.stream().map(Enum::ordinal).collect(Collectors.toCollection(LinkedList::new)));
        }

        public void states(InetSocketAddress peer, List<StreamSession.State> states)
        {
            stateTransitions.put(peer.getAddress(), states.stream().map(Enum::ordinal).collect(Collectors.toCollection(LinkedList::new)));
        }

        @Override
        public void recordState(InetAddressAndPort from, StreamSession.State state)
        {
            Queue<Integer> states = stateTransitions.get(from.getAddress());
            if (states.peek() == null)
                Assert.fail("Unexpected state " + state);

            int expected = states.poll();
            Assert.assertEquals(StreamSession.State.values()[expected], state);
        }

        @Override
        public void recordMessage(InetAddressAndPort from, StreamMessage.Type message)
        {
            if (message == StreamMessage.Type.KEEP_ALIVE)
                return;

            Queue<Integer> messages = messageSink.get(from.getAddress());
            if (messages.peek() == null)
                Assert.fail("Unexpected message " + message);

            int expected = messages.poll();
            Assert.assertEquals(StreamMessage.Type.values()[expected], message);
        }

        @Override
        public void onClose(InetAddressAndPort from)
        {
            Queue<Integer> states = stateTransitions.get(from.getAddress());
            Assert.assertTrue("Missing states: " + states, states.isEmpty());

            Queue<Integer> messages = messageSink.get(from.getAddress());
            Assert.assertTrue("Missing messages: " + messages, messages.isEmpty());
        }
    }
}
