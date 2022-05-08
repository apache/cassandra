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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.MessageFilters;
import org.apache.cassandra.hints.HintMessage;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MessageFiltersTest extends TestBaseImpl
{
    @Test
    public void simpleInboundFiltersTest()
    {
        simpleFiltersTest(true);
    }

    @Test
    public void simpleOutboundFiltersTest()
    {
        simpleFiltersTest(false);
    }

    private interface Permit
    {
        boolean test(int from, int to, IMessage msg);
    }

    private static void simpleFiltersTest(boolean inbound)
    {
        int VERB1 = Verb.READ_REQ.id;
        int VERB2 = Verb.READ_RSP.id;
        int VERB3 = Verb.READ_REPAIR_REQ.id;
        int i1 = 1;
        int i2 = 2;
        int i3 = 3;
        String MSG1 = "msg1";
        String MSG2 = "msg2";

        MessageFilters filters = new MessageFilters();
        Permit permit = inbound ? filters::permitInbound : filters::permitOutbound;

        IMessageFilters.Filter filter = filters.allVerbs().inbound(inbound).from(1).drop();
        Assert.assertFalse(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertFalse(permit.test(i1, i2, msg(VERB2, MSG1)));
        Assert.assertFalse(permit.test(i1, i2, msg(VERB3, MSG1)));
        Assert.assertTrue(permit.test(i2, i1, msg(VERB1, MSG1)));
        filter.off();
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        filters.reset();

        filters.verbs(VERB1).inbound(inbound).from(1).to(2).drop();
        Assert.assertFalse(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i2, msg(VERB2, MSG1)));
        Assert.assertTrue(permit.test(i2, i1, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i2, i3, msg(VERB2, MSG1)));

        filters.reset();
        AtomicInteger counter = new AtomicInteger();
        filters.verbs(VERB1).inbound(inbound).from(1).to(2).messagesMatching((from, to, msg) -> {
            counter.incrementAndGet();
            return Arrays.equals(msg.bytes(), MSG1.getBytes());
        }).drop();
        Assert.assertFalse(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertEquals(counter.get(), 1);
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG2)));
        Assert.assertEquals(counter.get(), 2);

        // filter chain gets interrupted because a higher level filter returns no match
        Assert.assertTrue(permit.test(i2, i1, msg(VERB1, MSG1)));
        Assert.assertEquals(counter.get(), 2);
        Assert.assertTrue(permit.test(i2, i1, msg(VERB2, MSG1)));
        Assert.assertEquals(counter.get(), 2);
        filters.reset();

        filters.allVerbs().inbound(inbound).from(3, 2).to(2, 1).drop();
        Assert.assertFalse(permit.test(i3, i1, msg(VERB1, MSG1)));
        Assert.assertFalse(permit.test(i3, i2, msg(VERB1, MSG1)));
        Assert.assertFalse(permit.test(i2, i1, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i2, i3, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i3, msg(VERB1, MSG1)));
        filters.reset();

        counter.set(0);
        filters.allVerbs().inbound(inbound).from(1).to(2).messagesMatching((from, to, msg) -> {
            counter.incrementAndGet();
            return false;
        }).drop();
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i3, msg(VERB1, MSG1)));
        Assert.assertTrue(permit.test(i1, i2, msg(VERB1, MSG1)));
        Assert.assertEquals(2, counter.get());
    }

    private static IMessage msg(int verb, String msg)
    {
        return new IMessage()
        {
            public int verb() { return verb; }
            public byte[] bytes() { return msg.getBytes(); }
            public int id() { return 0; }
            public int version() { return 0;  }
            public InetSocketAddress from() { return null; }
            public int fromPort()
            {
                return 0;
            }
        };
    }

    @Test
    public void testFilters() throws Throwable
    {
        String read = "SELECT * FROM " + KEYSPACE + ".tbl";
        String write = "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)";

        try (ICluster cluster = builder().withNodes(2).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // Reads and writes are going to time out in both directions
            cluster.filters().allVerbs().from(1).to(2).drop();
            for (int i : new int[]{ 1, 2 })
                assertTimeOut(() -> cluster.coordinator(i).execute(read, ConsistencyLevel.ALL));
            for (int i : new int[]{ 1, 2 })
                assertTimeOut(() -> cluster.coordinator(i).execute(write, ConsistencyLevel.ALL));

            cluster.filters().reset();
            // Reads are going to timeout only when 1 serves as a coordinator
            cluster.filters().verbs(Verb.RANGE_REQ.id).from(1).to(2).drop();
            assertTimeOut(() -> cluster.coordinator(1).execute(read, ConsistencyLevel.ALL));
            cluster.coordinator(2).execute(read, ConsistencyLevel.ALL);

            // Writes work in both directions
            for (int i : new int[]{ 1, 2 })
                cluster.coordinator(i).execute(write, ConsistencyLevel.ALL);
        }
    }

    @Test
    public void testMessageMatching() throws Throwable
    {
        String read = "SELECT * FROM " + KEYSPACE + ".tbl";
        String write = "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)";

        try (ICluster<IInvokableInstance> cluster = builder().withNodes(2).withConfig(c -> c.set("range_request_timeout", "2000ms")).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            AtomicInteger counter = new AtomicInteger();

            Set<Integer> verbs = Sets.newHashSet(Arrays.asList(Verb.RANGE_REQ.id,
                                                               Verb.RANGE_RSP.id,
                                                               Verb.MUTATION_REQ.id,
                                                               Verb.MUTATION_RSP.id));

            for (boolean inbound : Arrays.asList(true, false))
            {
                counter.set(0);
                // Reads and writes are going to time out in both directions
                IMessageFilters.Filter filter = cluster.filters()
                                                       .allVerbs()
                                                       .inbound(inbound)
                                                       .from(1)
                                                       .to(2)
                                                       .messagesMatching((from, to, msg) -> {
                                                           // Decode and verify message on instance; return the result back here
                                                           Integer id = cluster.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<Integer>) () -> {
                                                               Message decoded = Instance.deserializeMessage(msg);
                                                               return (Integer) decoded.verb().id;
                                                           }).call();
                                                           Assert.assertTrue(verbs.contains(id));
                                                           counter.incrementAndGet();
                                                           return false;
                                                       }).drop();

                for (int i : new int[]{ 1, 2 })
                    cluster.coordinator(i).execute(read, ConsistencyLevel.ALL);
                for (int i : new int[]{ 1, 2 })
                    cluster.coordinator(i).execute(write, ConsistencyLevel.ALL);

                filter.off();
                Assert.assertEquals(4, counter.get());
            }
        }
    }

    @Test
    public void outboundBeforeInbound() throws Throwable
    {
        try (Cluster cluster = Cluster.create(2))
        {
            InetAddressAndPort other = InetAddressAndPort.getByAddressOverrideDefaults(cluster.get(2).broadcastAddress().getAddress(),
                                                                                       cluster.get(2).broadcastAddress().getPort());
            CountDownLatch waitForIt = new CountDownLatch(1);
            Set<Integer> outboundMessagesSeen = new HashSet<>();
            Set<Integer> inboundMessagesSeen = new HashSet<>();
            AtomicBoolean outboundAfterInbound = new AtomicBoolean(false);
            cluster.filters().outbound().verbs(Verb.ECHO_REQ.id, Verb.ECHO_RSP.id).messagesMatching((from, to, msg) -> {
                outboundMessagesSeen.add(msg.verb());
                if (inboundMessagesSeen.contains(msg.verb()))
                    outboundAfterInbound.set(true);
                return false;
            }).drop(); // drop is confusing since I am not dropping, im just listening...
            cluster.filters().inbound().verbs(Verb.ECHO_REQ.id, Verb.ECHO_RSP.id).messagesMatching((from, to, msg) -> {
                inboundMessagesSeen.add(msg.verb());
                return false;
            }).drop(); // drop is confusing since I am not dropping, im just listening...
            cluster.filters().inbound().verbs(Verb.ECHO_RSP.id).messagesMatching((from, to, msg) -> {
                waitForIt.countDown();
                return false;
            }).drop(); // drop is confusing since I am not dropping, im just listening...
            cluster.get(1).runOnInstance(() -> {
                MessagingService.instance().send(Message.out(Verb.ECHO_REQ, NoPayload.noPayload), other);
            });

            waitForIt.await();

            Assert.assertEquals(outboundMessagesSeen, inboundMessagesSeen);
            // since both are equal, only need to confirm the size of one
            Assert.assertEquals(2, outboundMessagesSeen.size());
            Assert.assertFalse("outbound message saw after inbound", outboundAfterInbound.get());
        }
    }

    @Test
    public void hintSerializationTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(config -> config.with(GOSSIP)
                                                                         .with(NETWORK)
                                                                         .set("hinted_handoff_enabled", true))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)"));
            executeWithWriteFailure(cluster,
                                    withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1,1)"),
                                    ConsistencyLevel.QUORUM,
                                    1);
            CountDownLatch latch = new CountDownLatch(1);
            cluster.filters().verbs(Verb.HINT_REQ.id).messagesMatching((a,b,msg) -> {
                cluster.get(1).acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<IMessage>) (m) -> {
                    HintMessage hintMessage = (HintMessage) Instance.deserializeMessage(m).payload;
                    assert hintMessage != null;
                }).accept(msg);

                latch.countDown();
                return false;
            }).drop().on();
            cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl"));
            latch.await();
        }
    }

    public Object[][] executeWithWriteFailure(Cluster cluster, String statement, ConsistencyLevel cl, int coordinator, Object... bindings)
    {
        IMessageFilters filters = cluster.filters();

        // Drop exactly one coordinated message
        filters.verbs(Verb.MUTATION_REQ.id).from(coordinator).messagesMatching(new IMessageFilters.Matcher()
        {
            private final AtomicBoolean issued = new AtomicBoolean();

            public boolean matches(int from, int to, IMessage message)
            {
                if (from != coordinator || message.verb() != Verb.MUTATION_REQ.id)
                    return false;

                return !issued.getAndSet(true);
            }
        }).drop().on();
        Object[][] res = cluster
                         .coordinator(coordinator)
                         .execute(statement, cl, bindings);
        filters.reset();
        return res;
    }

    private static void assertTimeOut(Runnable r)
    {
        try
        {
            r.run();
            Assert.fail("Should have timed out");
        }
        catch (Throwable t)
        {
            if (!t.toString().contains("TimeoutException"))
                throw t;
            // ignore
        }
    }
}
