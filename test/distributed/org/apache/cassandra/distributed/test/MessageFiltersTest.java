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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.impl.MessageFilters;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

public class MessageFiltersTest extends DistributedTestBase
{
    @Test
    public void simpleFiltersTest() throws Throwable
    {
        int VERB1 = MessagingService.Verb.READ.ordinal();
        int VERB2 = MessagingService.Verb.REQUEST_RESPONSE.ordinal();
        int VERB3 = MessagingService.Verb.READ_REPAIR.ordinal();
        int i1 = 1;
        int i2 = 2;
        int i3 = 3;
        String MSG1 = "msg1";
        String MSG2 = "msg2";

        MessageFilters filters = new MessageFilters();
        MessageFilters.Filter filter = filters.allVerbs().from(1).drop();

        Assert.assertFalse(filters.permit(i1, i2, msg(VERB1, MSG1)));
        Assert.assertFalse(filters.permit(i1, i2, msg(VERB2, MSG1)));
        Assert.assertFalse(filters.permit(i1, i2, msg(VERB3, MSG1)));
        Assert.assertTrue(filters.permit(i2, i1, msg(VERB1, MSG1)));
        filter.off();
        Assert.assertTrue(filters.permit(i1, i2, msg(VERB1, MSG1)));
        filters.reset();

        filters.verbs(VERB1).from(1).to(2).drop();
        Assert.assertFalse(filters.permit(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i1, i2, msg(VERB2, MSG1)));
        Assert.assertTrue(filters.permit(i2, i1, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i2, i3, msg(VERB2, MSG1)));

        filters.reset();
        AtomicInteger counter = new AtomicInteger();
        filters.verbs(VERB1).from(1).to(2).messagesMatching((from, to, msg) -> {
            counter.incrementAndGet();
            return Arrays.equals(msg.bytes(), MSG1.getBytes());
        }).drop();
        Assert.assertFalse(filters.permit(i1, i2, msg(VERB1, MSG1)));
        Assert.assertEquals(counter.get(), 1);
        Assert.assertTrue(filters.permit(i1, i2, msg(VERB1, MSG2)));
        Assert.assertEquals(counter.get(), 2);

        // filter chain gets interrupted because a higher level filter returns no match
        Assert.assertTrue(filters.permit(i2, i1, msg(VERB1, MSG1)));
        Assert.assertEquals(counter.get(), 2);
        Assert.assertTrue(filters.permit(i2, i1, msg(VERB2, MSG1)));
        Assert.assertEquals(counter.get(), 2);
        filters.reset();

        filters.allVerbs().from(3, 2).to(2, 1).drop();
        Assert.assertFalse(filters.permit(i3, i1, msg(VERB1, MSG1)));
        Assert.assertFalse(filters.permit(i3, i2, msg(VERB1, MSG1)));
        Assert.assertFalse(filters.permit(i2, i1, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i2, i3, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i1, i3, msg(VERB1, MSG1)));
        filters.reset();

        counter.set(0);
        filters.allVerbs().from(1).to(2).messagesMatching((from, to, msg) -> {
            counter.incrementAndGet();
            return false;
        }).drop();
        Assert.assertTrue(filters.permit(i1, i2, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i1, i3, msg(VERB1, MSG1)));
        Assert.assertTrue(filters.permit(i1, i2, msg(VERB1, MSG1)));
        Assert.assertEquals(2, counter.get());
    }

    IMessage msg(int verb, String msg)
    {
        return new IMessage()
        {
            public int verb() { return verb; }
            public byte[] bytes() { return msg.getBytes(); }
            public int id() { return 0; }
            public int version() { return 0;  }
            public InetAddressAndPort from() { return null; }
        };
    }

    @Test
    public void testFilters() throws Throwable
    {
        String read = "SELECT * FROM " + KEYSPACE + ".tbl";
        String write = "INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)";

        try (Cluster cluster = Cluster.create(2))
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
            cluster.verbs(MessagingService.Verb.RANGE_SLICE).from(1).to(2).drop();
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

        try (Cluster cluster = Cluster.create(2))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + cluster.size() + "};");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            AtomicInteger counter = new AtomicInteger();

            Set<Integer> verbs = new HashSet<>(Arrays.asList(MessagingService.Verb.RANGE_SLICE.ordinal(),
                                                             MessagingService.Verb.MUTATION.ordinal()));

            // Reads and writes are going to time out in both directions
            IMessageFilters.Filter filter = cluster.filters()
                                                   .allVerbs()
                                                   .from(1)
                                                   .to(2)
                                                   .messagesMatching((from, to, msg) -> {
                                                       // Decode and verify message on instance; return the result back here
                                                       Integer id = cluster.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<Integer>) () -> {
                                                           MessageIn decoded = Instance.deserializeMessage(msg);
                                                           if (decoded != null)
                                                               return (Integer) decoded.verb.ordinal();
                                                           return -1;
                                                       }).call();
                                                       if (id > 0)
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