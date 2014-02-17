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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.sink.IMessageSink;
import org.apache.cassandra.sink.SinkManager;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.SyncComplete;
import org.apache.cassandra.utils.MerkleTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DifferencerTest extends SchemaLoader
{
    private static final IPartitioner partirioner = new Murmur3Partitioner();

    @After
    public void tearDown()
    {
        SinkManager.clear();
    }

    /**
     * When there is no difference between two, Differencer should respond SYNC_COMPLETE
     */
    @Test
    public void testNoDifference() throws Throwable
    {
        final InetAddress ep1 = InetAddress.getByName("127.0.0.1");
        final InetAddress ep2 = InetAddress.getByName("127.0.0.1");

        SinkManager.add(new IMessageSink()
        {
            @SuppressWarnings("unchecked")
            public MessageOut handleMessage(MessageOut message, int id, InetAddress to)
            {
                if (message.verb == MessagingService.Verb.REPAIR_MESSAGE)
                {
                    RepairMessage m = (RepairMessage) message.payload;
                    assertEquals(RepairMessage.Type.SYNC_COMPLETE, m.messageType);
                    // we should see SYNC_COMPLETE
                    assertEquals(new NodePair(ep1, ep2), ((SyncComplete)m).nodes);
                }
                return null;
            }

            public MessageIn handleMessage(MessageIn message, int id, InetAddress to)
            {
                return null;
            }
        });
        Range<Token> range = new Range<>(partirioner.getMinimumToken(), partirioner.getRandomToken());
        RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), "Keyspace1", "Standard1", range);

        MerkleTree tree1 = createInitialTree(desc);
        MerkleTree tree2 = createInitialTree(desc);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(ep1, tree1);
        TreeResponse r2 = new TreeResponse(ep2, tree2);
        Differencer diff = new Differencer(desc, r1, r2);
        diff.run();

        assertTrue(diff.differences.isEmpty());
    }

    @Test
    public void testDifference() throws Throwable
    {
        Range<Token> range = new Range<>(partirioner.getMinimumToken(), partirioner.getRandomToken());
        UUID parentRepairSession = UUID.randomUUID();
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("Standard1");

        ActiveRepairService.instance.registerParentRepairSession(parentRepairSession, Arrays.asList(cfs), Arrays.asList(range));

        RepairJobDesc desc = new RepairJobDesc(parentRepairSession, UUID.randomUUID(), "Keyspace1", "Standard1", range);

        MerkleTree tree1 = createInitialTree(desc);
        MerkleTree tree2 = createInitialTree(desc);

        // change a range in one of the trees
        Token token = partirioner.midpoint(range.left, range.right);
        tree1.invalidate(token);
        MerkleTree.TreeRange changed = tree1.get(token);
        changed.hash("non-empty hash!".getBytes());

        Set<Range<Token>> interesting = new HashSet<>();
        interesting.add(changed);

        // difference the trees
        // note: we reuse the same endpoint which is bogus in theory but fine here
        TreeResponse r1 = new TreeResponse(InetAddress.getByName("127.0.0.1"), tree1);
        TreeResponse r2 = new TreeResponse(InetAddress.getByName("127.0.0.2"), tree2);
        Differencer diff = new Differencer(desc, r1, r2);
        diff.run();

        // ensure that the changed range was recorded
        assertEquals("Wrong differing ranges", interesting, new HashSet<>(diff.differences));
    }

    private MerkleTree createInitialTree(RepairJobDesc desc)
    {
        MerkleTree tree = new MerkleTree(partirioner, desc.range, MerkleTree.RECOMMENDED_DEPTH, (int)Math.pow(2, 15));
        tree.init();
        for (MerkleTree.TreeRange r : tree.invalids())
        {
            r.ensureHashInitialised();
        }
        return tree;
    }
}
