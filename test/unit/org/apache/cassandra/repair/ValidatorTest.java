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
import java.util.UUID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IMessageSink;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ValidatorTest
{
    private static final String keyspace = "ValidatorTest";
    private static final String columnFamily = "Standard1";
    private static IPartitioner partitioner;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(keyspace,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(keyspace, columnFamily));
        partitioner = Schema.instance.getCFMetaData(keyspace, columnFamily).partitioner;
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
    }

    @Test
    public void testValidatorComplete() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspace, columnFamily, Arrays.asList(range));

        final SimpleCondition lock = new SimpleCondition();
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                try
                {
                    if (message.verb == MessagingService.Verb.REPAIR_MESSAGE)
                    {
                        RepairMessage m = (RepairMessage) message.payload;
                        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
                        assertEquals(desc, m.desc);
                        assertTrue(((ValidationComplete) m).success());
                        assertNotNull(((ValidationComplete) m).trees);
                    }
                }
                finally
                {
                    lock.signalAll();
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });

        InetAddress remote = InetAddress.getByName("127.0.0.2");

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        Validator validator = new Validator(desc, remote, 0);
        MerkleTrees tree = new MerkleTrees(partitioner);
        tree.addMerkleTrees((int) Math.pow(2, 15), validator.desc.ranges);
        validator.prepare(cfs, tree);

        // and confirm that the tree was split
        assertTrue(tree.size() > 1);

        // add a row
        Token mid = partitioner.midpoint(range.left, range.right);
        validator.add(EmptyIterators.unfilteredRow(cfs.metadata, new BufferDecoratedKey(mid, ByteBufferUtil.bytes("inconceivable!")), false));
        validator.complete();

        // confirm that the tree was validated
        Token min = tree.partitioner().getMinimumToken();
        assertNotNull(tree.hash(new Range<>(min, min)));

        if (!lock.isSignaled())
            lock.await();
    }


    @Test
    public void testValidatorFailed() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspace, columnFamily, Arrays.asList(range));

        final SimpleCondition lock = new SimpleCondition();
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddress to)
            {
                try
                {
                    if (message.verb == MessagingService.Verb.REPAIR_MESSAGE)
                    {
                        RepairMessage m = (RepairMessage) message.payload;
                        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
                        assertEquals(desc, m.desc);
                        assertFalse(((ValidationComplete) m).success());
                        assertNull(((ValidationComplete) m).trees);
                    }
                }
                finally
                {
                    lock.signalAll();
                }
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });

        InetAddress remote = InetAddress.getByName("127.0.0.2");

        Validator validator = new Validator(desc, remote, 0);
        validator.fail();

        if (!lock.isSignaled())
            lock.await();
    }
}
