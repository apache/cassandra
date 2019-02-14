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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.hash.Hasher;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionsTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
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
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ValidatorTest
{
    private static final long TEST_TIMEOUT = 60; //seconds
    private static int testSizeMegabytes;

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
        partitioner = Schema.instance.getTableMetadata(keyspace, columnFamily).partitioner;
        testSizeMegabytes = DatabaseDescriptor.getRepairSessionSpaceInMegabytes();
    }

    @After
    public void tearDown()
    {
        MessagingService.instance().clearMessageSinks();
        DatabaseDescriptor.setRepairSessionSpaceInMegabytes(testSizeMegabytes);
    }

    @Before
    public void setup()
    {
        DatabaseDescriptor.setRepairSessionSpaceInMegabytes(testSizeMegabytes);
    }

    @Test
    public void testValidatorComplete() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspace, columnFamily, Arrays.asList(range));

        final CompletableFuture<MessageOut> outgoingMessageSink = registerOutgoingMessageSink();

        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(columnFamily);

        Validator validator = new Validator(desc, remote, 0, PreviewKind.NONE);
        MerkleTrees tree = new MerkleTrees(partitioner);
        tree.addMerkleTrees((int) Math.pow(2, 15), validator.desc.ranges);
        validator.prepare(cfs, tree);

        // and confirm that the tree was split
        assertTrue(tree.size() > 1);

        // add a row
        Token mid = partitioner.midpoint(range.left, range.right);
        validator.add(EmptyIterators.unfilteredRow(cfs.metadata(), new BufferDecoratedKey(mid, ByteBufferUtil.bytes("inconceivable!")), false));
        validator.complete();

        // confirm that the tree was validated
        Token min = tree.partitioner().getMinimumToken();
        assertNotNull(tree.hash(new Range<>(min, min)));

        MessageOut message = outgoingMessageSink.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        assertEquals(MessagingService.Verb.REPAIR_MESSAGE, message.verb);
        RepairMessage m = (RepairMessage) message.payload;
        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
        assertEquals(desc, m.desc);
        assertTrue(((ValidationComplete) m).success());
        assertNotNull(((ValidationComplete) m).trees);
    }


    @Test
    public void testValidatorFailed() throws Throwable
    {
        Range<Token> range = new Range<>(partitioner.getMinimumToken(), partitioner.getRandomToken());
        final RepairJobDesc desc = new RepairJobDesc(UUID.randomUUID(), UUID.randomUUID(), keyspace, columnFamily, Arrays.asList(range));

        final CompletableFuture<MessageOut> outgoingMessageSink = registerOutgoingMessageSink();

        InetAddressAndPort remote = InetAddressAndPort.getByName("127.0.0.2");

        Validator validator = new Validator(desc, remote, 0, PreviewKind.NONE);
        validator.fail();

        MessageOut message = outgoingMessageSink.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        assertEquals(MessagingService.Verb.REPAIR_MESSAGE, message.verb);
        RepairMessage m = (RepairMessage) message.payload;
        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
        assertEquals(desc, m.desc);
        assertFalse(((ValidationComplete) m).success());
        assertNull(((ValidationComplete) m).trees);
    }

    @Test
    public void simpleValidationTest128() throws Exception
    {
        simpleValidationTest(128);
    }

    @Test
    public void simpleValidationTest1500() throws Exception
    {
        simpleValidationTest(1500);
    }

    /**
     * Test for CASSANDRA-5263
     * 1. Create N rows
     * 2. Run validation compaction
     * 3. Expect merkle tree with size 2^(log2(n))
     */
    public void simpleValidationTest(int n) throws Exception
    {
        Keyspace ks = Keyspace.open(keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnFamily);
        cfs.clearUnsafe();

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        CompactionsTest.populate(keyspace, columnFamily, 0, n, 0); //ttl=3s

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getLiveSSTables().size());

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        UUID repairSessionId = UUIDGen.getTimeUUID();
        final RepairJobDesc desc = new RepairJobDesc(repairSessionId, UUIDGen.getTimeUUID(), cfs.keyspace.getName(),
                                               cfs.getTableName(), Collections.singletonList(new Range<>(sstable.first.getToken(),
                                                                                                                sstable.last.getToken())));

        ActiveRepairService.instance.registerParentRepairSession(repairSessionId, FBUtilities.getBroadcastAddressAndPort(),
                                                                 Collections.singletonList(cfs), desc.ranges, false, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 false, PreviewKind.NONE);

        final CompletableFuture<MessageOut> outgoingMessageSink = registerOutgoingMessageSink();
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddressAndPort(), 0, true, false, PreviewKind.NONE);
        ValidationManager.instance.submitValidation(cfs, validator);

        MessageOut message = outgoingMessageSink.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        assertEquals(MessagingService.Verb.REPAIR_MESSAGE, message.verb);
        RepairMessage m = (RepairMessage) message.payload;
        assertEquals(RepairMessage.Type.VALIDATION_COMPLETE, m.messageType);
        assertEquals(desc, m.desc);
        assertTrue(((ValidationComplete) m).success());
        MerkleTrees trees = ((ValidationComplete) m).trees;

        Iterator<Map.Entry<Range<Token>, MerkleTree>> iterator = trees.iterator();
        while (iterator.hasNext())
        {
            assertEquals(Math.pow(2, Math.ceil(Math.log(n) / Math.log(2))), iterator.next().getValue().size(), 0.0);
        }
        assertEquals(trees.rowCount(), n);
    }

    /*
     * Test for CASSANDRA-14096 size limiting. We:
     * 1. Limit the size of a repair session
     * 2. Submit a validation
     * 3. Check that the resulting tree is of limited depth
     */
    @Test
    public void testSizeLimiting() throws Exception
    {
        Keyspace ks = Keyspace.open(keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnFamily);
        cfs.clearUnsafe();

        DatabaseDescriptor.setRepairSessionSpaceInMegabytes(1);

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        // 2 ** 14 rows would normally use 2^14 leaves, but with only 1 meg we should only use 2^12
        CompactionsTest.populate(keyspace, columnFamily, 0, 1 << 14, 0);

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getLiveSSTables().size());

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        UUID repairSessionId = UUIDGen.getTimeUUID();
        final RepairJobDesc desc = new RepairJobDesc(repairSessionId, UUIDGen.getTimeUUID(), cfs.keyspace.getName(),
                                                     cfs.getTableName(), Collections.singletonList(new Range<>(sstable.first.getToken(),
                                                                                                               sstable.last.getToken())));

        ActiveRepairService.instance.registerParentRepairSession(repairSessionId, FBUtilities.getBroadcastAddressAndPort(),
                                                                 Collections.singletonList(cfs), desc.ranges, false, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 false, PreviewKind.NONE);

        final CompletableFuture<MessageOut> outgoingMessageSink = registerOutgoingMessageSink();
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddressAndPort(), 0, true, false, PreviewKind.NONE);
        ValidationManager.instance.submitValidation(cfs, validator);

        MessageOut message = outgoingMessageSink.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        MerkleTrees trees = ((ValidationComplete) message.payload).trees;

        Iterator<Map.Entry<Range<Token>, MerkleTree>> iterator = trees.iterator();
        int numTrees = 0;
        while (iterator.hasNext())
        {
            assertEquals(1 << 12, iterator.next().getValue().size(), 0.0);
            numTrees++;
        }
        assertEquals(1, numTrees);

        assertEquals(trees.rowCount(), 1 << 14);
    }

    /*
     * Test for CASSANDRA-11390. When there are multiple subranges the trees should
     * automatically size down to make each subrange fit in the provided memory
     * 1. Limit the size of all the trees
     * 2. Submit a validation against more than one range
     * 3. Check that we have the right number and sizes of trees
     */
    @Test
    public void testRangeSplittingTreeSizeLimit() throws Exception
    {
        Keyspace ks = Keyspace.open(keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnFamily);
        cfs.clearUnsafe();

        DatabaseDescriptor.setRepairSessionSpaceInMegabytes(1);

        // disable compaction while flushing
        cfs.disableAutoCompaction();

        // 2 ** 14 rows would normally use 2^14 leaves, but with only 1 meg we should only use 2^12
        CompactionsTest.populate(keyspace, columnFamily, 0, 1 << 14, 0);

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getLiveSSTables().size());

        // wait enough to force single compaction
        TimeUnit.SECONDS.sleep(5);

        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        UUID repairSessionId = UUIDGen.getTimeUUID();

        List<Range<Token>> ranges = splitHelper(new Range<>(sstable.first.getToken(), sstable.last.getToken()), 2);


        final RepairJobDesc desc = new RepairJobDesc(repairSessionId, UUIDGen.getTimeUUID(), cfs.keyspace.getName(),
                                                     cfs.getTableName(), ranges);

        ActiveRepairService.instance.registerParentRepairSession(repairSessionId, FBUtilities.getBroadcastAddressAndPort(),
                                                                 Collections.singletonList(cfs), desc.ranges, false, ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                 false, PreviewKind.NONE);

        final CompletableFuture<MessageOut> outgoingMessageSink = registerOutgoingMessageSink();
        Validator validator = new Validator(desc, FBUtilities.getBroadcastAddressAndPort(), 0, true, false, PreviewKind.NONE);
        ValidationManager.instance.submitValidation(cfs, validator);

        MessageOut message = outgoingMessageSink.get(TEST_TIMEOUT, TimeUnit.SECONDS);
        MerkleTrees trees = ((ValidationComplete) message.payload).trees;

        // Should have 4 trees each with a depth of on average 10 (since each range should have gotten 0.25 megabytes)
        Iterator<Map.Entry<Range<Token>, MerkleTree>> iterator = trees.iterator();
        int numTrees = 0;
        double totalResolution = 0;
        while (iterator.hasNext())
        {
            long size = iterator.next().getValue().size();
            // So it turns out that sstable range estimates are pretty variable, depending on the sampling we can
            // get a wide range of values here. So we just make sure that we're smaller than in the single range
            // case and have the right total size.
            assertTrue(size <= (1 << 11));
            assertTrue(size >= (1 << 9));
            totalResolution += size;
            numTrees += 1;
        }

        assertEquals(trees.rowCount(), 1 << 14);
        assertEquals(4, numTrees);

        // With a single tree and a megabyte we should had a total resolution of 2^12 leaves; with multiple
        // ranges we should get similar overall resolution, but not more.
        assertTrue(totalResolution > (1 << 11) && totalResolution < (1 << 13));
    }

    private List<Range<Token>> splitHelper(Range<Token> range, int depth)
    {
        if (depth <= 0)
        {
            List<Range<Token>> tokens = new ArrayList<>();
            tokens.add(range);
            return tokens;
        }
        Token midpoint = partitioner.midpoint(range.left, range.right);
        List<Range<Token>> left = splitHelper(new Range<>(range.left, midpoint), depth - 1);
        List<Range<Token>> right = splitHelper(new Range<>(midpoint, range.right), depth - 1);
        left.addAll(right);
        return left;
    }

    @Test
    public void testCountingHasher()
    {
        Hasher [] hashers = new Hasher[] {new Validator.CountingHasher(), Validator.CountingHasher.hashFunctions[0].newHasher(), Validator.CountingHasher.hashFunctions[1].newHasher() };
        byte [] random = UUIDGen.getTimeUUIDBytes();

        // call all overloaded methods:
        for (Hasher hasher : hashers)
        {
            hasher.putByte((byte) 33)
                  .putBytes(random)
                  .putBytes(ByteBuffer.wrap(random))
                  .putBytes(random, 0, 3)
                  .putChar('a')
                  .putBoolean(false)
                  .putDouble(3.3)
                  .putInt(77)
                  .putFloat(99)
                  .putLong(101)
                  .putShort((short) 23);
        }

        long len = Byte.BYTES
                   + random.length * 2 // both the byte[] and the ByteBuffer
                   + 3 // 3 bytes from the random byte[]
                   + Character.BYTES
                   + Byte.BYTES
                   + Double.BYTES
                   + Integer.BYTES
                   + Float.BYTES
                   + Long.BYTES
                   + Short.BYTES;

        byte [] h = hashers[0].hash().asBytes();
        assertTrue(Arrays.equals(hashers[1].hash().asBytes(), Arrays.copyOfRange(h, 0, 16)));
        assertTrue(Arrays.equals(hashers[2].hash().asBytes(), Arrays.copyOfRange(h, 16, 32)));
        assertEquals(len, ((Validator.CountingHasher)hashers[0]).getCount());
    }

    private CompletableFuture<MessageOut> registerOutgoingMessageSink()
    {
        final CompletableFuture<MessageOut> future = new CompletableFuture<>();
        MessagingService.instance().addMessageSink(new IMessageSink()
        {
            public boolean allowOutgoingMessage(MessageOut message, int id, InetAddressAndPort to)
            {
                future.complete(message);
                return false;
            }

            public boolean allowIncomingMessage(MessageIn message, int id)
            {
                return false;
            }
        });
        return future;
    }
}
