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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.streaming.CassandraStreamHeader;
import org.apache.cassandra.db.streaming.CassandraStreamReader;
import org.apache.cassandra.db.streaming.IStreamReader;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.async.StreamCompressionSerializer;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TokenRangeTestUtil.broadcastAddress;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.randomInt;
import static org.apache.cassandra.utils.TokenRangeTestUtil.setLocalTokens;
import static org.apache.cassandra.utils.TokenRangeTestUtil.setPendingRanges;
import static org.apache.cassandra.utils.TokenRangeTestUtil.token;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamReaderTest
{
    private static final String TEST_NAME = "streamreader_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);
    }

    @Before
    public void setup() throws Exception
    {
        DatabaseDescriptor.setLogOutOfTokenRangeRequests(true);
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(true);

        StorageService.instance.getTokenMetadata().clearUnsafe();
        // All tests suppose a 2 node ring, with the other peer having the tokens 0, 200, 400
        // Initially, the local node has no tokens so when indivividual test set owned tokens or
        // pending ranges for the local node, they're always in relation to this.
        // e.g. test calls setLocalTokens(100, 300) the ring now looks like
        // peer  -> (min, 0], (100, 200], (300, 400]
        // local -> (0, 100], (200, 300], (400, max]
        //
        // Pending ranges are set in test using start/end pairs.
        // Ring is initialised:
        // peer  -> (min, max]
        // local -> (,]
        // e.g. test calls setPendingRanges(0, 100, 200, 300)
        // the pending ranges for local would be calculated as:
        // local -> (0, 100], (200, 300]
        StorageService.instance.getTokenMetadata().updateNormalTokens(Lists.newArrayList(token(0),
                                                                                         token(200),
                                                                                         token(400)),
                                                                      node1);
    }
    @Test
    public void testReceiveWithNoOwnedRanges() throws Exception
    {
        int[] tokens = {10, 20};
        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableWithRangeContained() throws Exception
    {
        int[] tokens = {10, 20};
        setLocalTokens(100);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivedTableLowestKeyBoundsExclusive() throws Exception
    {
        // verify that ranges are left exclusive
        int[] tokens = {0, 10};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeWithExactMatch() throws Exception
    {
        // Because ranges are left exlusive, for the range (0, 100] the lowest permissable key is 1
        int[] tokens = {1, 100};
        setLocalTokens(100);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeLessThanOwned() throws Exception
    {
        int[] tokens = {-100, 0};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeGreaterThanOwned() throws Exception
    {
        int[] tokens = {101, 200};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedRangeReceivingTableRangeOverlappingOwned() throws Exception
    {
        int[] tokens = {80, 120};
        setLocalTokens(100);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableContainedBeforeMax() throws Exception
    {
        int[] tokens = {110, 120};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableContainedAfterMin() throws Exception
    {
        int[] tokens = {-150, -140};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableOverlappingUpward() throws Exception
    {
        int[] tokens = {-10, 10};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSingleOwnedWrappingRangeReceivingTableOverlappingDownward() throws Exception
    {
        int[] tokens = {90, 110};

        // local node owns (min, 0] & (100, max], peer owns (0, 100]
        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(token(0), broadcastAddress);
        StorageService.instance.getTokenMetadata().updateNormalToken(token(100), node1);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeContainedInFirstOwned() throws Exception
    {
        int[] tokens = {10, 20};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeContainedInLastOwned() throws Exception
    {
        int[] tokens = {450, 460};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeLessThanOwned() throws Exception
    {
        int[] tokens = {510, 520};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeGreaterThanOwned() throws Exception
    {
        int[] tokens = {-20, -10};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivingTableRangeOverlappingOwned() throws Exception
    {
        int[] tokens = {80, 120};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesAllDisjointFromReceivingTableRange() throws Exception
    {
        int[] tokens = {310, 320};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesDisjointSpannedByReceivingTableRange() throws Exception
    {
        int[] tokens = {80, 320};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithMultipleOwnedRangesReceivedTableRangeExactMatch() throws Exception
    {
        // bacause ranges are left exclusive, for the range (200, 300] the lowest permissable key is 201
        int[] tokens = {201, 300};
        setLocalTokens(100, 300, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithOwnedRangeWrappingAndReceivedFileWhollyContained() throws Exception
    {
        // peer  -> (-100, 0], (100, 200], (300, 400]
        // local -> (min, -100], (400, max]
        setLocalTokens(-100);
        int[] tokens = {-200, 500};
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithOwnedRangeWrappingAndReceivedFilePartiallyContained() throws Exception
    {
        // peer  -> (-100, 0], (100, 200], (300, 400]
        // local -> (min, -100], (400, max]
        setLocalTokens(-100);
        int[] tokens = {-200, 300};
        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithOwnedRangeWrappingAndReceivedFileNotContained() throws Exception
    {
        // peer  -> (-100, 0], (100, 200], (300, 400]
        // local -> (min, -100], (400, max]
        setLocalTokens(-100);
        int[] tokens = {0, 300};
        tryReceiveExpectingFailure(tokens);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, true);
    }

    @Test
    public void testReceiveWithSinglePendingRangeReceivingTableWithRangeContained() throws Exception
    {
        int[] tokens = {10, 20};
        setPendingRanges(KEYSPACE, 0, 100);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveWithMultiplePendingRangesReceivingTableRangeContainedInFirstOwned() throws Exception
    {
        int[] tokens = {10, 20};
        setPendingRanges(KEYSPACE, 0, 100, 200, 300, 400, 500);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    @Test
    public void testReceiveNormalizesOwnedAndPendingRanges() throws Exception
    {
        // Incoming file is not covered by either a single owned or pending range,
        // but it is covered by the normalized set of both
        int[] tokens = {90, 110};
        setLocalTokens(100);
        setPendingRanges(KEYSPACE, 100, 120);

        tryReceiveExpectingSuccess(tokens, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryReceiveExpectingSuccess(tokens, false);
    }

    public static StreamSession setupStreamingSessionForTest()
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.REPAIR, 1, new DefaultConnectionFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(UUID.randomUUID(), StreamOperation.REPAIR, Collections.<StreamEventHandler>emptyList(), streamCoordinator);

        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        streamCoordinator.addSessionInfo(new SessionInfo(peer, 0, peer, Collections.emptyList(), Collections.emptyList(), StreamSession.State.INITIALIZED));

        StreamSession session = streamCoordinator.getOrCreateNextSession(peer);
        session.init(future);
        return session;
    }

    private static void tryReceiveExpectingSuccess(int[] tokens,
                                                   boolean isOutOfRange) throws IOException
    {
        StreamSession session = setupStreamingSessionForTest();
        StreamMessageHeader header = streamHeader();
        CassandraStreamHeader streamHeader = streamMessageHeader(tokens);
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        IStreamReader reader = streamReader(header, streamHeader, session);
        StreamSummary streamSummary = new StreamSummary(streamHeader.tableId, 1, 0);
        session.prepareReceiving(streamSummary);
        reader.read(incomingStream(tokens));
        assertEquals(isOutOfRange, StorageMetrics.totalOpsForInvalidToken.getCount() > startMetricCount);
    }

    private static void tryReceiveExpectingFailure(int[] tokens) throws IOException
    {
        StreamSession session = setupStreamingSessionForTest();
        StreamMessageHeader header = streamHeader();
        CassandraStreamHeader streamHeader = streamMessageHeader(tokens);
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        StreamSummary streamSummary = new StreamSummary(streamHeader.tableId, 1, 0);
        session.prepareReceiving(streamSummary);
        try
        {
            IStreamReader reader = streamReader(header, streamHeader, session);
            try (SSTableMultiWriter ignore = reader.read(incomingStream(tokens)))
            {
                fail("Expected StreamReceivedOfTokenRangeException");
            }
        }
        catch (StreamReceivedOutOfTokenRangeException e)
        {
            // expected
        }
        assertTrue(StorageMetrics.totalOpsForInvalidToken.getCount() > startMetricCount);
    }


    private static class BufferSupplier implements AsyncStreamingOutputPlus.BufferSupplier
    {
        ByteBuffer supplied;

        @Override
        public ByteBuffer get(int capacity)
        {
            supplied = ByteBuffer.allocateDirect(capacity);
            return supplied;
        }

        public ByteBuffer getSupplied()
        {
            return supplied;
        }

    }

    private static DataInputPlus incomingStream(int...tokens) throws IOException
    {
        final net.jpountz.lz4.LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();

        DataOutputBuffer out = new DataOutputBuffer();
        for (int token : tokens)
            ByteBufferUtil.writeWithShortLength(ByteBufferUtil.bytes((long)token), out);

        int current_version = MessagingService.current_version;

        BufferSupplier bufferSupplier = new BufferSupplier();
        StreamCompressionSerializer.serialize(compressor, out.buffer(), current_version).write(bufferSupplier);

        return new DataInputBuffer(bufferSupplier.getSupplied(), false);
    }

    private static IStreamReader streamReader(StreamMessageHeader header, CassandraStreamHeader streamHeader, StreamSession session)
    {
        return new KeysOnlyStreamReader(header, streamHeader, session);
    }

    private static StreamMessageHeader streamHeader()
    {
        TableMetadata tmd = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata();
        int fakeSession = randomInt(9);
        int fakeSeq = randomInt(9);
        return new StreamMessageHeader(tmd.id,
                                       null,
                                       null,
                                       true,
                                       fakeSession,
                                       fakeSeq,
                                       System.currentTimeMillis(),
                                       null);
    }

    private static CassandraStreamHeader streamMessageHeader(int...tokens)
    {
        TableMetadata tmd = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata();
        Version version = BigFormat.latestVersion;
        List<SSTableReader.PartitionPositionBounds> fakeSections = new ArrayList<>();
        // each decorated key takes up (2 + 8) bytes, so this enables the
        // StreamReader to calculate the expected number of bytes to read
        fakeSections.add(new SSTableReader.PartitionPositionBounds(0L, (tokens.length * 10L) - 1));

        return CassandraStreamHeader.builder()
                                    .withTableId(tmd.id)
                                    .withSerializationHeader(SerializationHeader.makeWithoutStats(tmd).toComponent())
                                    .withSSTableFormat(SSTableFormat.Type.BIG)
                                    .withSSTableVersion(version)
                                    .withSections(fakeSections)
                                    .build();
    }

    // Simplifies generating test data as token == key (expects key to be an encoded long)
    private static class FakeMurmur3Partitioner extends Murmur3Partitioner
    {
        public DecoratedKey decorateKey(ByteBuffer key)
        {
            return new BufferDecoratedKey(new LongToken(ByteBufferUtil.toLong(key)), key);
        }
    }

    // Stream reader which no-ops the reading/writing of the actual partition data.
    // As we only care about keys/tokens here, we don't need to generate the rest
    // of the sstable data to simulate a stream
    private static class KeysOnlyStreamReader extends CassandraStreamReader
    {
        public KeysOnlyStreamReader(StreamMessageHeader header, CassandraStreamHeader streamHeader, StreamSession session)
        {
            super(header, streamHeader, session);
        }

        protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, UUID pendingRepair, SSTableFormat.Type format) throws IOException
        {
            return super.createWriter(cfs, totalSize, repairedAt, pendingRepair, format);
        }

        @Override
        protected StreamDeserializer getDeserializer(TableMetadata metadata, TrackedDataInputPlus in, Version inputVersion, StreamSession session, SSTableMultiWriter writer) throws IOException
        {
            return new TestStreamDeserializer(metadata, in, inputVersion, getHeader(metadata), session, writer);
        }

        private static class TestStreamDeserializer extends CassandraStreamReader.StreamDeserializer
        {
            TestStreamDeserializer(TableMetadata metadata, DataInputPlus in, Version version, SerializationHeader header, StreamSession session, SSTableMultiWriter writer) throws IOException
            {
                super(metadata.unbuild().partitioner(new FakeMurmur3Partitioner()).build(), in, version, header, session, writer);
            }

            @Override
            protected void readPartition()
            {
                // no-op, our dummy stream contains only decorated keys
                partitionLevelDeletion = DeletionTime.LIVE;
                iterator = new SSTableSimpleIterator.EmptySSTableSimpleIterator(metadata());
            }

            @Override
            public Row staticRow()
            {
                return Rows.EMPTY_STATIC_ROW;
            }

            @Override
            public DeletionTime partitionLevelDeletion()
            {
                return DeletionTime.LIVE;
            }


        }
    }
}
