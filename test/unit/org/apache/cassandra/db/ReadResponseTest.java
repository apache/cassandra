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

package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadResponseTest
{
    private final Random random = new Random();
    private TableMetadata metadata;
    private TableMetadata metadataWithClustering;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }
    @Before
    public void setup()
    {
        metadata = TableMetadata.builder("ks", "t1")
                                .offline()
                                .addPartitionKeyColumn("p", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();

        metadataWithClustering = TableMetadata.builder("ks", "t2")
                                              .offline()
                                              .addPartitionKeyColumn("p", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .build();
    }

    @Test
    public void fromCommandWithConclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(digest, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(digest, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithConclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    /*
     * Digest responses should never include repaired data tracking as we only request
     * it in read repair or for range queries
     */
    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfRepairedDataDigestRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.repairedDataDigest();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIsConclusiveRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.isRepairedDigestConclusive();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIteratorRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.makeIterator(command);
    }

    @Test
    public void makeDigestDoesntConsiderRepairedDataInfo()
    {
        // It shouldn't be possible to get false positive DigestMismatchExceptions based
        // on differing repaired data tracking info because it isn't requested on initial
        // requests, only following a digest mismatch. Having a test doesn't hurt though
        int key = key();
        ByteBuffer digest1 = digest();
        ReadCommand command1 = command(key, metadata);
        StubRepairedDataInfo rdi1 = new StubRepairedDataInfo(digest1, true);
        ReadResponse response1 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi1);

        ByteBuffer digest2 = digest();
        ReadCommand command2 = command(key, metadata);
        StubRepairedDataInfo rdi2 = new StubRepairedDataInfo(digest2, false);
        ReadResponse response2 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi2);

        assertEquals(response1.digest(command1), response2.digest(command2));
    }

    @Test
    public void inMemoryResponseEmptyIteratorMatchesLocalDataResponse()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        ReadResponse localResponse = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(EmptyIterators.unfilteredPartition(metadata), rdi);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }

    @Test
    public void inMemoryResponseWithRowsMatchesLocalDataResponse()
    {
        int key = key();
        ReadCommand command = command(key, metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        DecoratedKey dk = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
        Row row = buildRow(metadata, dk);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata, dk, row);

        ReadResponse localResponse = command.createResponse(singlePartitionIterator(update), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void inMemoryResponseCannotBeSerialized()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createLocalObjectResponse(EmptyIterators.unfilteredPartition(metadata), rdi);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
        }
        catch (IOException e)
        {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void inMemoryResponseWithOverflowMatchesLocalDataResponse()
    {
        // 5 rows, only 2 fit in memory — 3 overflow into the serialized buffer
        testMultipleRows(2, 5);
    }

    @Test
    public void inMemoryResponseAllRowsInMemoryWhenUnderLimit()
    {
        // 3 rows, limit is 10 — all fit in memory with no overflow
        testMultipleRows(10, 3);
    }

    @Test
    public void inMemoryResponseWithZeroMaxRowsUsesOnlyOverflow()
    {
        // all rows go directly into the overflow buffer
        testMultipleRows(0, 3);
    }

    private void testMultipleRows(int maxRows, int rows)
    {
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, rows);

        ReadResponse localResponse = command.createResponse(singlePartitionIterator(update), rdi);
        ReadResponse inMemoryResponse = ReadResponse.createInMemoryDataResponse(singlePartitionIterator(update), command, rdi, maxRows);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }

    @Test
    public void inMemoryResponseCapturesRepairedDigestAfterIteratorIsConsumed()
    {
        // RepairedDataInfo updates its digest lazily as rows are consumed via withRepairedDataInfo
        // transformations; this test uses a stub that simulates the same timing.
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, 3);

        ByteBuffer expectedDigest = digest();
        // Returns EMPTY before any iteration; returns expectedDigest once the iterator has been consumed.
        LazyRepairedDataInfo rdi = new LazyRepairedDataInfo(expectedDigest);

        ReadResponse response = ReadResponse.createInMemoryDataResponse(lazyRdiWrappedIterator(update, rdi), command, rdi, 10);

        assertTrue("digest should be non-empty after iterator was consumed", rdi.wasConsumedBeforeDigestCaptured());
        assertEquals(expectedDigest, response.repairedDataDigest());
        assertTrue(response.isRepairedDigestConclusive());
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneOnlyAllInMemory()
    {
        // Partition contains only a range tombstone (no rows); limit is large enough that nothing overflows
        testWithTombstones(10, 0, 0, 5);
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneOnlyOverflow()
    {
        // 5 rows at clusterings 0-4 plus a range tombstone covering [6, 9];
        // rows 0-1 go in-memory, rows 2-4 and the RT go to overflow
        testWithTombstones(2, 5, 6, 9);
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneBetweenInMemoryAndOverflow()
    {
        // RT at [1, 2] sits between the in-memory rows (0) and the overflow rows (3-4)
        testWithTombstones(1, 5, 1, 2);
    }

    private void testWithTombstones(int maxRows, int rows, int rtStart, int rtEnd)
    {
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        PartitionUpdate update = buildUpdateWithRowsAndRangeTombstone(metadataWithClustering, key, rows, rtStart, rtEnd);

        ReadResponse localResponse = command.createResponse(singlePartitionIterator(update), rdi);
        ReadResponse inMemoryResponse = ReadResponse.createInMemoryDataResponse(singlePartitionIterator(update), command, rdi, maxRows);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }


    private PartitionUpdate buildUpdateWithRowsAndRangeTombstone(TableMetadata metadata, int partitionKey,
                                                                  int rowCount, int rtStart, int rtEnd)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, ByteBufferUtil.bytes(partitionKey)).timestamp(0);
        for (int i = 0; i < rowCount; i++)
            builder.row(i).add("v", i);
        builder.addRangeTombstone().start(rtStart).end(rtEnd);
        return builder.build();
    }

    private void assertIteratorsEqual(ReadCommand command, ReadResponse expected, ReadResponse actual)
    {
        List<String> expectedUnfiltered = collectUnfiltered(command, expected);
        List<String> actualUnfiltered = collectUnfiltered(command, actual);
        assertEquals(expectedUnfiltered, actualUnfiltered);
    }

    private List<String> collectUnfiltered(ReadCommand command, ReadResponse response)
    {
        List<String> result = new ArrayList<>();
        try (UnfilteredPartitionIterator iter = response.makeIterator(command))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    while (partition.hasNext())
                        result.add(partition.next().toString(partition.metadata(), true));
                }
            }
        }
        return result;
    }

    private Row buildRow(TableMetadata metadata, DecoratedKey key)
    {
        ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v"));
        Clustering<?> clustering = Clustering.EMPTY;
        return BTreeRow.singleCellRow(clustering, BufferCell.live(col, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(42)));
    }

    private PartitionUpdate buildMultiRowUpdate(TableMetadata metadata, int partitionKey, int rowCount)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, ByteBufferUtil.bytes(partitionKey)).timestamp(0);
        for (int i = 0; i < rowCount; i++)
            builder.row(i).add("v", i);
        return builder.build();
    }

    private UnfilteredPartitionIterator singlePartitionIterator(PartitionUpdate update)
    {
        UnfilteredRowIterator rowIter = update.unfilteredIterator();
        return new AbstractUnfilteredPartitionIterator()
        {
            private boolean returned = false;

            public TableMetadata metadata() { return update.metadata(); }

            public boolean hasNext() { return !returned; }

            public UnfilteredRowIterator next()
            {
                returned = true;
                return rowIter;
            }
        };
    }

    private void verifySerDe(ReadResponse response) {
        // check that roundtripping through ReadResponse.serializer behaves as expected
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            roundTripSerialization(response, version.value);

    }

    private void roundTripSerialization(ReadResponse response, int version)
    {
        try
        {
            DataOutputBuffer out = new DataOutputBuffer();
            ReadResponse.serializer.serialize(response, out, version);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            ReadResponse deser = ReadResponse.serializer.deserialize(in, version);
            assertTrue(version >= MessagingService.VERSION_40);
            assertTrue(deser.mayIncludeRepairedDigest());
            assertEquals(response.repairedDataDigest(), deser.repairedDataDigest());
            assertEquals(response.isRepairedDigestConclusive(), deser.isRepairedDigestConclusive());
        }
        catch (IOException e)
        {
            fail("Caught unexpected IOException during SerDe: " + e.getMessage());
        }
    }


    private int key()
    {
        return random.nextInt();
    }

    private ByteBuffer digest()
    {
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private ReadCommand digestCommand(int key, TableMetadata metadata)
    {
        return new StubReadCommand(key, metadata, true);
    }

    private ReadCommand command(int key, TableMetadata metadata)
    {
        return new StubReadCommand(key, metadata, false);
    }

    private ReadCommand command(int key)
    {
        return command(key, metadata);
    }

    private static class StubRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer repairedDigest;
        private final boolean conclusive;

        public StubRepairedDataInfo(ByteBuffer repairedDigest, boolean conclusive)
        {
            super(null);
            this.repairedDigest = repairedDigest;
            this.conclusive = conclusive;
        }
        
        @Override
        public ByteBuffer getDigest()
        {
            return repairedDigest;
        }
        
        @Override
        public boolean isConclusive()
        {
            return conclusive;
        }
    }

    /**
     * Simulates a RepairedDataInfo that updates its digest lazily as rows are consumed.
     * Returns EMPTY_BYTE_BUFFER until the partition iterator wrapping it is fully consumed,
     * at which point getDigest() returns the provided expected digest.
     * This lets us verify that InMemoryDataResponse captures the digest *after* consumption.
     */
    private static class LazyRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer finalDigest;
        private boolean iteratorConsumed = false;
        private boolean digestCapturedAfterConsumption = false;

        LazyRepairedDataInfo(ByteBuffer finalDigest)
        {
            super(null);
            this.finalDigest = finalDigest;
        }

        void markConsumed()
        {
            iteratorConsumed = true;
        }

        boolean wasConsumedBeforeDigestCaptured()
        {
            return digestCapturedAfterConsumption;
        }

        @Override
        public ByteBuffer getDigest()
        {
            if (iteratorConsumed)
                digestCapturedAfterConsumption = true;
            return iteratorConsumed ? finalDigest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        @Override
        public boolean isConclusive()
        {
            return true;
        }
    }

    private UnfilteredPartitionIterator lazyRdiWrappedIterator(PartitionUpdate update, LazyRepairedDataInfo rdi)
    {
        // Wraps the row iterator so that close() marks rdi as consumed, simulating RepairedDataInfo
        // updating its digest lazily after the partition has been fully read.
        UnfilteredRowIterator baseRowIter = update.unfilteredIterator();
        UnfilteredRowIterator wrappedRowIter = new WrappingUnfilteredRowIterator()
        {
            public UnfilteredRowIterator wrapped() { return baseRowIter; }

            @Override
            public void close()
            {
                rdi.markConsumed();
                baseRowIter.close();
            }
        };
        return new AbstractUnfilteredPartitionIterator()
        {
            private boolean returned = false;

            public TableMetadata metadata() { return update.metadata(); }

            public boolean hasNext() { return !returned; }

            public UnfilteredRowIterator next()
            {
                returned = true;
                return wrappedRowIter;
            }
        };
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(int key, TableMetadata metadata, boolean isDigest)
        {
            super(metadata.epoch,
                  isDigest,
                  0,
                  false,
                  PotentialTxnConflicts.DISALLOW,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
                  new ClusteringIndexSliceFilter(Slices.ALL, false),
                  null,
                  false,
                  null);

        }

        @Override
        public boolean selectsFullPartition()
        {
            return true;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            return EmptyIterators.unfilteredPartition(this.metadata());
        }
    }
}
