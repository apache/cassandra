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
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.metrics.ReadResponseMetrics;
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
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(EmptyIterators.unfilteredPartition(metadata), rdi, false);

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
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, false);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void inMemoryResponseCannotBeSerialized()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createLocalObjectResponse(EmptyIterators.unfilteredPartition(metadata), rdi, false);

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
    public void inMemoryResponseUsesHigherLimitsForOneConsistency()
    {
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        // More rows than the default row limit (128) but fewer than the ONE/LOCAL_ONE limit (512).
        int rowCount = 150;
        PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, rowCount);

        ReadResponse otherResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, false);
        ReadResponse oneResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, true);

        // The default-tier limits overflow this partition; ONE/LOCAL_ONE keeps more of it in memory.
        assertTrue("default tier should overflow", otherResponse.inMemoryUnfilteredCount() < rowCount);
        assertTrue("ONE/LOCAL_ONE tier should keep more rows in memory",
                   oneResponse.inMemoryUnfilteredCount() > otherResponse.inMemoryUnfilteredCount());
    }

    @Test
    public void inMemoryResponseCountsRowLimitHitOnOverflow()
    {
        // Row limit 1, size limit disabled: the rows beyond the first overflow, hitting the row-count limit.
        limitHitAssertion()
            .rows(5).maxRows(1)
            .expectRowLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseCountsSizeLimitHitOnOverflow()
    {
        // Row limit disabled, tiny size limit: the rows beyond the first overflow, hitting the size limit.
        limitHitAssertion()
            .rows(5).maxSize(1)
            .expectSizeLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseCountsRowLimitFirstWhenBothLimitsCrossed()
    {
        // Both limits would be crossed; the row-count limit is checked first, so only it is counted.
        limitHitAssertion()
            .rows(5).maxRows(1).maxSize(1)
            .expectRowLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseDoesNotCountLimitHitWhenAllInMemory()
    {
        // Limits above the response size: nothing overflows.
        limitHitAssertion()
            .rows(3).maxRows(100).maxSize(Long.MAX_VALUE)
            .expectNoLimitHit()
            .verify();
    }

    private LimitHitAssertionBuilder limitHitAssertion()
    {
        return new LimitHitAssertionBuilder();
    }

    /**
     * Builds an in-memory response for a partition of {@link #rows} rows with the configured per-request limits
     * ({@code 0} disables a limit) and asserts which limit-hit counter(s) were incremented (each by one).
     */
    private class LimitHitAssertionBuilder
    {
        private int rows = 0;
        private int maxRows = 0;   // 0 = row-count limit disabled
        private long maxSize = 0;  // 0 = heap-size limit disabled
        private boolean expectRowHit = false;
        private boolean expectSizeHit = false;

        LimitHitAssertionBuilder rows(int rows) { this.rows = rows; return this; }
        LimitHitAssertionBuilder maxRows(int maxRows) { this.maxRows = maxRows; return this; }
        LimitHitAssertionBuilder maxSize(long maxSize) { this.maxSize = maxSize; return this; }
        LimitHitAssertionBuilder expectRowLimitHit() { this.expectRowHit = true; return this; }
        LimitHitAssertionBuilder expectSizeLimitHit() { this.expectSizeHit = true; return this; }
        LimitHitAssertionBuilder expectNoLimitHit() { this.expectRowHit = false; this.expectSizeHit = false; return this; }

        void verify()
        {
            int key = key();
            ReadCommand command = command(key, metadataWithClustering);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
            PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, rows);

            long rowHitsBefore = ReadResponseMetrics.inMemoryRowLimitHits.getCount();
            long sizeHitsBefore = ReadResponseMetrics.inMemorySizeLimitHits.getCount();
            ReadResponse.createInMemoryDataResponse(singlePartitionIterator(update), command, rdi, maxRows, maxSize);
            assertEquals("unexpected row limit hit count", rowHitsBefore + (expectRowHit ? 1 : 0), ReadResponseMetrics.inMemoryRowLimitHits.getCount());
            assertEquals("unexpected size limit hit count", sizeHitsBefore + (expectSizeHit ? 1 : 0), ReadResponseMetrics.inMemorySizeLimitHits.getCount());
        }
    }

    @Test
    public void inMemoryResponseClosesRowIteratorOnceWhenAllInMemory()
    {
        // Nothing overflows, so the row iterator is only ever closed by the response builder itself.
        assertRowIteratorClosedOnce()
            .rows(3)
            .maxRows(10)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseClosesRowIteratorOnceOnRowLimitOverflow()
    {
        assertRowIteratorClosedOnce()
            .rows(5)
            .maxRows(2)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseClosesRowIteratorOnceOnSizeLimitOverflow()
    {
        assertRowIteratorClosedOnce()
            .rows(5)
            .maxSize(1)
            .expectSerialized()
            .verify();
    }

    private CloseAssertionBuilder assertRowIteratorClosedOnce()
    {
        return new CloseAssertionBuilder();
    }

    /**
     * Builds an in-memory response over a row iterator that counts how often it is closed, and asserts the count is
     * exactly one. On overflow the iterator is passed on to the serializer, which closes every partition it
     * serializes, so the response builder must not close it again. Closing a partition is what triggers the recording
     * of per-read metrics (see {@link ReadCommand#withMetricsRecording}) and similar end-of-partition work, and a
     * second close would count all of it twice. The count is taken on a wrapper handed straight to the response
     * builder rather than through the transform framework, whose iterators swallow repeated closes internally
     * ({@code BaseIterator.close}) and would therefore hide the problem.
     */
    private class CloseAssertionBuilder
    {
        private int rows = 0;
        private int maxRows = 0;   // 0 = row-count limit disabled
        private long maxSize = 0;  // 0 = heap-size limit disabled
        // true = the response is expected to stay in memory, false = a limit is crossed and it is serialized in full
        private Boolean expectInMemory = null;

        CloseAssertionBuilder rows(int rows) { this.rows = rows; return this; }
        CloseAssertionBuilder maxRows(int maxRows) { this.maxRows = maxRows; return this; }
        CloseAssertionBuilder maxSize(long maxSize) { this.maxSize = maxSize; return this; }
        CloseAssertionBuilder expectInMemory() { this.expectInMemory = true; return this; }
        CloseAssertionBuilder expectSerialized() { this.expectInMemory = false; return this; }

        void verify()
        {
            int partitionKey = key();
            ReadCommand command = command(partitionKey, metadataWithClustering);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
            PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, partitionKey, rows);

            CloseCountingRowIterator rowIter = new CloseCountingRowIterator(update.unfilteredIterator());
            ReadResponse response = ReadResponse.createInMemoryDataResponse(singlePartitionIterator(rowIter), command, rdi, maxRows, maxSize);

            // Guards against the assertion below passing on a path it was not meant to cover: a serialized response
            // keeps nothing in memory, one that stayed in memory holds every row.
            assertEquals("response took the wrong path", expectInMemory ? rows : 0, response.inMemoryUnfilteredCount());
            assertEquals("row iterator must be closed exactly once", 1, rowIter.closeCount);
        }
    }

    /**
     * Passes everything through to the wrapped iterator, counting the calls to close().
     */
    private static class CloseCountingRowIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        private int closeCount = 0;

        CloseCountingRowIterator(UnfilteredRowIterator wrapped)
        {
            this.wrapped = wrapped;
        }

        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        @Override
        public void close()
        {
            closeCount++;
            wrapped.close();
        }
    }

    @Test
    public void inMemoryResponseWithOverflowMatchesLocalDataResponse()
    {
        // Row limit crossed: the whole response is serialized.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseAllRowsInMemoryWhenUnderLimit()
    {
        inMemoryAssertion()
            .maxRows(10).rows(3)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseByteLimitWithOverflow()
    {
        // Row limit disabled; the byte limit is crossed, so the whole response is serialized.
        inMemoryAssertion()
            .maxBytesAsSizeOfFirstNRows(2).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseByteLimitAllRowsInMemoryWhenUnderLimit()
    {
        // Byte limit sized above the whole response: it stays in memory.
        inMemoryAssertion()
            .maxBytesAsSizeOfFirstNRows(10).rows(3)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseByteLimitReachedBeforeRowLimit()
    {
        // Row limit (10) is not reached; the byte limit (sized for 2 rows) is crossed first, so the response is serialized.
        inMemoryAssertion()
            .maxRows(10).maxBytesAsSizeOfFirstNRows(2).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseRowLimitReachedBeforeByteLimit()
    {
        // Byte limit (sized for all 5 rows) is not reached; the row limit (2) is crossed first, so the response is serialized.
        inMemoryAssertion()
            .maxRows(2).maxBytesAsSizeOfFirstNRows(5).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseKeepsEverythingWhenBothLimitsDisabled()
    {
        // Both limits disabled: the whole partition is kept in memory.
        inMemoryAssertion()
            .maxRows(0).rows(4)
            .expectInMemory()
            .verify();
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

        // large heap budget (row limit disabled) so all rows are kept in memory; this test only cares about digest capture timing
        ReadResponse response = ReadResponse.createInMemoryDataResponse(lazyRdiWrappedIterator(update, rdi), command, rdi, 0, Long.MAX_VALUE);

        assertTrue("digest should be non-empty after iterator was consumed", rdi.wasConsumedBeforeDigestCaptured());
        assertEquals(expectedDigest, response.repairedDataDigest());
        assertTrue(response.isRepairedDigestConclusive());
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneAllInMemory()
    {
        // A single range tombstone (open+close markers) well under the limit: kept in memory.
        inMemoryAssertion()
            .maxRows(10).rows(0)
            .withTombstone(rangeTombstone(0, 5))
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneOverflow()
    {
        // Rows plus a trailing range tombstone cross the row limit: the whole response is serialized.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .withTombstone(rangeTombstone(6, 9))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithTombstonesOnlyOverflow()
    {
        // Two range tombstones (open+close markers each) cross the row limit: the whole response is serialized.
        inMemoryAssertion()
            .maxRows(2).rows(0)
            .withTombstone(rangeTombstone(0, 3))
            .withTombstone(rangeTombstone(5, 8))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneAmongRowsOverflow()
    {
        // A range tombstone at [1, 2) interleaved with rows 0-4, crossing the row limit: serialized in full.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .withTombstone(rangeTombstone(1, 2))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneAmongRowsOverflowReversed()
    {
        // Same as above with reversed read order; the range tombstone is at [2, 3).
        inMemoryAssertion()
            .maxRows(2).rows(5).reversed()
            .withTombstone(rangeTombstone(2, 3))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithOverlappingRangeTombstonesAtDifferentTimestamps()
    {
        // Three overlapping RTs: [0,3) ts=100, [1,4) ts=200, [2,5) ts=50 form a single continuous open range
        // (bound, boundary, boundary, bound markers). Emission cannot stop while inside the open marker, so even
        // with a row limit of 2 the whole response is kept in memory.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .withTombstone(rangeTombstone(0, 3).timestamp(100))
            .withTombstone(rangeTombstone(1, 4).timestamp(200))
            .withTombstone(rangeTombstone(2, 5).timestamp(50))
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseWithOverlappingRangeTombstonesAtDifferentTimestampsReversed()
    {
        // Same tombstones as above with reversed read order; still kept in memory (open marker cannot be split).
        inMemoryAssertion()
            .maxRows(2).rows(5).reversed()
            .withTombstone(rangeTombstone(0, 3).timestamp(100))
            .withTombstone(rangeTombstone(1, 4).timestamp(200))
            .withTombstone(rangeTombstone(2, 5).timestamp(50))
            .expectInMemory()
            .verify();
    }

    private InMemoryAssertionBuilder inMemoryAssertion()
    {
        return new InMemoryAssertionBuilder();
    }

    private class InMemoryAssertionBuilder
    {
        private int maxRows = 0;                 // per-request row limit; 0 = disabled
        private Integer maxBytes = null; // if set, the byte limit is sized to hold this many leading Unfiltered objects; null = disabled
        private int rows = 0;
        private boolean reversed = false;
        private final List<RangeTombstoneSpec> tombstones = new ArrayList<>();
        // true = the whole response is expected to be kept as an object graph, false = serialized in full
        private Boolean expectInMemory = null;

        InMemoryAssertionBuilder maxRows(int maxRows) { this.maxRows = maxRows; return this; }
        // Enables the byte limit, sizing it to exactly hold the first n Unfiltered objects the read produces
        InMemoryAssertionBuilder maxBytesAsSizeOfFirstNRows(int n) {this.maxBytes = n; return this; }
        InMemoryAssertionBuilder rows(int rows) { this.rows = rows; return this; }
        InMemoryAssertionBuilder reversed() { this.reversed = true; return this; }
        InMemoryAssertionBuilder withTombstone(RangeTombstoneSpec rt) { tombstones.add(rt); return this; }
        // The response stays within the limits and is kept in memory as an object graph.
        InMemoryAssertionBuilder expectInMemory() { this.expectInMemory = true; return this; }
        // A limit is crossed, so the whole response is serialized into a buffer.
        InMemoryAssertionBuilder expectSerialized() { this.expectInMemory = false; return this; }

        void verify()
        {
            int partitionKey = key();
            ReadCommand command = command(partitionKey, metadataWithClustering, reversed);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadataWithClustering, ByteBufferUtil.bytes(partitionKey)).timestamp(0);
            for (int i = 0; i < rows; i++)
                builder.row(i).add("v", i);
            for (RangeTombstoneSpec rt : tombstones)
            {
                Slice slice = Slice.make(Clustering.make(ByteBufferUtil.bytes(rt.start)),
                                         Clustering.make(ByteBufferUtil.bytes(rt.end)));
                builder.addRangeTombstone(new RangeTombstone(slice,
                                                             DeletionTime.build(rt.markedForDeleteAt, FBUtilities.nowInSeconds())
                                          )
                );
            }
            PartitionUpdate update = builder.build();

            long maxHeapSize = maxBytes == null ? 0 : heapSizeOfFirstNRows(update, reversed, maxBytes);

            ReadResponse localResponse = command.createResponse(singlePartitionIterator(update, reversed), rdi);
            ReadResponse inMemoryResponse = ReadResponse.createInMemoryDataResponse(singlePartitionIterator(update, reversed), command, rdi, maxRows, maxHeapSize);

            // The reconstructed response must match the plain serialized response regardless of representation.
            List<String> expected = collectUnfiltered(command, localResponse);
            assertEquals(expected, collectUnfiltered(command, inMemoryResponse));

            // When kept in memory the whole response is an object graph (inMemoryUnfilteredCount == all unfiltereds);
            // when serialized nothing is kept in memory (inMemoryUnfilteredCount == 0).
            if (expectInMemory != null)
                assertEquals("unexpected inMemoryUnfilteredCount",
                             expectInMemory ? expected.size() : 0,
                             inMemoryResponse.inMemoryUnfilteredCount());
        }
    }

    /**
     * Returns a heap size of the first {@code n} Unfiltered objects the read of
     * {@code update} produces. Feeding this to the in-memory response keeps those {@code n} objects in memory (the
     * (n+1)th crosses the >= threshold and overflows), which lets the assertions above express the byte-based split
     * as a count of leading Unfiltered objects even though the limit itself is a heap size.
     */
    private long heapSizeOfFirstNRows(PartitionUpdate update, boolean reversed, int n)
    {
        if (n <= 0)
            return 0;

        long size = 0;
        int count = 0;
        try (UnfilteredRowIterator rowIter = update.unfilteredIterator(ColumnFilter.SelectionColumnFilter.all(update.columns()), Slices.ALL, reversed))
        {
            while (rowIter.hasNext() && count < n)
            {
                size += unsharedHeapSize(rowIter.next());
                count++;
            }
        }
        return size;
    }

    private static long unsharedHeapSize(Unfiltered unfiltered)
    {
        return unfiltered.isRow()
               ? ((Row) unfiltered).unsharedHeapSize()
               : ((RangeTombstoneMarker) unfiltered).unsharedHeapSize();
    }

    private static RangeTombstoneSpec rangeTombstone(int start, int end)
    {
        return new RangeTombstoneSpec(start, end);
    }

    private static class RangeTombstoneSpec
    {
        final int start;
        final int end;
        long markedForDeleteAt;

        RangeTombstoneSpec(int start, int end)
        {
            this.start = start;
            this.end = end;
        }

        RangeTombstoneSpec timestamp(long ts)
        {
            this.markedForDeleteAt = ts;
            return this;
        }
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
        return singlePartitionIterator(update, false);
    }
    private UnfilteredPartitionIterator singlePartitionIterator(PartitionUpdate update, boolean reversed)
    {
        return singlePartitionIterator(update.unfilteredIterator(ColumnFilter.SelectionColumnFilter.all(update.columns()), Slices.ALL, reversed));
    }

    private UnfilteredPartitionIterator singlePartitionIterator(UnfilteredRowIterator rowIter)
    {
        return new AbstractUnfilteredPartitionIterator()
        {
            private boolean returned = false;

            public TableMetadata metadata() { return rowIter.metadata(); }

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
        return new StubReadCommand(key, metadata, true, false);
    }

    private ReadCommand command(int key, TableMetadata metadata)
    {
        return command(key, metadata, false);
    }

    private ReadCommand command(int key, TableMetadata metadata, boolean reversed)
    {
        return new StubReadCommand(key, metadata, false, reversed);

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
        return singlePartitionIterator(new WrappingUnfilteredRowIterator()
        {
            public UnfilteredRowIterator wrapped() { return baseRowIter; }

            @Override
            public void close()
            {
                rdi.markConsumed();
                baseRowIter.close();
            }
        });
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(int key, TableMetadata metadata, boolean isDigest, boolean reversed)
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
                  new ClusteringIndexSliceFilter(Slices.ALL, reversed),
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
